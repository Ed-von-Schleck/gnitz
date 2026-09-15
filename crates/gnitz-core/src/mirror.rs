//! Mirroring: the store a client reads a local copy of a view through, and the
//! reconciliation that keeps that copy current.
//!
//! The seam sits at the **store**, not at the read surface. [`GnitzClient`] owns
//! the copy and delegates the reads it does not hold; the copy answers questions
//! about itself and never about a connection. So the trait below carries the
//! store's own lifecycle and two reads, and everything that resolves a name,
//! drives the feed or classifies a failure is the client's — [`GnitzClient`]'s
//! mirror methods and the state machine at the bottom of this file.
//!
//! **It is declared here so a client can hold a copy without linking an
//! engine.** Every signature is a `gnitz-core` type or a primitive, and
//! `gnitz-mirror` is the one implementor: a host that mirrors takes that crate
//! and its Linux-only engine, and a host that only reads remotely takes neither.
//!
//! # What a mirrored read promises
//!
//! **Freshness.** It answers at the copy's cursor round. A read against the
//! server drains pending ticks first, so it answers "what is current"; a poll
//! drives no tick and a local read never polls. So a copy is not
//! read-your-own-writes, and two mirrored views can sit at different rounds — a
//! read spanning both is no consistent cut. A relation the copy does not hold is
//! delegated upstream and keeps every guarantee a server read has.
//!
//! **Cost.** A copy trades W workers' parallelism for a local read: it wins
//! outright on point and small bounded reads, where the round trip dominates,
//! and the margin narrows as the walk grows until a full scan of a large view at
//! high W is a loss. Narrowing the bound is the lever.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use crate::client::{delta_reply_schema, park, DeltaCursor, GnitzClient};
use crate::connection::{Interest, PolledView, RawBlock, RelDescriptor, SlotId};
use crate::error::ClientError;
use crate::protocol::{ReplySchema, Schema, ZSetBatch};
use gnitz_wire::txn_frame::{DeltaPollItem, DELTA_POLL_MAX_VIEWS};
use gnitz_wire::RelClass;

// ---------------------------------------------------------------------------
// The seam
// ---------------------------------------------------------------------------

/// The local copy a [`GnitzClient`] reads through, and everything a client does
/// to one. Implemented once, by `gnitz_mirror::Mirror`.
///
/// An implementor answers questions about the copy it holds and never about a
/// connection, a statement or a plan — the client owns those, and the reads this
/// trait cannot answer are delegated by the client, not by the store.
///
/// `Send` is what lets [`GnitzClient`] hold a `Box<dyn MirrorStore>` and stay
/// `Send`. It is deliberately not `Sync`, because a store is a live engine: a
/// host that needs a `Sync` client wraps the whole client itself, rather than
/// every host paying a shim for a bound only some of them impose.
pub trait MirrorStore: Send {
    /// The data directory this store holds. What a second `attach_mirror` names
    /// when it refuses.
    fn base_dir(&self) -> &str;

    /// Register `tid` under `schema`, retracting whatever the store held at that
    /// id with another layout, or under that qualified name at another id.
    /// Returns the id whose registration that retracted, if any — the store's
    /// verdict, which is what keeps a client's own name→id bindings free of two
    /// live entries under one name.
    ///
    /// The qualified name is what the store records the copy under, and the only
    /// thing it needs beyond the id: the store mints no ids of its own, so there
    /// is no schema id for a schema to be entered under.
    fn register(
        &mut self,
        tid: u64,
        schema_name: &str,
        name: &str,
        schema: &Schema,
    ) -> Result<Option<u64>, MirrorError>;

    /// Tear `tid` down to `level`. See [`Invalidate`] — this is the *only* way a
    /// copy, a cursor or a registration is ever dropped. Idempotent, and a `tid`
    /// the store does not hold is `Ok(())`.
    fn invalidate(&mut self, tid: u64, level: Invalidate) -> Result<(), MirrorError>;

    /// Apply `blocks` to `tid`'s copy under `shape`, then advance its cursor to
    /// `next`.
    fn ingest(&mut self, tid: u64, blocks: Vec<RawBlock>, shape: Shape, next: DeltaCursor) -> Result<(), MirrorError>;

    /// Every row of `tid`'s copy, decoded under `schema` — the client-side schema
    /// its registration resolved, hidden columns included: the whole-copy read
    /// the client's `scan_local` verb asks for.
    fn scan(&mut self, tid: u64, schema: &Schema) -> Result<ZSetBatch, MirrorError>;

    /// Run `spec` against `tid`'s copy, replying under `reply_schema`.
    fn scan_spec(
        &mut self,
        tid: u64,
        spec: gnitz_wire::ReadSpec,
        reply_schema: &Schema,
    ) -> Result<ZSetBatch, MirrorError>;

    /// The round `tid`'s copy answers at, and by its presence that the copy is
    /// valid at all — the store half of the readability gate.
    fn cursor_of(&self, tid: u64) -> Option<DeltaCursor>;

    /// Drop every copy's feed position, leaving the copies themselves — what
    /// [`GnitzClient::reconnect`] does to shut the read gate on a connection that
    /// may name a different server.
    ///
    /// It covers every copy the store holds a position for, which is wider than
    /// the client's own registrations: a position outlives one session, and a
    /// cursor an earlier session left behind would otherwise stay honoured
    /// against the new server.
    fn clear_cursors(&mut self);

    /// Make every copy and its cursor durable.
    ///
    /// **A torn store publishes nothing**: an implementor that poisoned itself
    /// answers [`MirrorError::Poisoned`] instead. The client asks
    /// unconditionally, so that refusal is the store's alone to make.
    fn checkpoint(&mut self) -> Result<(), MirrorError>;

    /// The message that poisoned this store, if any.
    fn poisoned(&self) -> Option<&str>;
}

/// Which reply shape a train of blocks carries. The bytes do not say; the caller
/// does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// A bootstrap's: the view's own rows, in the view's own schema.
    Plain,
    /// A poll's: the delta store's rows, keyed by the round number prepended to
    /// the view's key.
    Stamped,
}

/// How far to tear a mirrored relation down. **A ladder: each level does
/// everything the level above it does, and then more**, which is what makes "a
/// cursor never outlives its copy" structural rather than a rule a caller has to
/// remember. The hazard that costs is stated beside the implementation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Invalidate {
    /// The feed position only. The copy stands but stops answering reads, and
    /// the next poll must reseed it.
    Cursor,
    /// The cursor, then the copy's rows — the registration stands, so a
    /// bootstrap can refill it under the same id.
    Copy,
    /// The cursor, the rows, and the record that names the relation; its
    /// directory goes with them.
    Registration,
}

/// Why a mirror operation did not happen.
///
/// It lives here rather than beside [`ClientError`] because it is the
/// [`MirrorStore`] seam's error channel and nothing else raises it;
/// [`ClientError::Mirror`] is how it reaches a caller of the client.
#[derive(Debug)]
pub enum MirrorError {
    /// The local engine refused or failed: a storage fault, a registration the
    /// registry rejected, a read the spec could not express.
    Engine(String),
    /// The store is poisoned and refuses every further call that touches a copy.
    /// A delta that did not reach the store leaves a hole the cursor would step
    /// over, so continuing would answer reads off a copy that is silently
    /// missing rows. The message names what poisoned it.
    ///
    /// [`GnitzClient::close_mirror`] is the only recovery.
    Poisoned(String),
}

impl std::fmt::Display for MirrorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MirrorError::Engine(m) => write!(f, "engine: {m}"),
            MirrorError::Poisoned(m) => write!(f, "mirror store is poisoned: {m}"),
        }
    }
}

impl std::error::Error for MirrorError {}

/// How a store failure reaches a caller of the client. It keeps its class rather
/// than flattening to a message, so a poisoned copy stays something a host can
/// catch and recover from.
impl From<MirrorError> for ClientError {
    fn from(e: MirrorError) -> Self {
        ClientError::Mirror(e)
    }
}

// ---------------------------------------------------------------------------
// What a poll reports
// ---------------------------------------------------------------------------

/// What one view's poll did.
#[derive(Debug)]
pub struct PollOutcome {
    /// The relation's server id, which a recreated view moves — so this is the
    /// id the view is mirrored under *after* the call, not the one it went in
    /// with.
    pub view_id: u64,
    /// The round this view now answers at; `None` when it has no valid copy.
    ///
    /// Carried rather than read back through [`GnitzClient::cursor_of`]: an
    /// async client shares one store across clones, where another clone can poll
    /// between the two calls.
    pub cursor: Option<DeltaCursor>,
    pub result: PollResult,
}

/// Which of the three things a poll of one view did.
#[derive(Debug)]
pub enum PollResult {
    /// Deltas were applied to the copy that was already there.
    Advanced,
    /// The copy was discarded and re-read whole, so anything derived from its
    /// previous contents is stale in a way no delta explains. **A discontinuity
    /// every subscriber has to react to**, and no cursor carries it: an
    /// expiry-driven reseed inside one boot keeps the tag and moves the tick
    /// forward, which is exactly what an ordinary advance looks like.
    Reseeded,
    /// This view's poll failed and the others went on. A view dropped upstream
    /// fails this way forever; [`GnitzClient::forget_view`] at
    /// [`PollOutcome::view_id`] clears it.
    Failed(ClientError),
}

impl PollResult {
    /// Whether the copy was re-read whole — what a host branches on.
    pub fn reseeded(&self) -> bool {
        matches!(self, PollResult::Reseeded)
    }
}

// ---------------------------------------------------------------------------
// The client's half
// ---------------------------------------------------------------------------

/// One mirrored view, as the client tracks it: what to re-resolve it by, and the
/// two schemas the wire verbs are handed.
pub(crate) struct MirroredView {
    /// Kept split rather than joined: the state machine re-resolves by
    /// `(schema, name)`.
    pub(crate) schema_name: String,
    pub(crate) name: String,
    /// The upstream descriptor registration resolved, handed back verbatim to
    /// every local resolve. Its schema is the client-side one, hidden columns
    /// included, which keeps `pk_stride` right for a view whose physical PK is a
    /// synthetic hidden column.
    pub(crate) desc: Arc<RelDescriptor>,
    /// The shape a poll's *request* carries, prepared once: it is fixed for the
    /// registration's life, so a poll clones one `Arc` instead of deep-cloning a
    /// schema and re-encoding its block.
    pub(crate) delta_reply: Arc<ReplySchema>,
}

/// Everything a mirroring client holds beyond a plain one.
pub(crate) struct MirrorState {
    pub(crate) store: Box<dyn MirrorStore>,
    /// The client's registrations — the poll's work list, and what
    /// [`GnitzClient::cursor_of`] reports a round for. Survives a
    /// [`GnitzClient::reconnect`]: it carries the `(schema_name, name)` the
    /// re-resolve runs on.
    pub(crate) views: HashMap<u64, MirroredView>,
    /// Views owed a [`PollResult::Reseeded`]: a bootstrap ran and no caller has
    /// been handed the outcome yet. Cleared only by delivery, so an interrupt
    /// that discards a report re-announces it next poll.
    owed_reseed: HashSet<u64>,
}

impl MirrorState {
    /// Check the tag `fetched` carries against `prev`, ingest, advance the cursor.
    ///
    /// **No recovery here**: a recovery can re-point a registration at a view
    /// whose reply is already in hand, and applying it twice doubles every weight
    /// in the overlap with the row set unchanged. Failures go back unclassified,
    /// to [`GnitzClient::recover`].
    fn advance_from(
        &mut self,
        tid: u64,
        prev: DeltaCursor,
        fetched: Result<(Vec<RawBlock>, DeltaCursor), ClientError>,
    ) -> Result<PollResult, ClientError> {
        let (blocks, at) = fetched?;
        let next = prev.advanced_to(at)?;
        self.store.ingest(tid, blocks, Shape::Stamped, next)?;
        Ok(PollResult::Advanced)
    }
}

// ---------------------------------------------------------------------------
// The reconciliation state machine
// ---------------------------------------------------------------------------
//
// It lands on the client and not in a crate of its own because what it is made
// of is connection work: it resolves names upstream, drives the two raw delta
// verbs, and classifies what comes back. The only non-connection steps in it are
// calls through `MirrorStore` — which is also what makes it exist once for the
// blocking, async and Python clients alike.

/// One poll's per-view outcomes, `(view id, that view's own result)`.
type ViewPollResults = Vec<(u64, Result<PollResult, ClientError>)>;

/// A whole request failed: fail every view it never answered — the cause to the
/// first, `Closed` to the rest, which is what a request each would have got.
/// Recorded here rather than stashed, because by the time a request is known to
/// have failed no further reply of it can arrive.
fn fail_range(
    applied: &mut ViewPollResults,
    views: &[(u64, DeltaCursor, Arc<ReplySchema>)],
    unanswered: Range<usize>,
    cause: ClientError,
) {
    let mut cause = Some(cause);
    for i in unanswered {
        applied.push((views[i].0, Err(cause.take().unwrap_or(ClientError::Closed))));
    }
}

impl GnitzClient {
    fn mirror_state(&mut self) -> Result<&mut MirrorState, ClientError> {
        self.mirror.as_deref_mut().ok_or(ClientError::NoMirrorStore)
    }

    /// The `(schema, name)` `tid` is registered under, for a re-resolve.
    fn mirrored_qname(&mut self, tid: u64) -> Result<(String, String), ClientError> {
        let v = self.mirrored_view(tid)?;
        Ok((v.schema_name.clone(), v.name.clone()))
    }

    fn mirrored_view(&mut self, tid: u64) -> Result<&MirroredView, ClientError> {
        self.mirror_state()?
            .views
            .get(&tid)
            .ok_or_else(|| ClientError::ServerError(format!("relation {tid} is not mirrored")))
    }

    /// Resolve `schema_name.name` upstream, check it can be mirrored, and bind
    /// the result. Returns the relation's server id.
    ///
    /// Reconciliation is keyed by **id**, not by name: the id is what the copy is
    /// stored under and what a read names. The store keeps a registration that
    /// still holds the resolved id at the same layout and retracts anything else
    /// — a relation dropped and recreated, or altered — whose local copy is
    /// worthless anyway, since the bootstrap that follows is the only correct
    /// answer.
    ///
    /// The one entry that takes **host-supplied** names, so the canonical fold
    /// every catalog gateway applies happens here: a record spelled differently
    /// from the server's row would never match a later local resolve.
    pub(crate) fn reconcile_registration(&mut self, schema_name: &str, name: &str) -> Result<u64, ClientError> {
        let schema_name = gnitz_wire::canonical_identifier(schema_name)?;
        let name = gnitz_wire::canonical_identifier(name)?;
        let rel = self.resolve_relation(&schema_name, &name)?;
        if !rel.class.is_view() {
            return Err(ClientError::ServerError(format!(
                "'{schema_name}.{name}' is a {}; only a view can be mirrored",
                rel.class.noun()
            )));
        }
        if rel.class == RelClass::BoundedView {
            return Err(ClientError::ServerError(format!(
                "view '{schema_name}.{name}' is capacity-bounded, and a capacity and a feed \
                 are refused together, so it carries no feed to subscribe to"
            )));
        }
        if !rel.delta {
            return Err(ClientError::ServerError(format!(
                "view '{schema_name}.{name}' keeps no delta feed; \
                 create it WITH (delta = '<size>') to mirror it"
            )));
        }
        self.bind(&schema_name, &name, rel)
    }

    /// Record `desc.tid` as mirrored under `schema_name.name` — **already
    /// canonical** — with `desc` the descriptor local resolves are answered from.
    /// Returns that id.
    ///
    /// Class, capacity and feed are **not** re-checked, so a caller holding a
    /// descriptor for a relation whose identity did not change — a rename — binds
    /// with no second round trip.
    pub(crate) fn bind(&mut self, schema_name: &str, name: &str, desc: Arc<RelDescriptor>) -> Result<u64, ClientError> {
        let tid = desc.tid;
        let schema = Arc::clone(&desc.schema);
        // Built before the store registers, so a schema the delta shape cannot
        // carry leaves no record the client never entered.
        let entry = MirroredView {
            schema_name: schema_name.to_string(),
            name: name.to_string(),
            desc,
            delta_reply: Arc::new(ReplySchema::new(Arc::new(delta_reply_schema(&schema)?), tid)),
        };
        let retracted = self.mirror_state()?.store.register(tid, schema_name, name, &schema)?;
        let m = self.mirror_state()?;
        // The store's verdict, not a second scan: this map and the store's
        // records must name the same displaced id, or `resolve_local_first`
        // picks one of two live entries out of a `HashMap`.
        if let Some(old) = retracted {
            m.views.remove(&old);
        }
        m.views.insert(tid, entry);
        Ok(tid)
    }

    /// Bring `tid`'s copy up to date: apply `(prev, T]`, or bootstrap it when it
    /// has no feed position to advance from.
    ///
    /// **`tid` must have been resolved upstream just now**, or the bootstrap arm
    /// reads whatever else the id has come to name.
    ///
    /// Re-enters itself through `recover` → `reseed_by_name`, once per
    /// registration those retract, so the depth is the mirrored-view count.
    fn sync(&mut self, tid: u64) -> Result<(u64, PollResult), ClientError> {
        let Some(prev) = self.mirror_state()?.store.cursor_of(tid) else {
            return self.bootstrap(tid);
        };
        let fetched = self.fetch_delta(tid, prev);
        let m = self.mirror_state()?;
        match m.advance_from(tid, prev, fetched) {
            Ok(result) => Ok((tid, result)),
            Err(e) => self.recover(tid, e),
        }
    }

    /// Fetch `(prev, T]` for one view — the single-view form of
    /// [`Self::delta_poll_many`], and the same unvalidated watermark.
    fn fetch_delta(&mut self, tid: u64, prev: DeltaCursor) -> Result<(Vec<RawBlock>, DeltaCursor), ClientError> {
        let reply_schema = Arc::clone(&self.mirrored_view(tid)?.delta_reply);
        let after = prev.poll_after()?;
        self.delta_read_raw(tid, after, &reply_schema)
    }

    /// The recovery a failed poll names, or the failure itself. Two classes name
    /// one; **every other refusal is returned rather than probed**, since a dead
    /// socket answers no differently the second time.
    fn recover(&mut self, tid: u64, err: ClientError) -> Result<(u64, PollResult), ClientError> {
        match err {
            // The feed stopped continuing. Re-resolve, then re-read whole: a
            // foreign tag is how a relation recreated under the same name reads.
            ClientError::DeltaExpired => self.reseed_by_name(tid),
            // The id is gone, and only the client's own name binding says whether
            // the view moved or died. Died → `Failed` with the cursor untouched,
            // so the copy keeps answering until the host forgets it.
            e @ ClientError::NotFound { .. } => match self.relation_id_moved(tid) {
                Ok(true) => self.reseed_by_name(tid),
                // Not recreated, or the probe failed too: either way the poll's
                // own error is the one that says what happened to this view.
                Ok(false) | Err(_) => Err(e),
            },
            e => Err(e),
        }
    }

    /// Whether `tid`'s name now resolves to a different id upstream.
    fn relation_id_moved(&mut self, tid: u64) -> Result<bool, ClientError> {
        let (schema_name, name) = self.mirrored_qname(tid)?;
        Ok(self.resolve_relation(&schema_name, &name)?.tid != tid)
    }

    /// Re-resolve `tid` by name and sync whatever the name now denotes.
    ///
    /// **The cursor goes first**, because the re-resolve can fail — an
    /// interrupt, a transport error, a removed feed — and a surviving cursor
    /// would leave the read gate open on a copy whose feed is known not to
    /// continue, with no reseed ever reported.
    ///
    /// Within one server the re-resolved id names the same relation or a newer
    /// one, never an older one's rows: relation ids are monotone and durably
    /// high-watermarked, so they are not recycled.
    fn reseed_by_name(&mut self, tid: u64) -> Result<(u64, PollResult), ClientError> {
        self.mirror_state()?.store.invalidate(tid, Invalidate::Cursor)?;
        let (schema_name, name) = self.mirrored_qname(tid)?;
        let tid2 = self.reconcile_registration(&schema_name, &name)?;
        // Not a bootstrap: the name may now denote a view this client already
        // mirrors, whose copy is live and correct.
        self.sync(tid2)
    }

    /// Replace `tid`'s copy with the view's whole current value.
    ///
    /// Reads whatever `tid` now names, so [`Self::sync`]'s precondition is this
    /// one too.
    fn bootstrap(&mut self, tid: u64) -> Result<(u64, PollResult), ClientError> {
        // Everything between here and the ingest below is a copy that does not
        // exist, and the missing cursor is what says so.
        self.mirror_state()?.store.invalidate(tid, Invalidate::Copy)?;
        let view_schema = ReplySchema::new(Arc::clone(&self.mirrored_view(tid)?.desc.schema), tid);
        let (blocks, cursor) = self.delta_bootstrap_raw(tid, &view_schema)?;
        let m = self.mirror_state()?;
        m.store.ingest(tid, blocks, Shape::Plain, cursor)?;
        m.owed_reseed.insert(tid);
        Ok((tid, PollResult::Reseeded))
    }

    /// The report for one finished view. A reseed still owed
    /// ([`MirrorState::owed_reseed`]) is announced here, one poll late — its
    /// definition, "anything derived from its previous contents is stale", holds
    /// just as well then.
    fn outcome(&mut self, view_id: u64, result: PollResult) -> PollOutcome {
        let owed = self.mirror.as_deref().is_some_and(|m| m.owed_reseed.contains(&view_id));
        // A failure is reported as itself, and stays owed.
        let result = match (result, owed) {
            (PollResult::Advanced, true) => PollResult::Reseeded,
            (r, _) => r,
        };
        PollOutcome {
            view_id,
            cursor: self.cursor_of(view_id),
            result,
        }
    }

    /// `out` has reached a caller, so nothing in it is owed any more. Every
    /// return path that hands a report over calls this, and only those.
    fn reported(&mut self, out: &[PollOutcome]) {
        if let Some(m) = self.mirror.as_deref_mut() {
            for o in out.iter().filter(|o| o.result.reseeded()) {
                m.owed_reseed.remove(&o.view_id);
            }
        }
    }

    /// Report one view, keyed by the **final** view id: a phase-2 recovery can
    /// land on a view phase 1 already advanced, so a collision keeps whichever
    /// side saw a reseed and the later cursor. The source id gets no entry — its
    /// registration moved, and a host watching it sees it leave
    /// [`Self::mirrored_ids`].
    fn record(&mut self, out: &mut Vec<PollOutcome>, view_id: u64, result: PollResult) {
        let o = self.outcome(view_id, result);
        let Some(prev) = out.iter_mut().find(|p| p.view_id == o.view_id) else {
            out.push(o);
            return;
        };
        if o.cursor.map(|c| c.tick) > prev.cursor.map(|c| c.tick) {
            prev.cursor = o.cursor;
        }
        if o.result.reseeded() {
            prev.result = PollResult::Reseeded;
        }
    }
}

// ---------------------------------------------------------------------------
// The host-facing surface
// ---------------------------------------------------------------------------

impl GnitzClient {
    /// Read a local copy of one or more views through `store`.
    ///
    /// It takes an opened store rather than a directory, because opening one is
    /// the engine's job and this crate does not link it: a host writes
    /// `client.attach_mirror(gnitz_mirror::Mirror::open(dir)?)?`, and the
    /// dependency is the host's. The directory has no default — a derived one
    /// would collide on the engine's `flock`.
    ///
    /// A second call is refused, naming the path already held and dropping the
    /// store it was passed (which releases that store's `flock`).
    ///
    /// **Dropping a mirroring client does not checkpoint**: [`Self::close_mirror`]
    /// is where a host pays the exit checkpoint, and reports it. A drop forfeits
    /// the rounds since the last checkpoint — at most one bootstrap per view,
    /// when the feed no longer covers them.
    pub fn attach_mirror(&mut self, store: impl MirrorStore + 'static) -> Result<(), ClientError> {
        if let Some(m) = &self.mirror {
            return Err(ClientError::ServerError(format!(
                "this client already mirrors at '{}'; close_mirror before attaching another store",
                m.store.base_dir()
            )));
        }
        self.mirror = Some(Box::new(MirrorState {
            store: Box::new(store),
            views: HashMap::new(),
            owed_reseed: HashSet::new(),
        }));
        Ok(())
    }

    /// Mirror `schema_name.name`, and bring its copy up to date.
    ///
    /// Idempotent, and the same call whether this is a first registration or a
    /// reopen: it resolves the relation upstream, reconciles that against
    /// whatever record the store replayed, and then either advances the copy
    /// from its persisted cursor or reseeds it. The outcome says which.
    ///
    /// Only a view with a delta feed can be mirrored — create it
    /// `WITH (delta = '<size>')`. A single-view call reports its own failure as
    /// `Err`, so the outcome never carries [`PollResult::Failed`].
    pub fn mirror_view(&mut self, schema_name: &str, name: &str) -> Result<PollOutcome, ClientError> {
        self.refuse_poisoned_mirror()?;
        // The resolve inside it is what makes `sync`'s bootstrap arm legal here.
        let tid = self.reconcile_registration(schema_name, name)?;
        let (id, result) = self.sync(tid)?;
        let out = self.outcome(id, result);
        self.reported(std::slice::from_ref(&out));
        Ok(out)
    }

    /// Stop mirroring `table_id`: its record is retracted, its directory
    /// removed, and a later read of it is delegated upstream.
    ///
    /// The host's word for the bottom of [`Invalidate`]'s ladder — a host says
    /// "stop mirroring this", not "tear it down to its registration".
    pub fn forget_view(&mut self, table_id: u64) -> Result<(), ClientError> {
        // The client-side entry goes first, as in `invalidate_own_copy`: it is
        // the read gate `resolve_local_first` consults. Unconditional, unlike
        // that one, because a reopened store holds copies this client has not
        // registered — and forgetting one is exactly the call that erases it.
        let m = self.mirror_state()?;
        m.views.remove(&table_id);
        m.store.invalidate(table_id, Invalidate::Registration)?;
        Ok(())
    }

    /// Advance every view in `views`, one request per `DELTA_POLL_MAX_VIEWS`,
    /// ingesting each view's blocks as that view's terminal arrives — so a poll
    /// over M views holds one train, not M. The watermark comes back
    /// unvalidated; the tag rule is [`MirrorState::advance_from`]'s.
    ///
    /// A view that fails gets **that view's** own entry. Only a caller error (a
    /// cursor with no round to poll after) and an interrupt end the call.
    fn delta_poll_many(
        &mut self,
        views: &[(u64, DeltaCursor, Arc<ReplySchema>)],
    ) -> Result<ViewPollResults, ClientError> {
        // Split, so the ingest writes to the mirror while the session drains:
        // disjoint fields, so both borrows hold.
        let Self { session, mirror, park_hook, .. } = self;
        let Some(mirror) = mirror.as_deref_mut() else {
            return Err(ClientError::NoMirrorStore);
        };
        // A cursor at round 0 names the bootstrap bound, which would read the
        // whole view in the wrong shape: the caller's bug, raised before
        // anything is encoded.
        let afters = views
            .iter()
            .map(|(_, prev, _)| prev.poll_after())
            .collect::<Result<Vec<_>, _>>()?;

        let mut applied = Vec::with_capacity(views.len());
        // Per open request, the positions it has yet to answer. They are filled
        // in order — the session refuses a terminal naming any other view — so
        // the unanswered set is always this tail, and failing a request is
        // failing the tail whole.
        let mut open: Vec<(SlotId, Range<usize>)> = Vec::new();
        for (chunk_at, chunk) in views.chunks(DELTA_POLL_MAX_VIEWS).enumerate() {
            let start = chunk_at * DELTA_POLL_MAX_VIEWS;
            let batch: Vec<DeltaPollItem> = chunk
                .iter()
                .enumerate()
                .map(|(i, (tid, _, schema))| DeltaPollItem {
                    view_id: *tid,
                    after_tick: afters[start + i],
                    reply_block: schema.block(),
                })
                .collect();
            let range = start..start + chunk.len();
            match session.submit_delta_poll(&batch) {
                Ok(slot) => open.push((slot, range)),
                Err(e) => fail_range(&mut applied, views, range, e),
            }
        }

        let mut ready = Interest::WRITE;
        while !open.is_empty() {
            let stepped = {
                // Addressed by slot, so a train an earlier call abandoned is
                // recognised rather than matched onto a live view of the same id.
                let mut sink = |slot: SlotId, result: PolledView| {
                    let Some((_, range)) = open.iter_mut().find(|(s, _)| *s == slot) else {
                        return;
                    };
                    let i = range.start;
                    range.start += 1;
                    let (tid, prev, _) = views[i];
                    let fetched = result.map(|(blocks, w)| (blocks, DeltaCursor::from_watermark(w)));
                    applied.push((tid, mirror.advance_from(tid, prev, fetched)));
                };
                session.step_polling(ready, Some(&mut sink))
            };
            match stepped {
                // Every view of a rejected request it had yet to answer fails
                // with it.
                Ok(done) => {
                    for (slot, result) in done {
                        let Some(at) = open.iter().position(|(s, _)| *s == slot) else {
                            continue; // an earlier call's abandoned train
                        };
                        let (_, range) = open.remove(at);
                        if let Err(e) = result {
                            fail_range(&mut applied, views, range, e);
                        }
                    }
                }
                // The framing is no longer trustworthy, so the connection goes
                // and every request still open goes with it.
                Err(e) => {
                    session.close();
                    let mut cause = Some(e);
                    for (_, range) in std::mem::take(&mut open) {
                        let e = cause.take().unwrap_or(ClientError::Closed);
                        fail_range(&mut applied, views, range, e);
                    }
                }
            }
            if !open.is_empty() {
                ready = park(session, park_hook)?;
            }
        }
        Ok(applied)
    }

    /// Advance every mirrored view by one poll each, and report **one entry per
    /// mirrored view, whatever happened to it** — keyed by the id each view is
    /// mirrored under *after* the call, so a view that moved is reported once,
    /// at its new id.
    ///
    /// **Two phases.** One advances every view that has a position to advance
    /// from, in a single round trip, stashing failures unclassified; two runs the
    /// recoveries sequentially. See [`MirrorState::advance_from`] for why
    /// recovery may not run inside phase one.
    ///
    /// `Err` is reserved for the failures that are the call's rather than a
    /// view's: no store attached, a poisoned store, a transport failure that ends
    /// the connection, and an interrupt, which ends the call because one Ctrl-C
    /// must. Everything else is that view's [`PollResult::Failed`] entry,
    /// carrying the id [`Self::forget_view`] takes — so a per-view failure is
    /// quiet unless the caller reads the vector, which a correct subscriber does
    /// anyway for [`PollResult::Reseeded`].
    ///
    /// A poll drives no tick server-side, so a drain is "read the view against
    /// the server, then poll once".
    pub fn poll_mirror(&mut self) -> Result<Vec<PollOutcome>, ClientError> {
        self.refuse_poisoned_mirror()?;

        // ── Phase 1: advance, in one round trip ────────────────────────────
        // A cursor at tick 0 names no round to poll after, so it is phase 2's.
        let mut requests: Vec<(u64, DeltaCursor, Arc<ReplySchema>)> = Vec::new();
        let mut recoveries: Vec<(u64, Option<ClientError>)> = Vec::new();
        {
            let m = self.mirror_state()?;
            for (&tid, v) in m.views.iter() {
                match m.store.cursor_of(tid).filter(|c| c.poll_after().is_ok()) {
                    Some(prev) => requests.push((tid, prev, Arc::clone(&v.delta_reply))),
                    None => recoveries.push((tid, None)),
                }
            }
        }

        let applied = self.delta_poll_many(&requests)?;

        // ── Phase 2: recover, strictly after every phase-1 ingest ──────────
        let mut out: Vec<PollOutcome> = Vec::with_capacity(applied.len() + recoveries.len());
        for (tid, r) in applied {
            match r {
                Ok(result) => self.record(&mut out, tid, result),
                Err(e) => recoveries.push((tid, Some(e))),
            }
        }
        for (tid, err) in recoveries {
            // An earlier recovery retracted this id, and that view already has an
            // entry under the id it moved to. Reporting it would put a `Failed`
            // at an id nothing is mirroring.
            if !self.mirror_state()?.views.contains_key(&tid) {
                continue;
            }
            let r = match err {
                None => self.reseed_by_name(tid),
                Some(e) => self.recover(tid, e),
            };
            match r {
                Ok((id, result)) => self.record(&mut out, id, result),
                // The report is discarded with the call, so what it announced
                // stays owed and the next poll announces it.
                Err(e @ ClientError::Interrupted(_)) => return Err(e),
                Err(e) => self.record(&mut out, tid, PollResult::Failed(e)),
            }
        }
        self.reported(&out);
        Ok(out)
    }

    /// The views this client holds a registration for — the set
    /// [`Self::poll_mirror`] advances, which is wider than [`Self::mirrors`] by
    /// the ones whose copy the next poll has yet to make valid.
    ///
    /// Answered off the client's own map, and provisional between a
    /// [`Self::reconnect`] and the next poll: the keys are still the previous
    /// server's ids, which is exactly what the poll will attempt.
    pub fn mirrored_ids(&self) -> Vec<u64> {
        self.mirror
            .as_deref()
            .map_or_else(Vec::new, |m| m.views.keys().copied().collect())
    }

    /// Whether a read of `table_id` is answered locally — which is exactly
    /// whether there is a round to answer it at.
    pub fn mirrors(&self, table_id: u64) -> bool {
        self.cursor_of(table_id).is_some()
    }

    /// The round a local read of `table_id` answers at, or `None` when there is
    /// no valid copy to read one off.
    ///
    /// A registration **and** a cursor: a copy a previous session left behind
    /// carries a position this one has not claimed, and no read reaches it.
    ///
    /// The tick is the master's global round counter, shared by every relation,
    /// so it advances over rounds that carried this view nothing. Whether a copy
    /// was discarded is [`PollResult::Reseeded`], not this.
    pub fn cursor_of(&self, table_id: u64) -> Option<DeltaCursor> {
        self.mirror
            .as_deref()
            .filter(|m| m.views.contains_key(&table_id))
            .and_then(|m| m.store.cursor_of(table_id))
    }

    /// Make every copy and its cursor durable.
    ///
    /// A failure is reported, not fatal: the flush writes shards and publishes
    /// manifests, neither of which mutates what a copy holds, so the store stays
    /// usable and a retry is sound.
    pub fn checkpoint_mirror(&mut self) -> Result<(), ClientError> {
        Ok(self.mirror_state()?.store.checkpoint()?)
    }

    /// Checkpoint and release the store, returning the checkpoint's own result so
    /// a failed final one is reported rather than swallowed by drop glue. The
    /// connection stays open and a later [`Self::attach_mirror`] is legal.
    ///
    /// It is the **only** recovery from a poisoned store, which cannot otherwise
    /// be cleared without discarding a working connection — so a poisoned store
    /// declining the checkpoint is not a failure to close.
    pub fn close_mirror(&mut self) -> Result<(), ClientError> {
        let Some(mut m) = self.mirror.take() else {
            return Err(ClientError::NoMirrorStore);
        };
        let out = match m.store.checkpoint() {
            Err(MirrorError::Poisoned(_)) => Ok(()),
            other => other.map_err(ClientError::from),
        };
        // The drop releases the directory lock.
        drop(m);
        out
    }

    /// The message that poisoned this client's store, if any. Answers on a
    /// poisoned store — diagnosing one is what it is for.
    pub fn mirror_poisoned(&self) -> Option<&str> {
        self.mirror.as_deref().and_then(|m| m.store.poisoned())
    }

    /// The call-level refusal [`Self::mirror_view`] and [`Self::poll_mirror`]
    /// take: no store, or a poisoned one. Every other verb that touches a copy is
    /// refused by the store itself; these two check first because they would
    /// otherwise spend a round trip before finding out.
    fn refuse_poisoned_mirror(&mut self) -> Result<(), ClientError> {
        match self.mirror_state()?.store.poisoned() {
            Some(why) => Err(MirrorError::Poisoned(why.to_string()).into()),
            None => Ok(()),
        }
    }

    /// Tear down this client's own copy of `tid`, because a DDL statement it just
    /// ran retired the relation the copy holds.
    ///
    /// **Infallible, because it runs after the statement committed.** The
    /// `views.remove` is the read gate and cannot fail; the teardown past it is
    /// reclamation, and its only refusal is a poisoned store — which is already
    /// an error in its own right, reported at [`Self::mirror_poisoned`] and
    /// raised by every read that reaches it.
    pub(crate) fn invalidate_own_copy(&mut self, tid: u64) {
        let Some(m) = self.mirror.as_deref_mut() else {
            return;
        };
        m.views.remove(&tid);
        let _ = m.store.invalidate(tid, Invalidate::Registration);
    }
}

#[cfg(test)]
#[path = "tests/mirror.rs"]
mod tests;
