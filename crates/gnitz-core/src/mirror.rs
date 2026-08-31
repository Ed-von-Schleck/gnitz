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
//! **A read ceiling it inherits.** A reply carrying STRING or BLOB values goes
//! out as one frame, so the server refuses one past its frame cap — for every
//! client, not just this one. A bootstrap cannot narrow its way out (it reads the
//! view whole) and its one recovery is to bootstrap again, so a view whose
//! STRING/BLOB rows on one worker exceed the cap cannot be mirrored;
//! [`GnitzClient::mirror_view`] reports the server's refusal.
//!
//! **Cost.** One thread reads what W workers read in parallel. Total work is the
//! same, so a copy wins outright on point and small bounded reads and the margin
//! narrows as the walk grows: a full scan of a large view at high W is a loss.
//! Narrowing the bound is the lever.

use std::collections::HashMap;
use std::sync::Arc;

use crate::client::{delta_reply_schema, qualified_name, DeltaCursor, GnitzClient, RelDescriptor};
use crate::connection::RawBlock;
use crate::error::ClientError;
use crate::protocol::{Schema, ZSetBatch};
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
/// `Send`. It is deliberately not `Sync`, because a store is a live engine: the
/// one host that needs a `Sync` client wraps the whole client itself, rather
/// than every host paying a shim for the bound only that one imposes.
pub trait MirrorStore: Send {
    /// The data directory this store holds. What a second `attach_mirror` names
    /// when it refuses.
    fn base_dir(&self) -> &str;

    /// Register `tid` under `schema`, retracting whatever the store held at that
    /// id or under that qualified name; returns the ids retracted.
    ///
    /// The qualified name is what the store records the copy under, and the only
    /// thing it needs beyond the id: the store mints no ids of its own, so there
    /// is no `SCHEMA_TAB` id for a schema to be entered under.
    fn register(&mut self, tid: u64, schema_name: &str, name: &str, schema: &Schema) -> Result<Vec<u64>, MirrorError>;

    /// Tear `tid` down to `level`. See [`Invalidate`] — this is the *only* way a
    /// copy, a cursor or a registration is ever dropped. Idempotent, and a `tid`
    /// the local catalog does not hold is `Ok(())`.
    fn invalidate(&mut self, tid: u64, level: Invalidate) -> Result<(), MirrorError>;

    /// Apply `blocks` to `tid`'s copy under `shape`, then advance its cursor to
    /// `next`.
    fn ingest(&mut self, tid: u64, blocks: Vec<RawBlock>, shape: Shape, next: DeltaCursor) -> Result<(), MirrorError>;

    /// Every row of `tid`; `None` when the copy does not hold it — the read is
    /// the client's to delegate. Not [`Self::scan_spec`] with an all-rows spec: a
    /// bare `SELECT *` with no WHERE, ORDER BY, LIMIT or OFFSET over a relation
    /// with no hidden payload column builds no `ReadSpec` at all, so this is the
    /// shape the planner actually asks for there.
    fn scan(&mut self, tid: u64) -> Result<Option<(Arc<Schema>, ZSetBatch)>, MirrorError>;

    /// Run the encoded `ReadSpec` `spec` against `tid`'s copy, replying under
    /// `reply_schema`.
    fn scan_spec(&mut self, tid: u64, spec: &[u8], reply_schema: &Schema) -> Result<StoreRead, MirrorError>;

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
    fn checkpoint(&mut self) -> Result<(), MirrorError>;

    /// The message that poisoned this store, if any.
    fn poisoned(&self) -> Option<&str>;
}

/// A [`MirrorStore::scan_spec`] answer.
///
/// `NotHeld` and `Held(None)` must stay distinct: an empty answer off a readable
/// copy *is* the answer, while `NotHeld` sends the read upstream. Collapsing them
/// would push every legitimately empty mirrored read over the wire at a different
/// freshness. Named rather than an `Option<Option<_>>` because it crosses a trait
/// boundary. [`MirrorStore::scan`] needs no such enum — a held relation always
/// answers with a batch, so `Option` is exact there.
pub enum StoreRead {
    /// The copy does not hold this relation. The client delegates the read.
    NotHeld,
    /// The copy answered. `None` is an empty answer, not an absent one.
    Held(Option<ZSetBatch>),
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
/// everything the level above it does, and then more.**
///
/// That is what makes "a cursor never outlives its copy" structural rather than
/// a rule a caller has to remember: there is no way to spell "erase the copy"
/// that has not already dropped the cursor, because the erase is the second half
/// of a function whose first half is the drop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Invalidate {
    /// The feed position only. The copy stands but stops answering reads, and
    /// the next poll must reseed it.
    Cursor,
    /// The cursor, then the copy's rows — the registration stands, so a
    /// bootstrap can refill it under the same id.
    Copy,
    /// The cursor, the rows, and the catalog rows that name the relation; its
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
    /// catalog rejected, a read the spec could not express.
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

impl From<String> for MirrorError {
    fn from(m: String) -> Self {
        MirrorError::Engine(m)
    }
}

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
    /// The client-side shape a poll's *request* carries. `Arc` because a poll
    /// hands it to the wire verb by clone, and a deep one would allocate per
    /// column on every poll, including the empty ones.
    pub(crate) delta_reply_schema: Arc<Schema>,
}

/// Everything a mirroring client holds beyond a plain one.
pub(crate) struct MirrorState {
    pub(crate) store: Box<dyn MirrorStore>,
    /// The client's registrations — the poll's work list, and what
    /// [`GnitzClient::cursor_of`] reports a round for. Survives a
    /// [`GnitzClient::reconnect`]: it carries the `(schema_name, name)` the
    /// re-resolve runs on.
    pub(crate) views: HashMap<u64, MirroredView>,
    /// Canonical `"schema.name"` → id, for the local resolve.
    ///
    /// Not a second copy of the store's own qname index — a *separately
    /// clearable* one, which is why it is here rather than read back through the
    /// trait. See [`MirrorState::rebind_to_new_server`].
    pub(crate) by_qname: HashMap<String, u64>,
}

impl MirrorState {
    /// Drop `tid`'s registration and its name binding together, so neither can
    /// go stale against the other.
    pub(crate) fn drop_view(&mut self, tid: u64) {
        if let Some(v) = self.views.remove(&tid) {
            self.by_qname.remove(&qualified_name(&v.schema_name, &v.name));
        }
    }

    /// Point the copies at a connection that may be a **different server**,
    /// where the same qualified name is a different id and ids collide by
    /// construction.
    ///
    /// So nothing that binds a name to an id or reads a copy survives until a
    /// poll has re-resolved it, and everything that poll needs does: `views` is
    /// its work list and carries the `(schema, name)` to re-resolve by, every
    /// name binding goes, and every cursor goes — which shuts the read gate at
    /// once. Nothing is erased: that would redo the bootstrap's work for every
    /// view, polled or not.
    pub(crate) fn rebind_to_new_server(&mut self) {
        self.by_qname.clear();
        self.store.clear_cursors();
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

/// The refusal every mirror verb gives on a client that never attached a store.
///
/// It names no method: each binding spells the attach differently, and a message
/// naming the Rust one would send a Python host looking for a call it does not
/// have.
fn no_store() -> ClientError {
    ClientError::ServerError("this client mirrors nothing; attach a store before mirroring a view".to_string())
}

impl GnitzClient {
    fn mirror_state(&mut self) -> Result<&mut MirrorState, ClientError> {
        self.mirror.as_deref_mut().ok_or_else(no_store)
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

    /// Resolve `schema_name.name` upstream, reconcile that against the local
    /// catalog, and leave a registration for the result. Returns the relation's
    /// server id.
    ///
    /// Reconciliation is keyed by **id**, not by name: the id is what the copy is
    /// stored under and what a read names. The store keeps a registration that
    /// still holds the resolved id at the same layout and retracts anything else
    /// — a relation dropped and recreated, or altered — whose local copy is
    /// worthless anyway, since the bootstrap that follows is the only correct
    /// answer.
    pub(crate) fn reconcile_registration(&mut self, schema_name: &str, name: &str) -> Result<u64, ClientError> {
        let rel = self.resolve_relation(schema_name, name)?;
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

        let tid = rel.tid;
        let schema = Arc::clone(&rel.schema);
        let retracted = self.mirror_state()?.store.register(tid, schema_name, name, &schema)?;

        let entry = MirroredView {
            schema_name: schema_name.to_string(),
            name: name.to_string(),
            desc: rel,
            delta_reply_schema: Arc::new(delta_reply_schema(&schema)?),
        };
        let m = self.mirror_state()?;
        for old in retracted {
            m.drop_view(old);
        }
        m.by_qname.insert(qualified_name(schema_name, name), tid);
        m.views.insert(tid, entry);
        Ok(tid)
    }

    /// One view's poll: advance from its cursor, or — with none — re-resolve by
    /// name and reseed.
    ///
    /// **The cursor-less arm is never a bootstrap in place.** After a reconnect,
    /// or an earlier bootstrap that failed part way, the id the client holds may
    /// name a different relation upstream, or nothing, and a bootstrap at it
    /// would read the wrong rows into the copy.
    fn poll_one(&mut self, tid: u64) -> Result<(u64, PollResult), ClientError> {
        match self.mirror_state()?.store.cursor_of(tid) {
            Some(prev) => self.advance(tid, prev),
            None => self.reseed_by_name(tid),
        }
    }

    /// Apply `(prev, T]` to the copy, recovering from the three ways the feed
    /// can stop continuing.
    fn advance(&mut self, tid: u64, prev: DeltaCursor) -> Result<(u64, PollResult), ClientError> {
        let reply_schema = Arc::clone(&self.mirrored_view(tid)?.delta_reply_schema);
        match self.delta_poll_raw(tid, prev, &reply_schema) {
            Ok((blocks, next)) => {
                self.mirror_state()?.store.ingest(tid, blocks, Shape::Stamped, next)?;
                Ok((tid, PollResult::Advanced))
            }
            // The one recovery the feed names, and it is single: discard the copy
            // and bootstrap. It re-resolves first, because a foreign tag is how
            // the server reports a relation recreated under the same name.
            Err(ClientError::DeltaExpired) => self.reseed_by_name(tid),
            // Not a refusal to reconcile: re-resolving would block on a second
            // round trip past a Ctrl-C already consumed.
            Err(e @ ClientError::Interrupted(_)) => Err(e),
            // A poll can also fail because the id itself is gone rather than
            // because its tag moved: a `DROP VIEW v; CREATE VIEW v …` upstream
            // leaves the client holding an id that names nothing, and the read is
            // refused outright rather than answered with a foreign tag. So any
            // other refusal is re-resolved once — a *failure* of that probe
            // returns the original error, which is what the caller branches on.
            Err(e) => match self.relation_id_moved(tid) {
                Ok(true) => self.reseed_by_name(tid),
                _ => Err(e),
            },
        }
    }

    /// Whether `tid`'s name now resolves to a different id upstream.
    fn relation_id_moved(&mut self, tid: u64) -> Result<bool, ClientError> {
        let (schema_name, name) = self.mirrored_qname(tid)?;
        Ok(self.resolve_relation(&schema_name, &name)?.tid != tid)
    }

    /// Re-resolve `tid` by name and bootstrap whatever the name now denotes.
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
        self.bootstrap(tid2)
    }

    /// Replace `tid`'s copy with the view's whole current value.
    ///
    /// Only ever entered from an id that was just resolved — [`Self::mirror_view`]
    /// runs the resolve on the line above, and [`Self::reseed_by_name`] is the
    /// only other caller. That is also what keeps a reconnect from reading a
    /// stale descriptor out of the registration it deliberately kept: nothing
    /// reads one without a cursor beside it, and a reconnect drops every cursor.
    fn bootstrap(&mut self, tid: u64) -> Result<(u64, PollResult), ClientError> {
        // Everything between here and the ingest below is a copy that does not
        // exist, and the missing cursor is what says so.
        self.mirror_state()?.store.invalidate(tid, Invalidate::Copy)?;
        let view_schema = Arc::clone(&self.mirrored_view(tid)?.desc.schema);
        let (blocks, cursor) = self.delta_bootstrap_raw(tid, &view_schema)?;
        self.mirror_state()?.store.ingest(tid, blocks, Shape::Plain, cursor)?;
        Ok((tid, PollResult::Reseeded))
    }

    /// The report for one finished view.
    fn outcome(&self, view_id: u64, result: PollResult) -> PollOutcome {
        PollOutcome {
            view_id,
            cursor: self.cursor_of(view_id),
            result,
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
    /// would collide on the engine's `flock` and take seconds to discover.
    ///
    /// A second call is refused, naming the path already held and dropping the
    /// store it was passed (which releases that store's `flock`).
    ///
    /// **Dropping a mirroring client checkpoints**: drop glue reaches the store's
    /// own `Drop` through the `Box`, which is fsync-bound and unbounded in the
    /// copy's size. [`Self::close_mirror`] is where a host pays that
    /// deliberately.
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
            by_qname: HashMap::new(),
        }));
        Ok(())
    }

    /// Mirror `schema_name.name`, and bring its copy up to date.
    ///
    /// Idempotent, and the same call whether this is a first registration or a
    /// reopen: it resolves the relation upstream, reconciles that against
    /// whatever the local catalog replayed, and then either advances the copy
    /// from its persisted cursor or reseeds it. The outcome says which.
    ///
    /// Only a view with a delta feed can be mirrored — create it
    /// `WITH (delta = '<size>')`. A single-view call reports its own failure as
    /// `Err`, so the outcome never carries [`PollResult::Failed`].
    pub fn mirror_view(&mut self, schema_name: &str, name: &str) -> Result<PollOutcome, ClientError> {
        self.refuse_poisoned_mirror()?;
        // The rows the registration writes must carry the same spelling the
        // server's do, or a later resolve of the local catalog would miss — so
        // the name takes the same validate-then-fold every catalog gateway
        // applies, not a bare fold of its own.
        let schema_name = gnitz_wire::canonical_identifier(schema_name).map_err(ClientError::ServerError)?;
        let name = gnitz_wire::canonical_identifier(name).map_err(ClientError::ServerError)?;

        let tid = self.reconcile_registration(&schema_name, &name)?;
        // The line above is the resolve, which is what makes a direct bootstrap
        // legal here.
        let (id, result) = match self.mirror_state()?.store.cursor_of(tid) {
            Some(prev) => self.advance(tid, prev)?,
            None => self.bootstrap(tid)?,
        };
        Ok(self.outcome(id, result))
    }

    /// Stop mirroring `table_id`: its local catalog rows are retracted, its
    /// directory removed, and a later read of it is delegated upstream.
    ///
    /// The host's word for the bottom of [`Invalidate`]'s ladder — a host says
    /// "stop mirroring this", not "tear it down to its registration".
    pub fn forget_view(&mut self, table_id: u64) -> Result<(), ClientError> {
        self.mirror_state()?
            .store
            .invalidate(table_id, Invalidate::Registration)?;
        self.mirror_state()?.drop_view(table_id);
        Ok(())
    }

    /// Advance every mirrored view by one poll each, and report **one entry per
    /// view, whatever happened to it**.
    ///
    /// `Err` is reserved for the failures that are the call's rather than a
    /// view's: no store attached, a poisoned store, and an interrupt, which ends
    /// the loop because one Ctrl-C must end the call. Everything else is that
    /// view's [`PollResult::Failed`] entry, carrying the id
    /// [`Self::forget_view`] takes — so a per-view failure is quiet unless the
    /// caller reads the vector, which a correct subscriber does anyway for
    /// [`PollResult::Reseeded`].
    ///
    /// A poll drives no tick server-side, so a drain is "read the view against
    /// the server, then poll once".
    pub fn poll_mirror(&mut self) -> Result<Vec<PollOutcome>, ClientError> {
        self.refuse_poisoned_mirror()?;
        // A snapshot, because a reseed whose id moved mutates the map.
        let ids: Vec<u64> = self.mirror_state()?.views.keys().copied().collect();
        let mut out = Vec::with_capacity(ids.len());
        for tid in ids {
            // An earlier iteration's reseed retracted this id, and that view
            // already has an entry under the id it moved to. Reporting it would
            // put a `Failed` at an id nothing is mirroring.
            if !self.mirror_state()?.views.contains_key(&tid) {
                continue;
            }
            match self.poll_one(tid) {
                Ok((id, result)) => out.push(self.outcome(id, result)),
                Err(e @ ClientError::Interrupted(_)) => return Err(e),
                Err(e) => out.push(self.outcome(tid, PollResult::Failed(e))),
            }
        }
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

    /// Checkpoint (unless poisoned) and release the store, returning the
    /// checkpoint's own result so a failed final one is reported rather than
    /// swallowed by drop glue. The connection stays open and a later
    /// [`Self::attach_mirror`] is legal.
    ///
    /// `close`, not `detach`, because it fsyncs — and it is not a convenience:
    /// it is the **only** recovery from a poisoned store, which cannot otherwise
    /// be cleared without discarding a working connection.
    pub fn close_mirror(&mut self) -> Result<(), ClientError> {
        let Some(mut m) = self.mirror.take() else {
            return Err(no_store());
        };
        // Checkpointing a store that may be torn would publish the tear.
        let store = &mut m.store;
        let out = match store.poisoned() {
            Some(_) => Ok(()),
            None => Ok(store.checkpoint()?),
        };
        // The drop releases the directory `flock`, and is where a non-poisoned
        // store's own exit checkpoint would otherwise have run unreported.
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
    /// ran retired or renamed the relation the copy holds.
    ///
    /// The client-side entries go first and unconditionally: they are the read
    /// gate, so the copy stops answering even if the store's own teardown then
    /// fails.
    pub(crate) fn invalidate_own_copy(&mut self, tid: u64) -> Result<(), ClientError> {
        let Some(m) = self.mirror.as_deref_mut() else {
            return Ok(());
        };
        // A relation no copy knows about is left alone, rather than sent through
        // a teardown a poisoned store would refuse: every DDL statement on a
        // client with a poisoned store reaches this, and only the ones that
        // retire a mirrored relation may fail here.
        if !m.views.contains_key(&tid) && m.store.cursor_of(tid).is_none() {
            return Ok(());
        }
        m.drop_view(tid);
        m.store.invalidate(tid, Invalidate::Registration)?;
        Ok(())
    }
}
