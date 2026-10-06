//! Mirroring: the store a client reads a local copy of a view through, and the
//! reconciliation that keeps that copy current.
//!
//! The seam sits at the **store**, not at the read surface. [`GnitzClient`] owns
//! the copy and delegates the reads it does not hold; the copy answers questions
//! about itself and never about a connection. So the trait below carries the
//! store's own lifecycle and one read, and everything that resolves a name,
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
//! high W is a loss. Narrowing the bound is the lever. Each index the view had at
//! its last resolve is a further local store, maintained on every applied delta.

use gnitz_expr::SchemaFacts;
use gnitz_wire::{WireFault, WireStatus};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::client::{offload, DeltaPoll, GnitzClient, Host};
use crate::connection::{DeltaCursor, Polled, RawBlock, RelDescriptor};
use crate::error::ClientError;
use crate::{Schema, ZSetBatch};
use gnitz_wire::txn_frame::DeltaPollItem;
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
/// `Send`, because the client runs its disk work where its host lets work
/// block, which may be another thread. Not `Sync`: one call runs at a time.
pub trait MirrorStore: Send {
    /// The data directory this store holds. What a second `attach_mirror` names
    /// when it refuses.
    fn base_dir(&self) -> &str;

    /// Register `desc.tid` as `schema_name.name`, with `desc`'s schema and
    /// indexes, retracting whatever the store held at that id as another
    /// relation, or under that qualified name at another id. Returns the id
    /// whose registration that retracted, if any — the store's verdict, which is
    /// what keeps a client's own name→id bindings free of two live entries under
    /// one name.
    ///
    /// The qualified name is what the store records the copy under: the store
    /// mints no ids of its own, so there is no schema id for a schema to be
    /// entered under.
    fn register(&mut self, schema_name: &str, name: &str, desc: &RelDescriptor) -> Result<Option<u64>, MirrorError>;

    /// Tear `tid` down to `level`. See [`Invalidate`]. Idempotent, and a `tid`
    /// the store does not hold is `Ok(())`.
    fn invalidate(&mut self, tid: u64, level: Invalidate) -> Result<(), MirrorError>;

    /// Erase `tid`'s copy and open it for the view's whole value: until
    /// [`Self::seal`] it holds no cursor and answers no read.
    fn refill(&mut self, tid: u64) -> Result<(), MirrorError>;

    /// Add `blocks` of the value to the copy [`Self::refill`] opened.
    fn fill(&mut self, tid: u64, blocks: &[&[u8]]) -> Result<(), MirrorError>;

    /// The blocks filled are `tid`'s whole value at `cursor`.
    fn seal(&mut self, tid: u64, cursor: DeltaCursor) -> Result<(), MirrorError>;

    /// Apply `blocks`, the view's deltas after the copy's cursor, and advance it to
    /// `next`. Refused for a copy holding no cursor.
    fn advance(&mut self, tid: u64, blocks: &[&[u8]], next: DeltaCursor) -> Result<(), MirrorError>;

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

/// How far to tear a mirrored relation down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Invalidate {
    /// The feed position only. The copy stands but stops answering reads, and
    /// the next poll must reseed it.
    Cursor,
    /// The cursor, the rows, and the record that names the relation; its
    /// directory goes with them.
    Registration,
}

/// Why a mirror operation did not happen.
///
/// It lives here rather than beside [`ClientError`] because it is the
/// [`MirrorStore`] seam's error channel and nothing else raises it;
/// [`ClientError::Mirror`] is how it reaches a caller of the client.
#[derive(Debug, Clone)]
pub enum MirrorError {
    /// The local engine refused or failed: a storage fault, a registration the
    /// registry rejected, a read the spec could not express.
    Engine(String),
    /// The store is poisoned and refuses every further call that touches a copy.
    /// Raised for a teardown that could not erase the copy it was clearing, and
    /// for a panic caught mid-call. A failed apply erases that copy instead.
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
/// descriptor its reads and polls are answered under.
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
}

impl MirroredView {
    /// A delta read of this view after `after_tick`, replied in its own schema.
    fn poll_item(&self, after_tick: u64) -> DeltaPollItem {
        DeltaPollItem {
            view_id: self.desc.tid,
            after_tick,
            reply_layout: self.desc.schema.layout_digest(),
        }
    }
}

/// The store, shared with the job a client's host runs its disk work on. A
/// lock is contended only by the job of a call its caller abandoned, which the
/// next call waits out.
#[derive(Clone)]
pub(crate) struct Store(Arc<Mutex<dyn MirrorStore>>);

impl Store {
    /// The store, for a call that stays in memory. A panic under this lock is
    /// the store's own to record: an implementor poisons itself.
    pub(crate) fn get(&self) -> MutexGuard<'_, dyn MirrorStore + 'static> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Run `work`, which may touch the disk, where `host` lets work block.
    pub(crate) async fn run<R: Send + 'static>(
        self,
        host: &mut dyn Host,
        work: impl FnOnce(&mut dyn MirrorStore) -> R + Send + 'static,
    ) -> Result<R, ClientError> {
        offload(host, move || work(&mut *self.get())).await
    }
}

/// Everything a mirroring client holds beyond a plain one.
pub(crate) struct MirrorState {
    pub(crate) store: Store,
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
    /// See [`GnitzClient::cursor_of`].
    pub(crate) fn cursor_of(&self, tid: u64) -> Option<DeltaCursor> {
        self.views.get(&tid)?;
        self.store.get().cursor_of(tid)
    }
}

/// One view's train of a poll, ready to apply: its id, the cursor it was
/// polled from, its blocks, and the cursor they ended at.
type Train = (u64, DeltaCursor, Vec<RawBlock>, DeltaCursor);

/// Check `at`, the cursor `blocks` ended at, against `prev`, apply, advance
/// the cursor. Handed no client, it cannot recover — which could re-point a
/// registration at a view whose reply is in hand and apply it twice.
fn advance_from(store: &mut dyn MirrorStore, (tid, prev, blocks, at): Train) -> Result<PollResult, ClientError> {
    let next = prev.advanced_to(at)?;
    let blocks: Vec<&[u8]> = blocks.iter().map(RawBlock::block).collect();
    store.advance(tid, &blocks, next)?;
    Ok(PollResult::Advanced)
}

// ---------------------------------------------------------------------------
// The reconciliation state machine
// ---------------------------------------------------------------------------
//
// It lands on the client and not in a crate of its own because what it is made
// of is connection work: it resolves names upstream, drives the raw delta verb,
// and classifies what comes back. The only non-connection steps in it are
// calls through `MirrorStore` — which is also what makes it exist once for the
// blocking, async and Python clients alike.

/// Where [`GnitzClient::settle`] is with a view.
enum Settle {
    /// Apply `(prev, T]`, or bootstrap a copy with no feed position.
    Sync,
    /// Take the recovery this failed poll names, or return the failure.
    Recover(ClientError),
    /// Drop the cursor, re-resolve the view by name, and sync what the name
    /// now denotes.
    Reseed,
}

/// One poll's per-view outcomes, `(view id, that view's own result)`.
type ViewPollResults = Vec<(u64, Result<PollResult, ClientError>)>;

/// A mirror verb on a client that never attached a store. The message names no
/// method — each binding spells the attach differently.
fn no_mirror_store() -> ClientError {
    ClientError::from("this client mirrors nothing; attach a store before mirroring a view".to_string())
}

impl GnitzClient {
    fn mirror_state(&mut self) -> Result<&mut MirrorState, ClientError> {
        self.mirror.as_deref_mut().ok_or_else(no_mirror_store)
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
            .ok_or_else(|| ClientError::from(format!("relation {tid} is not mirrored")))
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
    async fn reconcile_registration(&mut self, schema_name: &str, name: &str) -> Result<u64, ClientError> {
        let schema_name = gnitz_wire::canonical_identifier(schema_name)?;
        let name = gnitz_wire::canonical_identifier(name)?;
        let rel = self.resolve_relation(&schema_name, &name).await?;
        match rel.class {
            RelClass::FedView => self.bind(&schema_name, &name, rel).await,
            RelClass::Table | RelClass::Stream => Err(ClientError::from(format!(
                "'{schema_name}.{name}' is a {}; only a view can be mirrored",
                rel.class.noun()
            ))),
            RelClass::BoundedView => Err(ClientError::from(format!(
                "view '{schema_name}.{name}' is capacity-bounded, and a capacity and a feed \
                 are refused together, so it carries no feed to subscribe to"
            ))),
            RelClass::View => Err(ClientError::from(format!(
                "view '{schema_name}.{name}' keeps no delta feed; \
                 create it WITH (delta = '<size>') to mirror it"
            ))),
        }
    }

    /// Record `desc.tid` as mirrored under `schema_name.name` — **already
    /// canonical** — with `desc` the descriptor local resolves are answered from.
    /// Returns that id.
    ///
    /// Class, capacity and feed are **not** re-checked, so a caller holding a
    /// descriptor for a relation whose identity did not change — a rename — binds
    /// with no second round trip.
    pub(crate) async fn bind(
        &mut self,
        schema_name: &str,
        name: &str,
        desc: Arc<RelDescriptor>,
    ) -> Result<u64, ClientError> {
        let tid = desc.tid;
        let store = self.mirror_state()?.store.clone();
        let (schema, rel, registered) = (schema_name.to_owned(), name.to_owned(), Arc::clone(&desc));
        let register = move |s: &mut dyn MirrorStore| s.register(&schema, &rel, &registered);
        let retracted = store.run(&mut *self.host, register).await??;
        let entry = MirroredView {
            schema_name: schema_name.to_string(),
            name: name.to_string(),
            desc,
        };
        let m = self.mirror_state()?;
        // The store's verdict, not a second scan: this map and the store's
        // records must name the same displaced id, or `held`
        // picks one of two live entries out of a `HashMap`.
        if let Some(old) = retracted {
            m.views.remove(&old);
        }
        m.views.insert(tid, entry);
        Ok(tid)
    }

    /// Bring a view's copy up to date, starting at `step` and taking each
    /// recovery a failure names, and return the id it is mirrored under now.
    ///
    /// **For [`Settle::Sync`], `tid` must have been resolved upstream just now**,
    /// or the bootstrap reads whatever else the id has come to name.
    async fn settle(&mut self, mut tid: u64, mut step: Settle) -> Result<(u64, PollResult), ClientError> {
        loop {
            step = match step {
                Settle::Sync => {
                    let Some(prev) = self.mirror_state()?.store.get().cursor_of(tid) else {
                        return self.bootstrap(tid).await;
                    };
                    let item = self.mirrored_view(tid)?.poll_item(prev.tick.get());
                    let (_, polled) = self
                        .delta_poll_many(&[(prev, item)])
                        .await?
                        .pop()
                        .expect("one view, one result");
                    match polled {
                        Ok(result) => return Ok((tid, result)),
                        Err(e) => Settle::Recover(e),
                    }
                }
                // Two classes of failure name a recovery; **every other refusal
                // is returned rather than probed**, since a dead socket answers
                // no differently the second time.
                Settle::Recover(err) => match err {
                    // The feed stopped continuing. Re-resolve, then re-read whole: a
                    // foreign tag is how a relation recreated under the same name reads.
                    ClientError::Refused(WireFault { status: WireStatus::DeltaExpired, .. }) => Settle::Reseed,
                    // The id is gone, and only the client's own name binding says whether
                    // the view moved or died. Died → `Failed` with the cursor untouched,
                    // so the copy keeps answering until the host forgets it.
                    e @ ClientError::Refused(WireFault { status: WireStatus::NotFound, .. }) => {
                        match self.relation_id_moved(tid).await {
                            Ok(true) => Settle::Reseed,
                            // An interrupt is the call's, not the view's.
                            Err(i @ ClientError::Interrupted(_)) => return Err(i),
                            // Not recreated, or the probe failed too: either way the poll's
                            // own error is the one that says what happened to this view.
                            Ok(false) | Err(_) => return Err(e),
                        }
                    }
                    e => return Err(e),
                },
                Settle::Reseed => {
                    // First, because the re-resolve can fail — an interrupt, a
                    // transport error, a removed feed — and a surviving cursor
                    // would leave the read gate open on a copy whose feed is known
                    // not to continue, with no reseed ever reported.
                    self.mirror_state()?.store.get().invalidate(tid, Invalidate::Cursor)?;
                    let (schema_name, name) = self.mirrored_qname(tid)?;
                    // Within one server the re-resolved id names the same relation
                    // or a newer one, never an older one's rows: relation ids are
                    // monotone and durably high-watermarked, so they are not recycled.
                    tid = self.reconcile_registration(&schema_name, &name).await?;
                    // Not a bootstrap: the name may now denote a view this client
                    // already mirrors, whose copy is live and correct.
                    Settle::Sync
                }
            }
        }
    }

    /// Whether `tid`'s name now resolves to a different id upstream.
    async fn relation_id_moved(&mut self, tid: u64) -> Result<bool, ClientError> {
        let (schema_name, name) = self.mirrored_qname(tid)?;
        Ok(self.resolve_relation(&schema_name, &name).await?.tid != tid)
    }

    /// Replace `tid`'s copy with the view's whole current value.
    ///
    /// Reads whatever `tid` now names, so [`Settle::Sync`]'s precondition is
    /// this one too.
    async fn bootstrap(&mut self, tid: u64) -> Result<(u64, PollResult), ClientError> {
        let item = self.mirrored_view(tid)?.poll_item(0);
        let store = self.mirror_state()?.store.clone();
        let GnitzClient { session, host, .. } = self;
        let host = &mut **host;
        store.clone().run(host, move |s| s.refill(tid)).await??;
        let mut poll = DeltaPoll::start(session, &[item]);
        let (mut blocks, mut end, mut refused) = (Vec::new(), None, None);
        while let Some((_, polled)) = poll.next(session, host).await? {
            match polled {
                Polled::Block(b) => blocks.push(b),
                Polled::End(e) => end = Some(e),
            }
            // One job per step's blocks, which the store takes before the
            // session reads on.
            if poll.drained() && !blocks.is_empty() {
                let blocks = std::mem::take(&mut blocks);
                if refused.is_none() {
                    let fill = move |s: &mut dyn MirrorStore| {
                        let blocks: Vec<&[u8]> = blocks.iter().map(RawBlock::block).collect();
                        s.fill(tid, &blocks)
                    };
                    refused = store.clone().run(host, fill).await?.err();
                }
            }
        }
        if let Some(e) = refused {
            return Err(e.into());
        }
        let cursor = end.expect("one item ends once")?;
        store.run(host, move |s| s.seal(tid, cursor)).await??;
        self.mirror_state()?.owed_reseed.insert(tid);
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
    /// `client.attach_mirror(gnitz_mirror::Mirror::open(dir, config)?)?`, and the
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
            return Err(ClientError::from(format!(
                "this client already mirrors at '{}'; close_mirror before attaching another store",
                m.store.get().base_dir()
            )));
        }
        self.mirror = Some(Box::new(MirrorState {
            store: Store(Arc::new(Mutex::new(store))),
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
    pub async fn mirror_view(&mut self, schema_name: &str, name: &str) -> Result<PollOutcome, ClientError> {
        self.refuse_poisoned_mirror()?;
        // The resolve inside it is what makes a bootstrap legal here.
        let tid = self.reconcile_registration(schema_name, name).await?;
        let (id, result) = self.settle(tid, Settle::Sync).await?;
        let out = self.outcome(id, result);
        self.reported(std::slice::from_ref(&out));
        Ok(out)
    }

    /// Stop mirroring `table_id`: its record is retracted, its directory
    /// removed, and a later read of it is delegated upstream.
    ///
    /// The host's word for [`Invalidate::Registration`].
    pub async fn forget_view(&mut self, table_id: u64) -> Result<(), ClientError> {
        // The client-side entry goes first: it is the read gate
        // `held` consults. The store is asked whether or not
        // there was one, because a reopened store holds copies this client has
        // not registered — and forgetting one is exactly the call that erases it.
        let m = self.mirror_state()?;
        m.views.remove(&table_id);
        let store = m.store.clone();
        store
            .run(&mut *self.host, move |s| {
                s.invalidate(table_id, Invalidate::Registration)
            })
            .await??;
        Ok(())
    }

    /// Advance every view in `views`, one request per `DELTA_POLL_MAX_VIEWS`,
    /// applying the views each step ended before the session reads on — so a
    /// poll over M views holds one read's trains, not M.
    ///
    /// A view that fails gets **that view's** own entry. Only an interrupt ends
    /// the call.
    async fn delta_poll_many(
        &mut self,
        views: &[(DeltaCursor, DeltaPollItem)],
    ) -> Result<ViewPollResults, ClientError> {
        let store = self.mirror_state()?.store.clone();
        let GnitzClient { session, host, .. } = self;
        let host = &mut **host;
        let items: Vec<DeltaPollItem> = views.iter().map(|&(_, item)| item).collect();
        let mut poll = DeltaPoll::start(session, &items);
        let mut applied = Vec::with_capacity(views.len());
        // The blocks of the view being answered, held until its end; and the
        // views one step ended, applied together before the session reads on.
        let mut blocks = Vec::new();
        let mut due: Vec<Train> = Vec::new();
        while let Some((i, polled)) = poll.next(session, host).await? {
            match polled {
                Polled::Block(b) => blocks.push(b),
                Polled::End(end) => {
                    let (prev, DeltaPollItem { view_id: tid, .. }) = views[i];
                    let blocks = std::mem::take(&mut blocks);
                    match end {
                        Ok(at) => due.push((tid, prev, blocks, at)),
                        Err(e) => applied.push((tid, Err(e))),
                    }
                }
            }
            if poll.drained() && !due.is_empty() {
                let trains = std::mem::take(&mut due);
                let apply = move |s: &mut dyn MirrorStore| {
                    let apply = |train: Train| (train.0, advance_from(s, train));
                    trains.into_iter().map(apply).collect::<Vec<_>>()
                };
                applied.extend(store.clone().run(host, apply).await?);
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
    /// recoveries sequentially. See [`advance_from`] for why
    /// recovery may not run inside phase one.
    ///
    /// `Err` is reserved for the failures that are the call's rather than a
    /// view's: no store attached, a poisoned store, and an interrupt, which ends the call because one Ctrl-C
    /// must. Everything else is that view's [`PollResult::Failed`] entry,
    /// carrying the id [`Self::forget_view`] takes — so a per-view failure is
    /// quiet unless the caller reads the vector, which a correct subscriber does
    /// anyway for [`PollResult::Reseeded`].
    ///
    /// A poll drives no tick server-side, so a drain is "read the view against
    /// the server, then poll once".
    pub async fn poll_mirror(&mut self) -> Result<Vec<PollOutcome>, ClientError> {
        self.refuse_poisoned_mirror()?;

        // ── Phase 1: advance, in one round trip ────────────────────────────
        let mut requests: Vec<(DeltaCursor, DeltaPollItem)> = Vec::new();
        let mut recoveries: Vec<(u64, Option<ClientError>)> = Vec::new();
        {
            let m = self.mirror_state()?;
            let store = m.store.get();
            for (&tid, v) in m.views.iter() {
                match store.cursor_of(tid) {
                    Some(prev) => requests.push((prev, v.poll_item(prev.tick.get()))),
                    None => recoveries.push((tid, None)),
                }
            }
        }

        let applied = self.delta_poll_many(&requests).await?;

        // ── Phase 2: recover, strictly after every phase-1 apply ───────────
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
            let step = err.map_or(Settle::Reseed, Settle::Recover);
            match self.settle(tid, step).await {
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
        self.mirror.as_deref()?.cursor_of(table_id)
    }

    /// Make every copy and its cursor durable.
    ///
    /// A failure is reported, not fatal: the flush writes shards and publishes
    /// manifests, neither of which mutates what a copy holds, so the store stays
    /// usable and a retry is sound.
    pub async fn checkpoint_mirror(&mut self) -> Result<(), ClientError> {
        let store = self.mirror_state()?.store.clone();
        Ok(store.run(&mut *self.host, |s| s.checkpoint()).await??)
    }

    /// Checkpoint and release the store, returning the checkpoint's own result so
    /// a failed final one is reported rather than swallowed by drop glue. The
    /// connection stays open and a later [`Self::attach_mirror`] is legal.
    ///
    /// It is the **only** recovery from a poisoned store, which cannot otherwise
    /// be cleared without discarding a working connection — so a poisoned store
    /// declining the checkpoint is not a failure to close. With no store
    /// attached there is nothing to close, which is not a failure either.
    pub async fn close_mirror(&mut self) -> Result<(), ClientError> {
        let Some(m) = self.mirror.take() else {
            return Ok(());
        };
        // Dropping the store, in the job that holds its last handle, releases
        // the directory lock.
        match m.store.run(&mut *self.host, |s| s.checkpoint()).await? {
            Err(MirrorError::Poisoned(_)) => Ok(()),
            other => other.map_err(ClientError::from),
        }
    }

    /// The message that poisoned this client's store, if any. Answers on a
    /// poisoned store — diagnosing one is what it is for.
    pub fn mirror_poisoned(&self) -> Option<String> {
        self.mirror
            .as_deref()
            .and_then(|m| m.store.get().poisoned().map(str::to_owned))
    }

    /// The call-level refusal [`Self::mirror_view`] and [`Self::poll_mirror`]
    /// take: no store, or a poisoned one. Every other verb that touches a copy is
    /// refused by the store itself; these two check first because they would
    /// otherwise spend a round trip before finding out.
    fn refuse_poisoned_mirror(&mut self) -> Result<(), ClientError> {
        match self.mirror_state()?.store.get().poisoned() {
            Some(why) => Err(MirrorError::Poisoned(why.to_string()).into()),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "tests/mirror.rs"]
mod tests;
