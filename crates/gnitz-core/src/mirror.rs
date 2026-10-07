//! Mirroring: the store a client reads a local copy of a view through, and the
//! reconciliation that keeps that copy current.
//!
//! The seam sits at the **store**, not at the read surface. [`GnitzClient`] owns
//! the copy and delegates the reads it does not hold; the copy answers questions
//! about itself and never about a connection. So the trait below carries the
//! store's own lifecycle and one read, and everything that resolves a name,
//! drives the feed or classifies a failure is the client's — [`GnitzClient`]'s
//! mirror methods and the reconciliation at the bottom of this file.
//!
//! **It is declared here so a client can hold a copy without linking an
//! engine.** Every signature is a `gnitz-core` type or a primitive, and
//! `gnitz-mirror` is the one implementor: a host that mirrors takes that crate
//! and its Linux-only engine, and a host that only reads remotely takes neither.
//!
//! # What a mirrored read promises
//!
//! **Freshness.** It answers at the copy's cursor round, and a local read never
//! polls. So a copy is not read-your-own-writes, and two mirrored views can sit
//! at different rounds — a read spanning both is no consistent cut. A relation
//! the copy does not hold is delegated upstream and keeps every guarantee a
//! server read has.
//!
//! **Cost.** Each index the view had at its last resolve is a further local
//! store, maintained on every applied delta.
//!
//! # Aliases
//!
//! A copy may hold a [`Subscription`] instead of a whole view: what a planner's
//! spec keeps of it, under a name of the client's own schema. This file moves
//! such a copy exactly as it moves any other and compiles nothing: where a
//! view's registration is derived by resolving its name, an alias's is by
//! calling the [`Planner`] it was mirrored with.

use gnitz_expr::SchemaFacts;
use gnitz_wire::{WireFault, WireStatus};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::client::{offload, BoxFut, DeltaPoll, GnitzClient, Host};
use crate::connection::{DeltaCursor, Polled, RawBlock, RelDescriptor, Target};
use crate::error::ClientError;
use crate::{RelName, Schema, ZSetBatch};
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

    /// Register `desc.tid` as `name`, with `desc`'s schema and indexes,
    /// retracting whatever the store held at that id as another relation, or
    /// under that name at another id. A registration that already stands as
    /// given is left as it is.
    fn register(&mut self, name: &RelName, desc: &RelDescriptor) -> Result<(), MirrorError>;

    /// Drop `tid`'s feed position, leaving its rows.
    fn drop_cursor(&mut self, tid: u64);

    /// Retract `tid`: its cursor, its rows, its record and its directory.
    /// Idempotent, and a `tid` the store does not hold is `Ok(())`.
    fn forget(&mut self, tid: u64) -> Result<(), MirrorError>;

    /// Erase `tid`'s copy and open it for the view's whole value: until
    /// [`Self::seal`] it holds no cursor and answers no read.
    fn refill(&mut self, tid: u64) -> Result<(), MirrorError>;

    /// Add `blocks` of the value to the copy [`Self::refill`] opened.
    fn fill(&mut self, tid: u64, blocks: &[&[u8]]) -> Result<(), MirrorError>;

    /// The blocks filled are `tid`'s whole value at `cursor`.
    fn seal(&mut self, tid: u64, cursor: DeltaCursor) -> Result<(), MirrorError>;

    /// Apply `blocks`, the view's deltas after the copy's cursor, and advance it to
    /// `next`. Refused for a copy holding no cursor. With no block it moves the
    /// cursor alone and touches no disk: the client makes that call in place.
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

    /// Make every copy and its cursor durable.
    ///
    /// **A torn store publishes nothing**: an implementor that poisoned itself
    /// answers [`MirrorError::Poisoned`] instead. The client asks
    /// unconditionally, so that refusal is the store's alone to make.
    fn checkpoint(&mut self) -> Result<(), MirrorError>;

    /// The message that poisoned this store, if any.
    fn poisoned(&self) -> Option<&str>;
}

/// Why a mirror operation did not happen.
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
    /// This view's poll failed and the others went on.
    /// [`GnitzClient::forget_view`] at [`PollOutcome::view_id`] ends a failure
    /// that repeats.
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

/// One mirrored view, as the client tracks it: what to derive its registration
/// from again, and the descriptor its reads and polls are answered under.
#[derive(Clone)]
pub(crate) struct MirroredView {
    pub(crate) name: RelName,
    /// The descriptor handed back verbatim to every local resolve: the one
    /// registration resolved upstream, or an alias's own — a local id, and its
    /// subscription's reply schema and indexes. The schema is the client-side
    /// one, hidden columns included, which keeps `pk_stride` right for a view
    /// whose physical PK is a synthetic hidden column.
    pub(crate) desc: Arc<RelDescriptor>,
    /// The view a delta read names, under the token it was resolved at.
    upstream: Target,
    /// The encoded `ReadSpec` every delta read carries: the view whole, or an
    /// alias's subscription.
    spec: Vec<u8>,
    /// What compiles an alias's subscription again; `None` for a view, which
    /// is resolved by `name`.
    plan: Option<Planner>,
    /// Whether a sync on this connection succeeded for what this registration
    /// reads.
    confirmed: bool,
}

/// A view read under a spec, as a planner compiled it — what
/// [`GnitzClient::mirror_subscription`] mirrors under an alias.
#[derive(Clone, Debug)]
pub struct Subscription {
    /// The view read, as the server described it.
    pub upstream: Arc<RelDescriptor>,
    /// The encoded `ReadSpec` every delta read of the view carries.
    pub spec: Vec<u8>,
    /// The spec's reply schema, which is the alias's.
    pub schema: Arc<Schema>,
    /// The view's indexes whose columns the spec keeps, over `schema`'s
    /// numbering. Each is a local store over the alias's own rows.
    pub indexes: Vec<gnitz_wire::RelIndex>,
}

/// Compiles an alias's [`Subscription`] against what the connection holds now.
/// A poll may call it again.
pub type Planner =
    Arc<dyn for<'a> Fn(&'a mut GnitzClient) -> BoxFut<'a, Result<Subscription, ClientError>> + Send + Sync>;

impl Subscription {
    /// `upstream` read whole, in its own schema and under its own indexes.
    fn whole(upstream: Arc<RelDescriptor>) -> Self {
        Subscription {
            spec: gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::None).encode(),
            schema: Arc::clone(&upstream.schema),
            indexes: upstream.indexes.clone(),
            upstream,
        }
    }
}

/// The id an alias is mirrored under: outside the server's id space, and the
/// same at every reopen.
fn alias_id(name: &RelName) -> u64 {
    1 << 63 | gnitz_wire::checksum(name.name().as_bytes()) >> 1
}

impl MirroredView {
    /// `sub` mirrored as `name` — a view with no `plan`, an alias with one.
    /// Refused where no copy can be fed from what `sub` reads.
    fn new(name: RelName, sub: Subscription, plan: Option<Planner>) -> Result<Self, ClientError> {
        let Subscription { upstream, spec, schema, indexes } = sub;
        let desc = match &plan {
            None => {
                refuse_unfed(&format!("'{name}'"), upstream.class)?;
                Arc::clone(&upstream)
            }
            Some(_) => {
                refuse_unfed(&format!("what '{name}' reads"), upstream.class)?;
                Arc::new(RelDescriptor {
                    tid: alias_id(&name),
                    class: RelClass::FedView,
                    pk_repeats: upstream.pk_repeats,
                    serial: false,
                    schema,
                    indexes,
                    token: 0,
                })
            }
        };
        Ok(MirroredView {
            name,
            desc,
            upstream: Target::from(&*upstream),
            spec,
            plan,
            confirmed: false,
        })
    }

    /// This registration under another name.
    pub(crate) fn renamed(&self, name: RelName) -> Self {
        MirroredView { name, ..self.clone() }
    }

    /// Whether `other` reads the same rows of the same view.
    fn reads_what(&self, other: &MirroredView) -> bool {
        self.upstream.tid == other.upstream.tid && self.spec == other.spec
    }

    /// The round this view's copy in `store` answers reads at, if it answers any.
    fn answers_at(&self, store: &dyn MirrorStore) -> Option<DeltaCursor> {
        self.confirmed.then(|| store.cursor_of(self.desc.tid)).flatten()
    }

    /// A delta read of this view after `from` — the view whole with none —
    /// replied in its own schema.
    fn poll_item(&self, from: Option<DeltaCursor>) -> DeltaPollItem<'_> {
        let (tag, after_tick) = DeltaCursor::flat(from);
        DeltaPollItem {
            view: self.upstream,
            tag,
            after_tick,
            reply_layout: self.desc.schema.layout_digest(),
            spec: &self.spec,
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
    /// [`GnitzClient::reconnect`].
    pub(crate) views: HashMap<u64, MirroredView>,
    /// Views owed a [`PollResult::Reseeded`]: a bootstrap ran and no caller has
    /// been handed the outcome yet. Cleared only by [`Self::report`], so an
    /// interrupt that discards a report re-announces it next poll.
    owed_reseed: HashSet<u64>,
}

impl MirrorState {
    fn view(&self, tid: u64) -> Result<&MirroredView, ClientError> {
        self.views
            .get(&tid)
            .ok_or_else(|| ClientError::from(format!("relation {tid} is not mirrored")))
    }

    /// See [`GnitzClient::cursor_of`].
    pub(crate) fn cursor_of(&self, tid: u64) -> Option<DeltaCursor> {
        self.views.get(&tid)?.answers_at(&*self.store.get())
    }

    /// The registration of `name`, while its copy answers reads.
    pub(crate) fn answering(&self, name: &RelName) -> Option<&MirroredView> {
        let store = self.store.get();
        self.views
            .values()
            .find(|v| v.name == *name && v.answers_at(&*store).is_some())
    }

    /// The connection was replaced: no copy answers a read until it is synced
    /// on the new one.
    pub(crate) fn connection_replaced(&mut self) {
        self.views.values_mut().for_each(|v| v.confirmed = false);
    }

    /// One entry for each view of `done` still registered, for a caller that is
    /// about to be handed it.
    fn report(&mut self, done: Vec<(u64, Option<ClientError>)>) -> Vec<PollOutcome> {
        let MirrorState { store, views, owed_reseed } = self;
        let store = store.get();
        done.into_iter()
            .filter_map(|(view_id, failed)| {
                let view = views.get(&view_id)?;
                Some(PollOutcome {
                    view_id,
                    cursor: view.answers_at(&*store),
                    result: match failed {
                        Some(e) => PollResult::Failed(e),
                        None if owed_reseed.remove(&view_id) => PollResult::Reseeded,
                        None => PollResult::Advanced,
                    },
                })
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// The reconciliation
// ---------------------------------------------------------------------------

/// Rounds one view is synced in before its refusal is the caller's to see.
const SETTLE_ATTEMPTS: usize = 4;

/// Refuse `what`, a relation of `class`, as one no copy can be fed from.
fn refuse_unfed(what: &str, class: RelClass) -> Result<(), ClientError> {
    Err(ClientError::from(match class {
        RelClass::FedView => return Ok(()),
        RelClass::Table | RelClass::Stream => format!("{what} is a {}; only a view can be mirrored", class.noun()),
        RelClass::BoundedView => format!(
            "{what} is capacity-bounded, and a capacity and a feed \
             are refused together, so it carries no feed to subscribe to"
        ),
        RelClass::View => format!(
            "{what} keeps no delta feed; \
             create it WITH (delta = '<size>') to mirror it"
        ),
    }))
}

/// A mirror verb on a client that never attached a store. The message names no
/// method — each binding spells the attach differently.
fn no_mirror_store() -> ClientError {
    ClientError::from("this client mirrors nothing; attach a store before mirroring a view".to_string())
}

impl GnitzClient {
    fn mirror_state(&mut self) -> Result<&mut MirrorState, ClientError> {
        self.mirror.as_deref_mut().ok_or_else(no_mirror_store)
    }

    fn mirrored_view(&mut self, tid: u64) -> Result<&MirroredView, ClientError> {
        self.mirror_state()?.view(tid)
    }

    /// What `name` reads upstream now — a view resolved, an alias planned —
    /// as a registration.
    async fn derive(&mut self, name: RelName, plan: Option<Planner>) -> Result<MirroredView, ClientError> {
        let sub = match &plan {
            None => Subscription::whole(self.resolve_relation(&name).await?),
            Some(plan) => plan(self).await?,
        };
        MirroredView::new(name, sub, plan)
    }

    /// Record `entry` as mirrored, under its descriptor's id, which is returned.
    pub(crate) async fn bind(&mut self, mut entry: MirroredView) -> Result<u64, ClientError> {
        let tid = entry.desc.tid;
        let store = self.mirror_state()?.store.clone();
        let (rel, registered) = (entry.name.clone(), Arc::clone(&entry.desc));
        store
            .run(&mut *self.host, move |s| s.register(&rel, &registered))
            .await??;
        let m = self.mirror_state()?;
        let held = m.views.get(&tid);
        entry.confirmed = held.is_some_and(|held| held.confirmed && held.reads_what(&entry));
        // One registration per name.
        m.views.retain(|_, v| v.name != entry.name);
        m.views.insert(tid, entry);
        Ok(tid)
    }

    /// Derive `tid`'s registration again and bind it; the id it is mirrored
    /// under now.
    async fn rebind(&mut self, tid: u64) -> Result<u64, ClientError> {
        let v = self.mirrored_view(tid)?;
        let (name, plan) = (v.name.clone(), v.plan.clone());
        let entry = self.derive(name, plan).await?;
        self.bind(entry).await
    }

    /// Mirror `name` and bring its copy up to date.
    async fn mirror_as(&mut self, name: RelName, plan: Option<Planner>) -> Result<PollOutcome, ClientError> {
        self.refuse_poisoned_mirror()?;
        let entry = self.derive(name, plan).await?;
        let tid = self.bind(entry).await?;
        let done = self.settle([tid], Duration::ZERO).await?;
        let out = self.mirror_state()?.report(done).pop();
        let out = out.expect("the view settled is registered");
        match out.result {
            PollResult::Failed(e) => Err(e),
            _ => Ok(out),
        }
    }

    /// Bring every view of `tids` up to date, in rounds: each syncs every pending
    /// view, then takes the recovery each refusal names. Returns each view's
    /// failure, if it has one, under the id it ended up mirrored at.
    async fn settle(
        &mut self,
        tids: impl IntoIterator<Item = u64>,
        mut wait: Duration,
    ) -> Result<Vec<(u64, Option<ClientError>)>, ClientError> {
        let mut pending: Vec<u64> = tids.into_iter().collect();
        let mut done = Vec::with_capacity(pending.len());
        for attempt in 1..=SETTLE_ATTEMPTS {
            // A recovery may have moved a registration off its id.
            if attempt > 1 {
                let m = self.mirror_state()?;
                pending.retain(|tid| m.views.contains_key(tid));
            }
            if pending.is_empty() {
                break;
            }
            let synced = self.sync(&pending, wait).await?;
            wait = Duration::ZERO;
            pending.clear();
            for (tid, result) in synced {
                // A view synced again answers for its last sync alone.
                if attempt > 1 {
                    done.retain(|(synced, _)| *synced != tid);
                }
                let recovered = match result {
                    Ok(()) => {
                        if let Some(v) = self.mirror_state()?.views.get_mut(&tid) {
                            v.confirmed = true;
                        }
                        done.push((tid, None));
                        continue;
                    }
                    Err(e) if attempt == SETTLE_ATTEMPTS => Err(e),
                    Err(e) => self.recover(tid, e).await,
                };
                match recovered {
                    Ok(now) if pending.contains(&now) => {}
                    Ok(now) => pending.push(now),
                    Err(e @ ClientError::Interrupted(_)) => return Err(e),
                    Err(e) => done.push((tid, Some(e))),
                }
            }
        }
        Ok(done)
    }

    /// Take the recovery `failure` names for `tid`'s sync, and return the id to
    /// sync next; a failure that names none is handed back.
    async fn recover(&mut self, tid: u64, failure: ClientError) -> Result<u64, ClientError> {
        let ClientError::Refused(WireFault { status, .. }) = &failure else {
            return Err(failure);
        };
        match status {
            // The registration no longer resolves as it was derived.
            WireStatus::StaleCatalog => self.rebind(tid).await,
            // The cursor does not continue.
            WireStatus::DeltaExpired => {
                self.mirror_state()?.store.get().drop_cursor(tid);
                Ok(tid)
            }
            _ => Err(failure),
        }
    }

    /// One sync of each view of `tids`: every one holding a cursor in a single
    /// request, then each of the rest read whole. Only an interrupt ends the call.
    async fn sync(&mut self, tids: &[u64], wait: Duration) -> Result<Vec<(u64, Result<(), ClientError>)>, ClientError> {
        let (mut polls, mut whole) = (Vec::new(), Vec::new());
        {
            let store = self.mirror_state()?.store.get();
            for &tid in tids {
                match store.cursor_of(tid) {
                    Some(prev) => polls.push((tid, prev)),
                    None => whole.push(tid),
                }
            }
        }
        // A whole read is work in hand, which the poll must not wait in front of.
        let wait = if whole.is_empty() { wait } else { Duration::ZERO };
        let mut synced = self.delta_poll_many(&polls, wait).await?;
        for tid in whole {
            match self.bootstrap(tid).await {
                Err(e @ ClientError::Interrupted(_)) => return Err(e),
                read => synced.push((tid, read)),
            }
        }
        Ok(synced)
    }

    /// Replace `tid`'s copy with the view's whole current value.
    async fn bootstrap(&mut self, tid: u64) -> Result<(), ClientError> {
        let GnitzClient { session, host, mirror, .. } = self;
        let m = mirror.as_deref().ok_or_else(no_mirror_store)?;
        let view = m.view(tid)?;
        let (store, item) = (m.store.clone(), view.poll_item(None));
        let host = &mut **host;
        // Registered again, for a registration whose record the store lost.
        let (name, desc) = (view.name.clone(), Arc::clone(&view.desc));
        let open = move |s: &mut dyn MirrorStore| {
            s.register(&name, &desc)?;
            s.refill(tid)
        };
        store.clone().run(host, open).await??;
        let mut poll = DeltaPoll::start(session, &[item], Duration::ZERO);
        let (mut blocks, mut end) = (Vec::new(), None);
        while let Some((_, polled)) = poll.next(host).await? {
            match polled {
                Polled::Block(b) => blocks.push(b),
                Polled::End(e) => end = Some(e),
            }
            // One job per step's blocks, which the store takes before the
            // session reads on.
            if poll.drained() && !blocks.is_empty() {
                let blocks = std::mem::take(&mut blocks);
                let fill = move |s: &mut dyn MirrorStore| {
                    let blocks: Vec<&[u8]> = blocks.iter().map(RawBlock::block).collect();
                    s.fill(tid, &blocks)
                };
                store.clone().run(host, fill).await??;
            }
        }
        drop(poll);
        let cursor = end.expect("one item ends once")?;
        store.run(host, move |s| s.seal(tid, cursor)).await??;
        self.mirror_state()?.owed_reseed.insert(tid);
        Ok(())
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

    /// Mirror `name`, and bring its copy up to date.
    ///
    /// Idempotent, and the same call whether this is a first registration or a
    /// reopen: it resolves the relation upstream, reconciles that against
    /// whatever record the store replayed, and then either advances the copy
    /// from its persisted cursor or reseeds it. The outcome says which.
    ///
    /// Only a view with a delta feed can be mirrored — create it
    /// `WITH (delta = '<size>')`. A single-view call reports its own failure as
    /// `Err`, so the outcome never carries [`PollResult::Failed`].
    pub async fn mirror_view(&mut self, name: &RelName) -> Result<PollOutcome, ClientError> {
        self.mirror_as(name.clone(), None).await
    }

    /// Mirror what `plan` compiles as the relation `alias` of
    /// [`gnitz_wire::LOCAL_SCHEMA`], and bring its copy up to date. Idempotent,
    /// as [`Self::mirror_view`] is: the same subscription resumes from its
    /// persisted cursor, another one under the same alias replaces it.
    ///
    /// A read of the alias is answered off the copy and never upstream; the
    /// view itself is still read upstream, whole. The outcome's id is the
    /// alias's own.
    pub async fn mirror_subscription(&mut self, alias: &str, plan: Planner) -> Result<PollOutcome, ClientError> {
        self.mirror_as(RelName::new(gnitz_wire::LOCAL_SCHEMA, alias)?, Some(plan))
            .await
    }

    /// Stop mirroring `table_id`: its record is retracted, its directory
    /// removed, and a later read of it is delegated upstream.
    pub async fn forget_view(&mut self, table_id: u64) -> Result<(), ClientError> {
        // The store is asked whether or not this client registered `table_id`:
        // a reopened store holds copies of an earlier session's.
        let m = self.mirror_state()?;
        m.views.remove(&table_id);
        let store = m.store.clone();
        store.run(&mut *self.host, move |s| s.forget(table_id)).await??;
        Ok(())
    }

    /// Advance every view in `views` in one request, applying the views each
    /// step ended before the session reads on — so a poll over M views holds
    /// one read's trains, not M.
    ///
    /// A view that fails gets **that view's** own entry. Only an interrupt ends
    /// the call.
    async fn delta_poll_many(
        &mut self,
        views: &[(u64, DeltaCursor)],
        wait: Duration,
    ) -> Result<Vec<(u64, Result<(), ClientError>)>, ClientError> {
        let GnitzClient { session, host, mirror, .. } = self;
        let m = mirror.as_deref().ok_or_else(no_mirror_store)?;
        let store = m.store.clone();
        let host = &mut **host;
        let items: Result<Vec<DeltaPollItem>, ClientError> = views
            .iter()
            .map(|&(tid, prev)| Ok(m.view(tid)?.poll_item(Some(prev))))
            .collect();
        let mut poll = DeltaPoll::start(session, &items?, wait);
        let mut applied = Vec::with_capacity(views.len());
        // The blocks of the view being answered, held until its end; and the
        // views one step ended — each its id, its blocks and the cursor they
        // ended at — applied together before the session reads on.
        let mut blocks = Vec::new();
        let mut due: Vec<(u64, Vec<RawBlock>, DeltaCursor)> = Vec::new();
        while let Some((i, polled)) = poll.next(host).await? {
            match polled {
                Polled::Block(b) => blocks.push(b),
                Polled::End(end) => {
                    let tid = views[i].0;
                    let blocks = std::mem::take(&mut blocks);
                    match end {
                        Ok(next) => due.push((tid, blocks, next)),
                        Err(e) => applied.push((tid, Err(e))),
                    }
                }
            }
            if poll.drained() && !due.is_empty() {
                let trains = std::mem::take(&mut due);
                let idle = trains.iter().all(|(_, blocks, _)| blocks.is_empty());
                let apply = move |s: &mut dyn MirrorStore| {
                    let advance = |(tid, blocks, next): (u64, Vec<RawBlock>, DeltaCursor)| {
                        let blocks: Vec<&[u8]> = blocks.iter().map(RawBlock::block).collect();
                        (tid, s.advance(tid, &blocks, next).map_err(ClientError::from))
                    };
                    trains.into_iter().map(advance).collect::<Vec<_>>()
                };
                applied.extend(match idle {
                    // Only cursors move, so there is nothing to block on.
                    true => apply(&mut *store.get()),
                    false => store.clone().run(host, apply).await?,
                });
            }
        }
        Ok(applied)
    }

    /// Advance every mirrored view, and report one entry for each, at the id it
    /// is mirrored under after the call.
    ///
    /// `Err` is the call's own failure: no store attached, a poisoned store, or
    /// an interrupt. Every other failure is that view's [`PollResult::Failed`].
    ///
    /// `wait` is [`Self::delta_poll`]'s, over every mirrored view at once.
    pub async fn poll_mirror(&mut self, wait: Duration) -> Result<Vec<PollOutcome>, ClientError> {
        self.refuse_poisoned_mirror()?;
        let done = self.settle(self.mirrored_ids(), wait).await?;
        Ok(self.mirror_state()?.report(done))
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
    /// A copy an earlier session or connection left answers no read until a
    /// sync on this connection succeeds.
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

    /// Refuse a call with no store, or a poisoned one, before it spends a round
    /// trip finding out.
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
