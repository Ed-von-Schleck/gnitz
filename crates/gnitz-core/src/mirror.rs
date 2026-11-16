//! Mirroring: the store a client reads a local copy of a view through, and the
//! reconciliation that keeps that copy current.
//!
//! [`MirrorStore`] is the copy: its lifecycle and one read. Everything that
//! resolves a name, drives the feed or classifies a failure is
//! [`GnitzClient`]'s — its mirror methods and the reconciliation below — and
//! so is delegating the reads the copy does not hold.
//!
//! **The trait is declared here so a client can hold a copy without linking an
//! engine.** Every signature is a `gnitz-core` type or a primitive, and
//! `gnitz-mirror` is the one implementor: a host that mirrors takes that crate
//! and its Linux-only engine, and a host that only reads remotely takes neither.
//!
//! # What a mirrored read promises
//!
//! **Freshness.** It answers at the copy's cursor round, which only a poll
//! moves, and a local read never polls. So a copy is not read-your-own-writes,
//! and two mirrored views can sit at different rounds — a read spanning both
//! is no consistent cut. A relation the copy does not hold is delegated
//! upstream and keeps every guarantee a server read has.
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

use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::client::GnitzClient;
use crate::connection::{DeltaCursor, Polled, RawBlock, RelDescriptor, ScanReply, Session, Target};
use crate::error::ClientError;
use crate::host::{BoxFut, Host, Pending, Sent, offload};
use crate::{RelName, Schema, ZSetBatch};
use gnitz_wire::txn_frame::{DeltaPollItem, SubId, kept_as};
use gnitz_wire::{RELTAB_PAY_NAME, RelClass, VIEW_TAB, WireStatus, payload_str};

// ---------------------------------------------------------------------------
// The store
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
///
/// Every `Err` is the local engine's refusal or failure: a storage fault, a
/// registration the registry rejected, a read the spec could not express.
pub trait MirrorStore: Send {
    /// Register `desc.tid` as `name`, with `desc`'s schema and indexes,
    /// retracting whatever the store held at that id as another relation, or
    /// under that name at another id. A registration that already stands as
    /// given is left as it is.
    fn register(&mut self, name: &RelName, desc: &RelDescriptor) -> Result<(), String>;

    /// Retract `tid`: its cursor, its rows, its record and its directory.
    /// Idempotent, and a `tid` the store does not hold is `Ok(())`. On `Err` the
    /// store no longer holds it, and a later open may find it at its last
    /// checkpoint.
    fn forget(&mut self, tid: u64) -> Result<(), String>;

    /// Register `desc.tid` as `name`, as [`Self::register`] does, then erase its
    /// copy and open it for the view's whole value. Until a [`Self::fill`] seals
    /// it, it holds no cursor, so [`Self::cursor_of`] gates every read of it out.
    fn refill(&mut self, name: &RelName, desc: &RelDescriptor) -> Result<(), String>;

    /// Add `blocks` of the value to the copy [`Self::refill`] opened. At
    /// `Some(cursor)` the blocks filled so far and these are `tid`'s whole value
    /// at that cursor, and the copy answers at it. A block that fails erases the
    /// copy and ends the refill: no further block or seal is taken until the
    /// next refill.
    fn fill(&mut self, tid: u64, blocks: &[&[u8]], sealed_at: Option<DeltaCursor>) -> Result<(), String>;

    /// Apply `blocks`, the view's deltas after the copy's cursor, and advance it to
    /// `next`. Refused for a copy holding no cursor. May touch the disk when
    /// `blocks` is non-empty; with no blocks it touches no disk, since the
    /// client calls it on its own thread.
    fn advance(&mut self, tid: u64, blocks: &[&[u8]], next: DeltaCursor) -> Result<(), String>;

    /// Run `spec` against `tid`'s copy, replying under `reply_schema`.
    fn scan_spec(&mut self, tid: u64, spec: gnitz_wire::ReadSpec, reply_schema: &Schema) -> Result<ZSetBatch, String>;

    /// The round `tid`'s copy answers at, and by its presence that the copy is
    /// valid at all — the store half of the readability gate.
    fn cursor_of(&self, tid: u64) -> Option<DeltaCursor>;

    /// Make every copy and its cursor durable.
    fn checkpoint(&mut self) -> Result<(), String>;
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
struct MirroredView {
    name: RelName,
    /// The descriptor handed back verbatim to every local resolve: the one
    /// registration resolved upstream, or an alias's own — a local id, and its
    /// subscription's reply schema and indexes. The schema is the client-side
    /// one, hidden columns included, which keeps `pk_stride` right for a view
    /// whose physical PK is a synthetic hidden column.
    desc: Arc<RelDescriptor>,
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
    /// How the next poll moves its copy.
    link: Link,
    /// Whether its copy was read whole and no caller has been handed the
    /// [`PollResult::Reseeded`] yet. Cleared only by [`MirrorState::report`],
    /// so an interrupt that discards a report re-announces it next poll; set
    /// again for a report handed back unclaimed.
    owed_reseed: bool,
}

/// How a poll moves a mirrored view's copy. A copy that keeps answering at its
/// old cursor after its feed failed is `Failed` and still confirmed, so this is
/// no measure of whether the copy answers reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Link {
    /// By a delta read, which the next sync is not held up for.
    Due,
    /// By a delta read, after the last one failed; a sync waits as asked.
    Failed,
    /// By the trains pushed for this connection's subscription, by the id it
    /// was asked for under; no delta read may. A sync names it to the server
    /// from here, so leaving this state is what ends the subscription.
    Subscribed(SubId),
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

/// The id an alias is mirrored under: outside the server's id space, and the
/// same at every reopen.
fn alias_id(name: &RelName) -> u64 {
    1 << 63 | gnitz_wire::checksum(name.name().as_bytes()) >> 1
}

impl MirroredView {
    /// Whether `other` reads the same rows of the same view.
    fn reads_what(&self, other: &MirroredView) -> bool {
        self.upstream.tid == other.upstream.tid && self.spec == other.spec
    }

    /// A delta read brought this copy up to date and left it subscribed as
    /// `id`: from here it answers reads.
    fn subscribed(&mut self, id: SubId) {
        self.link = Link::Subscribed(id);
        self.confirmed = true;
    }

    /// The round this view's copy in `store` answers reads at, if it answers any.
    fn answers_at(&self, store: &dyn MirrorStore) -> Option<DeltaCursor> {
        self.confirmed.then(|| store.cursor_of(self.desc.tid)).flatten()
    }
}

/// The store, shared with the job a client's host runs its disk work on. A
/// lock is contended only by the job of a call its caller abandoned, which the
/// next call waits out.
#[derive(Clone)]
struct Store(Arc<Mutex<dyn MirrorStore>>);

impl Store {
    /// The store, poisoned or not, for a lookup made in place.
    fn get(&self) -> MutexGuard<'_, dyn MirrorStore + 'static> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Refused once a call panicked under the lock — the store's poison, and so
    /// the one source of [`ClientError::MirrorPoisoned`]: a torn store answers
    /// no read and publishes nothing.
    fn refusal(&self) -> Result<(), ClientError> {
        match self.0.is_poisoned() {
            true => Err(ClientError::MirrorPoisoned),
            false => Ok(()),
        }
    }

    /// The store, for a call that touches a copy: a cursor move or a local
    /// read in place, so a mirrored read costs no dispatch, and work that
    /// writes the disk through [`Self::run`]. Refused as [`Self::refusal`] is.
    fn touch(&self) -> Result<MutexGuard<'_, dyn MirrorStore + 'static>, ClientError> {
        self.0.lock().map_err(|_| ClientError::MirrorPoisoned)
    }

    /// `work` on the store, in place, refused as [`Self::touch`] is; the
    /// store's own failure is [`ClientError::Mirror`].
    fn with<R>(&self, work: impl FnOnce(&mut dyn MirrorStore) -> Result<R, String>) -> Result<R, ClientError> {
        work(&mut *self.touch()?).map_err(ClientError::Mirror)
    }

    /// [`Self::with`], run where `host` lets work block, since `work` may touch
    /// the disk. The outer `Err` is the job's, the inner the store's.
    async fn run<R: Send + 'static>(
        self,
        host: &mut dyn Host,
        work: impl FnOnce(&mut dyn MirrorStore) -> Result<R, String> + Send + 'static,
    ) -> Result<Result<R, ClientError>, ClientError> {
        offload(host, move || self.with(work)).await
    }

    /// Advance each copy of `trains` — its id, its blocks and the cursor past
    /// them — and answer each, in train order, with how its ingest went.
    async fn advance(
        self,
        host: &mut dyn Host,
        trains: Vec<(u64, Vec<RawBlock>, DeltaCursor)>,
    ) -> Result<Vec<Result<(), ClientError>>, ClientError> {
        // A cursor move touches no disk, so it runs on the client's thread.
        let quiet = trains.iter().all(|(_, blocks, _)| blocks.is_empty());
        let apply = move || {
            let advance = |(tid, blocks, next): (u64, Vec<RawBlock>, DeltaCursor)| {
                let blocks: Vec<&[u8]> = blocks.iter().map(RawBlock::block).collect();
                self.with(|s| s.advance(tid, &blocks, next))
            };
            trains.into_iter().map(advance).collect()
        };
        if quiet {
            return Ok(apply());
        }
        offload(host, apply).await
    }
}

/// Everything a mirroring client holds beyond a plain one.
pub(crate) struct MirrorState {
    store: Store,
    /// The client's registrations — the poll's work list, and what
    /// [`GnitzClient::cursor_of`] reports a round for. Survives a
    /// [`GnitzClient::reconnect`].
    views: HashMap<u64, MirroredView>,
}

impl MirrorState {
    /// The connection was replaced: no copy answers a read until it is synced
    /// on the new one.
    pub(crate) fn connection_replaced(&mut self) {
        for v in self.views.values_mut() {
            v.confirmed = false;
            v.link = Link::Due;
        }
    }

    /// One failed entry for each view, its copy answering at no round: the
    /// store is poisoned.
    fn poisoned(&self) -> Vec<PollOutcome> {
        let failed = |&view_id: &u64| PollOutcome {
            view_id,
            cursor: None,
            result: PollResult::Failed(ClientError::MirrorPoisoned),
        };
        self.views.keys().map(failed).collect()
    }

    /// Owe each view `outcomes` reports reseeded, and still registered, its
    /// [`PollResult::Reseeded`] again.
    pub(crate) fn owe_reseeds(&mut self, outcomes: &[PollOutcome]) {
        for o in outcomes.iter().filter(|o| o.result.reseeded()) {
            if let Some(view) = self.views.get_mut(&o.view_id) {
                view.owed_reseed = true;
            }
        }
    }

    /// One entry for each view of `done` still registered, for a caller that is
    /// about to be handed it.
    fn report(&mut self, done: Vec<(u64, Result<(), ClientError>)>) -> Vec<PollOutcome> {
        let MirrorState { store, views } = self;
        let store = store.get();
        done.into_iter()
            .filter_map(|(view_id, synced)| {
                let view = views.get_mut(&view_id)?;
                Some(PollOutcome {
                    view_id,
                    cursor: view.answers_at(&*store),
                    result: match synced {
                        Err(e) => PollResult::Failed(e),
                        Ok(()) if std::mem::take(&mut view.owed_reseed) => PollResult::Reseeded,
                        Ok(()) => PollResult::Advanced,
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
        RelClass::Table | RelClass::Stream | RelClass::Catalog => {
            format!("{what} is a {}; only a view can be mirrored", class.noun())
        }
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
    ClientError::from("this client mirrors nothing; attach a store before mirroring a view")
}

impl GnitzClient {
    fn mirror_state(&mut self) -> Result<&mut MirrorState, ClientError> {
        self.mirror.as_deref_mut().ok_or_else(no_mirror_store)
    }

    /// What `name` reads upstream now — a view resolved, an alias planned —
    /// as a registration. Refused where no copy can be fed from what it reads.
    async fn derive(&mut self, name: RelName, plan: Option<Planner>) -> Result<MirroredView, ClientError> {
        let (upstream, desc, spec) = match &plan {
            None => {
                let upstream = self.resolve_relation(&name).await?;
                refuse_unfed(&format!("'{name}'"), upstream.class)?;
                let spec = gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::None).encode();
                (Arc::clone(&upstream), upstream, spec)
            }
            Some(plan) => {
                let Subscription { upstream, spec, schema, indexes } = plan(self).await?;
                refuse_unfed(&format!("what '{name}' reads"), upstream.class)?;
                let desc = Arc::new(RelDescriptor {
                    tid: alias_id(&name),
                    class: RelClass::FedView,
                    pk_repeats: upstream.pk_repeats,
                    serial: false,
                    schema,
                    indexes,
                    token: 0,
                });
                (upstream, desc, spec)
            }
        };
        Ok(MirroredView {
            name,
            desc,
            upstream: Target::from(&*upstream),
            spec,
            plan,
            confirmed: false,
            link: Link::Due,
            owed_reseed: false,
        })
    }

    /// Record `entry` as mirrored, under its descriptor's id, which is returned.
    async fn bind(&mut self, mut entry: MirroredView) -> Result<u64, ClientError> {
        let tid = entry.desc.tid;
        let store = self.mirror_state()?.store.clone();
        let (rel, registered) = (entry.name.clone(), Arc::clone(&entry.desc));
        store
            .run(&mut *self.host, move |s| s.register(&rel, &registered))
            .await??;
        let views = &mut self.mirror_state()?.views;
        let held = views.get(&tid);
        entry.confirmed = held.is_some_and(|held| held.confirmed && held.reads_what(&entry));
        entry.owed_reseed |= held.is_some_and(|held| held.owed_reseed);
        // One registration per name, and one at an id; a subscription outlives
        // only a rename, which carries it.
        views.retain(|_, v| v.name != entry.name);
        views.insert(tid, entry);
        Ok(tid)
    }

    /// Derive `tid`'s registration again and bind it; the id it is mirrored
    /// under now.
    async fn rebind(&mut self, tid: u64) -> Result<u64, ClientError> {
        let v = self.mirror_state()?.views.get(&tid);
        let v = v.ok_or_else(|| ClientError::from(format!("relation {tid} is not mirrored")))?;
        let (name, plan) = (v.name.clone(), v.plan.clone());
        let entry = self.derive(name, plan).await?;
        self.bind(entry).await
    }

    /// Mirror `name` and bring its copy up to date.
    async fn mirror_as(&mut self, name: RelName, plan: Option<Planner>) -> Result<PollOutcome, ClientError> {
        // Refused before a round trip finds the store poisoned.
        self.mirror_state()?.store.refusal()?;
        let entry = self.derive(name, plan).await?;
        let tid = self.bind(entry).await?;
        let done = self.settle([tid]).await?;
        let out = self.mirror_state()?.report(done).pop();
        let out = out.expect("the view settled is registered");
        match out.result {
            PollResult::Failed(e) => Err(e),
            _ => Ok(out),
        }
    }

    /// Bring every view of `tids` up to date and subscribed, each by a delta
    /// read and then the recovery its refusal names. Returns how each view's
    /// sync went, under the id it ended up mirrored at.
    async fn settle(
        &mut self,
        tids: impl IntoIterator<Item = u64>,
    ) -> Result<Vec<(u64, Result<(), ClientError>)>, ClientError> {
        let mut pending: Vec<u64> = tids.into_iter().collect();
        let mut done = Vec::with_capacity(pending.len());
        // The views whose cursor the feed no longer continues.
        let mut expired = Vec::new();
        for attempt in 1..=SETTLE_ATTEMPTS {
            // A recovery may have moved a registration off its id.
            if attempt > 1 {
                let m = self.mirror_state()?;
                pending.retain(|tid| m.views.contains_key(tid));
            }
            if pending.is_empty() {
                break;
            }
            let synced = self.read_copies(&pending, &expired).await?;
            pending.clear();
            for (tid, result) in synced {
                // A view synced again answers for its last sync alone.
                if attempt > 1 {
                    done.retain(|(synced, _)| *synced != tid);
                }
                let recovered = match result {
                    Ok(()) => {
                        done.push((tid, Ok(())));
                        continue;
                    }
                    Err(e) if attempt == SETTLE_ATTEMPTS => Err(e),
                    Err(e) => self.recover(tid, e, &mut expired).await,
                };
                match recovered {
                    Ok(now) if pending.contains(&now) => {}
                    Ok(now) => pending.push(now),
                    Err(e @ ClientError::Interrupted(_)) => return Err(e),
                    Err(e) => {
                        if let Some(v) = self.mirror_state()?.views.get_mut(&tid) {
                            v.link = Link::Failed;
                        }
                        done.push((tid, Err(e)))
                    }
                }
            }
        }
        Ok(done)
    }

    /// Take the recovery `failure` names for `tid`'s sync, and return the id to
    /// sync next; a failure that names none is handed back. A view to read
    /// whole joins `expired`.
    async fn recover(&mut self, tid: u64, failure: ClientError, expired: &mut Vec<u64>) -> Result<u64, ClientError> {
        match failure.status() {
            // The registration no longer resolves as it was derived.
            Some(WireStatus::StaleCatalog) => self.rebind(tid).await,
            // The cursor does not continue.
            Some(WireStatus::DeltaExpired) => {
                expired.push(tid);
                Ok(tid)
            }
            _ => Err(failure),
        }
    }

    /// One delta read of each view of `tids`, which keeps each one it brings
    /// up to date subscribed: a copy is advanced from its cursor, and one that
    /// holds none, or is one of `whole`, replaced with the view's whole value.
    /// `Err` is the host's alone.
    async fn read_copies(
        &mut self,
        tids: &[u64],
        whole: &[u64],
    ) -> Result<Vec<(u64, Result<(), ClientError>)>, ClientError> {
        let GnitzClient { session, host, mirror, next_sub, .. } = self;
        let MirrorState { store, views } = mirror.as_deref_mut().ok_or_else(no_mirror_store)?;
        let host = &mut **host;
        let froms: Vec<Option<DeltaCursor>> = {
            let store = store.get();
            let from = |tid: &u64| store.cursor_of(*tid).filter(|_| !whole.contains(tid));
            tids.iter().map(from).collect()
        };
        let first = *next_sub;
        let mut poll = {
            let item = |(tid, &from): (&u64, &Option<DeltaCursor>)| {
                let view = &views[tid];
                debug_assert!(
                    !matches!(view.link, Link::Subscribed(_)),
                    "a subscribed copy is moved by its trains alone"
                );
                DeltaPollItem {
                    view: view.upstream,
                    from,
                    reply_layout: view.desc.schema.layout().layout_digest(),
                    spec: &view.spec,
                }
            };
            let items: Vec<DeltaPollItem> = tids.iter().zip(&froms).map(item).collect();
            *next_sub = kept_as(first, items.len());
            DeltaPoll::start(session, first, &items)
        };
        let mut done = Vec::with_capacity(tids.len());
        // The view being answered: its blocks not yet in the store and, for
        // one read whole, whether its copy was opened and what failed it.
        let mut blocks = Vec::new();
        let (mut opened, mut failed) = (false, None);
        // The views holding a cursor that one step ended — each its id, its
        // blocks and the cursor they ended at, and in `ended` its item —
        // applied together before the session reads on. Such a copy takes its
        // train whole, with the cursor past it, in one advance: rows and cursor
        // move together, so a read abandoned mid-train reads again from a
        // cursor none of its rows were applied past. A copy read whole is
        // unreadable until sealed and erased by its next refill, so it takes
        // each step's blocks as they come.
        let mut ended: Vec<usize> = Vec::new();
        let mut due: Vec<(u64, Vec<RawBlock>, DeltaCursor)> = Vec::new();
        while let Some((i, polled)) = poll.next(host).await? {
            let tid = tids[i];
            let end = match polled {
                // What follows a block the store refused goes nowhere.
                Polled::Block(_) if failed.is_some() => None,
                Polled::Block(b) => {
                    blocks.push(b);
                    None
                }
                Polled::End(end) => Some(end),
            };
            match (froms[i], end) {
                // A copy read whole takes each step's blocks before the
                // session reads on, and is erased by the first of them.
                (None, end) => {
                    let sealed_at = end.as_ref().and_then(|end| end.as_ref().ok().copied());
                    if failed.is_none()
                        && (sealed_at.is_some() || end.is_none() && poll.drained() && !blocks.is_empty())
                    {
                        // Owed from here: the job erases the copy whether or not this call
                        // lives to see it end.
                        let open = match std::mem::replace(&mut opened, true) {
                            true => None,
                            false => views.get_mut(&tid).map(|v| {
                                v.owed_reseed = true;
                                (v.name.clone(), Arc::clone(&v.desc))
                            }),
                        };
                        let taken = std::mem::take(&mut blocks);
                        let fill = move |s: &mut dyn MirrorStore| {
                            if let Some((name, desc)) = open {
                                s.refill(&name, &desc)?;
                            }
                            let taken: Vec<&[u8]> = taken.iter().map(RawBlock::block).collect();
                            s.fill(tid, &taken, sealed_at)
                        };
                        failed = store.clone().run(host, fill).await?.err();
                    }
                    if let Some(end) = end {
                        blocks.clear();
                        opened = false;
                        let read = match (failed.take(), end) {
                            (Some(e), _) | (None, Err(e)) => Err(e),
                            (None, Ok(_)) => Ok(()),
                        };
                        if read.is_ok()
                            && let Some(view) = views.get_mut(&tid)
                        {
                            view.subscribed(kept_as(first, i));
                        }
                        done.push((tid, read));
                    }
                }
                (Some(_), None) => {}
                (Some(_), Some(Ok(next))) => {
                    ended.push(i);
                    due.push((tid, std::mem::take(&mut blocks), next));
                }
                (Some(_), Some(Err(e))) => {
                    blocks.clear();
                    done.push((tid, Err(e)));
                }
            }
            if poll.drained() && !due.is_empty() {
                let advanced = store.clone().advance(host, std::mem::take(&mut due)).await?;
                for (i, advanced) in ended.drain(..).zip(advanced) {
                    let tid = tids[i];
                    if advanced.is_ok()
                        && let Some(view) = views.get_mut(&tid)
                    {
                        view.subscribed(kept_as(first, i));
                    }
                    done.push((tid, advanced));
                }
            }
        }
        Ok(done)
    }

    /// Apply to every subscribed view what was pushed for it, given the reply
    /// to the sync that brought it, and return each one's failure as
    /// [`Self::settle`] does. A view whose subscription ended is left without
    /// one and out of the result.
    async fn advance_pushed(
        &mut self,
        synced: &Result<(), ClientError>,
    ) -> Result<Vec<(u64, Result<(), ClientError>)>, ClientError> {
        let GnitzClient { session, host, mirror, .. } = self;
        let MirrorState { views, store, .. } = mirror.as_deref_mut().ok_or_else(no_mirror_store)?;
        let (mut done, mut due) = (Vec::new(), Vec::new());
        {
            let store = store.get();
            for (&tid, view) in views.iter_mut() {
                let Link::Subscribed(id) = view.link else { continue };
                let pushed = match (store.cursor_of(tid), synced) {
                    (Some(at), Ok(())) => session.take_pushed(id).ok().map(|taken| (at, taken)),
                    _ => None,
                };
                let Some((at, (blocks, next))) = pushed else {
                    view.link = Link::Due;
                    continue;
                };
                if next != at || !blocks.is_empty() {
                    due.push((tid, blocks, next));
                } else {
                    done.push((tid, Ok(())));
                }
            }
        }
        let taken: Vec<u64> = due.iter().map(|(tid, ..)| *tid).collect();
        let advanced = match store.clone().advance(&mut **host, due).await {
            Ok(advanced) => advanced,
            Err(lost) => {
                // The trains taken went with the job: each copy is read from
                // its own cursor again.
                for tid in &taken {
                    if let Some(view) = views.get_mut(tid) {
                        view.link = Link::Due;
                    }
                }
                return Err(lost);
            }
        };
        for (tid, advanced) in taken.into_iter().zip(advanced) {
            // A copy its store let go of is read whole by the next poll.
            if advanced.is_err()
                && let Some(view) = views.get_mut(&tid)
            {
                view.link = Link::Due;
            }
            done.push((tid, advanced));
        }
        Ok(done)
    }
}

// ---------------------------------------------------------------------------
// The host-facing surface
// ---------------------------------------------------------------------------

impl GnitzClient {
    /// Read a local copy of one or more views through the store `open` opens.
    ///
    /// It takes an opener rather than a directory, because opening one is the
    /// engine's job and this crate does not link it: a host writes
    /// `client.attach_mirror(move || gnitz_mirror::Mirror::open(&dir, config)).await?`,
    /// and the dependency is the host's. The directory has no default — a
    /// derived one would collide on the engine's `flock`.
    ///
    /// `open` runs where the host lets work block, and only while no store is
    /// attached: a second call is refused and opens nothing.
    ///
    /// **Dropping a mirroring client does not checkpoint**: [`Self::close_mirror`]
    /// is where a host pays the exit checkpoint, and reports it. A drop forfeits
    /// the rounds since the last checkpoint — at most one bootstrap per view,
    /// when the feed no longer covers them.
    pub async fn attach_mirror<S: MirrorStore + 'static>(
        &mut self,
        open: impl FnOnce() -> Result<S, String> + Send + 'static,
    ) -> Result<(), ClientError> {
        if self.mirror.is_some() {
            return Err(ClientError::from(
                "this client already mirrors; close_mirror before attaching another store",
            ));
        }
        let store = offload(&mut *self.host, open).await?.map_err(ClientError::Mirror)?;
        self.mirror = Some(Box::new(MirrorState {
            store: Store(Arc::new(Mutex::new(store))),
            views: HashMap::new(),
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

    /// Mirror what `plan` compiles as the relation `alias` of the client's own
    /// schema `_local`, and bring its copy up to date. Idempotent, as
    /// [`Self::mirror_view`] is: the same subscription resumes from its persisted
    /// cursor, another one under the same alias replaces it.
    ///
    /// A read of the alias is answered off the copy and never upstream; the
    /// view itself is still read upstream, whole. The outcome's id is the
    /// alias's own.
    pub async fn mirror_subscription(&mut self, alias: &str, plan: Planner) -> Result<PollOutcome, ClientError> {
        self.mirror_as(RelName::new(crate::rel_name::LOCAL_SCHEMA, alias)?, Some(plan))
            .await
    }

    /// After this client's own DDL `families` committed: drop the copy of every
    /// view it retracted, except a mirrored view it also wrote back — a rename —
    /// which is rebound under its new name.
    pub(crate) async fn ddl_committed(&mut self, families: &[(u64, ZSetBatch)]) {
        let Some(m) = self.mirror.as_deref() else {
            return;
        };
        let Some((_, b)) = families.iter().find(|(family, _)| *family == VIEW_TAB) else {
            return;
        };
        let vid = |i| gnitz_wire::sys_leading_id(b.pks.get_bytes(i));
        let mut dropped: Vec<u64> = Vec::new();
        let mut renamed = Vec::new();
        for v in (0..b.len()).filter(|&i| b.weights[i] < 0).map(vid) {
            let written_back = m
                .views
                .get(&v)
                .and_then(|view| Some((view, b.live_rows().find(|&j| vid(j) == v)?)));
            match written_back {
                Some((view, j)) => renamed.push(MirroredView {
                    name: payload_str(b, j, RELTAB_PAY_NAME)
                        .ok()
                        .and_then(|new| view.name.sibling(&new).ok())
                        .expect("a bundle this client built names its views"),
                    ..view.clone()
                }),
                None => dropped.push(v),
            }
        }
        for v in dropped {
            let _ = self.forget_view(v).await;
        }
        for entry in renamed {
            let tid = entry.desc.tid;
            if self.bind(entry).await.is_err() {
                let _ = self.forget_view(tid).await;
            }
        }
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

    /// The wait a sync may be held for — none while a copy is due a delta
    /// read or owed a reseed — and the copies' subscriptions it names. A
    /// poisoned store names none and leaves `wait` as asked: a sync advances
    /// none of its copies, so their subscriptions end, and a reader's are
    /// synced as on any client.
    pub(crate) fn mirror_hold(&self, wait: Duration) -> (Duration, impl Iterator<Item = SubId>) {
        let live = self.mirror.as_deref().filter(|m| m.store.refusal().is_ok());
        let views = live.into_iter().flat_map(|m| m.views.values());
        let due = views.clone().any(|v| v.link == Link::Due || v.owed_reseed);
        let held = views.filter_map(|v| match v.link {
            Link::Subscribed(id) => Some(id),
            Link::Due | Link::Failed => None,
        });
        (if due { Duration::ZERO } else { wait }, held)
    }

    /// Advance every mirrored view, a subscribed one by what the sync `synced`
    /// answers brought it and any other by a delta read. On a poisoned store
    /// each view is reported failed with [`ClientError::MirrorPoisoned`], and
    /// the store is not touched. `Err` is an interrupt.
    pub(crate) async fn advance_mirror(
        &mut self,
        synced: &Result<(), ClientError>,
    ) -> Result<Vec<PollOutcome>, ClientError> {
        let Some(m) = self.mirror.as_deref() else {
            return Ok(Vec::new());
        };
        if m.store.refusal().is_err() {
            return Ok(m.poisoned());
        }
        let mut done = self.advance_pushed(synced).await?;
        let views = &self.mirror_state()?.views;
        let unsubscribed = views.iter().filter(|(_, v)| !matches!(v.link, Link::Subscribed(_)));
        let unsubscribed = unsubscribed.map(|(tid, _)| *tid);
        let polled: Vec<u64> = unsubscribed
            .filter(|tid| done.iter().all(|(done, _)| done != tid))
            .collect();
        done.extend(self.settle(polled).await?);
        Ok(self.mirror_state()?.report(done))
    }

    /// The views this client holds a registration for — the set
    /// [`Self::sync`] advances, which is wider than the views
    /// [`Self::cursor_of`] answers by the ones whose copy the next poll has yet
    /// to make valid.
    ///
    /// Answered off the client's own map, and provisional between a
    /// [`Self::reconnect`] and the next poll: the keys are still the previous
    /// server's ids, which is exactly what the poll will attempt.
    pub fn mirrored_ids(&self) -> Vec<u64> {
        self.mirror
            .as_deref()
            .map_or_else(Vec::new, |m| m.views.keys().copied().collect())
    }

    /// The round a local read of `table_id` answers at, or `None` when there is
    /// no valid copy to read one off: a copy an earlier session or connection
    /// left is none until a sync on this connection succeeds. A discarded copy
    /// is told by [`PollResult::Reseeded`], never by its cursor.
    pub fn cursor_of(&self, table_id: u64) -> Option<DeltaCursor> {
        let m = self.mirror.as_deref()?;
        m.views.get(&table_id)?.answers_at(&*m.store.get())
    }

    /// The descriptor of the mirrored view `name`, while its copy answers
    /// reads. As stale as the copy.
    pub fn mirrored_desc(&self, name: &RelName) -> Option<Arc<RelDescriptor>> {
        let m = self.mirror.as_deref()?;
        let view = m.views.values().find(|v| v.name == *name)?;
        view.answers_at(&*m.store.get())?;
        Some(Arc::clone(&view.desc))
    }

    /// [`Self::scan_spec`], answered off the copy when it holds `target`, with
    /// no served LSN: a copy's freshness is [`Self::cursor_of`]. The gate and
    /// the scan run under one guard, and a poisoned store refuses a read its
    /// copy would answer.
    pub fn scan_spec_local_first(
        &mut self,
        target: impl Into<Target>,
        spec: gnitz_wire::ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Pending<'_, ScanReply> {
        let target = target.into();
        if let Some(m) = self.mirror.as_deref() {
            let mut store = m.store.get();
            if m.views.get(&target.tid).and_then(|v| v.answers_at(&*store)).is_some() {
                let scanned = m.store.refusal().and_then(|()| {
                    store
                        .scan_spec(target.tid, spec, reply_schema)
                        .map_err(ClientError::Mirror)
                });
                drop(store);
                let reply = scanned.map(|batch| ScanReply {
                    batch,
                    schema: Arc::clone(reply_schema),
                    lsn: None,
                });
                return self.wait(Sent::ready(reply));
            }
        }
        self.scan_spec(target, &spec, reply_schema)
    }

    /// Make every copy and its cursor durable.
    ///
    /// A failure is reported, not fatal: the flush writes shards and publishes
    /// manifests, neither of which mutates what a copy holds, so the store stays
    /// usable and a retry is sound.
    pub async fn checkpoint_mirror(&mut self) -> Result<(), ClientError> {
        let store = self.mirror_state()?.store.clone();
        store.run(&mut *self.host, |s| s.checkpoint()).await?
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
            Err(ClientError::MirrorPoisoned) => Ok(()),
            other => other,
        }
    }

    /// Whether this client's store is poisoned. Answers on a poisoned store —
    /// diagnosing one is what it is for.
    pub fn mirror_poisoned(&self) -> bool {
        self.mirror.as_deref().is_some_and(|m| m.store.refusal().is_err())
    }
}

/// A delta poll in flight: `items` in one request, handed out as each item's
/// blocks and then its one end, the items in order. Dropping it drops what is
/// left of its trains. A subscription it kept, item `i` as `kept_as(first, i)`,
/// is its caller's only once recorded where the next sync names it from.
struct DeltaPoll<'s> {
    session: &'s mut Session,
    /// Ends handed out so far: the index of the item being answered.
    answered: usize,
    items: usize,
}

impl Drop for DeltaPoll<'_> {
    fn drop(&mut self) {
        self.session.abandon_poll();
    }
}

impl<'s> DeltaPoll<'s> {
    /// Poll `items`, item `i` kept as the subscription `kept_as(first, i)`.
    fn start(session: &'s mut Session, first: SubId, items: &[DeltaPollItem]) -> Self {
        session.submit_delta_poll(first, items);
        DeltaPoll { session, answered: 0, items: items.len() }
    }

    /// Everything the last step queued has been handed out.
    fn drained(&self) -> bool {
        self.session.polled_out()
    }

    /// The next block or end, with its item's index; `None` once every item
    /// has ended. `Err` is the host's alone.
    async fn next(&mut self, host: &mut dyn Host) -> Result<Option<(usize, Polled)>, ClientError> {
        loop {
            if let Some(next) = self.session.next_polled() {
                let item = self.answered;
                self.answered += matches!(next, Polled::End(_)) as usize;
                return Ok(Some((item, next)));
            }
            if self.answered == self.items {
                return Ok(None);
            }
            poll_fn(|cx| self.session.turn(host, cx)).await?;
        }
    }
}

#[cfg(test)]
#[path = "tests/mirror.rs"]
mod tests;
