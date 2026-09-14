//! The catalog-zone write path: one protocol, two entry points.
//! [`handle_ddl_txn`] ingests a client bundle of system-table families;
//! [`commit_serial_range_durable`] advances one sequence. Both reserve a zone
//! LSN, mutate the catalog under it, [`emit_zone_to_sal`] and
//! [`publish_after_fsync`].
//!
//! A DDL bundle additionally runs inside a `TickGate`. The serial path needs none
//! of that: a `sys_sequences` advance has no DAG evaluation and no rollback path,
//! and the row it broadcasts is one no worker reads.
//!
//! A child of `executor`, so it reads that module's private items — `Shared` and
//! its accessors included — with no visibility widened, and the DDL seams sit
//! beside the code they perturb.

use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::{
    await_barrier, decode_client_batch, guard_panic, park_until, request_quiesce, send_fault, send_msg, Shared,
    TickPark,
};
use crate::catalog::{family_pk_partition, idx_tab_partition, PkPartition, SysFamily};
use crate::runtime::committer::BarrierKind;
use crate::runtime::lsn::ZoneLsnAllocator;
use crate::runtime::master::UniqueFilter;
use crate::runtime::peer::Peer;
use crate::runtime::reactor::FsyncFuture;
use crate::runtime::wire as ipc;
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::Relation;
use gnitz_store::storage::Batch;
use gnitz_wire::{PkColList, WireFault, STATUS_OK};

/// `GNITZ_INJECT_RELAY_HOLD_FOR_DDL`: see `hold_relay_for_ddl`.
pub(super) static RELAY_HOLD_FOR_DDL: Seam = Seam::new("GNITZ_INJECT_RELAY_HOLD_FOR_DDL");

/// DDL tick-quiesce requests the tick loop has not yet acked, counted while
/// [`RELAY_HOLD_FOR_DDL`] is armed — its only reader, so this folds away in
/// release. A level, not a running total: a request made before the held relay
/// arrived still reads as waiting, where a delta against a snapshot taken at the
/// hold would miss it.
static DDL_QUIESCE_PENDING: AtomicU64 = AtomicU64::new(0);

/// The window every DDL bundle runs inside: the tick loop is parked and
/// `Shared::ddl_window` raised for exactly as long as the gate lives, so the
/// window ends on every exit path of `handle_ddl_txn`.
///
/// While the depth is non-zero no checkpoint round may run: its drain would
/// never complete against a parked tick loop, and the DDL's own synchronous W2M
/// collectors read the rings by position, so they would eat the round's ACKs and
/// park the committer forever holding `sal_writer_excl`.
///
/// A depth rather than a flag: at shutdown the tick loop returns and drops every
/// queued `Quiesce` sender at once, so several `enter`s resolve together.
struct TickGate {
    /// See [`TickPark`]: dropping it releases the tick loop. `None` when the
    /// loop was already gone — nothing parked, nothing to release.
    _release: Option<TickPark>,
    shared: Rc<Shared>,
}

impl TickGate {
    /// Park the tick loop and enter a DDL window. Returns once the loop has
    /// acked — no tick is in flight and none will start until this gate drops.
    async fn enter(shared: &Rc<Shared>) -> Self {
        let acked_rx = request_quiesce(shared);
        let counted = RELAY_HOLD_FOR_DDL.armed();
        if counted {
            DDL_QUIESCE_PENDING.fetch_add(1, Ordering::Relaxed);
        }
        let park = acked_rx.await;
        if counted {
            DDL_QUIESCE_PENDING.fetch_sub(1, Ordering::Relaxed);
        }
        shared.ddl_window.set(shared.ddl_window.get() + 1);
        TickGate {
            _release: park,
            shared: Rc::clone(shared),
        }
    }
}

impl Drop for TickGate {
    fn drop(&mut self) {
        let depth = &self.shared.ddl_window;
        depth.set(depth.get() - 1);
    }
}

/// The DDL window's own accessor on [`Shared`].
impl Shared {
    /// Reserve this catalog zone's LSN above `floor` and pin every system-table
    /// write to it. Reserve-then-open is one step so the `NonZeroU64` narrowing —
    /// `open_ddl_zone`'s guard against a `0` sentinel — has one site.
    fn open_zone(&self, floor: u64) -> u64 {
        let zone_lsn = self.lsn_alloc.reserve(floor);
        self.cat_mut()
            .open_ddl_zone(NonZeroU64::new(zone_lsn).expect("the zone LSN allocator starts above 0"));
        zone_lsn
    }
}

/// Poll bound for [`RELAY_HOLD_FOR_DDL`], in 1 ms ticks.
const HOLD_RELAY_MAX_POLLS: u32 = 10_000;

/// Hold the FIRST steady-state exchange relay until a DDL is waiting on the tick
/// loop to quiesce, keeping every worker parked in `do_exchange_wait` while that
/// DDL is already issued — so the DDL provably lands on a mid-epoch tick, whether
/// its request came before or after this relay. One-shot: the rest of the run
/// relays at full speed.
pub(super) async fn hold_relay_for_ddl(shared: &Shared) {
    park_until(shared, HOLD_RELAY_MAX_POLLS, "relay hold", || {
        DDL_QUIESCE_PENDING.load(Ordering::Relaxed) > 0
    })
    .await
}

/// [`family_pk_partition`] over a bundle slot, empty where the bundle carries no
/// block for that family.
fn partition_of(families: &[Option<Batch>; SysFamily::COUNT], family: SysFamily) -> PkPartition {
    families[family.index()]
        .as_ref()
        .map(|b| family_pk_partition(family, b))
        .unwrap_or_default()
}

/// Decode a client wal-block slice against family `tid`'s own schema. The sole
/// client → family boundary, so the [`SysFamily::client_writable`] allowlist is here.
fn decode_sys_family(tid: i64, slice: &[u8]) -> Result<(SysFamily, Batch), String> {
    let family = SysFamily::from_id(tid).ok_or_else(|| format!("{tid} is not a system family"))?;
    if !family.client_writable() {
        return Err(format!(
            "family {tid} ({}) is not writable from the wire",
            family.name()
        ));
    }
    let batch = decode_client_batch(slice, family.schema()).map_err(|e| format!("family {tid} decode error: {e}"))?;
    Ok((family, batch))
}

/// Atomic DDL transaction: ingest a bundle of system-table family batches under
/// one durable SAL zone. Reached only via the `DDL_TXN` route. Every catalog
/// write — a CREATE's N families or a DROP/CREATE INDEX/CREATE SCHEMA's single
/// family — flows here, so there is one system-write code path end to end.
///
/// The ACK is a header-only frame carrying the zone LSN in `seek_pk`, sent after
/// [`ddl_txn_body`] returns — outside the catalog write guard, so a client stalled
/// on its `GNITZ_CLIENT_SEND_TIMEOUT_MS` deadline cannot block every other reader
/// for the length of the window. Sound outside the tick gate too: the quiesce
/// exists to keep a worker from being mid-epoch at *broadcast* time, and the body
/// returns only after the broadcast, its `signal_all` and the fsync.
///
/// No schema block: a `DDL_TXN` names no relation (its reply target is `0`), so
/// one could only describe a relation that does not exist.
pub(super) async fn handle_ddl_txn(shared: &Rc<Shared>, peer: &Peer, client_id: u64, data: &[u8]) {
    let t_ddl_start = Instant::now();
    match ddl_txn_body(shared, data).await {
        Ok((zone_lsn, family_count)) => {
            send_msg(
                peer,
                ipc::WireMsg {
                    client_id,
                    seek_pk: zone_lsn as u128,
                    status: STATUS_OK,
                    ..Default::default()
                },
            );
            let total = t_ddl_start.elapsed();
            if total > Duration::from_millis(20) {
                gnitz_debug!("DDL_TXN SLOW total={:?} families={}", total, family_count);
            }
        }
        // `validate_unique_index_create`'s refusal keeps its own status; every
        // other failure in the body is the untyped `STATUS_ERROR` it already was.
        Err(f) => send_fault(peer, 0, client_id, &f),
    }
}

/// The body of [`handle_ddl_txn`]. Returns the durable zone LSN and the bundle's
/// family count (which the caller's slow-DDL log line reports).
///
/// Families are ingested in topo order — ascending for a bundle that creates, so
/// every register/index hook sees its dependencies already in the memtable;
/// descending for one that only drops, so a dependent is retired first. The loop
/// prechecks and applies one family at a time, so a later family's precheck reads
/// the caches an earlier family's apply updated — which is what lets one DROP
/// SCHEMA bundle pass the empty-schema guard. On any failure the applied families
/// are negated in master memory before broadcast, so neither a crash nor a
/// precheck failure can strand an orphan row.
async fn ddl_txn_body(shared: &Rc<Shared>, data: &[u8]) -> Result<(u64, usize), WireFault> {
    // Decode the bundle and materialise each family's wal-block slice into an
    // owned Batch up front (before any lock) — see `decode_sys_family`.
    let raw_families = gnitz_wire::txn_frame::decode_ddl_txn(data).map_err(|e| format!("decode error: {e}"))?;
    if raw_families.is_empty() {
        return Err("DDL_TXN: empty family bundle".into());
    }
    let family_count = raw_families.len();
    // Slotted by discriminant, so "at most one block per family" is a structural
    // error at insert rather than an assumption: every derived list below reads
    // one block per family, and a second VIEW_TAB block would register both sets
    // of views while the new-view ids came from the first alone. The check belongs
    // here and not in the decoder, as `txn_frame::validate_item_ids` does.
    let mut families: [Option<Batch>; SysFamily::COUNT] = std::array::from_fn(|_| None);
    for &(tid, slice) in &raw_families {
        let (family, batch) = decode_sys_family(tid as i64, slice).map_err(|e| format!("DDL_TXN: {e}"))?;
        if families[family.index()].replace(batch).is_some() {
            return Err(format!("DDL_TXN: bundle carries two blocks for family {tid}").into());
        }
    }

    // Both signs of every family, up front: the ingest loop below consumes
    // `families`, and the post-fsync reclamation needs the `-1` ids.
    let views = partition_of(&families, SysFamily::View);
    let tables = partition_of(&families, SysFamily::Table);
    let indices = families[SysFamily::Index.index()]
        .as_ref()
        .map(idx_tab_partition)
        .unwrap_or_default();

    // A CREATE VIEW is a stop-the-world op (source drain + distributed backfill,
    // reactor parked). The VIEW_TAB family's +1 rows, if any, are the new views;
    // they alone need the lock-held barrier and the in-loop source drain below.
    let new_view_ids = views.creates;
    let view_create = !new_view_ids.is_empty();

    // Drain the committer barrier BEFORE acquiring the catalog write lock. The
    // barrier flushes user-table WAL and waits for worker ACKs (tens of ms under
    // load); holding the write lock across that wait would block every concurrent
    // SCAN/SEEK read for no reason — no catalog mutation happens until after the
    // barrier returns.
    //
    // It is also where this bundle's SAL space comes from: the committer answers
    // this barrier with a full checkpoint whenever the SAL is low, and no
    // checkpoint can run once the window below is open.
    await_barrier(shared, BarrierKind::Ddl).await;

    // Quiesce the ticks before the write lock (run_tick/relay_loop take the read
    // lock, so a write-lock-held quiesce would deadlock) and after the barrier
    // (a Quiesce queued ahead of the checkpoint sequence's own Drain parks the
    // tick loop on a release this handler sends only once that barrier returns).
    //
    // Every bundle, not just the stop-the-world ones: a worker parked mid-epoch
    // defers the broadcast DdlSync while still serving reads and pushes inline,
    // so quiescing first is what makes a mutation reach every worker between
    // epochs rather than behind one.
    let _gate = TickGate::enter(shared).await;
    let catalog_write = shared.catalog_rwlock.write().await;

    // The cross-family guards, before anything is reserved or applied: a
    // rejection here returns having written nothing, so it needs no
    // compensation. Placing it after the ingest loop instead would
    // make a check that needs no applied state indistinguishable from a
    // post-apply failure.
    shared.cat().precheck_bundle(&families, &new_view_ids)?;

    if view_create {
        // Lock-held committer barrier: a push could have committed between the
        // pre-lock barrier and the write lock; flush it so every straggler is
        // resident in pending_deltas before the in-loop source drain. The
        // committer stays idle for the rest of the handler (the write lock blocks
        // new pushes).
        await_barrier(shared, BarrierKind::Ddl).await;
    }

    // Discard any stale queue entries from a prior failed DDL so they don't
    // piggyback on this one. (pending_dir_deletions is NOT discarded here: a
    // failed DDL already clears it on the error path, and recovery legitimately
    // queues drops here that must be drained — not discarded — by the post-fsync
    // drain.)
    let _ = shared.cat_mut().drain_pending_broadcasts();

    // Pre-flight global uniqueness for every unique secondary index in this
    // bundle BEFORE reserving the zone LSN or mutating the catalog, so a
    // violation needs no rollback — it just surfaces to the client. This runs
    // before the ingest loop, so for a table created in the same bundle the owner
    // is not yet in the registry and `validate_unique_index_create`
    // short-circuits to an empty filter (sound: the new table is empty, and
    // hook_index_register's own owner-check still succeeds later in the loop). The
    // IDX_TAB row layout (and the IDXTAB_PAY_* payload indices) is fixed by
    // `create_index` and read identically by `hook_index_register`.
    let mut filter_seeds: Vec<(i64, PkColList, UniqueFilter)> = Vec::new();
    for (owner_id, cols) in indices
        .creates
        .into_iter()
        .filter(|(_, _, props)| props.is_unique)
        .map(|(owner_id, cols, _)| (owner_id, cols))
    {
        match shared
            .disp()
            .validate_unique_index_create(&shared.reactor, owner_id, cols.as_slice())
            .await
        {
            // No zone LSN reserved, no catalog mutation yet: just surface
            // the violation to the client. The write lock drops on return.
            Err(e) => return Err(e),
            // Hold the pre-flight's filter to publish post-commit, keyed by
            // the column list (the filter-map key).
            Ok(filter) => filter_seeds.push((owner_id, cols, filter)),
        }
    }

    // Reserve the zone LSN but do NOT publish it until fsync confirms
    // durability. A DDL bundle writes arbitrary system families, so the floor is
    // `max_current_lsn` — the zone must dominate EVERY family's counter
    // (see `ZoneLsnAllocator::reserve` for why a drifted counter would dedup-drop
    // the zone on recovery).
    let zone_lsn = shared.open_zone(shared.cat().registry().max_current_lsn());

    // Ingest the families in ascending topo order so every register/index hook
    // sees its dependencies already in the memtable. For a CREATE VIEW, drain the
    // new view's base sources once the circuit rows are in the memtable
    // (so get_source_ids resolves) but before VIEW_TAB registers the view — after
    // registration the view is a dependent of those bases, so an undrained pending
    // delta would tick it through `evaluate_dag` over rows the backfill below also
    // scans, counting them twice. VIEW_TAB is the first family at or past view
    // priority. A stream source is not drained (see `base_tables_reachable_from`):
    // the backfill scans its empty store, so a still-pending stream row can only
    // reach the new view through the tick, and so reaches it at most once.
    // The between-precheck-and-apply marker holds the single family that was
    // applied but not yet enqueued (a hook/panic failure), which compensation must
    // negate; a precheck failure leaves the marker None, so no ghost -1 is written.
    // The ingest loop writes nothing to the SAL (broadcasts are queued and emitted
    // only in the tail below), so the in-loop drain's tick precedes the zone's
    // broadcasts in SAL order.
    let mut ordered: Vec<(SysFamily, Batch)> = SysFamily::ALL
        .iter()
        .filter_map(|&f| families[f.index()].take().map(|b| (f, b)))
        .collect();
    // A bundle that only drops is ingested in the reverse of creation order —
    // a view retired before the tables it reads, a schema after its members —
    // which is what lets one DROP SCHEMA bundle carry
    // `[VIEW_TAB, TABLE_TAB, SCHEMA_TAB]`. A mixed-sign bundle (an ALTER
    // VIEW's retract-then-register) keeps the creation order.
    if ordered.iter().all(|(_, b)| (0..b.len()).all(|i| b.get_weight(i) < 0)) {
        ordered.sort_by_key(|(f, _)| std::cmp::Reverse(f.topo_priority()));
    } else {
        ordered.sort_by_key(|(f, _)| f.topo_priority());
    }
    let view_prio = SysFamily::View.topo_priority();
    let mut applied_not_enqueued: Option<(SysFamily, Batch)> = None;
    let mut drained_sources = false;
    let ingest_res = guard_panic("DDL", || {
        let cat = shared.cat_mut();
        for (family, fbatch) in ordered {
            if view_create && !drained_sources && family.topo_priority() >= view_prio {
                let (dag, reg) = cat.dag_and_registry_mut();
                for src in dag.base_tables_reachable_from(reg, new_view_ids.clone()) {
                    shared.disp().drain_tick_blocking(src)?;
                }
                drained_sources = true;
            }
            cat.precheck_family(family, &fbatch)?;
            cat.apply_bundle_family(family, fbatch, &mut applied_not_enqueued)?;
        }
        // Compile every new view's circuit here, on the master, while the bundle
        // is still undoable. VIEW_TAB has been applied, so each view is registered
        // and every source resolves — and nothing has reached the SAL yet, so a
        // rejection leaves through the arm below with the view uncreated.
        // Compiling only on the workers, as the backfill does, puts the verdict
        // after the DDL is durable, where it can be nothing but a log line and a
        // view that returns no rows forever.
        for &vid in &new_view_ids {
            cat.preflight_view_compile(vid)?;
        }
        Ok(())
    });
    if let Err(e) = &ingest_res {
        guard_panic("DDL-compensate", || {
            shared.cat_mut().compensate_stage_a(applied_not_enqueued.take())
        })
        .unwrap_or_else(|ce| {
            gnitz_fatal_abort!("Stage-A DDL compensation failed after DDL error '{}': {}", e, ce);
        });
    }
    // The zone closes on every exit from the ingest phase, compensated or not.
    // Nothing between here and its old position past the fsync reads
    // `ctx.ddl_zone_lsn`: the emission takes the zone LSN as a parameter, and it
    // all runs under the catalog write lock.
    shared.cat_mut().close_ddl_zone();
    ingest_res?;

    // SAL emission window: broadcast each queued family under the shared
    // zone_lsn, close the zone with the commit sentinel, then fsync. A failure
    // here is unrecoverable — workers already applied the DdlSync groups in
    // real time — so abort.
    let fsync_fut = {
        let _sal_excl = shared.disp().sal_excl().lock().await;
        emit_zone_to_sal(shared, "DDL", zone_lsn)
    };
    publish_after_fsync(&shared.lsn_alloc, "DDL", zone_lsn, fsync_fut).await;

    // Defer dir removals to the next checkpoint (whose worker-ACK barrier proves
    // every worker consumed past this DROP; removing here races a lagging
    // worker's child-dir create).
    shared.cat_mut().defer_pending_dir_deletions();

    // Invalidate unique-filter state for durably-dropped tables/indices so a
    // recreated table with the same ID does not inherit stale filter entries.
    for &tid in &tables.drops {
        shared.disp().unique_filter_invalidate_table(tid);
    }
    // Relation ids are never reissued within a boot, so these entries are dead.
    for &id in tables.drops.iter().chain(&views.drops) {
        shared.forget_relation(&catalog_write, id);
    }
    // Keying by the whole column list means dropping `(a, b)` never clears a
    // distinct single-column filter on `a`.
    for &(owner_id, cols) in &indices.drops {
        shared.disp().unique_filter_remove(owner_id, cols);
    }

    // Publish the pre-flight's filters so the first INSERT skips a redundant
    // full-cluster warmup scan. Post-fsync only: a broadcast/fsync failure
    // aborts the process before this point, so no filter is published for an
    // index that never committed.
    for (owner_id, cols, filter) in filter_seeds {
        shared.disp().unique_filter_seed(owner_id, cols, filter);
    }

    // Populate every new view. A post-fsync Err cannot be rolled back (the CREATE
    // is durable), so abort — restart's boot rebuild refills it.
    guard_panic("view-backfill", || {
        shared.disp().backfill_views_in_dep_order(&new_view_ids)
    })
    .unwrap_or_else(|e| {
        gnitz_fatal_abort!(
            "live CREATE VIEW backfill failed after the CREATE was made durable: {}",
            e
        );
    });

    // Finish the checkpoint here, while the reactor is still parked and the tick
    // loop still quiesced, rather than leaving the database rebuild-on-boot until
    // something wakes the committer.
    if shared.disp().derived_needs_restamp() {
        let mut pending = Vec::new();
        shared.drain_live_tick_rows_into(&mut pending);
        guard_panic("view-restamp", || shared.disp().restamp_derived(&pending)).unwrap_or_else(|e| {
            gnitz_fatal_abort!("re-stamping derived state after a CREATE VIEW reclaim failed: {}", e);
        });
    }

    Ok((zone_lsn, family_count))
}

/// Emit a closed catalog zone to the SAL: drain the queued family broadcasts,
/// write each under `zone_lsn`, close with the commit sentinel, and submit the
/// fdatasync SQE. Draining here rather than at each caller is what stops a queued
/// broadcast riding the *next* zone's LSN.
///
/// Caller holds `sal_writer_excl`, so reservation order == SAL write order. Every
/// failure aborts: this runs after the in-memory catalog mutation, so a partial
/// emit diverges master and worker permanently.
fn emit_zone_to_sal(shared: &Shared, op: &'static str, zone_lsn: u64) -> FsyncFuture {
    let disp = shared.disp();
    let drained = shared.cat_mut().drain_pending_broadcasts();
    // Nothing inside the scope is visible until it commits, so a refused group
    // leaves no half-written zone behind. The block is synchronous throughout,
    // which is what the scope requires.
    let scope = disp.begin(zone_lsn, "ddl");
    let emitted = guard_panic(op, || {
        // The wire carries the family as its tid; this is the one place the
        // typed family narrows.
        for (family, bat) in &drained {
            disp.broadcast_ddl(&scope, family.id(), bat).map_err(|f| f.text)?;
        }
        Ok(())
    });
    if let Err(e) = emitted {
        gnitz_fatal_abort!("{} broadcast failed after in-memory catalog mutation: {}", op, e);
    }
    // An empty bundle (`apply_and_enqueue_family` drops empty batches) opened no
    // zone, so this closes nothing: every sentinel follows an ordinary group,
    // which keeps a run of them out of the checkpoint reserve.
    if let Err(e) = scope.commit() {
        gnitz_fatal_abort!("{} commit sentinel was refused, so its zone never published: {}", op, e);
    }
    disp.signal_all();
    shared.reactor.fsync(shared.disp().sal_fd())
}

/// Await `fsync`, then publish `zone`. Paired for the same reason
/// `ZoneLsnAllocator` pairs reserve and publish: no caller can publish an LSN
/// whose bytes are not yet on disk.
async fn publish_after_fsync(alloc: &ZoneLsnAllocator, op: &'static str, zone: u64, fsync: FsyncFuture) {
    let rc = fsync.await;
    if rc < 0 {
        gnitz_fatal_abort!("SAL fdatasync ({}) failed rc={}", op, rc);
    }
    alloc.publish(zone);
}

/// Durably reserve a SERIAL id range for `seq_id` and return the range base.
///
/// The high-water must be persisted *at allocation time*: `recover_sequences`
/// runs pre-fork and the master holds no user-table rows, so a lost advance
/// cannot be re-derived. Hence the DDL SAL commit path, the same one `CREATE`
/// uses.
///
/// Both locks are released before the fsync — the whole reserve/mutate/emit span
/// is synchronous, so catalog readers never block across an `fdatasync`.
/// `handle_ddl_txn` holds its write guard past the fsync instead, needing it for
/// `forget_relation`, the filter invalidation and the backfill.
pub(super) async fn commit_serial_range_durable(shared: &Rc<Shared>, seq_id: i64, count: i64) -> Result<i64, String> {
    let (base, zone_lsn, fsync_fut) = {
        // Lock order catalog -> SAL, matching INSERT/SEEK, so acquiring SAL under
        // catalog.write cannot deadlock. Both guards drop at the end of this block.
        let _write = shared.catalog_rwlock.write().await;

        // A SERIAL sequence id IS the owning table's id, and only a base table may
        // own a SERIAL column. Checked under the write lock that guards the
        // reservation: an unvalidated id would durably write a `sys_sequences`
        // row that `recover_sequences` replays straight into the catalog's own
        // id counters at the next open. Not through `target_kind`: this reserves a
        // range rather than reading or writing rows, so neither `Access` fits and
        // both its rejections would name a push.
        if !shared
            .cat()
            .registry()
            .relation(seq_id)
            .map(Relation::kind)
            .is_some_and(|k| k.is_base_table())
        {
            return Err(format!("sequence {seq_id} is not a base table"));
        }

        let _sal_excl = shared.disp().sal_excl().lock().await;

        let (base, delta, zone_floor) = shared.cat_mut().reserve_user_sequence(seq_id, count);
        let zone_lsn = shared.open_zone(zone_floor);

        // A sys_sequences advance is a pure system-table write (no evaluate_dag,
        // no rollback); a hook failure on a well-formed 2-row delta is an
        // invariant violation — abort rather than compensate.
        let applied = {
            let cat = shared.cat_mut();
            cat.precheck_family(SysFamily::Sequence, &delta)
                .and_then(|()| cat.apply_and_enqueue_family(SysFamily::Sequence, delta))
        };
        if let Err(e) = applied {
            gnitz_fatal_abort!("sys_sequences ingest (serial range) failed: {}", e);
        }
        shared.cat_mut().close_ddl_zone();

        // SAL emission under the still-held sal_writer_excl; the fdatasync SQE is
        // submitted synchronously. Both guards drop as this block ends, before
        // the await below.
        (base, zone_lsn, emit_zone_to_sal(shared, "serial-range", zone_lsn))
    };

    publish_after_fsync(&shared.lsn_alloc, "serial range", zone_lsn, fsync_fut).await;
    Ok(base)
}
