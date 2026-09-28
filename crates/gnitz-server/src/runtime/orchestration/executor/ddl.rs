//! The catalog-zone write path: one protocol, two entry points.
//! [`handle_ddl_txn`] ingests a client bundle of system-table families;
//! [`commit_serial_range_durable`] advances one sequence. Both reserve a zone
//! LSN, mutate the catalog, pin the families it wrote, [`emit_zone_to_sal`] and
//! [`publish_after_fsync`].
//!
//! A DDL bundle additionally runs under [`DdlLocks`]. The serial path needs none of that: a `sys_sequences`
//! advance has no DAG evaluation and no rollback path, and the row it broadcasts
//! is one no worker reads.
//!
//! A child of `executor`, so it reads that module's private items — `Shared` and
//! its accessors included — with no visibility widened, and the DDL seams sit
//! beside the code they perturb.

use std::future::Future;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::{guard_panic, park_until, request_barrier, send_msg, Shared};
use crate::catalog::{family_pk_partition, idx_tab_partition, PkPartition, SysFamily};
use crate::runtime::committer::BarrierKind;
use crate::runtime::lsn::ZoneLsnAllocator;
use crate::runtime::master::UniqueFilter;
use crate::runtime::orchestration::guard_panic_async;
use crate::runtime::peer::Peer;
use crate::runtime::reactor::WriteGuard;
use crate::runtime::sal::SalExcl;
use crate::runtime::wire as ipc;
use gnitz_foundation::fault::Seam;
use gnitz_store::storage::Batch;
use gnitz_wire::control::DecodedControl;
use gnitz_wire::{ClientVerb, PkColList, WireFault};

/// `GNITZ_INJECT_TICK_HOLD_FOR_DDL`: see `hold_tick_for_ddl`.
pub(super) static TICK_HOLD_FOR_DDL: Seam = Seam::new("GNITZ_INJECT_TICK_HOLD_FOR_DDL");

/// DDLs waiting for the tick gate, counted only while [`TICK_HOLD_FOR_DDL`] is
/// armed: its one reader.
static DDL_GATE_WAITERS: AtomicU64 = AtomicU64::new(0);

/// What a DDL body runs under: the catalog write, and the tick gate's write, so
/// no tick is in flight and every broadcast DdlSync lands between epochs.
struct DdlLocks {
    catalog: WriteGuard,
    _ticks: WriteGuard,
}

/// Take [`DdlLocks`]. The committer barrier comes first, holding no lock: a
/// checkpoint sequence it is deferred behind drains ticks the gate would stop,
/// and a low SAL gets its checkpoint there. Past the gate, wait out a committer
/// round in flight; the committer starts none under the gate.
async fn enter_ddl(shared: &Shared) -> DdlLocks {
    request_barrier(shared, BarrierKind::Ddl).await;
    let catalog = shared.catalog_rwlock.write().await;
    let counted = TICK_HOLD_FOR_DDL.armed();
    if counted {
        DDL_GATE_WAITERS.fetch_add(1, Ordering::Relaxed);
    }
    let ticks = shared.tick_gate.write().await;
    if counted {
        DDL_GATE_WAITERS.fetch_sub(1, Ordering::Relaxed);
    }
    drop(shared.disp().sal().lock().await);
    DdlLocks { catalog, _ticks: ticks }
}

/// Poll bound for [`TICK_HOLD_FOR_DDL`], in 1 ms ticks.
const HOLD_TICK_MAX_POLLS: u32 = 10_000;

/// Hold the first steady-state tick in flight until a DDL waits on the tick gate,
/// so that DDL provably lands mid-tick. One-shot.
pub(super) async fn hold_tick_for_ddl(shared: &Shared) {
    park_until(shared, HOLD_TICK_MAX_POLLS, "tick hold", || {
        DDL_GATE_WAITERS.load(Ordering::Relaxed) > 0
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

/// Decode one `DDL_TXN` item's block under its family's own schema, behind the
/// [`SysFamily::client_writable`] allowlist.
fn decode_sys_family(frame: &[u8], ctrl: DecodedControl) -> Result<(SysFamily, Batch), String> {
    let tid = ctrl.hdr.target_id;
    let family = SysFamily::from_id(tid).ok_or_else(|| format!("{tid} is not a system family"))?;
    if !family.client_writable() {
        return Err(format!(
            "family {tid} ({}) is not writable from the wire",
            family.name()
        ));
    }
    let batch = ipc::decode_client_frame(frame, ctrl, Some(family.schema()), ipc::unknown)
        .map_err(|e| format!("family {tid} decode error: {e}"))?
        .data_batch
        .expect("a DDL_TXN item carries a data block");
    Ok((family, batch))
}

/// Atomic DDL transaction: ingest a bundle of system-table family batches under
/// one durable SAL zone. Reached only via the `DDL_TXN` route. Every catalog
/// write — a CREATE's N families or a DROP/CREATE INDEX/CREATE SCHEMA's single
/// family — flows here, so there is one system-write code path end to end.
///
/// Families are ingested in topo order — ascending for a bundle that creates, so
/// every register/index hook sees its dependencies already in the memtable;
/// descending for one that only drops, so a dependent is retired first. The loop
/// prechecks and applies one family at a time, so a later family's precheck reads
/// the caches an earlier family's apply updated — which is what lets one DROP
/// SCHEMA bundle pass the empty-schema guard. On any failure the applied families
/// are negated in master memory before broadcast, so neither a crash nor a
/// precheck failure can strand an orphan row.
///
/// The ACK is a header-only frame carrying the zone LSN in `arg0`, sent once the
/// DDL guards have dropped — so a client stalled on its
/// `GNITZ_CLIENT_SEND_TIMEOUT_MS` deadline cannot block every other reader for the
/// length of the window. Sound outside the tick gate too: the gate exists to keep
/// a worker from being mid-epoch at *broadcast* time, and the ACK goes out only
/// after the broadcast, its wake and the fsync.
///
/// No schema block: a `DDL_TXN` names no relation (its reply target is `0`), so
/// one could only describe a relation that does not exist.
pub(super) async fn handle_ddl_txn(shared: &Rc<Shared>, peer: &Peer, body: &[u8]) -> Result<(), WireFault> {
    let t_ddl_start = Instant::now();
    // Decode the bundle and materialise each family's wal-block slice into an
    // owned Batch up front (before any lock) — see `decode_sys_family`.
    let items =
        gnitz_wire::txn_frame::decode_items(body, ClientVerb::DdlTxn).map_err(|e| format!("decode error: {e}"))?;
    let family_count = items.len();
    // One slot per family: every list derived below reads one block of each.
    let mut families: [Option<Batch>; SysFamily::COUNT] = std::array::from_fn(|_| None);
    for (frame, ctrl) in items {
        let (family, batch) = decode_sys_family(frame, ctrl).map_err(|e| format!("DDL_TXN: {e}"))?;
        if families[family.index()].replace(batch).is_some() {
            return Err(format!("DDL_TXN: bundle carries two blocks for family {}", family.id()).into());
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

    let new_view_ids = views.creates;
    let view_create = !new_view_ids.is_empty();

    let locks = enter_ddl(shared).await;

    // The cross-family guards, before anything is reserved or applied: a
    // rejection here returns having written nothing, so it needs no
    // compensation. Placing it after the ingest loop instead would
    // make a check that needs no applied state indistinguishable from a
    // post-apply failure.
    shared.cat().precheck_bundle(&families, &new_view_ids)?;

    // Pre-flight global uniqueness for every unique secondary index in this
    // bundle before reserving the zone LSN or mutating the catalog, so a
    // violation needs no rollback.
    let mut filter_seeds: Vec<(u64, PkColList, UniqueFilter)> = Vec::new();
    for (owner_id, cols) in indices
        .creates
        .into_iter()
        .filter(|(_, _, props)| props.is_unique)
        .map(|(owner_id, cols, _)| (owner_id, cols))
    {
        match shared
            .disp()
            .validate_unique_index_create(owner_id, cols.as_slice())
            .await
        {
            // No zone LSN reserved, no catalog mutation yet: just surface
            // the violation to the client. The write lock drops on return.
            Err(e) => return Err(e),
            Ok(Some(filter)) => filter_seeds.push((owner_id, cols, filter)),
            // The index covers the owner's PK: nothing will consult a filter.
            Ok(None) => {}
        }
    }

    // Reserve the zone LSN but do NOT publish it until fsync confirms
    // durability.
    let zone_lsn = shared.lsn_alloc.reserve();

    // Ingest the families in ascending topo order so every register/index hook
    // sees its dependencies already in the memtable. For a CREATE VIEW, drain the
    // new view's base sources once the circuit rows are applied (so the dependency
    // map names the view's sources) but before VIEW_TAB registers the view — after
    // registration the view is a dependent of those bases, so an undrained pending
    // delta would tick it through a `Drive::Tick` over rows the backfill below also
    // scans, counting them twice. VIEW_TAB is the first family at or past view
    // priority. A stream source is not drained (see `base_tables_reachable_from`):
    // the backfill scans its empty store, so a still-pending stream row can only
    // reach the new view through the tick, and so reaches it at most once.
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
    let mut drained_sources = false;
    let ingest_res = guard_panic_async("DDL", async {
        for (family, fbatch) in ordered {
            if view_create && !drained_sources && family.topo_priority() >= view_prio {
                let sources = {
                    let cat = shared.cat();
                    cat.dag.base_tables_reachable_from(&cat.registry, new_view_ids.clone())
                };
                shared.disp().drain_tick(&sources).await?;
                drained_sources = true;
            }
            shared.cat_mut().submit(family, fbatch)?;
        }
        // Compile every new view's circuit here, on the master, while the bundle
        // is still undoable. VIEW_TAB has been applied, so each view is registered
        // and every source resolves — and nothing has reached the SAL yet, so a
        // rejection leaves through the arm below with the view uncreated.
        // Compiling only on the workers, as the backfill does, puts the verdict
        // after the DDL is durable, where it can be nothing but a log line and a
        // view that returns no rows forever.
        for &vid in &new_view_ids {
            crate::query::preflight_compile(&shared.cat().registry, vid)?;
        }
        Ok::<_, WireFault>(())
    })
    .await;
    if let Err(e) = &ingest_res {
        guard_panic("DDL-compensate", || shared.cat_mut().compensate_stage_a()).unwrap_or_else(|ce| {
            gnitz_fatal_abort!("Stage-A DDL compensation failed after DDL error '{}': {}", e, ce);
        });
    }
    ingest_res?;
    shared.cat_mut().mark_zone_applied(zone_lsn);

    // SAL emission window: broadcast each queued family under the shared
    // zone_lsn, commit the zone, then fsync. A failure here is unrecoverable —
    // workers already applied the DdlSync groups in real time — so abort.
    let synced = emit_zone_to_sal(shared, &mut shared.disp().sal().lock().await, "DDL", zone_lsn);
    publish_after_fsync(&shared.lsn_alloc, zone_lsn, synced).await;

    // Relation ids are never reissued within a boot, so these entries are dead.
    for &id in tables.drops.iter().chain(&views.drops) {
        shared.forget_relation(&locks.catalog, id);
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
    guard_panic_async(
        "view-backfill",
        shared.disp().backfill_views_in_dep_order(&new_view_ids),
    )
    .await
    .unwrap_or_else(|e| {
        gnitz_fatal_abort!(
            "live CREATE VIEW backfill failed after the CREATE was made durable: {}",
            e
        );
    });

    drop(locks);
    send_msg(peer, ipc::WireMsg { arg0: zone_lsn, ..Default::default() });
    let total = t_ddl_start.elapsed();
    if total > Duration::from_millis(20) {
        gnitz_debug!("DDL_TXN SLOW total={:?} families={}", total, family_count);
    }
    Ok(())
}

/// Emit every queued family broadcast as one zone at `zone_lsn` and submit its
/// fdatasync. Aborts on failure: the catalog is already mutated in memory.
fn emit_zone_to_sal<'w>(
    shared: &Shared,
    excl: &mut SalExcl<'w>,
    op: &'static str,
    zone_lsn: u64,
) -> impl Future<Output = ()> + 'w {
    let disp = shared.disp();
    let drained = shared.cat_mut().drain_pending_broadcasts();
    // Nothing inside the scope is visible until it commits, so a refused group
    // leaves no half-written zone behind. The block is synchronous throughout,
    // which is what the scope requires.
    let scope = excl.begin(zone_lsn, "ddl");
    let emitted = guard_panic(op, || {
        // The wire carries the family as its tid; this is the one place the
        // typed family narrows.
        for (family, bat) in &drained {
            disp.broadcast_ddl(&scope, family.id(), bat)?;
        }
        Ok::<_, WireFault>(())
    });
    if let Err(e) = emitted {
        gnitz_fatal_abort!("{} broadcast failed after in-memory catalog mutation: {}", op, e);
    }
    // Synced even when no zone opened: publishing `zone_lsn` vouches for every
    // LSN below it.
    scope.commit();
    excl.sync(disp.reactor(), op)
}

/// Await `synced`, then publish `zone`. Paired for the same reason
/// `ZoneLsnAllocator` pairs reserve and publish: no caller can publish an LSN
/// whose bytes are not yet on disk.
async fn publish_after_fsync(alloc: &ZoneLsnAllocator, zone: u64, synced: impl Future<Output = ()>) {
    synced.await;
    alloc.publish(zone);
}

/// Durably reserve a SERIAL id range for `seq_id` and return its base. It commits
/// through a DDL SAL zone because the master holds no user-table rows to re-derive
/// a lost high-water from.
///
/// Both locks are released before the fsync — the whole reserve/mutate/emit span
/// is synchronous, so catalog readers never block across an `fdatasync`.
/// `handle_ddl_txn` holds its write guard past the fsync instead, needing it for
/// its post-fsync catalog cleanup and the backfill.
pub(super) async fn commit_serial_range_durable(shared: &Rc<Shared>, seq_id: u64, count: u64) -> Result<i64, String> {
    let (base, zone_lsn, synced) = {
        // Lock order catalog -> SAL, matching INSERT/SEEK, so acquiring SAL under
        // catalog.write cannot deadlock. Both guards drop at the end of this block.
        let _write = shared.catalog_rwlock.write().await;

        let mut excl = shared.disp().sal().lock().await;

        let (base, delta) = shared.cat().reserve_user_sequence(seq_id, count)?;
        let zone_lsn = shared.lsn_alloc.reserve();

        // A sys_sequences advance is a pure system-table write (no view tick,
        // no rollback); a hook failure on a well-formed 2-row delta is an
        // invariant violation — abort rather than compensate.
        if let Err(e) = shared.cat_mut().submit(SysFamily::Sequence, delta) {
            gnitz_fatal_abort!("sys_sequences ingest (serial range) failed: {}", e);
        }
        shared.cat_mut().mark_zone_applied(zone_lsn);

        // SAL emission under the still-held `SalExcl`; the fdatasync SQE is
        // submitted synchronously. Both guards drop as this block ends, before
        // the await below.
        let synced = emit_zone_to_sal(shared, &mut excl, "serial-range", zone_lsn);
        (base, zone_lsn, synced)
    };

    publish_after_fsync(&shared.lsn_alloc, zone_lsn, synced).await;
    Ok(base)
}
