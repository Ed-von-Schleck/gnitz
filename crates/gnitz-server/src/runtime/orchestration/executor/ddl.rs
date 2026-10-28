//! The catalog-zone write path: one protocol, two entry points.
//! [`handle_ddl_txn`] ingests a client bundle of system-table families;
//! [`reserve_serial_range`] advances one sequence. Both take the SAL lock and
//! emit the zone while the catalog applies it; only the DDL awaits the zone's
//! fsync.
//!
//! A DDL bundle additionally runs under [`DdlLocks`]. The serial path needs
//! none of that: a `sys_sequences` advance has no DAG evaluation, and the row
//! it logs is one no worker reads.

use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::{park_until, request_barrier, send_ack, Shared};
use crate::catalog::{unique_index_creates, AppliedDdl, DdlBundle, SysFamily};
use crate::runtime::committer::{BarrierKind, CHECKPOINTS_BEHIND_DDL};
use crate::runtime::master::UniqueFilter;
use crate::runtime::peer::Peer;
use crate::runtime::reactor::WriteGuard;
use crate::runtime::sal::Read;
use gnitz_foundation::fault::Seam;
use gnitz_wire::{ClientVerb, DecodedControl, PkColList, Target, WireFault};
use gnitz_zset::repr::Batch;

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
/// and a low SAL gets its checkpoint there. A committer round in flight holds
/// the SAL lock to its end, which the driver waits on before it mutates.
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

/// `GNITZ_INJECT_DDL_HOLD_FOR_CHECKPOINT`: see `hold_ddl_for_checkpoint`.
static DDL_HOLD_FOR_CHECKPOINT: Seam = Seam::new("GNITZ_INJECT_DDL_HOLD_FOR_CHECKPOINT");

/// Hold the first DDL, its zone written and its locks still held, until a
/// checkpoint sequence waits on its tick gate, so that sequence provably starts
/// inside the DDL. The zone moves the SAL cursor, so a checkpoint threshold
/// below one zone has the watchdog wake the committer into that wait. One-shot.
async fn hold_ddl_for_checkpoint(shared: &Shared) {
    let seen = CHECKPOINTS_BEHIND_DDL.load(Ordering::Relaxed);
    park_until(shared, HOLD_TICK_MAX_POLLS, "ddl hold", || {
        CHECKPOINTS_BEHIND_DDL.load(Ordering::Relaxed) > seen
    })
    .await
}

/// Decode one `DDL_TXN` item's block under its family's own schema.
fn decode_sys_family(frame: &[u8], ctrl: DecodedControl) -> Result<(SysFamily, Batch), String> {
    let tid = ctrl.hdr.target_id;
    let family = SysFamily::from_id(tid).ok_or_else(|| format!("{tid} is not a system family"))?;
    let block = &frame[ctrl.data.expect("a DDL_TXN item carries a data block")];
    let batch = Batch::decode_foreign_wal_block(block, family.schema())
        .map_err(|e| format!("family {tid} decode error: {e}"))?;
    Ok((family, batch))
}

/// A `DDL_TXN` body as one block per family, each family's wal-block slice
/// materialised into an owned batch under its own schema — see
/// [`decode_sys_family`]. One slot per family: every list derived from a bundle
/// reads one block of each.
pub(super) fn decode_bundle(body: &[u8]) -> Result<DdlBundle, WireFault> {
    let items =
        gnitz_wire::txn_frame::decode_items(body, ClientVerb::DdlTxn).map_err(|e| format!("decode error: {e}"))?;
    let mut families = DdlBundle::default();
    for (frame, ctrl) in items {
        decode_sys_family(frame, ctrl)
            .and_then(|(family, batch)| families.insert(family, batch))
            .map_err(|e| format!("DDL_TXN: {e}"))?;
    }
    Ok(families)
}

/// Atomic DDL transaction: ingest a bundle of system-table family batches under
/// one durable SAL zone. Reached only via the `DDL_TXN` route. Every catalog
/// write — a CREATE's N families or a DROP/CREATE INDEX/CREATE SCHEMA's single
/// family — flows here, so there is one system-write code path end to end.
///
/// The bundle is applied and its zone laid out under one hold of the SAL lock,
/// with no await between: a bundle the catalog or the log refuses is negated in
/// master memory there, so no system flush sees a row no zone carries.
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
    // Decoded up front, before any lock.
    let families = decode_bundle(body)?;
    let family_count = families.family_count();

    let unique_creates = families
        .get(SysFamily::Index)
        .map(unique_index_creates)
        .unwrap_or_default();

    let locks = enter_ddl(shared).await;
    // Only an owner that already exists is scanned, so only it leaves claims.
    let claims = {
        let cat = shared.cat();
        unique_creates
            .iter()
            .any(|&(owner, _)| cat.registry.relation(owner).is_some())
    };
    let committed = commit_bundle(shared, families, &unique_creates).await;
    if claims {
        release_preflight(shared).await;
    }
    let Committed { applied, zone_lsn, filter_seeds } = committed?;

    // Relation ids are never reissued within a boot, so these entries are dead.
    for &id in &applied.dropped_relations {
        shared.forget_relation(&locks.catalog, id);
    }
    // Keying by the whole column list means dropping `(a, b)` never clears a
    // distinct single-column filter on `a`.
    for &(owner_id, cols) in &applied.dropped_indexes {
        shared.disp().unique_filter_remove(owner_id, cols);
    }

    // Post-fsync: a refused zone has returned and an fsync failure has aborted
    // the process before this point, so no filter is published for an index
    // that never committed.
    for (owner_id, cols, filter) in filter_seeds {
        shared.disp().unique_filter_seed(owner_id, cols, filter);
    }

    // Populate every new view. A post-fsync Err cannot be rolled back (the CREATE
    // is durable), so abort — restart's boot rebuild refills it.
    if let Err(e) = shared.disp().backfill_views_in_dep_order(&applied.created_views).await {
        gnitz_fatal_abort!("live CREATE VIEW backfill failed after the CREATE was made durable: {e}");
    }

    // After the backfill: a new view has read the sealed families, and these
    // rows reach the views that already scanned them.
    if let Err(e) = shared.disp().drain_tick(&applied.held_above).await {
        gnitz_fatal_abort!("catalog tick failed after the DDL was made durable: {e}");
    }
    for &family in &applied.held_above {
        shared.sync_waiters.wake(family);
    }

    if DDL_HOLD_FOR_CHECKPOINT.take_once() {
        hold_ddl_for_checkpoint(shared).await;
    }
    drop(locks);
    send_ack(peer, 0, zone_lsn);
    let total = t_ddl_start.elapsed();
    if total > Duration::from_millis(20) {
        gnitz_debug!("DDL_TXN SLOW total={:?} families={}", total, family_count);
    }
    Ok(())
}

/// A bundle made durable: what it applied, its zone, and the unique-filter seed of
/// each unique index it created over rows already present.
struct Committed {
    applied: AppliedDdl,
    zone_lsn: u64,
    filter_seeds: Vec<(u64, PkColList, UniqueFilter)>,
}

/// Pre-flight every unique index of the bundle, then apply it and make its zone
/// durable. Every `Err` leaves the catalog as it was.
async fn commit_bundle(
    shared: &Rc<Shared>,
    families: DdlBundle,
    unique_creates: &[(u64, PkColList)],
) -> Result<Committed, WireFault> {
    // Pre-flight global uniqueness for every unique secondary index in this
    // bundle before reserving the zone LSN or mutating the catalog, so a
    // violation needs no rollback.
    let mut filter_seeds = Vec::new();
    for &(owner_id, cols) in unique_creates {
        // `None`: the index covers the owner's PK, so nothing will consult a filter.
        if let Some(filter) = shared.disp().validate_unique_index_create(owner_id, cols).await? {
            filter_seeds.push((owner_id, cols, filter));
        }
    }

    // Before the views exist: a tick owed now carries rows pushed ahead of them,
    // and a stream's must not reach them.
    if families.get(SysFamily::Circuit).is_some() {
        if let Some(lease) = shared.emit_pending_tick().await? {
            lease.acks().await;
        }
    }
    let ((applied, zone_lsn), synced) = {
        let mut excl = shared.disp().sal().lock().await;
        let written = shared
            .disp()
            .emit_zone(&mut excl, |cat, sink| cat.apply_bundle(families, sink))?;
        (written, excl.sync(shared.disp().reactor()))
    };
    synced.await;
    Ok(Committed { applied, zone_lsn, filter_seeds })
}

/// Release the pre-flight's index claims, committed or not, while the DDL's locks
/// still hold off every checkpoint. The release follows the zone in the log, so a
/// worker enters the catalog's claim on an index the DDL created before it drops
/// the pre-flight's. A claim left standing would keep an index no catalog row
/// owns, whose directory the next checkpoint's sweep removes from under it.
async fn release_preflight(shared: &Shared) {
    let released = async { shared.disp().scan(Read::ReleasePreflight).await?.finish().await }.await;
    if let Err(e) = released {
        gnitz_fatal_abort!("releasing the unique-index pre-flight's claims failed: {}", e.text);
    }
}

/// Reserve a SERIAL id range for `seq` and return its base. The advance is a
/// DDL SAL zone because the master holds no user-table rows to re-derive a lost
/// high-water from.
///
/// The ACK waits for no fdatasync. A row holding one of these ids is durable
/// through a later zone's fdatasync of the same log or through a checkpoint, and
/// either makes this zone durable first. An advance a crash loses was used by no
/// durable row, and a client drops its reserved ids with its connection.
pub(super) async fn reserve_serial_range(shared: &Rc<Shared>, seq: Target, count: u64) -> Result<i64, WireFault> {
    // A read guard holds the token and the sequence's table against a DDL; the
    // SAL lock orders reservations among themselves, and everything under it is
    // synchronous. Lock order catalog -> SAL, matching INSERT and every read.
    let _read = shared.catalog_rwlock.read().await;
    shared.cat().check_token(seq)?;

    let mut excl = shared.disp().sal().lock().await;
    let (base, _) = shared
        .disp()
        .emit_zone(&mut excl, |cat, sink| cat.reserve_user_sequence(seq.tid, count, sink))?;
    Ok(base)
}
