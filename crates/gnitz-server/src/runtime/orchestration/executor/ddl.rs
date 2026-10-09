//! The catalog-zone write path: one protocol, two entry points.
//! [`handle_ddl_txn`] ingests a client bundle of system-table families;
//! [`reserve_serial_range`] advances one sequence. Both take the SAL lock,
//! mutate the catalog, then emit the zone the write returned;
//! only the DDL awaits the zone's fsync.
//!
//! A DDL bundle additionally runs under [`DdlLocks`]. The serial path needs
//! none of that: a `sys_sequences` advance has no DAG evaluation, and the row
//! it logs is one no worker reads.
//!
//! A child of `executor`, so it reads that module's private items — `Shared` and
//! its accessors included — with no visibility widened, and the DDL seams sit
//! beside the code they perturb.

use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::{park_until, request_barrier, send_ack, Shared};
use crate::catalog::{family_pk_partition, idx_tab_partition, PkPartition, SysFamily, ZoneError};
use crate::runtime::committer::BarrierKind;
use crate::runtime::master::UniqueFilter;
use crate::runtime::peer::Peer;
use crate::runtime::reactor::WriteGuard;
use crate::runtime::wire as ipc;
use gnitz_foundation::fault::Seam;
use gnitz_wire::control::{DecodedControl, Target};
use gnitz_wire::{ClientVerb, PkColList, WireFault};
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

/// [`family_pk_partition`] over a bundle slot, empty where the bundle carries no
/// block for that family.
fn partition_of(families: &[Option<Batch>; SysFamily::COUNT], family: SysFamily) -> PkPartition {
    families[family.index()]
        .as_ref()
        .map(|b| family_pk_partition(family, b))
        .unwrap_or_default()
}

/// Decode one `DDL_TXN` item's block under its family's own schema.
fn decode_sys_family(frame: &[u8], ctrl: DecodedControl) -> Result<(SysFamily, Batch), String> {
    let tid = ctrl.hdr.target_id;
    let family = SysFamily::from_id(tid).ok_or_else(|| format!("{tid} is not a system family"))?;
    let batch = ipc::decode_client_rows(frame, &ctrl, family.schema())
        .map_err(|e| format!("family {tid} decode error: {e}"))?
        .expect("a DDL_TXN item carries a data block");
    Ok((family, batch))
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

    // Both signs of every family, up front: applying the bundle consumes
    // `families`, and the post-fsync reclamation needs the `-1` ids.
    let views = partition_of(&families, SysFamily::View);
    let tables = partition_of(&families, SysFamily::Table);
    let indices = families[SysFamily::Index.index()]
        .as_ref()
        .map(idx_tab_partition)
        .unwrap_or_default();

    let new_view_ids = views.creates;

    let locks = enter_ddl(shared).await;

    // Pre-flight global uniqueness for every unique secondary index in this
    // bundle before reserving the zone LSN or mutating the catalog, so a
    // violation needs no rollback.
    let mut filter_seeds: Vec<(u64, PkColList, UniqueFilter)> = Vec::new();
    for (owner_id, cols) in indices
        .creates
        .into_iter()
        .filter(|&(_, _, is_unique)| is_unique)
        .map(|(owner_id, cols, _)| (owner_id, cols))
    {
        match shared.disp().validate_unique_index_create(owner_id, cols).await {
            // No catalog mutation yet: just surface the violation to the
            // client. The write lock drops on return.
            Err(e) => return Err(e),
            Ok(Some(filter)) => filter_seeds.push((owner_id, cols, filter)),
            // The index covers the owner's PK: nothing will consult a filter.
            Ok(None) => {}
        }
    }

    // Before the views exist: a tick owed now carries rows pushed ahead of them,
    // and a stream's must not reach them.
    if families[SysFamily::Circuit.index()].is_some() {
        if let Some(lease) = shared.emit_pending_tick().await? {
            lease.acks().await;
        }
    }
    let (zone_lsn, held_above, synced) = {
        let mut excl = shared.disp().sal().lock().await;
        let zone = zone_or_abort(shared.cat_mut().apply_bundle(families), "DDL")?;
        // The families a worker holds above its cut once the zone is emitted.
        let held_above: Vec<u64> = SysFamily::ALL
            .into_iter()
            .filter(|f| zone.iter().any(|g| g.family == *f && g.scanned))
            .map(SysFamily::id)
            .collect();
        let zone_lsn = shared.disp().emit_zone(&mut excl, zone, "DDL")?;
        (zone_lsn, held_above, excl.sync(shared.disp().reactor(), "DDL"))
    };
    synced.await;

    // Relation ids are never reissued within a boot, so these entries are dead.
    for &id in tables.drops.iter().chain(&views.drops) {
        shared.forget_relation(&locks.catalog, id);
    }
    // Keying by the whole column list means dropping `(a, b)` never clears a
    // distinct single-column filter on `a`.
    for &(owner_id, cols) in &indices.drops {
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
    if let Err(e) = shared.disp().backfill_views_in_dep_order(&new_view_ids).await {
        gnitz_fatal_abort!("live CREATE VIEW backfill failed after the CREATE was made durable: {e}");
    }

    // After the backfill: a new view has read the sealed families, and these
    // rows reach the views that already scanned them.
    if let Err(e) = shared.disp().drain_tick(&held_above).await {
        gnitz_fatal_abort!("catalog tick failed after the DDL was made durable: {e}");
    }
    for &family in &held_above {
        shared.sync_waiters.wake(family);
    }

    drop(locks);
    send_ack(peer, 0, zone_lsn);
    let total = t_ddl_start.elapsed();
    if total > Duration::from_millis(20) {
        gnitz_debug!("DDL_TXN SLOW total={:?} families={}", total, family_count);
    }
    Ok(())
}

/// A catalog write's value, or its refusal for the client. A diverged catalog
/// is one no log describes, so the process ends on it.
fn zone_or_abort<T>(written: Result<T, ZoneError>, op: &str) -> Result<T, WireFault> {
    match written {
        Ok(v) => Ok(v),
        Err(ZoneError::Refused(e)) => Err(e.into()),
        Err(ZoneError::Diverged(e)) => gnitz_fatal_abort!("{op}: {e}"),
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
    let (base, group) = zone_or_abort(shared.cat_mut().reserve_user_sequence(seq.tid, count), "serial-range")?;
    shared.disp().emit_zone(&mut excl, vec![group], "serial-range")?;
    Ok(base)
}
