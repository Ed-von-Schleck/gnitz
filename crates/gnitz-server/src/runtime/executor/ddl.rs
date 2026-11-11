//! The catalog-zone write path: one protocol, two entry points.
//! [`handle_ddl_txn`] ingests a client bundle of system-table families;
//! [`reserve_serial_range`] advances one sequence. Both take the SAL lock and
//! emit the zone while the catalog applies it; only the DDL awaits the zone's
//! fsync.
//!
//! A DDL bundle additionally runs under [`DdlLocks`]. The serial path needs
//! none of that: a `sys_sequences` advance has no DAG evaluation, and the row
//! it logs is one no worker reads.

use super::validate::{self, UniqueFilter};
use super::{decode_fault, park_until, CatalogLock, Shared};
use crate::catalog::{AppliedDdl, DdlBundle, SysFamily};
use crate::runtime::sal::Read;
use crate::runtime::sync::WriteGuard;
use gnitz_foundation::fault::Seam;
use gnitz_wire::{ClientVerb, DecodedControl, PkColList, Target, WireFault};
use gnitz_zset::repr::Batch;

/// What a DDL body runs under: the catalog write, and the tick gate's write, so
/// no tick is in flight, every broadcast DdlSync lands between epochs, and no
/// checkpoint, which holds the gate from its base round through its ephemeral
/// round, runs inside the DDL.
struct DdlLocks {
    catalog: WriteGuard<CatalogLock>,
    ticks: WriteGuard,
}

/// Take [`DdlLocks`].
async fn enter_ddl(shared: &Shared) -> DdlLocks {
    // A checkpoint already queued, or due before the writes queued with this
    // settle, finishes before the catalog write holds off readers.
    shared.committer.settle().await;
    let catalog = shared.catalog_rwlock.write().await;
    // Every writer holds a catalog read until its fdatasync, so none is queued:
    // a checkpoint due now runs, and the DDL's groups start on a log below the
    // threshold. Ahead of the gate, which that checkpoint takes.
    shared.committer.settle().await;
    let ticks = shared.ticks.gate.write().await;
    DdlLocks { catalog, ticks }
}

/// `GNITZ_INJECT_DDL_HOLD_FOR_CHECKPOINT`: hold the first DDL, its zone written
/// and its locks still held, until a task parks on the tick gate it holds.
/// Below a checkpoint threshold of one zone, and with no tick request
/// outstanding, that task is the committer, inside the checkpoint the
/// watchdog's reclaim wake called for. One-shot.
static DDL_HOLD_FOR_CHECKPOINT: Seam = Seam::new("GNITZ_INJECT_DDL_HOLD_FOR_CHECKPOINT");

/// Poll bound for [`DDL_HOLD_FOR_CHECKPOINT`], in 1 ms ticks.
const DDL_HOLD_MAX_POLLS: u32 = 10_000;

/// Decode one `DDL_TXN` item's block, in `frame`, under its family's own schema.
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
    let items = gnitz_wire::txn_frame::decode_items(body, 0..body.len(), ClientVerb::DdlTxn).map_err(decode_fault)?;
    let mut families = DdlBundle::default();
    for ctrl in items {
        decode_sys_family(body, ctrl)
            .and_then(|(family, batch)| families.insert(family, batch))
            .map_err(|e| format!("DDL_TXN: {e}"))?;
    }
    Ok(families)
}

/// DDL_TXN: apply a bundle of system-family batches as one durable SAL zone,
/// under [`DdlLocks`]; a refused bundle leaves the catalog as it was. Returns
/// the zone LSN.
///
/// The bundle is applied and its zone laid out under one hold of the SAL lock,
/// with no await between: a bundle the catalog or the log refuses is negated in
/// master memory there, so no system flush sees a row no zone carries.
pub(super) async fn handle_ddl_txn(shared: &Shared, body: &[u8]) -> Result<u64, WireFault> {
    // Decoded up front, before any lock.
    let families = decode_bundle(body)?;
    let unique_creates = families.unique_index_creates();

    let locks = enter_ddl(shared).await;
    let committed = commit_bundle(shared, families, &unique_creates).await;
    if !unique_creates.is_empty() {
        release_preflight(shared).await;
    }
    let Committed { applied, zone_lsn, filter_seeds } = committed?;

    for &id in &applied.dropped_relations {
        shared.forget_relation(&locks.catalog, id);
    }
    for &(owner_id, cols) in &applied.dropped_indexes {
        shared.writes.checks.unique_filter_remove(owner_id, cols);
    }

    // After the fsync, which aborts on failure: a seed is published only for an
    // index that committed.
    for (owner_id, cols, filter) in filter_seeds {
        shared.writes.checks.unique_filter_seed(owner_id, cols, filter);
    }

    // Populate every new view. A post-fsync Err cannot be rolled back (the CREATE
    // is durable), so abort — restart's boot rebuild refills it.
    if let Err(e) = shared
        .dispatcher
        .backfill_views_in_dep_order(&applied.created_views)
        .await
    {
        gnitz_fatal_abort!("live CREATE VIEW backfill failed after the CREATE was made durable: {e}");
    }

    // After the backfill: a new view has read the sealed families, and these
    // rows reach the views that already scanned them.
    if let Err(e) = shared.dispatcher.drain_tick(&applied.held_above).await {
        gnitz_fatal_abort!("catalog tick failed after the DDL was made durable: {e}");
    }
    // Any DDL may change what a parked sync reads: a view dropped, a source's
    // token reset, catalog rows a view scans.
    shared.feeds.wake_all();

    if DDL_HOLD_FOR_CHECKPOINT.take_once() {
        park_until(shared, DDL_HOLD_MAX_POLLS, "ddl hold", || locks.ticks.contended()).await;
    }
    drop(locks);
    Ok(zone_lsn)
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
    shared: &Shared,
    families: DdlBundle,
    unique_creates: &[(u64, PkColList)],
) -> Result<Committed, WireFault> {
    // Pre-flight global uniqueness for every unique secondary index in this
    // bundle before opening the zone or mutating the catalog, so a
    // violation needs no rollback.
    let mut filter_seeds = Vec::new();
    for &(owner_id, cols) in unique_creates {
        // `None`: the index covers the owner's PK, so nothing will consult a filter.
        if let Some(filter) = validate::validate_unique_index_create(&shared.dispatcher, owner_id, cols).await? {
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
    let (applied, zone_lsn) = {
        let mut excl = shared.dispatcher.sal().lock().await;
        shared
            .dispatcher
            .emit_zone(&mut excl, |cat, sink| cat.apply_bundle(families, sink))?
    };
    shared.dispatcher.sal().durable_through(zone_lsn).await;
    Ok(Committed { applied, zone_lsn, filter_seeds })
}

/// Release the pre-flight's index claims, committed or not, while the DDL still
/// holds the tick gate. The release follows the zone in the log, so a
/// worker enters the catalog's claim on an index the DDL created before it drops
/// the pre-flight's. A claim left standing would keep an index no catalog row
/// owns, whose directory the next checkpoint's sweep removes from under it.
async fn release_preflight(shared: &Shared) {
    let released = shared.dispatcher.command(Read::ReleasePreflight).await;
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
pub(super) async fn reserve_serial_range(shared: &Shared, seq: Target, count: u64) -> Result<u64, WireFault> {
    // A read guard holds the token and the sequence's table against a DDL; the
    // SAL lock orders reservations among themselves, and everything under it is
    // synchronous.
    let _read = shared.catalog_rwlock.read().await;
    shared.cat().check_token(seq)?;

    let mut excl = shared.dispatcher.sal().lock().await;
    let (base, _) = shared
        .dispatcher
        .emit_zone(&mut excl, |cat, sink| cat.reserve_user_sequence(seq.tid, count, sink))?;
    Ok(base)
}
