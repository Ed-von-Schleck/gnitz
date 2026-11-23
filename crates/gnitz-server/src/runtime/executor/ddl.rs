//! The catalog-zone write path: one protocol, three entry points.
//! [`handle_ddl_txn`] ingests a client bundle of system-table families;
//! [`allocate_ids`] and [`reserve_serial_range`] each advance one sequence.
//! All take the SAL lock and emit the zone while the catalog applies it; only
//! the DDL awaits the zone's fsync.
//!
//! A DDL bundle additionally runs under the catalog write and the apply lock.
//! A sequence advance needs none of that: it has no DAG evaluation, and the
//! row it logs is one no worker reads.

use super::validate;
use super::{Shared, decode_fault, park_until};
use crate::catalog::{DdlBundle, SysFamily, SysFamilyExt};
use crate::runtime::sal::Read;
use gnitz_foundation::fault::Switch;
use gnitz_wire::{ClientVerb, Target, WireFault};
use gnitz_zset::repr::Batch;
use std::ops::Range;

/// `GNITZ_INJECT_DDL_HOLD_FOR_CHECKPOINT`: hold the first DDL, its zone written
/// and its locks still held, until a task parks on the apply lock it holds.
/// Below a checkpoint threshold of one zone, and with no tick request
/// outstanding, that task is the committer, inside the checkpoint the zone's
/// crossing of the threshold woke it for. One-shot.
static DDL_HOLD_FOR_CHECKPOINT: Switch = Switch::new("GNITZ_INJECT_DDL_HOLD_FOR_CHECKPOINT");

/// The `DDL_TXN` item list at `body` of `frame` as one block per family, each
/// family's wal-block slice materialised into an owned batch under its own
/// schema. One slot per family: every list derived from a bundle reads one
/// block of each.
pub(super) fn decode_bundle(frame: &[u8], body: Range<usize>) -> Result<DdlBundle, WireFault> {
    let items = gnitz_wire::multi_frame::decode_items(frame, body, ClientVerb::DdlTxn).map_err(decode_fault)?;
    let mut families = DdlBundle::default();
    for ctrl in items {
        let tid = ctrl.hdr.target_id;
        let family = SysFamily::from_id(tid).ok_or_else(|| format!("DDL_TXN: {tid} is not a system family"))?;
        let block = &frame[ctrl.data.expect("a DDL_TXN item carries a data block")];
        let batch = Batch::decode_foreign_wal_blocks(&[block], family.schema())
            .map_err(|e| format!("DDL_TXN: family {tid} decode error: {e}"))?;
        families.insert(family, batch).map_err(|e| format!("DDL_TXN: {e}"))?;
    }
    Ok(families)
}

/// DDL_TXN: apply a bundle of system-family batches as one durable SAL zone;
/// a refused bundle leaves the catalog as it was. Returns the zone LSN.
///
/// The body runs under the catalog write, and the apply lock, so no tick is in
/// flight, every broadcast DdlSync lands between epochs, and no checkpoint,
/// which holds the lock from its base round through its ephemeral round, runs
/// inside the DDL.
///
/// The bundle is applied and its zone laid out under one hold of the SAL lock,
/// with no await between: a bundle the catalog or the log refuses is negated in
/// master memory there, so no system flush sees a row no zone carries.
pub(super) async fn handle_ddl_txn(shared: &Shared, frame: &[u8], body: Range<usize>) -> Result<u64, WireFault> {
    // Decoded up front, before any lock.
    let families = decode_bundle(frame, body)?;
    let unique_creates = families.unique_index_creates();

    // A checkpoint already queued, or due before the writes queued with this
    // settle, finishes before the catalog write holds off readers.
    shared.committer.settle().await;
    let catalog = shared.catalog_rwlock.write().await;
    // Every writer holds a catalog read until its fdatasync, so none is queued:
    // a checkpoint due now runs, and the DDL's groups start on a log below the
    // threshold. Ahead of the apply lock, which that checkpoint takes.
    shared.committer.settle().await;
    let applies = shared.dispatcher.apply_lock().await;

    // Pre-flight every unique index of the bundle, then apply it and make its
    // zone durable. Every `Err` leaves the catalog as it was.
    let committed = async {
        // Pre-flight global uniqueness for every unique secondary index in this
        // bundle before opening the zone or mutating the catalog, so a
        // violation needs no rollback.
        for &(owner_id, cols) in &unique_creates {
            validate::validate_unique_index_create(&shared.dispatcher, owner_id, cols).await?;
        }
        // Before the views exist: a tick owed now carries rows pushed ahead of
        // them, and a stream's must not reach them.
        if families.get(SysFamily::Circuit).is_some() {
            shared.dispatcher.tick(&applies).await?;
        }
        let mut excl = shared.dispatcher.sal().lock().await;
        let (applied, zone_lsn) = shared
            .dispatcher
            .emit_zone(&mut excl, |cat, sink| cat.apply_bundle(families, sink))?;
        // The families whose groups landed above the cut are owed the tick
        // that follows the backfill, even when the zone dropped every view that
        // scanned them; `applies` keeps every other emit from taking them first.
        for &family in &applied.held_above {
            shared.dispatcher.owe(&excl, family, 0);
        }
        drop(excl);
        shared.dispatcher.sal().durable_through(zone_lsn).await;
        Ok::<_, WireFault>((applied, zone_lsn))
    }
    .await;
    // Release the pre-flight's index claims, committed or not, while the DDL
    // still holds the apply lock. The release follows the zone in the log, so
    // a worker enters the catalog's claim on an index the DDL created before it
    // drops the pre-flight's. A claim left standing would keep an index no
    // catalog row owns, whose directory the next checkpoint's sweep removes
    // from under it.
    if !unique_creates.is_empty()
        && let Err(e) = shared.dispatcher.command(Read::ReleasePreflight).await
    {
        gnitz_fatal_abort!("releasing the unique-index pre-flight's claims failed: {}", e.text);
    }
    let (applied, zone_lsn) = committed?;

    // Every per-relation state a durable DROP reclaims. Every table lock is
    // taken under a catalog read guard, so the catalog write guard held here
    // proves no task holds or awaits a lock being removed.
    for &id in &applied.dropped_relations {
        shared.writes.forget(id);
        shared.dispatcher.forget_relation(id);
    }
    for &(owner_id, cols) in &applied.dropped_indexes {
        shared.writes.checks.unique_filter_remove(owner_id, cols);
    }

    // Populate every new view. A post-fsync Err cannot be rolled back (the CREATE
    // is durable), so abort — restart's boot rebuild refills it.
    if let Err(e) = shared
        .dispatcher
        .backfill_views_in_dep_order(&applies, &applied.created_views)
        .await
    {
        gnitz_fatal_abort!("live CREATE VIEW backfill failed after the CREATE was made durable: {e}");
    }

    // After the backfill: a new view has read the sealed families, and their
    // rows reach the views that already scanned them. The tick takes whatever
    // user commit is pending too: every writer holds a catalog read until its
    // fdatasync, so each of those was laid out before this DDL.
    if let Err(e) = shared.dispatcher.tick(&applies).await {
        gnitz_fatal_abort!("catalog tick failed after the DDL was made durable: {e}");
    }
    // Any DDL may change what a parked sync reads: a view dropped, a source's
    // token reset, catalog rows a view scans.
    shared.dispatcher.wake_watches();

    if DDL_HOLD_FOR_CHECKPOINT.take_once() {
        park_until(shared, "ddl hold", || applies.contended()).await;
    }
    drop((catalog, applies));
    Ok(zone_lsn)
}

/// Allocate `count` contiguous catalog object ids and return the first. The
/// advance is a DDL SAL zone, which is how boot recovers the counter.
///
/// The ACK waits for no fdatasync, and the allocation takes no catalog lock:
/// the SAL lock orders it among the zones. Nor does it take the apply lock: its
/// one group is of a family no worker holds, so it reaches no drive in flight.
/// A DDL creating one of these ids is durable through its own zone's fdatasync
/// or through a checkpoint, and either makes this zone durable first. An allocation a crash loses was used by no
/// durable row, and a client drops its allocated ids with its connection.
pub(super) async fn allocate_ids(shared: &Shared, count: u64) -> Result<u64, WireFault> {
    let mut excl = shared.dispatcher.sal().lock().await;
    let (base, _) = shared
        .dispatcher
        .emit_zone(&mut excl, |cat, sink| cat.reserve_catalog_ids(count, sink))?;
    Ok(base)
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
