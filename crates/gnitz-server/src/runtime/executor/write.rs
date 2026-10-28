//! The write path: PUSH and PUSH_TXN decoded off the catalog guard, admitted
//! under it, serialized per table, checked for OCC conflicts, and committed
//! through the committer.

use std::cell::RefCell;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use gnitz_foundation::fault::Seam;

use super::peer::Peer;
use super::{not_found, park_until, send_ack, target_kind, Access, Shared};
use crate::catalog::CatalogEngine;
use crate::runtime::master::TxnFamily;
use crate::runtime::reactor::{AsyncRwLock, ReadGuard, RecvBuf, WriteGuard};
use gnitz_store::relation::{Relation, RelationKind};
use gnitz_wire::{ClientVerb, DecodedControl, Target, WireFault, WireStatus};
use gnitz_zset::repr::Batch;

/// `GNITZ_INJECT_PUSH_HOLD_FOR_DDL`: see `hold_push_for_ddl`.
static PUSH_HOLD_FOR_DDL: Seam = Seam::new("GNITZ_INJECT_PUSH_HOLD_FOR_DDL");

/// Poll bound for [`PUSH_HOLD_FOR_DDL`], in 1 ms ticks.
const PUSH_HOLD_MAX_POLLS: u32 = 2_000;

/// Hold one decoded push, before its catalog read lock, until a DDL has replaced
/// the schema record `seen` its decode matched — the window a queued `ALTER TABLE`
/// writer opens between that match and the gate.
async fn hold_push_for_ddl(shared: &Shared, target_id: u64, seen: &Rc<[u8]>) {
    park_until(shared, PUSH_HOLD_MAX_POLLS, "push hold", || {
        shared
            .cat()
            .schema_record(target_id)
            .is_none_or(|now| !Rc::ptr_eq(seen, &now))
    })
    .await
}

/// The per-table write state: the table locks and the OCC commit-LSN map.
pub(super) struct Writes {
    /// Per-table write serialization. A push whose validation reads committed
    /// state (`push_reads_committed_state`) and every transaction take the write
    /// guard; a push that reads no committed state takes the read guard, so
    /// same-table pushes reach the committer concurrently and share one fsync.
    table_locks: RefCell<FxHashMap<u64, AsyncRwLock>>,
    /// OCC per-table commit-LSN map: `tid → the zone LSN its last accepted write
    /// this boot rode`. A missing entry reads as `boot_seed`.
    /// Single-threaded reactor — a plain `RefCell`, and no borrow is ever held across an
    /// `.await`.
    table_commit_lsn: RefCell<FxHashMap<u64, u64>>,
    /// The SAL watermark when the executor started: every read watermark this
    /// boot is at or above it, and every commit's zone above it.
    boot_seed: u64,
}

/// The table locks one task holds.
enum HeldTables {
    Shared { tid: u64, _guard: ReadGuard },
    Exclusive { tids: Vec<u64>, _guards: Vec<WriteGuard> },
}

impl HeldTables {
    fn holds(&self, tid: u64) -> bool {
        match self {
            Self::Shared { tid: t, .. } => *t == tid,
            Self::Exclusive { tids, .. } => tids.contains(&tid),
        }
    }

    fn holds_exclusive(&self, tid: u64) -> bool {
        matches!(self, Self::Exclusive { tids, .. } if tids.contains(&tid))
    }
}

impl Writes {
    /// No table locked or written yet this boot, which started at `boot_seed`.
    pub(super) fn new(boot_seed: u64) -> Writes {
        Writes {
            table_locks: RefCell::default(),
            table_commit_lsn: RefCell::default(),
            boot_seed,
        }
    }

    fn table_lock(&self, tid: u64) -> AsyncRwLock {
        self.table_locks.borrow_mut().entry(tid).or_default().clone()
    }

    /// The read guard on `tid`.
    async fn lock_table_shared(&self, tid: u64) -> HeldTables {
        let guard = self.table_lock(tid).read().await;
        HeldTables::Shared { tid, _guard: guard }
    }

    /// The write guard on every table in `tids`, taken in ascending order so a
    /// child INSERT and a parent DELETE cannot deadlock on the same set, and each
    /// once, since re-guarding a lock this task holds would hang forever.
    ///
    /// Owned, because the set is read out of the catalog and this loop awaits.
    async fn lock_tables_exclusive(&self, mut tids: Vec<u64>) -> HeldTables {
        tids.sort_unstable();
        tids.dedup();
        let mut guards = Vec::with_capacity(tids.len());
        for &tid in &tids {
            guards.push(self.table_lock(tid).write().await);
        }
        HeldTables::Exclusive { tids, _guards: guards }
    }

    /// OCC: whether `tid` committed a write after `basis`. `held`'s write guard on
    /// `tid` keeps any other commit to it from landing before the caller's. A
    /// table not written this boot reads as written at `boot_seed`: no basis
    /// read this boot is below it.
    fn written_since(&self, held: &HeldTables, tid: u64, basis: u64) -> bool {
        assert!(held.holds_exclusive(tid), "an OCC check holds its table's write guard");
        let committed = self.table_commit_lsn.borrow().get(&tid).copied();
        committed.unwrap_or(self.boot_seed) > basis
    }

    /// Drop relation `id`'s table lock and commit LSN. The caller proves no task
    /// holds or awaits the lock; see [`Shared::forget_relation`].
    pub(super) fn forget(&self, id: u64) {
        self.table_locks.borrow_mut().remove(&id);
        self.table_commit_lsn.borrow_mut().remove(&id);
    }
}

impl Shared {
    /// Validate `families` as one bundle, commit it with the families its
    /// deletes cascade to as one write, and raise each written table's commit
    /// LSN to the write's.
    async fn commit(
        &self,
        _catalog: &ReadGuard,
        held: &HeldTables,
        mut families: Vec<TxnFamily>,
    ) -> Result<u64, WireFault> {
        debug_assert!(
            families
                .iter()
                .all(|f| held.holds_exclusive(f.tid) || !self.cat().push_reads_committed_state(f.tid, f.mode)),
            "a write whose validation reads committed state holds its table exclusively"
        );
        self.dispatcher.validate_txn_distributed(&mut families).await?;
        let tids: Vec<u64> = families.iter().map(|f| f.tid).collect();
        assert!(
            tids.iter().all(|&t| held.holds(t)),
            "a commit bumps only tables whose lock it holds"
        );
        let lsn = self.committer.write(families)?.await?;
        let mut map = self.writes.table_commit_lsn.borrow_mut();
        for tid in tids {
            // Pushes sharing a read guard resume from the await in any order.
            let e = map.entry(tid).or_default();
            *e = (*e).max(lsn);
            // After the raise, which is what a woken poll reads as staleness.
            self.feeds.wake(tid);
        }
        Ok(lsn)
    }
}

/// Decode a push frame against the catalog: its token must still name its target,
/// its record must lay out its target's columns, and the target's record is
/// answered for [`CatalogEngine::recheck_record`]. The token comes first, as in
/// [`target_kind`], which checks it again under the guard.
/// Runs before the catalog read guard, and off it: `AsyncRwLock` is
/// writer-preferring, so a bulk load's multi-megabyte decode under the guard
/// would stall every DDL writer behind it.
pub(super) fn decode_push_frame(
    cat: &CatalogEngine,
    data: &[u8],
    ctrl: DecodedControl,
) -> Result<(Option<Batch>, Rc<[u8]>), WireFault> {
    let tid = ctrl.hdr.target_id;
    cat.check_token(ctrl.hdr.target())?;
    let target = cat.schema_record(tid).ok_or_else(|| not_found(tid))?;
    let schema = cat
        .registry
        .relation(tid)
        .map(Relation::schema)
        .ok_or_else(|| not_found(tid))?;
    match &ctrl.schema {
        Some(r) => gnitz_wire::schema_block::check_same_types(&data[r.clone()], &target)
            .map_err(|e| format!("decode error: {e}"))?,
        None if ctrl.data.is_some() => return Err("decode error: a data block without a schema block".into()),
        None => {}
    }
    let batch = ctrl
        .data
        .map(|r| Batch::decode_foreign_wal_block(&data[r], &schema))
        .transpose()
        .map_err(|e| format!("decode error: {e}"))?;
    Ok((batch, target))
}

/// Handle a client push: decode the frame, then commit it under the catalog read
/// lock and the target's table lock(s).
///
/// One handler for both batch shapes. An empty delta — a frame with no data
/// block — is ACKed with the "nothing written" LSN `0`, but only after the same
/// existence, writability and stream-mode gates a non-empty push passes, so a
/// client bug that happens to produce an empty batch (a `delete` with an empty
/// pk list) fails the way a non-empty one would instead of being masked by a
/// no-op ACK.
pub(super) async fn handle_push(
    shared: &Rc<Shared>,
    peer: &Peer,
    buf: RecvBuf,
    ctrl: DecodedControl,
) -> Result<(), WireFault> {
    let target = ctrl.hdr.target();
    let target_id = target.tid;
    let mode = ctrl.hdr.flags.conflict_mode;

    let (decoded, _charge) = buf.decode(|data| decode_push_frame(shared.cat(), data, ctrl));
    let (batch, seen) = decoded?;
    if PUSH_HOLD_FOR_DDL.take_once() {
        hold_push_for_ddl(shared, target_id, &seen).await;
    }
    let catalog = shared.catalog_rwlock.read().await;
    // Under the catalog guard, not at decode: a dropped id can be re-created as
    // a stream between the two, and the stream rules below read the kind.
    let is_stream = target_kind(shared.cat(), target, Access::Write)? == RelationKind::Stream;
    if is_stream {
        check_stream_push(target_id, batch.as_ref(), mode)?;
    }

    let Some(batch) = batch else {
        send_ack(peer, target_id, 0);
        return Ok(());
    };

    shared.cat().recheck_record(target_id, &seen)?;

    // See `Writes::table_locks`. The exclusive set covers the tables this
    // push's deletes cascade to.
    let families = vec![TxnFamily { tid: target_id, mode, batch }];
    let held = if shared.cat().push_reads_committed_state(target_id, mode) {
        let deletes = !families[0].batch.all_weights_positive();
        let locks = shared.cat().write_lock_set(target_id, deletes);
        shared.writes.lock_tables_exclusive(locks).await
    } else {
        shared.writes.lock_table_shared(target_id).await
    };
    let zone_lsn = shared.commit(&catalog, &held, families).await?;
    // A stream replies `0`: its push is not durable, and an ACK reports an LSN
    // only for a write a restart must recover. Keyed on the target rather than on
    // whether this batch happened to open a zone, so a stream push the committer
    // coalesced with a base-table push still answers `0`.
    let reply_lsn = if is_stream { 0 } else { zone_lsn };
    send_ack(peer, target_id, reply_lsn);
    Ok(())
}

/// PUSH_TXN: validate the bundle under the catalog read lock and its tables'
/// lock union — the plain-push arm's order — check OCC, and commit it as one
/// zone. Every rejection is pre-SAL, so a fault or a conflict commits nothing.
pub(super) async fn handle_push_txn(
    shared: &Rc<Shared>,
    peer: &Peer,
    ctrl: &DecodedControl,
    buf: RecvBuf,
) -> Result<(), WireFault> {
    // 1. Decode.
    let (decoded, _charge) = buf.decode(|data| decode_push_txn_frame(shared.cat(), &data[ctrl.body.clone()]));
    let items = decoded?;

    // 2. Catalog read lock (excludes a concurrent DROP/DDL), then the
    //    catalog-dependent rules per family.
    let catalog = shared.catalog_rwlock.read().await;
    for (head, _) in &items {
        target_kind(shared.cat(), head.target, Access::TxnWrite)?;
        shared.cat().recheck_record(head.target.tid, &head.seen)?;
    }

    // 3. Acquire the per-table lock union ⋃ write_lock_set(tid) exclusively.
    let union = items
        .iter()
        .flat_map(|(_, f)| shared.cat().write_lock_set(f.tid, !f.batch.all_weights_positive()))
        .collect();
    let held = shared.writes.lock_tables_exclusive(union).await;

    // 3b. OCC. A blind family's `BLIND` basis no commit exceeds.
    if let Some((head, _)) = items
        .iter()
        .find(|(h, _)| shared.writes.written_since(&held, h.target.tid, h.basis))
    {
        return Err(WireFault {
            status: WireStatus::TxnConflict,
            text: format!(
                "transaction conflict on '{}': it was written after the read this write was built from; retry",
                shared.cat().qualified_name(head.target.tid)
            ),
        });
    }

    // 4. Distributed bundle validation, and the commit.
    let families = items.into_iter().map(|(_, f)| f).collect();
    let lsn = shared.commit(&catalog, &held, families).await?;
    send_ack(peer, 0, lsn);
    Ok(())
}

/// What the gate and the OCC check read of one `PUSH_TXN` family.
pub(super) struct TxnHead {
    pub(super) target: Target,
    pub(super) basis: u64,
    pub(super) seen: Rc<[u8]>,
}

/// Decode a `PUSH_TXN` frame body into its families, in frame order, each with
/// its head, applying every rule that needs no catalog lock.
pub(super) fn decode_push_txn_frame(cat: &CatalogEngine, body: &[u8]) -> Result<Vec<(TxnHead, TxnFamily)>, WireFault> {
    let items =
        gnitz_wire::txn_frame::decode_items(body, ClientVerb::PushTxn).map_err(|e| format!("decode error: {e}"))?;
    items
        .into_iter()
        .map(|(frame, ctrl)| {
            let (target, mode, basis) = (ctrl.hdr.target(), ctrl.hdr.flags.conflict_mode, ctrl.hdr.arg0);
            let tid = target.tid;
            let (batch, seen) =
                decode_push_frame(cat, frame, ctrl).map_err(|e| e.in_context(format_args!("TXN family {tid}")))?;
            let batch = batch.expect("a PUSH_TXN item carries a data block");
            Ok((TxnHead { target, basis, seen }, TxnFamily { tid, mode, batch }))
        })
        .collect()
}

/// Refuse a push a stream cannot accept; `batch` is `None` for an empty delta.
/// Both rules restate what a stream lacks — a unique primary key, and any
/// retraction at all — and rejecting `Error` mode is what keeps
/// `push_reads_committed_state` false, and with it the shared table lock and a
/// validator that would probe a stream's absent store.
pub(super) fn check_stream_push(
    target_id: u64,
    batch: Option<&Batch>,
    mode: gnitz_wire::WireConflictMode,
) -> Result<(), String> {
    if mode == gnitz_wire::WireConflictMode::Error {
        return Err(format!(
            "table {target_id} is a stream: conflict mode 'error' asserts a primary-key \
             uniqueness a stream does not have"
        ));
    }
    let Some(batch) = batch.filter(|b| !b.all_weights_positive()) else {
        return Ok(());
    };
    // Located again only to name it in the message.
    let i = (0..batch.len())
        .find(|&i| batch.get_weight(i) <= 0)
        .expect("just found one");
    Err(format!(
        "table {target_id} is a stream: a stream is append-only, but row {i} of this push carries \
         weight {}",
        batch.get_weight(i)
    ))
}

#[cfg(test)]
#[path = "tests/write.rs"]
mod tests;
