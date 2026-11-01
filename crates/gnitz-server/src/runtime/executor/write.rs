//! The write path: PUSH and PUSH_TXN decoded off the catalog guard, admitted
//! under it, serialized per table, checked for OCC conflicts, and committed
//! through the committer.

use std::cell::RefCell;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use gnitz_foundation::fault::Seam;

use super::validate::{Checks, TxnFamily};
use super::{decode_fault, not_found, park_until, target_kind, Access, CatalogLock, Shared};
use crate::catalog::CatalogEngine;
use crate::runtime::reactor::RecvBuf;
use crate::runtime::sync::{AsyncRwLock, ReadGuard, WriteGuard};
use gnitz_store::relation::RelationKind;
use gnitz_wire::{ClientVerb, DecodedControl, Target, WireConflictMode, WireFault, WireStatus};
use gnitz_zset::repr::Batch;

/// `GNITZ_INJECT_PUSH_HOLD_FOR_DDL`: see `hold_push_for_ddl`.
static PUSH_HOLD_FOR_DDL: Seam = Seam::new("GNITZ_INJECT_PUSH_HOLD_FOR_DDL");

/// Poll bound for [`PUSH_HOLD_FOR_DDL`], in 1 ms ticks.
const PUSH_HOLD_MAX_POLLS: u32 = 2_000;

/// Hold one decoded push, before its catalog read lock, until a DDL has replaced
/// the schema record its decode matched — the window a queued `ALTER TABLE`
/// writer opens between that match and the gate.
async fn hold_push_for_ddl(shared: &Shared, push: &DecodedPush) {
    park_until(shared, PUSH_HOLD_MAX_POLLS, "push hold", || {
        shared
            .cat()
            .wire_target(push.target.tid)
            .is_none_or(|(_, now)| !Rc::ptr_eq(&push.seen, now))
    })
    .await
}

/// The per-table write state: the table locks, the OCC commit-LSN map, and the
/// validation caches.
pub(super) struct Writes {
    /// Per-table write serialization. A push whose validation reads committed
    /// state (`push_reads_committed_state`) and every transaction take the write
    /// guard; a push that reads no committed state takes the read guard, so
    /// same-table pushes reach the committer concurrently and share one fsync.
    table_locks: RefCell<FxHashMap<u64, AsyncRwLock>>,
    /// OCC per-table commit-LSN map: `tid → the zone LSN its last accepted write
    /// this boot rode`, raised by [`Writes::committed`]. A missing entry reads
    /// as `boot_seed`.
    table_commit_lsn: RefCell<FxHashMap<u64, u64>>,
    /// The SAL watermark when the executor started: every read watermark this
    /// boot is at or above it, and every commit's zone above it.
    boot_seed: u64,
    /// The caches write validation probes less through; see
    /// [`Checks::committed`].
    pub(super) checks: Checks,
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
            checks: Checks::new(),
        }
    }

    fn table_lock(&self, tid: u64) -> AsyncRwLock {
        self.table_locks.borrow_mut().entry(tid).or_default().clone()
    }

    /// The read guard on `tid`, taken under the catalog read guard.
    async fn lock_table_shared(&self, _catalog: &ReadGuard<CatalogLock>, tid: u64) -> HeldTables {
        let guard = self.table_lock(tid).read().await;
        HeldTables::Shared { tid, _guard: guard }
    }

    /// The write guard on every table in `tids`, taken in ascending order so a
    /// child INSERT and a parent DELETE cannot deadlock on the same set, and each
    /// once, since re-guarding a lock this task holds would hang forever.
    ///
    /// Owned, because the set is read out of the catalog and this loop awaits.
    async fn lock_tables_exclusive(&self, _catalog: &ReadGuard<CatalogLock>, mut tids: Vec<u64>) -> HeldTables {
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

    /// Record a committed write of `batch` to `tid` at commit LSN `lsn`: the
    /// validation caches, then `tid`'s commit LSN, raised to `lsn`. The
    /// committer calls it before it answers the writer, which still holds its
    /// table locks, so an OCC check of a later write to `tid` sees the raise.
    pub(super) fn committed(&self, cat: &CatalogEngine, tid: u64, batch: &Batch, lsn: u64) {
        self.checks.committed(cat, tid, batch);
        let mut map = self.table_commit_lsn.borrow_mut();
        let e = map.entry(tid).or_default();
        *e = (*e).max(lsn);
    }

    /// Drop relation `id`'s table lock, commit LSN and validation caches. The
    /// caller proves no task holds or awaits the lock; see
    /// [`Shared::forget_relation`].
    pub(super) fn forget(&self, id: u64) {
        self.table_locks.borrow_mut().remove(&id);
        self.table_commit_lsn.borrow_mut().remove(&id);
        self.checks.forget(id);
    }
}

impl Shared {
    /// Validate `families` as one bundle, and commit it with the families its
    /// deletes cascade to as one write. The committer raises each written
    /// table's commit LSN to the write's.
    async fn commit(
        &self,
        _catalog: &ReadGuard<CatalogLock>,
        held: &HeldTables,
        mut families: Vec<TxnFamily>,
    ) -> Result<u64, WireFault> {
        debug_assert!(
            families
                .iter()
                .all(|f| held.holds_exclusive(f.tid) || !self.cat().push_reads_committed_state(f.tid, f.mode)),
            "a write whose validation reads committed state holds its table exclusively"
        );
        self.writes.checks.validate_txn(&self.dispatcher, &mut families).await?;
        assert!(
            families.iter().all(|f| held.holds(f.tid)),
            "a commit bumps only tables whose lock it holds"
        );
        self.committer.write(families).await
    }
}

/// A push frame decoded off the catalog guard by [`decode_push_frame`]. Its batch
/// is handed out only by [`DecodedPush::admit`], which re-judges it under the
/// guard.
pub(super) struct DecodedPush {
    target: Target,
    /// The push's conflict mode, from its own control header.
    mode: WireConflictMode,
    batch: Option<Batch>,
    /// The target's schema record the frame's record matched.
    seen: Rc<[u8]>,
}

impl DecodedPush {
    /// Re-judge the push under the catalog guard, as `access`: its token still
    /// names a target that serves it, and its record still lays out the
    /// target's columns. A push to a stream must also pass the stream rule
    /// ([`check_stream_push`]), an empty one included. Answers the family, `None`
    /// for an empty delta.
    pub(super) fn admit(self, cat: &CatalogEngine, access: Access) -> Result<Option<TxnFamily>, WireFault> {
        let kind = target_kind(cat, self.target, access)?;
        let tid = self.target.tid;
        if self.batch.is_some() {
            cat.recheck_record(tid, &self.seen)?;
        }
        if kind == RelationKind::Stream {
            check_stream_push(tid, self.batch.as_ref(), self.mode)?;
        }
        Ok(self.batch.map(|batch| TxnFamily { tid, mode: self.mode, batch }))
    }
}

/// Decode a push frame against the catalog: its token must still name its
/// target, and its record must lay out its target's columns. Runs off the
/// catalog guard, since the lock is writer-preferring and a bulk load's decode is
/// long; [`DecodedPush::admit`] re-judges it under the guard.
pub(super) fn decode_push_frame(
    cat: &CatalogEngine,
    data: &[u8],
    ctrl: DecodedControl,
) -> Result<DecodedPush, WireFault> {
    let target = ctrl.hdr.target();
    let mode = ctrl.hdr.flags.conflict_mode;
    cat.check_token(target)?;
    let (rel, record) = cat.wire_target(target.tid).ok_or_else(|| not_found(target.tid))?;
    let schema = rel.schema();
    match &ctrl.schema {
        Some(r) => gnitz_wire::schema_block::check_same_types(&data[r.clone()], record).map_err(decode_fault)?,
        None if ctrl.data.is_some() => return Err(decode_fault("a data block without a schema block")),
        None => {}
    }
    let batch = ctrl
        .data
        .map(|r| Batch::decode_foreign_wal_block(&data[r], &schema))
        .transpose()
        .map_err(decode_fault)?;
    Ok(DecodedPush {
        target,
        mode,
        batch,
        seen: record.clone(),
    })
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
pub(super) async fn handle_push(shared: &Shared, buf: RecvBuf, ctrl: DecodedControl) -> Result<u64, WireFault> {
    let (decoded, _charge) = buf.decode(|data| decode_push_frame(&shared.cat(), data, ctrl));
    let push = decoded?;
    if PUSH_HOLD_FOR_DDL.take_once() {
        hold_push_for_ddl(shared, &push).await;
    }
    let catalog = shared.catalog_rwlock.read().await;
    let Some(family) = push.admit(&shared.cat(), Access::Write)? else {
        return Ok(0);
    };

    // See `Writes::table_locks`. The exclusive set covers the tables this
    // push's deletes cascade to.
    let held = if shared.cat().push_reads_committed_state(family.tid, family.mode) {
        let deletes = !family.batch.all_weights_positive();
        let mut locks = Vec::with_capacity(4);
        shared.cat().write_lock_set_into(&mut locks, family.tid, deletes);
        shared.writes.lock_tables_exclusive(&catalog, locks).await
    } else {
        shared.writes.lock_table_shared(&catalog, family.tid).await
    };
    shared.commit(&catalog, &held, vec![family]).await
}

/// PUSH_TXN: validate the bundle under the catalog read lock and its tables'
/// lock union — the plain-push arm's order — check OCC, and commit it as one
/// zone. Every rejection is pre-SAL, so a fault or a conflict commits nothing.
pub(super) async fn handle_push_txn(shared: &Shared, buf: RecvBuf, ctrl: DecodedControl) -> Result<u64, WireFault> {
    // 1. Decode.
    let (decoded, _charge) = buf.decode(|data| decode_push_txn_frame(&shared.cat(), &data[ctrl.body]));
    let items = decoded?;

    // 2. Catalog read lock (excludes a concurrent DROP/DDL), then each family
    //    admitted under it.
    let catalog = shared.catalog_rwlock.read().await;
    let mut bases = Vec::with_capacity(items.len());
    let mut families = Vec::with_capacity(items.len());
    for item in items {
        let (basis, family) = item.admit(&shared.cat())?;
        bases.push(basis);
        families.push(family);
    }

    // 3. Acquire the per-table lock union ⋃ write_lock_set_into(tid) exclusively.
    let mut union = Vec::with_capacity(4 * families.len());
    for f in &families {
        shared
            .cat()
            .write_lock_set_into(&mut union, f.tid, !f.batch.all_weights_positive());
    }
    let held = shared.writes.lock_tables_exclusive(&catalog, union).await;

    // 3b. OCC. A blind family's `BLIND` basis no commit exceeds.
    if let Some(f) = (families.iter().zip(&bases))
        .find_map(|(f, &basis)| shared.writes.written_since(&held, f.tid, basis).then_some(f))
    {
        return Err(WireFault {
            status: WireStatus::TxnConflict,
            text: format!(
                "transaction conflict on '{}': it was written after the read this write was built from; retry",
                shared.cat().qualified_name(f.tid)
            ),
        });
    }

    // 4. Distributed bundle validation, and the commit.
    shared.commit(&catalog, &held, families).await
}

/// One `PUSH_TXN` family as decoded: the push and the read basis its OCC check
/// compares against.
pub(super) struct TxnItem {
    push: DecodedPush,
    basis: u64,
}

impl TxnItem {
    /// [`DecodedPush::admit`] as a transaction write: the family's basis and
    /// the family.
    pub(super) fn admit(self, cat: &CatalogEngine) -> Result<(u64, TxnFamily), WireFault> {
        let family = self.push.admit(cat, Access::TxnWrite)?;
        Ok((self.basis, family.expect("a PUSH_TXN item carries a data block")))
    }
}

/// Decode a `PUSH_TXN` frame body into its families, in frame order, applying
/// every rule that needs no catalog lock.
pub(super) fn decode_push_txn_frame(cat: &CatalogEngine, body: &[u8]) -> Result<Vec<TxnItem>, WireFault> {
    let items = gnitz_wire::txn_frame::decode_items(body, ClientVerb::PushTxn).map_err(decode_fault)?;
    items
        .into_iter()
        .map(|(frame, ctrl)| {
            let (basis, tid) = (ctrl.hdr.arg0, ctrl.hdr.target_id);
            let push =
                decode_push_frame(cat, frame, ctrl).map_err(|e| e.in_context(format_args!("TXN family {tid}")))?;
            Ok(TxnItem { push, basis })
        })
        .collect()
}

/// Refuse a push a stream cannot accept; `batch` is `None` for an empty delta.
/// Both rules restate what a stream lacks — a unique primary key, and any
/// retraction at all — and rejecting `Error` mode is what keeps
/// `push_reads_committed_state` false, and with it the shared table lock and a
/// validator that would probe a stream's absent store.
fn check_stream_push(target_id: u64, batch: Option<&Batch>, mode: WireConflictMode) -> Result<(), String> {
    if mode == WireConflictMode::Error {
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
