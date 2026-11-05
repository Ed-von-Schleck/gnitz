//! The write path: PUSH and PUSH_TXN decoded off the catalog guard, admitted
//! under it, serialized per table, checked for OCC conflicts, and committed
//! through the committer.

use std::cell::RefCell;
use std::ops::Range;
use std::rc::Rc;

use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use gnitz_foundation::fault::Seam;

use super::validate::{Checks, FamilyRows, PushedBlock, TxnFamily};
use super::{decode_fault, not_found, park_until, target_kind, Access, CatalogLock, Shared};
use crate::catalog::{CatalogEngine, LockSet};
use crate::runtime::reactor::RecvBuf;
use crate::runtime::sync::{AsyncRwLock, ReadGuard, WriteGuard};
use gnitz_store::relation::RelationKind;
use gnitz_wire::txn_frame::BLIND;
use gnitz_wire::{ClientVerb, DecodedControl, Target, WireConflictMode, WireFault, WireStatus};
use gnitz_zset::repr::MemBatch;

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
    /// Per-table write serialization. A write with an OCC check, or whose
    /// validation reads committed state (`push_reads_committed_state`), takes
    /// the write guard of every table it writes and of those its deletes
    /// cascade to; any other write — a push, or a transaction of blind pushes —
    /// takes the read guard of every table it writes, so such writes reach the
    /// committer concurrently and share one fsync.
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

/// The table locks one write holds: a guard on each of `tids`, the write guard
/// when `exclusive`, else the read guard. The committer drops them once the
/// write is laid out.
#[derive(Default)]
pub(super) struct HeldTables {
    tids: LockSet,
    exclusive: bool,
    guards: SmallVec<[TableGuard; 4]>,
}

/// One table lock's guard.
enum TableGuard {
    Read { guard: ReadGuard },
    Write { guard: WriteGuard },
}

impl HeldTables {
    fn holds(&self, tid: u64) -> bool {
        self.tids.contains(&tid)
    }

    fn holds_exclusive(&self, tid: u64) -> bool {
        self.exclusive && self.holds(tid)
    }

    /// Whether another write is parked on one of these locks.
    pub(super) fn contended(&self) -> bool {
        self.guards.iter().any(|g| match g {
            TableGuard::Read { guard } => guard.contended(),
            TableGuard::Write { guard } => guard.contended(),
        })
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

    /// A guard on every table in `tids` — the write guard when `exclusive`, else
    /// the read guard — taken under the catalog read guard in ascending order, so
    /// no two sets deadlock (a child INSERT and a parent DELETE among them), and
    /// each once, since re-guarding a lock this task holds would hang forever.
    async fn lock_tables(&self, _catalog: &ReadGuard<CatalogLock>, mut tids: LockSet, exclusive: bool) -> HeldTables {
        tids.sort_unstable();
        tids.dedup();
        let mut guards = SmallVec::with_capacity(tids.len());
        for &tid in &tids {
            let lock = self.table_lock(tid);
            guards.push(match exclusive {
                true => TableGuard::Write { guard: lock.write().await },
                false => TableGuard::Read { guard: lock.read().await },
            });
        }
        HeldTables { tids, exclusive, guards }
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
    /// committer calls it before it drops the writer's table locks, so an OCC
    /// check of a later write to `tid` sees the raise.
    pub(super) fn committed(&self, cat: &CatalogEngine, tid: u64, batch: MemBatch<'_>, lsn: u64) {
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

/// A push frame decoded off the catalog guard by [`decode_push_frame`]. Its rows
/// are handed out only by [`DecodedPush::admit`], which re-judges them under the
/// guard.
pub(super) struct DecodedPush {
    target: Target,
    /// The push's conflict mode, from its own control header.
    mode: WireConflictMode,
    block: Option<PushedBlock>,
    /// The target's schema record the frame's record matched.
    seen: Rc<[u8]>,
}

impl DecodedPush {
    /// Re-judge the push under the catalog guard, as `access`: its token still
    /// names a target that serves it, and its record still lays out the
    /// target's columns. A push to a stream must also pass the stream rule
    /// ([`check_stream_push`]), an empty one included. Answers the family, its
    /// rows read where they lie in `frame`, the frame the push was decoded
    /// from; `None` for an empty delta.
    pub(super) fn admit(
        self,
        cat: &CatalogEngine,
        access: Access,
        frame: &Rc<RecvBuf>,
    ) -> Result<Option<TxnFamily>, WireFault> {
        let kind = target_kind(cat, self.target, access)?;
        let tid = self.target.tid;
        if self.block.is_some() {
            cat.recheck_record(tid, &self.seen)?;
        }
        let rows = self.block.map(|block| FamilyRows::Frame(Rc::clone(frame), block));
        if kind == RelationKind::Stream {
            check_stream_push(tid, rows.as_ref().map(FamilyRows::view), self.mode)?;
        }
        Ok(rows.map(|rows| TxnFamily { tid, mode: self.mode, rows }))
    }
}

/// Decode the push frame `data` against the catalog: its token must still name
/// its target, and its record must lay out its target's columns. Its data block
/// is certified where it lies, and the answer holds the block's range in
/// `data`. Runs off the catalog guard, since the lock is writer-preferring and a
/// bulk load's decode is long; [`DecodedPush::admit`] re-judges it under the
/// guard.
pub(super) fn decode_push_frame(
    cat: &CatalogEngine,
    data: &mut [u8],
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
    let block = ctrl
        .data
        .map(|r| PushedBlock::certify(data, r, schema))
        .transpose()
        .map_err(decode_fault)?;
    Ok(DecodedPush {
        target,
        mode,
        block,
        seen: record.clone(),
    })
}

/// Handle a client push: decode the frame, then admit it under the catalog read
/// lock and [`commit`] it.
///
/// One handler for both batch shapes. An empty delta — a frame with no data
/// block — is ACKed with the "nothing written" LSN `0`, but only after the same
/// existence, writability and stream-mode gates a non-empty push passes, so a
/// client bug that happens to produce an empty batch (a `delete` with an empty
/// pk list) fails the way a non-empty one would instead of being masked by a
/// no-op ACK.
pub(super) async fn handle_push(shared: &Shared, mut buf: RecvBuf, ctrl: DecodedControl) -> Result<u64, WireFault> {
    let push = decode_push_frame(&shared.cat(), buf.as_mut_slice(), ctrl)?;
    if PUSH_HOLD_FOR_DDL.take_once() {
        hold_push_for_ddl(shared, &push).await;
    }
    let catalog = shared.catalog_rwlock.read().await;
    let Some(family) = push.admit(&shared.cat(), Access::Write, &Rc::new(buf))? else {
        return Ok(0);
    };
    commit(shared, &catalog, vec![family], &[]).await
}

/// PUSH_TXN: decode the bundle, then admit every family under the catalog read
/// lock and [`commit`] them as one zone. Every family reads its rows from the
/// one frame.
pub(super) async fn handle_push_txn(shared: &Shared, mut buf: RecvBuf, ctrl: DecodedControl) -> Result<u64, WireFault> {
    let items = decode_push_txn_frame(&shared.cat(), buf.as_mut_slice(), ctrl.body)?;
    let catalog = shared.catalog_rwlock.read().await;
    let frame = Rc::new(buf);
    let mut occ = Vec::new();
    let mut families = Vec::with_capacity(items.len());
    for (basis, push) in items {
        let family = push.admit(&shared.cat(), Access::TxnWrite, &frame)?;
        let family = family.expect("a PUSH_TXN item carries a data block");
        // A blind family's basis is one no commit exceeds: there is nothing to check.
        if basis != BLIND {
            occ.push((family.tid, basis));
        }
        families.push(family);
    }
    // The families hold the frame, so it is freed once they are laid out.
    drop(frame);
    commit(shared, &catalog, families, &occ).await
}

/// Commit `families` as one write: the table locks, OCC, the bundle's
/// validation, and the commit, whose families its deletes cascade to ride
/// along. Every rejection is pre-SAL, so a fault or a conflict commits nothing.
/// The committer raises each written table's commit LSN to the write's and
/// drops the table locks once the write is laid out; the answer comes at the
/// fdatasync that covers it.
///
/// `occ` is a transaction's OCC checks: each a table and the read basis its
/// family was built from. The locks taken are [`Writes::table_locks`]'.
async fn commit(
    shared: &Shared,
    catalog: &ReadGuard<CatalogLock>,
    mut families: Vec<TxnFamily>,
    occ: &[(u64, u64)],
) -> Result<u64, WireFault> {
    let (tids, exclusive) = {
        let cat = shared.cat();
        let exclusive = !occ.is_empty() || families.iter().any(|f| cat.push_reads_committed_state(f.tid, f.mode));
        let mut tids = LockSet::new();
        for f in &families {
            // A shared write's table has no FK edge, so its set is the table alone.
            let deletes = exclusive && !f.rows.view().all_weights_positive();
            cat.write_lock_set_into(&mut tids, f.tid, deletes);
        }
        (tids, exclusive)
    };
    let held = shared.writes.lock_tables(catalog, tids, exclusive).await;

    let conflict = occ
        .iter()
        .find(|&&(tid, basis)| shared.writes.written_since(&held, tid, basis));
    if let Some(&(tid, _)) = conflict {
        return Err(WireFault {
            status: WireStatus::TxnConflict,
            text: format!(
                "transaction conflict on '{}': it was written after the read this write was built from; retry",
                shared.cat().qualified_name(tid)
            ),
        });
    }

    shared
        .writes
        .checks
        .validate_txn(&shared.dispatcher, &mut families)
        .await?;
    assert!(
        families.iter().all(|f| held.holds(f.tid)),
        "a commit bumps only tables whose lock it holds"
    );
    shared.committer.write(families, held).await
}

/// Decode the `PUSH_TXN` frame `frame`, whose item list is at `body`, into its
/// families, in frame order, each with its read basis, applying every rule that
/// needs no catalog lock. Each answer holds its data block's range in `frame`.
pub(super) fn decode_push_txn_frame(
    cat: &CatalogEngine,
    frame: &mut [u8],
    body: Range<usize>,
) -> Result<Vec<(u64, DecodedPush)>, WireFault> {
    let items = gnitz_wire::txn_frame::decode_items(frame, body, ClientVerb::PushTxn).map_err(decode_fault)?;
    items
        .into_iter()
        .map(|ctrl| {
            let (basis, tid) = (ctrl.hdr.arg0, ctrl.hdr.target_id);
            let push =
                decode_push_frame(cat, frame, ctrl).map_err(|e| e.in_context(format_args!("TXN family {tid}")))?;
            Ok((basis, push))
        })
        .collect()
}

/// Refuse a push a stream cannot accept; `batch` is `None` for an empty delta.
/// Both rules restate what a stream lacks — a unique primary key, and any
/// retraction at all — and rejecting `Error` mode is what keeps
/// `push_reads_committed_state` false, and with it the shared table lock and a
/// validator that would probe a stream's absent store.
fn check_stream_push(target_id: u64, batch: Option<MemBatch<'_>>, mode: WireConflictMode) -> Result<(), String> {
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
