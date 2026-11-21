//! The write path: PUSH and PUSH_TXN judged under the catalog read guard,
//! serialized per table, checked for OCC conflicts, and committed through the
//! committer; and the frames of a write spanning several, held until its last
//! arrives ([`OpenWrite`]).

use std::cell::RefCell;
use std::ops::Range;
use std::rc::Rc;

use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use super::family::{FamilyRows, PushedBlock, TxnFamily};
use super::validate::Checks;
use super::{Access, CatalogLock, Shared, decode_fault, target_kind};
use crate::catalog::{CatalogEngine, LockSet};
use crate::runtime::reactor::{Budget, Charge, RecvBuf};
use crate::runtime::sync::{AsyncRwLock, ReadGuard, WriteGuard};
use gnitz_store::relation::RelationKind;
use gnitz_wire::multi_frame::BLIND;
use gnitz_wire::{ClientVerb, DecodedControl, WireConflictMode, WireFault, WireStatus};
use gnitz_zset::repr::MemBatch;

/// One table's write-serialization state, from the first write that locks it
/// this boot until a durable DROP forgets it.
#[derive(Default)]
struct TableWrites {
    /// Write serialization. A write with an OCC check, or whose validation
    /// reads committed state (`push_reads_committed_state`), takes the write
    /// guard of every table it writes and of those its deletes cascade to; any
    /// other write — a push, or a transaction of blind pushes — takes the read
    /// guard of every table it writes, so such writes reach the committer
    /// concurrently and share one fsync.
    lock: AsyncRwLock,
    /// The zone LSN the table's last accepted write this boot rode, raised by
    /// [`Writes::committed`]; 0 until then, and for a stream, whose writes ride
    /// no zone. OCC reads it floored at `boot_seed`.
    commit_lsn: u64,
}

/// The write state: each written table's lock and commit LSN, and the
/// validation caches.
pub(super) struct Writes {
    tables: RefCell<FxHashMap<u64, TableWrites>>,
    /// The SAL watermark when the executor started: every read watermark this
    /// boot is at or above it, and every commit's zone above it.
    boot_seed: u64,
    /// The caches write validation probes less through.
    pub(super) checks: Checks,
}

/// The table locks one write holds: each table's id and its guard, the write
/// guard or the read guard. The committer drops them once the write is laid out.
#[derive(Default)]
pub(super) struct HeldTables {
    guards: SmallVec<[(u64, TableGuard); 4]>,
}

/// One table lock's guard.
enum TableGuard {
    Read { guard: ReadGuard },
    Write { guard: WriteGuard },
}

impl HeldTables {
    fn holds(&self, tid: u64) -> bool {
        self.guards.iter().any(|(t, _)| *t == tid)
    }

    fn holds_exclusive(&self, tid: u64) -> bool {
        self.guards
            .iter()
            .any(|(t, g)| *t == tid && matches!(g, TableGuard::Write { .. }))
    }

    /// Whether another write is parked on one of these locks.
    pub(super) fn contended(&self) -> bool {
        self.guards.iter().any(|(_, g)| match g {
            TableGuard::Read { guard } => guard.contended(),
            TableGuard::Write { guard } => guard.contended(),
        })
    }
}

impl Writes {
    /// No table locked or written yet this boot, which started at `boot_seed`.
    pub(super) fn new(boot_seed: u64) -> Writes {
        Writes {
            tables: RefCell::default(),
            boot_seed,
            checks: Checks::new(),
        }
    }

    fn table_lock(&self, tid: u64) -> AsyncRwLock {
        self.tables.borrow_mut().entry(tid).or_default().lock.clone()
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
            let guard = match exclusive {
                true => TableGuard::Write { guard: lock.write().await },
                false => TableGuard::Read { guard: lock.read().await },
            };
            guards.push((tid, guard));
        }
        HeldTables { guards }
    }

    /// OCC: whether `tid` committed a write after `basis`. `held`'s write guard on
    /// `tid` keeps any other commit to it from landing before the caller's. A
    /// table not written this boot reads as written at `boot_seed`: no basis
    /// read this boot is below it.
    fn written_since(&self, held: &HeldTables, tid: u64, basis: u64) -> bool {
        assert!(held.holds_exclusive(tid), "an OCC check holds its table's write guard");
        // `lock_tables` made the record, and the caller's catalog read guard keeps it.
        self.tables.borrow()[&tid].commit_lsn.max(self.boot_seed) > basis
    }

    /// Record a committed write of `batch` to `tid` at commit LSN `lsn`: its
    /// FK presence cache, then `tid`'s commit LSN, raised to `lsn`. The
    /// committer calls it before it drops the writer's table locks, so an OCC
    /// check of a later write to `tid` sees the raise.
    pub(super) fn committed(&self, tid: u64, batch: MemBatch<'_>, lsn: u64) {
        self.checks.committed(tid, batch);
        let mut tables = self.tables.borrow_mut();
        let t = tables.entry(tid).or_default();
        t.commit_lsn = t.commit_lsn.max(lsn);
    }

    /// Drop relation `id`'s write state and validation caches. The caller
    /// proves no task holds or awaits its lock; see
    /// [`Shared::forget_relation`].
    pub(super) fn forget(&self, id: u64) {
        self.tables.borrow_mut().remove(&id);
        self.checks.forget(id);
    }
}

/// A non-empty push [`admit_push`] judged against the catalog;
/// [`AdmittedPush::family`] hands its rows out.
pub(super) struct AdmittedPush {
    tid: u64,
    /// The push's conflict mode, from its own control header.
    mode: WireConflictMode,
    block: PushedBlock,
}

impl AdmittedPush {
    /// The family, its rows read where they lie in `frame`, the frame the push
    /// was admitted from.
    pub(super) fn family(self, frame: &Rc<RecvBuf>) -> TxnFamily {
        TxnFamily::new(self.tid, self.mode, FamilyRows::Frame(Rc::clone(frame), self.block))
    }
}

/// Judge the push frame `data` against the catalog, as `access`: its token must
/// name a target that serves it, and the shape record in its blob must lay out
/// the target's columns. Its data block is certified where it lies, and the
/// answer holds the block's range in `data`; `None` for an empty delta. A push
/// to a stream must also pass the stream rule ([`check_stream_push`]), an empty
/// one included, which names a row by its index in the write: `first_row` and
/// up.
///
/// A handler calls this under the catalog read guard and holds the guard
/// through its commit, so no DDL lands between the judgement and the write.
pub(super) fn admit_push(
    cat: &CatalogEngine,
    data: &[u8],
    ctrl: DecodedControl,
    access: Access,
    first_row: usize,
) -> Result<Option<AdmittedPush>, WireFault> {
    let target = ctrl.hdr.target();
    let (tid, mode) = (target.tid, ctrl.hdr.flags.conflict_mode);
    let kind = target_kind(cat, target, access)?;
    let (rel, record) = cat.shape_target(tid).expect("target_kind found the target");
    gnitz_wire::schema_record::check_shape(&data[ctrl.blob.clone()], record).map_err(decode_fault)?;
    let block = ctrl
        .data
        .map(|r| PushedBlock::certify(data, r, rel.schema()))
        .transpose()
        .map_err(decode_fault)?;
    if kind == RelationKind::Stream {
        check_stream_push(tid, block.as_ref().map(|b| b.view(data)), mode, first_row)?;
    }
    Ok(block.map(|block| AdmittedPush { tid, mode, block }))
}

/// A write's request frames in arrival order, each with its peeked control:
/// one, or every frame of a write spanning several.
pub(super) type WriteFrames = SmallVec<[(RecvBuf, DecodedControl); 1]>;

/// The frames of a `PUSH` or `PUSH_TXN` spanning several whose last frame has
/// not yet arrived. They arrive back to back: the connection takes no other
/// request until the last.
pub(super) struct OpenWrite {
    /// Each frame so far with its charge to the budget every connection's
    /// open writes share; or why the write is refused, its frames dropped from
    /// then on as they arrive, and its last answered with this.
    held: Result<Vec<(RecvBuf, DecodedControl, Charge)>, WireFault>,
}

impl OpenWrite {
    pub(super) fn new() -> Self {
        OpenWrite { held: Ok(Vec::new()) }
    }

    /// Hold the next frame of the write, charged to `spans`; or refuse the
    /// write: a frame of its verb that does not decode, or one `spans` has no
    /// room for. Whether more frames of the write follow.
    pub(super) fn take(&mut self, buf: RecvBuf, ctrl: DecodedControl, spans: &Rc<Budget>) -> bool {
        let more = ctrl.hdr.flags.continuation;
        let Ok(held) = &mut self.held else { return more };
        let why = match ctrl.client_verb() {
            Err(e) => decode_fault(e),
            Ok(_) => match spans.charge(buf.as_slice().len()) {
                Some(charge) => {
                    held.push((buf, ctrl, charge));
                    return more;
                }
                None => format!(
                    "the writes over several frames would hold more than the server's {}-byte limit \
                     together; nothing was written",
                    spans.cap()
                )
                .into(),
            },
        };
        self.held = Err(why);
        more
    }

    /// The answer to a write whose frames a request of another verb, or one
    /// that does not decode, broke off.
    pub(super) fn broken_off() -> WireFault {
        "a write's frames were broken off by another request; nothing was written, and the \
         connection is closed"
            .into()
    }

    /// The write's frames, its last one taken, their charges released; or why
    /// it was refused.
    pub(super) fn finish(self) -> Result<WriteFrames, WireFault> {
        Ok(self.held?.into_iter().map(|(buf, ctrl, _)| (buf, ctrl)).collect())
    }
}

/// Handle a client write, a `verb` of `frames` in arrival order: admit every
/// frame under the catalog read lock and [`commit`] every family as one zone.
/// Each family reads its rows where they lie in the frame that carried it.
/// `followed`: the client's connection has a request queued behind this one.
///
/// An empty `PUSH` — no frame with a data block — is ACKed with the "nothing
/// written" LSN `0`, but only after the same existence, writability and
/// stream-mode gates a non-empty push passes, so a client bug that happens to
/// produce an empty batch (a `delete` with an empty pk list) fails the way a
/// non-empty one would instead of being masked by a no-op ACK.
pub(super) async fn handle_write(
    shared: &Shared,
    verb: ClientVerb,
    frames: WriteFrames,
    followed: bool,
) -> Result<u64, WireFault> {
    let catalog = shared.catalog_rwlock.read().await;
    let (families, occ) = admit_write(&shared.cat(), verb, frames)?;
    if verb == ClientVerb::Push && families.is_empty() {
        return Ok(0);
    }
    commit(shared, &catalog, families, &occ, followed).await
}

/// A write's families in frame order, and its OCC checks: each a table and
/// the read basis its family was built from.
type AdmittedWrite = (Vec<TxnFamily>, Vec<(u64, u64)>);

/// The families of a `verb` write of `frames`, in frame order; and the OCC
/// checks of a `PUSH_TXN`'s. The frames of a `PUSH` name one relation, token
/// and conflict mode, and its stream rule names a row by its index in the
/// whole write.
pub(super) fn admit_write(
    cat: &CatalogEngine,
    verb: ClientVerb,
    frames: WriteFrames,
) -> Result<AdmittedWrite, WireFault> {
    let (mut families, mut occ) = (Vec::new(), Vec::new());
    let first = frames[0].1.hdr;
    let mut rows = 0;
    for (buf, ctrl) in frames {
        // The families hold the frame, so it is freed once they are laid out.
        let frame = Rc::new(buf);
        if verb == ClientVerb::PushTxn {
            for (basis, push) in admit_push_txn(cat, frame.as_slice(), ctrl.body)? {
                let family = push.family(&frame);
                // A blind family's basis is one no commit exceeds: there is nothing to check.
                if basis != BLIND {
                    occ.push((family.tid, basis));
                }
                families.push(family);
            }
            continue;
        }
        let h = &ctrl.hdr;
        if h.target() != first.target() || h.flags.conflict_mode != first.flags.conflict_mode {
            return Err(decode_fault(
                "the frames of a push name one relation, token and conflict mode",
            ));
        }
        if let Some(push) = admit_push(cat, frame.as_slice(), ctrl, Access::Write, rows)? {
            rows += push.block.rows();
            families.push(push.family(&frame));
        }
    }
    Ok((families, occ))
}

/// Commit `families` as one write: the table locks, OCC, the validation of a
/// bundle that reads committed state, and the commit, whose families its
/// deletes cascade to ride along. Such a bundle holds its locks exclusively.
/// Every rejection is pre-SAL, so a fault or a conflict commits nothing.
/// The committer raises each written table's commit LSN to the write's and
/// drops the table locks once the write is laid out, and answers the LSN then:
/// not yet durable, so the caller holds the write's ACK until it is. Workers
/// apply a group once it is published, so from here a read, or a later write's
/// validation, can see the write before a restart would recover it. Nothing
/// laid out is taken back: a failed fdatasync aborts.
///
/// `occ` is a transaction's OCC checks: each a table and the read basis its
/// family was built from. [`Writes::lock_tables`] takes the locks. `followed`
/// is the committer's: see [`Committer::write`](super::committer::Committer::write).
async fn commit(
    shared: &Shared,
    catalog: &ReadGuard<CatalogLock>,
    mut families: Vec<TxnFamily>,
    occ: &[(u64, u64)],
    followed: bool,
) -> Result<u64, WireFault> {
    let (tids, exclusive, reads_committed) = {
        let cat = shared.cat();
        let reads_committed = families.iter().any(|f| cat.push_reads_committed_state(f.tid, f.mode));
        let exclusive = reads_committed || !occ.is_empty();
        let mut tids = LockSet::new();
        for f in &families {
            // A shared write's table has no FK edge, so its set is the table alone.
            let deletes = exclusive && !f.rows.view().all_weights_positive();
            cat.write_lock_set_into(&mut tids, f.tid, deletes);
        }
        (tids, exclusive, reads_committed)
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

    if reads_committed {
        shared
            .writes
            .checks
            .validate_txn(&shared.dispatcher, &mut families)
            .await?;
    }
    assert!(
        families.iter().all(|f| held.holds(f.tid)),
        "a commit bumps only tables whose lock it holds"
    );
    shared.committer.write(families, held, followed).await
}

/// Judge the `PUSH_TXN` frame `frame`, whose item list is at `body`: each item
/// as [`admit_push`] judges a transactional write, in frame order, with its
/// read basis. Each answer holds its data block's range in `frame`.
pub(super) fn admit_push_txn(
    cat: &CatalogEngine,
    frame: &[u8],
    body: Range<usize>,
) -> Result<Vec<(u64, AdmittedPush)>, WireFault> {
    let items = gnitz_wire::multi_frame::decode_items(frame, body, ClientVerb::PushTxn).map_err(decode_fault)?;
    items
        .into_iter()
        .map(|ctrl| {
            let (basis, tid) = (ctrl.hdr.arg0, ctrl.hdr.target_id);
            let push = admit_push(cat, frame, ctrl, Access::TxnWrite, 0)
                .map_err(|e| e.in_context(format_args!("TXN family {tid}")))?;
            Ok((basis, push.expect("a PUSH_TXN item carries a data block")))
        })
        .collect()
}

/// Refuse a push a stream cannot accept; `batch` is `None` for an empty delta,
/// and its rows are rows `first_row` and up of the write.
/// Both rules restate what a stream lacks — a unique primary key, and any
/// retraction at all — and rejecting `Error` mode is what keeps
/// `push_reads_committed_state` false for a stream: its push takes the shared
/// table lock and skips the validator, which would probe a stream's absent
/// store.
fn check_stream_push(
    target_id: u64,
    batch: Option<MemBatch<'_>>,
    mode: WireConflictMode,
    first_row: usize,
) -> Result<(), String> {
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
        "table {target_id} is a stream: a stream is append-only, but row {} of this push carries \
         weight {}",
        first_row + i,
        batch.get_weight(i)
    ))
}

#[cfg(test)]
#[path = "tests/write.rs"]
mod tests;
