//! The write path: PUSH and PUSH_TXN judged under the catalog read guard,
//! serialized per table, checked for OCC conflicts, and committed through the
//! committer; and the frames of a write spanning several, held until its last
//! arrives ([`OpenWrite`]).

use std::cell::RefCell;
use std::rc::Rc;

use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use super::family::{FamilyRows, TxnFamily};
use super::validate::Checks;
use super::{Access, Shared, decode_fault, target_kind};
use crate::catalog::CatalogEngine;
use crate::runtime::reactor::{Budget, Charge, RecvBuf};
use crate::runtime::sync::{AsyncRwLock, ReadGuard, WriteGuard};
use gnitz_store::relation::RelationKind;
use gnitz_wire::multi_frame::BLIND;
use gnitz_wire::{ClientVerb, DecodedControl, WireConflictMode, WireFault, WireStatus};
use gnitz_zset::repr::{CertifiedBlock, MemBatch};

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

/// The table locks one write holds: every read guard, or every write guard.
/// The committer drops them once the write is laid out.
pub(super) enum HeldTables {
    Shared(SmallVec<[ReadGuard; 4]>),
    Exclusive(SmallVec<[WriteGuard; 4]>),
}

#[cfg(test)]
impl Default for HeldTables {
    fn default() -> Self {
        HeldTables::Shared(SmallVec::new())
    }
}

impl HeldTables {
    /// Whether another write is parked on one of these locks.
    pub(super) fn contended(&self) -> bool {
        match self {
            HeldTables::Shared(guards) => guards.iter().any(ReadGuard::contended),
            HeldTables::Exclusive(guards) => guards.iter().any(WriteGuard::contended),
        }
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
    /// proves no task holds or awaits its lock.
    pub(super) fn forget(&self, id: u64) {
        self.tables.borrow_mut().remove(&id);
        self.checks.forget(id);
    }
}

/// Judge the push `ctrl` heads in `frame` against the catalog, as `access`: its
/// token must name a target that serves it, and the shape record in its blob
/// must lay out the target's columns. Its data block is certified where it
/// lies, and the family reads its rows there; `None` for an empty delta.
///
/// `stream_rows_from` is `None` for a `PUSH_TXN` item, which may not name a
/// stream: refusing one keeps every family inside the zone, so a transaction
/// always opens one. Otherwise a push to a stream must pass the stream rule
/// ([`check_stream_push`]), an empty one included, which names a row by its
/// index in the write: `stream_rows_from` and up.
fn admit_push(
    cat: &CatalogEngine,
    frame: &Rc<RecvBuf>,
    ctrl: DecodedControl,
    stream_rows_from: Option<usize>,
) -> Result<Option<TxnFamily>, WireFault> {
    let target = ctrl.hdr.target();
    let (tid, mode) = (target.tid, ctrl.hdr.flags.conflict_mode);
    let kind = target_kind(cat, target, Access::Write)?;
    let (rel, record) = cat.shape_target(tid).expect("target_kind found the target");
    let data = frame.as_slice();
    gnitz_wire::schema_record::check_shape(&data[ctrl.blob.clone()], record).map_err(decode_fault)?;
    let schema = rel.schema();
    let family = match ctrl.data {
        None => None,
        Some(r) => {
            let block = CertifiedBlock::certify(data, r, &schema).map_err(decode_fault)?;
            Some(TxnFamily::new(
                tid,
                mode,
                FamilyRows::Frame(Rc::clone(frame), block, schema),
            ))
        }
    };
    if kind == RelationKind::Stream {
        let Some(first_row) = stream_rows_from else {
            return Err(format!("table {tid} is a stream: a stream cannot be written inside a transaction").into());
        };
        check_stream_push(tid, family.as_ref().map(|f| f.rows.view()), mode, first_row)?;
    }
    Ok(family)
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

    /// The write's frames, its last one taken, their charges released; or why
    /// it was refused.
    pub(super) fn finish(self) -> Result<WriteFrames, WireFault> {
        Ok(self.held?.into_iter().map(|(buf, ctrl, _)| (buf, ctrl)).collect())
    }
}

/// Handle a client write, a `verb` of `frames` in arrival order, as one write:
/// every frame admitted under the catalog read guard, which is held through
/// the commit so no DDL lands between the judgement and the write; the table
/// locks and OCC; the validation of a write that reads committed state, whose
/// families its deletes cascade to ride along; and the commit
/// ([`Committer::write`](super::committer::Committer::write)). Such a
/// write, and one with an OCC check, holds its locks exclusively. Every
/// rejection is pre-SAL, so a fault or a conflict commits nothing.
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
    let _catalog = shared.catalog_rwlock.read().await;
    let (mut families, occ) = admit_write(&shared.cat(), verb, frames)?;
    if verb == ClientVerb::Push && families.is_empty() {
        return Ok(0);
    }

    let (tids, exclusive, reads_committed) = {
        let cat = shared.cat();
        let reads_committed = families.iter().any(|f| cat.push_reads_committed_state(f.tid, f.mode));
        let exclusive = reads_committed || !occ.is_empty();
        // A shared write's table has no FK edge, so its set is the table alone.
        let tids =
            cat.write_lock_set((families.iter()).map(|f| (f.tid, exclusive && !f.rows.view().all_weights_positive())));
        (tids, exclusive, reads_committed)
    };
    // `tids` is sorted and each once: guarded in ascending order, so no two
    // sets deadlock (a child INSERT and a parent DELETE among them), and each
    // once, since re-guarding a lock this task holds would hang forever.
    let writes = &shared.writes;
    let lock = |tid| writes.tables.borrow_mut().entry(tid).or_default().lock.clone();
    let held = if exclusive {
        let mut guards = SmallVec::with_capacity(tids.len());
        for &tid in &tids {
            guards.push(lock(tid).write().await);
        }
        HeldTables::Exclusive(guards)
    } else {
        let mut guards = SmallVec::with_capacity(tids.len());
        for &tid in &tids {
            guards.push(lock(tid).read().await);
        }
        HeldTables::Shared(guards)
    };
    // OCC: refused when a table of `occ` committed a write after its basis
    // read. The write guard on it keeps any other commit to it from landing
    // before this one. A table not written this boot reads as written at
    // `boot_seed`: no basis read this boot is below it. Every table of `occ` is
    // in `tids`, so the locking made its record, and the catalog read guard
    // keeps it.
    let conflict = {
        let tables = writes.tables.borrow();
        occ.iter()
            .find(|&&(tid, basis)| tables[&tid].commit_lsn.max(writes.boot_seed) > basis)
            .map(|&(tid, _)| tid)
    };
    if let Some(tid) = conflict {
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
        families.iter().all(|f| tids.binary_search(&f.tid).is_ok()),
        "a commit bumps only tables whose lock it holds"
    );
    shared.committer.write(families, held, followed).await
}

/// A write's families in frame order, and its OCC checks: each a table and
/// the read basis its family was built from.
type AdmittedWrite = (Vec<TxnFamily>, Vec<(u64, u64)>);

/// The families of a `verb` write of `frames`, in frame order; and the OCC
/// checks of a `PUSH_TXN`'s. Each `PUSH_TXN` item is judged as a transactional
/// write. The frames of a `PUSH` name one relation, token and conflict mode,
/// and its stream rule names a row by its index in the whole write.
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
            let items = gnitz_wire::multi_frame::decode_items(frame.as_slice(), ctrl.body, ClientVerb::PushTxn)
                .map_err(decode_fault)?;
            for item in items {
                let (basis, tid) = (item.hdr.arg0, item.hdr.target_id);
                let family = admit_push(cat, &frame, item, None)
                    .map_err(|e| e.in_context(format_args!("TXN family {tid}")))?
                    .expect("a PUSH_TXN item carries a data block");
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
        if let Some(family) = admit_push(cat, &frame, ctrl, Some(rows))? {
            rows += family.rows.view().len();
            families.push(family);
        }
    }
    Ok((families, occ))
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
