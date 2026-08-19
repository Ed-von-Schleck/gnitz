//! Output / drain — turning the merge stream into owned `Batch`es.
//!
//! `materialize`/`drain_chunk` collect the merge order into a thread-local scratch
//! buffer (`DrainGuard`), then column-scatter it through `repr::scatter`. The
//! `impl ReadCursor` here is a continuation of the merge-engine impl in the parent
//! module; it reads `ReadCursor`'s private fields directly (this submodule is a
//! descendant) and drives the cursor via the parent's private advance/drive
//! helpers.

use std::cell::Cell;
use std::rc::Rc;

use super::super::batch::{write_to_batch, Batch, Layout};
use super::super::columnar::with_payload_cmp;
use super::super::merge::DirectWriter;
use super::super::run::Run;
use super::super::scatter::scatter_unified_sources_with_weights;
use super::{ReadCursor, RowComparator, SourceMode};
use gnitz_expr::RowSource;

thread_local! {
    /// Reusable per-thread scratch buffer for `drain_sorted_into`. Each
    /// 16-byte tuple is much smaller than the corresponding output row, so
    /// keeping peak capacity for the thread's lifetime is cheap relative to
    /// the batches it feeds.
    ///
    /// `Cell<Vec<_>>` (not `RefCell`) — `DrainGuard` moves the Vec out via
    /// `Cell::take` and returns it on drop, skipping the runtime borrow
    /// check `RefCell` would impose.
    static DRAIN_BUFFER: Cell<Vec<(u32, u32, i64)>> =
        const { Cell::new(Vec::new()) };
}

/// RAII handle wrapping the thread-local drain scratch buffer.  Behaves like
/// `&mut Vec<_>` via `Deref`/`DerefMut`.  On drop it returns the buffer to the
/// thread-local, keeping whichever Vec has the larger capacity (capped at
/// `MAX_DRAIN_BUFFER_CAP`); because the slot is a `Cell`, an unwind through
/// `Drop` cannot poison it.
pub(crate) struct DrainGuard {
    inner: Vec<(u32, u32, i64)>,
}

impl DrainGuard {
    #[inline]
    pub(crate) fn new() -> Self {
        // The thread-local Vec is reused across queries and may hold stale
        // elements; clear so `new()` always yields an empty buffer. The
        // elements are `Copy`, so this is an O(1) length reset.
        let mut inner = DRAIN_BUFFER.with(|b| b.take());
        inner.clear();
        Self { inner }
    }
}

impl std::ops::Deref for DrainGuard {
    type Target = Vec<(u32, u32, i64)>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for DrainGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// 65 536 × 16 bytes = 1 MB. Caps the retained scratch so a one-off oversized
/// query can't pin an unusually large allocation for the thread's lifetime.
const MAX_DRAIN_BUFFER_CAP: usize = 65_536;

impl Drop for DrainGuard {
    #[inline]
    fn drop(&mut self) {
        DRAIN_BUFFER.with(|b| {
            // Keep whichever Vec has the larger capacity. A nested cursor may
            // drop a smaller guard after we took the (larger) thread-local;
            // unconditionally writing our own inner back would shrink it.
            let mut cached = b.take();
            if self.inner.capacity() > cached.capacity() && self.inner.capacity() <= MAX_DRAIN_BUFFER_CAP {
                cached = std::mem::take(&mut self.inner);
            }
            b.set(cached);
        });
    }
}

impl ReadCursor {
    /// Copy the current row into `batch` with an explicit weight, downgrading
    /// `batch`'s layout to `Raw` (the shared appender does this — any append can
    /// break order/folding).
    ///
    /// This is `Batch::append_row_from_source_bytes` applied at the cursor's
    /// current position: `Run` implements `ColumnarSource`, so the shared
    /// appender does the weight/null/payload write (German-string blob relocation
    /// included) with no hand-rolled per-column loop here. The byte-form PK is
    /// correct at every width.
    ///
    /// Cold catalog-builder callers re-fold downstream; the operator caller
    /// (`op_reduce`) wants `Raw` anyway — it emits retract/insert pairs in emit
    /// order, an unconsolidated delta.
    pub(crate) fn copy_current_row_into(&self, batch: &mut Batch, weight: i64) {
        // `current_pk_bytes()` and the appender both index the positioned source,
        // which panics on an unpositioned cursor (empty `sources`). Keep the
        // no-op-on-`!valid` contract.
        if !self.valid {
            return;
        }
        // A skeleton row's payload columns do not exist on disk; the appender
        // would copy `ZERO_CELL` for each and relocate a zero-length blob for
        // each German string, producing a row that is not the view's. Its key
        // has to be hydrated instead (`materialize_bounded_store`).
        debug_assert!(
            !self.current_is_skeleton(),
            "copy_current_row_into on a skeleton row: hydrate the key instead",
        );
        batch.append_row_from_source_bytes(
            self.current_pk_bytes(),
            weight,
            &self.sources[self.current_entry_idx],
            self.current_row,
            None,
        );
    }

    /// Drain up to `limit` net rows in merge order into an owned `Batch`
    /// (sorted + consolidated). `limit == 0` means unbounded. Returns `None`
    /// once the cursor is exhausted / nothing drained. Single owner of the
    /// drain → scatter → flag pipeline shared by `materialize` and
    /// `drain_chunk`.
    pub(super) fn drain_to_batch(&mut self, limit: usize) -> Option<Batch> {
        if !self.valid {
            return None;
        }
        if let Some(batch) = self.drain_single_source(limit) {
            // Faithful verbatim copy carrying the source's own flags (no
            // re-sort / re-consolidate) — exactly `drain_chunk`'s prior
            // fast-path behavior. Every drain caller opens over
            // `Table`, whose single `Batch` sources are always
            // sorted + consolidated, so the propagated flags are `true`.
            return (batch.count > 0).then_some(batch);
        }
        let mut merge_order = DrainGuard::new();
        self.drain_sorted_into(limit, &mut merge_order);
        if merge_order.is_empty() {
            return None;
        }
        // Full drains: `total_blob_len()` is a tight upper bound. Chunked
        // drains: pass 0 and let `DirectWriter` grow the blob on demand
        // (reserving the whole relation's blob arena per chunk otherwise).
        let blob_cap = if limit == 0 { self.total_blob_len() } else { 0 };
        let mut batch = write_to_batch(&self.schema, merge_order.len(), blob_cap, |writer| {
            self.scatter_drained_into(&merge_order, writer)
        });
        // The merge walk emits in (PK, payload) order with consolidated weights;
        // `write_to_batch` returns `Raw`, so certify `Consolidated`.
        batch.certify_layout(Layout::Consolidated, &self.schema);
        Some(batch)
    }

    /// Materialize all non-zero-weight rows in merge order into an owned
    /// `Rc<Batch>`.
    pub(crate) fn materialize(mut self) -> Rc<Batch> {
        // Sharing the backing `Rc` requires the whole source: no second source to
        // have skipped rows, nothing consumed at the front, and no range seek
        // clamping the back.
        if self.sources.len() == 1
            && self.valid
            && self.current_row == 0
            && self.states[0].count == self.sources[0].count()
        {
            if let Run::Mem(rc) = &self.sources[0] {
                if rc.consolidated_verified(&self.schema) {
                    return Rc::clone(rc);
                }
            }
        }
        self.drain_to_batch(0)
            .map(Rc::new)
            .unwrap_or_else(|| Rc::new(Batch::empty_with_schema(&self.schema)))
    }

    /// Drain up to `max_rows` net rows in merge order into an owned `Batch`
    /// (sorted + consolidated, like `materialize`). Returns `None` once the
    /// cursor is exhausted. Chunk boundaries cannot split a (PK, payload)
    /// group: each drained entry is one fully-folded merge group.
    ///
    /// DDL backfills and uniqueness scans call this in a loop instead of
    /// `materialize` so peak memory is O(chunk) instead of O(relation).
    pub(crate) fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        debug_assert!(max_rows > 0, "drain_chunk: 0 means unlimited in the drain helpers");
        self.drain_to_batch(max_rows)
    }

    /// Bulk-drain a cursor with exactly one live source into a Batch, bypassing
    /// per-row iteration. Returns `None` when two or more sources can still
    /// contribute, signaling the caller to fall back to row-at-a-time.
    ///
    /// Keys on `SourceMode::Single(i)`, not on `sources.len() == 1`: the other
    /// sources have an empty `[position, count)` window, so nothing they hold can
    /// fold against the drained rows — which is the precondition this bulk copy
    /// actually needs.
    ///
    /// `limit == 0` means drain all remaining rows.
    pub(super) fn drain_single_source(&mut self, limit: usize) -> Option<Batch> {
        let SourceMode::Single(i) = self.mode else {
            return None;
        };
        if !self.valid {
            return None;
        }
        // The undrained window starts at the committed row: the drive that
        // emitted it already stepped `position` past it.
        let start = self.current_row;
        let remaining = self.states[i].count - start;
        let row_count = if limit > 0 { remaining.min(limit) } else { remaining };
        let schema = &self.schema;

        let batch = match &self.sources[i] {
            Run::Mem(b) => {
                // A verbatim slice copy — neither sorts nor consolidates — so it
                // carries the source's own flags rather than asserting them. (In
                // practice every cursor-source batch is already consolidated; see
                // the note in `drain_to_batch`. This helper relies on neither.)
                //
                // Both blob arms are available here, so the same predicate the
                // shard arm below uses picks between them: share the source's heap
                // verbatim, or relocate only the drained rows' strings.
                let mut out = Batch::with_capacity(*schema, row_count.max(1));
                if !Batch::should_relocate_blob(row_count, b.count, b.blob.len()) {
                    out.share_blob_from(b);
                }
                out.append_ranges(b, &[(start, start + row_count)]);
                // A contiguous slice of a sorted/consolidated source preserves its
                // layout (faithful); `append_ranges` downgraded `out` to `Raw` first.
                out.inherit_layout(b);
                out
            }
            Run::Shard(s) => s.slice_to_owned_batch(start, row_count, schema),
        };

        // Advance position past the drained rows
        self.states[i].position = start + row_count;
        self.drive();
        Some(batch)
    }

    /// Sum of blob arena sizes across every source. Tight upper bound on the
    /// blob bytes a full drain can produce; callers use this to size the
    /// output blob arena.
    fn total_blob_len(&self) -> usize {
        self.sources.iter().map(|s| s.blob().len()).sum()
    }

    /// Walk the merge order and fill `out` with `(entry_idx, row_idx, weight)`
    /// for every row whose net consolidated weight is non-zero.  Drains up to
    /// `limit` rows (`limit == 0` means unlimited).  Clears `out` first.
    ///
    /// The buffered weight is the **net** weight produced by the merge —
    /// callers must not read it back from the exemplar source's stored weight,
    /// which is the per-source contribution and may not equal the net.
    /// Callers needing custom termination (group-bounded iteration, predicate
    /// filters) collect into a local `Vec` instead — this helper only supports
    /// row-count and full-cursor termination.
    fn drain_sorted_into(&mut self, limit: usize, out: &mut Vec<(u32, u32, i64)>) {
        with_payload_cmp!(self.schema, Self::drain_sorted_into_with, self, limit, out);
    }

    #[inline]
    fn drain_sorted_into_with<RowCmp: RowComparator>(
        &mut self,
        limit: usize,
        out: &mut Vec<(u32, u32, i64)>,
        row_cmp: RowCmp,
    ) {
        out.clear();
        let mut count = 0usize;
        while self.valid {
            if limit > 0 && count >= limit {
                break;
            }
            let w = self.current_weight;
            if w != 0 {
                // src_idx is u32 because partitioned-table cursors can exceed
                // 256 entries; a u8 cast would wrap silently.
                out.push((self.current_entry_idx as u32, self.current_row as u32, w));
                count += 1;
            }
            self.drive_with(row_cmp);
        }
    }

    pub(crate) fn scatter_drained_into(&self, rows: &[(u32, u32, i64)], writer: &mut DirectWriter<'_>) {
        if rows.is_empty() {
            return;
        }
        let unified = self
            .unified_sources
            .get_or_init(|| self.sources.iter().map(|s| s.to_unified(&self.schema)).collect());
        scatter_unified_sources_with_weights(unified, rows, writer);
    }
}
