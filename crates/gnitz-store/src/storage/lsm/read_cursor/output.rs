//! Output / drain — turning the merge stream into owned `Batch`es.
//!
//! `materialize`/`drain_chunk` collect the merge order into the cursor's own
//! scratch buffer, then column-scatter it through `repr::scatter`. The
//! `impl ReadCursor` here is a continuation of the merge-engine impl in the parent
//! module; it reads `ReadCursor`'s private fields directly (this submodule is a
//! descendant) and drives the cursor via the parent's private advance helpers.

use std::rc::Rc;

use super::super::batch::{write_to_batch, Batch, Layout};
use super::super::columnar::with_payload_cmp;
use super::super::merge::{prorated_blob_cap, RowComparator};
use super::super::run::Run;
use super::super::scatter::scatter_unified_sources;
use super::ReadCursor;
use gnitz_expr::RowSource;

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
    pub fn copy_current_row_into(&self, batch: &mut Batch, weight: i64) {
        // `current_pk_bytes()` and the appender both index the positioned source,
        // which panics on an unpositioned cursor (empty `sources`). Keep the
        // no-op-on-`!valid` contract.
        if !self.valid {
            return;
        }
        // A skeleton row's payload columns do not exist on disk; the appender
        // would copy `ZERO_CELL` for each and relocate a zero-length blob for
        // each German string, producing a row that is not the view's. Its key
        // has to be hydrated instead (`materialize_hydrated`).
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

    /// Drain up to `max_rows` net rows in merge order into an owned `Batch`
    /// (sorted + consolidated); `None` once the cursor is exhausted. A chunk
    /// boundary cannot split a (PK, payload) group — each drained entry is one
    /// fully-folded merge group — so a DDL backfill or uniqueness scan loops on
    /// this instead of `materialize` and keeps peak memory O(chunk).
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        // Load-bearing: without it a zero-row drain would copy nothing, return an
        // empty batch and still advance, consuming a row.
        if max_rows == 0 || !self.valid {
            return None;
        }
        if let Some(batch) = self.drain_single_source(max_rows) {
            return Some(batch);
        }
        // Read before the drain, which shrinks `estimated_length`. The proration's
        // denominator is the whole source set, so the per-row density it implies
        // stays constant across a chunked drain (pinned by
        // `drain_chunk_blob_reservation_stays_o_chunk`).
        let rows_ahead = self.estimated_length();
        let src_rows: usize = self.sources.iter().map(Run::count).sum();
        let blob_cap = prorated_blob_cap(self.total_blob_len(), src_rows, max_rows.min(rows_ahead));

        let mut order = std::mem::take(&mut self.merge_order);
        self.drain_sorted_into(max_rows, rows_ahead, &mut order);
        let drained = (!order.is_empty()).then(|| {
            let mut cols = Vec::with_capacity(self.sources.len() * self.schema.num_payload_cols());
            let unified: Vec<_> = self
                .sources
                .iter()
                .map(|s| s.to_unified(&self.schema, &mut cols))
                .collect();
            let mut batch = write_to_batch(&self.schema, order.len(), blob_cap, |writer| {
                scatter_unified_sources(&unified, &cols, &order, writer);
            });
            // The merge walk emits in (PK, payload) order with consolidated
            // weights; `write_to_batch` returns `Raw`, so certify `Consolidated`.
            batch.certify_layout(Layout::Consolidated);
            batch
        });
        self.merge_order = order;
        drained
    }

    /// Materialize all non-zero-weight rows in merge order into an owned
    /// `Rc<Batch>`.
    pub fn materialize(mut self) -> Rc<Batch> {
        // Sharing the backing `Rc` requires the whole source: no second source to
        // have skipped rows, nothing consumed at the front, and no range seek
        // clamping the back.
        if self.sources.len() == 1
            && self.valid
            && self.current_row == 0
            && self.states[0].count == self.sources[0].count()
        {
            if let Run::Mem(rc) = &self.sources[0] {
                // Non-verifying, deliberately: `RunSet::push` verified this run
                // on the way in, and re-walking it per drain buys nothing.
                if rc.is_consolidated() {
                    return Rc::clone(rc);
                }
            }
        }
        self.drain_chunk(usize::MAX)
            .map(Rc::new)
            .unwrap_or_else(|| Rc::new(Batch::empty_with_schema(&self.schema)))
    }

    /// Bulk-drain a cursor with exactly one live source into a Batch, bypassing
    /// per-row iteration. Returns `None` when two or more sources can still
    /// contribute, signaling the caller to fall back to row-at-a-time.
    ///
    /// Keys on the drive mode, not on `sources.len() == 1`: every other source's
    /// window is empty, which is the precondition this bulk copy actually needs.
    /// Never empty — `max_rows >= 1` and the committed row is still undrained.
    pub(super) fn drain_single_source(&mut self, max_rows: usize) -> Option<Batch> {
        let i = self.mode?;
        // The undrained window starts at the committed row: the advance that
        // emitted it already stepped `position` past it.
        let start = self.current_row;
        let remaining = self.states[i].count - start;
        let row_count = remaining.min(max_rows);
        let schema = &self.schema;

        // A verbatim slice copy — neither sorts nor consolidates — so it carries
        // whatever the backing can claim.
        let batch = self.sources[i].slice_to_owned_batch(start, row_count, schema);

        // Advance position past the drained rows
        self.states[i].position = start + row_count;
        self.advance();
        Some(batch)
    }

    /// Sum of blob arena sizes across every source. Tight upper bound on the
    /// blob bytes a full drain can produce; callers use this to size the
    /// output blob arena.
    fn total_blob_len(&self) -> usize {
        self.sources.iter().map(|s| s.blob().len()).sum()
    }

    /// Fill `out` with `(entry_idx, row_idx, net_weight)` for up to `max_rows`
    /// merge groups, clearing it first; `rows_ahead` pre-sizes it. The weight is
    /// the merge's **net**, not the exemplar source's stored contribution. On
    /// return `current_*` holds the first undrained group, or the cursor is
    /// invalid.
    fn drain_sorted_into(&mut self, max_rows: usize, rows_ahead: usize, out: &mut Vec<(u32, u32, i64)>) {
        with_payload_cmp!(
            self.schema,
            Self::drain_sorted_into_with,
            self,
            max_rows,
            rows_ahead,
            out
        );
    }

    /// One `merge::drive` for the whole chunk: re-entering it per emitted group
    /// rebuilt the comparator closures each time, ~39 instructions per row.
    #[inline]
    fn drain_sorted_into_with<RowCmp: RowComparator<Run>>(
        &mut self,
        max_rows: usize,
        rows_ahead: usize,
        out: &mut Vec<(u32, u32, i64)>,
        row_cmp: RowCmp,
    ) {
        out.clear();
        out.reserve(max_rows.min(rows_ahead));
        // The committed row is an earlier drive's, not yet drained — and the
        // drive emits only non-zero groups, so it needs no weight gate.
        // `u32` because a partitioned-table cursor can exceed 256 entries.
        out.push((
            self.current_entry_idx as u32,
            self.current_row as u32,
            self.current_weight,
        ));

        // The group that did not fit, which the next drain resumes from.
        let mut last: Option<(usize, usize, i64)> = None;
        self.drive(row_cmp, |gs, gr, nw| {
            if out.len() == max_rows {
                last = Some((gs, gr, nw));
                std::ops::ControlFlow::Break(())
            } else {
                out.push((gs as u32, gr as u32, nw));
                std::ops::ControlFlow::Continue(())
            }
        });
        self.commit_emitted(last);
    }
}
