//! Draining the merge into owned batches.

use std::rc::Rc;

use super::super::run::Run;
use super::{ReadCursor, SkeletonKeys};
use crate::repr::batch::Batch;
use crate::repr::merge::ColumnarSource;
use crate::repr::scatter::UnifiedSet;
use crate::schema::payload_order::{with_payload_cmp, PayloadOrder};
use gnitz_expr::RowSource;

impl ReadCursor {
    /// Copy the current row into `batch` at `weight`, clearing its consolidated claim.
    pub fn copy_current_row_into(&self, batch: &mut Batch, weight: i64) {
        debug_assert!(self.valid, "copy_current_row_into on an invalid cursor");
        debug_assert!(
            !self.current_is_skeleton(),
            "a skeleton row has no payload to copy; hydrate its key"
        );
        batch.append_row_from_source(weight, &self.sources[self.current_entry_idx], self.current_row, None);
    }

    /// Up to `max_rows` rows in merge order, consolidated, each a whole (PK, payload)
    /// group; `None` once the cursor is exhausted.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        let mut skeletons = SkeletonKeys::default();
        let chunk = self.drain_live_chunk(max_rows, &mut skeletons);
        skeletons.assert_none();
        chunk
    }

    /// [`Self::drain_chunk`], with each skeleton row's key pushed to `skeletons` instead of
    /// copied; a chunk of skeleton rows alone is `Some` and empty.
    pub(super) fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        assert!(max_rows > 0, "drain_live_chunk: max_rows must be positive");
        if !self.valid {
            return None;
        }
        if let Some(i) = self.mode {
            return Some(self.drain_single(i, max_rows, skeletons));
        }
        // Per source, the rows this drain can emit: its current row on to where the drive stops.
        let mut windows = std::mem::take(&mut self.drain_windows);
        windows.clear();
        windows.extend(self.states.iter().map(|s| s.position..s.position));
        windows[self.current_entry_idx].start = self.current_row;
        let mut order = std::mem::take(&mut self.merge_order);
        self.drain_sorted_into(max_rows, &mut order);
        for (w, s) in windows.iter_mut().zip(&self.states) {
            w.end = s.position;
        }
        if self.any_skeleton {
            let sources = &self.sources;
            order.retain(|&(e, r, w)| {
                let src = &sources[e as usize];
                if src.is_skeleton() {
                    skeletons.push(src.get_pk_bytes(r as usize), w);
                }
                !src.is_skeleton()
            });
        }
        let set = UnifiedSet::of(&self.sources, &self.schema, windows.iter().cloned());
        let mut batch = set.materialize(&order, set.src_rows());
        batch.certify_consolidated();
        self.merge_order = order;
        self.drain_windows = windows;
        Some(batch)
    }

    /// Every remaining row in merge order, consolidated.
    pub fn materialize(mut self) -> Rc<Batch> {
        if let [Run::Mem(rc)] = &self.sources[..] {
            let whole = self.valid && self.current_row == 0 && self.states[0].count == rc.count;
            if whole {
                return Rc::clone(rc);
            }
        }
        self.drain_chunk(usize::MAX)
            .map(Rc::new)
            .unwrap_or_else(|| Rc::new(Batch::empty_with_schema(&self.schema)))
    }

    /// Up to `max_rows` rows of `i`, the one live source, as one slice copy.
    fn drain_single(&mut self, i: usize, max_rows: usize, skeletons: &mut SkeletonKeys) -> Batch {
        // `position` is already past the committed row.
        let start = self.current_row;
        let row_count = (self.states[i].count - start).min(max_rows);
        let src = &self.sources[i];
        let batch = if src.is_skeleton() {
            for r in start..start + row_count {
                skeletons.push(src.get_pk_bytes(r), src.get_weight(r));
            }
            Batch::empty_with_schema(&self.schema)
        } else {
            src.slice_to_owned_batch(start, row_count, &self.schema)
        };
        self.states[i].position = start + row_count;
        self.advance_single(i);
        batch
    }

    /// Replace `out` with up to `max_rows` merge groups as `(source, row, net weight)`,
    /// in merge order.
    fn drain_sorted_into(&mut self, max_rows: usize, out: &mut Vec<(u32, u32, i64)>) {
        out.clear();
        out.reserve(max_rows.min(self.estimated_length()));
        with_payload_cmp!(self.schema, Self::drain_sorted_with, self, max_rows, out);
    }

    #[inline]
    fn drain_sorted_with<P: PayloadOrder>(&mut self, max_rows: usize, out: &mut Vec<(u32, u32, i64)>, payload: P) {
        out.push((
            self.current_entry_idx as u32,
            self.current_row as u32,
            self.current_weight,
        ));
        let mut first_undrained = None;
        self.drive(payload, |gs, gr, nw| {
            if out.len() == max_rows {
                first_undrained = Some((gs, gr, nw));
                std::ops::ControlFlow::Break(())
            } else {
                out.push((gs as u32, gr as u32, nw));
                std::ops::ControlFlow::Continue(())
            }
        });
        self.commit_emitted(first_undrained);
    }
}
