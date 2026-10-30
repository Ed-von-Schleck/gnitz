//! Draining the merge into owned batches.

use std::ops::ControlFlow;
use std::rc::Rc;

use super::super::run::Run;
use super::{ReadCursor, SkeletonKeys};
use crate::repr::batch::Batch;
use crate::repr::scatter::{ColumnarSource, Pick, UnifiedSet};
use crate::schema::with_payload_cmp;
use gnitz_wire::RowSource;

impl ReadCursor {
    /// Copy the current row into `batch` at `weight`, clearing its consolidated claim.
    pub fn copy_current_row_into(&self, batch: &mut Batch, weight: i64) {
        debug_assert!(self.valid, "copy_current_row_into on an invalid cursor");
        debug_assert!(
            !self.current_is_skeleton(),
            "a skeleton row has no payload to copy; hydrate its key"
        );
        let (src, row) = self.row_at(self.current);
        batch.append_row_from_source(weight, src, row, None);
    }

    /// Up to `max_rows` rows in merge order, consolidated, each a whole (PK, payload)
    /// group; `None` once the cursor is exhausted.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        SkeletonKeys::raw(|s| self.drain_live_chunk(max_rows, s))
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
        windows.extend(self.states.iter().map(|s| s.start..s.start));
        windows[self.current.src as usize].start = self.current.row as usize;
        let mut picks = std::mem::take(&mut self.drain_picks);
        self.drain_sorted_into(max_rows, &mut picks);
        for (w, s) in windows.iter_mut().zip(&self.states) {
            w.end = s.start;
        }
        if self.order.coarsen {
            let sources = &self.sources;
            picks.retain(|&(e, r, w)| {
                let src = &sources[e as usize];
                let skeleton = src.is_skeleton();
                if skeleton {
                    skeletons.push(src.get_pk_bytes(r as usize), w);
                }
                !skeleton
            });
        }
        let set = UnifiedSet::of(&self.sources, &self.schema, windows.iter().cloned());
        let mut batch = set.materialize(&picks, None);
        batch.certify_consolidated();
        self.drain_picks = picks;
        self.drain_windows = windows;
        Some(batch)
    }

    /// Every remaining row in merge order, consolidated.
    pub fn materialize(mut self) -> Rc<Batch> {
        if let [Run::Mem(rc)] = &self.sources[..] {
            let whole = self.valid && self.current.row == 0 && self.states[0].end == rc.len();
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
        // `states[i].start` is already past the committed row, so the slice
        // starts at `current.row`.
        let start = self.current.row as usize;
        let row_count = (self.states[i].end - start).min(max_rows);
        let batch = match &self.sources[i] {
            Run::Shard(shard) if shard.is_skeleton() => {
                skeletons.extend(shard, start..start + row_count);
                Batch::empty_with_schema(&self.schema)
            }
            Run::Shard(shard) => {
                debug_assert!(
                    *shard.schema() == self.schema,
                    "a shard slices under the schema it is bound to"
                );
                shard.slice_to_owned_batch(start, row_count)
            }
            Run::Mem(b) => {
                let mut slice = Batch::from_ranges(b, &[(start, start + row_count)], 0);
                slice.set_schema(&self.schema);
                slice
            }
        };
        self.states[i].start = start + row_count;
        self.advance_single(i);
        batch
    }

    /// Replace `out` with up to `max_rows` merge groups, one [`Pick`] each at its
    /// net weight, in merge order.
    fn drain_sorted_into(&mut self, max_rows: usize, out: &mut Vec<Pick>) {
        out.clear();
        out.reserve(max_rows.min(self.estimated_length()));
        out.push(self.current_pick());
        let mut first_undrained = None;
        with_payload_cmp!(self.schema, |payload| self.drive(payload, |gs, gr, nw| {
            if out.len() == max_rows {
                first_undrained = Some((gs, gr, nw));
                ControlFlow::Break(())
            } else {
                out.push((gs as u32, gr as u32, nw));
                ControlFlow::Continue(())
            }
        }));
        self.commit_emitted(first_undrained);
    }
}
