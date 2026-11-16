//! Draining the merge into owned batches.

use std::ops::{ControlFlow, Range};
use std::rc::Rc;

use super::super::run::Run;
use super::{ReadCursor, SkeletonKeys, hydrate_skeletons};
use crate::repr::batch::Batch;
use crate::repr::merge;
use crate::repr::scatter::{ColumnarSource, Pick, UnifiedSet, with_picks};
use crate::schema::with_payload_cmp;
use gnitz_expr::RowFilter;
use gnitz_wire::{PkKeys, RowSource};

impl ReadCursor {
    /// Copy the current row into `batch` at `weight`, clearing its consolidated claim.
    pub fn copy_current_row_into(&self, batch: &mut Batch, weight: i64) {
        debug_assert!(self.valid, "copy_current_row_into on an invalid cursor");
        debug_assert!(
            !self.current_is_skeleton(),
            "a skeleton row has no payload to copy; hydrate its key"
        );
        let (src, row) = self.row_at(self.current);
        batch.append_row_from_source(weight, src, row, 0, None);
    }

    /// Up to `max_rows` rows in merge order, consolidated, each a whole (PK, payload)
    /// group; `None` once the cursor is exhausted.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        SkeletonKeys::raw(|s| {
            self.drain_live_chunk(max_rows, s, &mut RowFilter::default())
                .map(|(b, _)| b)
        })
    }

    /// [`Self::drain_chunk`] over a store that may hold skeleton rows: each one the
    /// chunk meets is replaced by its key's rows, which `hydrate` recomputes from
    /// the keys, ascending. A skeleton row counts once against `max_rows` however
    /// many rows it hydrates to. The rows of in-memory runs `filter` drops are left
    /// out; every other row stays, hydrated ones included, for the caller to
    /// filter. The `bool` is whether `filter` already decided every row of the
    /// chunk, so the caller need not filter it again: only for a chunk drawn from
    /// in-memory runs alone, with no skeleton row hydrated.
    pub fn drain_hydrated(
        &mut self,
        max_rows: usize,
        filter: &mut RowFilter,
        hydrate: &mut impl FnMut(PkKeys) -> Result<Batch, String>,
    ) -> Result<Option<(Batch, bool)>, String> {
        let mut skeletons = SkeletonKeys::default();
        let Some((live, decided)) = self.drain_live_chunk(max_rows, &mut skeletons, filter) else {
            return Ok(None);
        };
        let decided = decided && skeletons.keys.is_empty();
        Ok(Some((hydrate_skeletons(live, skeletons, hydrate)?, decided)))
    }

    /// [`Self::drain_chunk`], with each skeleton row's key pushed to `skeletons` instead of
    /// copied; a chunk of skeleton rows alone is `Some` and empty. The rows of
    /// in-memory runs `filter` drops are left out of the chunk; a shard's rows are
    /// copied whatever it says. The `bool` is whether `filter` decided every row
    /// of the chunk: `false` once a shard contributed a row or a skeleton key, and
    /// then the caller filters the chunk again.
    pub(super) fn drain_live_chunk(
        &mut self,
        max_rows: usize,
        skeletons: &mut SkeletonKeys,
        filter: &mut RowFilter,
    ) -> Option<(Batch, bool)> {
        assert!(max_rows > 0, "drain_live_chunk: max_rows must be positive");
        if !self.valid {
            return None;
        }
        if let Some(i) = self.mode {
            let decided = matches!(self.sources[i], Run::Mem(_));
            return Some((self.drain_single(i, max_rows, skeletons, filter), decided));
        }
        // Per source, the rows this drain can emit: its current row on to where the drive stops.
        let mut windows = std::mem::take(&mut self.drain_windows);
        windows.clear();
        windows.extend(self.states.iter().map(|s| s.start..s.start));
        windows[self.current.src as usize].start = self.current.row as usize;
        let mut decided = true;
        let mut batch = with_picks(|picks| {
            self.drain_sorted_into(max_rows, picks);
            for (w, s) in windows.iter_mut().zip(&self.states) {
                w.end = s.start;
            }
            decided = !self
                .sources
                .iter()
                .zip(&windows)
                .any(|(src, w)| matches!(src, Run::Shard(_)) && !w.is_empty());
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
            if !filter.keeps_all() {
                retain_kept(&self.sources, &windows, picks, filter);
            }
            UnifiedSet::with(
                &self.sources,
                &self.schema,
                windows.iter().cloned(),
                self.skip_payload,
                |set| set.materialize(picks, None),
            )
        });
        batch.certify_consolidated();
        self.drain_windows = windows;
        Some((batch, decided))
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

    /// Up to `max_rows` rows of `i`, the one live source, as one slice copy; of an
    /// in-memory run, only the rows `filter` keeps.
    fn drain_single(
        &mut self,
        i: usize,
        max_rows: usize,
        skeletons: &mut SkeletonKeys,
        filter: &mut RowFilter,
    ) -> Batch {
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
                shard.slice_to_owned_batch(start, row_count, self.skip_payload)
            }
            Run::Mem(b) => {
                let ranges = filter.ranges_in(&b.as_mem_batch(), start..start + row_count);
                let mut slice = Batch::from_ranges(b, ranges, 0);
                slice.set_schema(&self.schema);
                slice
            }
        };
        self.states[i].start = start + row_count;
        self.advance_single(i);
        batch
    }

    /// Replace `out` with up to `max_rows` merge groups, one [`Pick`] each at its
    /// net weight, in merge order. A [dominated](merge::dominated) drain gallops
    /// its stretches: every run is folded, so each holds an element once.
    fn drain_sorted_into(&mut self, max_rows: usize, out: &mut Vec<Pick>) {
        out.clear();
        out.reserve(max_rows.min(self.estimated_length()));
        out.push(self.current_pick());
        let mut first_undrained = None;
        merge::with_spans!(merge::dominated(&self.states), |SPANS| with_payload_cmp!(
            self.schema,
            |payload| {
                self.drive::<_, SPANS>(payload, |gs, gr, nw| {
                    if out.len() == max_rows {
                        first_undrained = Some((gs, gr, nw));
                        ControlFlow::Break(())
                    } else {
                        out.push((gs as u32, gr as u32, nw));
                        ControlFlow::Continue(())
                    }
                })
            }
        ));
        self.commit_emitted(first_undrained);
    }
}

/// Drop from `picks` every row of an in-memory run that `filter` drops. A pick
/// stands for its whole (PK, payload) group, which the predicate reads alone, so
/// one row decides the group. A shard's picks stay: its regions may be encoded,
/// and are read only through the scatter.
fn retain_kept(sources: &[Run], windows: &[Range<usize>], picks: &mut Vec<Pick>, filter: &mut RowFilter) {
    // Per source, its first word in `bits`, or `None` for a source kept whole.
    let mut first_word = Vec::with_capacity(sources.len());
    let mut bits = Vec::new();
    for (src, w) in sources.iter().zip(windows) {
        first_word.push(match src {
            Run::Mem(b) if !w.is_empty() => {
                let at = bits.len();
                bits.extend_from_slice(filter.mark(&b.as_mem_batch(), w.clone()));
                Some(at)
            }
            _ => None,
        });
    }
    picks.retain(|&(s, r, _)| match first_word[s as usize] {
        Some(at) => {
            let i = r as usize - windows[s as usize].start;
            bits[at + i / 64] >> (i % 64) & 1 == 1
        }
        None => true,
    });
}
