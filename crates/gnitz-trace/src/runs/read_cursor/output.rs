//! Draining the merge into owned batches.

use std::ops::{ControlFlow, Range};
use std::rc::Rc;

use super::super::run::Run;
use super::{ReadCursor, SkeletonKeys, hydrate_skeletons};
use gnitz_expr::RowFilter;
use gnitz_wire::PkKeys;
use gnitz_zset::repr::Batch;
use gnitz_zset::repr::{ColumnarSource, Pick, UnifiedSet, span_of, span_pick, with_picks, with_spans};
use gnitz_zset::schema::with_payload_cmp;

impl ReadCursor {
    /// Copy the current row into `batch` at `weight`, clearing its consolidated claim.
    pub fn copy_current_row_into(&self, batch: &mut Batch, weight: i64) {
        let (src, row) = self.row_at(self.current_position());
        debug_assert!(
            !src.is_skeleton(),
            "a skeleton row has no payload to copy; hydrate its key"
        );
        batch.append_row_from_source(weight, src, row);
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
        let (src, row, _) = self.current?;
        if let Some(i) = self.mode {
            let decided = matches!(self.sources[i], Run::Mem(_));
            return Some((self.drain_single(i, row as usize, max_rows, skeletons, filter), decided));
        }
        // Per source, the rows this drain can emit: its current row on to where the drive stops.
        let mut windows = std::mem::take(&mut self.drain_windows);
        windows.clear();
        windows.extend(self.merge.cursors().iter().map(|s| s.start..s.start));
        windows[src as usize].start = row as usize;
        let mut decided = true;
        let mut batch = with_picks(|picks| {
            let spans = self.drain_sorted_into(max_rows, picks);
            for (w, s) in windows.iter_mut().zip(self.merge.cursors()) {
                w.end = s.start;
            }
            decided = !self
                .sources
                .iter()
                .zip(&windows)
                .any(|(src, w)| matches!(src, Run::Shard(_)) && !w.is_empty());
            if self.merge.coarsens() {
                skeletons.split_from(&self.sources, picks);
            }
            if !filter.keeps_all() {
                retain_kept(&self.sources, &windows, picks, filter, spans);
            }
            UnifiedSet::with(
                &self.sources,
                &self.schema,
                windows.iter().cloned(),
                self.skip_payload,
                spans,
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
            let whole = self.current.is_some_and(|(_, row, _)| row == 0) && self.merge.cursors()[0].end == rc.len();
            if whole {
                return Rc::clone(rc);
            }
        }
        self.drain_chunk(usize::MAX)
            .map(Rc::new)
            .unwrap_or_else(|| Rc::new(Batch::empty_with_schema(&self.schema)))
    }

    /// Up to `max_rows` rows of `i`, the one live source, from `start`, the
    /// current row, as one slice copy; of an in-memory run, only the rows
    /// `filter` keeps.
    fn drain_single(
        &mut self,
        i: usize,
        start: usize,
        max_rows: usize,
        skeletons: &mut SkeletonKeys,
        filter: &mut RowFilter,
    ) -> Batch {
        let row_count = (self.merge.cursors()[i].end - start).min(max_rows);
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
        self.merge.cursors_mut()[i].start = start + row_count;
        self.advance_single(i);
        batch
    }

    /// Replace `out` with up to `max_rows` merge groups, one [`Pick`] each at its
    /// net weight, in merge order; `true` where some are spans. A
    /// [dominated](gnitz_zset::repr::RunMerge::dominated) drain gallops its
    /// stretches and emits each as a span: every run is folded, so each holds
    /// an element once.
    fn drain_sorted_into(&mut self, max_rows: usize, out: &mut Vec<Pick>) -> bool {
        out.clear();
        out.reserve(max_rows.min(self.estimated_length()));
        out.push(self.current_position());
        with_spans!(self.merge.dominated(), |SPANS| {
            with_payload_cmp!(self.schema, |payload| {
                let mut first_undrained = None;
                // The rows the spans in `out` hold past one each.
                let mut extra = 0usize;
                // Whether a span was cut at `max_rows`, its rest left unconsumed.
                let mut cut = false;
                let ReadCursor { merge, schema, sources, .. } = &mut *self;
                merge.drive::<_, _, SPANS>(schema, sources, payload, |src, row, w| {
                    let pick = (src as u32, row as u32, w);
                    if SPANS && let Some((src, rows)) = span_of(pick) {
                        let take = rows.len().min(max_rows - out.len() - extra);
                        if take > 0 {
                            out.push(span_pick(src, rows.start..rows.start + take));
                            extra += take - 1;
                        }
                        cut = take < rows.len();
                        return match cut {
                            true => ControlFlow::Break(take),
                            false => ControlFlow::Continue(()),
                        };
                    }
                    if out.len() + extra == max_rows {
                        first_undrained = Some(pick);
                        ControlFlow::Break(0)
                    } else {
                        out.push(pick);
                        ControlFlow::Continue(())
                    }
                });
                match cut {
                    // The cut span's next row is the cursor's next group.
                    true => self.advance_merge_with(payload),
                    false => self.current = first_undrained,
                }
            });
            SPANS
        })
    }
}

/// Drop from `picks` every row of an in-memory run that `filter` drops. A pick
/// stands for its whole (PK, payload) group, which the predicate reads alone, so
/// one row decides the group; a span of an in-memory run, which `spans` says
/// `picks` may hold, is cut to the stretches of it `filter` keeps. A shard's
/// picks stay: its regions may be encoded, and are read only through the
/// scatter.
fn retain_kept(sources: &[Run], windows: &[Range<usize>], picks: &mut Vec<Pick>, filter: &mut RowFilter, spans: bool) {
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
    let kept = |s: usize, r: usize| match first_word[s] {
        Some(at) => {
            let i = r - windows[s].start;
            bits[at + i / 64] >> (i % 64) & 1 == 1
        }
        None => true,
    };
    if !spans {
        return picks.retain(|&(s, r, _)| kept(s as usize, r as usize));
    }
    let mut out = Vec::with_capacity(picks.len());
    for &pick in picks.iter() {
        match span_of(pick) {
            Some((s, rows)) if first_word[s].is_some() => {
                let mut from = None;
                for r in rows.start..=rows.end {
                    match (r < rows.end && kept(s, r), from) {
                        (true, None) => from = Some(r),
                        (false, Some(f)) => {
                            out.push(span_pick(s, f..r));
                            from = None;
                        }
                        _ => {}
                    }
                }
            }
            Some(_) => out.push(pick),
            None => {
                if kept(pick.0 as usize, pick.1 as usize) {
                    out.push(pick)
                }
            }
        }
    }
    *picks = out;
}
