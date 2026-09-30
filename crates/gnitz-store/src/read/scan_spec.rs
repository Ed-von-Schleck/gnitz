//! The worker half of an ad-hoc `ReadSpec` read, over this worker's slice: open
//! the bound, filter, map, then forward rows or fold them. A capacity-bounded
//! view's rows hydrate chunk by chunk as the sink drains them.

use crate::schema::SchemaFacts;
use gnitz_wire::{PkKeys, ReadBound, ReadSpec, SinkKind, WireFault, WireStatus};

use std::rc::Rc;

use super::{SkeletonHydrator, SourceCursor};
use crate::ops::AdhocFold;
use crate::ops::MapPlan;
use crate::relation::RelationRegistry;
use crate::schema::key::{key_range_between_cuts, KeyCut};
use crate::schema::{delta_round, delta_round_prefix, delta_view_key, SchemaDescriptor};
use crate::storage::{Batch, SkeletonKeys};
use gnitz_expr::{cmp_order_keys, order_locators, OrderLocator, RowFilter};

impl RelationRegistry {
    /// Execute `spec` on this worker's slice, replying in the layout whose
    /// [`SchemaFacts::layout_digest`] is `reply_layout`.
    pub fn scan_spec(
        &self,
        target_id: u64,
        spec: ReadSpec,
        reply_layout: u64,
        hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<Rc<Batch>, String> {
        let ReadSpec { bound, predicate, sink } = spec;
        let entry = self.relation_or_err(target_id)?;
        let src_schema = entry.schema();
        // Nothing bounded, filtered, mapped or cut, and nothing to hydrate: the
        // relation whole, off the store's cached snapshot.
        if let (ReadBound::None, true, None, SinkKind::Rows { limit_k: 0, .. }) =
            (&bound, predicate.is_empty(), &sink.map, &sink.kind)
        {
            if !entry.store().held().has_skeleton_rows() {
                check_layout(reply_layout, &src_schema)?;
                return Ok(entry.full_scan());
            }
        }
        let (source, unapplied) = self.open_bound(target_id, bound)?;
        // A bad predicate and a bad walk are both a corrupt request: the client
        // pre-compiled the identical program at plan time.
        let filter = RowFilter::for_read(&predicate, &unapplied, &src_schema).map_err(|e| format!("scan_spec: {e}"))?;
        let mut map = sink
            .map
            .as_ref()
            .map(|m| MapPlan::from_compute_map(&src_schema, m))
            .transpose()
            .map_err(|e| format!("scan_spec map: {e}"))?;
        let sink_in = map.as_ref().map_or(src_schema, |m| *m.out_schema());
        let mut rows = Survivors {
            registry: self,
            id: target_id,
            source,
            hydrator,
            filter,
            ranges: Vec::new(),
        };
        let chunk_rows = self.config.scan_chunk_rows;
        Ok(Rc::new(match &sink.kind {
            SinkKind::Fold(agg) => {
                let fold = AdhocFold::new(&sink_in, agg, self.config.adhoc_group_cap)?;
                check_layout(reply_layout, fold.output_schema())?;
                run_fold_sink(&mut rows, chunk_rows, map.as_mut(), fold)?
            }
            SinkKind::Rows { order, limit_k } => {
                check_layout(reply_layout, &sink_in)?;
                let window = saturated_window(*limit_k);
                if order.is_empty() {
                    stream_rows(&mut rows, chunk_rows, map.as_mut(), &sink_in, window)?
                } else {
                    debug_assert!(window > 0, "decode admits an order only under a cut");
                    sink_in
                        .check_cols(order.iter().map(|k| ("scan_spec: order key column", k.col as u32)))
                        .map_err(|e| e.to_string())?;
                    topk_rows(
                        &mut rows,
                        chunk_rows,
                        map.as_mut(),
                        &sink_in,
                        &order_locators(order, &sink_in),
                        window,
                    )?
                }
            }
        }))
    }

    /// Every delta `id`'s feed recorded in rounds `(after_tick, cut_tick]`, in the
    /// view's schema whatever `after_tick`; `0` walks the view's own output store.
    /// A cursor below the retained floor is refused as [`WireStatus::DeltaExpired`];
    /// every other refusal is `Error`.
    pub fn delta_read(
        &self,
        id: u64,
        after_tick: u64,
        cut_tick: u64,
        reply_layout: u64,
    ) -> Result<Rc<Batch>, WireFault> {
        let entry = self.relation_or_err(id)?;
        if !entry.has_delta_feed() {
            return Err(format!(
                "delta_read: relation {id} carries no delta feed; \
                 create the view WITH (delta = '<size>') to subscribe to it"
            )
            .into());
        }
        let view = entry.schema();
        check_layout(reply_layout, &view)?;
        if after_tick == 0 {
            return Ok(entry.full_scan());
        }
        let feed = entry
            .delta()
            .ok_or_else(|| format!("delta_read: this process holds no delta store for relation {id}"))?;
        let dropped_through = delta_round(feed.dropped_max().pk_bytes());
        // A cursor at the floor has lost nothing.
        if after_tick < dropped_through {
            return Err(WireFault {
                status: WireStatus::DeltaExpired,
                text: format!(
                    "delta cursor {after_tick} of relation {id} is below the \
                     retained floor {dropped_through}; re-read at 0"
                ),
            });
        }
        let band = key_range_between_cuts(
            KeyCut::above(&delta_round_prefix(after_tick)),
            KeyCut::above(&delta_round_prefix(cut_tick)),
            feed.schema().pk_stride(),
        );
        let rows = feed.range_cursor(band).materialize();
        Ok(Rc::new(
            rows.rekeyed(&view, |src, dst| dst.copy_from_slice(delta_view_key(src))),
        ))
    }
}

/// The reply guard's refusal: a keeper built in any other layout would ship its
/// regions under the client's strides.
fn check_layout(reply_layout: u64, produced: &SchemaDescriptor) -> Result<(), String> {
    if reply_layout != produced.layout_digest() {
        return Err("reply schema does not match the output layout".to_string());
    }
    Ok(())
}

/// The client's `limit_k` (`0` = unbounded) as an `i64` weight window, saturated:
/// a wrapped negative window would truncate the answer.
fn saturated_window(limit_k: u64) -> i64 {
    limit_k.min(i64::MAX as u64) as i64
}

/// A source chunk and its surviving row ranges.
type SurvivorChunk<'r> = (Batch, &'r mut Vec<(usize, usize)>);

/// The rows surviving the bound and the predicate, one source chunk at a time —
/// the one input every sink reads. Each skeleton row a chunk meets is replaced by
/// that key's rows, recomputed through `hydrator`.
struct Survivors<'a, 'h> {
    registry: &'a RelationRegistry,
    id: u64,
    source: SourceCursor,
    hydrator: Option<&'h mut dyn SkeletonHydrator>,
    /// The predicate, and the part of the bound the source did not apply.
    filter: RowFilter,
    /// The current chunk's surviving row ranges; scratch reused across chunks.
    ranges: Vec<(usize, usize)>,
}

impl Survivors<'_, '_> {
    /// The next source chunk and its surviving row ranges; `None` once the source is
    /// exhausted. `max_rows` bounds the merge groups visited; a skeleton row counts
    /// once however many rows it hydrates to.
    fn next(&mut self, max_rows: usize) -> Result<Option<SurvivorChunk<'_>>, String> {
        let mut skeletons = SkeletonKeys::default();
        let Some(mut chunk) = self.source.drain_live_chunk(max_rows, &mut skeletons) else {
            return Ok(None);
        };
        if !skeletons.keys.is_empty() {
            chunk = self.hydrate(chunk, skeletons)?;
        }
        self.filter.ranges(&chunk.as_mem_batch(), &mut self.ranges);
        Ok(Some((chunk, &mut self.ranges)))
    }

    /// `live` merged with the rows recomputed at `skeletons`' keys.
    fn hydrate(&mut self, live: Batch, mut skeletons: SkeletonKeys) -> Result<Batch, String> {
        let Some(hydrator) = self.hydrator.as_deref_mut() else {
            return Err(format!(
                "relation {} holds skeleton rows but this process maintains no circuit",
                self.id
            ));
        };
        let keys = PkKeys::from_sorted(live.schema().pk_stride(), std::mem::take(&mut skeletons.keys));
        #[cfg(debug_assertions)]
        let asked = keys.clone();
        let hydrated = hydrator
            .hydrate_keys(self.registry, self.id, keys)
            .map_err(|e| format!("hydrate: view {}: {e}", self.id))?;
        #[cfg(debug_assertions)]
        assert_hydration_matches(&hydrated, &asked, &skeletons.coarse);
        // Both consolidated and PK-disjoint.
        let schema = *live.schema();
        Ok(match live.is_empty() {
            true => hydrated,
            false => hydrated.merged_consolidated(&live, &schema),
        })
    }
}

/// Tripwire: the replay's per-PK weight sum must equal the coarse weight the
/// skeleton row carried, by linearity of the PK projection. `keys` and `out` are
/// both ascending, so one co-walk checks every key and catches a PK no key named.
#[cfg(debug_assertions)]
fn assert_hydration_matches(out: &Batch, keys: &PkKeys, coarse: &[i64]) {
    assert_eq!(keys.len(), coarse.len());
    let mut expected = keys.iter().zip(coarse).peekable();
    let mut i = 0;
    while i < out.len() {
        let pk = out.get_pk_bytes(i);
        if let Some((key, _)) = expected.next_if(|(key, _)| crate::schema::key::compare_pk_bytes(key, pk).is_lt()) {
            panic!("hydration produced no rows for skeleton key {key:?}");
        }
        let Some((_, &weight)) = expected.next().filter(|(key, _)| *key == pk) else {
            panic!("hydration produced rows for a PK no skeleton row named");
        };
        let j = crate::storage::pk_group_end(out, i);
        let sum = out.as_mem_batch().sum_weights(i, j);
        assert_eq!(sum, weight, "hydration weight mismatch for key {pk:?}");
        i = j;
    }
    assert!(
        expected.next().is_none(),
        "hydration produced no rows for a trailing skeleton key"
    );
}

/// Fold every surviving chunk, through `map` when there is one, into partial
/// reduce-output rows; `Err` once the per-worker group cap is exceeded.
fn run_fold_sink(
    rows: &mut Survivors,
    chunk_rows: usize,
    map: Option<&mut MapPlan>,
    mut fold: AdhocFold,
) -> Result<Batch, String> {
    // One mapped batch for the whole scan: `clear` keeps its buffers.
    let mut map = map.map(|p| {
        let dst = Batch::empty_with_schema(p.out_schema());
        (p, dst)
    });
    while let Some((chunk, ranges)) = rows.next(chunk_rows)? {
        match &mut map {
            Some((plan, dst)) => {
                dst.clear();
                plan.append_map_ranges(&chunk, dst, ranges);
                // The map already dropped the non-survivors, so every row of
                // `dst` folds.
                fold.fold_ranges(dst, &[(0, dst.count)])?;
            }
            None => fold.fold_ranges(&chunk, ranges)?,
        }
    }
    Ok(fold.finish())
}

/// Append one chunk's survivor ranges onto the keeper — straight, or through the
/// map. There is no intermediate survivor batch and no mapped batch.
fn append_survivors(map: Option<&mut MapPlan>, chunk: &Batch, keeper: &mut Batch, ranges: &[(usize, usize)]) {
    match map {
        None => keeper.append_ranges(&chunk.as_mem_batch(), ranges),
        Some(p) => p.append_map_ranges(chunk, keeper, ranges),
    }
}

/// The rows sink with no ORDER BY: append survivors until their summed weight
/// reaches a `window > 0`, cutting the range list at the range that covers it.
fn stream_rows(
    rows: &mut Survivors,
    chunk_rows: usize,
    mut map: Option<&mut MapPlan>,
    keeper_schema: &SchemaDescriptor,
    window: i64,
) -> Result<Batch, String> {
    let early_stop = window > 0;
    let first = match early_stop {
        true => (window as usize).clamp(1, chunk_rows),
        false => chunk_rows,
    };

    let mut keeper = Batch::empty_with_schema(keeper_schema);
    let mut summed: i64 = 0;

    for drain_rows in drain_ramp(first, chunk_rows) {
        let Some((chunk, ranges)) = rows.next(drain_rows)? else {
            break;
        };
        let mb = chunk.as_mem_batch();
        if early_stop {
            // Cut at the row whose weight reaches the window: a range, or a hydrated group, can
            // run far past it.
            for i in 0..ranges.len() {
                let (s, e) = ranges[i];
                let range_sum = mb.sum_weights(s, e);
                if summed + range_sum < window {
                    summed += range_sum;
                    continue;
                }
                let mut end = s;
                while summed < window && end < e {
                    summed += mb.get_weight(end);
                    end += 1;
                }
                ranges[i].1 = end;
                ranges.truncate(i + 1);
                break;
            }
        }
        append_survivors(map.as_deref_mut(), &chunk, &mut keeper, ranges);
        if early_stop && summed >= window {
            break;
        }
    }
    Ok(keeper)
}

/// Drain sizes doubling from `first`, each cut at the next multiple of
/// `chunk_rows`, so the drain crosses every boundary a flat `chunk_rows` drain does.
fn drain_ramp(first: usize, chunk_rows: usize) -> impl Iterator<Item = usize> {
    let (mut step, mut drained) = (first, 0usize);
    std::iter::from_fn(move || {
        let rows = step.min(chunk_rows - drained % chunk_rows);
        drained += rows;
        step = step.saturating_mul(2);
        Some(rows)
    })
}

/// The rows sink with an ORDER BY and a `window > 0`: append every survivor and
/// trim the keeper back down with [`topk_keep`], at two thresholds.
fn topk_rows(
    rows: &mut Survivors,
    chunk_rows: usize,
    mut map: Option<&mut MapPlan>,
    keeper_schema: &SchemaDescriptor,
    order: &[OrderLocator],
    window: i64,
) -> Result<Batch, String> {
    // Mid-scan the keeper is still growing, so a trim at `window` would re-sort
    // after every chunk to shed rows the next chunk replaces. Saturating: an
    // unbounded `limit_k` leaves this unfireable rather than overflowing.
    let residency_cap = window.saturating_mul(2);
    let mut keeper = Batch::empty_with_schema(keeper_schema);
    // Summed survivor weight of what the keeper currently holds.
    let mut summed: i64 = 0;

    while let Some((chunk, ranges)) = rows.next(chunk_rows)? {
        // Weighed off the source — the same weights that land in the keeper,
        // read from a contiguous region rather than row-by-row off the
        // destination.
        let mb = chunk.as_mem_batch();
        summed = ranges
            .iter()
            .fold(summed, |a, &(s, e)| a.wrapping_add(mb.sum_weights(s, e)));
        append_survivors(map.as_deref_mut(), &chunk, &mut keeper, ranges);
        if summed > residency_cap {
            (keeper, summed) = topk_keep(keeper, order, window);
        }
    }
    // The keeper IS the reply now, and a rows reply carrying a STRING/BLOB column
    // goes out as one frame — so shed down to the smallest superset the client
    // can still cut exactly. At or below the window it provably cuts nothing.
    if summed > window {
        (keeper, _) = topk_keep(keeper, order, window);
    }
    Ok(keeper)
}

/// The comparator-smallest rows of `keeper` whose summed weight covers `window`,
/// in no particular order, and that weight. The boundary row stays whole: only
/// the client, which sees every worker's rows, may clip it.
fn topk_keep(keeper: Batch, order: &[OrderLocator], window: i64) -> (Batch, i64) {
    if keeper.count == 0 {
        return (keeper, 0);
    }
    let mut perm: Vec<u32> = (0..keeper.count as u32).collect();
    let cmp = |a: &u32, b: &u32| {
        let (ra, rb) = (*a as usize, *b as usize);
        cmp_order_keys(order, &keeper, ra, &keeper, rb)
    };
    let k = (window as usize).min(perm.len());
    if k < perm.len() {
        perm.select_nth_unstable_by(k - 1, cmp);
        perm.truncate(k);
    }
    let mut acc: i64 = perm.iter().map(|&r| keeper.get_weight(r as usize)).sum();
    // Every row weighs ≥ 1, so the k selected rows cover the window; a surplus is
    // the only way a comparator-larger row among them is droppable.
    if acc > window {
        perm.sort_unstable_by(cmp);
        acc = 0;
        let cut = perm
            .iter()
            .position(|&r| {
                acc += keeper.get_weight(r as usize);
                acc >= window
            })
            .map_or(perm.len(), |i| i + 1);
        perm.truncate(cut);
    }
    (keeper.indexed_rows(&perm), acc)
}

#[cfg(test)]
#[path = "tests/scan_spec.rs"]
mod tests;
