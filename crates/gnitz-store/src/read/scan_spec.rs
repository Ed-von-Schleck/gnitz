//! The worker half of an ad-hoc `ReadSpec` read, over this worker's slice: open
//! the bound, filter, map, then forward rows or fold them. A capacity-bounded
//! view's rows hydrate chunk by chunk as the sink drains them.

use gnitz_wire::{Cut, OrderKey, RangeDescriptor, ReadBound, ReadSpec, SinkKind};

use std::rc::Rc;

use super::store_io::LiveSource;
use super::SkeletonHydrator;
use crate::expr::MapPlan;
use crate::ops::AdhocFold;
use crate::relation::RelationRegistry;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, SourceCursor, StoreError};
use gnitz_expr::{cmp_order_keys, push_identity_tiebreak, Evaluator, LogicalProgram, OrderLocator};

/// The sink a request resolved to, ahead of any walk.
enum Sink {
    Fold(Box<AdhocFold>),
    /// ORDER BY locators over the sink input, tiebreak appended when any key is
    /// present, and the summed-weight window.
    Rows {
        order: Vec<OrderLocator>,
        window: i64,
    },
}

impl RelationRegistry {
    /// Execute `spec` on this worker's slice, replying in `reply_schema`'s layout.
    pub fn scan_spec(
        &self,
        target_id: i64,
        spec: ReadSpec,
        reply_schema: &SchemaDescriptor,
        hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<Rc<Batch>, StoreError> {
        let ReadSpec { bound, predicate, sink } = spec;
        let src_schema = self.relation_or_err(target_id)?.schema();
        // Nothing bounded, filtered, mapped or cut (a worker orders only under a cut): the
        // relation whole, off the store's cached snapshot.
        if let (ReadBound::None, true, None, SinkKind::Rows { order, limit_k: 0 }) =
            (&bound, predicate.is_empty(), &sink.map, &sink.kind)
        {
            resolve_order_locs(order, &src_schema)?;
            if !reply_schema.same_physical_layout(&src_schema) {
                return Err(layout_mismatch());
            }
            return self.scan(target_id, hydrator);
        }
        let source = self.open_bound(target_id, bound)?;
        let predicate = (!predicate.is_empty())
            .then(|| compile_predicate(&predicate, &src_schema))
            .transpose()?;
        let map = sink
            .map
            .as_ref()
            .map(|m| MapPlan::from_compute_map(&src_schema, m))
            .transpose()
            .map_err(|e| StoreError::rejected(format!("scan_spec map: {e}")))?;
        let sink_in = map.as_ref().map_or(src_schema, |m| *m.out_schema());
        let sink = match &sink.kind {
            SinkKind::Fold(agg) => Sink::Fold(Box::new(AdhocFold::new(&sink_in, agg, self.config.adhoc_group_cap)?)),
            SinkKind::Rows { order, limit_k } => Sink::Rows {
                order: resolve_order_locs(order, &sink_in)?,
                window: saturated_window(*limit_k),
            },
        };
        let produced = match &sink {
            Sink::Fold(f) => f.output_schema(),
            Sink::Rows { .. } => &sink_in,
        };
        if !reply_schema.same_physical_layout(produced) {
            return Err(layout_mismatch());
        }
        let mut rows = Survivors {
            source: LiveSource::new(self, target_id, source, hydrator),
            predicate,
            ranges: Vec::new(),
        };
        let chunk_rows = self.config.scan_chunk_rows;
        Ok(Rc::new(match sink {
            Sink::Fold(fold) => run_fold_sink(&mut rows, chunk_rows, map.as_ref(), *fold)?,
            Sink::Rows { order, window } if !order.is_empty() && window > 0 => {
                topk_rows(&mut rows, chunk_rows, map.as_ref(), &sink_in, &order, window)?
            }
            Sink::Rows { window, .. } => stream_rows(&mut rows, chunk_rows, map.as_ref(), &sink_in, window)?,
        }))
    }

    /// Every delta `id`'s feed recorded in rounds `(after_tick, cut_tick]`, in the
    /// delta store's schema — or, at `after_tick = 0`, the view's own output store.
    pub fn delta_read(
        &self,
        id: i64,
        after_tick: u64,
        cut_tick: u64,
        reply_schema: &SchemaDescriptor,
    ) -> Result<Rc<Batch>, StoreError> {
        let entry = self.relation_or_err(id)?;
        if !entry.has_delta_feed() {
            return Err(StoreError::rejected(format!(
                "delta_read: relation {id} carries no delta feed; \
                 create the view WITH (delta = '<size>') to subscribe to it"
            )));
        }
        // Everything after round 0 is the output store itself. A fresh cursor: the
        // cached snapshot would pin a second copy of the view until its next ingest.
        if after_tick == 0 {
            if !reply_schema.same_physical_layout(&entry.schema()) {
                return Err(layout_mismatch());
            }
            // A fed view carries no capacity, so no skeleton row needs hydrating.
            return Ok(entry.cursor().materialize());
        }
        let feed = entry.delta_or_err()?;
        let dropped_through = feed.dropped_through();
        if delta_cursor_expired(after_tick, dropped_through) {
            return Err(StoreError::DeltaExpired(format!(
                "delta cursor {after_tick} of relation {id} is below the \
                 retained floor {dropped_through}; re-read at 0"
            )));
        }
        let schema = feed.schema();
        if !reply_schema.same_physical_layout(&schema) {
            return Err(layout_mismatch());
        }
        // Both ends `Cut::After`: no arithmetic on the client's `after_tick`,
        // where `after_tick + 1` at `u64::MAX` would wrap.
        let desc = RangeDescriptor::new(&[], Cut::After(after_tick as u128), Cut::After(cut_tick as u128));
        let cursor = feed.pk_range_cursor(&desc).map_err(StoreError::rejected)?;
        let mut rows = Survivors {
            source: LiveSource::new(self, id, SourceCursor::Full(Box::new(cursor)), None),
            predicate: None,
            ranges: Vec::new(),
        };
        Ok(Rc::new(stream_rows(
            &mut rows,
            self.config.scan_chunk_rows,
            None,
            &schema,
            0,
        )?))
    }
}

/// The reply guard's refusal: a keeper built in any other layout would ship its
/// regions under the client's strides.
fn layout_mismatch() -> StoreError {
    StoreError::rejected("reply schema does not match the output layout")
}

/// Whether a walk over rounds `(after_tick, cut]` misses a round this worker
/// dropped: a cursor at the floor has lost nothing.
fn delta_cursor_expired(after_tick: u64, dropped_through: u64) -> bool {
    after_tick < dropped_through
}

/// The client's `limit_k` (`0` = unbounded) as an `i64` weight window, saturated:
/// a wrapped negative window would truncate the answer.
fn saturated_window(limit_k: u64) -> i64 {
    limit_k.min(i64::MAX as u64) as i64
}

/// A source chunk and its surviving row ranges.
type SurvivorChunk<'r> = (Batch, &'r mut Vec<(usize, usize)>);

/// The rows surviving the bound and the predicate, one source chunk at a time —
/// the one input every sink reads.
struct Survivors<'a, 'h> {
    source: LiveSource<'a, 'h>,
    predicate: Option<Evaluator>,
    /// The current chunk's surviving row ranges; scratch reused across chunks.
    ranges: Vec<(usize, usize)>,
}

impl Survivors<'_, '_> {
    /// The next non-empty source chunk and its surviving row ranges — the whole
    /// chunk when there is no predicate — or `None` once the source is exhausted.
    fn next(&mut self, max_rows: usize) -> Result<Option<SurvivorChunk<'_>>, StoreError> {
        let chunk = loop {
            let Some(chunk) = self.source.next_chunk(max_rows)? else {
                return Ok(None);
            };
            if chunk.count > 0 {
                break chunk;
            }
        };
        match &self.predicate {
            Some(f) => f.filter_ranges(&chunk.as_mem_batch(), &mut self.ranges),
            None => {
                self.ranges.clear();
                self.ranges.push((0, chunk.count));
            }
        }
        Ok(Some((chunk, &mut self.ranges)))
    }
}

/// Fold every surviving chunk, through `map` when there is one, into partial
/// reduce-output rows; `Err` once the per-worker group cap is exceeded.
fn run_fold_sink(
    rows: &mut Survivors,
    chunk_rows: usize,
    map: Option<&MapPlan>,
    mut fold: AdhocFold,
) -> Result<Batch, StoreError> {
    // One mapped batch for the whole scan: `clear` keeps its buffers.
    let mut map = map.map(|p| (p, Batch::empty_with_schema(p.out_schema())));
    while let Some((chunk, ranges)) = rows.next(chunk_rows)? {
        match &mut map {
            Some((plan, dst)) => {
                dst.clear();
                plan.append_map_ranges(&chunk, dst, ranges);
                // The map already dropped the non-survivors, so every row of
                // `dst` folds — and a chunk none survived maps to nothing.
                if dst.count > 0 {
                    fold.fold_ranges(dst, &[(0, dst.count)])?;
                }
            }
            None => fold.fold_ranges(&chunk, ranges)?,
        }
    }
    Ok(fold.finish())
}

/// Append one chunk's survivor ranges onto the keeper — straight, or through the
/// map. There is no intermediate survivor batch and no mapped batch.
fn append_survivors(map: Option<&MapPlan>, chunk: &Batch, keeper: &mut Batch, ranges: &[(usize, usize)]) {
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
    map: Option<&MapPlan>,
    keeper_schema: &SchemaDescriptor,
    window: i64,
) -> Result<Batch, StoreError> {
    let early_stop = window > 0;
    // `window`-sized chunks make the early stop O(window) rather than O(chunk),
    // but only while every drained row survives: with a predicate a tiny chunk
    // degrades to row-at-a-time cursor driving, so over-read a full one instead.
    let drain_rows = match early_stop && rows.predicate.is_none() {
        true => (window as usize).clamp(1, chunk_rows),
        false => chunk_rows,
    };

    let mut keeper = Batch::empty_with_schema(keeper_schema);
    let mut summed: i64 = 0;

    while let Some((chunk, ranges)) = rows.next(drain_rows)? {
        if early_stop {
            // Cut at the row whose weight reaches the window: a range, or a hydrated group, can
            // run far past it.
            for i in 0..ranges.len() {
                let (s, e) = ranges[i];
                let range_sum = chunk.sum_weights(s, e);
                if summed + range_sum < window {
                    summed += range_sum;
                    continue;
                }
                let mut end = s;
                while summed < window && end < e {
                    summed += chunk.get_weight(end);
                    end += 1;
                }
                ranges[i].1 = end;
                ranges.truncate(i + 1);
                break;
            }
        }
        append_survivors(map, &chunk, &mut keeper, ranges);
        if early_stop && summed >= window {
            break;
        }
    }
    Ok(keeper)
}

/// The rows sink with an ORDER BY and a `window > 0`: append every survivor and
/// trim the keeper back down with [`topk_keep`], at two thresholds.
fn topk_rows(
    rows: &mut Survivors,
    chunk_rows: usize,
    map: Option<&MapPlan>,
    keeper_schema: &SchemaDescriptor,
    order: &[OrderLocator],
    window: i64,
) -> Result<Batch, StoreError> {
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
        summed += ranges.iter().map(|&(s, e)| chunk.sum_weights(s, e)).sum::<i64>();
        append_survivors(map, &chunk, &mut keeper, ranges);
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
        cmp_order_keys(
            order,
            &keeper,
            ra,
            keeper.get_null_word(ra),
            &keeper,
            rb,
            keeper.get_null_word(rb),
        )
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
    (
        Batch::from_indexed_rows(&keeper.as_mem_batch(), &perm, keeper.schema()),
        acc,
    )
}

/// Resolve each ORDER BY key over the sink input — a forged column is a
/// rejection — then append the identity tiebreak.
fn resolve_order_locs(order: &[OrderKey], schema: &SchemaDescriptor) -> Result<Vec<OrderLocator>, StoreError> {
    let mut locs = order
        .iter()
        .map(|k| match schema.try_locate(k.col as usize) {
            Some(loc) => Ok(OrderLocator::of(loc, k)),
            None => Err(StoreError::rejected(format!(
                "scan_spec: order key column {} out of range ({} cols)",
                k.col,
                schema.num_columns()
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !locs.is_empty() {
        push_identity_tiebreak(&mut locs, schema);
    }
    Ok(locs)
}

/// Decode + validate a client predicate blob against `schema`, then build its
/// predicate `Evaluator` — the same path the circuit compiler runs. Any failure
/// is a corrupt frame (the client pre-compiled the identical program at plan time).
fn compile_predicate(blob: &[u8], schema: &SchemaDescriptor) -> Result<Evaluator, StoreError> {
    LogicalProgram::from_blob(blob, "scan_spec predicate")
        .and_then(|p| p.resolve_filter(schema))
        .map_err(|e| StoreError::rejected(format!("scan_spec: invalid predicate program: {e}")))
}

#[cfg(test)]
#[path = "tests/scan_spec.rs"]
mod tests;
