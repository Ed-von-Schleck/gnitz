//! The worker half of an ad-hoc `ReadSpec` read, over this worker's slice: open
//! the bound, filter, map, then forward rows or fold them. Hydrating a
//! capacity-bounded view runs after every check.

use gnitz_wire::{Cut, OrderKey, RangeDescriptor, ReadBound, ReadSpec, SinkKind};

use std::rc::Rc;

use super::SkeletonHydrator;
use crate::expr::MapPlan;
use crate::ops::AdhocFold;
use crate::relation::RelationRegistry;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, PkSetGather, ReadCursor, SourceCursor, StoreError};
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
    /// `cut_tick` bounds a delta read above; every other bound ignores it.
    pub fn scan_spec(
        &self,
        target_id: i64,
        spec: &ReadSpec,
        reply_schema: &SchemaDescriptor,
        cut_tick: u64,
        hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<Batch, StoreError> {
        let (source, src_schema) = self.open_bound(target_id, &spec.bound, cut_tick)?;
        let predicate = (!spec.predicate.is_empty())
            .then(|| compile_predicate(&spec.predicate, &src_schema))
            .transpose()?;
        let map = spec
            .sink
            .map
            .as_ref()
            .map(|m| MapPlan::from_compute_map(&src_schema, m))
            .transpose()
            .map_err(|e| StoreError::rejected(format!("scan_spec map: {e}")))?;
        let sink_in = map.as_ref().map_or(src_schema, |m| *m.out_schema());
        let sink = match &spec.sink.kind {
            SinkKind::Fold(agg) => Sink::Fold(Box::new(AdhocFold::new(&sink_in, agg, self.config.adhoc_group_cap)?)),
            SinkKind::Rows { order, limit_k } => Sink::Rows {
                order: resolve_order_locs(order, &sink_in)?,
                window: saturated_window(*limit_k),
            },
        };
        // The one reply guard: a keeper built in any other layout ships its
        // regions under the client's strides.
        let produced = match &sink {
            Sink::Fold(f) => f.output_schema(),
            Sink::Rows { .. } => &sink_in,
        };
        if !reply_schema.same_physical_layout(produced) {
            return Err(StoreError::rejected(
                "scan_spec: reply schema does not match the sink's output layout",
            ));
        }
        let source = self.hydrated(target_id, source, hydrator)?;
        let mut rows = Survivors { source, predicate, ranges: Vec::new() };
        let chunk_rows = self.config.scan_chunk_rows;
        Ok(match sink {
            Sink::Fold(fold) => run_fold_sink(&mut rows, chunk_rows, map.as_ref(), *fold)?,
            Sink::Rows { order, window } if !order.is_empty() && window > 0 => {
                topk_rows(&mut rows, chunk_rows, map.as_ref(), &sink_in, &order, window)
            }
            Sink::Rows { window, .. } => stream_rows(&mut rows, chunk_rows, map.as_ref(), &sink_in, window),
        })
    }

    /// Open `bound`'s source cursor, without walking it, and the schema its rows
    /// arrive in — for a delta read, the delta store's rather than the relation's.
    fn open_bound(
        &self,
        id: i64,
        bound: &ReadBound,
        cut_tick: u64,
    ) -> Result<(SourceCursor, SchemaDescriptor), StoreError> {
        let entry = self.relation_or_err(id)?;
        let full = |c: ReadCursor| SourceCursor::Full(Box::new(c));
        Ok(match bound {
            ReadBound::None => (full(entry.cursor()), entry.schema()),
            ReadBound::PkRange(desc) => {
                let schema = entry.schema();
                let c = entry.store().pk_range_cursor(desc).map_err(StoreError::rejected)?;
                (full(c), schema)
            }
            ReadBound::PkSet(keys) => {
                let schema = entry.schema();
                if keys.stride() != schema.pk_stride() {
                    return Err(StoreError::rejected(format!(
                        "scan_spec: PkSet key stride {} != pk_stride {} (table {id})",
                        keys.stride(),
                        schema.pk_stride()
                    )));
                }
                // A key this worker holds no row for copies nothing — the request
                // is broadcast, so at W workers most of the list belongs elsewhere.
                let gather = PkSetGather::open(keys.as_bytes().to_vec(), schema, |s, e| entry.cursor_in_range(s, e));
                (SourceCursor::PkSet(Box::new(gather)), schema)
            }
            ReadBound::IndexRange { bound, walk } => {
                let cols = self.bound_cols_against(id, bound.idx_cols, "scan_spec")?;
                (
                    self.open_index_source(id, cols.as_slice(), &bound.desc, *walk)?,
                    entry.schema(),
                )
            }
            // Refused at every `after_tick` when no feed exists: the reply would
            // promise a continuation the server cannot serve.
            ReadBound::Delta { .. } if !entry.has_delta_feed() => {
                return Err(StoreError::rejected(format!(
                    "scan_spec: relation {id} carries no delta feed; \
                     create the view WITH (delta = '<size>') to subscribe to it"
                )))
            }
            // The bootstrap: every delta after round 0 is the view's whole history,
            // which is what its own output store holds.
            ReadBound::Delta { after_tick: 0 } => (full(entry.cursor()), entry.schema()),
            ReadBound::Delta { after_tick } => {
                let feed = entry.delta_or_err()?;
                let dropped_through = feed.dropped_through();
                if delta_cursor_expired(*after_tick, dropped_through) {
                    return Err(StoreError::DeltaExpired(format!(
                        "delta cursor {after_tick} of relation {id} is below the \
                         retained floor {dropped_through}; re-read at 0"
                    )));
                }
                let schema = feed.schema();
                // Both ends `Cut::After`: no arithmetic on the client's
                // `after_tick`, where `after_tick + 1` at `u64::MAX` would wrap.
                let desc = RangeDescriptor::new(&[], Cut::After(*after_tick as u128), Cut::After(cut_tick as u128));
                let c = feed.pk_range_cursor(&desc).map_err(StoreError::rejected)?;
                (full(c), schema)
            }
        })
    }

    /// `source` unchanged unless the runs it opened hold a skeleton row, else
    /// hydrated and materialized whole.
    fn hydrated(
        &self,
        id: i64,
        source: SourceCursor,
        hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<SourceCursor, StoreError> {
        let (cursor, keys) = match source {
            SourceCursor::Full(c) if c.any_skeleton() => (*c, None),
            SourceCursor::PkSet(g) if g.any_skeleton() => {
                let (c, k) = g.into_parts();
                (c, Some(k))
            }
            // An index owner is a base table, which never holds a skeleton row.
            other => return Ok(other),
        };
        let schema = cursor.schema;
        let rows = self.materialize_hydrated(id, cursor, keys.as_deref(), hydrator)?;
        Ok(SourceCursor::Full(Box::new(ReadCursor::over_batches(
            &[Rc::new(rows)],
            schema,
        ))))
    }
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

/// The rows surviving the bound and the predicate, one source chunk at a time —
/// the one input every sink reads.
struct Survivors {
    source: SourceCursor,
    predicate: Option<Evaluator>,
    /// The current chunk's surviving row ranges; scratch reused across chunks.
    ranges: Vec<(usize, usize)>,
}

impl Survivors {
    /// The next non-empty source chunk and its surviving row ranges — the whole
    /// chunk when there is no predicate — or `None` once the source is exhausted.
    fn next(&mut self, max_rows: usize) -> Option<(Batch, &mut Vec<(usize, usize)>)> {
        let chunk = loop {
            let chunk = self.source.drain_chunk(max_rows)?;
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
        Some((chunk, &mut self.ranges))
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
    while let Some((chunk, ranges)) = rows.next(chunk_rows) {
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
) -> Batch {
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

    while let Some((chunk, ranges)) = rows.next(drain_rows) {
        if early_stop {
            // Weighed off the source — the same weights that land in the keeper,
            // read from a contiguous region rather than row-by-row off the
            // destination.
            for (i, &(s, e)) in ranges.iter().enumerate() {
                summed += chunk.sum_weights(s, e);
                if summed >= window {
                    ranges.truncate(i + 1);
                    break;
                }
            }
        }
        append_survivors(map, &chunk, &mut keeper, ranges);
        if early_stop && summed >= window {
            break;
        }
    }
    keeper
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
) -> Batch {
    // Mid-scan the keeper is still growing, so a trim at `window` would re-sort
    // after every chunk to shed rows the next chunk replaces. Saturating: an
    // unbounded `limit_k` leaves this unfireable rather than overflowing.
    let residency_cap = window.saturating_mul(2);
    let mut keeper = Batch::empty_with_schema(keeper_schema);
    // Summed survivor weight of what the keeper currently holds.
    let mut summed: i64 = 0;

    while let Some((chunk, ranges)) = rows.next(chunk_rows) {
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
    keeper
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
