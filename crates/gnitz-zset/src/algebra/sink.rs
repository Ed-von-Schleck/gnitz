//! [`SinkPlan`]: the sink of an ad-hoc read over one worker's surviving rows —
//! the map, then a forward, a top-k or a fold.

use gnitz_expr::{cmp_order_keys, order_locators, OrderLocator};
use gnitz_wire::{ReadSink, SinkKind};

use super::aggregate::AdhocFold;
use super::map::MapPlan;
use crate::repr::Batch;
use crate::schema::SchemaDescriptor;

/// A read sink resolved against its source schema, and the reply it is building.
pub struct SinkPlan {
    map: Option<MapPlan>,
    kind: Kind,
}

enum Kind {
    /// Forward rows under a weight `window`, `0` = unbounded. With no `order`,
    /// survivors until their summed weight reaches the window; with one — legal
    /// only under a `window > 0` — every survivor, trimmed back down with
    /// [`topk_keep`] at two thresholds. `summed` is the survivor weight `keeper`
    /// currently holds.
    Rows {
        keeper: Batch,
        order: Vec<OrderLocator>,
        window: i64,
        summed: i64,
    },
    /// Partial reduce-output rows. `mapped` is the one mapped batch of the whole
    /// scan: `clear` keeps its buffers.
    Fold {
        fold: Box<AdhocFold>,
        mapped: Option<Batch>,
    },
}

impl SinkPlan {
    /// `sink` over `src_schema`, refused for a sink the schema cannot serve — a
    /// corrupt request: the client pre-compiled the identical program at plan
    /// time. `group_cap` bounds a fold's distinct groups.
    pub fn from_wire(src_schema: &SchemaDescriptor, sink: &ReadSink, group_cap: usize) -> Result<Self, String> {
        let map = sink
            .map
            .as_ref()
            .map(|m| MapPlan::from_compute_map(src_schema, m))
            .transpose()
            .map_err(|e| format!("scan_spec map: {e}"))?;
        let sink_in = map.as_ref().map_or(*src_schema, |m| *m.out_schema());
        let kind = match &sink.kind {
            SinkKind::Fold(agg) => Kind::Fold {
                fold: Box::new(AdhocFold::new(&sink_in, agg, group_cap)?),
                mapped: map.as_ref().map(|m| Batch::empty_with_schema(m.out_schema())),
            },
            SinkKind::Rows { order, limit_k } => {
                // `0` = unbounded. Saturated: a wrapped negative window would
                // truncate the answer.
                let window = (*limit_k).min(i64::MAX as u64) as i64;
                debug_assert!(
                    order.is_empty() || window > 0,
                    "decode admits an order only under a cut"
                );
                sink_in.check_cols(order.iter().map(|k| ("scan_spec: order key column", k.col as u32)))?;
                Kind::Rows {
                    keeper: Batch::empty_with_schema(&sink_in),
                    order: order_locators(order, &sink_in),
                    window,
                    summed: 0,
                }
            }
        };
        Ok(SinkPlan { map, kind })
    }

    /// The layout [`Self::finish`] emits.
    pub fn output_schema(&self) -> &SchemaDescriptor {
        match &self.kind {
            Kind::Rows { keeper, .. } => keeper.schema(),
            Kind::Fold { fold, .. } => fold.output_schema(),
        }
    }

    /// How many source rows the first drain should visit, of at most
    /// `chunk_rows`: an unordered cut needs no more than its window.
    pub fn first_drain(&self, chunk_rows: usize) -> usize {
        match &self.kind {
            Kind::Rows { order, window, .. } if order.is_empty() && *window > 0 => {
                (*window as usize).clamp(1, chunk_rows)
            }
            _ => chunk_rows,
        }
    }

    /// Take the surviving `[start, end)` row `ranges` of one source chunk,
    /// cutting the list where a window ends. `Ok(true)` once the sink needs no
    /// more rows; `Err` past a fold's group cap.
    pub fn push(&mut self, chunk: &Batch, ranges: &mut Vec<(usize, usize)>) -> Result<bool, String> {
        let mb = chunk.as_mem_batch();
        match &mut self.kind {
            Kind::Rows { keeper, order, window, summed } if order.is_empty() => {
                let window = *window;
                if window > 0 {
                    // Cut at the row whose weight reaches the window: a range, or
                    // a hydrated group, can run far past it.
                    for i in 0..ranges.len() {
                        let (s, e) = ranges[i];
                        let range_sum = mb.sum_weights(s, e);
                        if *summed + range_sum < window {
                            *summed += range_sum;
                            continue;
                        }
                        let mut end = s;
                        while *summed < window && end < e {
                            *summed += mb.get_weight(end);
                            end += 1;
                        }
                        ranges[i].1 = end;
                        ranges.truncate(i + 1);
                        break;
                    }
                }
                append_survivors(self.map.as_mut(), chunk, keeper, ranges);
                Ok(window > 0 && *summed >= window)
            }
            Kind::Rows { keeper, order, window, summed } => {
                // Weighed off the source — the same weights that land in the
                // keeper, read from a contiguous region rather than row-by-row
                // off the destination.
                *summed = ranges
                    .iter()
                    .fold(*summed, |a, &(s, e)| a.wrapping_add(mb.sum_weights(s, e)));
                append_survivors(self.map.as_mut(), chunk, keeper, ranges);
                // Mid-scan the keeper is still growing, so a trim at `window`
                // would re-sort after every chunk to shed rows the next chunk
                // replaces. Saturating: an unbounded window leaves this
                // unfireable rather than overflowing.
                if *summed > window.saturating_mul(2) {
                    *summed = topk_keep(keeper, order, *window);
                }
                Ok(false)
            }
            Kind::Fold { fold, mapped } => {
                match (&mut self.map, mapped) {
                    (Some(plan), Some(dst)) => {
                        dst.clear();
                        plan.append_map_ranges(chunk, dst, ranges);
                        // The map already dropped the non-survivors, so every
                        // row of `dst` folds.
                        fold.fold_ranges(dst, &[(0, dst.len())])?;
                    }
                    _ => fold.fold_ranges(chunk, ranges)?,
                }
                Ok(false)
            }
        }
    }

    /// The reply: the kept rows, or one partial row per group.
    pub fn finish(self) -> Batch {
        match self.kind {
            Kind::Rows { mut keeper, order, window, summed } => {
                // The keeper IS the reply now, and a rows reply carrying a
                // STRING/BLOB column goes out as one frame — so shed an ordered
                // keeper down to the smallest superset the client can still cut
                // exactly. At or below the window it provably cuts nothing.
                if !order.is_empty() && summed > window {
                    topk_keep(&mut keeper, &order, window);
                }
                keeper
            }
            Kind::Fold { fold, .. } => fold.finish(),
        }
    }
}

/// Append one chunk's survivor ranges onto the keeper — straight, or through the
/// map. There is no intermediate survivor batch and no mapped batch.
fn append_survivors(map: Option<&mut MapPlan>, chunk: &Batch, keeper: &mut Batch, ranges: &[(usize, usize)]) {
    match map {
        None => keeper.append_ranges(&chunk.as_mem_batch(), ranges),
        Some(p) => p.append_map_ranges(chunk, keeper, ranges),
    }
}

/// Trim `keeper` to its comparator-smallest rows whose summed weight covers
/// `window`, in no particular order, and return that weight. The boundary row
/// stays whole: only the client, which sees every worker's rows, may clip it.
fn topk_keep(keeper: &mut Batch, order: &[OrderLocator], window: i64) -> i64 {
    if keeper.is_empty() {
        return 0;
    }
    let mut perm: Vec<u32> = (0..keeper.len() as u32).collect();
    let cmp = |a: &u32, b: &u32| {
        let (ra, rb) = (*a as usize, *b as usize);
        cmp_order_keys(order, &*keeper, ra, &*keeper, rb)
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
    *keeper = keeper.indexed_rows(&perm);
    acc
}
