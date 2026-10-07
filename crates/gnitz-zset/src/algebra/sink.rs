//! [`SinkPlan`]: the sink of an ad-hoc read over one worker's surviving rows —
//! the map, then a forward, a top-k or a fold.

use gnitz_expr::{order_locators, Lead, OrderLocator, RowRanking};
use std::num::NonZeroI64;

use gnitz_wire::{ReadSink, RowsCut, SinkKind};

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
    /// Forward rows: every survivor, or the ones `cut` keeps.
    Rows { keeper: Batch, cut: Option<Cut> },
    /// Partial reduce-output rows. `mapped` is the one mapped batch of the whole
    /// scan: `clear` keeps its buffers.
    Fold {
        fold: Box<AdhocFold>,
        mapped: Option<Batch>,
    },
}

/// A rows sink's weight `window`. With no `order`, survivors until their summed
/// weight reaches the window; with one, every survivor, trimmed back down with
/// [`topk_keep`] at two thresholds.
struct Cut {
    order: Vec<OrderLocator>,
    window: NonZeroI64,
    /// The survivor weight the keeper currently holds.
    summed: i64,
    /// The largest lead the last trim kept: a later row above it is outside the window.
    bound: Option<Lead>,
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
            SinkKind::Rows { cut } => {
                let cut = match cut {
                    None => None,
                    Some(RowsCut { k, order }) => {
                        sink_in.check_cols(order.iter().map(|k| ("scan_spec: order key column", k.col as u32)))?;
                        Some(Cut {
                            order: order_locators(order, &sink_in),
                            // Saturated: a wrapped negative window would truncate
                            // the answer.
                            window: NonZeroI64::try_from(*k).unwrap_or(NonZeroI64::MAX),
                            summed: 0,
                            bound: None,
                        })
                    }
                };
                Kind::Rows {
                    keeper: Batch::empty_with_schema(&sink_in),
                    cut,
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
            Kind::Rows { cut: Some(cut), .. } if cut.order.is_empty() => (cut.window.get() as usize).min(chunk_rows),
            _ => chunk_rows,
        }
    }

    /// Take the surviving `[start, end)` row `ranges` of one source chunk,
    /// cutting the list where a window ends. `Ok(true)` once the sink needs no
    /// more rows; `Err` past a fold's group cap.
    pub fn push(&mut self, chunk: &Batch, ranges: &mut Vec<(usize, usize)>) -> Result<bool, String> {
        let mb = chunk.as_mem_batch();
        match &mut self.kind {
            Kind::Rows { keeper, cut: None } => {
                append_survivors(self.map.as_mut(), chunk, keeper, ranges);
                Ok(false)
            }
            Kind::Rows { keeper, cut: Some(cut) } if cut.order.is_empty() => {
                let window = cut.window.get();
                // Cut at the row whose weight reaches the window: a range, or a
                // hydrated group, can run far past it.
                for i in 0..ranges.len() {
                    let (s, e) = ranges[i];
                    let range_sum = mb.sum_weights(s, e);
                    if cut.summed + range_sum < window {
                        cut.summed += range_sum;
                        continue;
                    }
                    let mut end = s;
                    while cut.summed < window && end < e {
                        cut.summed += mb.get_weight(end);
                        end += 1;
                    }
                    ranges[i].1 = end;
                    ranges.truncate(i + 1);
                    break;
                }
                append_survivors(self.map.as_mut(), chunk, keeper, ranges);
                Ok(cut.summed >= window)
            }
            Kind::Rows { keeper, cut: Some(cut) } => {
                // Weighed off the source — the same weights that land in the
                // keeper, read from a contiguous region rather than row-by-row
                // off the destination.
                cut.summed = ranges
                    .iter()
                    .fold(cut.summed, |a, &(s, e)| a.wrapping_add(mb.sum_weights(s, e)));
                append_survivors(self.map.as_mut(), chunk, keeper, ranges);
                // Mid-scan the keeper is still growing, so a trim at `window`
                // would re-sort after every chunk to shed rows the next chunk
                // replaces.
                if cut.summed > cut.window.get().saturating_mul(2) {
                    cut.summed = topk_keep(keeper, &cut.order, cut.window, &mut cut.bound);
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
            Kind::Rows { mut keeper, cut } => {
                // The keeper IS the reply now, and a rows reply carrying a
                // STRING/BLOB column goes out as one frame — so shed an ordered
                // keeper down to the smallest superset the client can still cut
                // exactly. At or below the window it provably cuts nothing.
                if let Some(mut cut) = cut.filter(|c| !c.order.is_empty() && c.summed > c.window.get()) {
                    topk_keep(&mut keeper, &cut.order, cut.window, &mut cut.bound);
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
/// `bound` carries the largest lead kept from one trim to the next.
fn topk_keep(keeper: &mut Batch, order: &[OrderLocator], window: NonZeroI64, bound: &mut Option<Lead>) -> i64 {
    if keeper.is_empty() {
        return 0;
    }
    let window = window.get();
    let mut ranking = RowRanking::new(order, &*keeper);
    if let Some(bound) = *bound {
        ranking.drop_above(bound);
    }
    ranking.keep_smallest(window as usize);
    *bound = ranking.max_lead();
    let mut acc: i64 = ranking.rows().map(|r| keeper.get_weight(r as usize)).sum();
    // Every row weighs ≥ 1, so the kept rows cover the window; a surplus is the only way a
    // comparator-larger row among them is droppable.
    let perm = if acc > window {
        let mut perm = ranking.sorted();
        acc = 0;
        let cut = perm.iter().position(|&r| {
            acc += keeper.get_weight(r as usize);
            acc >= window
        });
        perm.truncate(cut.map_or(perm.len(), |i| i + 1));
        perm
    } else {
        ranking.rows().collect()
    };
    *keeper = keeper.indexed_rows(&perm);
    acc
}
