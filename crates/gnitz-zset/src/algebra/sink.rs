//! [`SinkPlan`]: the sink of an ad-hoc read over one worker's surviving rows —
//! the map, then a forward, a prefix cut, a top-k or a fold.

use gnitz_expr::{order_locators, Lead, OrderLocator, RowRanking};

use gnitz_wire::{ReadSink, RowsCut, SinkKind};

use super::aggregate::AdhocFold;
use super::map::MapPlan;
use crate::repr::Batch;
use crate::schema::SchemaDescriptor;

/// A read sink resolved against its source schema, and the reply it is building.
pub struct SinkPlan {
    /// The map every survivor goes through first, if any.
    map: Option<MapPlan>,
    kind: Kind,
}

enum Kind {
    /// Forward rows: every survivor, or the ones `cut` keeps.
    Rows { keeper: Batch, cut: Option<Cut> },
    /// Partial reduce-output rows.
    Fold {
        /// The one mapped batch of the whole scan, used only under a map:
        /// `clear` keeps its buffers.
        mapped: Batch,
        fold: Box<AdhocFold>,
    },
}

/// A rows sink's weight window.
enum Cut {
    /// Survivors until their summed weight reaches the window; `left` is the
    /// weight still to reach.
    Prefix {
        left: i64,
    },
    TopK(TopK),
}

/// Every survivor, trimmed back down to the smallest under `order` whose summed
/// weight covers `window`.
struct TopK {
    order: Vec<OrderLocator>,
    window: i64,
    /// The weight the keeper holds.
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
            .transpose()?;
        let sink_in = map.as_ref().map_or(*src_schema, |m| *m.out_schema());
        let kind = match &sink.kind {
            SinkKind::Fold(agg) => Kind::Fold {
                fold: Box::new(AdhocFold::new(&sink_in, agg, group_cap)?),
                mapped: Batch::empty_with_schema(&sink_in),
            },
            SinkKind::Rows { cut } => {
                let cut = match cut {
                    None => None,
                    Some(RowsCut { k, order }) => {
                        for key in order {
                            sink_in.wire_col("order key column", key.col as u32)?;
                        }
                        // Saturated: a wrapped negative window would truncate the answer.
                        let window = i64::try_from(k.get()).unwrap_or(i64::MAX);
                        Some(if order.is_empty() {
                            Cut::Prefix { left: window }
                        } else {
                            Cut::TopK(TopK {
                                order: order_locators(order, &sink_in, true),
                                window,
                                summed: 0,
                                bound: None,
                            })
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
            Kind::Rows { cut: Some(Cut::Prefix { left }), .. } => (*left as usize).min(chunk_rows),
            _ => chunk_rows,
        }
    }

    /// Take the surviving `[start, end)` row `ranges` of one source chunk,
    /// cutting the last range a window ends in. `Ok(true)` once the sink needs
    /// no more rows; `Err` past a fold's group cap.
    pub fn push(&mut self, chunk: &Batch, ranges: &mut [(usize, usize)]) -> Result<bool, String> {
        let map = self.map.as_mut();
        match &mut self.kind {
            Kind::Rows { keeper, cut: None } => {
                append_survivors(map, chunk, keeper, ranges);
                Ok(false)
            }
            Kind::Rows { keeper, cut: Some(Cut::Prefix { left }) } => {
                let mb = chunk.as_mem_batch();
                // Cut at the row whose weight reaches the window: a range, or a
                // hydrated group, can run far past it.
                let mut taken = ranges.len();
                for (i, (s, e)) in ranges.iter_mut().enumerate() {
                    let range_sum = mb.sum_weights(*s, *e);
                    if range_sum < *left {
                        *left -= range_sum;
                        continue;
                    }
                    let mut end = *s;
                    while *left > 0 && end < *e {
                        *left -= mb.get_weight(end);
                        end += 1;
                    }
                    *e = end;
                    taken = i + 1;
                    break;
                }
                append_survivors(map, chunk, keeper, &ranges[..taken]);
                Ok(*left <= 0)
            }
            Kind::Rows { keeper, cut: Some(Cut::TopK(top)) } => {
                let before = keeper.len();
                append_survivors(map, chunk, keeper, ranges);
                top.summed = top
                    .summed
                    .saturating_add(keeper.as_mem_batch().sum_weights(before, keeper.len()));
                // Mid-scan the keeper is still growing, so a trim at `window`
                // would re-sort after every chunk to shed rows the next chunk
                // replaces.
                if top.summed > top.window.saturating_mul(2) {
                    top.trim(keeper);
                }
                Ok(false)
            }
            Kind::Fold { mapped, fold } => {
                match map {
                    Some(plan) => {
                        mapped.clear();
                        plan.append_map_ranges(chunk, mapped, ranges);
                        // The map already dropped the non-survivors, so every
                        // row of `mapped` folds.
                        fold.fold_ranges(mapped, &[(0, mapped.len())])?;
                    }
                    None => fold.fold_ranges(chunk, ranges)?,
                }
                Ok(false)
            }
        }
    }

    /// The reply: the kept rows, or one partial row per group.
    pub fn finish(self) -> Batch {
        match self.kind {
            Kind::Rows { mut keeper, cut, .. } => {
                // The keeper is the reply now: shed an ordered keeper down to
                // the smallest superset the client can still cut exactly. At or
                // below the window that cuts nothing.
                if let Some(Cut::TopK(mut top)) = cut {
                    if top.summed > top.window {
                        top.trim(&mut keeper);
                    }
                }
                keeper
            }
            Kind::Fold { fold, .. } => fold.finish(),
        }
    }
}

impl TopK {
    /// Trim `keeper` to its comparator-smallest rows whose summed weight covers
    /// the window, in no particular order. The boundary row stays whole: only
    /// the client, which sees every worker's rows, may clip it.
    fn trim(&mut self, keeper: &mut Batch) {
        let mut ranking = RowRanking::new(&self.order, &*keeper);
        if let Some(bound) = self.bound {
            ranking.drop_above(bound);
        }
        ranking.keep_smallest(self.window as usize);
        let weight = |r: u32| keeper.get_weight(r as usize);
        let mut acc = ranking.rows().fold(0i64, |a, r| a.saturating_add(weight(r)));
        // Every row weighs ≥ 1, so the kept rows cover the window; a surplus is the only way a
        // comparator-larger row among them is droppable.
        if acc > self.window {
            ranking.sort();
            acc = 0;
            let window = self.window;
            let cut = ranking.rows().position(|r| {
                acc = acc.saturating_add(weight(r));
                acc >= window
            });
            if let Some(i) = cut {
                ranking.truncate(i + 1);
            }
        }
        self.bound = ranking.max_lead();
        self.summed = acc;
        let perm: Vec<u32> = ranking.rows().collect();
        *keeper = keeper.indexed_rows(&perm);
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
