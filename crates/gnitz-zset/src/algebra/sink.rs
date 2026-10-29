//! [`SinkPlan`]: the sink of an ad-hoc read over one worker's surviving rows —
//! the map, then a forward, a prefix cut, a top-k or a fold.

use gnitz_expr::{order_locators, Lead, OrderLocator, RowFilter, RowRanking};

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
    /// The surviving row ranges of the chunk being pushed; reused across chunks.
    ranges: Vec<(usize, usize)>,
}

enum Kind {
    /// Forward every survivor.
    Rows(Batch),
    /// Survivors until their summed weight reaches the window; `left` is the
    /// weight still to reach.
    Prefix { keeper: Batch, left: i64 },
    /// The survivors a top-k keeps.
    TopK { keeper: Batch, top: TopK },
    /// Partial reduce-output rows.
    Fold {
        /// The one mapped batch of the whole scan, used only under a map:
        /// `clear` keeps its buffers.
        mapped: Batch,
        fold: Box<AdhocFold>,
    },
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
        let sink_in = map.as_ref().map_or(*src_schema, |m| *m.output_schema());
        let kind = match &sink.kind {
            SinkKind::Fold(agg) => Kind::Fold {
                fold: Box::new(AdhocFold::new(&sink_in, agg, group_cap)?),
                mapped: Batch::empty_with_schema(&sink_in),
            },
            SinkKind::Rows { cut } => {
                let keeper = Batch::empty_with_schema(&sink_in);
                match cut {
                    None => Kind::Rows(keeper),
                    Some(RowsCut { k, order }) => {
                        // Saturated: a wrapped negative window would truncate the answer.
                        let window = i64::try_from(k.get()).unwrap_or(i64::MAX);
                        if order.is_empty() {
                            Kind::Prefix { keeper, left: window }
                        } else {
                            let top = TopK {
                                order: order_locators(order, &sink_in, true)?,
                                window,
                                summed: 0,
                                bound: None,
                            };
                            Kind::TopK { keeper, top }
                        }
                    }
                }
            }
        };
        Ok(SinkPlan { map, kind, ranges: Vec::new() })
    }

    /// The layout [`Self::finish`] emits.
    pub fn output_schema(&self) -> &SchemaDescriptor {
        match &self.kind {
            Kind::Rows(keeper) | Kind::Prefix { keeper, .. } | Kind::TopK { keeper, .. } => keeper.schema(),
            Kind::Fold { fold, .. } => fold.output_schema(),
        }
    }

    /// How many source rows the first drain needs to visit: an unordered cut
    /// no more than its window, any other sink no fewer than every row.
    pub fn first_drain(&self) -> usize {
        match &self.kind {
            Kind::Prefix { left, .. } => *left as usize,
            _ => usize::MAX,
        }
    }

    /// Take the rows of one source chunk `filter` keeps, cutting at the row a
    /// window ends in. `Ok(true)` once the sink needs no more rows; `Err` past a
    /// fold's group cap.
    pub fn push(&mut self, chunk: &Batch, filter: &mut RowFilter) -> Result<bool, String> {
        let SinkPlan { map, kind, ranges } = self;
        filter.ranges(&chunk.as_mem_batch(), ranges);
        let map = map.as_mut();
        match kind {
            Kind::Rows(keeper) => {
                append_survivors(map, chunk, keeper, ranges);
                Ok(false)
            }
            Kind::Prefix { keeper, left } => {
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
            Kind::TopK { keeper, top } => {
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
            Kind::Rows(keeper) | Kind::Prefix { keeper, .. } => keeper,
            Kind::TopK { mut keeper, mut top } => {
                // The keeper is the reply now: shed it down to the smallest
                // superset the client can still cut exactly. At or below the
                // window that cuts nothing.
                if top.summed > top.window {
                    top.trim(&mut keeper);
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

/// Append one chunk's survivor ranges onto the keeper, through the map when there
/// is one.
fn append_survivors(map: Option<&mut MapPlan>, chunk: &Batch, keeper: &mut Batch, ranges: &[(usize, usize)]) {
    match map {
        None => keeper.append_ranges(&chunk.as_mem_batch(), ranges),
        Some(p) => p.append_map_ranges(chunk, keeper, ranges),
    }
}
