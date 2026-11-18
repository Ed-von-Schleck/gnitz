//! Client-side finishing for an ad-hoc aggregate / DISTINCT SELECT.

use std::cmp::Ordering;
use std::sync::Arc;

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::{ColumnLocator, RowFilter};
use gnitz_wire::AggFunc as WireAggFunc;

use crate::error::GnitzSqlError;
use crate::exec::client_map::ClientMap;
use crate::exec::keys::{Firsts, firsts};
use crate::expr_lower::compile_filter_program;
use crate::ir::BoundExpr;

/// An ad-hoc fold's reply to its result: combine the partials, then the HAVING filter and
/// finalize map a grouped view runs over its reduce output.
pub(crate) struct FoldFinish {
    /// Per aggregate column, in partial order: its locator, and the ordering by which an
    /// extreme's later partial replaces the kept one, `None` for a linear aggregate, whose
    /// partials add.
    aggs: Vec<(ColumnLocator, Option<Ordering>)>,
    having: Option<RowFilter>,
    /// Over the partial reply layout, which the combined groups keep and HAVING reads.
    finalize: ClientMap,
}

impl FoldFinish {
    pub(crate) fn new(
        aggs: &[gnitz_wire::AggDescriptor],
        having: &[BoundExpr],
        finalize: ClientMap,
    ) -> Result<FoldFinish, GnitzSqlError> {
        let partial_schema = finalize.src();
        // The aggregate columns trail the group columns.
        let layout = partial_schema.layout();
        let n_group = layout.num_payload_cols() - aggs.len();
        let locs = layout.payload_locators();
        let nullable = layout.nullable_payload_slots();
        let aggs = aggs
            .iter()
            .zip(&locs[n_group..])
            .enumerate()
            .map(|(k, (d, &loc))| {
                let extreme = match d.agg_op.merge_op() {
                    WireAggFunc::Min => Some(Ordering::Less),
                    WireAggFunc::Max => Some(Ordering::Greater),
                    _ => None,
                };
                // `combine` adds a linear partial as one 8-byte register image.
                debug_assert!(
                    extreme.is_some() || (!gnitz_wire::null_word_get(nullable, n_group + k) && loc.size() == 8),
                    "a linear partial is a non-NULL 8-byte cell"
                );
                (loc, extreme)
            })
            .collect();
        let having = compile_filter_program(having, partial_schema.columns())?
            .map(|p| p.resolve_filter(partial_schema.layout()))
            .transpose()?;
        Ok(FoldFinish { aggs, having, finalize })
    }

    /// The layout of the partial reply the fold's read is decoded into.
    pub(crate) fn partial_schema(&self) -> &Arc<Schema> {
        self.finalize.src()
    }

    /// The finalized result, keyed by the output key.
    #[cfg(test)]
    pub(crate) fn out_schema(&self) -> &Arc<Schema> {
        self.finalize.out_schema()
    }

    pub(crate) fn has_having(&self) -> bool {
        self.having.is_some()
    }

    /// The concatenated worker partials (in `partial_schema`) to the result: combined, then
    /// HAVING, then finalize.
    pub(crate) fn finish(&mut self, partial: ZSetBatch) -> ZSetBatch {
        let mut groups = self.combine(partial);
        if let Some(ev) = &mut self.having {
            let ranges = ev.ranges(&groups);
            groups.retain_ranges(ranges);
        }
        self.finalize.apply(groups)
    }

    /// The partials as one row per group. A group's first row absorbs every later row of the
    /// group, in reply order; the absorbed rows are then dropped.
    fn combine(&self, mut partial: ZSetBatch) -> ZSetBatch {
        debug_assert!(partial.weights().all(|w| w == 1), "a fold partial is one reduce row");
        let Firsts { absorbed, keep } = firsts(partial.pk_region(), partial.pk_stride());
        for &(loc, extreme) in &self.aggs {
            match extreme {
                Some(wins) => {
                    for &(row, first) in &absorbed {
                        take_extreme(&mut partial, first as usize, row as usize, wins, loc);
                    }
                }
                // An integer wraps mod 2^64 as the engine accumulator does; a float adds in
                // reply order, so its low bits follow the worker count.
                None => {
                    let float = loc.type_code().is_float();
                    let pi = loc.payload_slot().expect("an aggregate is a payload column");
                    let cells = partial.col_mut(pi).as_chunks_mut::<8>().0;
                    for &(row, first) in &absorbed {
                        let (acc, add) = (
                            u64::from_le_bytes(cells[first as usize]),
                            u64::from_le_bytes(cells[row as usize]),
                        );
                        let sum = match float {
                            true => (f64::from_bits(acc) + f64::from_bits(add)).to_bits(),
                            false => acc.wrapping_add(add),
                        };
                        cells[first as usize] = sum.to_le_bytes();
                    }
                }
            }
        }
        partial.retain_ranges(&keep);
        partial
    }
}

/// Row `row`'s cell at payload `loc` replaces row `first`'s when it compares `wins` against
/// it; a NULL cell loses to any value.
fn take_extreme(b: &mut ZSetBatch, first: usize, row: usize, wins: Ordering, loc: ColumnLocator) {
    if loc.is_null_word(b.null_word(row)) {
        return;
    }
    if loc.is_null_word(b.null_word(first)) || loc.cmp_non_null(&*b, row, &*b, first) == wins {
        // Both rows are in one batch, so one arena: a string cell moves verbatim.
        let (pi, w) = (
            loc.payload_slot().expect("an aggregate is a payload column"),
            loc.size(),
        );
        b.col_mut(pi).copy_within(row * w..(row + 1) * w, first * w);
        b.set_null_word(first, b.null_word(first) & !loc.null_bit());
    }
}

#[cfg(test)]
#[path = "tests/agg_finish.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/agg_finish.rs"]
mod bench;
