//! Client-side finishing for an ad-hoc aggregate / DISTINCT SELECT.

use std::rc::Rc;
use std::sync::Arc;

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::RowFilter;
use gnitz_zset::algebra::{Agg, AggValues, pk_firsts};
use gnitz_zset::repr::{Batch, range_rows};

use crate::error::GnitzSqlError;
use crate::exec::client_map::ClientMap;
use crate::expr_lower::compile_filter_program;
use crate::ir::BoundExpr;

/// An ad-hoc fold's reply to its result: combine the partials, then the HAVING filter and
/// finalize map a grouped view runs over its reduce output.
#[derive(Clone)]
pub(crate) struct FoldFinish {
    /// The aggregates over the partial reply layout, each merging its own partials.
    aggs: Box<[Agg]>,
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
        let locs = layout.payload_locators();
        let aggs = aggs
            .iter()
            .zip(&locs[locs.len() - aggs.len()..])
            .map(|(d, &loc)| Agg::merging(d.agg_op, loc))
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

    pub(crate) fn has_having(&self) -> bool {
        self.having.is_some()
    }

    /// The concatenated worker partials (in `partial_schema`) to the result: combined, then
    /// HAVING, then finalize.
    pub(crate) fn finish(&mut self, partial: ZSetBatch) -> ZSetBatch {
        let mut groups = self.combine(partial);
        if let Some(ev) = &mut self.having {
            let ranges = ev.ranges(&groups.view());
            groups.retain_ranges(ranges);
        }
        self.finalize.apply(groups)
    }

    /// The partials as one row per group: a group's first row, its values merged with every
    /// later row of the group in reply order. An integer wraps mod 2^64 as the engine
    /// accumulator does; a float adds in reply order, so its low bits follow the worker count.
    fn combine(&self, partial: ZSetBatch) -> ZSetBatch {
        let v = partial.view();
        debug_assert!(v.weights().all(|w| w == 1), "a fold partial is one reduce row");
        let firsts = pk_firsts(&v);
        if !firsts.repeats() {
            // Every group is one partial already; a write would copy a received reply out
            // of its frame for nothing.
            return partial;
        }
        let n = range_rows(&firsts.keep);
        let mut vals = AggValues::new(&Rc::from(&*self.aggs), n);
        vals.fold_stored_rows(&v, &firsts.groups);
        let mut out = Batch::with_capacity(v.schema(), n);
        out.append_ranges(&v, &firsts.keep);
        out.rewrite(|w| vals.emit_at(w, (0..n).map(|g| (g, g))));
        ZSetBatch::of(partial.schema(), out)
    }
}

#[cfg(test)]
#[path = "tests/agg_finish.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/agg_finish.rs"]
mod bench;
