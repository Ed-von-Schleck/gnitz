//! Client-side finishing for an ad-hoc aggregate / DISTINCT SELECT, and for a
//! FROM-less SELECT's constant row.

use std::collections::hash_map::Entry;
use std::sync::Arc;

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::{ColumnLocator, RowFilter, SchemaFacts};
use gnitz_wire::{read_u64_le, write_u64_le, AggFunc as WireAggFunc};
use gnitz_wire::{ColumnDef, PkBuf};
use rustc_hash::FxHashMap;

use crate::error::GnitzSqlError;
use crate::exec::client_map::ClientMap;
use crate::expr_lower::compile_filter_program;
use crate::ir::BoundExpr;
use crate::project::reply_program;

/// An ad-hoc fold's reply, or a FROM-less SELECT's ground row, to its result: combine the
/// partials, then the HAVING filter and finalize map a grouped view runs over its reduce output.
pub(crate) struct FoldFinish {
    /// The partial reply layout; the combined groups keep it, HAVING and finalize read it.
    pub(crate) partial_schema: Arc<Schema>,
    /// Per aggregate column, in partial order.
    merge: Vec<Merge>,
    pub(crate) having: Option<RowFilter>,
    finalize: ClientMap,
}

impl FoldFinish {
    pub(crate) fn new(
        partial_schema: Arc<Schema>,
        ops: impl IntoIterator<Item = WireAggFunc>,
        having: &[BoundExpr],
        finalize: Vec<(BoundExpr, ColumnDef)>,
    ) -> Result<FoldFinish, GnitzSqlError> {
        let merge = ops
            .into_iter()
            .map(|op| match op.merge_op() {
                WireAggFunc::Min => Merge::Min,
                WireAggFunc::Max => Merge::Max,
                _ => Merge::Add,
            })
            .collect();
        let having = compile_filter_program(having, &partial_schema.columns)?
            .map(|p| p.resolve_filter(partial_schema.as_ref()))
            .transpose()?;
        let (out_schema, program) = reply_program(finalize, &partial_schema)?;
        let finalize = ClientMap::new(program, &partial_schema, Arc::new(out_schema))?;
        Ok(FoldFinish { partial_schema, merge, having, finalize })
    }

    /// The finalized result, keyed by the output key.
    pub(crate) fn out_schema(&self) -> &Arc<Schema> {
        self.finalize.out_schema()
    }

    /// The concatenated worker partials (in `partial_schema`) to the result: combined, then
    /// HAVING, then finalize.
    pub(crate) fn finish(&mut self, partial: ZSetBatch) -> ZSetBatch {
        let mut groups = self.combine(partial);
        if let Some(ev) = &mut self.having {
            let mut ranges = Vec::new();
            ev.ranges(&groups, &mut ranges);
            groups.retain_ranges(&ranges);
        }
        self.finalize.apply(groups)
    }

    /// The partials as one row per group. A group's first row absorbs every later row of the
    /// group, in reply order; the absorbed rows are then dropped.
    fn combine(&self, mut partial: ZSetBatch) -> ZSetBatch {
        let schema = self.partial_schema.as_ref();
        let n_group = schema.num_payload_cols() - self.merge.len();
        let payload = schema.payload_locators();
        let agg_locs = &payload[n_group..];
        // Output key → the group's first row.
        let mut first_of: FxHashMap<PkBuf, usize> =
            FxHashMap::with_capacity_and_hasher(partial.len(), Default::default());
        let mut keep: Vec<(usize, usize)> = Vec::new();
        for row in 0..partial.len() {
            debug_assert_eq!(partial.weights[row], 1, "a fold partial is one reduce row");
            let key = PkBuf::from_bytes(partial.pks.get_bytes(row));
            match first_of.entry(key) {
                Entry::Occupied(e) => {
                    let first = *e.get();
                    for (k, (loc, &merge)) in agg_locs.iter().zip(&self.merge).enumerate() {
                        merge_cell(&mut partial, first, row, n_group + k, merge, loc);
                    }
                }
                Entry::Vacant(e) => {
                    e.insert(row);
                    match keep.last_mut() {
                        Some((_, end)) if *end == row => *end += 1,
                        _ => keep.push((row, row + 1)),
                    }
                }
            }
        }
        partial.retain_ranges(&keep);
        partial
    }
}

/// How two partials of one aggregate combine.
#[derive(Clone, Copy)]
enum Merge {
    Add,
    Min,
    Max,
}

/// Merge row `row`'s cell at payload slot `pi` into row `first`; NULL is every merge's identity.
fn merge_cell(b: &mut ZSetBatch, first: usize, row: usize, pi: usize, merge: Merge, loc: &ColumnLocator) {
    if loc.is_null_word(b.nulls[row]) {
        return;
    }
    let replace = match merge {
        _ if loc.is_null_word(b.nulls[first]) => true,
        Merge::Min => loc.cmp_non_null(&*b, row, &*b, first).is_lt(),
        Merge::Max => loc.cmp_non_null(&*b, row, &*b, first).is_gt(),
        // F64 adds in reply order, so its low bits follow the worker count; an integer wraps
        // mod 2^64 as the engine accumulator does.
        Merge::Add => {
            let col = &mut b.payload[pi].bytes;
            let (acc, add) = (read_u64_le(col, first * 8), read_u64_le(col, row * 8));
            let sum = if loc.type_code().is_float() {
                (f64::from_bits(acc) + f64::from_bits(add)).to_bits()
            } else {
                acc.wrapping_add(add)
            };
            write_u64_le(col, first * 8, sum);
            false
        }
    };
    if replace {
        // Both rows share one arena, so a string cell moves verbatim.
        let w = loc.size();
        b.payload[pi].bytes.copy_within(row * w..(row + 1) * w, first * w);
        gnitz_wire::null_word_set(&mut b.nulls[first], pi, false);
    }
}

#[cfg(test)]
#[path = "tests/agg_finish.rs"]
mod tests;
