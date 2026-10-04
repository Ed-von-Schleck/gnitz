//! Client-side finishing for an ad-hoc aggregate / DISTINCT SELECT, and for a
//! FROM-less SELECT's constant row.

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::sync::Arc;

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::{ColumnLocator, RowFilter, SchemaFacts};
use gnitz_wire::AggFunc as WireAggFunc;
use gnitz_wire::ColumnDef;
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
    /// Per aggregate column, in partial order: the ordering by which an extreme's later
    /// partial replaces the kept one, `None` for a linear aggregate, whose partials add.
    merge: Vec<Option<Ordering>>,
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
                WireAggFunc::Min => Some(Ordering::Less),
                WireAggFunc::Max => Some(Ordering::Greater),
                _ => None,
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
        debug_assert!(
            partial.weights.iter().all(|&w| w == 1),
            "a fold partial is one reduce row"
        );
        let n = partial.len();
        let (first, keep) = firsts(&partial);
        // Every row its own group: nothing to merge or drop.
        if keep == [(0, n)] {
            return partial;
        }
        let absorbed = || first.iter().enumerate().filter(|&(row, &f)| f as usize != row);
        let locs = schema.payload_locators();
        let aggs = locs[n_group..].iter().zip(&self.merge);
        for ((pi, _, col), (loc, &extreme)) in schema.payload_columns().skip(n_group).zip(aggs) {
            match extreme {
                Some(wins) => {
                    absorbed().for_each(|(row, &f)| take_extreme(&mut partial, f as usize, row, pi, wins, loc))
                }
                // An integer wraps mod 2^64 as the engine accumulator does; a float adds in
                // reply order, so its low bits follow the worker count.
                None => {
                    debug_assert!(!col.is_nullable, "a linear partial is never NULL");
                    let float = loc.type_code().is_float();
                    let cells = partial.payload[pi].bytes.as_chunks_mut::<8>().0;
                    for (row, &f) in absorbed() {
                        let (acc, add) = (u64::from_le_bytes(cells[f as usize]), u64::from_le_bytes(cells[row]));
                        let sum = match float {
                            true => (f64::from_bits(acc) + f64::from_bits(add)).to_bits(),
                            false => acc.wrapping_add(add),
                        };
                        cells[f as usize] = sum.to_le_bytes();
                    }
                }
            }
        }
        partial.retain_ranges(&keep);
        partial
    }
}

/// Each row's key's first position, and the runs of first positions.
pub(crate) fn firsts(rows: &ZSetBatch) -> (Vec<u32>, Vec<(usize, usize)>) {
    let (stride, region) = (rows.pks.stride(), rows.pks.region());
    // A narrow key groups by its image, a wide one by its bytes.
    match stride <= gnitz_wire::NARROW_PK_MAX_BYTES {
        true => firsts_of(region.chunks_exact(stride).map(gnitz_wire::widen_pk_be)),
        false => firsts_of(region.chunks_exact(stride)),
    }
}

fn firsts_of<K: Hash + Eq>(keys: impl ExactSizeIterator<Item = K>) -> (Vec<u32>, Vec<(usize, usize)>) {
    let n = keys.len();
    assert!(n <= u32::MAX as usize, "row count exceeds u32");
    let mut first_of: FxHashMap<K, u32> = FxHashMap::with_capacity_and_hasher(n, Default::default());
    let mut first = Vec::with_capacity(n);
    let mut keep: Vec<(usize, usize)> = Vec::new();
    for (row, key) in keys.enumerate() {
        match first_of.entry(key) {
            Entry::Occupied(e) => first.push(*e.get()),
            Entry::Vacant(e) => {
                e.insert(row as u32);
                first.push(row as u32);
                match keep.last_mut() {
                    Some((_, end)) if *end == row => *end += 1,
                    _ => keep.push((row, row + 1)),
                }
            }
        }
    }
    (first, keep)
}

/// Row `row`'s cell at payload slot `pi` replaces row `first`'s when it compares `wins`
/// against it; a NULL cell loses to any value.
fn take_extreme(b: &mut ZSetBatch, first: usize, row: usize, pi: usize, wins: Ordering, loc: &ColumnLocator) {
    if loc.is_null_word(b.nulls[row]) {
        return;
    }
    if loc.is_null_word(b.nulls[first]) || loc.cmp_non_null(&*b, row, &*b, first) == wins {
        // Both rows share one arena, so a string cell moves verbatim.
        let w = loc.size();
        b.payload[pi].bytes.copy_within(row * w..(row + 1) * w, first * w);
        gnitz_wire::null_word_set(&mut b.nulls[first], pi, false);
    }
}

#[cfg(test)]
#[path = "tests/agg_finish.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/agg_finish.rs"]
mod bench;
