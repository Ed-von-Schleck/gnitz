//! Client-side finishing for an ad-hoc aggregate / DISTINCT SELECT, and for a
//! FROM-less SELECT's constant row.

use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::sync::Arc;

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::{ColumnLocator, RowFilter, SchemaFacts};
use gnitz_wire::ColumnDef;
use gnitz_wire::{read_u64_le, write_u64_le, AggFunc as WireAggFunc};
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
        debug_assert!(
            partial.weights.iter().all(|&w| w == 1),
            "a fold partial is one reduce row"
        );
        let n = partial.len();
        let (stride, region) = (partial.pks.stride(), partial.pks.region());
        // A narrow key groups by its image, a wide one by its bytes.
        let (first, keep) = match stride <= gnitz_wire::NARROW_PK_MAX_BYTES {
            true => firsts(region.chunks_exact(stride).map(gnitz_wire::widen_pk_be)),
            false => firsts(region.chunks_exact(stride)),
        };
        // Every row its own group: nothing to merge or drop.
        if keep == [(0, n)] {
            return partial;
        }
        let absorbed = || first.iter().enumerate().filter(|&(row, &f)| f as usize != row);
        for (k, (loc, &merge)) in agg_locs.iter().zip(&self.merge).enumerate() {
            let pi = n_group + k;
            match merge {
                // An integer wraps mod 2^64 as the engine accumulator does.
                Merge::Add if !loc.type_code().is_float() => {
                    let nulls = &mut partial.nulls;
                    let cells = partial.payload[pi].bytes.as_chunks_mut::<8>().0;
                    for (row, &f) in absorbed() {
                        let f = f as usize;
                        if gnitz_wire::null_word_get(nulls[row], pi) {
                            continue;
                        }
                        if gnitz_wire::null_word_get(nulls[f], pi) {
                            cells[f] = cells[row];
                            gnitz_wire::null_word_set(&mut nulls[f], pi, false);
                        } else {
                            let sum = u64::from_le_bytes(cells[f]).wrapping_add(u64::from_le_bytes(cells[row]));
                            cells[f] = sum.to_le_bytes();
                        }
                    }
                }
                // Row order, so a float sum adds in reply order.
                _ => absorbed().for_each(|(row, &f)| merge_cell(&mut partial, f as usize, row, pi, merge, loc)),
            }
        }
        partial.retain_ranges(&keep);
        partial
    }
}

/// Each key's first position, per position, and the runs of first positions.
fn firsts<K: Hash + Eq>(keys: impl ExactSizeIterator<Item = K>) -> (Vec<u32>, Vec<(usize, usize)>) {
    let n = keys.len();
    assert!(n <= u32::MAX as usize, "partial row count exceeds u32");
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
        // F64 adds in reply order, so its low bits follow the worker count. An integer
        // sum takes `combine`'s column loop; this arm states the same wrap.
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
