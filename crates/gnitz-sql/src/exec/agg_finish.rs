//! Client-side finishing for an ad-hoc aggregate / DISTINCT SELECT, and for a
//! FROM-less SELECT's constant row.
//!
//! The workers return one concatenated `ZSetBatch` of per-worker partial reduce
//! rows in the SyntheticFold layout the fold lowering declares:
//! `[_group_pk U128 (hidden PK) | group cols | one partial per physical agg spec]`.
//! [`FoldFinish::combine`] folds them into one row per group in place.
//! [`FoldFinish::apply`] then runs the two
//! operators a grouped view runs over its reduce output — the HAVING filter and
//! the finalize map, compiled as a view's are. Every output row keeps the
//! engine's `_group_pk`, which makes a tied ORDER BY / LIMIT a function of the
//! data alone.
//!
//! A float SUM adds the partials in reply order, so its low bits follow the
//! worker count.

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use gnitz_core::{ColumnDef, Schema, ZSetBatch};
use gnitz_expr::{ColumnLocator, Evaluator, SchemaFacts};
use gnitz_wire::AggFunc as WireAggFunc;
use rustc_hash::FxHashMap;

use crate::agg::group_pk_def;
use crate::codec::project_schema::{reply_program, ProjItem};
use crate::error::GnitzSqlError;
use crate::exec::client_map::ClientMap;
use crate::expr_lower::compile_conjuncts_evaluator;
use crate::ir::BoundExpr;

/// An ad-hoc fold's reply, or a FROM-less SELECT's ground row, to its result: combine the
/// partials, then the HAVING filter and finalize map a grouped view runs over its reduce output.
pub(crate) struct FoldFinish {
    /// The partial reply layout; the combined groups keep it, HAVING and finalize read it.
    pub(crate) partial_schema: Arc<Schema>,
    /// `[_group_pk (hidden PK) | finalize items]`.
    pub(crate) out_schema: Arc<Schema>,
    /// Per physical agg spec, in partial order: the ordering by which a partial replaces the
    /// held value, `None` for a summing merge.
    merge: Vec<Option<Ordering>>,
    pub(crate) having: Option<Evaluator>,
    finalize: ClientMap,
}

impl FoldFinish {
    pub(crate) fn new(
        partial_schema: Schema,
        ops: impl IntoIterator<Item = WireAggFunc>,
        having: &[BoundExpr],
        finalize: Vec<(BoundExpr, ColumnDef)>,
    ) -> Result<FoldFinish, GnitzSqlError> {
        let merge = ops
            .into_iter()
            .map(|op| match op {
                WireAggFunc::Count | WireAggFunc::CountNonNull | WireAggFunc::Sum => None,
                WireAggFunc::Min => Some(Ordering::Less),
                WireAggFunc::Max => Some(Ordering::Greater),
            })
            .collect();
        let having = compile_conjuncts_evaluator(having, &partial_schema)?;
        let mut items = vec![ProjItem::PassThrough { src_col: 0 }];
        let mut cols = vec![group_pk_def()];
        for (expr, def) in finalize {
            items.push(ProjItem::from_bound(expr));
            cols.push(def);
        }
        let (out_schema, program) = reply_program(&items, cols, &partial_schema, "aggregate SELECT output schema")?;
        let finalize = ClientMap::new(program, &partial_schema, Arc::new(out_schema))?;
        Ok(FoldFinish {
            partial_schema: Arc::new(partial_schema),
            out_schema: Arc::clone(finalize.out_schema()),
            merge,
            having,
            finalize,
        })
    }

    /// The concatenated worker partials as one row per group. A group's first row absorbs every
    /// later row of the group, in reply order; the absorbed rows are then dropped.
    pub(crate) fn combine(&self, mut partial: ZSetBatch) -> ZSetBatch {
        let schema = self.partial_schema.as_ref();
        let n_group = schema.num_payload_cols() - self.merge.len();
        let locs: Vec<ColumnLocator> = (1..schema.columns.len())
            .map(|ci| SchemaFacts::locate(schema, ci))
            .collect();
        let agg_locs = &locs[n_group..];
        // `_group_pk` → the group's first row. The key is the group's identity, as it is on the
        // view path.
        let mut first_of: FxHashMap<u128, usize> =
            FxHashMap::with_capacity_and_hasher(partial.len(), Default::default());
        let mut keep: Vec<(usize, usize)> = Vec::new();
        for row in 0..partial.len() {
            debug_assert_eq!(partial.weights[row], 1, "a fold partial is one reduce row");
            let key = u128::from_le_bytes(partial.pks.get_bytes(row).try_into().expect("_group_pk is 16 bytes"));
            match first_of.entry(key) {
                Entry::Occupied(e) => {
                    let first = *e.get();
                    for (k, (loc, &wins)) in agg_locs.iter().zip(&self.merge).enumerate() {
                        merge_cell(&mut partial, first, row, n_group + k, wins, loc);
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

    /// HAVING, then finalize, over `groups` (in `partial_schema`).
    pub(crate) fn apply(&self, mut groups: ZSetBatch) -> ZSetBatch {
        if let Some(ev) = &self.having {
            let mut ranges = Vec::new();
            ev.filter_ranges(&groups, &mut ranges);
            groups.retain_ranges(&ranges);
        }
        self.finalize.apply(groups)
    }
}

/// Merge row `row`'s cell at payload slot `pi` into row `first`. `wins` is the ordering by
/// which a partial replaces the held value (MIN: `Less`, MAX: `Greater`); `None` sums. NULL is
/// every merge's identity. Both rows share one arena, so a winning cell moves verbatim.
fn merge_cell(b: &mut ZSetBatch, first: usize, row: usize, pi: usize, wins: Option<Ordering>, loc: &ColumnLocator) {
    if loc.is_null_word(b.nulls[row]) {
        return;
    }
    let replace = match wins {
        _ if loc.is_null_word(b.nulls[first]) => true,
        Some(wins) => loc.cmp_non_null(&*b, row, &*b, first) == wins,
        // 8-byte cells: F64, or the sum mod 2^64, wrapping as the engine accumulator does.
        None => {
            let col = &mut b.payload[pi].bytes;
            let cell = |r: usize| <[u8; 8]>::try_from(&col[r * 8..(r + 1) * 8]).unwrap();
            let (acc, add) = (cell(first), cell(row));
            let sum = if gnitz_wire::is_float(loc.type_code()) {
                (f64::from_le_bytes(acc) + f64::from_le_bytes(add)).to_le_bytes()
            } else {
                i64::from_le_bytes(acc)
                    .wrapping_add(i64::from_le_bytes(add))
                    .to_le_bytes()
            };
            col[first * 8..(first + 1) * 8].copy_from_slice(&sum);
            false
        }
    };
    if replace {
        let w = loc.size();
        b.payload[pi].bytes.copy_within(row * w..(row + 1) * w, first * w);
        gnitz_wire::null_word_set(&mut b.nulls[first], pi, false);
    }
}

#[cfg(test)]
#[path = "tests/agg_finish.rs"]
mod tests;
