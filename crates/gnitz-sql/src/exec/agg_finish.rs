//! Client-side finishing for an ad-hoc aggregate / DISTINCT SELECT, and for a
//! FROM-less SELECT's constant row.
//!
//! The workers return one concatenated `ZSetBatch` of per-worker partial reduce
//! rows in the SyntheticFold layout the fold lowering declares:
//! `[_group_pk U128 (hidden PK) | group cols | one partial per physical agg spec]`.
//! [`FoldFinish::combine`] folds them into one row per group in place, by the
//! partial-merge rule the view path's two-phase combine ships
//! (`gnitz_wire::AggFunc::merge_func`). [`FoldFinish::apply`] then runs the two
//! operators a grouped view runs over its reduce output — the HAVING filter and
//! the finalize map, compiled as a view's are. Every output row keeps the
//! engine's `_group_pk`, which makes a tied ORDER BY / LIMIT a function of the
//! data alone.
//!
//! A float SUM adds the partials in reply order, so its low bits follow the
//! worker count.

use std::cmp::Ordering;
use std::sync::Arc;

use gnitz_core::{ColumnDef, Schema, ZSetBatch};
use gnitz_expr::{ColumnLocator, Evaluator, SchemaFacts};
use gnitz_wire::AggFunc as WireAggFunc;
use rustc_hash::FxHashMap;

use crate::agg::group_pk_def;
use crate::codec::project_schema::{reply_program, ProjItem};
use crate::error::GnitzSqlError;
use crate::exec::batch::move_payload;
use crate::expr_lower::compile_conjuncts_evaluator;
use crate::ir::BoundExpr;

/// The end of an `older` chain.
const NO_ROW: usize = usize::MAX;

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
    finalize: Evaluator,
    /// The finalize map's column moves as `(output slot, source slot)`: equal-width payload
    /// copies only.
    copies: Vec<(usize, usize)>,
    /// The finalize map reproduces its input.
    identity: bool,
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
            .map(|op| match op.merge_func() {
                WireAggFunc::Min => Some(Ordering::Less),
                WireAggFunc::Max => Some(Ordering::Greater),
                _ => None,
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
        let identity = program.is_identity_map(&partial_schema, &out_schema);
        let finalize = program.resolve_map(&partial_schema, &out_schema)?;
        // A finalize pass-through keeps its partial column's type (a group column's def, or an
        // aggregate's view type), so every move is a verbatim payload copy.
        let copies = finalize
            .copies()
            .iter()
            .map(|&(loc, out, width)| match loc {
                ColumnLocator::Payload { slot, size, .. } if size == width => Ok((out as usize, slot as usize)),
                _ => Err(GnitzSqlError::Internal(
                    "a finalize item copies a key or a promoted column".into(),
                )),
            })
            .collect::<Result<_, _>>()?;
        Ok(FoldFinish {
            partial_schema: Arc::new(partial_schema),
            out_schema: Arc::new(out_schema),
            merge,
            having,
            finalize,
            copies,
            identity,
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
        let (group_locs, agg_locs) = locs.split_at(n_group);
        // `_group_pk` → the newest group's first row; `older[r]` the next-older first row with
        // r's key. The key is a digest, so a hit is confirmed by value — and already
        // hashed, so the map need not hash it again.
        let mut newest: FxHashMap<u128, usize> = FxHashMap::with_capacity_and_hasher(partial.len(), Default::default());
        let mut older: Vec<usize> = Vec::with_capacity(partial.len());
        let mut keep: Vec<(usize, usize)> = Vec::new();
        for row in 0..partial.len() {
            debug_assert_eq!(partial.weights[row], 1, "a fold partial is one reduce row");
            let key = u128::from_le_bytes(partial.pks.get_bytes(row).try_into().expect("_group_pk is 16 bytes"));
            let hit = std::iter::successors(newest.get(&key).copied(), |&r| Some(older[r]).filter(|&o| o != NO_ROW))
                .find(|&first| same_group(group_locs, &partial, row, first));
            match hit {
                Some(first) => {
                    older.push(NO_ROW);
                    for (k, (loc, &wins)) in agg_locs.iter().zip(&self.merge).enumerate() {
                        merge_cell(&mut partial, first, row, n_group + k, wins, loc);
                    }
                }
                None => {
                    older.push(newest.insert(key, row).unwrap_or(NO_ROW));
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
        if self.identity {
            return groups;
        }
        self.map_batch(groups)
    }

    /// The finalize map over every row of `src`, each row keeping its PK and weight.
    fn map_batch(&self, src: ZSetBatch) -> ZSetBatch {
        let (ev, n) = (&self.finalize, src.len());
        let mut out = ZSetBatch::new(&self.out_schema);
        out.nulls = vec![0; n];
        let str_emits = !ev.str_emits().is_empty();
        if str_emits {
            // Appended to, so copied: a copied German cell keeps its offset into it.
            out.blob = src.blob.clone();
        }
        for &(_, pi, stride) in ev.scalar_emits() {
            out.payload[pi as usize].bytes = vec![0; n * stride as usize];
        }
        for &(_, pi) in ev.str_emits() {
            out.payload[pi as usize].bytes = vec![0; n * 16];
        }
        {
            let ZSetBatch { nulls, payload, blob, .. } = &mut out;
            let nb = gnitz_wire::as_le_bytes_mut(nulls);
            ev.null_perm()
                .write_rows(gnitz_wire::as_le_bytes(&src.nulls), 0, nb, 0, n);
            if ev.emits_anything() {
                ev.eval_morsels(&src, 0, n, |row0, mo| {
                    for &(reg, pi, stride) in ev.scalar_emits() {
                        mo.emit_scalar_cells(
                            reg as usize,
                            &mut payload[pi as usize].bytes,
                            nb,
                            row0,
                            pi as usize,
                            stride as usize,
                        );
                    }
                    for &(reg, pi) in ev.str_emits() {
                        mo.emit_str_cells(
                            reg as usize,
                            &mut payload[pi as usize].bytes,
                            nb,
                            blob,
                            row0,
                            pi as usize,
                        );
                    }
                });
            }
        }
        // The evaluator is done with `src`; its parts move into the output.
        let ZSetBatch { pks, weights, mut payload, blob, .. } = src;
        move_payload(&mut out.payload, &mut payload, &self.copies);
        if !str_emits {
            out.blob = blob;
        }
        out.pks = pks;
        out.weights = weights;
        out
    }
}

/// Whether rows `a` and `b` hold the same group values: NULL equals NULL, non-NULL values
/// compare by `cmp_non_null` (the engine's `compare_by_group_cols` rule).
fn same_group(locs: &[ColumnLocator], batch: &ZSetBatch, a: usize, b: usize) -> bool {
    let (aw, bw) = (batch.nulls[a], batch.nulls[b]);
    locs.iter().all(|l| match (l.is_null_word(aw), l.is_null_word(bw)) {
        (false, false) => l.cmp_non_null(batch, a, batch, b).is_eq(),
        (x, y) => x == y,
    })
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
