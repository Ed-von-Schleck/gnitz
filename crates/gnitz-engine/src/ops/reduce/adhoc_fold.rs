//! Ad-hoc aggregation hash-fold — the stateless per-worker sink behind a
//! fold-sink `ReadSpec` (single-relation GROUP BY / global aggregate / DISTINCT
//! over the committed base). No DBSP circuit, no operator-trace tables, no
//! exchange: each surviving scan chunk folds into per-group accumulators in
//! bounded RAM, then one partial reduce-output batch is emitted after full
//! accumulation.
//!
//! Semantics parity with a CREATE VIEW of the same statement is **structural**,
//! not reimplemented: the accumulation kernel (`Accumulator::step_from_batch`),
//! the group comparator (`compare_by_group_cols`), the group-key hash
//! (`GroupKeyCols`), and the emission path (`emit_reduce_row` driven by a
//! `ReducePlan`) are the exact shared code a view's reduce runs, so partial agg
//! columns are byte-identical to a view's reduce output.
//!
//! Exactness over the committed base: the scan cursor delivers consolidated,
//! positive-net-weight rows (ghosts excluded, base tables DML-forced
//! non-negative), so with no history there is no retraction arithmetic — the
//! accumulator over such rows *is* the aggregate. The fold has no `should_emit`
//! gate: it emits one partial per present group (over positive weights a present
//! group always has net cardinality > 0, matching the view). It carries no
//! COUNT(*) cardinality companion — that is a `should_emit` signal the stateless
//! fold does not need.

use std::cmp::Ordering;

use rustc_hash::FxHashMap;

use gnitz_wire::AggReadSpec;

use super::super::util::{global_group_key, GroupKeyCols};
use super::agg::{Accumulator, AggDescriptor, AggOp};
use super::emit::emit_reduce_row;
use super::plan::{build_reduce_output_schema, ReducePlan};
use super::sort::compare_by_group_cols;
use crate::schema::key::NarrowPkOpk;
use crate::schema::{ReduceOutKey, SchemaDescriptor, TypeCode};
use crate::storage::Batch;

/// The request-scoped fold state. `pub(crate)` so `catalog::scan_spec` can drive
/// it; every reduce building block it composes is reached at `pub(super)` from
/// this descendant of `ops::reduce`.
pub(crate) struct AdhocFold {
    /// The baked reduce plan — the single home of the schemas, group columns,
    /// aggregate descriptors/locators, group comparator descs, and emission
    /// roles the fold reads (`ReducePlan::new` derives them all once).
    plan: ReducePlan,
    /// Baked per-row group keyer; `None` = a global aggregate — one group at
    /// ordinal 0, no per-row hash or probe (its emit key is the constant
    /// `global_group_key()`).
    keyer: Option<GroupKeyCols>,
    /// One representative source row per group, in group-discovery order; the
    /// row index IS the group ordinal (and `rep_rows.count` the group count).
    /// `emit_reduce_row` reads the group columns from it, and the group key is
    /// re-derived from it at `finish` (a pure function of the group columns).
    rep_rows: Batch,
    /// Flat accumulator matrix: group `ord` owns
    /// `accs[ord * n_aggs .. (ord + 1) * n_aggs]`.
    accs: Vec<Accumulator>,
    /// Group-key hash → group ordinals sharing it (FxHash — the key is already
    /// a uniform 128-bit XXH3 digest, so no second strong hash is needed).
    /// Collisions are disambiguated by `compare_by_group_cols` (value grouping,
    /// never hash-only), exactly as the view path never trusts a hash alone.
    by_hash: FxHashMap<u128, Vec<u32>>,
    /// Same-group memo: the previous row's `(key, ordinal)`. Consecutive rows
    /// of one group — per cluster, when the scan is ordered by the group
    /// column — resolve without a map probe.
    last: Option<(u128, u32)>,
    group_cap: usize,
}

impl AdhocFold {
    /// Build the fold state from a decoded fold spec. Validates group/agg
    /// column indices against the source schema and the echoed `reply_schema`
    /// against the engine-derived SyntheticFold layout (the spec AND the echoed
    /// client blob are a trust boundary — a wrong-shaped but structurally valid
    /// reply schema would otherwise panic `ReducePlan::new`); aggregate-op
    /// validity is decode-enforced (`AggReadItem.op` is typed).
    pub(crate) fn new(
        src_schema: &SchemaDescriptor,
        reply_schema: &SchemaDescriptor,
        agg: &AggReadSpec,
        group_cap: usize,
    ) -> Result<Self, String> {
        let n_cols = src_schema.num_columns();

        let mut group_cols = Vec::with_capacity(agg.group_cols.len());
        for &c in &agg.group_cols {
            let c = c as usize;
            if c >= n_cols {
                return Err(format!("scan_spec fold: group column {c} out of range ({n_cols} cols)"));
            }
            group_cols.push(c as u32);
        }

        let mut agg_descs = Vec::with_capacity(agg.aggs.len());
        for item in &agg.aggs {
            let c = item.src_col as usize;
            if c >= n_cols {
                return Err(format!("scan_spec fold: agg column {c} out of range ({n_cols} cols)"));
            }
            agg_descs.push(AggDescriptor {
                col_idx: c as u32,
                agg_op: AggOp::from(item.op),
                col_type_code: TypeCode::from_validated_u8(src_schema.columns[c].type_code),
            });
        }

        // The ad-hoc partial layout is ALWAYS synthetic-fold: `_agg_pk` U128 PK,
        // group cols as payload, then the agg partial columns — a pure function
        // of `(src_schema, agg)`, derived here through the same authority the
        // compiler lays every reduce output with. The echoed client schema must
        // match it physically (types + PK region; nullability is presentation)
        // or the frame is malformed.
        let derived = build_reduce_output_schema(src_schema, &group_cols, &agg_descs, ReduceOutKey::SyntheticFold)
            .ok_or("scan_spec fold: group + agg columns exceed the schema column cap")?;
        if !reply_schema.same_physical_layout(&derived) {
            return Err("scan_spec fold: reply schema does not match the derived fold layout".to_string());
        }
        let plan = ReducePlan::new(
            src_schema,
            &derived,
            &group_cols,
            &agg_descs,
            ReduceOutKey::SyntheticFold,
            false, // has_avi — no AggValueIndex on the one-shot path
            false, // global_ground — the client synthesizes the empty-input ground row
            false, // i_am_owner
        );
        let keyer = (!group_cols.is_empty()).then(|| GroupKeyCols::new(src_schema, &group_cols));

        Ok(AdhocFold {
            plan,
            keyer,
            rep_rows: Batch::empty_with_schema(src_schema),
            accs: Vec::new(),
            by_hash: FxHashMap::default(),
            last: None,
            group_cap,
        })
    }

    /// Fold every `[start, end)` row range of one source chunk into the group
    /// state — the caller passes the filter's surviving row ranges directly, so
    /// no survivor batch is materialized. `Err` on exceeding the per-worker group
    /// cap.
    ///
    /// The whole list rather than one range: `MemBatch` carries its region
    /// offsets by value (~½ KiB), so building the chunk's and the
    /// representative-rows' views once per *range* would cost more than the fold
    /// itself over a fragmented survivor list.
    pub(crate) fn fold_ranges(&mut self, chunk: &Batch, ranges: &[(usize, usize)]) -> Result<(), String> {
        let Self {
            plan,
            keyer,
            rep_rows,
            accs,
            by_hash,
            last,
            group_cap,
        } = self;
        let n_aggs = plan.agg_descs.len();
        let mb = chunk.as_mem_batch();
        let new_accs = |accs: &mut Vec<Accumulator>| {
            accs.extend(
                plan.agg_descs
                    .iter()
                    .zip(&plan.agg_locs)
                    .map(|(d, &loc)| Accumulator::new(d, loc)),
            );
        };
        let Some(keyer) = keyer else {
            // Global aggregate: one group at ordinal 0, created on the first
            // surviving row — no per-row key hash, memo, or comparator probe.
            for row in ranges.iter().flat_map(|&(s, e)| s..e) {
                let w = mb.get_weight(row);
                debug_assert!(w > 0, "adhoc fold: scan cursor must deliver positive weights");
                if w <= 0 {
                    continue;
                }
                if rep_rows.count == 0 {
                    rep_rows.append_batch(chunk, row, row + 1);
                    new_accs(accs);
                }
                for acc in &mut accs[..n_aggs] {
                    acc.step_from_batch(&mb, row, w);
                }
            }
            return Ok(());
        };
        // Rebuilt only when a group insert mutates `rep_rows` — never per row.
        let mut rep_mb = rep_rows.as_mem_batch();
        for row in ranges.iter().flat_map(|&(s, e)| s..e) {
            let w = mb.get_weight(row);
            // The scan cursor delivers consolidated, positive-net-weight rows
            // (ghosts excluded). MIN/MAX correctness depends on stepping only
            // positive weights (its arm never reads `weight`, and there is no
            // weight-sign assert in the kernel); guard defensively — exactly as
            // `op_reduce`'s replay loop guards with `if w > 0`.
            debug_assert!(w > 0, "adhoc fold: scan cursor must deliver positive weights");
            if w <= 0 {
                continue;
            }
            let key = keyer.key_row(&mb, row);
            // The memo hit and the bucket probe both confirm by VALUE, so a
            // hash collision can never merge two groups.
            let same =
                |ord: u32| compare_by_group_cols(&mb, row, &rep_mb, ord as usize, &plan.sort_descs) == Ordering::Equal;
            let ord = match *last {
                Some((k, ord)) if k == key && same(ord) => ord,
                _ => {
                    let found = by_hash.get(&key).and_then(|b| b.iter().copied().find(|&o| same(o)));
                    let ord = match found {
                        Some(ord) => ord,
                        None => {
                            // A new group. The cap is per-worker (a worker sees
                            // a subset of the global groups), so it never fires
                            // when the global group count ≤ cap; firing is a
                            // stated resource-exhaustion abort, not silent
                            // degradation.
                            let ord = rep_rows.count;
                            if ord >= *group_cap {
                                return Err(format!(
                                    "GROUP BY exceeds {group_cap} distinct groups for ad-hoc execution; \
                                     CREATE VIEW to maintain this aggregation incrementally"
                                ));
                            }
                            rep_rows.append_batch(chunk, row, row + 1);
                            new_accs(accs);
                            by_hash.entry(key).or_default().push(ord as u32);
                            rep_mb = rep_rows.as_mem_batch();
                            ord as u32
                        }
                    };
                    *last = Some((key, ord));
                    ord
                }
            };
            let ord = ord as usize;
            for acc in &mut accs[ord * n_aggs..(ord + 1) * n_aggs] {
                acc.step_from_batch(&mb, row, w);
            }
        }
        Ok(())
    }

    /// Emit one partial reduce-output row per present group (weight +1), in the
    /// synthetic-fold reply layout and group-discovery order. A worker that saw
    /// no rows emits an empty batch (the client synthesizes the global ground
    /// row when needed).
    pub(crate) fn finish(self) -> Batch {
        let n_aggs = self.plan.agg_descs.len();
        // The exact output row count is the group count — reserve once.
        let mut output = Batch::with_capacity(self.plan.output_schema, self.rep_rows.count.max(1));
        let rep_mb = self.rep_rows.as_mem_batch();
        let stride = self.plan.output_schema.pk_stride() as usize;
        for ord in 0..self.rep_rows.count {
            // Synthetic `_agg_pk`: the group key (re-derived from the retained
            // representative row — a pure function of its group columns),
            // order-preserving big-endian truncated to the PK stride — the same
            // bytes `op_reduce` writes.
            let key = match &self.keyer {
                Some(k) => k.key_row(&rep_mb, ord),
                None => global_group_key(),
            };
            let pk = NarrowPkOpk::new(key, stride);
            emit_reduce_row(
                &mut output,
                (&rep_mb, ord),
                pk.bytes(),
                &self.accs[ord * n_aggs..(ord + 1) * n_aggs],
                &self.plan,
            );
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::codec::read_i64_le;
    use crate::schema::{type_code, SchemaColumn};
    use crate::storage::Layout;
    use gnitz_wire::{AggFunc, AggReadItem, AGG_COUNT, AGG_COUNT_NON_NULL, AGG_MAX, AGG_MIN, AGG_SUM};

    // Source: pk(U64), grp(I64), val(I64, nullable). val is payload slot 1.
    fn src_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        )
    }

    // Reply (SyntheticFold): _agg_pk(U128), grp(I64), then one I64 per agg spec.
    fn reply_schema(n_aggs: usize) -> SchemaDescriptor {
        let mut cols = vec![
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
        ];
        for _ in 0..n_aggs {
            cols.push(SchemaColumn::new(type_code::I64, 1));
        }
        SchemaDescriptor::new(&cols, &[0])
    }

    /// (pk, weight, grp, Option<val>) → a consolidated source batch.
    fn build(rows: &[(u64, i64, i64, Option<i64>)]) -> Batch {
        let s = src_schema();
        let mut b = Batch::with_capacity(s, rows.len().max(1));
        for &(pk, w, grp, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            let null_word = if val.is_none() { 1u64 << 1 } else { 0 };
            b.extend_null_bmp(&null_word.to_le_bytes());
            b.extend_col(0, &grp.to_le_bytes()); // grp (payload 0)
            b.extend_col(1, &val.unwrap_or(0).to_le_bytes()); // val (payload 1)
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    }

    fn agg(op: u64, col: u16) -> AggReadItem {
        AggReadItem {
            op: AggFunc::from_wire(op).unwrap(),
            src_col: col,
        }
    }

    /// Collect the partial output by group value (grp is payload col 0), returning
    /// per group `(weight, [Option<i64> per agg col])`.
    #[allow(clippy::type_complexity)]
    fn by_group(out: &Batch, n_aggs: usize) -> std::collections::HashMap<i64, (i64, Vec<Option<i64>>)> {
        let mb = out.as_mem_batch();
        let mut map = std::collections::HashMap::new();
        for row in 0..out.count {
            // `read_i64_le` takes a byte offset; every column here is an 8-byte I64.
            let grp = read_i64_le(out.col_data(0), row * 8);
            let nw = mb.get_null_word(row);
            let vals: Vec<Option<i64>> = (0..n_aggs)
                .map(|k| {
                    // Agg col k is payload slot 1 + k (slot 0 is the group col).
                    if gnitz_wire::null_word_get(nw, 1 + k) {
                        None
                    } else {
                        Some(read_i64_le(out.col_data(1 + k), row * 8))
                    }
                })
                .collect();
            map.insert(grp, (mb.get_weight(row), vals));
        }
        map
    }

    #[test]
    fn fold_grouped_multi_agg_with_nulls() {
        // group 10: 100, 200, NULL  → COUNT*=3, COUNT(val)=2, SUM=300, MIN=100, MAX=200
        // group 20: 50 at weight 2   → COUNT*=2, COUNT(val)=2, SUM=100, MIN=50,  MAX=50
        // group 30: NULL             → COUNT*=1, COUNT(val)=0, SUM/MIN/MAX = NULL
        let batch = build(&[
            (1, 1, 10, Some(100)),
            (2, 1, 10, Some(200)),
            (3, 1, 10, None),
            (4, 2, 20, Some(50)),
            (5, 1, 30, None),
        ]);
        let spec = AggReadSpec {
            group_cols: vec![1],
            aggs: vec![
                agg(AGG_COUNT, 0),
                agg(AGG_COUNT_NON_NULL, 2),
                agg(AGG_SUM, 2),
                agg(AGG_MIN, 2),
                agg(AGG_MAX, 2),
            ],
        };
        let (src, reply) = (src_schema(), reply_schema(5));
        let mut fold = AdhocFold::new(&src, &reply, &spec, 1000).unwrap();
        fold.fold_ranges(&batch, &[(0, batch.count)]).unwrap();
        let g = by_group(&fold.finish(), 5);
        assert_eq!(g.len(), 3);
        assert_eq!(g[&10], (1, vec![Some(3), Some(2), Some(300), Some(100), Some(200)]));
        assert_eq!(g[&20], (1, vec![Some(2), Some(2), Some(100), Some(50), Some(50)]));
        assert_eq!(g[&30], (1, vec![Some(1), Some(0), None, None, None]));
    }

    #[test]
    fn fold_accumulates_across_chunks() {
        let spec = AggReadSpec {
            group_cols: vec![1],
            aggs: vec![agg(AGG_COUNT, 0), agg(AGG_SUM, 2)],
        };
        let (src, reply) = (src_schema(), reply_schema(2));
        let mut fold = AdhocFold::new(&src, &reply, &spec, 1000).unwrap();
        let (c1, c2) = (
            build(&[(1, 1, 7, Some(10)), (2, 1, 7, Some(20))]),
            build(&[(3, 1, 7, Some(5)), (4, 1, 8, Some(99))]),
        );
        fold.fold_ranges(&c1, &[(0, c1.count)]).unwrap();
        fold.fold_ranges(&c2, &[(0, c2.count)]).unwrap();
        let g = by_group(&fold.finish(), 2);
        assert_eq!(g[&7], (1, vec![Some(3), Some(35)]));
        assert_eq!(g[&8], (1, vec![Some(1), Some(99)]));
    }

    /// The range bounds are the filter's survivor ranges: rows outside every
    /// folded range contribute nothing, and a group discovered only in a skipped
    /// range never appears.
    #[test]
    fn fold_ranges_folds_only_the_given_ranges() {
        let spec = AggReadSpec {
            group_cols: vec![1],
            aggs: vec![agg(AGG_COUNT, 0), agg(AGG_SUM, 2)],
        };
        let (src, reply) = (src_schema(), reply_schema(2));
        let batch = build(&[
            (1, 1, 7, Some(10)),
            (2, 1, 9, Some(999)), // skipped
            (3, 1, 7, Some(20)),
            (4, 1, 7, Some(30)),
        ]);
        let mut fold = AdhocFold::new(&src, &reply, &spec, 1000).unwrap();
        // Two survivor ranges: [0,1) and [2,4) — row 1 (group 9) is filtered out.
        fold.fold_ranges(&batch, &[(0, 1), (2, 4)]).unwrap();
        let g = by_group(&fold.finish(), 2);
        assert_eq!(g.len(), 1, "group 9 lived only in the skipped range");
        assert_eq!(g[&7], (1, vec![Some(3), Some(60)]));
    }

    #[test]
    fn fold_global_single_group() {
        let spec = AggReadSpec {
            group_cols: vec![],
            aggs: vec![agg(AGG_COUNT, 0), agg(AGG_MAX, 2)],
        };
        let src = src_schema();
        // Global reply: _agg_pk(U128) + 2 agg cols (no group col).
        let reply = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 1),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );
        let mut fold = AdhocFold::new(&src, &reply, &spec, 1000).unwrap();
        let b = build(&[(1, 1, 0, Some(3)), (2, 1, 0, Some(9)), (3, 1, 0, Some(1))]);
        fold.fold_ranges(&b, &[(0, b.count)]).unwrap();
        let out = fold.finish();
        assert_eq!(out.count, 1);
        assert_eq!(read_i64_le(out.col_data(0), 0), 3); // COUNT*
        assert_eq!(read_i64_le(out.col_data(1), 0), 9); // MAX
        assert_eq!(out.as_mem_batch().get_weight(0), 1);
    }

    #[test]
    fn fold_group_cap_aborts() {
        let spec = AggReadSpec {
            group_cols: vec![1],
            aggs: vec![agg(AGG_COUNT, 0)],
        };
        let (src, reply) = (src_schema(), reply_schema(1));
        // Cap of 2 distinct groups; a third distinct group trips it.
        let mut fold = AdhocFold::new(&src, &reply, &spec, 2).unwrap();
        let b = build(&[(1, 1, 1, Some(0)), (2, 1, 2, Some(0)), (3, 1, 3, Some(0))]);
        let err = fold.fold_ranges(&b, &[(0, b.count)]).unwrap_err();
        assert!(err.contains("CREATE VIEW"), "{err}");
    }

    /// A structurally valid reply schema that is not the derived SyntheticFold
    /// layout (here: missing the agg column) is a malformed frame — rejected,
    /// never fed to `ReducePlan::new` (whose `cbase` arithmetic would panic).
    #[test]
    fn fold_rejects_mismatched_reply_schema() {
        let spec = AggReadSpec {
            group_cols: vec![1],
            aggs: vec![agg(AGG_COUNT, 0)],
        };
        let src = src_schema();
        let too_few = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        assert!(AdhocFold::new(&src, &too_few, &spec, 1000).is_err());
        let wrong_type = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::F64, 0), // COUNT partial is I64
            ],
            &[0],
        );
        assert!(AdhocFold::new(&src, &wrong_type, &spec, 1000).is_err());
    }

    #[test]
    fn fold_rejects_out_of_range_column() {
        let spec = AggReadSpec {
            group_cols: vec![9], // no such column
            aggs: vec![agg(AGG_COUNT, 0)],
        };
        let (src, reply) = (src_schema(), reply_schema(1));
        assert!(AdhocFold::new(&src, &reply, &spec, 1000).is_err());
    }
}
