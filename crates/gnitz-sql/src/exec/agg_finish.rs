//! Client-side finishing for an ad-hoc aggregate / DISTINCT SELECT.
//!
//! The workers return one concatenated `ZSetBatch` of per-worker partial reduce
//! rows (pure append, never consolidated). This module combines them by
//! group-column **value** (weight-aware / Z-set-exact), synthesizes the global
//! ground row, finishes AVG / nullable-SUM, filters by HAVING through the shared
//! expression evaluator (the same compiled program a grouped view's post-reduce
//! FILTER runs), projects to SELECT order, and emits a batch carrying a hidden
//! synthetic PK (stripped at presentation). The caller then applies the shared
//! ORDER BY / OFFSET / LIMIT sink.
//!
//! Partial reply layout (the batch this module consumes) is the shared
//! SyntheticFold layout (`crate::agg::synthetic_fold_cols`):
//! `[_group_pk U128 (hidden PK) | group cols (source types) | one partial
//! column per physical agg spec]`. The `_group_pk` hash is ignored — grouping
//! is by value, so no collision handling is needed.

use std::cmp::Ordering;
use std::collections::HashMap;

use gnitz_core::{null_word_get, null_word_set, ColData, ColumnDef, ReduceOutKey, Schema, TypeCode, ZSetBatch};
use gnitz_expr::Evaluator;
use gnitz_wire::{cmp_typed_le, AggFunc as WireAggFunc};

use crate::agg::{group_col_reduce_pos, AggShape, AggSpec, GroupByLayout, GroupBySelectItem};
use crate::error::GnitzSqlError;
use crate::exec::batch::filter_batch;
use crate::validate::reject_duplicate_column_names;

/// Everything the finish needs from the SQL layer, borrowed from the routing
/// arm. `having` is the HAVING predicate **compiled at plan time** against
/// `partial_schema` — `None` when the query has no HAVING or it folded to a
/// statically-true constant.
pub(crate) struct AggFinish<'a> {
    pub source_schema: &'a Schema,
    /// The shared aggregate layout (group columns, specs, mappings, SELECT
    /// order) — identical to what the view path would compile.
    pub layout: &'a GroupByLayout,
    /// The partial reply schema (what `partial` decodes against).
    pub partial_schema: &'a Schema,
    /// The final output schema (`build_agg_out_schema`, computed at plan time
    /// so a bad shape rejects before the fold is dispatched).
    pub out_schema: &'a Schema,
    pub having: Option<&'a Evaluator>,
}

/// One physical agg column's cross-worker combiner. The variant is selected by
/// the shared partial-merge rule (`gnitz_wire::AggFunc::merge_func` — the same
/// rule the view path's two-phase combine reduce ships to the engine), so the
/// two combiners cannot drift.
enum ColAcc {
    /// Integer SUM (I64/U64 bit pattern) — Σ w·partial, and the COUNT /
    /// COUNT_NON_NULL partials too, which accumulate identically and differ only
    /// in starting out non-NULL. `seen` is false until a partial contributes, so
    /// an uncontributed SUM (the ground row, or an all-NULL NullfillSum group)
    /// renders NULL. `tc` is the declared partial-column type, which is what
    /// `sum_f64` divides at: `agg_output_type(Sum, U64)` is U64, so past 2^63 a
    /// signed read of `bits` would be negative.
    IntSum { bits: i64, seen: bool, tc: TypeCode },
    /// Float SUM — Σ (w as f64)·partial. Deliberately worker-count-
    /// nondeterministic: IEEE-754 addition is non-associative and the partials
    /// arrive in worker order, whereas a float-SUM *view* keeps the
    /// deterministic single-worker funnel (its two-phase combine excludes
    /// float SUM). The whole point of the fold is not shipping rows, so the
    /// per-worker split is inherent here.
    FloatSum { val: f64, seen: bool },
    /// MIN / MAX — the winning cell's raw LE bytes (first `wire_stride(tc)` of
    /// `best`), ordered by the shared `cmp_typed_le` — the same typed order the
    /// engine accumulator and the client sort sink use.
    Extreme {
        best: Option<[u8; 8]>,
        is_max: bool,
        tc: TypeCode,
    },
}

impl ColAcc {
    fn new(spec: &AggSpec) -> ColAcc {
        match spec.op.merge_func() {
            // A count is a sum that starts out non-NULL: its partials are never
            // null, and an uncontributed group renders 0 rather than NULL.
            WireAggFunc::SumZero => ColAcc::IntSum {
                bits: 0,
                seen: true,
                tc: spec.out_type,
            },
            WireAggFunc::Sum if spec.out_type.is_float() => ColAcc::FloatSum { val: 0.0, seen: false },
            WireAggFunc::Sum => ColAcc::IntSum {
                bits: 0,
                seen: false,
                tc: spec.out_type,
            },
            WireAggFunc::Min => ColAcc::Extreme {
                best: None,
                is_max: false,
                tc: spec.out_type,
            },
            WireAggFunc::Max => ColAcc::Extreme {
                best: None,
                is_max: true,
                tc: spec.out_type,
            },
            WireAggFunc::Count | WireAggFunc::CountNonNull => {
                unreachable!("merge_func never yields a raw count")
            }
        }
    }
}

/// Pre-resolved SELECT item (computed once per query, not per group): where the
/// value comes from, and the output cell it lands in.
struct OutItem {
    src: ItemSrc,
    /// Output column index, its dense payload slot, and its type — the output
    /// schema is immutable, so resolving these per group would repeat one answer.
    ci: usize,
    pi: usize,
    tc: TypeCode,
}

/// A group column's partial-batch column index, or the aggregate mapping index.
enum ItemSrc {
    Group { partial_ci: usize },
    Agg { agg_idx: usize },
}

/// Combine, finish, and project the concatenated worker partials into the final
/// result batch (with a hidden synthetic PK). The caller applies ORDER BY /
/// OFFSET / LIMIT afterwards.
pub(crate) fn agg_finish(spec: &AggFinish, partial: &ZSetBatch) -> ZSetBatch {
    let layout = spec.layout;
    let n_group = layout.group_col_indices.len();
    let n_aggs = layout.agg_specs.len();
    // Partial columns: group cols at ci 1..1+n_group, agg partials at
    // 1+n_group+k (ci 0 is the hidden _group_pk PK).

    // 1. Combine by group-column value image (weight-aware). `reps[g]` is the
    //    representative partial row of group ordinal `g` (`None` = the
    //    synthesized global ground group, which has no group columns to read),
    //    `accs` the flat accumulator matrix `accs[g*n_aggs..(g+1)*n_aggs]` —
    //    one growth stream, mirroring the engine fold. The key is built into
    //    one reused scratch buffer; an owned copy is allocated only for a
    //    genuinely new group (each group recurs on up to W workers).
    let mut by_key: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut reps: Vec<Option<usize>> = Vec::new();
    let mut accs: Vec<ColAcc> = Vec::new();
    let mut key_scratch: Vec<u8> = Vec::new();
    for row in 0..partial.len() {
        let w = partial.weights[row];
        if w <= 0 {
            continue;
        }
        group_key(partial, spec.partial_schema, n_group, row, &mut key_scratch);
        let g = match by_key.get(key_scratch.as_slice()) {
            Some(&g) => g,
            None => {
                let g = reps.len();
                by_key.insert(key_scratch.clone(), g);
                reps.push(Some(row));
                accs.extend(layout.agg_specs.iter().map(ColAcc::new));
                g
            }
        };
        for (k, acc) in accs[g * n_aggs..(g + 1) * n_aggs].iter_mut().enumerate() {
            combine(acc, partial, spec.partial_schema, 1 + n_group + k, row, w);
        }
    }

    // 2. Global ground row: a global aggregate with no surviving partial emits one
    //    synthetic row (COUNT 0 / others NULL), which HAVING still filters.
    if layout.global_ground() && reps.is_empty() {
        reps.push(None);
        accs.extend(layout.agg_specs.iter().map(ColAcc::new));
    }

    // 3–5. Finish + HAVING + project into the output batch. Zero groups needs no
    // guard: `filter` over a 0-row batch calls back zero times, and
    // `emit_range(0, 0)` is a no-op.
    let out_items: Vec<OutItem> = layout
        .select_items
        .iter()
        .enumerate()
        .map(|(si, item)| OutItem {
            src: match item {
                GroupBySelectItem::GroupCol { src_col, .. } => ItemSrc::Group {
                    partial_ci: group_col_reduce_pos(
                        *src_col,
                        ReduceOutKey::SyntheticFold,
                        spec.source_schema,
                        &layout.group_col_indices,
                    ),
                },
                GroupBySelectItem::Aggregate { agg_idx } => ItemSrc::Agg { agg_idx: *agg_idx },
            },
            // Output column 0 is the hidden PK, so SELECT item `si` lands at `si + 1`.
            ci: 1 + si,
            pi: spec.out_schema.payload_idx(1 + si),
            tc: spec.out_schema.columns[1 + si].type_code,
        })
        .collect();
    let mut out = ZSetBatch::with_capacity(spec.out_schema, reps.len());
    // Both arms speak in half-open group ranges, which is what
    // `Evaluator::filter` hands back; without a HAVING every group is one range.
    let mut emit_range = |start: usize, end: usize| {
        for g in start..end {
            emit_row(
                spec,
                partial,
                &out_items,
                &mut out,
                reps[g],
                &accs[g * n_aggs..(g + 1) * n_aggs],
            );
        }
    };
    match spec.having {
        None => emit_range(0, reps.len()),
        Some(ev) => {
            // One batch, one `filter` call — so the truth rule is the engine
            // filter's own (`bool_bits & !null_bits`), and the region list, which
            // borrows the buffers and so cannot be cached, is built once.
            let groups = fill_group_batch(spec, partial, &reps, &accs);
            filter_batch(ev, &groups, spec.partial_schema, emit_range);
        }
    }
    out
}

/// One row per group in the partial-reply layout — the client's reduce output:
/// `[_group_pk | group cols copied from the representative partial row | raw
/// accumulator values]`. `reps[g] == None` is the synthesized global ground
/// group, which by construction has no group columns.
///
/// Filled a column at a time, so the destination column and its `ColData`
/// variant are resolved once per column rather than once per cell.
fn fill_group_batch(spec: &AggFinish, partial: &ZSetBatch, reps: &[Option<usize>], accs: &[ColAcc]) -> ZSetBatch {
    let schema = spec.partial_schema;
    let n_group = spec.layout.group_col_indices.len();
    let n_aggs = spec.layout.agg_specs.len();
    let n = reps.len();

    let mut dst = ZSetBatch::with_capacity(schema, n);
    // `_group_pk` is the dense group ordinal: present, unreferenceable. Pushed
    // rather than assigned as a `PkColumn` variant, so the variant stays the one
    // `empty_for_schema` derived from the schema.
    for g in 0..n {
        dst.pks.push_u128(g as u128);
    }
    dst.weights.resize(n, 1);
    // One word per row, one bit per payload slot: materialized at its final
    // length up front so each column pass can OR in its own bit `pi`.
    dst.nulls.resize(n, 0);
    let ZSetBatch { nulls, columns, .. } = &mut dst;

    // `payload_columns` yields the payload slot as its enumeration ordinal, so
    // `pi` is also the column's position in this layout: slots `0..n_group` are
    // the group columns, the rest the agg partials.
    for (pi, ci, col) in schema.payload_columns() {
        let (tc, w) = (col.type_code, col.type_code.wire_stride());
        if pi < n_group {
            // A group column keeps its source type, so the move goes through the
            // exhaustive `push_row_from` — a new `ColData` variant then has to be
            // handled there rather than panicking at runtime.
            for (g, &rep) in reps.iter().enumerate() {
                let rep = rep.expect("a grouped result always has a representative row");
                if null_word_get(partial.nulls[rep], pi) {
                    push_null_cell(&mut columns[ci], tc, &mut nulls[g], pi);
                } else {
                    partial.columns[ci].push_row_from(rep, w, &mut columns[ci]);
                }
            }
        } else {
            // `accs` is group-major (the combine loop writes a whole group row at
            // a time), so this column's cells are `n_aggs` apart.
            let k = pi - n_group;
            for g in 0..n {
                match acc_bits(&accs[g * n_aggs + k]) {
                    None => push_null_cell(&mut columns[ci], tc, &mut nulls[g], pi),
                    Some(bits) => push_fixed_bits(&mut columns[ci], bits, w),
                }
            }
        }
    }

    // The one rule `ViewBuffers::regions` does not already assert on this batch
    // two lines later: a set null bit under a NOT NULL group column would leave
    // the resolved program's `no_nulls` on, and the evaluator would read
    // `push_null`'s zero bytes as a real `0`. Unconditional — release is where a
    // stale bit becomes a silently wrong answer rather than a panic.
    dst.validate(schema)
        .expect("the group batch must satisfy the partial schema");
    dst
}

/// Append a NULL cell to `col` and record it at payload slot `pi` in `word` —
/// the one place the null bitmap and the pushed filler bytes are kept in step.
fn push_null_cell(col: &mut ColData, tc: TypeCode, word: &mut u64, pi: usize) {
    null_word_set(word, pi, true);
    col.push_null(tc);
}

// ---------------------------------------------------------------------------
// Combine
// ---------------------------------------------------------------------------

/// Build one flattened group-identity key for a partial row into `key`
/// (cleared first): per group column a null marker byte, then the
/// length-prefixed value image. Collision-free (the length prefix keeps
/// adjacent columns from aliasing) and allocation-free per row.
fn group_key(partial: &ZSetBatch, schema: &Schema, n_group: usize, row: usize, key: &mut Vec<u8>) {
    key.clear();
    for g in 0..n_group {
        let ci = 1 + g;
        if partial.is_null(schema, row, ci) {
            key.push(0);
            continue;
        }
        key.push(1);
        let mut put = |b: &[u8]| {
            key.extend_from_slice(&(b.len() as u32).to_le_bytes());
            key.extend_from_slice(b);
        };
        match &partial.columns[ci] {
            ColData::Fixed(buf) => {
                let s = schema.columns[ci].type_code.wire_stride();
                put(&buf[row * s..(row + 1) * s]);
            }
            ColData::Strings(v) => put(v[row].as_deref().unwrap_or("").as_bytes()),
            ColData::Bytes(v) => put(v[row].as_deref().unwrap_or(&[])),
            ColData::U128s(v) => put(&v[row].to_le_bytes()),
        }
    }
}

fn combine(acc: &mut ColAcc, partial: &ZSetBatch, schema: &Schema, ci: usize, row: usize, w: i64) {
    if partial.is_null(schema, row, ci) {
        return; // NULL partials skip (COUNT/COUNT_NON_NULL partials are never null)
    }
    match acc {
        // COUNT and SUM partials are 8-byte cells (I64, or U64 whose bit
        // pattern is the true sum mod 2^64 — the same i64 accumulator the
        // engine folds).
        ColAcc::IntSum { bits, seen, .. } => {
            *bits = bits.wrapping_add(w.wrapping_mul(read_i64_8(partial, ci, row)));
            *seen = true;
        }
        ColAcc::FloatSum { val, seen } => {
            *val += (w as f64) * read_f64(partial, ci, row);
            *seen = true;
        }
        ColAcc::Extreme { best, is_max, tc } => {
            let s = tc.wire_stride();
            let cand = fixed_slice(partial, ci, row, s);
            let replace = match best {
                None => true,
                Some(b) => (cmp_typed_le(cand, &b[..s], *tc as u8) == Ordering::Greater) == *is_max,
            };
            if replace {
                let mut b = [0u8; 8];
                b[..s].copy_from_slice(cand);
                *best = Some(b);
            }
        }
    }
}

fn fixed_slice(partial: &ZSetBatch, ci: usize, row: usize, stride: usize) -> &[u8] {
    match &partial.columns[ci] {
        ColData::Fixed(buf) => &buf[row * stride..(row + 1) * stride],
        _ => unreachable!("ad-hoc numeric agg partial column is not Fixed"),
    }
}

fn read_i64_8(partial: &ZSetBatch, ci: usize, row: usize) -> i64 {
    i64::from_le_bytes(fixed_slice(partial, ci, row, 8).try_into().unwrap())
}

fn read_f64(partial: &ZSetBatch, ci: usize, row: usize) -> f64 {
    f64::from_le_bytes(fixed_slice(partial, ci, row, 8).try_into().unwrap())
}

// ---------------------------------------------------------------------------
// Finishing / rendering
// ---------------------------------------------------------------------------

/// Finish one aggregate to its output cell, per its mapping's shape (AVG
/// divide, nullable-SUM null-gate, or the accumulator's own value).
fn finish_agg(spec: &AggFinish, accs: &[ColAcc], agg_idx: usize) -> Option<u64> {
    let m = &spec.layout.agg_mappings[agg_idx];
    let sum = &accs[m.specs_start];
    if !m.shape.has_count_companion() {
        return acc_bits(sum);
    }
    // AVG and nullable SUM both take their null-ness from the CountNonNull
    // companion at `specs_start + 1` rather than from the value accumulator.
    let cnt = acc_count(&accs[m.specs_start + 1]);
    if cnt == 0 {
        return None;
    }
    match m.shape {
        AggShape::Avg => Some((sum_f64(sum) / cnt as f64).to_bits()),
        AggShape::NullfillSum | AggShape::Direct => acc_bits(sum),
    }
}

/// The combined accumulator's register image, or `None` for SQL NULL — shared by
/// the aggregate render and the group batch the HAVING filter runs over, so the
/// two cannot drift. NULL for an uncontributed SUM / MIN / MAX (the global ground
/// row, or an all-NULL group); counts are always concrete.
///
/// Every aggregate column is a Fixed of width `wire_stride(tc)`, and both writers
/// keep only that many low bytes, so an accumulator's bytes pass through
/// unchanged — the value never has to be decoded to a number to be re-emitted.
fn acc_bits(acc: &ColAcc) -> Option<u64> {
    match acc {
        ColAcc::IntSum { bits, seen, .. } => seen.then_some(*bits as u64),
        ColAcc::FloatSum { val, seen } => seen.then(|| val.to_bits()),
        // `combine` zero-fills above the winning cell's `wire_stride(tc)` bytes.
        ColAcc::Extreme { best, .. } => best.map(u64::from_le_bytes),
    }
}

/// AVG's numerator as a number. The one place an accumulator is read as a
/// quantity rather than as bytes, so it is also the one place signedness
/// matters: a SUM over a U64 source is typed U64, and its i64 accumulator holds
/// the true sum mod 2^64 — read signed, a sum past 2^63 averages negative.
fn sum_f64(acc: &ColAcc) -> f64 {
    match acc {
        ColAcc::IntSum { bits, tc, .. } if tc.is_signed_int() => *bits as f64,
        ColAcc::IntSum { bits, .. } => *bits as u64 as f64,
        ColAcc::FloatSum { val, .. } => *val,
        ColAcc::Extreme { .. } => unreachable!("AVG's numerator is its SUM component"),
    }
}

fn acc_count(acc: &ColAcc) -> i64 {
    match acc {
        ColAcc::IntSum { bits, .. } => *bits,
        _ => unreachable!("companion CountNonNull is an integer accumulator"),
    }
}

// ---------------------------------------------------------------------------
// Output schema + row emission
// ---------------------------------------------------------------------------

/// The final output schema of an ad-hoc aggregate / DISTINCT SELECT: a hidden
/// synthetic PK (like `_distinct_pk` / `_group_pk`, stripped at presentation)
/// followed by the SELECT-order visible columns. Rejects duplicate visible
/// names — the same gate every view compile applies — so the routing arm fails
/// at plan time (and the executor re-raises the identical error) instead of
/// returning a dup-named result the view path would refuse to create.
pub(crate) fn build_agg_out_schema(layout: &GroupByLayout, source_schema: &Schema) -> Result<Schema, GnitzSqlError> {
    let mut cols = vec![ColumnDef::new("_agg_pk", TypeCode::U128, false).hidden()];
    for item in &layout.select_items {
        match item {
            GroupBySelectItem::GroupCol { src_col, name } => {
                let src = &source_schema.columns[*src_col];
                cols.push(ColumnDef::new(name.clone(), src.type_code, src.is_nullable));
            }
            GroupBySelectItem::Aggregate { agg_idx } => {
                let m = &layout.agg_mappings[*agg_idx];
                cols.push(ColumnDef::new(m.output_name.clone(), m.output_type, m.output_nullable));
            }
        }
    }
    reject_duplicate_column_names(&cols, "aggregate SELECT")?;
    Schema::from_parts(cols, vec![0])
        .map_err(|e| GnitzSqlError::Unsupported(format!("ad-hoc aggregate output schema is invalid: {e}")))
}

/// Project one group into `out`. The synthetic PK is the row's own ordinal, so
/// it is read off `out` rather than threaded in.
fn emit_row(
    spec: &AggFinish,
    partial: &ZSetBatch,
    out_items: &[OutItem],
    out: &mut ZSetBatch,
    rep: Option<usize>,
    accs: &[ColAcc],
) {
    let out_pk = out.len() as u128;
    out.pks.push_u128(out_pk);
    out.weights.push(1);
    let mut null_word: u64 = 0;
    for item in out_items {
        let col = &mut out.columns[item.ci];
        match &item.src {
            ItemSrc::Group { partial_ci } => {
                let rep = rep.expect("a grouped result always has a representative row");
                if partial.is_null(spec.partial_schema, rep, *partial_ci) {
                    push_null_cell(col, item.tc, &mut null_word, item.pi);
                } else {
                    partial.columns[*partial_ci].push_row_from(rep, item.tc.wire_stride(), col);
                }
            }
            ItemSrc::Agg { agg_idx } => match finish_agg(spec, accs, *agg_idx) {
                None => push_null_cell(col, item.tc, &mut null_word, item.pi),
                Some(bits) => push_fixed_bits(col, bits, item.tc.wire_stride()),
            },
        }
    }
    out.nulls.push(null_word);
}

/// Push the low `stride` bytes of `bits` into a Fixed aggregate column. Every
/// aggregate column — partial or output — is a `Fixed` of width ≤ 8:
/// `agg_output_type` routes float SUM/MIN/MAX to F64 and SUM through
/// `register_image_type`, and preserves a ≤8-byte integer source's own width for
/// MIN/MAX.
fn push_fixed_bits(col: &mut ColData, bits: u64, stride: usize) {
    match col {
        ColData::Fixed(buf) => buf.extend_from_slice(&bits.to_le_bytes()[..stride]),
        _ => unreachable!("an ad-hoc aggregate column is a Fixed of width <= 8"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg::{synthetic_fold_cols, AggMapping};
    use crate::ir::AggFunc;
    use crate::test_support::col_def;
    use gnitz_core::PkColumn;

    /// `(pk U64 | g I64 nullable | sm I16 nullable)` — one nullable group column
    /// and a narrow aggregate source, so the fill exercises a NULL group value
    /// and a sub-8-byte width.
    fn source_schema() -> Schema {
        Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("g", TypeCode::I64, true),
                col_def("sm", TypeCode::I16, true),
            ],
            pk_cols: vec![0],
        }
    }

    /// Two specs so `accs` is genuinely group-major with a stride of 2:
    /// `MIN(sm)` at the source's own 2-byte width, then `COUNT(*)`.
    fn agg_specs() -> Vec<AggSpec> {
        vec![
            AggSpec {
                op: WireAggFunc::Min,
                col: 2,
                out_type: TypeCode::I16,
            },
            AggSpec {
                op: WireAggFunc::Count,
                col: 0,
                out_type: TypeCode::I64,
            },
        ]
    }

    fn partial_schema(src: &Schema, specs: &[AggSpec]) -> Schema {
        Schema::from_parts(synthetic_fold_cols(src, &[1], specs, &|_| true), vec![0])
            .expect("the SyntheticFold layout is a valid client schema")
    }

    /// A combined COUNT accumulator, in the shape `ColAcc::new` builds for a
    /// `SumZero` merge.
    fn count_acc(n: i64) -> ColAcc {
        ColAcc::IntSum {
            bits: n,
            seen: true,
            tc: TypeCode::I64,
        }
    }

    fn fixed(col: &ColData) -> &[u8] {
        match col {
            ColData::Fixed(b) => b,
            _ => panic!("expected a Fixed column"),
        }
    }

    /// The group batch the HAVING filter runs over: group columns copied from
    /// each representative partial row, aggregate partials taken from `accs` at
    /// the declared width, and one null bit per payload slot. A wrong bit here
    /// is a silently wrong HAVING verdict, not a crash.
    #[test]
    fn fill_group_batch_lays_out_values_and_null_bits() {
        let src = source_schema();
        let specs = agg_specs();
        let partial_s = partial_schema(&src, &specs);
        // `fill_group_batch` never reads the output schema; it only has to exist.
        let out_s = Schema::from_parts(vec![col_def("_agg_pk", TypeCode::U128, false).hidden()], vec![0]).unwrap();
        let layout = GroupByLayout {
            group_col_indices: vec![1],
            agg_specs: specs,
            agg_mappings: vec![],
            select_items: vec![],
        };

        // Two representative partial rows: group 0 has g = 10, group 1 has g NULL
        // (payload slot 0). The agg columns are never read from `partial`.
        let mut partial = ZSetBatch::new(&partial_s);
        for (row, g) in [10i64, 0].into_iter().enumerate() {
            partial.pks.push_u128(row as u128);
            partial.weights.push(1);
            partial.nulls.push(if row == 1 { 0b1 } else { 0 });
            push_fixed_bits(&mut partial.columns[1], g as u64, 8);
            push_fixed_bits(&mut partial.columns[2], 0, 2);
            push_fixed_bits(&mut partial.columns[3], 0, 8);
        }

        let reps = [Some(0usize), Some(1usize)];
        let accs = vec![
            // Group 0: MIN(sm) = -5, COUNT = 2.
            ColAcc::Extreme {
                best: Some([0xfb, 0xff, 0, 0, 0, 0, 0, 0]),
                is_max: false,
                tc: TypeCode::I16,
            },
            count_acc(2),
            // Group 1: an all-NULL MIN group, COUNT = 1.
            ColAcc::Extreme {
                best: None,
                is_max: false,
                tc: TypeCode::I16,
            },
            count_acc(1),
        ];

        let spec = AggFinish {
            source_schema: &src,
            layout: &layout,
            partial_schema: &partial_s,
            out_schema: &out_s,
            having: None,
        };
        let got = fill_group_batch(&spec, &partial, &reps, &accs);

        assert_eq!(got.len(), 2);
        assert_eq!(got.weights, vec![1, 1]);
        // Slot 0 = g, slot 1 = MIN(sm), slot 2 = COUNT. Group 1 nulls g and MIN.
        assert_eq!(got.nulls, vec![0, 0b011]);
        // `_group_pk` is the dense group ordinal.
        assert_eq!(got.pks, PkColumn::U128s(vec![0, 1]));
        // g: the copied value, then `push_null`'s zero filler.
        assert_eq!(
            fixed(&got.columns[1]),
            10i64.to_le_bytes().iter().chain(&[0; 8]).copied().collect::<Vec<_>>()
        );
        // MIN(sm) truncated to its declared 2 bytes, then a zeroed NULL cell.
        assert_eq!(fixed(&got.columns[2]), &[0xfb, 0xff, 0, 0]);
        // COUNT is never NULL.
        assert_eq!(
            fixed(&got.columns[3]),
            2i64.to_le_bytes()
                .iter()
                .chain(&1i64.to_le_bytes())
                .copied()
                .collect::<Vec<_>>()
        );
    }

    /// The global ground row: no group columns, every aggregate uncontributed.
    /// `reps[0] == None` must not be dereferenced.
    #[test]
    fn fill_group_batch_handles_the_global_ground_row() {
        let src = source_schema();
        let specs = agg_specs();
        let partial_s = Schema::from_parts(synthetic_fold_cols(&src, &[], &specs, &|_| true), vec![0]).unwrap();
        let out_s = Schema::from_parts(vec![col_def("_agg_pk", TypeCode::U128, false).hidden()], vec![0]).unwrap();
        let layout = GroupByLayout {
            group_col_indices: vec![],
            agg_specs: specs,
            agg_mappings: vec![],
            select_items: vec![],
        };
        let spec = AggFinish {
            source_schema: &src,
            layout: &layout,
            partial_schema: &partial_s,
            out_schema: &out_s,
            having: None,
        };
        let accs = vec![
            ColAcc::Extreme {
                best: None,
                is_max: false,
                tc: TypeCode::I16,
            },
            count_acc(0),
        ];
        let got = fill_group_batch(&spec, &ZSetBatch::new(&partial_s), &[None], &accs);

        assert_eq!(got.len(), 1);
        // Slot 0 = MIN (NULL), slot 1 = COUNT (0, never NULL).
        assert_eq!(got.nulls, vec![0b01]);
        assert_eq!(fixed(&got.columns[2]), &0i64.to_le_bytes());
    }

    /// One AVG mapping over `[Sum, CountNonNull]` — the only layout `finish_agg`
    /// reads, so the rest of the query can stay empty.
    fn avg_layout() -> GroupByLayout {
        GroupByLayout {
            group_col_indices: vec![],
            agg_specs: vec![],
            agg_mappings: vec![AggMapping {
                specs_start: 0,
                shape: AggShape::Avg,
                output_name: "a".to_string(),
                output_type: TypeCode::F64,
                output_nullable: true,
                agg_func: AggFunc::Avg,
                arg_col: None,
            }],
            select_items: vec![],
        }
    }

    fn finish_avg(layout: &GroupByLayout, accs: &[ColAcc]) -> Option<f64> {
        let src = source_schema();
        let out_s = Schema::from_parts(vec![col_def("_agg_pk", TypeCode::U128, false).hidden()], vec![0]).unwrap();
        let spec = AggFinish {
            source_schema: &src,
            layout,
            partial_schema: &src,
            out_schema: &out_s,
            having: None,
        };
        finish_agg(&spec, accs, 0).map(f64::from_bits)
    }

    /// AVG divides its SUM accumulator at the accumulator's declared type. A SUM
    /// over a `BIGINT UNSIGNED` source is typed U64, so past 2^63 its i64 bit
    /// pattern only reads as the true sum unsigned.
    #[test]
    fn avg_divides_an_unsigned_sum_unsigned() {
        let layout = avg_layout();
        // One cell of 2^64 - 1: the accumulator holds -1, which is that sum only
        // when read unsigned. (2^64 - 1 has no exact f64 image; it rounds to 2^64.)
        let unsigned = ColAcc::IntSum {
            bits: -1,
            seen: true,
            tc: TypeCode::U64,
        };
        assert_eq!(
            finish_avg(&layout, &[unsigned, count_acc(1)]),
            Some(1.8446744073709552e19)
        );
        // The same bit pattern over a signed source is genuinely -1.
        let signed = ColAcc::IntSum {
            bits: -1,
            seen: true,
            tc: TypeCode::I64,
        };
        assert_eq!(finish_avg(&layout, &[signed, count_acc(1)]), Some(-1.0));
    }

    /// A zero CountNonNull companion is AVG's NULL — an empty or all-NULL group.
    #[test]
    fn avg_nulls_on_a_zero_count_companion() {
        let layout = avg_layout();
        let sum = ColAcc::IntSum {
            bits: 0,
            seen: false,
            tc: TypeCode::I64,
        };
        assert_eq!(finish_avg(&layout, &[sum, count_acc(0)]), None);
    }
}
