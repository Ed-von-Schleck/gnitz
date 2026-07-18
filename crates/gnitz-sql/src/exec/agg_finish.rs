//! Client-side finishing for an ad-hoc aggregate / DISTINCT SELECT.
//!
//! The workers return one concatenated `ZSetBatch` of per-worker partial reduce
//! rows (pure append, never consolidated). This module combines them by
//! group-column **value** (weight-aware / Z-set-exact), synthesizes the global
//! ground row, finishes AVG / nullable-SUM, evaluates HAVING with a typed IEEE
//! evaluator that mirrors the engine expr VM, projects to SELECT order, and
//! emits a batch carrying a hidden synthetic PK (stripped at presentation). The
//! caller then applies the shared ORDER BY / OFFSET / LIMIT sink.
//!
//! Partial reply layout (the batch this module consumes) is the shared
//! SyntheticFold layout (`crate::agg::synthetic_fold_cols`):
//! `[_group_pk U128 (hidden PK) | group cols (source types) | one partial
//! column per physical agg spec]`. The `_group_pk` hash is ignored — grouping
//! is by value, so no collision handling is needed.

use std::cmp::Ordering;
use std::collections::HashMap;

use gnitz_core::{null_word_set, ColData, ColumnDef, FixedInt, ReduceOutKey, Schema, TypeCode, ZSetBatch};
use gnitz_wire::{cmp_typed_le, AggFunc as WireAggFunc};

use crate::agg::{group_col_reduce_pos, AggShape, AggSpec, GroupByLayout, GroupBySelectItem};
use crate::error::GnitzSqlError;
use crate::ir::{BinOp, BoundExpr, UnaryOp};
use crate::lower::{lower_bound_expr, BoundExprBackend};
use crate::plan::validate::reject_duplicate_column_names;

/// Everything the finish needs from the SQL layer, borrowed from the routing
/// arm. `having` is the **pre-bound, pre-probed** HAVING expression (ColRefs
/// into the reduce-output layout; no aggregate nodes) — `None` when the query
/// has no HAVING or it was rejected to the executor at plan time.
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
    pub having: Option<&'a BoundExpr>,
}

/// One physical agg column's cross-worker combiner. The variant is selected by
/// the shared partial-merge rule (`gnitz_wire::AggFunc::merge_func` — the same
/// rule the view path's two-phase combine reduce ships to the engine), so the
/// two combiners cannot drift.
enum ColAcc {
    /// `SumZero` merge (COUNT / COUNT_NON_NULL partials) — Σ w·partial. Never
    /// NULL; the ground row renders 0.
    Count { n: i64 },
    /// Integer SUM (I64/U64 bit pattern) — Σ w·partial. NULL until a partial
    /// contributes (the ground row, or an all-NULL NullfillSum group).
    IntSum { bits: i64, seen: bool },
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
            WireAggFunc::SumZero => ColAcc::Count { n: 0 },
            WireAggFunc::Sum if spec.out_type.is_float() => ColAcc::FloatSum { val: 0.0, seen: false },
            WireAggFunc::Sum => ColAcc::IntSum { bits: 0, seen: false },
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

/// A typed value used by the HAVING evaluator and the aggregate render.
#[derive(Clone, Copy)]
enum Val {
    Int(i64),
    Float(f64),
    Null,
}

/// Pre-resolved SELECT-item source (computed once per query, not per group): a
/// group column's partial-batch column index, or the aggregate mapping index.
enum ItemSrc {
    Group { partial_ci: usize },
    Agg { agg_idx: usize },
}

/// Combine, finish, and project the concatenated worker partials into the final
/// result batch (with a hidden synthetic PK). The caller applies ORDER BY /
/// OFFSET / LIMIT afterwards.
pub(crate) fn agg_finish(spec: &AggFinish, partial: &ZSetBatch) -> Result<ZSetBatch, GnitzSqlError> {
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

    // 3–5. Finish + HAVING + project into the output batch.
    let item_srcs: Vec<ItemSrc> = layout
        .select_items
        .iter()
        .map(|item| match item {
            GroupBySelectItem::GroupCol { src_col, .. } => ItemSrc::Group {
                partial_ci: group_col_reduce_pos(
                    *src_col,
                    ReduceOutKey::SyntheticFold,
                    spec.source_schema,
                    &layout.group_col_indices,
                ),
            },
            GroupBySelectItem::Aggregate { agg_idx } => ItemSrc::Agg { agg_idx: *agg_idx },
        })
        .collect();
    // HAVING evaluation state, built only when a HAVING survives to runtime.
    let mut having_state = spec.having.map(|having| {
        let types: Vec<TypeCode> = spec.partial_schema.columns.iter().map(|c| c.type_code).collect();
        (having, types, Vec::new())
    });
    let mut out = ZSetBatch::new(spec.out_schema);
    let mut out_pk: u128 = 0;
    for (g, &rep) in reps.iter().enumerate() {
        let gaccs = &accs[g * n_aggs..(g + 1) * n_aggs];
        if let Some((having, types, row)) = &mut having_state {
            fill_having_row(spec, partial, rep, gaccs, row);
            if !truthy(eval_having(having, row, types)?) {
                continue;
            }
        }
        emit_row(spec, partial, &item_srcs, &mut out, out_pk, rep, gaccs);
        out_pk += 1;
    }
    Ok(out)
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
        ColAcc::Count { n } => *n = n.wrapping_add(w.wrapping_mul(read_i64_8(partial, ci, row))),
        ColAcc::IntSum { bits, seen } => {
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

/// Finish one aggregate to its output value, per its mapping's shape (AVG
/// divide, nullable-SUM null-gate, or the accumulator's own value).
fn finish_agg(spec: &AggFinish, accs: &[ColAcc], agg_idx: usize) -> Val {
    let m = &spec.layout.agg_mappings[agg_idx];
    let sum = &accs[m.specs_start];
    match m.shape {
        // AVG and NullfillSum carry a CountNonNull companion at specs_start + 1.
        AggShape::Avg => {
            let cnt = acc_count(&accs[m.specs_start + 1]);
            if cnt == 0 {
                Val::Null
            } else {
                Val::Float(acc_f64(sum) / cnt as f64)
            }
        }
        // Nullable SUM: NULL iff no non-null contributor.
        AggShape::NullfillSum => {
            let cnt = acc_count(&accs[m.specs_start + 1]);
            if cnt == 0 {
                Val::Null
            } else {
                acc_val(sum)
            }
        }
        AggShape::Direct => acc_val(sum),
    }
}

/// The combined accumulator's typed value — shared by the aggregate render and
/// the HAVING row, so the two cannot drift. NULL for an uncontributed SUM /
/// MIN / MAX (the global ground row, or an all-NULL group); counts are always
/// concrete.
fn acc_val(acc: &ColAcc) -> Val {
    match acc {
        ColAcc::Count { n } => Val::Int(*n),
        ColAcc::IntSum { bits, seen } => {
            if *seen {
                Val::Int(*bits)
            } else {
                Val::Null
            }
        }
        ColAcc::FloatSum { val, seen } => {
            if *seen {
                Val::Float(*val)
            } else {
                Val::Null
            }
        }
        ColAcc::Extreme { best, tc, .. } => match best {
            None => Val::Null,
            // MIN/MAX over a float source is typed F64; an integer extreme
            // decodes at its own width (sign-extended for signed sources).
            Some(b) => {
                if tc.is_float() {
                    Val::Float(f64::from_le_bytes(b[..8].try_into().unwrap()))
                } else {
                    let s = tc.wire_stride();
                    Val::Int(
                        FixedInt::from_type_code(*tc)
                            .expect("MIN/MAX output is a ≤8-byte integer or F64")
                            .decode_le_i64(&b[..s]),
                    )
                }
            }
        },
    }
}

fn acc_count(acc: &ColAcc) -> i64 {
    match acc {
        ColAcc::Count { n } => *n,
        _ => unreachable!("companion CountNonNull is always a Count accumulator"),
    }
}

fn acc_f64(acc: &ColAcc) -> f64 {
    match acc {
        ColAcc::IntSum { bits, .. } => *bits as f64,
        ColAcc::FloatSum { val, .. } => *val,
        // AVG's first spec is always AGG_SUM, so its accumulator is a sum.
        ColAcc::Count { .. } | ColAcc::Extreme { .. } => {
            unreachable!("AVG's SUM component is a sum accumulator")
        }
    }
}

// ---------------------------------------------------------------------------
// HAVING evaluation (typed, IEEE, 3VL — mirrors the engine expr VM)
// ---------------------------------------------------------------------------

/// Fill the reduce-output-layout typed row a bound HAVING expr evaluates over:
/// `[_group_pk placeholder | group col values | combined agg values]`, matching
/// the SyntheticFold ColRef indices the HAVING binder produced. `rep` is the
/// group's representative partial row (`None` only for the global ground
/// group, which has no group columns to read).
fn fill_having_row(spec: &AggFinish, partial: &ZSetBatch, rep: Option<usize>, accs: &[ColAcc], row: &mut Vec<Val>) {
    let n_group = spec.layout.group_col_indices.len();
    row.clear();
    row.resize(1 + n_group + spec.layout.agg_specs.len(), Val::Null);
    for g in 0..n_group {
        let rep = rep.expect("a grouped result always has a representative row");
        let ci = 1 + g;
        let tc = spec.partial_schema.columns[ci].type_code;
        // A group column the evaluator supports decodes to i64 (the probe
        // rejected any HAVING that references another type); unsupported types
        // stay NULL — they are unreferenced.
        if !partial.is_null(spec.partial_schema, rep, ci) {
            if let Some(fi) = FixedInt::from_type_code(tc) {
                let s = tc.wire_stride();
                row[ci] = Val::Int(fi.decode_le_i64(fixed_slice(partial, ci, rep, s)));
            }
        }
    }
    for (k, acc) in accs.iter().enumerate() {
        row[1 + n_group + k] = acc_val(acc);
    }
}

/// Whether a HAVING result keeps the group (truthy, NULL/false drop) — matching
/// the engine post-reduce FILTER (`eval_pred_row`: `v != 0`, NULL excluded).
fn truthy(v: Val) -> bool {
    match v {
        Val::Int(i) => i != 0,
        Val::Float(f) => f != 0.0,
        Val::Null => false,
    }
}

/// Typed interpreter backend over one reduce-layout row (`Out = Val`), on the
/// shared `BoundExprBackend` walk. One backend serves both roles — the
/// plan-time support probe ([`having_supported`], over a NULL row) and the
/// per-group runtime evaluation — so the supported node/type set is defined
/// exactly once, and a new `BoundExpr` variant cannot slip through silently
/// (the shared walk fails to compile, or the node lands on a typed
/// `Unsupported` arm that routes the query to the executor).
struct HavingEval<'a> {
    row: &'a [Val],
    /// Reduce-output layout types (`[_group_pk, group cols…, agg partials…]`).
    types: &'a [TypeCode],
}

impl HavingEval<'_> {
    /// Whether `e` statically evaluates to a float: a float literal, a
    /// float-typed column, arithmetic over either, or its negation.
    /// Comparisons and logic are integer-valued. Exact — `Val::Float` arises
    /// at runtime from precisely these shapes — so the plan-time float-modulo
    /// rejection in `binop` covers every runtime occurrence.
    fn is_float_expr(&self, e: &BoundExpr) -> bool {
        match e {
            BoundExpr::ColRef(c) => self.types.get(*c).is_some_and(|tc| tc.is_float()),
            BoundExpr::LitFloat(_) => true,
            BoundExpr::BinOp(l, op, r) => {
                matches!(op, BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod)
                    && (self.is_float_expr(l) || self.is_float_expr(r))
            }
            BoundExpr::UnaryOp(UnaryOp::Neg, inner) => self.is_float_expr(inner),
            _ => false,
        }
    }
}

fn unsupported(what: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("HAVING: {what} not supported in ad-hoc evaluation"))
}

impl BoundExprBackend for HavingEval<'_> {
    type Out = Val;

    fn col_ref(&mut self, c: usize) -> Result<Val, GnitzSqlError> {
        // Only signed / narrow-unsigned integers and floats evaluate; a string
        // / U64 / wide column routes the whole query to the executor at plan
        // time (a U64 would need unsigned compare arms the typed evaluator
        // does not carry).
        let supported = self.types.get(c).is_some_and(|tc| {
            tc.is_float()
                || matches!(tc, TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64)
                || matches!(tc, TypeCode::U8 | TypeCode::U16 | TypeCode::U32)
        });
        if !supported {
            return Err(unsupported("column type"));
        }
        Ok(self.row[c])
    }

    fn lit_int(&mut self, v: i64) -> Result<Val, GnitzSqlError> {
        Ok(Val::Int(v))
    }

    fn lit_float(&mut self, v: f64) -> Result<Val, GnitzSqlError> {
        Ok(Val::Float(v))
    }

    fn lit_str(&mut self, _s: &str) -> Result<Val, GnitzSqlError> {
        Err(unsupported("string literal"))
    }

    fn lit_null(&mut self) -> Result<Val, GnitzSqlError> {
        Ok(Val::Null)
    }

    fn binop(&mut self, l: &BoundExpr, op: BinOp, r: &BoundExpr) -> Result<Val, GnitzSqlError> {
        // The engine expr VM has no float-modulo instruction, so a CREATE VIEW
        // rejects it at compile — reject it here (statically, before the NULL
        // short-circuit, so the all-NULL plan-time probe sees it too) to keep
        // ad-hoc HAVING ⊆ view HAVING.
        if matches!(op, BinOp::Mod) && (self.is_float_expr(l) || self.is_float_expr(r)) {
            return Err(unsupported("float modulo"));
        }
        // Both operands evaluate unconditionally — required by the plan-time
        // probe (every node must be visited) and harmless at runtime (no
        // supported node errors: div-by-zero is NULL, not an error).
        let a = lower_bound_expr(l, self)?;
        let b = lower_bound_expr(r, self)?;
        Ok(match op {
            BinOp::And | BinOp::Or => eval_3vl(op, a, b),
            _ if matches!(a, Val::Null) || matches!(b, Val::Null) => Val::Null,
            BinOp::Eq | BinOp::Ne | BinOp::Gt | BinOp::Ge | BinOp::Lt | BinOp::Le => eval_cmp(op, a, b),
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => eval_arith(op, a, b),
        })
    }

    fn unop(&mut self, op: UnaryOp, inner: &BoundExpr) -> Result<Val, GnitzSqlError> {
        let v = lower_bound_expr(inner, self)?;
        Ok(match op {
            UnaryOp::Neg => match v {
                Val::Int(i) => Val::Int(i.wrapping_neg()),
                Val::Float(f) => Val::Float(-f),
                Val::Null => Val::Null,
            },
            UnaryOp::Not => match v {
                Val::Null => Val::Null, // NOT NULL = NULL
                v => Val::Int((!truthy(v)) as i64),
            },
        })
    }

    fn null_test(&mut self, _col: usize, _want_null: bool) -> Result<Val, GnitzSqlError> {
        // Post-fold IS [NOT] NULL on a bound column (the binder const-folds the
        // decidable shapes away before this).
        Err(unsupported("IS [NOT] NULL"))
    }

    fn agg_call(&mut self) -> Result<Val, GnitzSqlError> {
        // The HAVING binder resolves every aggregate to a reduce-layout ColRef.
        Err(unsupported("aggregate call"))
    }

    fn case(&mut self, _branches: &[(BoundExpr, BoundExpr)], _else: Option<&BoundExpr>) -> Result<Val, GnitzSqlError> {
        Err(unsupported("CASE"))
    }

    fn in_list(&mut self, _inner: &BoundExpr, _items: &[BoundExpr]) -> Result<Val, GnitzSqlError> {
        Err(unsupported("IN"))
    }
}

/// Plan-time support probe: run the typed evaluator over an all-NULL row. The
/// walk visits every node (`binop`/`unop` recurse unconditionally, unsupported
/// composites reject without recursing), so `is_ok()` iff every node and every
/// referenced column type evaluates at runtime.
pub(crate) fn having_supported(e: &BoundExpr, partial_schema: &Schema) -> bool {
    let types: Vec<TypeCode> = partial_schema.columns.iter().map(|c| c.type_code).collect();
    let row = vec![Val::Null; types.len()];
    lower_bound_expr(
        e,
        &mut HavingEval {
            row: &row,
            types: &types,
        },
    )
    .is_ok()
}

/// Evaluate the bound HAVING over one combined group row. Infallible after
/// [`having_supported`] passed at plan time (the same backend walked the same
/// tree); a residual `Err` propagates as the routing bug it would be.
fn eval_having(e: &BoundExpr, row: &[Val], types: &[TypeCode]) -> Result<Val, GnitzSqlError> {
    lower_bound_expr(e, &mut HavingEval { row, types })
}

fn eval_3vl(op: BinOp, a: Val, b: Val) -> Val {
    let (ta, tb) = (tri(a), tri(b)); // Some(true/false) truth, None = unknown
    match op {
        BinOp::And => match (ta, tb) {
            (Some(false), _) | (_, Some(false)) => Val::Int(0),
            (Some(true), Some(true)) => Val::Int(1),
            _ => Val::Null,
        },
        BinOp::Or => match (ta, tb) {
            (Some(true), _) | (_, Some(true)) => Val::Int(1),
            (Some(false), Some(false)) => Val::Int(0),
            _ => Val::Null,
        },
        _ => unreachable!("eval_3vl only handles AND/OR"),
    }
}

fn tri(v: Val) -> Option<bool> {
    match v {
        Val::Null => None,
        other => Some(truthy(other)),
    }
}

fn eval_cmp(op: BinOp, a: Val, b: Val) -> Val {
    // Either float → native IEEE f64 compare (partial_cmp is None only for NaN,
    // where only Ne holds — matching the engine `FCmp`); else signed i64.
    let ord = if matches!(a, Val::Float(_)) || matches!(b, Val::Float(_)) {
        as_f64(a).partial_cmp(&as_f64(b))
    } else {
        Some(as_i64(a).cmp(&as_i64(b)))
    };
    let res = match op {
        BinOp::Eq => ord == Some(Ordering::Equal),
        BinOp::Ne => ord != Some(Ordering::Equal),
        BinOp::Lt => ord == Some(Ordering::Less),
        BinOp::Le => matches!(ord, Some(Ordering::Less | Ordering::Equal)),
        BinOp::Gt => ord == Some(Ordering::Greater),
        BinOp::Ge => matches!(ord, Some(Ordering::Greater | Ordering::Equal)),
        _ => unreachable!("eval_cmp only handles comparisons"),
    };
    Val::Int(res as i64)
}

fn eval_arith(op: BinOp, a: Val, b: Val) -> Val {
    if matches!(a, Val::Float(_)) || matches!(b, Val::Float(_)) {
        let (x, y) = (as_f64(a), as_f64(b));
        match op {
            BinOp::Add => Val::Float(x + y),
            BinOp::Sub => Val::Float(x - y),
            BinOp::Mul => Val::Float(x * y),
            // Float divide by zero → NULL (engine `div_like`), not ±inf.
            BinOp::Div => {
                if y != 0.0 {
                    Val::Float(x / y)
                } else {
                    Val::Null
                }
            }
            // Rejected statically in `binop` (the engine VM has no float Mod).
            BinOp::Mod => unreachable!("float modulo is rejected at plan time"),
            _ => unreachable!(),
        }
    } else {
        let (x, y) = (as_i64(a), as_i64(b));
        match op {
            BinOp::Add => Val::Int(x.wrapping_add(y)),
            BinOp::Sub => Val::Int(x.wrapping_sub(y)),
            BinOp::Mul => Val::Int(x.wrapping_mul(y)),
            // Integer divide/mod by zero → NULL (engine semantics).
            BinOp::Div => {
                if y != 0 {
                    Val::Int(x.wrapping_div(y))
                } else {
                    Val::Null
                }
            }
            BinOp::Mod => {
                if y != 0 {
                    Val::Int(x.wrapping_rem(y))
                } else {
                    Val::Null
                }
            }
            _ => unreachable!(),
        }
    }
}

fn as_f64(v: Val) -> f64 {
    match v {
        Val::Int(i) => i as f64,
        Val::Float(f) => f,
        Val::Null => unreachable!("NULL is handled before arithmetic"),
    }
}

fn as_i64(v: Val) -> i64 {
    match v {
        Val::Int(i) => i,
        // The integer branch of eval_cmp/eval_arith is only taken when neither
        // operand is Float — a silent float truncation here would be a bug.
        Val::Float(_) => unreachable!("integer arithmetic never receives a float"),
        Val::Null => unreachable!("NULL is handled before arithmetic"),
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

fn emit_row(
    spec: &AggFinish,
    partial: &ZSetBatch,
    item_srcs: &[ItemSrc],
    out: &mut ZSetBatch,
    out_pk: u128,
    rep: Option<usize>,
    accs: &[ColAcc],
) {
    let out_schema = spec.out_schema;
    out.pks.push_u128(out_pk);
    out.weights.push(1);
    let mut null_word: u64 = 0;
    for (si, src) in item_srcs.iter().enumerate() {
        let out_ci = 1 + si; // col 0 is the hidden PK
        let out_pi = out_schema.payload_idx(out_ci);
        let out_tc = out_schema.columns[out_ci].type_code;
        match src {
            ItemSrc::Group { partial_ci } => {
                let rep = rep.expect("a grouped result always has a representative row");
                if partial.is_null(spec.partial_schema, rep, *partial_ci) {
                    null_word_set(&mut null_word, out_pi, true);
                    out.columns[out_ci].push_null(out_tc);
                } else {
                    partial.columns[*partial_ci].push_row_from(rep, out_tc.wire_stride(), &mut out.columns[out_ci]);
                }
            }
            ItemSrc::Agg { agg_idx } => match finish_agg(spec, accs, *agg_idx) {
                Val::Null => {
                    null_word_set(&mut null_word, out_pi, true);
                    out.columns[out_ci].push_null(out_tc);
                }
                Val::Int(bits) => push_fixed_bits(&mut out.columns[out_ci], bits as u64, out_tc.wire_stride()),
                Val::Float(f) => push_fixed_bits(&mut out.columns[out_ci], f.to_bits(), out_tc.wire_stride()),
            },
        }
    }
    out.nulls.push(null_word);
}

/// Push the low `stride` bytes of `bits` into a Fixed output column.
fn push_fixed_bits(col: &mut ColData, bits: u64, stride: usize) {
    match col {
        ColData::Fixed(buf) => buf.extend_from_slice(&bits.to_le_bytes()[..stride]),
        _ => unreachable!("ad-hoc aggregate output column is not Fixed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::col_def;

    fn schema() -> Schema {
        Schema {
            columns: vec![
                col_def("_group_pk", TypeCode::U128, false),
                col_def("g", TypeCode::I64, false),
                col_def("agg0", TypeCode::I64, true),
            ],
            pk_cols: vec![0],
        }
    }

    /// The probe must visit BOTH operands of a logical connective: an
    /// unsupported node in the right branch of an `OR` whose left is a
    /// satisfied literal has to reject at plan time. A short-circuiting probe
    /// would pass it and the unsupported node would surface as a hard error
    /// mid-query — this pins the non-short-circuiting walk `having_supported`
    /// relies on.
    #[test]
    fn probe_visits_both_or_branches() {
        let bad_rhs = BoundExpr::BinOp(
            Box::new(BoundExpr::LitInt(1)),
            BinOp::Or,
            Box::new(BoundExpr::LitStr("x".to_string())),
        );
        assert!(!having_supported(&bad_rhs, &schema()));
        let ok = BoundExpr::BinOp(
            Box::new(BoundExpr::LitInt(1)),
            BinOp::Or,
            Box::new(BoundExpr::ColRef(2)),
        );
        assert!(having_supported(&ok, &schema()));
    }

    /// Float modulo has no engine VM instruction, so a view HAVING rejects it —
    /// the ad-hoc probe must too (ad-hoc HAVING ⊆ view HAVING), even through
    /// the all-NULL probe row (the guard is static, pre-NULL-short-circuit).
    #[test]
    fn probe_rejects_float_modulo() {
        let float_mod = BoundExpr::BinOp(
            Box::new(BoundExpr::LitFloat(2.5)),
            BinOp::Mod,
            Box::new(BoundExpr::ColRef(2)),
        );
        assert!(!having_supported(&float_mod, &schema()));
        // Integer modulo stays supported.
        let int_mod = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(2)),
            BinOp::Mod,
            Box::new(BoundExpr::LitInt(2)),
        );
        assert!(having_supported(&int_mod, &schema()));
    }
}
