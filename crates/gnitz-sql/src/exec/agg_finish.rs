//! Client-side finishing for an ad-hoc aggregate / DISTINCT SELECT.
//!
//! The workers return one concatenated `ZSetBatch` of per-worker partial reduce
//! rows (pure append, never consolidated). This module combines them by
//! group-column **value** (weight-aware / Z-set-exact), synthesizes the global
//! ground row, and so materializes the client's own reduce output; it then runs
//! the two operators a grouped view runs over that output — the HAVING filter
//! and the finalize map — through the shared expression evaluator, and emits a
//! batch carrying a hidden synthetic PK (stripped at presentation). The caller
//! then applies the shared ORDER BY / OFFSET / LIMIT sink.
//!
//! **The aggregate finishing is not reimplemented here.** AVG's divide and a
//! nullable SUM's null gate are in the finalize expressions the planner built
//! (`agg::finalize_agg_bexpr`) — the same composites the view path's post-reduce
//! MAP evaluates — so this module has no per-aggregate shape switch that could
//! drift from the view's. What is genuinely client-side is the cross-worker
//! combine below: one accumulator per physical spec, merged by the shared
//! partial-merge rule.
//!
//! Partial reply layout (the batch this module consumes) is the shared
//! SyntheticFold layout (`crate::agg::synthetic_fold_cols`):
//! `[_group_pk U128 (hidden PK) | group cols (source types) | one partial
//! column per physical agg spec]`. Grouping is by *value*, so the key is never
//! consulted to form a group and no collision handling is needed — but it is
//! not discarded either: it is carried through to the output PK, which is what
//! makes a tied ORDER BY / LIMIT a function of the data alone.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use gnitz_core::{push_zero_cell, ColumnDef, PkColumn, Schema, TypeCode, ZSetBatch, ZSetBatchView};
use gnitz_expr::{ColumnLocator, Evaluator, ExprResults, SchemaFacts};
use gnitz_wire::{AggFunc as WireAggFunc, ComputeMap};

use crate::agg::AggSpec;
use crate::error::GnitzSqlError;

/// The fold sink's whole shape, built once at plan time
/// (`dml::select::build_fold_shape`) and read by the dispatch, by EXPLAIN, and by
/// [`agg_finish`] — so what was planned and what is finished cannot describe
/// different folds.
pub(crate) struct FoldShape {
    /// The reduce input: the pre-map's output schema when there is one, else the
    /// source's. The schema `group_positions` and `agg_specs[].col` index — what
    /// EXPLAIN names them against.
    pub(crate) reduce_schema: Arc<Schema>,
    /// The group columns as **reduce-input** positions. Shipped as
    /// `AggReadSpec.group_cols` and, being the partial layout's leading payload
    /// slots, also what the combine groups by.
    pub(crate) group_positions: Vec<usize>,
    /// One accumulator per physical reduce spec, in the partial layout's agg
    /// order.
    pub(crate) agg_specs: Vec<AggSpec>,
    /// The fold sink's pre-map, `None` when the reduce reads source columns
    /// directly.
    pub(crate) pre: Option<ComputeMap>,
    /// The per-worker SyntheticFold reduce-output layout: what the partial reply
    /// decodes against, and what `having` and every [`FinalizeItem`] resolve
    /// against.
    pub(crate) partial_schema: Arc<Schema>,
    /// The final output schema (`build_agg_out_schema`, computed at plan time
    /// so a bad shape rejects before the fold is dispatched).
    pub(crate) out_schema: Schema,
    /// The HAVING predicate compiled against `partial_schema` — `None` when the
    /// query has no HAVING or it folded to a statically-true constant.
    pub(crate) having: Option<Evaluator>,
    /// The finalize projection in SELECT order, one item per visible output
    /// column.
    pub(crate) finalize: Vec<FinalizeItem>,
}

impl FoldShape {
    /// `true` iff the group set is empty (an ungrouped / global scalar
    /// aggregate): it emits one row even over an empty source, so the client
    /// synthesizes a ground row when no worker contributed one. Derived, so no
    /// constructor can state the impossible `{empty groups, not global}`.
    pub(crate) fn global_ground(&self) -> bool {
        self.group_positions.is_empty()
    }
}

/// One finalize item: where an output column's value comes from.
///
/// The split is the projection's own (`ProjItem`'s pass-through/computed
/// classification, which the view path's finalize map makes too): a bare
/// reference to a reduce-output column of the same type is a byte move, and
/// everything else — AVG's divide, a nullable SUM's null gate, an expression
/// over group columns and aggregates — is one evaluated register.
pub(crate) enum FinalizeItem {
    /// Column `partial_ci` of the group batch, moved verbatim.
    PassThrough { partial_ci: usize },
    /// An expression over the group batch, of whichever class it resolved to.
    /// Boxed because a projection is mostly pass-throughs, which inline would
    /// each pay a computed item's width.
    Computed { ev: Box<Evaluator> },
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
    /// renders NULL.
    ///
    /// The accumulator holds the true sum mod 2^64; nothing here reads it as a
    /// quantity. Its declared column type — `agg_output_type(Sum, U64)` is U64 —
    /// travels on the partial schema, so the finalize expression that *does* read
    /// it (AVG's divide) dispatches on that type and a sum past 2^63 stays
    /// unsigned.
    IntSum { bits: i64, seen: bool },
    /// Float SUM — Σ (w as f64)·partial. IEEE-754 addition is non-associative, so
    /// the value follows the summation order: the order each worker's access path
    /// visited rows in, and — only here — the reply order the partials arrive in.
    /// A float-SUM *view* keeps the single-worker funnel (its two-phase combine
    /// excludes float SUM for exactly this reason), so the worker-count term is
    /// the ad-hoc path's alone; the access-path term is common to both, an
    /// index-bounded walk sorting PKs a chunk at a time. The whole point of the
    /// fold is not shipping rows, so the per-worker split is inherent here. Use an
    /// integer type where exactness matters.
    FloatSum { val: f64, seen: bool },
    /// MIN / MAX — the row of `partial` holding the winner, ordered by
    /// `cmp_col_window`. A row index, not a copy: `partial` outlives the fold,
    /// so a variable-width STRING/BLOB extreme needs no per-group buffer.
    Extreme { best: Option<usize>, is_max: bool },
}

impl ColAcc {
    fn new(spec: &AggSpec) -> ColAcc {
        match spec.op.merge_func() {
            // A count is a sum that starts out non-NULL: its partials are never
            // null, and an uncontributed group renders 0 rather than NULL. The
            // raw counts land here too — `merge_func` maps them to `SumZero`, so
            // they would take this arm anyway.
            WireAggFunc::SumZero | WireAggFunc::Count | WireAggFunc::CountNonNull => {
                ColAcc::IntSum { bits: 0, seen: true }
            }
            WireAggFunc::Sum if spec.out_type.is_float() => ColAcc::FloatSum { val: 0.0, seen: false },
            WireAggFunc::Sum => ColAcc::IntSum { bits: 0, seen: false },
            WireAggFunc::Min => ColAcc::Extreme { best: None, is_max: false },
            WireAggFunc::Max => ColAcc::Extreme { best: None, is_max: true },
        }
    }
}

/// Combine, finish, and project the concatenated worker partials into the final
/// result batch (with a hidden synthetic PK). The caller applies ORDER BY /
/// OFFSET / LIMIT afterwards.
pub(crate) fn agg_finish(spec: &FoldShape, partial: &ZSetBatch) -> ZSetBatch {
    let n_group = spec.group_positions.len();
    let n_aggs = spec.agg_specs.len();
    // Partial columns: group cols at ci 1..1+n_group, agg partials at
    // 1+n_group+k (ci 0 is the hidden _group_pk PK).

    // 1. Combine by group-column value image (weight-aware). `reps[g]` is the
    //    representative partial row of group ordinal `g` (`None` = the
    //    synthesized global ground group, which has no group columns to read),
    //    `accs` the flat accumulator matrix `accs[g*n_aggs..(g+1)*n_aggs]` —
    //    one growth stream, mirroring the engine fold. The key is built into
    //    one reused scratch buffer; an owned copy is allocated only for a
    //    genuinely new group (each group recurs on up to W workers).
    // One locator per partial column, resolved here rather than per (row ×
    // column): it carries both the NULL bit's position and the Fixed stride.
    let locs: Vec<ColumnLocator> = (0..1 + n_group + n_aggs)
        .map(|ci| SchemaFacts::locate(spec.partial_schema.as_ref(), ci))
        .collect();
    let mut by_key: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut reps: Vec<Option<usize>> = Vec::new();
    let mut accs: Vec<ColAcc> = Vec::new();
    let mut key_scratch: Vec<u8> = Vec::new();
    for row in 0..partial.len() {
        let w = partial.weights[row];
        if w <= 0 {
            continue;
        }
        group_key(partial, &locs[1..1 + n_group], row, &mut key_scratch);
        let g = match by_key.get(key_scratch.as_slice()) {
            Some(&g) => {
                // Equal group values imply an equal engine `_group_pk`. Checked
                // here because this is the only point where a non-representative
                // row of the group is still in scope to compare against; every
                // `reps` entry is `Some` until step 2 pushes the ground group.
                debug_assert_eq!(
                    partial.pks.get_bytes(row),
                    partial.pks.get_bytes(reps[g].unwrap()),
                    "partial rows of one group disagree on _group_pk"
                );
                g
            }
            None => {
                let g = reps.len();
                by_key.insert(key_scratch.clone(), g);
                reps.push(Some(row));
                accs.extend(spec.agg_specs.iter().map(ColAcc::new));
                g
            }
        };
        for (k, (acc, loc)) in accs[g * n_aggs..(g + 1) * n_aggs]
            .iter_mut()
            .zip(&locs[1 + n_group..])
            .enumerate()
        {
            combine(acc, partial, loc, 1 + n_group + k, row, w);
        }
    }

    // 2. Global ground row: a global aggregate with no surviving partial emits one
    //    synthetic row (COUNT 0 / others NULL), which HAVING still filters.
    if spec.global_ground() && reps.is_empty() {
        reps.push(None);
        accs.extend(spec.agg_specs.iter().map(ColAcc::new));
    }

    // 3. The client's reduce output, one row per group. Materialized
    //    unconditionally: it is what HAVING filters and what the finalize map
    //    projects, which is the same two-operator tail a grouped view runs, and
    //    it is bounded by the result the caller is about to sort and ship.
    let groups = fill_group_batch(spec, partial, &reps, &accs);

    // 4. HAVING, then 5. finalize — both over **one** view: building one
    //    allocates a region list.
    let view = ZSetBatchView::new(&groups, &spec.partial_schema);

    // One batch, one drive — so the truth rule is the engine filter's own
    // (`bool_bits & !null_bits`). Without a HAVING every group is one range.
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    match spec.having.as_ref() {
        None => ranges.push((0, groups.len())),
        Some(ev) => ev.filter_ranges(&view, &mut ranges),
    }

    // Zero surviving groups needs no guard: the range list is then empty and the
    // row loop never runs.
    project_groups(spec, &view, &ranges)
}

/// One row per group in the partial-reply layout — the client's reduce output:
/// `[_group_pk | group cols copied from the representative partial row | raw
/// accumulator values]`. `reps[g] == None` is the synthesized global ground
/// group, which by construction has no group columns.
///
/// Filled a column at a time, so the destination column is resolved once per
/// column rather than once per cell.
fn fill_group_batch(spec: &FoldShape, partial: &ZSetBatch, reps: &[Option<usize>], accs: &[ColAcc]) -> ZSetBatch {
    let schema = spec.partial_schema.as_ref();
    let n_group = spec.group_positions.len();
    let n_aggs = spec.agg_specs.len();
    let n = reps.len();

    let mut dst = ZSetBatch::with_capacity(schema, n);
    // Pushed row by row, so the column keeps the stride `empty_for_schema`
    // derived from the schema.
    for &rep in reps {
        push_group_key(&mut dst.pks, schema, partial, rep);
    }
    dst.weights.resize(n, 1);
    // One word per row, one bit per payload slot: materialized at its final
    // length up front so each column pass can OR in its own bit `pi`.
    dst.nulls.resize(n, 0);

    // `payload_columns` yields the payload slot as its enumeration ordinal, so
    // `pi` is also the column's position in this layout: slots `0..n_group` are
    // the group columns, the rest the agg partials.
    for (pi, ci, col) in schema.payload_columns() {
        let tc = col.type_code;
        if pi < n_group {
            // A group column keeps its source type, so its cell is copied whole.
            let loc = SchemaFacts::locate(schema, ci);
            for (g, &rep) in reps.iter().enumerate() {
                let rep = rep.expect("a grouped result always has a representative row");
                if loc.is_null_word(partial.nulls[rep]) {
                    push_null_cell(&mut dst.columns[ci], &mut dst.nulls[g], tc, pi);
                } else {
                    dst.push_cell_from(ci, partial, ci, tc, rep);
                }
            }
        } else {
            // `accs` is group-major (the combine loop writes a whole group row at
            // a time), so this column's cells are `n_aggs` apart.
            let k = pi - n_group;
            for g in 0..n {
                push_acc_cell(&mut dst, ci, pi, g, &accs[g * n_aggs + k], partial, tc);
            }
        }
    }

    // The one rule `regions` does not already assert on this batch two lines
    // later: a set null bit under a NOT NULL group column would leave
    // the resolved program's `no_nulls` on, and the evaluator would read
    // `push_null`'s zero bytes as a real `0`. Unconditional — release is where a
    // stale bit becomes a silently wrong answer rather than a panic.
    dst.validate(schema)
        .expect("the group batch must satisfy the partial schema");
    dst
}

/// Append a NULL cell to `col` and record it at payload slot `pi` in `word` —
/// the one place the null bitmap and the pushed filler bytes are kept in step.
fn push_null_cell(col: &mut Vec<u8>, word: &mut u64, tc: TypeCode, pi: usize) {
    gnitz_wire::null_word_set(word, pi, true);
    push_zero_cell(col, tc);
}

// ---------------------------------------------------------------------------
// Combine
// ---------------------------------------------------------------------------

/// Build one flattened group-identity key for a partial row into `key`
/// (cleared first): per group column a null marker byte, then the
/// length-prefixed value image. Collision-free (the length prefix keeps
/// adjacent columns from aliasing) and allocation-free per row.
///
/// `cols` is the group columns' locators in layout order, so `cols[g]` addresses
/// column `1 + g`; a `Fixed` cell's stride is that locator's own size.
fn group_key(partial: &ZSetBatch, cols: &[ColumnLocator], row: usize, key: &mut Vec<u8>) {
    key.clear();
    let null_word = partial.nulls[row];
    for (g, loc) in cols.iter().enumerate() {
        if loc.is_null_word(null_word) {
            key.push(0);
            continue;
        }
        key.push(1);
        let mut put = |b: &[u8]| {
            key.extend_from_slice(&(b.len() as u32).to_le_bytes());
            key.extend_from_slice(b);
        };
        let s = loc.size();
        let cell = &partial.columns[1 + g][row * s..(row + 1) * s];
        if gnitz_wire::is_german_string(loc.type_code()) {
            put(gnitz_wire::german_string_content(cell, &partial.blob));
        } else {
            put(cell);
        }
    }
}

fn combine(acc: &mut ColAcc, partial: &ZSetBatch, loc: &ColumnLocator, ci: usize, row: usize, w: i64) {
    if loc.is_null_word(partial.nulls[row]) {
        return; // NULL partials skip (COUNT/COUNT_NON_NULL partials are never null)
    }
    match acc {
        // COUNT and SUM partials are 8-byte cells (I64, or U64 whose bit
        // pattern is the true sum mod 2^64 — the same i64 accumulator the
        // engine folds).
        ColAcc::IntSum { bits, seen } => {
            *bits = bits.wrapping_add(w.wrapping_mul(i64::from_le_bytes(read_le8(partial, ci, row))));
            *seen = true;
        }
        ColAcc::FloatSum { val, seen } => {
            *val += (w as f64) * f64::from_le_bytes(read_le8(partial, ci, row));
            *seen = true;
        }
        ColAcc::Extreme { best, is_max } => {
            let wanted = if *is_max { Ordering::Greater } else { Ordering::Less };
            let cell = |row| fixed_slice(partial, ci, row, loc.size());
            // Both operands live in the same batch, so one blob heap backs both
            // sides of a German-string comparison.
            let replace = best.is_none_or(|b| {
                gnitz_wire::cmp_col_window(cell(row), &partial.blob, cell(b), &partial.blob, loc.type_code()) == wanted
            });
            if replace {
                *best = Some(row);
            }
        }
    }
}

fn fixed_slice(partial: &ZSetBatch, ci: usize, row: usize, stride: usize) -> &[u8] {
    partial
        .cell(ci, row, stride)
        .expect("ad-hoc agg partial column is fixed-width and in range")
}

/// One 8-byte partial-aggregate cell; the caller picks how to read it.
fn read_le8(partial: &ZSetBatch, ci: usize, row: usize) -> [u8; 8] {
    fixed_slice(partial, ci, row, 8).try_into().unwrap()
}

/// Append group `g`'s cell for `acc` to column `ci`. An accumulator nothing
/// contributed to is the global ground row, or an all-NULL group.
fn push_acc_cell(dst: &mut ZSetBatch, ci: usize, pi: usize, g: usize, acc: &ColAcc, partial: &ZSetBatch, tc: TypeCode) {
    match *acc {
        ColAcc::IntSum { seen: false, .. }
        | ColAcc::FloatSum { seen: false, .. }
        | ColAcc::Extreme { best: None, .. } => push_null_cell(&mut dst.columns[ci], &mut dst.nulls[g], tc, pi),
        ColAcc::IntSum { bits, .. } => push_fixed_bits(&mut dst.columns[ci], bits as u64, tc.wire_stride()),
        ColAcc::FloatSum { val, .. } => push_fixed_bits(&mut dst.columns[ci], val.to_bits(), tc.wire_stride()),
        // The partial column and this one are the same column of the same
        // schema, so the winner is copied exactly as a group column is — blob
        // relocation included.
        ColAcc::Extreme { best: Some(row), .. } => dst.push_cell_from(ci, partial, ci, tc, row),
    }
}

// ---------------------------------------------------------------------------
// Output schema + finalize
// ---------------------------------------------------------------------------

/// The final output schema of an ad-hoc aggregate / DISTINCT SELECT: a hidden
/// synthetic PK (like `_distinct_pk` / `_group_pk`, stripped at presentation)
/// followed by the SELECT-order visible columns the binder named and typed.
///
/// The duplicate-name guard is `build_fold_shape`'s: only there is the projection
/// in hand, and a wildcard that names nothing of its own must stay exempt.
/// Returns the schema and the index the projection starts at — the hidden key
/// slots prepended ahead of it, which a caller addressing an item by its
/// position in `out_cols` shifts past.
pub(crate) fn build_agg_out_schema(out_cols: Vec<ColumnDef>) -> Result<(Schema, usize), GnitzSqlError> {
    let mut cols = vec![ColumnDef::new("_agg_pk", TypeCode::U128, false).hidden()];
    let base = cols.len();
    cols.extend(out_cols);
    Schema::from_parts(cols, vec![0])
        .map(|s| (s, base))
        .map_err(|e| GnitzSqlError::Unsupported(format!("ad-hoc aggregate output schema is invalid: {e}")))
}

/// Append group `rep`'s key — the one answer in this module to "what is a
/// group's key". It is the engine's `_group_pk`, a pure function of the group's
/// column values, and must never be invented from emission order: `_agg_pk` is
/// the *leading* key of the ordering sink's identity tiebreak, so an ordinal
/// would decide every tie by worker count and reply arrival order, and the
/// payload keys behind it would never be reached.
///
/// The copy is a byte move, not a re-encode — the partial's `_group_pk` and both
/// destination keys are the same U128 stride.
fn push_group_key(dst: &mut PkColumn, schema: &Schema, partial: &ZSetBatch, rep: Option<usize>) {
    match rep {
        Some(row) => dst.push_from(&partial.pks, row),
        // The synthesized global ground row has no partial to copy from, so it
        // takes V₀ directly — the same key the engine's own ground row carries
        // when a worker did contribute one.
        None => dst.push_u128(schema, gnitz_wire::global_group_key()),
    }
}

/// One finalize item resolved against the group batch: its output coordinates,
/// and — for the computed arms — every group's value, driven before the row
/// loop, where a per-row drive would pay `eval_batch`'s prologue per group.
/// The row loop is infallible, so nothing observable moves with the drive.
struct Finalized {
    /// Output column index, dense payload slot, and declared type.
    ci: usize,
    pi: usize,
    tc: TypeCode,
    values: FinalValues,
}

enum FinalValues {
    /// Column `partial_ci` of the group batch, with the locator its NULL bit is
    /// read through.
    PassThrough {
        partial_ci: usize,
        loc: ColumnLocator,
    },
    Computed(ExprResults),
}

/// Project the surviving groups through the finalize items — the client's
/// post-reduce MAP. Each group keeps the `_group_pk` it was combined under.
///
/// `view` is the caller's one view of the group batch; the batch itself is
/// reached back through it, so no second region list is built here.
fn project_groups(spec: &FoldShape, view: &ZSetBatchView<'_>, ranges: &[(usize, usize)]) -> ZSetBatch {
    let groups = view.batch();
    let n_out: usize = ranges.iter().map(|(s, e)| e - s).sum();
    let mut out = ZSetBatch::with_capacity(&spec.out_schema, n_out);
    // Output column 0 is the hidden PK, so finalize item `si` lands at `si + 1`:
    // its dense payload slot and type are resolved once here, not once per group.
    let items: Vec<Finalized> = spec
        .finalize
        .iter()
        .enumerate()
        .map(|(si, item)| Finalized {
            ci: 1 + si,
            pi: spec.out_schema.payload_idx(1 + si),
            tc: spec.out_schema.columns[1 + si].type_code,
            values: match item {
                FinalizeItem::PassThrough { partial_ci } => FinalValues::PassThrough {
                    partial_ci: *partial_ci,
                    loc: SchemaFacts::locate(spec.partial_schema.as_ref(), *partial_ci),
                },
                FinalizeItem::Computed { ev } => FinalValues::Computed(ev.eval_all(view)),
            },
        })
        .collect();

    for &(start, end) in ranges {
        for g in start..end {
            out.pks.push_from(&groups.pks, g);
            out.weights.push(1);
            let mut null_word: u64 = 0;
            for item in &items {
                match &item.values {
                    FinalValues::PassThrough { partial_ci, loc } => {
                        if loc.is_null_word(groups.nulls[g]) {
                            push_null_cell(&mut out.columns[item.ci], &mut null_word, item.tc, item.pi);
                        } else {
                            out.push_cell_from(item.ci, groups, *partial_ci, item.tc, g);
                        }
                    }
                    FinalValues::Computed(ExprResults::Scalar(vals)) => match vals[g] {
                        None => push_null_cell(&mut out.columns[item.ci], &mut null_word, item.tc, item.pi),
                        Some(v) => push_fixed_bits(&mut out.columns[item.ci], v as u64, item.tc.wire_stride()),
                    },
                    FinalValues::Computed(ExprResults::Str { bytes, spans }) => match spans[g] {
                        None => push_null_cell(&mut out.columns[item.ci], &mut null_word, item.tc, item.pi),
                        Some((o, l)) => {
                            let cell = gnitz_wire::encode_german_string(&bytes[o..o + l], &mut out.blob);
                            out.columns[item.ci].extend_from_slice(&cell);
                        }
                    },
                }
            }
            out.nulls.push(null_word);
        }
    }
    out
}

/// Push the low `stride` bytes of `bits` into a Fixed output column — the
/// results that *are* a register image: an accumulated SUM / COUNT
/// (`agg_output_type` keeps both ≤ 8 bytes) and the evaluator's own register.
fn push_fixed_bits(col: &mut Vec<u8>, bits: u64, stride: usize) {
    col.extend_from_slice(&bits.to_le_bytes()[..stride]);
}

#[cfg(test)]
#[path = "tests/agg_finish.rs"]
mod tests;
