//! Equi-join and range/band-join view compilation, plus the shared join-output
//! projection helper. Both builders reindex each side onto the join key, run the
//! symmetric 2-term DBSP join against the other side's trace, normalize the two
//! terms onto `[A cols, B cols]`, and (for outer joins) attach the null-fill.

use crate::ast_util::{
    collect_column_refs, collect_projection_column_refs, extract_table_name_and_alias, flatten_conjuncts,
    is_wildcard_projection,
};
use crate::bind::{resolve_qualified_column, resolve_unqualified_column, AliasMap, Binder, ResolvedRelation};
use crate::error::GnitzSqlError;
use crate::plan::validate::{
    reject_column_overflow, reject_duplicate_column_names, reject_unhonored_select_clauses, HonoredClauses,
};
use crate::plan::view::predicates::{
    build_reindex_program, build_reindex_program_keep, build_residual_filter_prog, converse_rel,
    extract_join_predicates, multi_null_filter_prog, null_gate, pure_range_m_output_cols, schema_type_codes,
    RangeConjunct,
};
use crate::plan::view::{EmitPieces, ViewChain};
use gnitz_core::{
    CircuitBuilder, ColumnDef, ExprBuilder, FixedInt, GnitzClient, RangeRel, ReduceOutKey, Schema, TypeCode,
};
use gnitz_wire::{AGG_MAX, AGG_MIN};
use sqlparser::ast::{Expr, Ident, JoinConstraint, JoinOperator, SelectItem};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Which side(s) of a join survive unmatched. Threaded through both builders in
/// place of the old `is_left_join: bool`. The two predicates drive both null-fill
/// emission and output-column nullability:
///   - `preserves_left()`  (`Left | Full`): a left row survives unmatched ⇒ the
///     right columns can be NULL, and the null-fill `ν_A = positive_part(A − π_A(inner))`
///     is emitted.
///   - `preserves_right()` (`Right | Full`): a right row survives unmatched ⇒ the
///     left columns can be NULL, and the mirror `ν_B = positive_part(B − π_B(inner))`
///     is emitted.
///
/// `Full` satisfies both. `Inner` neither.
#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    fn preserves_left(self) -> bool {
        matches!(self, JoinType::Left | JoinType::Full)
    }

    fn preserves_right(self) -> bool {
        matches!(self, JoinType::Right | JoinType::Full)
    }
}

/// The combined `[A cols, B cols]` output payload of one join step, with the
/// outer-join nullability adjustment applied: a row preserved on one side carries
/// NULLs on the OTHER side's columns, so a left column is nullable iff the RIGHT
/// side is preserved (Right|Full) and a right column iff the LEFT side is
/// (Left|Full). A client decoding a NULL in a NOT NULL column would panic, so
/// this rule has exactly this one home — shared by the equi (`emit_join`) and
/// range (`emit_range_join`) builders.
fn combined_payload_coldefs(left_schema: &Schema, right_schema: &Schema, join_type: JoinType) -> Vec<ColumnDef> {
    let mut cols = Vec::with_capacity(left_schema.columns.len() + right_schema.columns.len());
    for col in &left_schema.columns {
        let mut c = col.clone();
        if join_type.preserves_right() {
            c.is_nullable = true;
        }
        cols.push(c);
    }
    for col in &right_schema.columns {
        let mut c = col.clone();
        if join_type.preserves_left() {
            c.is_nullable = true;
        }
        cols.push(c);
    }
    cols
}

/// Per-side carried reindex target type for each join key slot: `T_i` only when
/// the side's own self-derived reindex output type differs from `T_i` (a
/// cross-width/cross-sign promotion on that side), else `0` (self-derive). The
/// `0` keeps same-type / U128-vs-UUID / string circuits byte-identical to the
/// pre-promotion serialization. The encode rule lives in `carried_reindex_tc`,
/// the round-trip inverse of the engine's `resolve_reindex_type`. Shared by the
/// equi builder (`slot_tcs = target_tcs`), the range builder
/// (`slot_tcs = all_tcs`, the eq prefix plus the range slot), and the
/// EXISTS/IN semi-join builder.
pub(crate) fn side_target_tcs(cols: &[usize], schema: &Schema, slot_tcs: &[TypeCode]) -> Vec<u8> {
    cols.iter()
        .zip(slot_tcs)
        .map(|(&c, &t)| schema.columns[c].type_code.carried_reindex_tc(t))
        .collect()
}

/// Normalize the two per-term join outputs onto the canonical `[A cols, B cols]`
/// payload layout and union them. Term AB is already canonical
/// (`[_join_pk × k, A, B]`); term BA is `[_join_pk × k, B, A]` and is reordered.
/// Shared verbatim by the equi (`k = eq slots`) and range (`k = n_eq + 1`)
/// builders, and by the band EXISTS/IN semi-join circuit.
pub(crate) fn normalize_to_ab(
    cb: &mut CircuitBuilder,
    join_ab: gnitz_core::NodeId,
    join_ba: gnitz_core::NodeId,
    k: usize,
    left_n: usize,
    right_n: usize,
) -> gnitz_core::NodeId {
    // Path AB projection: identity (already canonical: [PK cols, A_cols, B_cols])
    let proj_ab: Vec<usize> = (k..k + left_n + right_n).collect();
    let proj_ab_node = cb.map(join_ab, &proj_ab);

    // Path BA projection: reorder [A_cols, B_cols]
    let mut proj_ba: Vec<usize> = Vec::new();
    for i in 0..left_n {
        proj_ba.push(k + right_n + i);
    }
    for i in 0..right_n {
        proj_ba.push(k + i);
    }
    let proj_ba_node = cb.map(join_ba, &proj_ba);

    cb.union(proj_ab_node, proj_ba_node)
}

/// One side of the symmetric equi-join term emission: its (unfiltered) input
/// node, the join key columns with their per-slot carried target types, the
/// source schema, and the kept payload columns (`0..n` for the unpruned
/// layout — `build_reindex_program_keep(0..n)` is byte-identical to
/// `build_reindex_program`).
pub(crate) struct EquiSide<'a> {
    pub(crate) input: gnitz_core::NodeId,
    pub(crate) cols: &'a [usize],
    pub(crate) target_tcs: &'a [u8],
    pub(crate) schema: &'a Schema,
    pub(crate) keep: &'a [usize],
}

/// The nodes [`emit_equi_join_terms`] emits, plus each side's key-nullability
/// fact (from the shared `null_gate`, so a caller's `a_all`/`b_all` reuse
/// decision cannot drift from the gate that was actually emitted).
pub(crate) struct EquiTerms {
    pub(crate) reindex_a: gnitz_core::NodeId,
    pub(crate) reindex_b: gnitz_core::NodeId,
    pub(crate) a_nullable: bool,
    pub(crate) b_nullable: bool,
    pub(crate) join_ab: gnitz_core::NodeId,
    pub(crate) join_ba: gnitz_core::NodeId,
}

/// Emit the symmetric 2-term equi join over two NULL-gated, reindexed sides —
/// B-first interleaved gates/reindexes, then both traces, then
/// `join_ab`/`join_ba` — node-for-node the sequence `emit_join` and the equi
/// EXISTS builder always emitted. A NULL equi-join key must match nothing
/// (SQL 3VL: NULL = anything, including NULL = NULL, is unknown); `map_reindex`
/// would promote a NULL integer key to synthetic PK 0 and a NULL string to the
/// empty-content hash 0, colliding with a real 0/"" key and with every other
/// NULL — so `null_gate` drops NULL-keyed rows from the match on both sides
/// (and leaves a NOT NULL side untouched, zero overhead).
pub(crate) fn emit_equi_join_terms(
    cb: &mut CircuitBuilder,
    a: EquiSide<'_>,
    b: EquiSide<'_>,
) -> Result<EquiTerms, GnitzSqlError> {
    let (b_gated, b_nullable) = null_gate(cb, b.input, b.cols, b.schema)?;
    let reindex_b = cb.map_reindex(
        b_gated,
        b.cols,
        b.target_tcs,
        build_reindex_program_keep(b.schema, b.keep),
    );
    let (a_gated, a_nullable) = null_gate(cb, a.input, a.cols, a.schema)?;
    let reindex_a = cb.map_reindex(
        a_gated,
        a.cols,
        a.target_tcs,
        build_reindex_program_keep(a.schema, a.keep),
    );
    let trace_a = cb.integrate_trace(reindex_a);
    let trace_b = cb.integrate_trace(reindex_b);
    let join_ab = cb.join_with_trace_node(reindex_a, trace_b); // ΔA ⋈ z^{-1}(I(B))
    let join_ba = cb.join_with_trace_node(reindex_b, trace_a); // ΔB ⋈ z^{-1}(I(A))
    Ok(EquiTerms {
        reindex_a,
        reindex_b,
        a_nullable,
        b_nullable,
        join_ab,
        join_ba,
    })
}

/// The pure-data reindex-slot layout of a range/band join: per-side reindex
/// columns (eq pairs first, range slot last), the per-slot common types
/// (`all_tcs` and each side's carried target tcs), and the §3 term relations
/// (`rel_ab = converse(op)` for term AB, `rel_ba = op` for term BA). Knob-free —
/// no methods, no behavior flags; both range builders destructure it.
pub(crate) struct RangeSlots {
    pub(crate) left_reindex_cols: Vec<usize>,
    pub(crate) right_reindex_cols: Vec<usize>,
    pub(crate) all_tcs: Vec<TypeCode>,
    pub(crate) left_target_tcs: Vec<u8>,
    pub(crate) right_target_tcs: Vec<u8>,
    pub(crate) rel_ab: RangeRel,
    pub(crate) rel_ba: RangeRel,
}

pub(crate) fn range_slots(
    left_cols: &[usize],
    right_cols: &[usize],
    eq_tcs: &[TypeCode],
    range: &RangeConjunct,
    left_schema: &Schema,
    right_schema: &Schema,
) -> RangeSlots {
    let left_reindex_cols: Vec<usize> = left_cols
        .iter()
        .copied()
        .chain(std::iter::once(range.left_col))
        .collect();
    let right_reindex_cols: Vec<usize> = right_cols
        .iter()
        .copied()
        .chain(std::iter::once(range.right_col))
        .collect();
    let all_tcs: Vec<TypeCode> = eq_tcs.iter().copied().chain(std::iter::once(range.tc)).collect();
    let left_target_tcs = side_target_tcs(&left_reindex_cols, left_schema, &all_tcs);
    let right_target_tcs = side_target_tcs(&right_reindex_cols, right_schema, &all_tcs);
    RangeSlots {
        left_reindex_cols,
        right_reindex_cols,
        all_tcs,
        left_target_tcs,
        right_target_tcs,
        rel_ab: converse_rel(range.op),
        rel_ba: range.op,
    }
}

/// The A-first batched gates + reindexes both range builders open with: NULL
/// exclusion (SQL 3VL) over ALL key cols (eq + range) on each side, then both
/// reindexes onto `[eq slots…, range slot]`. Returns
/// `(reindex_a, reindex_b, left_key_nullable)` — the callers keep the
/// unfiltered inputs for the preserved side's null-fill, and reuse the
/// left-side nullability fact for the NULL-key branch. Partition filters,
/// traces, and join terms stay in the callers (they diverge per shape).
pub(crate) fn range_gate_reindex_prologue(
    cb: &mut CircuitBuilder,
    a_input: gnitz_core::NodeId,
    b_input: gnitz_core::NodeId,
    slots: &RangeSlots,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<(gnitz_core::NodeId, gnitz_core::NodeId, bool), GnitzSqlError> {
    let (a_gated, a_nullable) = null_gate(cb, a_input, &slots.left_reindex_cols, left_schema)?;
    let (b_gated, _) = null_gate(cb, b_input, &slots.right_reindex_cols, right_schema)?;
    let reindex_a = cb.map_reindex(
        a_gated,
        &slots.left_reindex_cols,
        &slots.left_target_tcs,
        build_reindex_program(left_schema),
    );
    let reindex_b = cb.map_reindex(
        b_gated,
        &slots.right_reindex_cols,
        &slots.right_target_tcs,
        build_reindex_program(right_schema),
    );
    Ok((reindex_a, reindex_b, a_nullable))
}

/// The resolved front-end of one 2-way join step: both sides resolved, the ON
/// clause classified into equality pairs + an optional range conjunct + the
/// residual, and the join kind decided. Produced per step by `plan_join_chain`,
/// consumed by the emit path. (The residual/projection still bind through the
/// `AliasMap` at emit; the per-side re-bind against hidden-view inputs lands
/// with the multi-anchor surface.)
struct LoweredJoin {
    left_tid: u64,
    right_tid: u64,
    left_schema: std::rc::Rc<Schema>,
    right_schema: std::rc::Rc<Schema>,
    alias_map: AliasMap,
    join_type: JoinType,
    left_join_cols: Vec<usize>,
    right_join_cols: Vec<usize>,
    target_tcs: Vec<TypeCode>,
    range_conjunct: Option<RangeConjunct>,
    residual: Vec<Expr>,
    /// Top-level WHERE conjuncts of an OUTER join, applied as a linear filter over
    /// the merged (post-null-fill) output — uniformly for equi, range, and band
    /// OUTER joins. Empty for INNER (folded into `residual`).
    where_filter: Vec<Expr>,
}

/// The ON expression and type of one join step. Each step supports
/// INNER / LEFT / RIGHT / FULL, left-deep in syntactic order (no reordering),
/// so `a LEFT JOIN b JOIN c` is `(a LEFT JOIN b) JOIN c` — each step's emit is the
/// standard 2-way emit, outer null-fill included.
///
/// sqlparser 0.56 spells the bare/`OUTER` forms as separate variants
/// (`Left`/`LeftOuter`, `Right`/`RightOuter`); FULL has only `FullOuter`.
fn join_on_and_type(join: &sqlparser::ast::Join) -> Result<(&Expr, JoinType), GnitzSqlError> {
    match &join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(e)) | JoinOperator::Join(JoinConstraint::On(e)) => {
            Ok((e, JoinType::Inner))
        }
        JoinOperator::LeftOuter(JoinConstraint::On(e)) | JoinOperator::Left(JoinConstraint::On(e)) => {
            Ok((e, JoinType::Left))
        }
        JoinOperator::RightOuter(JoinConstraint::On(e)) | JoinOperator::Right(JoinConstraint::On(e)) => {
            Ok((e, JoinType::Right))
        }
        JoinOperator::FullOuter(JoinConstraint::On(e)) => Ok((e, JoinType::Full)),
        _ => Err(GnitzSqlError::Unsupported(
            "CREATE VIEW JOIN: only INNER / LEFT / RIGHT / FULL JOIN ... ON supported".into(),
        )),
    }
}

/// Classify a top-level WHERE over a join step, mutating `residual` and returning
/// the post-join `where_filter` — the single home for this rule:
///  - INNER: `ON p WHERE q ≡ ON (p AND q)`, so fold q into the residual — the same
///    linear filter over the join output the ON residual uses.
///  - OUTER (equi, range, or band): a WHERE is a linear 3VL filter over the merged
///    *post-null-fill* output. Its 3VL is exactly right — a null-filled column failing
///    the predicate drops its row, `IS NULL` keeps unmatched rows — and it yields the
///    same result as pushing preserved-side conjuncts below the join. The ON residual
///    (a match condition) stays outer-rejected; WHERE is post-join, uniformly across
///    the equi and range/band paths.
fn classify_join_where(
    selection: Option<&Expr>,
    join_type: JoinType,
    residual: &mut Vec<Expr>,
) -> Result<Vec<Expr>, GnitzSqlError> {
    let Some(where_expr) = selection else {
        return Ok(Vec::new());
    };
    let mut leaves = Vec::new();
    flatten_conjuncts(where_expr, &mut leaves);
    let conjuncts: Vec<Expr> = leaves.into_iter().cloned().collect();
    if join_type == JoinType::Inner {
        residual.extend(conjuncts);
        Ok(Vec::new())
    } else {
        Ok(conjuncts)
    }
}

/// Join-chain provenance: one entry per original relation alias —
/// `(alias, its pruned per-alias schema, column offset within the current
/// accumulator)`. The schema is pruned to the columns kept in the segment:
/// resolution reads only `.columns` (name → position), so a dead column is simply
/// absent; the segment's real PK region comes from the emitted segment schema,
/// never these entries.
type Provenance = Vec<(String, Rc<Schema>, usize)>;

/// Per-alias set of live column names, both lowercased (`alias → {name}`): the
/// columns of each original relation alias still referenced by a later ON, the
/// final projection, or the final WHERE. Keyed/matched by NAME (never base index)
/// so it maps cleanly onto each segment's already-pruned schema — names survive
/// pruning, indices do not. The chain pre-pass builds one per intermediate
/// segment; `live_columns_projection` reads it.
type LiveNames = HashMap<String, HashSet<String>>;

/// Resolve `qual`/`name` against the aliases visible at a reference site and mark
/// it live: a qualified `q.c` marks `q`'s column `c`; a bare `c` marks it in every
/// visible alias carrying a column named `c` (unique → precise, ambiguous → a safe
/// over-approximation — a real binding error surfaces at emit regardless). Names
/// resolve against the UNPRUNED base schemas carried by `visible` (whose aliases
/// are already lowercased).
fn mark_live(qual: Option<&str>, name: &str, visible: &[(String, Rc<Schema>)], live: &mut LiveNames) {
    for (alias, sch) in visible {
        if qual.is_some_and(|q| !alias.eq_ignore_ascii_case(q)) {
            continue;
        }
        if sch.columns.iter().any(|c| c.name.eq_ignore_ascii_case(name)) {
            live.entry(alias.clone()).or_default().insert(name.to_ascii_lowercase());
        }
    }
}

/// Mark live every column referenced by `expr`, resolving each `(qualifier, name)`
/// against `visible` via `mark_live`.
fn collect_live_from_expr(expr: &Expr, visible: &[(String, Rc<Schema>)], live: &mut LiveNames) {
    let mut refs: Vec<(Option<&str>, &str)> = Vec::new();
    collect_column_refs(expr, &mut refs);
    for (qual, name) in refs {
        mark_live(qual, name, visible, live);
    }
}

/// Mark live every column the final projection references (over all aliases). A
/// wildcard (`*` / `tbl.*`) marks every column of every alias — the degenerate
/// no-pruning case that keeps a `SELECT *` chain byte-identical to today.
fn collect_live_from_projection(projection: &[SelectItem], aliases: &[(String, Rc<Schema>)], live: &mut LiveNames) {
    let mut refs: Vec<(Option<&str>, &str)> = Vec::new();
    if collect_projection_column_refs(projection, &mut refs) {
        for (qual, name) in refs {
            mark_live(qual, name, aliases, live);
        }
    } else {
        for (alias, sch) in aliases {
            let set = live.entry(alias.clone()).or_default();
            set.extend(sch.columns.iter().map(|c| c.name.to_ascii_lowercase()));
        }
    }
}

/// Intermediate hidden segment projection plus the next provenance: per accumulated
/// alias (via the provenance) then the new relation, only the columns still LIVE
/// downstream (`live`), keeping their original names in schema order. Carries the
/// live real columns forward while dropping the accumulator's leading synthetic
/// `_join_pk` (the provenance offsets skip it), so no synthetic PK ever accumulates
/// in the payload. Original names (possibly duplicated across the two sides) are
/// kept so a downstream reduce/distinct over a hidden `H` can resolve columns by
/// name; the duplicate-output-name guard is skipped for these hidden segments
/// (never user-queried — see `emit_join`'s `check_dup_names`), and the next join's
/// own ON resolves through the provenance (pruned per-alias schemas), never these
/// names.
///
/// Each provenance entry carries the alias's PRUNED schema (kept `ColumnDef`s
/// verbatim, `pk_cols` filtered to kept positions — possibly empty) and the
/// payload-relative offset. The caller shifts the offsets past the emitted
/// segment's synthetic-PK region (whose arity depends on the join kind) once it is
/// known. One loop builds projection and provenance, so their order/offsets cannot
/// drift.
fn live_columns_projection(
    prov: &Provenance,
    right_alias: &str,
    right_schema: &Rc<Schema>,
    live: &LiveNames,
) -> (Vec<SelectItem>, Provenance) {
    let mut items = Vec::new();
    let mut new_prov: Provenance = Vec::with_capacity(prov.len() + 1);
    let empty = HashSet::new();
    let mut push_alias = |alias: &str, sch: &Rc<Schema>| {
        let live_names = live.get(&alias.to_ascii_lowercase()).unwrap_or(&empty);
        // Kept columns of `sch`, schema order — those still referenced downstream.
        let kept: Vec<usize> = (0..sch.columns.len())
            .filter(|&i| live_names.contains(&sch.columns[i].name.to_ascii_lowercase()))
            .collect();
        let pruned = Rc::new(Schema {
            columns: kept.iter().map(|&i| sch.columns[i].clone()).collect(),
            pk_cols: sch
                .pk_cols
                .iter()
                .filter_map(|&pk| kept.iter().position(|&k| k == pk))
                .collect(),
        });
        new_prov.push((alias.to_string(), pruned, items.len()));
        for &i in &kept {
            items.push(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                Ident::new(alias),
                Ident::new(sch.columns[i].name.as_str()),
            ])));
        }
    };
    for (alias, sch, _) in prov {
        push_alias(alias, sch);
    }
    push_alias(right_alias, right_schema);
    (items, new_prov)
}

/// One resolved join step of the chain: its ON + join type and the right
/// relation's alias and base `(tid, schema)` — resolved once, consumed by both
/// the liveness pre-pass and the emit loop.
struct JoinStep<'a> {
    on: &'a Expr,
    join_type: JoinType,
    right_alias: String,
    right_base_tid: u64,
    right_base_schema: Rc<Schema>,
}

/// Plan a join FROM (`from[0].joins.len() >= 1`) as a left-deep chain of 2-way
/// join steps, reusing the standard join emitter unchanged. Each intermediate
/// `h_i = h_{i-1} ⋈ r_i` is a hidden view pushed onto `chain` (pre-allocated id,
/// live columns projected); the final step is emitted with `emit_vid`, the
/// user's projection, and the top-level WHERE (`classify_join_where`). A single
/// join is the degenerate case: no intermediates, one emit. Original table
/// aliases are tracked through a *provenance* map — alias → (accumulator, column
/// offset) — so a later `ON`/projection/WHERE reference to `a.x` resolves to the
/// accumulated hidden view's physical column with no name rewriting. Returns the
/// final pieces.
pub(crate) fn plan_join_chain(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    emit_vid: u64,
    select: &sqlparser::ast::Select,
    chain: &mut ViewChain,
) -> Result<EmitPieces, GnitzSqlError> {
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
            distinct: false,
        },
        "CREATE VIEW JOIN",
    )?;
    let from = &select.from[0];
    let n_joins = from.joins.len();
    debug_assert!(n_joins >= 1, "plan_join_chain requires a join");

    // The leftmost relation seeds the accumulator.
    let (left_name, left_alias) = extract_table_name_and_alias(&from.relation, "CREATE VIEW JOIN")?;
    let (mut acc_tid, mut acc_schema) = binder.resolve(client, &left_name)?;

    // Resolve every join step once — ON + join type, right alias, right base
    // (tid, schema) — for the liveness pre-pass and the emit loop below.
    let mut steps: Vec<JoinStep<'_>> = Vec::with_capacity(n_joins);
    for join in &from.joins {
        let (on, join_type) = join_on_and_type(join)?;
        let (right_name, right_alias) = extract_table_name_and_alias(&join.relation, "CREATE VIEW JOIN")?;
        let (right_base_tid, right_base_schema) = binder.resolve(client, &right_name)?;
        steps.push(JoinStep {
            on,
            join_type,
            right_alias,
            right_base_tid,
            right_base_schema,
        });
    }

    // ── Liveness pre-pass (chains only — a 2-way join emits no intermediate
    // segment, so there is nothing to prune). Collect the column references of each
    // step's ON (visible = aliases 0..=that step's right relation), the final
    // projection, and the final WHERE, against the relations' (lowercased alias,
    // unpruned base schema) in left-deep order — pruning starts at the first emitted
    // segment's projection. `suffix[k]` unions steps ≥ k with the final consumer;
    // the segment emitted after step i keeps exactly `suffix[i + 1]` (`suffix[0]`
    // has no segment and stays empty).
    let suffix: Vec<LiveNames> = if n_joins > 1 {
        let mut alias_order: Vec<(String, Rc<Schema>)> = Vec::with_capacity(n_joins + 1);
        alias_order.push((left_alias.to_ascii_lowercase(), Rc::clone(&acc_schema)));
        for step in &steps {
            alias_order.push((
                step.right_alias.to_ascii_lowercase(),
                Rc::clone(&step.right_base_schema),
            ));
        }
        let mut final_refs = LiveNames::new();
        collect_live_from_projection(&select.projection, &alias_order, &mut final_refs);
        if let Some(sel) = &select.selection {
            collect_live_from_expr(sel, &alias_order, &mut final_refs);
        }
        let mut suffix: Vec<LiveNames> = vec![LiveNames::new(); n_joins + 1];
        suffix[n_joins] = final_refs;
        for k in (1..n_joins).rev() {
            let mut s = suffix[k + 1].clone();
            collect_live_from_expr(steps[k].on, &alias_order[..=k + 1], &mut s);
            suffix[k] = s;
        }
        suffix
    } else {
        Vec::new()
    };

    // Provenance: (original alias, its pruned per-alias schema, column offset within
    // `acc`). Seeded with the leftmost relation's full base schema (pruned once it
    // enters the first emitted segment).
    let mut prov: Provenance = vec![(left_alias, Rc::clone(&acc_schema), 0)];
    // Base relation ids already incorporated. A right relation whose base id repeats is a
    // self-join, wrapped in a distinct-source pass-through hidden view below; the wrapper
    // is recorded per base id so a third and later occurrence reuses it instead of
    // materializing another full-table copy.
    let mut used_base_tids: Vec<u64> = vec![acc_tid];
    let mut wrappers: Vec<(u64, (u64, Rc<Schema>))> = Vec::new();

    for (i, step) in steps.iter().enumerate() {
        let (on, join_type, right_alias) = (step.on, step.join_type, &step.right_alias);
        if prov.iter().any(|(a, _, _)| a.eq_ignore_ascii_case(right_alias)) {
            return Err(GnitzSqlError::Unsupported(format!(
                "duplicate relation alias '{right_alias}' in a multi-way join"
            )));
        }
        let (r_base_tid, r_base_schema) = (step.right_base_tid, &step.right_base_schema);
        // Self-join: a repeated base relation is wrapped in a distinct-source pass-through
        // hidden view, so the two join inputs are never the same source (single-source-
        // per-epoch is preserved; the shared base reaches them in separate epochs). Its
        // output schema equals the source's (identity), possibly PK-reordered, so use the
        // emitted schema. One wrapper per base id: reusing it for a later occurrence keeps
        // every join step's two inputs distinct sources (the accumulator is always a
        // hidden intermediate by then) — the accepted shared-source-branch shape.
        let (r_tid, r_schema) = if used_base_tids.contains(&r_base_tid) {
            match wrappers.iter().find(|(base, _)| *base == r_base_tid) {
                Some((_, w)) => w.clone(),
                None => {
                    let w = chain.add_segment(client, |_, _, vid| {
                        let rel = crate::plan::lp::passthrough_rel(r_base_tid, Rc::clone(r_base_schema))?;
                        crate::plan::view::simple::emit_linear(vid, rel)
                    })?;
                    wrappers.push((r_base_tid, w.clone()));
                    w
                }
            }
        } else {
            used_base_tids.push(r_base_tid);
            (r_base_tid, Rc::clone(r_base_schema))
        };

        // Provenance alias map: original aliases → accumulator (at their offsets); the
        // new relation → right, at offset = accumulator width.
        let mut alias_map = AliasMap::with_capacity(prov.len() + 1);
        for (a, sch, off) in &prov {
            alias_map.insert(
                a.to_ascii_lowercase(),
                ResolvedRelation {
                    table_id: acc_tid,
                    schema: Rc::clone(sch),
                    col_offset: *off,
                },
            );
        }
        alias_map.insert(
            right_alias.to_ascii_lowercase(),
            ResolvedRelation {
                table_id: r_tid,
                schema: Rc::clone(&r_schema),
                col_offset: acc_schema.columns.len(),
            },
        );

        let (left_join_cols, right_join_cols, target_tcs, range_conjunct, mut residual) =
            extract_join_predicates(on, &acc_schema, &r_schema, &alias_map)?;

        let is_last = i == n_joins - 1;
        // The top-level WHERE applies to the final step's output.
        let where_filter = if is_last {
            classify_join_where(select.selection.as_ref(), join_type, &mut residual)?
        } else {
            Vec::new()
        };

        let lowered = LoweredJoin {
            left_tid: acc_tid,
            right_tid: r_tid,
            left_schema: Rc::clone(&acc_schema),
            right_schema: Rc::clone(&r_schema),
            alias_map,
            join_type,
            left_join_cols,
            right_join_cols,
            target_tcs,
            range_conjunct,
            residual,
            where_filter,
        };

        if is_last {
            let pieces = emit_join(emit_vid, &select.projection, lowered)?;
            // Reject duplicate output names from explicit user aliases (e.g.
            // `SELECT l.a AS x, r.b AS x`) at the one user-facing emit site. A
            // `SELECT *` join legitimately surfaces same-named columns from both
            // sides (both tables' `id`); that is the established wildcard
            // contract, so the guard applies only to explicit projections.
            if !is_wildcard_projection(&select.projection) {
                reject_duplicate_column_names(&pieces.1, "join view")?;
            }
            return Ok(pieces);
        }

        // Intermediate: emit a hidden segment carrying forward only the columns still
        // LIVE downstream (`suffix[i + 1]` — later ONs + the final projection/WHERE). It
        // is never user-queried (downstream resolves by provenance/name), so keep
        // original names — the duplicate-output-name guard runs only at the final
        // user-facing emit above.
        let (projection, mut new_prov) = live_columns_projection(&prov, right_alias, &r_schema, &suffix[i + 1]);
        let (view_id, seg_schema) = chain.add_segment(client, |_, _, vid| emit_join(vid, &projection, lowered))?;

        // Advance the accumulator, shifting the payload-relative provenance past
        // this segment's synthetic-PK region.
        let k_i = seg_schema.pk_cols.len();
        for p in &mut new_prov {
            p.2 += k_i;
        }
        acc_tid = view_id;
        acc_schema = seg_schema;
        prov = new_prov;
    }
    unreachable!("join-chain loop returns on the last (is_last) join")
}

/// The combined `[left ‖ right]` keep bitset for equi-join reindex-payload pruning:
/// which source columns must survive into the reindex payload because a downstream
/// consumer reads them. A column is kept iff referenced by
///   1. the emit `projection` argument (a `Wildcard`/`tbl.*` — or any item that
///      cannot be enumerated — marks everything, so `SELECT *` degenerates to the
///      unpruned layout);
///   2. a residual-ON conjunct (INNER) or a top-level WHERE conjunct (OUTER); or
///   3. — on a preserved outer side — a nullable join-key component. The packed
///      key is not injective across `{NULL, real 0}`, so a NULL-keyed preserved
///      row (true S = 0) and a 0-keyed row (S from matches) would coarsen to one ν
///      identity if the key's null-bit-carrying payload copy were dropped; keeping
///      the nullable components restores the split. Only the *nullable* components
///      are needed — a non-nullable component packs injectively.
///
/// Join keys are otherwise not auto-kept: they live in the PK slots, and a payload
/// copy exists only if a rule above references them. Resolution reuses the same
/// `resolve_*` helpers the residual/projection binders use, against the UNPRUNED
/// `alias_map`, so a keep-set and its later binding never disagree; a resolution
/// error is the identical error those binders would raise.
fn equi_keep_combined(projection: &[SelectItem], j: &LoweredJoin) -> Result<Vec<bool>, GnitzSqlError> {
    let (left_n, right_n) = (j.left_schema.columns.len(), j.right_schema.columns.len());
    let resolve = |qual: Option<&str>, name: &str| -> Result<usize, GnitzSqlError> {
        match qual {
            Some(q) => resolve_qualified_column(q, name, &j.alias_map),
            None => resolve_unqualified_column(name, &j.alias_map),
        }
    };
    let mut keep = vec![false; left_n + right_n];

    // Rule 1: projection.
    let mut proj_refs: Vec<(Option<&str>, &str)> = Vec::new();
    if collect_projection_column_refs(projection, &mut proj_refs) {
        for (qual, name) in &proj_refs {
            keep[resolve(*qual, name)?] = true;
        }
    } else {
        // A wildcard references everything — nothing to prune.
        return Ok(vec![true; left_n + right_n]);
    }

    // Rule 2: residual ON + top-level WHERE conjuncts.
    let mut post_refs: Vec<(Option<&str>, &str)> = Vec::new();
    for e in j.residual.iter().chain(&j.where_filter) {
        collect_column_refs(e, &mut post_refs);
    }
    for (qual, name) in &post_refs {
        keep[resolve(*qual, name)?] = true;
    }

    // Rule 3: preserved side's nullable join-key components.
    if j.join_type.preserves_left() {
        for &c in &j.left_join_cols {
            if j.left_schema.columns[c].is_nullable {
                keep[c] = true;
            }
        }
    }
    if j.join_type.preserves_right() {
        for &c in &j.right_join_cols {
            if j.right_schema.columns[c].is_nullable {
                keep[left_n + c] = true;
            }
        }
    }
    Ok(keep)
}

/// A schema pruned to its `keep` columns (ascending source order), payload-only —
/// `pk_cols` is dropped (empty), since the pruned schema drives output-layout
/// derivation and name resolution, never a PK region.
fn prune_schema(schema: &Schema, keep: &[usize]) -> Schema {
    Schema {
        columns: keep.iter().map(|&i| schema.columns[i].clone()).collect(),
        pk_cols: Vec::new(),
    }
}

/// Rebuild the join `alias_map` onto the pruned `[pruned_left ‖ pruned_right]`
/// layout. Per-alias — a chain segment's left side carries several original
/// aliases at distinct offsets, so collapsing them all to `{0, pl}` would alias
/// their column spaces and silently misresolve a later reference. Each alias keeps
/// only its kept columns (name resolution reads `.columns` by name, so a dropped
/// column is simply absent) and moves to its offset within the pruned layout =
/// the count of kept columns before its span, on its own side. `keep` is the
/// EFFECTIVE combined bitset (post dummy-column), so the offsets it yields match
/// the physical pruned widths.
fn prune_alias_map(alias_map: &AliasMap, keep: &[bool], left_n: usize) -> AliasMap {
    let pl = keep[..left_n].iter().filter(|&&b| b).count();
    let mut out = AliasMap::with_capacity(alias_map.len());
    for (alias, rel) in alias_map {
        let base = rel.col_offset;
        let kept: Vec<usize> = (0..rel.schema.columns.len()).filter(|j| keep[base + j]).collect();
        let new_offset = if base < left_n {
            keep[..base].iter().filter(|&&b| b).count()
        } else {
            pl + keep[left_n..base].iter().filter(|&&b| b).count()
        };
        out.insert(
            alias.clone(),
            ResolvedRelation {
                table_id: rel.table_id,
                schema: Rc::new(prune_schema(&rel.schema, &kept)),
                col_offset: new_offset,
            },
        );
    }
    out
}

/// Emit a join view's circuit (equi or range/band, INNER or outer) from its
/// resolved `LoweredJoin`, returning `(circuit, output_columns, pk_cols)`.
/// `view_id` is pre-allocated so a chain's downstream segment can reference this
/// segment's store; the segment itself is created by the caller.
fn emit_join(view_id: u64, projection: &[SelectItem], mut lowered: LoweredJoin) -> Result<EmitPieces, GnitzSqlError> {
    // Outer (LEFT/RIGHT/FULL) + residual is rejected (§3): for an outer join the ON
    // predicate is part of the *match* condition — a preserved-side row whose only
    // physical matches all fail the residual must still null-fill, but the null-fill
    // decides match existence from the inner output (or the MAX/MIN threshold
    // witness), independently of the residual, and so would not retro-null-fill a row
    // matched only by residual-failing pairs. A consistent boundary across all join
    // shapes beats an inconsistent partial one; INNER residuals are fully supported.
    if lowered.join_type != JoinType::Inner && !lowered.residual.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "LEFT/RIGHT/FULL JOIN with a residual ON predicate (a non-equi/non-range \
             conjunct, or a second range conjunct) is not supported; the residual \
             would have to participate in the outer null-fill. Use INNER JOIN, or \
             move the predicate to a WHERE over a wrapping view."
                .into(),
        ));
    }

    // Range (band) join: one range conjunct (optionally behind equality
    // conjuncts) routes to the broadcast / re-key / output-exchange circuit. The
    // band null-fill (`ν = positive_part(P − π_P(inner))`) is weight-exact, so a
    // bag-valued preserved side — a view, a UNION ALL dropping the PK — is correct
    // and needs no base-table guard (`positive_part` subtracts the raw matched
    // multiplicity and clamps the result, never over-filling). RIGHT/FULL extend it
    // symmetrically for the band path; pure-range RIGHT/FULL is rejected inside.
    if let Some(range) = lowered.range_conjunct.take() {
        return emit_range_join(view_id, projection, &lowered, range);
    }

    // ── Reindex-payload pruning. Keep only the source columns a downstream
    // consumer reads — the emit `projection` PARAMETER (a chain's synthetic segment
    // projection, not the user SELECT), the residual/WHERE, and the preserved-side
    // nullable keys — so a dead column stops flowing through the reindex MAP and
    // persisting in the join trace. The engine derives the reindex output schema
    // from the program's copy list, so pruning is entirely a matter of which
    // columns the reindex programs copy. `SELECT *` marks everything, so the
    // pruned circuit is identical to the unpruned one.
    let mut keep = equi_keep_combined(projection, &lowered)?;

    let LoweredJoin {
        left_tid,
        right_tid,
        left_schema,
        right_schema,
        alias_map,
        join_type,
        left_join_cols,
        right_join_cols,
        target_tcs,
        range_conjunct: _,
        residual,
        where_filter,
    } = lowered;

    // Equi-join path (zero range conjuncts) — byte-identical to before. The
    // reindex-slot arity cap was already enforced in extract_join_predicates.
    let k = left_join_cols.len(); // == right_join_cols.len(), 1..=PK_LIST_MAX_COLS

    // Per-side carried target tc for each key pair (0 = self-derive); see
    // `side_target_tcs`.
    let left_target_tcs = side_target_tcs(&left_join_cols, &left_schema, &target_tcs);
    let right_target_tcs = side_target_tcs(&right_join_cols, &right_schema, &target_tcs);

    let left_n = left_schema.columns.len();
    let right_n = right_schema.columns.len();

    // A side that keeps nothing still retains source column 0: a zero-payload
    // batch is an unexercised shape across the storage stack, and the dead column
    // is dropped by the final projection anyway.
    if left_n > 0 && !keep[..left_n].iter().any(|&b| b) {
        keep[0] = true;
    }
    if right_n > 0 && !keep[left_n..].iter().any(|&b| b) {
        keep[left_n] = true;
    }
    // Kept source indices (ascending) and the pruned side widths. The pruned
    // schemas describe the reindex OUTPUT payloads (the reindex INPUT stays the
    // full source); the pruned alias map re-offsets per alias — a chain segment's
    // left side carries several aliases at distinct offsets. The unpruned map has
    // no further reader, so shadow it. Reindex programs are built fresh at each
    // `map_reindex` site (the inner reindex and, for an outer null-fill, the
    // `a_all`/`b_all` re-key of the unfiltered input) from the SAME keep list, so
    // `p_all` and `π_P(inner)` carry byte-identical rows and the null-fill
    // cancellation is unaffected.
    let keep_l: Vec<usize> = (0..left_n).filter(|&i| keep[i]).collect();
    let keep_r: Vec<usize> = (0..right_n).filter(|&i| keep[left_n + i]).collect();
    let pl = keep_l.len();
    let pr = keep_r.len();
    let pruned_left_schema = prune_schema(&left_schema, &keep_l);
    let pruned_right_schema = prune_schema(&right_schema, &keep_r);
    let alias_map = prune_alias_map(&alias_map, &keep, left_n);

    // The merged join output is [k _join_pk cols, kept-left cols..., kept-right cols...].
    reject_column_overflow("JOIN view output", k + pl + pr)?;

    let mut cb = CircuitBuilder::new(view_id, 0); // no single primary source
    let input_a_raw = cb.input_delta_tagged(left_tid);
    let input_b_raw = cb.input_delta_tagged(right_tid);

    // The symmetric 2-term join over the NULL-gated, reindexed sides. The
    // unfiltered `input_a_raw`/`input_b_raw` are kept: an outer null-fill's
    // `a_all`/`b_all` re-keys the FULL preserved input (NULL-keyed rows
    // included) — a NULL-keyed row is never in `inner`, so `π_P(inner)` is 0
    // for it and `positive_part(P − π_P(inner)) = w_P` null-fills it at full
    // multiplicity. For a non-nullable key the gated node IS the raw input and
    // `a_all`/`b_all` reuse the reindex node directly.
    let terms = emit_equi_join_terms(
        &mut cb,
        EquiSide {
            input: input_a_raw,
            cols: &left_join_cols,
            target_tcs: &left_target_tcs,
            schema: &left_schema,
            keep: &keep_l,
        },
        EquiSide {
            input: input_b_raw,
            cols: &right_join_cols,
            target_tcs: &right_target_tcs,
            schema: &right_schema,
            keep: &keep_r,
        },
    )?;
    let EquiTerms {
        reindex_a,
        reindex_b,
        a_nullable: left_key_nullable,
        b_nullable: right_key_nullable,
        join_ab,
        join_ba,
    } = terms;

    // Normalize both terms onto [PK cols, kept-A cols, kept-B cols] and union them
    // (widths are the PRUNED payloads `pl`/`pr`).
    let inner_merged = normalize_to_ab(&mut cb, join_ab, join_ba, k, pl, pr);

    let merged = if join_type == JoinType::Inner {
        inner_merged
    } else {
        // ── Outer null-fill (symmetric ν_A / ν_B), keyed by `_join_pk`. ───────────
        // For preserved side P (payload span `[p0, p0+p_n)` in `inner_merged`), the
        // null-fill is `ν_P = positive_part(P − π_P(inner))` (the source PK rides in
        // the payload as the disambiguator):
        //   - `proj_P` re-projects the inner output to `[_join_pk, P]` (drops the O
        //     cols); `cb.map` indices are full-column (the k `_join_pk` PK cols are
        //     0..k; the A‖B payload starts at k).
        //   - `p_all` re-keys the FULL preserved input (every row, NULL-keyed included)
        //     to the IDENTICAL `[_join_pk, P]` encoding — same target tcs + reindex
        //     program as `reindex_P` — so a matched row's `+w_P` and `−m` (= `−w_P·S`)
        //     cancel before the clamp. The join-shard scatter co-locates `p_all` and
        //     the inner output on the `_join_pk` worker — no exchange.
        //   - `positive_part` subtracts the RAW matched multiplicity (weight-exact for
        //     a bag-valued preserved side) and absorbs the within-epoch ΔP/Δπ_P(inner)
        //     simultaneity and the cross-epoch transient.
        // `null_extend` is append-only ⇒ canonical `[_join_pk, A, B]` for P = A; for
        // P = B the appended NULL-A lands after B and needs the `normalize_to_ab` BA
        // reorder to become `[_join_pk, NULL-A, B]`.
        let equi_nf = |cb: &mut CircuitBuilder,
                       p_all: gnitz_core::NodeId,
                       p0: usize,
                       p_n: usize,
                       o_col_tcs: &[u64],
                       preserved_is_left: bool|
         -> gnitz_core::NodeId {
            let proj_p = cb.map(inner_merged, &(p0..p0 + p_n).collect::<Vec<_>>()); // [_join_pk, P]
            let nu_p = cb.positive_diff(p_all, proj_p); // max(0, P − π_P(inner))
            let ext = cb.null_extend(nu_p, o_col_tcs);
            if preserved_is_left {
                ext // [_join_pk, A, NULL-B] — canonical
            } else {
                // Pruned widths: appended NULL-A (pl cols) after B (pr cols).
                let reorder: Vec<usize> = (k + pr..k + pr + pl).chain(k..k + pr).collect();
                cb.map(ext, &reorder) // [_join_pk, B, NULL-A] ⇒ [_join_pk, NULL-A, B]
            }
        };

        // Emit inner ∪ ν_A ∪ ν_B by unioning each preserved side's null-fill onto the
        // inner output. `inner_merged` feeds `proj_p` (inside `equi_nf`) and these
        // unions, so it rides the non-destructive PORT_IN_B operand throughout (op_union
        // empties PORT_IN_A; each null-fill is a fresh single-consumer node, as is the
        // accumulator after the first union). A non-Inner join preserves ≥ 1 side, so at
        // least one null-fill is always folded in.
        let mut merged = inner_merged;

        // ν_A (preserved left): `a_all` re-keys the unfiltered left input — reusing
        // `reindex_a` when the key is non-nullable (`input_a_match == input_a_raw`).
        if join_type.preserves_left() {
            let a_all = if left_key_nullable {
                cb.map_reindex(
                    input_a_raw,
                    &left_join_cols,
                    &left_target_tcs,
                    build_reindex_program_keep(&left_schema, &keep_l),
                )
            } else {
                reindex_a
            };
            let nf_a = equi_nf(&mut cb, a_all, k, pl, &schema_type_codes(&pruned_right_schema), true);
            merged = cb.union(nf_a, merged);
        }
        // ν_B (preserved right): `b_all` re-keys the unfiltered right input; kept-B sits
        // at `k + pl` in `inner_merged`.
        if join_type.preserves_right() {
            let b_all = if right_key_nullable {
                cb.map_reindex(
                    input_b_raw,
                    &right_join_cols,
                    &right_target_tcs,
                    build_reindex_program_keep(&right_schema, &keep_r),
                )
            } else {
                reindex_b
            };
            let nf_b = equi_nf(
                &mut cb,
                b_all,
                k + pl,
                pr,
                &schema_type_codes(&pruned_left_schema),
                false,
            );
            merged = cb.union(nf_b, merged);
        }
        merged
    };

    // Build virtual combined output schema: k synthetic join PK cols + kept left
    // cols + kept right cols. After proj_ab/proj_ba, the UNION output has this layout
    // (union col indices):
    //   col 0..k: _join_pk[_i] (PK region, one slot per key pair)
    //   col k..k+pl: kept A columns (in A schema order)
    //   col k+pl..k+pl+pr: kept B columns (in B schema order)
    let mut out_cols: Vec<ColumnDef> = join_pk_coldefs(&target_tcs);
    // The kept-A‖kept-B payload with outer nullability applied; the `_join_pk`
    // columns above stay non-nullable (a NULL key packs to the synthetic 0, never a
    // NULL PK).
    out_cols.extend(combined_payload_coldefs(
        &pruned_left_schema,
        &pruned_right_schema,
        join_type,
    ));

    // One linear filter over the normalized join output `merged` ([_join_pk × k, A, B])
    // before projection/sink, bound against `out_cols` (which carries each column's real
    // nullability) — incrementally free (no state). At most one source is non-empty
    // (`classify_join_where`), so concatenating them emits a single program:
    //   - Residual ON predicates ⇒ INNER (outer was rejected above), so `merged ==
    //     inner_merged`; INNER 3VL drops a NULL/UNKNOWN predicate row natively.
    //   - OUTER-equi WHERE (`where_filter`) is applied over the *post-null-fill* output:
    //     a null-filled column failing the predicate drops its row (correct 3VL), which
    //     matches pushing preserved-side conjuncts below the join.
    let post_filter: Vec<Expr> = residual.into_iter().chain(where_filter).collect();
    let merged = if post_filter.is_empty() {
        merged
    } else {
        let merged_schema = Schema {
            columns: out_cols.clone(),
            pk_cols: (0..k).collect(),
        };
        let prog = build_residual_filter_prog(&post_filter, &alias_map, &merged_schema, k)?;
        cb.filter(merged, Some(prog))
    };

    // Compute the user projection + view schema via the shared join-projection
    // helper, resolving against the PRUNED alias map / combined width (`pl + pr`) so
    // every reference lands on its physical column in the pruned layout. A lone
    // `SELECT *` flows through its Wildcard arm and stays identity (the projection
    // map is then skipped below), so no wildcard fast path here.
    let (final_cols, final_projection) = build_join_view_projection(
        projection,
        &alias_map,
        &out_cols[..k],
        pl + pr,
        k,
        |idx| out_cols[k + idx].clone(),
        "JOIN view",
    )?;

    // Apply final column projection before sink when not identity.
    // Identity = selecting all kept left+right cols in canonical order [k..k+pl+pr].
    let is_identity = is_identity_projection(&final_projection, pl + pr, k);
    let sink_input = if is_identity {
        merged
    } else {
        cb.map(merged, &final_projection)
    };
    cb.sink(sink_input);
    let circuit = cb.build();

    // The view's physical PK is the k synthetic `_join_pk` columns at slots 0..k
    // (final_cols lists them first). At k = 1 this is the existing single `[0]`.
    // The duplicate-output-name guard runs at `plan_join_chain`'s final-segment
    // call site — the one user-facing emit; hidden intermediates keep original
    // (possibly duplicated) names by design.
    let view_pk: Vec<u32> = (0..k as u32).collect();
    Ok((circuit, final_cols, view_pk))
}

/// Build a non-equi (range / band) join view: `n_eq` equality conjuncts (possibly
/// zero) plus exactly one range conjunct. Both sides reindex onto
/// `[eq slots…, range slot]` at the pair's common promoted type, so each side's
/// trace is an ordered arrangement by the range key; the active delta is
/// eq-prefix-scattered (band join, `n_eq ≥ 1`) or broadcast (pure range join,
/// `n_eq == 0`) and probed against the other side's trace by an ordered range
/// walk. Because the two terms emit with different delta-side keys, the output is
/// re-keyed onto the **source-PK pair** `(a.pk…, b.pk…)` — the only identity
/// under which a `+1` and its later `-1` (from opposite terms, on different
/// workers) are byte-identical — then exchanged by that pair-PK, so the view is
/// PK-partitioned like every other view.
///
/// `join_type` selects INNER / LEFT / RIGHT / FULL. INNER emits only the matched
/// pairs. An outer join additionally emits one null-filled row per unmatched
/// preserved-side row, keyed by that row's source PK (the other side's PK packs to
/// the synthetic 0), computed by `n_eq`:
///   - **Band (`n_eq ≥ 1`):** the weight-exact `ν_P = positive_part(P − π_P(inner))`
///     — the matched preserved rows are read straight off the inner output and
///     re-keyed onto the source PK — partition-local on the eq-scatter. LEFT emits
///     `ν_A`, RIGHT the mirror `ν_B`, FULL both.
///   - **Pure range (`n_eq == 0`):** LEFT only (RIGHT/FULL rejected up front — the
///     mirror null-fill has no `π(inner)` witness). Match existence collapses to a
///     threshold test `a.x OP MAX/MIN(b.range_col)`, so the null-fill is the
///     subtraction `A − (A ⋈ {m})` against the one-row threshold `m`, computed by an
///     INLINE shard-free reduce over the broadcast ΔB — no catalog helpers, no
///     sentinel, no `distinct`.
///
/// All ride the single pair-PK output exchange.
///
/// An INNER `lowered` carries an empty `where_filter` (WHERE folds into the
/// residual); an OUTER one carries the WHERE conjuncts, applied as one linear
/// 3VL filter over the full-width post-null-fill output. `lowered.range_conjunct`
/// was taken by the caller and arrives as `range`.
fn emit_range_join(
    view_id: u64,
    projection: &[SelectItem],
    lowered: &LoweredJoin,
    range: RangeConjunct,
) -> Result<EmitPieces, GnitzSqlError> {
    let LoweredJoin {
        left_tid,
        right_tid,
        join_type,
        ..
    } = *lowered;
    let left_schema: &Schema = &lowered.left_schema;
    let right_schema: &Schema = &lowered.right_schema;
    let alias_map = &lowered.alias_map;
    let left_join_cols = &lowered.left_join_cols;
    let right_join_cols = &lowered.right_join_cols;
    let eq_tcs = &lowered.target_tcs;
    let residual = &lowered.residual;
    let where_filter = &lowered.where_filter;

    let left_n = left_schema.columns.len();
    let right_n = right_schema.columns.len();
    let n_eq = left_join_cols.len();
    let k = n_eq + 1; // reindex slots: eq prefix + range slot
    let pa = left_schema.pk_cols.len();
    let pb = right_schema.pk_cols.len();
    let pair_pk = pa + pb; // output PK arity

    // The reindex-slot arity cap (k ≤ PK_LIST_MAX_COLS) was enforced in
    // extract_join_predicates. Here we additionally cap the output pair-PK.
    //
    // Output pair-PK arity cap (a.pk_count + b.pk_count ≤ PK_LIST_MAX_COLS) — the
    // binding constraint on the synthesized output PK; the stride ceiling is
    // non-binding (≤ 4·16 = 64 ≤ MAX_PK_BYTES). The engine's validate_pk_cols is
    // the backstop; this is the friendly planner error.
    if pair_pk > gnitz_core::PK_LIST_MAX_COLS {
        return Err(GnitzSqlError::Unsupported(format!(
            "range JOIN output PK has {pair_pk} columns (a.pk {pa} + b.pk {pb}), \
             exceeding the {}-column limit",
            gnitz_core::PK_LIST_MAX_COLS
        )));
    }
    // The widest intermediate is the re-key output: pair-PK + k `_join_pk` slots +
    // every A and B column.
    reject_column_overflow("range JOIN view intermediate", pair_pk + k + left_n + right_n)?;

    // Reindex-slot layout (eq pairs first, range slot last) and the §3 term
    // relations, shared with the range EXISTS builder.
    let slots = range_slots(
        left_join_cols,
        right_join_cols,
        eq_tcs,
        &range,
        left_schema,
        right_schema,
    );

    // Pure-range (n_eq == 0) outer-join restrictions. The RIGHT/FULL rejection is checked
    // FIRST so it wins for `Full` (which satisfies both `preserves_right()` and
    // `preserves_left()`): a 16-byte FULL pure-range gets the RIGHT/FULL message, not the
    // LEFT ≤8-byte one. Band (n_eq ≥ 1) has neither restriction — it uses the cap-free
    // `positive_part(P − π_P(inner))` null-fill, so it has no range-type restriction.
    if n_eq == 0 {
        // RIGHT/FULL is rejected: its mirror null-fill `ν_B` has no `π_B(inner)` witness.
        // The broadcast range join scatters the inner output by the OTHER side's range
        // key, so gathering it onto the preserved-key worker would need a second
        // sequential exchange the compiler forbids — the very reason LEFT uses a threshold.
        // A RIGHT/FULL pure-range null-fill would need a full second threshold pipeline (a
        // B-side `m_A = MAX/MIN(a.range)`), a standalone effort.
        if join_type.preserves_right() {
            return Err(GnitzSqlError::Unsupported(
                "pure-range RIGHT/FULL JOIN (a sole inequality range conjunct with no \
                 equality prefix) is not supported; its mirror null-fill has no inner-join \
                 witness on the preserved side. Use INNER/LEFT JOIN, or add an equality \
                 conjunct to make it a band join."
                    .into(),
            ));
        }
        // LEFT (FULL already returned above): the inline threshold null-fill reduces the
        // range column with MIN/MAX (an 8-byte accumulator), then reindexes the result onto
        // the range slot type. MIN/MAX preserves the source integer type, so any ≤8-byte
        // integer range column yields a result the reindex consumes directly. A 16-byte
        // U128/UUID/I128 range column has no 8-byte accumulator — reject up front rather
        // than failing the compile.
        if join_type.preserves_left() && FixedInt::from_type_code(range.tc).is_none() {
            let range_tc = range.tc;
            return Err(GnitzSqlError::Unsupported(format!(
                "pure-range LEFT JOIN needs a ≤8-byte integer range column (got {range_tc:?}); \
                 its threshold null-fill reduces the range column with MIN/MAX, which has no \
                 16-byte accumulator — use a narrower range column, INNER JOIN, or a band join"
            )));
        }
    }

    let mut cb = CircuitBuilder::new(view_id, 0);
    let input_a_raw = cb.input_delta_tagged(left_tid);
    let input_b_raw = cb.input_delta_tagged(right_tid);

    // NULL exclusion + both reindexes (the shared range prologue). The inner
    // match drops NULL-key rows on both sides; the unfiltered inputs are kept
    // (`input_a_raw` / `input_b_raw`): an outer null-fill taps the preserved
    // side's raw input so a preserved row with a NULL eq/range column (never an
    // inner match, never in `D`) is still null-filled (§4). The gated nodes feed
    // only the inner match.
    let (reindex_a, reindex_b, left_key_nullable) =
        range_gate_reindex_prologue(&mut cb, input_a_raw, input_b_raw, &slots, left_schema, right_schema)?;
    let RangeSlots {
        left_reindex_cols,
        all_tcs,
        rel_ab,
        rel_ba,
        ..
    } = slots;

    // Band join (n_eq ≥ 1): the input relay scatters by the eq prefix, so each
    // side's trace is already eq-prefix-partitioned — integrate the scattered
    // reindex directly. Pure range join (n_eq == 0): the input is broadcast, so
    // each worker must drop the rows it does not own (PartitionFilter between
    // reindex and integrate) before integrating, or traces replicate and matches
    // duplicate. The join terms probe the UNFILTERED reindex either way — for a
    // band join that reindex IS the worker's scattered eq-prefix slice.
    //
    // The compiler makes each `PartitionFilter` a keep-all identity when the view's
    // sources are all replicated (it then runs correct-local over full broadcast
    // traces), so this stays unconditional here.
    let (int_a, int_b) = if n_eq == 0 {
        (cb.partition_filter(reindex_a), cb.partition_filter(reindex_b))
    } else {
        (reindex_a, reindex_b)
    };
    let trace_a = cb.integrate_trace(int_a);
    let trace_b = cb.integrate_trace(int_b);
    let join_ab = cb.join_with_trace_range_node(reindex_a, trace_b, n_eq as u8, rel_ab); // ΔA ⋈θ I(B)
    let join_ba = cb.join_with_trace_range_node(reindex_b, trace_a, n_eq as u8, rel_ba); // ΔB ⋈θ I(A)

    // Per-term normalize payload to [A cols, B cols] and union (k = n_eq + 1).
    let merged = normalize_to_ab(&mut cb, join_ab, join_ba, k, left_n, right_n);

    // Re-key onto the source-PK pair `[a.pk…, b.pk…]`. The merged layout is
    // `[_join_pk × k, A cols, B cols]`; A's PK col j sits at `k + j`, B's at
    // `k + left_n + j`. Targets all 0 (self-derive — each slot keeps its source PK
    // type, no cross-side promotion).
    let union_schema = band_union_schema(&all_tcs, left_schema, right_schema);

    // Residual ON predicates: filter `merged` ([_join_pk × k, A, B]) before the
    // source-PK re-key + output exchange (filtering ahead of a linear exchange is
    // order-immaterial and a throughput win — dropped rows are never shipped). A
    // residual ⇒ INNER, so the band LEFT null-fill below (which also reads `merged`)
    // is unreachable and shadowing `merged` here is safe. `k = n_eq + 1`.
    let merged = if residual.is_empty() {
        merged
    } else {
        let prog = build_residual_filter_prog(residual, alias_map, &union_schema, k)?;
        cb.filter(merged, Some(prog))
    };

    let mut pair_pk_cols: Vec<usize> = Vec::with_capacity(pair_pk);
    for &a_pk in &left_schema.pk_cols {
        pair_pk_cols.push(k + a_pk);
    }
    for &b_pk in &right_schema.pk_cols {
        pair_pk_cols.push(k + left_n + b_pk);
    }
    let zero_tcs = vec![0u8; pair_pk];
    let rekey = cb.map_reindex(merged, &pair_pk_cols, &zero_tcs, build_reindex_program(&union_schema));

    // Re-key output layout: `[_pair_pk × pair_pk (PK), _join_pk × k, A cols, B cols]`.
    // The user projection drops the k `_join_pk` slots (they DIFFER per term and
    // must not survive into the exchanged/consolidated output) and selects user
    // columns from A/B. A/B columns start at this payload offset.
    let payload_offset = pair_pk + k;
    // The combined A‖B output columns (outer nullability applied, same rule as the
    // equi path) — the projection helper and the full-width WHERE's combined schema
    // both read them.
    let combined_payload = combined_payload_coldefs(left_schema, right_schema, join_type);

    // Leading output PK columns: A's then B's source-PK column types (the re-key
    // self-derive output type), non-nullable, `_pair_pk_{slot}` numbered across
    // both sides.
    let pair_pk_coldefs: Vec<ColumnDef> = left_schema
        .pk_cols
        .iter()
        .map(|&c| (left_schema, c))
        .chain(right_schema.pk_cols.iter().map(|&c| (right_schema, c)))
        .enumerate()
        .map(|(slot, (schema, c))| {
            // Hidden: the synthetic pair-PK is a physical PK column, not a
            // presentation column (`SELECT *` omits it; not name-resolvable).
            ColumnDef::new(
                format!("_pair_pk_{slot}"),
                schema.columns[c].type_code.reindex_output_type(),
                false,
            )
            .hidden()
        })
        .collect();

    // User projection via the shared helper, built against the layout its final map
    // reads: INNER projects straight off `rekey` (`[pair-PK, _join_pk × k, A, B]`,
    // payload at `payload_offset`) and always maps (the per-term `_join_pk` slots
    // DIFFER per term and may never reach the sink); OUTER projects off the
    // full-width post-null-fill union (`[pair-PK, A, B]`, payload at `pair_pk`).
    let user_offset = if join_type == JoinType::Inner {
        payload_offset
    } else {
        pair_pk
    };
    let (final_cols, final_projection) = build_join_view_projection(
        projection,
        alias_map,
        &pair_pk_coldefs,
        left_n + right_n,
        user_offset,
        |i| combined_payload[i].clone(),
        "range JOIN view",
    )?;

    // ── Outer null-fill + full-width WHERE ───────────────────────────────────────
    // INNER: a single user-projection map over the rekey output (drops the per-term
    // `_join_pk` slots); no null-fill, no `where_filter` (WHERE folded into the
    // residual, already filtered before rekey). OUTER: keep the full combined width
    // `[pair-PK, A, B]` through the null-fill union so the WHERE runs as one linear
    // 3VL filter over the post-null-fill output, then project to the user columns.
    let sink_input = if join_type == JoinType::Inner {
        cb.map(rekey, &final_projection)
    } else {
        // Shared tail: null-extend ν_P with the O-side NULL columns, re-key onto the
        // pair-PK `[a.pk…, b.pk…]` (the other side's PK rides in the NULL-O payload,
        // packing to the synthetic 0), and project to the FULL combined width
        // `[pair-PK, A, B]`. `preserved_is_left` selects the canonical (P=A) vs
        // reordered (P=B) layout: `null_extend` appends and `map_reindex` locks its
        // payload to input order, so only the final `cb.map` can place the NULL-O
        // columns before P (the P=B case).
        let nf_tail =
            |cb: &mut CircuitBuilder, nf_keyed: gnitz_core::NodeId, preserved_is_left: bool| -> gnitz_core::NodeId {
                let (p_pk, p_n) = if preserved_is_left { (pa, left_n) } else { (pb, right_n) };
                let o_col_tcs = schema_type_codes(if preserved_is_left { right_schema } else { left_schema });
                let nullfill = cb.null_extend(nf_keyed, &o_col_tcs); // [P.pk × p_pk, P, NULL-O]

                // Pair-PK [a.pk…, b.pk…]: preserved side's pk from the PK region (0..p_pk),
                // other side's pk from the NULL-O payload (p_pk + p_n + pk → synthetic 0).
                let mut nf_pair_pk_cols: Vec<usize> = Vec::with_capacity(pair_pk);
                if preserved_is_left {
                    nf_pair_pk_cols.extend(0..pa);
                    for &b_pk in &right_schema.pk_cols {
                        nf_pair_pk_cols.push(p_pk + p_n + b_pk);
                    }
                } else {
                    for &a_pk in &left_schema.pk_cols {
                        nf_pair_pk_cols.push(p_pk + p_n + a_pk);
                    }
                    nf_pair_pk_cols.extend(0..pb);
                }

                // Identity copy of the [P, NULL-O] payload (src = dst = p_pk + ci); the
                // P.pk × p_pk output payload slots stay 0 (projected away). The payload is
                // P's columns followed by O's (`o_col_tcs`), in input order — no reorder,
                // since map_reindex locks the output payload to input order.
                let p_col_tcs = schema_type_codes(if preserved_is_left { left_schema } else { right_schema });
                let mut eb = ExprBuilder::new();
                for (ci, &tc) in p_col_tcs.iter().chain(&o_col_tcs).enumerate() {
                    eb.copy_col(tc as u32, (p_pk + ci) as u32, (p_pk + ci) as u32);
                }
                let nf_rekey = cb.map_reindex(nullfill, &nf_pair_pk_cols, &zero_tcs, eb.build(0));

                // nf_rekey output: [pair-PK, P.pk × p_pk, <[P, NULL-O] in input order>];
                // the [P, NULL-O] payload sits at pair_pk + p_pk. Map EVERY combined
                // column (canonical [A, B] index) to its slot, canonicalizing P=B, so
                // the branch is full width `[pair-PK, A, B]` — the layout the post-union
                // WHERE and the final user projection both read.
                let nf_full_projection: Vec<usize> = (0..left_n + right_n)
                    .map(|ci| {
                        if preserved_is_left {
                            pair_pk + pa + ci // [A, B] contiguous after pair-PK + a.pk
                        } else if ci < left_n {
                            pair_pk + pb + right_n + ci // A column → trailing NULL-A region
                        } else {
                            pair_pk + pb + (ci - left_n) // B column → leading B region
                        }
                    })
                    .collect();
                cb.map(nf_rekey, &nf_full_projection)
            };

        // Full-width inner pairs `[pair-PK, A, B]` — drop the per-term `_join_pk`
        // slots (they DIFFER per term and must never survive the union). Fresh and
        // single-consumer, so it seeds the union accumulator on the non-destructive
        // PORT_IN_B and each fresh null-fill branch rides PORT_IN_A.
        let inner_full = cb.map(
            rekey,
            &(payload_offset..payload_offset + left_n + right_n).collect::<Vec<_>>(),
        );

        let zero_a = vec![0u8; pa];
        let unioned = if n_eq == 0 {
            // ── Pure-range threshold SUBTRACTION form ───────────────────────────
            // Unmatched left rows are `A − matched`, where `matched = {a : a.x OP m}`
            // against the ONE-ROW threshold `m = MAX/MIN(b.range_col)`. Subtracting
            // (rather than emitting the complement directly) makes an empty
            // `trace_m` mean "every left row null-fills" for free — `A − ∅ = A` —
            // so empty/all-NULL `b` needs no sentinel; and because
            // `matched = A ⋈ {m}` carries each `a`'s true weight (one-row `m` cannot
            // multiply, no `distinct`), it is weight-exact for a bag-valued left
            // input too. The pipeline itself is shared with the pure-range EXISTS
            // builder (`build_pure_range_threshold`).
            let thr = build_pure_range_threshold(
                &mut cb,
                left_schema,
                range.tc,
                range.op,
                reindex_b,
                int_a,
                trace_a,
                true,
            );
            let matched = thr.matched;
            let a_pass = thr.a_pass.expect("a_pass requested");
            let neg = cb.negate(matched);
            let nf_match = cb.union(a_pass, neg); // A − matched, keyed [a.pk…, A]

            // NULL-range-key rows are filtered out of `reindex_a`/`trace_a` (3VL) and
            // never match the threshold, so they get their own branch off the
            // UNFILTERED broadcast `input_a_raw` (`union_null_key_rows`).
            let nf_keyed = if left_key_nullable {
                union_null_key_rows(&mut cb, nf_match, input_a_raw, &left_reindex_cols, left_schema)?
            } else {
                nf_match
            };
            let branch = nf_tail(&mut cb, nf_keyed, true);
            cb.union(branch, inner_full)
        } else {
            // ── Band (n_eq ≥ 1): ν_A (preserves_left) and/or ν_B (preserves_right). ──
            // ν_P = positive_part(P − π_P(inner)): the matched preserved rows are read
            // straight off the inner output `merged` as π_P(inner), re-keyed onto the
            // source PK; `positive_part` subtracts the RAW matched multiplicity and
            // clamps the result — weight-exact for a bag-valued preserved side, absorbing
            // the within-epoch ΔP/Δπ_P(inner) simultaneity. A reindex Map's output schema
            // is [new-PK, <every input col>], so the clean [P.pk, P] is a full re-key of
            // `merged` THEN a project of the P region (P's PK cols are its slice of
            // `pair_pk_cols`). All nodes are co-located on the eq-prefix worker (the
            // source PK is unique → never spans workers).
            let zero_b = vec![0u8; pb];
            let mut acc = inner_full;
            if join_type.preserves_left() {
                let rekey_a = cb.map_reindex(
                    merged,
                    &pair_pk_cols[..pa],
                    &zero_a,
                    build_reindex_program(&union_schema),
                );
                let proj_a = cb.map(rekey_a, &(pa + k..pa + k + left_n).collect::<Vec<_>>()); // π_A(inner)
                let a_all = cb.map_reindex(
                    input_a_raw,
                    &left_schema.pk_cols,
                    &zero_a,
                    build_reindex_program(left_schema),
                );
                let nu_a = cb.positive_diff(a_all, proj_a); // max(0, A − π_A(inner)), keyed [a.pk, A]
                let branch = nf_tail(&mut cb, nu_a, true);
                acc = cb.union(branch, acc);
            }
            if join_type.preserves_right() {
                let rekey_b = cb.map_reindex(
                    merged,
                    &pair_pk_cols[pa..],
                    &zero_b,
                    build_reindex_program(&union_schema),
                );
                let proj_b = cb.map(
                    rekey_b,
                    &(pb + k + left_n..pb + k + left_n + right_n).collect::<Vec<_>>(),
                ); // π_B(inner)
                let b_all = cb.map_reindex(
                    input_b_raw,
                    &right_schema.pk_cols,
                    &zero_b,
                    build_reindex_program(right_schema),
                );
                let nu_b = cb.positive_diff(b_all, proj_b); // max(0, B − π_B(inner)), keyed [b.pk, B]
                let branch = nf_tail(&mut cb, nu_b, false);
                acc = cb.union(branch, acc);
            }
            acc
        };

        // One linear 3VL WHERE filter over the full-width post-null-fill union, bound
        // against the combined schema `[pair-PK, A, B]` (which carries each column's
        // outer-adjusted nullability, so a null-filled column failing the predicate
        // drops its row and `IS NULL` keeps unmatched rows). No source is non-empty
        // for INNER (handled above), so this is the single WHERE home for OUTER.
        let filtered = if where_filter.is_empty() {
            unioned
        } else {
            let combined_cols: Vec<ColumnDef> = pair_pk_coldefs
                .iter()
                .cloned()
                .chain(combined_payload.iter().cloned())
                .collect();
            let combined_schema = Schema {
                columns: combined_cols,
                pk_cols: (0..pair_pk).collect(),
            };
            let prog = build_residual_filter_prog(where_filter, alias_map, &combined_schema, pair_pk)?;
            cb.filter(unioned, Some(prog))
        };

        // User projection over the full-width `[pair-PK, A, B]` layout
        // (`final_projection` was built with `user_offset = pair_pk`); identity —
        // e.g. `SELECT *` — skips the map, mirroring the equi path.
        if is_identity_projection(&final_projection, left_n + right_n, pair_pk) {
            filtered
        } else {
            cb.map(filtered, &final_projection)
        }
    };

    // ExchangeShard on EXACTLY the pair-PK columns in order — must equal view_pk,
    // so the GroupKey scatter routes by `partition_for_pk_bytes` identically to
    // the view scan/seek (the compound-key alignment invariant).
    let pair_pk_idxs: Vec<usize> = (0..pair_pk).collect();
    let sharded = cb.shard(sink_input, &pair_pk_idxs);
    cb.sink(sharded);
    let circuit = cb.build();

    let view_pk: Vec<u32> = (0..pair_pk as u32).collect();
    debug_assert_eq!(
        pair_pk_idxs,
        view_pk.iter().map(|&c| c as usize).collect::<Vec<_>>(),
        "range join: ExchangeShard cols must equal view_pk in strict order"
    );
    Ok((circuit, final_cols, view_pk))
}

/// The pure-range threshold pipeline's outputs: `matched = A ⋈ {m}` and (when
/// requested) the bare `int_a` passthrough, both re-keyed onto `[a.pk…, A]`.
pub(crate) struct PureRangeThreshold {
    pub(crate) matched: gnitz_core::NodeId,
    pub(crate) a_pass: Option<gnitz_core::NodeId>,
}

/// Build the inline pure-range (`n_eq == 0`) threshold pipeline: the one-row
/// `m = MAX/MIN(b.range_col)` reduce and its trace, the two matched terms
/// (`Δint_a ⋈ trace_m`, `Δreindex_m ⋈ trace_a`) with their A-side projections,
/// and the shared `[a.pk…, A]` re-key applied to the matched union and (when
/// `want_a_pass`) the `int_a` passthrough. Shared by the pure-range LEFT JOIN
/// null-fill (`A − matched`, `want_a_pass = true`) and the pure-range EXISTS
/// builder (EXISTS = `matched` alone; NOT EXISTS = `A − matched`). Node
/// allocation order is load-bearing: the LEFT JOIN keeps compiling
/// byte-identically through this extraction.
///
/// `m` is computed LOCALLY on every worker over the broadcast `reindex_b` (NOT
/// a partition-filtered slice): a global extremum over a fully-broadcast input
/// is identical on every worker, so no scatter/gather is needed. `map_hash_row`
/// moves `reindex_b`'s `Tc` PK into a payload column, DECODING OPK → native AND
/// carrying the `Tc` type verbatim, so the reduce aggregates the native value
/// (col 1), not the OPK bytes (which would invert signed MIN/MAX), and — because
/// non-float MIN/MAX carries its source type (`agg_output_type`) — emits its
/// result already typed `Tc`; `reindex_m` self-derives the correct OPK order
/// straight off the reduce. `global_ground = false`: a ground row would seed
/// `(m=NULL)` into the threshold trace and break the `A − ∅ = A` subtraction
/// that needs the trace empty over an empty other side.
///
/// The two matched terms carry the MATCH op (= the inner op), mirroring
/// `join_ab`/`join_ba` EXACTLY (`rel_ab = converse(op)`, `rel_ba = op`) with
/// only the trace swapped to `trace_m`/`reindex_m`. The `a`-side term taps the
/// filtered `int_a` against the replicated `trace_m`; the `m`-side term taps
/// the replicated `reindex_m` against the filtered `trace_a`, so neither
/// duplicates under broadcast. All range nodes carry n_eq == 0 — load-bearing
/// for the view-level `circuit_range_join_n_eq` discriminator, which reads an
/// ARBITRARY `DeltaTraceRange.n_eq` and is correct only because they all agree.
///
/// Both `matched_raw` and the `int_a` passthrough are `[Tc PK, A]` (a.pk lives
/// INSIDE A, not in the PK). Re-keying BOTH onto `[a.pk…, A]` with one shared
/// reindex program makes a matched `a`'s `+a` (passthrough) and `−a` (matched,
/// negated) byte-identical so they cancel in-epoch. The passthrough re-keys
/// `int_a` (the broadcast ΔA minus its non-owned / NULL-key rows), NOT the raw
/// input: a pure-range relay broadcasts `a`, so re-keying the raw input would
/// emit `W×` copies the output shard would sum.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_pure_range_threshold(
    cb: &mut CircuitBuilder,
    left_schema: &Schema,
    range_tc: TypeCode,
    range_op: RangeRel,
    reindex_b: gnitz_core::NodeId,
    int_a: gnitz_core::NodeId,
    trace_a: gnitz_core::NodeId,
    want_a_pass: bool,
) -> PureRangeThreshold {
    let left_n = left_schema.columns.len();
    let pa = left_schema.pk_cols.len();
    let k = 1usize; // pure range: no eq prefix, one range slot
    let zero_a = vec![0u8; pa];
    let rel_ab = converse_rel(range_op);
    let rel_ba = range_op;
    let want_max = matches!(range_op, RangeRel::Lt | RangeRel::Le);
    let agg_func = if want_max { AGG_MAX } else { AGG_MIN };
    let m_schema = Schema {
        columns: pure_range_m_output_cols(range_tc),
        pk_cols: vec![0],
    };

    let mbh = cb.map_hash_row(reindex_b, &[0], &[], 0);
    // Empty group set (one global threshold over the broadcast other side) → the
    // synthetic `_group_pk` fold, like every other empty-group reduce.
    let red = cb.reduce_multi_local(mbh, &[], &[(agg_func, 1)], false, ReduceOutKey::SyntheticFold); // [_group_pk:U128, m:Tc]
    let carried_m = range_tc.carried_reindex_tc(range_tc);
    let reindex_m = cb.map_reindex(red, &[1], &[carried_m], build_reindex_program(&m_schema));
    let trace_m = cb.integrate_trace(reindex_m);

    let j_am = cb.join_with_trace_range_node(int_a, trace_m, 0, rel_ab);
    let j_ma = cb.join_with_trace_range_node(reindex_m, trace_a, 0, rel_ba);
    // Range-join output = [_join_pk × k, delta payload, trace payload]; project
    // each to the A columns. `j_am`: A is the delta payload at `k..k+left_n`.
    // `j_ma`: A is the trace payload, after Δm's payload (`m`'s 2 columns).
    let m_payload = m_schema.columns.len(); // m's output arity ([_group_pk, m])
    let m_am = cb.map(j_am, &(k..k + left_n).collect::<Vec<_>>());
    let m_ma = cb.map(j_ma, &(k + m_payload..k + m_payload + left_n).collect::<Vec<_>>());
    let matched_raw = cb.union(m_am, m_ma); // [_join_pk(PK), A]

    let jp = ColumnDef::new("_jp", range_tc, false);
    let nf_raw_schema = Schema {
        columns: std::iter::once(jp).chain(left_schema.columns.iter().cloned()).collect(),
        pk_cols: vec![0],
    };
    let a_pk_in_raw: Vec<usize> = left_schema.pk_cols.iter().map(|&p| 1 + p).collect();
    let a_cols: Vec<usize> = (pa + 1..pa + 1 + left_n).collect();
    // One shared reindex program drives both re-keys, so the IDENTICAL encoding
    // is structural — `matched` and `a_pass` differ only in their input node.
    let nf_reindex_prog = build_reindex_program(&nf_raw_schema);
    let rekey_a = |cb: &mut CircuitBuilder, input: gnitz_core::NodeId| {
        let keyed = cb.map_reindex(input, &a_pk_in_raw, &zero_a, nf_reindex_prog.clone());
        cb.map(keyed, &a_cols) // [a.pk…, A]
    };
    let matched = rekey_a(cb, matched_raw);
    let a_pass = want_a_pass.then(|| rekey_a(cb, int_a));
    PureRangeThreshold { matched, a_pass }
}

/// The k synthetic `_join_pk` output PK columns of an equi join / equi EXISTS
/// view, hidden (a physical PK column, not a presentation column — `SELECT *`
/// omits it and it is not name-resolvable). Each slot carries its pair's common
/// type `T_i` — the single persisted stride both sides' reindex Maps and every
/// cross-process consumer re-derive. The first column keeps the name
/// `_join_pk` at k = 1 (preserving the shippable single-key catalog name);
/// composite keys use `_join_pk_{i}`.
pub(crate) fn join_pk_coldefs(target_tcs: &[TypeCode]) -> Vec<ColumnDef> {
    let k = target_tcs.len();
    target_tcs
        .iter()
        .enumerate()
        .map(|(i, &t)| {
            let name = if k == 1 {
                "_join_pk".to_string()
            } else {
                format!("_join_pk_{i}")
            };
            ColumnDef::new(name, t, false).hidden()
        })
        .collect()
}

/// The band re-key's union-layout schema `[_join_pk × k, A cols, B cols]`
/// (`k = all_tcs.len()` synthetic slots as the PK region, both sides' columns
/// verbatim behind them) — the schema the band builders compile their re-key
/// reindex programs and residual filters against.
pub(crate) fn band_union_schema(all_tcs: &[TypeCode], left: &Schema, right: &Schema) -> Schema {
    let k = all_tcs.len();
    let mut cols: Vec<ColumnDef> = Vec::with_capacity(k + left.columns.len() + right.columns.len());
    for (i, &t) in all_tcs.iter().enumerate() {
        cols.push(ColumnDef::new(format!("_join_pk_{i}"), t, false));
    }
    cols.extend(left.columns.iter().cloned());
    cols.extend(right.columns.iter().cloned());
    Schema {
        columns: cols,
        pk_cols: (0..k).collect(),
    }
}

/// Union the pure-range NULL-range-key rows into `nf_match` (`A − matched`):
/// NULL-key rows never reach the integrated trace (3VL) and never match the
/// threshold, so they get their own branch off the NULL-gate-unfiltered
/// `source`, re-keyed to the preserved side's source PK and routed ONCE by a
/// local `partition_filter` (no exchange) — broadcast would emit W× copies the
/// output shard would sum. (The compiler makes the filter a keep-all identity
/// for an all-replicated view, which runs correct-local over the full broadcast
/// on every worker.) `source` is the caller's semantic preserved input (the raw
/// input for the LEFT join; the locally pre-filtered outer for EXISTS).
pub(crate) fn union_null_key_rows(
    cb: &mut CircuitBuilder,
    nf_match: gnitz_core::NodeId,
    source: gnitz_core::NodeId,
    cols: &[usize],
    schema: &Schema,
) -> Result<gnitz_core::NodeId, GnitzSqlError> {
    let anull = cb.filter(source, Some(multi_null_filter_prog(cols, schema, true)?));
    let zero = vec![0u8; schema.pk_cols.len()];
    let anull_keyed = cb.map_reindex(anull, &schema.pk_cols, &zero, build_reindex_program(schema));
    let anull_owned = cb.partition_filter(anull_keyed);
    Ok(cb.union(nf_match, anull_owned))
}

/// Whether a user projection is the identity over the `n` combined payload
/// columns sitting at `offset` in the layout its final `Map` reads — the shared
/// skip-the-map contract of the join and EXISTS emit tails.
pub(crate) fn is_identity_projection(proj: &[usize], n: usize, offset: usize) -> bool {
    proj.len() == n && proj.iter().enumerate().all(|(i, &p)| p == i + offset)
}

/// Build a join view's user projection: the leading PK columns followed by the
/// selected payload columns, plus the parallel union-column index list the
/// output `Map` projects. Shared by the equi (`emit_join`) and
/// range (`emit_range_join`) builders, which differ only in their PK region
/// and where the A‖B payload columns sit in the union layout:
///   - `leading_cols`: the synthesized PK columns, cloned verbatim into the output
///     schema (`_join_pk` slots for equi, `_pair_pk` slots for range).
///   - `coldef(i)`: maps a combined A‖B column index (`0..n_combined`) to its
///     output `ColumnDef` (equi and range read the outer-nullability-adjusted
///     `combined_payload_coldefs` list; EXISTS reads the outer schema).
///   - `payload_offset`: the union-column index of combined column 0 in the
///     layout the caller's final `Map` reads.
///
/// A lone `SELECT *` flows through the `Wildcard` arm, so no caller needs a
/// separate wildcard fast path. `label` names the view kind in error messages.
/// The EXISTS/IN builder also calls it, with an alias map holding only the
/// outer relation (inner columns are then unresolvable — correct SQL scoping).
pub(crate) fn build_join_view_projection(
    projection: &[SelectItem],
    alias_map: &AliasMap,
    leading_cols: &[ColumnDef],
    n_combined: usize,
    payload_offset: usize,
    coldef: impl Fn(usize) -> ColumnDef,
    label: &str,
) -> Result<(Vec<ColumnDef>, Vec<usize>), GnitzSqlError> {
    let mut cols: Vec<ColumnDef> = leading_cols.to_vec();
    let mut proj: Vec<usize> = Vec::new();
    for item in projection {
        if matches!(item, SelectItem::Wildcard(_)) {
            // Visible combined columns only: a `SELECT *` join must not
            // re-admit an upstream source's hidden key (e.g. joining over a
            // view whose PK is a synthetic `_join_pk`) into this view's
            // payload.
            for i in 0..n_combined {
                let cd = coldef(i);
                if cd.is_hidden {
                    continue;
                }
                cols.push(cd);
                proj.push(payload_offset + i);
            }
            continue;
        }
        // Resolve the item's column reference once; an alias only renames the
        // resolved output column afterwards.
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            _ => {
                return Err(GnitzSqlError::Unsupported(format!(
                    "unsupported SELECT item in {label}"
                )))
            }
        };
        let idx = match expr {
            Expr::Identifier(ident) => resolve_unqualified_column(&ident.value, alias_map)?,
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                resolve_qualified_column(&parts[0].value, &parts[1].value, alias_map)?
            }
            _ if alias.is_some() => {
                return Err(GnitzSqlError::Unsupported(format!(
                    "{label}: only column references supported in AS clause"
                )))
            }
            _ => {
                return Err(GnitzSqlError::Unsupported(format!(
                    "unsupported SELECT item in {label}"
                )))
            }
        };
        let mut col = coldef(idx);
        if let Some(alias) = alias {
            col.name = alias.value.clone();
        }
        cols.push(col);
        proj.push(payload_offset + idx);
    }
    Ok((cols, proj))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> ColumnDef {
        ColumnDef::new(name, TypeCode::I64, false)
    }
    fn sch(names: &[&str]) -> Rc<Schema> {
        Rc::new(Schema {
            columns: names.iter().map(|n| col(n)).collect(),
            pk_cols: Vec::new(),
        })
    }
    fn rel(tid: u64, schema: Rc<Schema>, col_offset: usize) -> ResolvedRelation {
        ResolvedRelation {
            table_id: tid,
            schema,
            col_offset,
        }
    }

    /// Chain regression: a 3-way chain's left accumulator carries several original
    /// aliases at distinct offsets, so `prune_alias_map` must recompute each alias's
    /// offset from the count of kept columns before its span — not collapse all
    /// left-side aliases to `{0, pl}`, which would silently alias `a.x` and `b.m`
    /// onto the same physical column.
    #[test]
    fn prune_alias_map_recomputes_per_alias_offsets() {
        // Accumulator layout: [_join_pk(0), a.x(1), b.m(2), b.n(3)] (left_n = 4);
        // right relation c.z at combined index 4.
        let mut am = AliasMap::new();
        am.insert("a".to_string(), rel(10, sch(&["x"]), 1));
        am.insert("b".to_string(), rel(10, sch(&["m", "n"]), 2));
        am.insert("c".to_string(), rel(20, sch(&["z"]), 4));
        let left_n = 4;
        // Kept: a.x(1), b.m(2), c.z(4). Dropped: _join_pk(0), b.n(3).
        let keep = vec![false, true, true, false, true];
        let pl = keep[..left_n].iter().filter(|&&b| b).count(); // 2
        let pruned = prune_alias_map(&am, &keep, left_n);

        // Each surviving column resolves to its physical index in the pruned layout
        // [a.x, b.m, c.z] behind the k PK slots.
        assert_eq!(resolve_qualified_column("a", "x", &pruned).unwrap(), 0);
        assert_eq!(resolve_qualified_column("b", "m", &pruned).unwrap(), 1);
        assert_eq!(resolve_qualified_column("c", "z", &pruned).unwrap(), pl); // 2
                                                                              // The dropped column is absent from its alias's pruned schema.
        assert!(resolve_qualified_column("b", "n", &pruned).is_err());
    }

    /// A 2-way join degenerates to the `{0, pl}` layout: left alias at 0, right at
    /// `pl`. With a fully-kept left and a partly-pruned right, offsets and pruned
    /// schemas track the kept columns.
    #[test]
    fn prune_alias_map_two_way_offsets() {
        // left a=[p, q] (offsets 0,1); right b=[r, s, t] (offsets 2,3,4). left_n = 2.
        let mut am = AliasMap::new();
        am.insert("a".to_string(), rel(1, sch(&["p", "q"]), 0));
        am.insert("b".to_string(), rel(2, sch(&["r", "s", "t"]), 2));
        let left_n = 2;
        // Keep all of a; from b keep only s (idx 3). Dropped: r(2), t(4).
        let keep = vec![true, true, false, true, false];
        let pl = 2;
        let pruned = prune_alias_map(&am, &keep, left_n);
        assert_eq!(resolve_qualified_column("a", "p", &pruned).unwrap(), 0);
        assert_eq!(resolve_qualified_column("a", "q", &pruned).unwrap(), 1);
        assert_eq!(resolve_qualified_column("b", "s", &pruned).unwrap(), pl); // 2
        assert!(resolve_qualified_column("b", "r", &pruned).is_err());
        assert!(resolve_qualified_column("b", "t", &pruned).is_err());
    }
}
