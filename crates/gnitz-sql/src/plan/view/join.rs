//! Equi-join and range/band-join view compilation, plus the shared join-output
//! projection helper. Both builders reindex each side onto the join key, run the
//! symmetric 2-term DBSP join against the other side's trace, normalize the two
//! terms onto `[A cols, B cols]`, and (for outer joins) attach the null-fill.

use crate::ast_util::{extract_table_factor_name, is_wildcard_projection};
use crate::bind::{resolve_qualified_column, resolve_unqualified_column, AliasMap, Binder, ResolvedRelation};
use crate::error::GnitzSqlError;
use crate::plan::validate::{reject_duplicate_column_names, reject_unhonored_select_clauses, HonoredClauses};
use crate::plan::view::predicates::{
    build_reindex_program, build_residual_filter_prog, converse_rel, extract_join_predicates, multi_null_filter_prog,
    pure_range_m_output_cols, schema_type_codes, RangeConjunct,
};
use crate::SqlResult;
use gnitz_core::{
    CircuitBuilder, ColumnDef, ExprBuilder, FixedInt, GnitzClient, RangeRel, Schema, TypeCode, AGG_MAX, AGG_MIN,
};
use sqlparser::ast::{Expr, JoinConstraint, JoinOperator, SelectItem, TableFactor};
use std::collections::HashMap;
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

/// Per-side carried reindex target type for each join key slot: `T_i` only when
/// the side's own self-derived reindex output type differs from `T_i` (a
/// cross-width/cross-sign promotion on that side), else `0` (self-derive). The
/// `0` keeps same-type / U128-vs-UUID / string circuits byte-identical to the
/// pre-promotion serialization. The encode rule lives in `carried_reindex_tc`,
/// the round-trip inverse of the engine's `resolve_reindex_type`. Shared by the
/// equi builder (`slot_tcs = target_tcs`) and the range builder
/// (`slot_tcs = all_tcs`, the eq prefix plus the range slot).
fn side_target_tcs(cols: &[usize], schema: &Schema, slot_tcs: &[TypeCode]) -> Vec<u8> {
    cols.iter()
        .zip(slot_tcs)
        .map(|(&c, &t)| schema.columns[c].type_code.carried_reindex_tc(t))
        .collect()
}

/// Normalize the two per-term join outputs onto the canonical `[A cols, B cols]`
/// payload layout and union them. Term AB is already canonical
/// (`[_join_pk × k, A, B]`); term BA is `[_join_pk × k, B, A]` and is reordered.
/// Shared verbatim by the equi (`k = eq slots`) and range (`k = n_eq + 1`)
/// builders.
fn normalize_to_ab(
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

pub(crate) fn execute_create_join_view(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name: &str,
    sql_text: &str,
    select: &sqlparser::ast::Select,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // Join views consume only the FROM/ON join and the projection — not a top-level WHERE
    // (no builder reads it) nor GROUP BY/HAVING/PREWHERE/… Reject all of them so a dropped
    // clause is a clean error; a WHERE belongs over a wrapping view.
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: false,
            grouping: false,
            distinct: false,
        },
        "CREATE VIEW JOIN",
    )?;
    // Extract left table
    let left_name = extract_table_factor_name(&select.from[0].relation, "CREATE VIEW JOIN")?;
    let left_alias = match &select.from[0].relation {
        TableFactor::Table { alias: Some(a), .. } => a.name.value.clone(),
        _ => left_name.clone(),
    };

    // Only support one join
    if select.from[0].joins.len() != 1 {
        return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW: only single JOIN supported".to_string(),
        ));
    }
    let join = &select.from[0].joins[0];

    // Extract right table
    let right_name = extract_table_factor_name(&join.relation, "CREATE VIEW JOIN")?;
    let right_alias = match &join.relation {
        TableFactor::Table { alias: Some(a), .. } => a.name.value.clone(),
        _ => right_name.clone(),
    };

    // Determine join type. sqlparser 0.56 spells the bare/`OUTER` forms as separate
    // variants (`Left`/`LeftOuter`, `Right`/`RightOuter`); FULL has only `FullOuter`.
    let (on_expr, join_type) = match &join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr)) | JoinOperator::Join(JoinConstraint::On(expr)) => {
            (expr, JoinType::Inner)
        }
        JoinOperator::LeftOuter(JoinConstraint::On(expr)) | JoinOperator::Left(JoinConstraint::On(expr)) => {
            (expr, JoinType::Left)
        }
        JoinOperator::RightOuter(JoinConstraint::On(expr)) | JoinOperator::Right(JoinConstraint::On(expr)) => {
            (expr, JoinType::Right)
        }
        JoinOperator::FullOuter(JoinConstraint::On(expr)) => (expr, JoinType::Full),
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "CREATE VIEW: only INNER / LEFT / RIGHT / FULL JOIN ... ON supported".to_string(),
            ))
        }
    };

    // Resolve both tables
    let (left_tid, left_schema) = binder.resolve(client, &left_name)?;
    let (right_tid, right_schema) = binder.resolve(client, &right_name)?;

    // The 2-term DBSP join formula assumes the left and right input deltas are
    // never simultaneously active. A self-join feeds the same delta to both
    // sides in one epoch, dropping the bilinear ΔT⋈ΔT cross-term — silent data
    // loss whenever same-key rows arrive in one batch.
    if left_tid == right_tid {
        return Err(GnitzSqlError::Unsupported(
            "self-join (joining a table with itself) is not supported".into(),
        ));
    }

    // Build alias map for qualified column resolution
    let mut alias_map: AliasMap = HashMap::new();
    alias_map.insert(
        left_alias.to_lowercase(),
        ResolvedRelation {
            table_id: left_tid,
            schema: Rc::clone(&left_schema),
            col_offset: 0,
        },
    );
    alias_map.insert(
        right_alias.to_lowercase(),
        ResolvedRelation {
            table_id: right_tid,
            schema: Rc::clone(&right_schema),
            col_offset: left_schema.columns.len(),
        },
    );

    // Classify the ON clause: equality prefix pairs, an optional range conjunct,
    // and the residual list (conjuncts the physical join cannot consume directly).
    let (left_join_cols, right_join_cols, target_tcs, range_conjunct, residual) =
        extract_join_predicates(on_expr, &left_schema, &right_schema, &alias_map)?;

    // Outer (LEFT/RIGHT/FULL) + residual is rejected (§3): for an outer join the ON
    // predicate is part of the *match* condition — a preserved-side row whose only
    // physical matches all fail the residual must still null-fill, but the null-fill
    // decides match existence from the inner output (or the MAX/MIN threshold
    // witness), independently of the residual, and so would not retro-null-fill a row
    // matched only by residual-failing pairs. A consistent boundary across all join
    // shapes beats an inconsistent partial one; INNER residuals are fully supported.
    if join_type != JoinType::Inner && !residual.is_empty() {
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
    if let Some(range) = range_conjunct {
        return build_range_join_view(
            client,
            schema_name,
            view_name,
            sql_text,
            select,
            &alias_map,
            left_tid,
            right_tid,
            &left_schema,
            &right_schema,
            &left_join_cols,
            &right_join_cols,
            &target_tcs,
            range,
            join_type,
            &residual,
        );
    }

    // Equi-join path (zero range conjuncts) — byte-identical to before. The
    // reindex-slot arity cap was already enforced in extract_join_predicates.
    let k = left_join_cols.len(); // == right_join_cols.len(), 1..=PK_LIST_MAX_COLS

    // Per-side carried target tc for each key pair (0 = self-derive); see
    // `side_target_tcs`.
    let left_target_tcs = side_target_tcs(&left_join_cols, &left_schema, &target_tcs);
    let right_target_tcs = side_target_tcs(&right_join_cols, &right_schema, &target_tcs);

    // The merged join output is [k _join_pk cols, left cols..., right cols...].
    // Reject before the server's schema build hits its hard column-count assertion.
    let combined_cols = k + left_schema.columns.len() + right_schema.columns.len();
    if combined_cols > gnitz_core::MAX_COLUMNS {
        return Err(GnitzSqlError::Unsupported(format!(
            "JOIN view output has {} columns, exceeding the {}-column limit",
            combined_cols,
            gnitz_core::MAX_COLUMNS,
        )));
    }

    // Allocate view_id
    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;

    // Build circuit. Each side's reindex program copies all columns as payload and
    // reindexes by the join key; it is a pure function of the schema, so it is built
    // fresh at each `map_reindex` site (the inner reindex and, for an outer null-fill,
    // the `a_all`/`b_all` re-key of the unfiltered input).
    let left_n = left_schema.columns.len();
    let right_n = right_schema.columns.len();

    // A NULL equi-join key must match nothing (SQL 3VL: NULL = anything, including
    // NULL = NULL, is unknown). map_reindex would promote a NULL integer key to
    // synthetic PK 0 and a NULL string to the empty-content hash 0, colliding with
    // a real 0/"" key and with every other NULL. Gate NULL keys out of the match.
    // A NOT NULL key leaves its side untouched (no filter node) — byte-identical to
    // the original plan, so the common case has zero overhead.
    let left_key_nullable = left_join_cols.iter().any(|&c| left_schema.columns[c].is_nullable);
    let right_key_nullable = right_join_cols.iter().any(|&c| right_schema.columns[c].is_nullable);

    let mut cb = CircuitBuilder::new(view_id, 0); // no single primary source
    let input_a_raw = cb.input_delta_tagged(left_tid);
    let input_b_raw = cb.input_delta_tagged(right_tid);

    // Inner (right) side: a key with any NULL component can never match — drop it.
    // Keep the unfiltered `input_b_raw` so a RIGHT/FULL null-fill's `b_all` can re-key
    // NULL-keyed right rows too — the mirror of the left side's `input_a_raw`. For a
    // non-nullable right key the two coincide and `b_all` reuses `reindex_b` directly.
    let input_b_match = if right_key_nullable {
        cb.filter(
            input_b_raw,
            Some(multi_null_filter_prog(&right_join_cols, &right_schema, false)?),
        )
    } else {
        input_b_raw
    };
    let reindex_b = cb.map_reindex(
        input_b_match,
        &right_join_cols,
        &right_target_tcs,
        build_reindex_program(&right_schema),
    );

    // Preserved (left) side. A left row with any NULL key component matches nothing
    // (SQL 3VL), so it is dropped from the inner match on every join kind — it would
    // otherwise collide with a real 0/"" key and pollute `trace_a`. For a LEFT/FULL
    // join such a row is still null-filled, but via the `a_all` term below (which
    // re-keys the FULL `input_a_raw`, NULL-keyed rows included), not a dedicated
    // bypass: a NULL-keyed row is never in `inner`, so `π_A(inner)` is 0 for it and
    // `positive_part(A − π_A(inner)) = w_A` null-fills it at full multiplicity.
    let input_a_match = if left_key_nullable {
        cb.filter(
            input_a_raw,
            Some(multi_null_filter_prog(&left_join_cols, &left_schema, false)?),
        )
    } else {
        input_a_raw
    };
    let reindex_a = cb.map_reindex(
        input_a_match,
        &left_join_cols,
        &left_target_tcs,
        build_reindex_program(&left_schema),
    );

    let trace_a = cb.integrate_trace(reindex_a);
    let trace_b = cb.integrate_trace(reindex_b);
    let join_ab = cb.join_with_trace_node(reindex_a, trace_b); // ΔA ⋈ z^{-1}(I(B))
    let join_ba = cb.join_with_trace_node(reindex_b, trace_a); // ΔB ⋈ z^{-1}(I(A))

    // Normalize both terms onto [PK cols, A cols, B cols] and union them.
    let inner_merged = normalize_to_ab(&mut cb, join_ab, join_ba, k, left_n, right_n);

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
                let reorder: Vec<usize> = (k + right_n..k + right_n + left_n).chain(k..k + right_n).collect();
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
                    build_reindex_program(&left_schema),
                )
            } else {
                reindex_a
            };
            let nf_a = equi_nf(&mut cb, a_all, k, left_n, &schema_type_codes(&right_schema), true);
            merged = cb.union(nf_a, merged);
        }
        // ν_B (preserved right): `b_all` re-keys the unfiltered right input; B sits at
        // `k + left_n` in `inner_merged`.
        if join_type.preserves_right() {
            let b_all = if right_key_nullable {
                cb.map_reindex(
                    input_b_raw,
                    &right_join_cols,
                    &right_target_tcs,
                    build_reindex_program(&right_schema),
                )
            } else {
                reindex_b
            };
            let nf_b = equi_nf(
                &mut cb,
                b_all,
                k + left_n,
                right_n,
                &schema_type_codes(&left_schema),
                false,
            );
            merged = cb.union(nf_b, merged);
        }
        merged
    };

    // Build virtual combined output schema: k synthetic join PK cols + all left cols
    // + all right cols. After proj_ab/proj_ba, the UNION output has this layout (union
    // col indices):
    //   col 0..k: _join_pk[_i] (PK region, one slot per key pair)
    //   col k..k+left_n: all A columns (in A schema order)
    //   col k+left_n..k+left_n+right_n: all B columns (in B schema order)
    //
    // Each synthetic PK column's type is the pair's `join_key_common_type` `T_i`
    // (returned by validate_join_key_pair): the single persisted stride that both
    // sides' reindex Maps and every cross-process consumer re-derive. For a
    // same-type pair `T_i` equals the source's reindex_output_type, so the catalog
    // value is unchanged; for a cross-width pair it is the wider promoted type.
    //
    // The first key column keeps the name `_join_pk` at k = 1 (the catalog name is not
    // referenced by any code, but preserving it keeps the shippable single-key view
    // byte-identical); composite keys use `_join_pk_{i}`.
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    for (i, &t) in target_tcs.iter().enumerate() {
        let name = if k == 1 {
            "_join_pk".to_string()
        } else {
            format!("_join_pk_{i}")
        };
        // The pair's common type T_i — the single persisted stride both sides'
        // reindex Maps and every cross-process consumer re-derive.
        out_cols.push(ColumnDef::new(name, t, false));
    }
    // Output-column nullability: a row preserved on one side carries NULLs on the
    // OTHER side's columns, so left columns are nullable iff the RIGHT side is
    // preserved (Right|Full) and right columns iff the LEFT side is (Left|Full). The
    // `_join_pk` columns stay non-nullable (a NULL key packs to the synthetic 0,
    // never a NULL PK). A client decoding a NULL in a NOT NULL column would panic.
    for col in &left_schema.columns {
        let mut c = col.clone();
        if join_type.preserves_right() {
            c.is_nullable = true;
        }
        out_cols.push(c);
    }
    for col in &right_schema.columns {
        let mut c = col.clone();
        if join_type.preserves_left() {
            c.is_nullable = true;
        }
        out_cols.push(c);
    }

    // Residual ON predicates: splice one linear Filter over the normalized join
    // output `merged` ([_join_pk × k, A, B]) before projection/sink. A residual ⇒
    // INNER (outer was rejected above), so `merged == inner_merged` and `out_cols`
    // carries each base column's real (INNER) nullability — exactly the schema the
    // engine resolves the predicate against. The filter is linear (incrementally
    // free, no state), and INNER 3VL drops a NULL/UNKNOWN predicate row natively.
    let merged = if residual.is_empty() {
        merged
    } else {
        let merged_schema = Schema {
            columns: out_cols.clone(),
            pk_cols: (0..k).collect(),
        };
        let prog = build_residual_filter_prog(&residual, &alias_map, &merged_schema, k)?;
        cb.filter(merged, Some(prog))
    };

    // Compute the user projection + view schema via the shared join-projection
    // helper. A lone `SELECT *` flows through its Wildcard arm and stays identity
    // (the projection map is then skipped below), so no wildcard fast path here.
    let is_wildcard = is_wildcard_projection(&select.projection);
    let (final_cols, final_projection) = build_join_view_projection(
        &select.projection,
        &alias_map,
        &out_cols[..k],
        left_n + right_n,
        k,
        |idx| out_cols[k + idx].clone(),
        "JOIN view",
    )?;

    // Apply final column projection before sink when not identity.
    // Identity = selecting all left+right cols in canonical order [k..k+left_n+right_n].
    let is_identity =
        final_projection.len() == left_n + right_n && final_projection.iter().enumerate().all(|(i, &p)| p == i + k);
    let sink_input = if is_identity {
        merged
    } else {
        cb.map(merged, &final_projection)
    };
    cb.sink(sink_input);
    let circuit = cb.build();

    // The view's physical PK is the k synthetic `_join_pk` columns at slots 0..k
    // (final_cols lists them first). At k = 1 this is the existing single `[0]`.
    let view_pk: Vec<u32> = (0..k as u32).collect();
    // Reject duplicate output names from explicit user aliases (e.g.
    // `SELECT l.a AS x, r.b AS x`). A `SELECT *` join legitimately surfaces
    // same-named columns from both sides (both tables' `id`); that is the
    // established wildcard contract, so the guard applies only to explicit
    // projections.
    if !is_wildcard {
        reject_duplicate_column_names(final_cols.iter().map(|c| c.name.as_str()), "join view")?;
    }
    client
        .create_view_with_circuit(schema_name, view_name, sql_text, circuit, &final_cols, &view_pk)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
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
#[allow(clippy::too_many_arguments)]
fn build_range_join_view(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name: &str,
    sql_text: &str,
    select: &sqlparser::ast::Select,
    alias_map: &AliasMap,
    left_tid: u64,
    right_tid: u64,
    left_schema: &Schema,
    right_schema: &Schema,
    left_join_cols: &[usize],
    right_join_cols: &[usize],
    eq_tcs: &[TypeCode],
    range: RangeConjunct,
    join_type: JoinType,
    residual: &[Expr],
) -> Result<SqlResult, GnitzSqlError> {
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
    // every A and B column. Reject before the server's hard column-count assertion.
    let rekey_cols = pair_pk + k + left_n + right_n;
    if rekey_cols > gnitz_core::MAX_COLUMNS {
        return Err(GnitzSqlError::Unsupported(format!(
            "range JOIN view has {rekey_cols} intermediate columns, exceeding the \
             {}-column limit",
            gnitz_core::MAX_COLUMNS
        )));
    }

    // Reindex columns and per-pair common type T: eq pairs first, range slot last.
    let left_reindex_cols: Vec<usize> = left_join_cols
        .iter()
        .copied()
        .chain(std::iter::once(range.left_col))
        .collect();
    let right_reindex_cols: Vec<usize> = right_join_cols
        .iter()
        .copied()
        .chain(std::iter::once(range.right_col))
        .collect();
    let all_tcs: Vec<TypeCode> = eq_tcs.iter().copied().chain(std::iter::once(range.tc)).collect();

    // Per-side carried target tc per slot (0 = self-derive); see `side_target_tcs`.
    let left_target_tcs = side_target_tcs(&left_reindex_cols, left_schema, &all_tcs);
    let right_target_tcs = side_target_tcs(&right_reindex_cols, right_schema, &all_tcs);

    // §3 table: term AB ({y : x OP y}) wants trace y `converse(OP)` delta x; term
    // BA ({x : x OP y}) wants trace x `OP` delta y.
    let rel_ab = converse_rel(range.op);
    let rel_ba = range.op;

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

    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
    let mut cb = CircuitBuilder::new(view_id, 0);
    let input_a_raw = cb.input_delta_tagged(left_tid);
    let input_b_raw = cb.input_delta_tagged(right_tid);

    // NULL exclusion (SQL 3VL) over ALL key cols (eq + range) when any is nullable.
    // The inner match drops NULL-key rows on both sides. The unfiltered inputs are
    // kept (`input_a_raw` / `input_b_raw`): an outer null-fill taps the preserved
    // side's raw input so a preserved row with a NULL eq/range column (never an inner
    // match, never in `D`) is still null-filled (§4). The filtered nodes feed only the
    // inner match.
    let left_key_nullable = left_reindex_cols.iter().any(|&c| left_schema.columns[c].is_nullable);
    let right_key_nullable = right_reindex_cols.iter().any(|&c| right_schema.columns[c].is_nullable);
    let input_a = if left_key_nullable {
        cb.filter(
            input_a_raw,
            Some(multi_null_filter_prog(&left_reindex_cols, left_schema, false)?),
        )
    } else {
        input_a_raw
    };
    let input_b_match = if right_key_nullable {
        cb.filter(
            input_b_raw,
            Some(multi_null_filter_prog(&right_reindex_cols, right_schema, false)?),
        )
    } else {
        input_b_raw
    };

    let reindex_a = cb.map_reindex(
        input_a,
        &left_reindex_cols,
        &left_target_tcs,
        build_reindex_program(left_schema),
    );
    let reindex_b = cb.map_reindex(
        input_b_match,
        &right_reindex_cols,
        &right_target_tcs,
        build_reindex_program(right_schema),
    );

    // Band join (n_eq ≥ 1): the input relay scatters by the eq prefix, so each
    // side's trace is already eq-prefix-partitioned — integrate the scattered
    // reindex directly. Pure range join (n_eq == 0): the input is broadcast, so
    // each worker must drop the rows it does not own (PartitionFilter between
    // reindex and integrate) before integrating, or traces replicate and matches
    // duplicate. The join terms probe the UNFILTERED reindex either way — for a
    // band join that reindex IS the worker's scattered eq-prefix slice.
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
    let mut union_cols: Vec<ColumnDef> = Vec::with_capacity(k + left_n + right_n);
    for (i, &t) in all_tcs.iter().enumerate() {
        union_cols.push(ColumnDef::new(format!("_join_pk_{i}"), t, false));
    }
    for col in &left_schema.columns {
        union_cols.push(col.clone());
    }
    for col in &right_schema.columns {
        union_cols.push(col.clone());
    }
    let union_schema = Schema {
        columns: union_cols,
        pk_cols: (0..k).collect(),
    };

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
    // Output-column nullability mirrors the equi path: a row preserved on one side
    // carries NULLs on the OTHER side's columns. So a left column is nullable iff the
    // RIGHT side is preserved (Right|Full) and a right column iff the LEFT side is
    // (Left|Full), or a client decoding a NULL in a NOT NULL column panics.
    let combined_coldef = |idx: usize| -> ColumnDef {
        if idx < left_n {
            let mut c = left_schema.columns[idx].clone();
            if join_type.preserves_right() {
                c.is_nullable = true;
            }
            c
        } else {
            let mut c = right_schema.columns[idx - left_n].clone();
            if join_type.preserves_left() {
                c.is_nullable = true;
            }
            c
        }
    };

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
            ColumnDef::new(
                format!("_pair_pk_{slot}"),
                schema.columns[c].type_code.reindex_output_type(),
                false,
            )
        })
        .collect();

    // User projection via the shared helper. Always applied (it must drop the
    // `_join_pk` slots, which DIFFER per term); keeps the pair-PK region and
    // selects user columns from A/B as the payload.
    let is_wildcard = is_wildcard_projection(&select.projection);
    let (final_cols, final_projection) = build_join_view_projection(
        &select.projection,
        alias_map,
        &pair_pk_coldefs,
        left_n + right_n,
        payload_offset,
        combined_coldef,
        "range JOIN view",
    )?;

    let projected = cb.map(rekey, &final_projection);

    // ── Outer null-fill ─────────────────────────────────────────────────────────
    // Each branch produces `nf_keyed`, the null-fill keyed by the preserved-row
    // identity `[P.pk…, P]`, then runs it through the shared `nf_tail` (null-extend →
    // re-key onto the pair-PK → project), riding the inner pairs' output exchange.
    let sink_input = if join_type == JoinType::Inner {
        projected
    } else {
        // Shared tail: null-extend ν_P with the O-side NULL columns, re-key onto the
        // pair-PK `[a.pk…, b.pk…]` (the other side's PK rides in the NULL-O payload,
        // packing to the synthetic 0), and project onto final_cols. `preserved_is_left`
        // selects the canonical (P=A) vs reordered (P=B) layout: `null_extend` appends
        // and `map_reindex` locks its payload to input order, so only the final
        // `cb.map` (nf_proj) can place the NULL-O columns before P (the P=B case).
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
                // the [P, NULL-O] payload sits at pair_pk + p_pk. Map each user-selected
                // combined column (canonical [A, B] index) to its slot, canonicalizing P=B.
                let nf_projection: Vec<usize> = final_projection
                    .iter()
                    .map(|&idx| {
                        let ci = idx - payload_offset; // combined A‖B index
                        if preserved_is_left {
                            pair_pk + pa + ci // [A, B] contiguous after pair-PK + a.pk
                        } else if ci < left_n {
                            pair_pk + pb + right_n + ci // A column → trailing NULL-A region
                        } else {
                            pair_pk + pb + (ci - left_n) // B column → leading B region
                        }
                    })
                    .collect();
                cb.map(nf_rekey, &nf_projection)
            };

        if n_eq == 0 {
            let zero_a = vec![0u8; pa];
            // ── Pure-range threshold SUBTRACTION form (plan §1–§3, §6) ──────────
            // Unmatched left rows are `A − matched`, where `matched = {a : a.x OP m}`
            // against the ONE-ROW threshold `m = MAX/MIN(b.range_col)`. The reduce
            // computing `m` is INLINED here (no `__m`/`__sent` catalog helpers).
            // Subtracting (rather than emitting the complement directly) makes an
            // empty `trace_m` mean "every left row null-fills" for free —
            // `A − ∅ = A` — so empty/all-NULL `b` needs no sentinel; and because
            // `matched = A ⋈ {m}` carries each `a`'s true weight (one-row `m` cannot
            // multiply, no `distinct`), it is weight-exact for a bag-valued left
            // input too.
            let range_tc = range.tc;
            let want_max = matches!(range.op, RangeRel::Lt | RangeRel::Le);
            let agg_func = if want_max { AGG_MAX } else { AGG_MIN };
            let m_schema = Schema {
                columns: pure_range_m_output_cols(range_tc),
                pk_cols: vec![0],
            };

            // Inline `m = MAX/MIN(b.range_col)`, computed LOCALLY on every worker
            // over the broadcast `reindex_b` (NOT the partition-filtered `int_b`): a
            // global extremum over a fully-broadcast input is identical on every
            // worker, so no scatter/gather is needed (plan §1A). `map_hash_row`
            // moves `reindex_b`'s `Tc` PK into a payload column, DECODING OPK →
            // native AND carrying the `Tc` type verbatim, so the reduce aggregates
            // the native value (col 1), not the OPK bytes (which would invert signed
            // MIN/MAX), and — because non-float MIN/MAX now carries its source type
            // (`agg_output_type`) — emits its result already typed `Tc`. `reindex_m`
            // therefore self-derives the correct OPK order straight off the reduce,
            // with no relabel. `reduce_multi_local` is the shard-free reduce; its one
            // output row is replicated on EVERY worker by the n_eq==0 broadcast, so
            // `reindex_m` carries NO `partition_filter` (unlike `int_a`/`int_b`);
            // `trace_m` is the one-row trace `matched` probes.
            let mbh = cb.map_hash_row(reindex_b, &[0], 0);
            // `global_ground = false`: this empty-group reduce is the range-join's
            // threshold `m = MAX/MIN(b.range)`, NOT a user scalar aggregate. A
            // ground row here would seed `(m=NULL)` into the threshold trace and
            // break the `A − ∅ = A` null-fill that needs that trace empty over an
            // empty other side.
            let red = cb.reduce_multi_local(mbh, &[], &[(agg_func, 1)], false); // [_group_pk:U128, m:Tc]
            let carried_m = range_tc.carried_reindex_tc(range_tc);
            let reindex_m = cb.map_reindex(red, &[1], &[carried_m], build_reindex_program(&m_schema));
            let trace_m = cb.integrate_trace(reindex_m);

            // The two `matched` terms carry the MATCH op (= the inner op), mirroring
            // `join_ab`/`join_ba` EXACTLY (`rel_ab = converse(op)`, `rel_ba = op`)
            // with only the trace swapped to `trace_m`/`reindex_m` — no
            // `complement_rel`. The `a`-side term taps the filtered `int_a` against
            // the replicated `trace_m`; the `m`-side term taps the replicated
            // `reindex_m` against the filtered `trace_a`, so neither duplicates under
            // broadcast. All four range nodes carry n_eq == 0 — load-bearing for the
            // view-level `circuit_range_join_n_eq` discriminator, which reads an
            // ARBITRARY `DeltaTraceRange.n_eq` (HashMap order) and is correct only
            // because they all agree.
            let j_am = cb.join_with_trace_range_node(int_a, trace_m, 0, rel_ab);
            let j_ma = cb.join_with_trace_range_node(reindex_m, trace_a, 0, rel_ba);
            // Range-join output = [_join_pk × k, delta payload, trace payload]; project
            // each to the A columns. `j_am`: A is the delta payload at `k..k+left_n`.
            // `j_ma`: A is the trace payload, after Δm's payload (`m`'s 2 columns).
            let m_payload = m_schema.columns.len(); // m's output arity ([_group_pk, m])
            let m_am = cb.map(j_am, &(k..k + left_n).collect::<Vec<_>>());
            let m_ma = cb.map(j_ma, &(k + m_payload..k + m_payload + left_n).collect::<Vec<_>>());
            let matched_raw = cb.union(m_am, m_ma); // [_join_pk(PK), A]

            // Both `matched_raw` and the bare `int_a` passthrough are `[Tc PK, A]`
            // (a.pk lives INSIDE A, not in the PK — `int_a`'s PK is a.x reindexed to
            // `Tc`, identical in type to each range term's `_join_pk`). Re-key BOTH
            // onto `[a.pk…, A]` (band's `a_all` shape) with the IDENTICAL encoding —
            // reindex by a.pk's position within A (`1 + pk`, past the leading `Tc`
            // PK), then project the A region — so a matched `a`'s `+a` (passthrough)
            // and `−a` (matched, negated) are byte-identical and cancel in-epoch
            // (plan §2). The passthrough re-keys `int_a` (the broadcast ΔA minus its
            // non-owned / NULL-key rows), NOT `input_a_raw`: a pure-range relay
            // broadcasts `a`, so re-keying the raw input would emit `W×` copies the
            // pair-PK shard would sum (plan §2, unlike band's scattered `a_all`).
            let jp = ColumnDef::new("_jp", range_tc, false);
            let nf_raw_schema = Schema {
                columns: std::iter::once(jp).chain(left_schema.columns.iter().cloned()).collect(),
                pk_cols: vec![0],
            };
            let a_pk_in_raw: Vec<usize> = left_schema.pk_cols.iter().map(|&p| 1 + p).collect();
            let a_cols: Vec<usize> = (pa + 1..pa + 1 + left_n).collect();
            // One shared reindex program drives both re-keys, so the IDENTICAL
            // encoding above is structural — `matched` and `a_pass` differ only in
            // their input node.
            let nf_reindex_prog = build_reindex_program(&nf_raw_schema);
            let rekey_a = |cb: &mut CircuitBuilder, input: gnitz_core::NodeId| {
                let keyed = cb.map_reindex(input, &a_pk_in_raw, &zero_a, nf_reindex_prog.clone());
                cb.map(keyed, &a_cols) // [a.pk…, A]
            };
            let matched = rekey_a(&mut cb, matched_raw);
            let a_pass = rekey_a(&mut cb, int_a);
            let neg = cb.negate(matched);
            let nf_match = cb.union(a_pass, neg); // A − matched, keyed [a.pk…, A]

            // NULL-range-key rows are filtered out of `reindex_a`/`trace_a` (3VL) and
            // never match the threshold, so they need their own branch off the
            // UNFILTERED broadcast `input_a_raw`, re-keyed to a.pk (already the clean
            // `[a.pk…, A]` shape) and routed ONCE by a local `partition_filter` (no
            // exchange) — else broadcast yields W× copies the pair-PK shard would sum.
            let nf_keyed = if left_key_nullable {
                let anull = cb.filter(
                    input_a_raw,
                    Some(multi_null_filter_prog(&left_reindex_cols, left_schema, true)?),
                );
                let anull_keyed =
                    cb.map_reindex(anull, &left_schema.pk_cols, &zero_a, build_reindex_program(left_schema));
                let anull_owned = cb.partition_filter(anull_keyed);
                cb.union(nf_match, anull_owned)
            } else {
                nf_match
            };
            let branch = nf_tail(&mut cb, nf_keyed, true);
            cb.union(projected, branch)
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
            let zero_a = vec![0u8; pa];
            let zero_b = vec![0u8; pb];
            let mut sink = projected;
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
                sink = cb.union(sink, branch);
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
                sink = cb.union(sink, branch);
            }
            sink
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
    if !is_wildcard {
        reject_duplicate_column_names(final_cols.iter().map(|c| c.name.as_str()), "range join view")?;
    }
    client
        .create_view_with_circuit(schema_name, view_name, sql_text, circuit, &final_cols, &view_pk)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

/// Build a join view's user projection: the leading PK columns followed by the
/// selected payload columns, plus the parallel union-column index list the
/// output `Map` projects. Shared by the equi (`execute_create_join_view`) and
/// range (`build_range_join_view`) builders, which differ only in their PK region
/// and where the A‖B payload columns sit in the union layout:
///   - `leading_cols`: the synthesized PK columns, cloned verbatim into the output
///     schema (`_join_pk` slots for equi, `_pair_pk` slots for range).
///   - `coldef(i)`: maps a combined A‖B column index (`0..n_combined`) to its
///     output `ColumnDef` (equi reads the pre-built, LEFT-join-nullable-adjusted
///     list; range reads the raw source schemas — INNER only).
///   - `payload_offset`: the union-column index of combined column 0.
///
/// A lone `SELECT *` flows through the `Wildcard` arm, so neither caller needs a
/// separate wildcard fast path. `label` names the view kind in error messages.
fn build_join_view_projection(
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
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let idx = resolve_unqualified_column(&ident.value, alias_map)?;
                cols.push(coldef(idx));
                proj.push(payload_offset + idx);
            }
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                let idx = resolve_qualified_column(&parts[0].value, &parts[1].value, alias_map)?;
                cols.push(coldef(idx));
                proj.push(payload_offset + idx);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let idx = match expr {
                    Expr::Identifier(ident) => resolve_unqualified_column(&ident.value, alias_map)?,
                    Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                        resolve_qualified_column(&parts[0].value, &parts[1].value, alias_map)?
                    }
                    _ => {
                        return Err(GnitzSqlError::Unsupported(format!(
                            "{label}: only column references supported in AS clause"
                        )))
                    }
                };
                let mut col = coldef(idx);
                col.name = alias.value.clone();
                cols.push(col);
                proj.push(payload_offset + idx);
            }
            SelectItem::Wildcard(_) => {
                for i in 0..n_combined {
                    cols.push(coldef(i));
                    proj.push(payload_offset + i);
                }
            }
            _ => {
                return Err(GnitzSqlError::Unsupported(format!(
                    "unsupported SELECT item in {label}"
                )))
            }
        }
    }
    Ok((cols, proj))
}
