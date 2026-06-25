//! Equi-join and range/band-join view compilation, plus the shared join-output
//! projection helper. Both builders reindex each side onto the join key, run the
//! symmetric 2-term DBSP join against the other side's trace, normalize the two
//! terms onto `[A cols, B cols]`, and (for LEFT joins) attach the null-fill.

use crate::ast_util::{extract_table_factor_name, is_wildcard_projection};
use crate::bind::{resolve_qualified_column, resolve_unqualified_column, AliasMap, Binder, ResolvedRelation};
use crate::error::GnitzSqlError;
use crate::plan::validate::{reject_duplicate_column_names, reject_unhonored_select_clauses, HonoredClauses};
use crate::plan::view::predicates::{
    build_reindex_program, build_residual_filter_prog, converse_rel, extract_join_predicates, multi_null_filter_prog,
    pure_range_m_output_cols, RangeConjunct,
};
use crate::SqlResult;
use gnitz_core::{
    CircuitBuilder, ColumnDef, ExprBuilder, FixedInt, GnitzClient, RangeRel, Schema, TypeCode, AGG_MAX, AGG_MIN,
};
use sqlparser::ast::{Expr, JoinConstraint, JoinOperator, SelectItem, TableFactor};
use std::collections::HashMap;
use std::rc::Rc;

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

    // Determine join type
    let (on_expr, is_left_join) = match &join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr)) | JoinOperator::Join(JoinConstraint::On(expr)) => (expr, false),
        JoinOperator::LeftOuter(JoinConstraint::On(expr)) | JoinOperator::Left(JoinConstraint::On(expr)) => {
            (expr, true)
        }
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "CREATE VIEW: only INNER JOIN / LEFT JOIN ... ON supported".to_string(),
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

    // LEFT/OUTER + residual is rejected (§3): for an outer join the ON predicate is
    // part of the *match* condition — a preserved-side row whose only physical
    // matches all fail the residual must still null-fill, but two of the three
    // null-fill constructions (equi, pure-range) are keyed independently of the
    // inner output and so would not retro-null-fill. A consistent boundary beats an
    // inconsistent partial one; INNER residuals are fully supported.
    if is_left_join && !residual.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "LEFT/OUTER JOIN with a residual ON predicate (a non-equi/non-range \
             conjunct, or a second range conjunct) is not supported; the residual \
             would have to participate in the outer null-fill. Use INNER JOIN, or \
             move the predicate to a WHERE over a wrapping view."
                .into(),
        ));
    }

    // Range (band) join: one range conjunct (optionally behind equality
    // conjuncts) routes to the broadcast / re-key / output-exchange circuit.
    if let Some(range) = range_conjunct {
        // Band (n_eq >= 1) LEFT null-fill is `A − distinct(π_A(inner))`; `distinct` clamps
        // each matched left identity to weight 1, so it is weight-exact only when the
        // preserved (left) side's identities are unique — true for a unique_pk base table,
        // but a view can be bag-valued (e.g. a UNION ALL dropping the PK), leaking a spurious
        // null-fill. `resolve_table_id` hits TABLE_TAB only, so a view/CTE misses it (the same
        // discriminator `resolve_base_table` uses). Pure-range LEFT (n_eq == 0) is unaffected.
        let n_eq = left_join_cols.len();
        if is_left_join && n_eq >= 1 && client.resolve_table_id(schema_name, &left_name).is_err() {
            return Err(GnitzSqlError::Unsupported(
                "band LEFT JOIN does not currently support a non-base-table preserved \
                 (left) side (e.g. a view); its null-fill clamps matched multiplicity to \
                 1, over-filling a bag-valued input — use INNER JOIN or a base table on \
                 the left"
                    .into(),
            ));
        }
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
            is_left_join,
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

    // Build the reindex ExprProgram for each side.
    // Each side: COPY_COL all columns as payload, reindex by join key column.
    let left_reindex_prog = build_reindex_program(&left_schema);
    let right_reindex_prog = build_reindex_program(&right_schema);

    // Build circuit
    let left_n = left_schema.columns.len();
    let right_n = right_schema.columns.len();

    // Right-side type codes for null-extend (used by LEFT-join null fills).
    let right_col_tcs: Vec<u64> = right_schema.columns.iter().map(|c| c.type_code as u64).collect();

    // A NULL equi-join key must match nothing (SQL 3VL: NULL = anything, including
    // NULL = NULL, is unknown). map_reindex would promote a NULL integer key to
    // synthetic PK 0 and a NULL string to the empty-content hash 0, colliding with
    // a real 0/"" key and with every other NULL. Gate NULL keys out of the match.
    // A NOT NULL key leaves its side untouched (no filter node) — byte-identical to
    // the original plan, so the common case has zero overhead.
    let left_key_nullable = left_join_cols.iter().any(|&c| left_schema.columns[c].is_nullable);
    let right_key_nullable = right_join_cols.iter().any(|&c| right_schema.columns[c].is_nullable);

    let mut cb = CircuitBuilder::new(view_id, 0); // no single primary source
    let input_a = cb.input_delta_tagged(left_tid);
    let input_b = cb.input_delta_tagged(right_tid);

    // Inner (right) side: a key with any NULL component can never match — drop it.
    let input_b = if right_key_nullable {
        cb.filter(
            input_b,
            Some(multi_null_filter_prog(&right_join_cols, &right_schema, false)?),
        )
    } else {
        input_b
    };
    let reindex_b = cb.map_reindex(input_b, &right_join_cols, &right_target_tcs, right_reindex_prog);

    // Preserved (left) side.
    //   INNER join: a left row with any NULL key component matches nothing — drop it.
    //   LEFT  join: such a row must still be emitted with NULL right columns but must
    //               bypass the match (else it collides with a right 0/"" key and
    //               pollutes trace_a, corrupting join_ba and the ΔB null-fill
    //               correction). Split it out, reindex it to the synthetic join PK
    //               (so it is layout-compatible with the join output — a bare Filter
    //               would keep the left table's native PK stride and corrupt the
    //               downstream Union, which merges by raw PK bytes into one stride),
    //               null-extend it, and union it into the unmatched-left stream.
    //               Its NULL components pack to 0 bytes, which is harmless: the source
    //               columns are among the copied payload and compare_rows treats
    //               NULL ≠ 0, so it never merges with a real-0-key row.
    let (input_a_match, left_null_filled) = if !left_key_nullable {
        (input_a, None)
    } else {
        let left_not_null = cb.filter(
            input_a,
            Some(multi_null_filter_prog(&left_join_cols, &left_schema, false)?),
        );
        if !is_left_join {
            (left_not_null, None)
        } else {
            let left_null = cb.filter(
                input_a,
                Some(multi_null_filter_prog(&left_join_cols, &left_schema, true)?),
            );
            // MUST use the same left_target_tcs and reindex program as reindex_a,
            // else this branch reindexes at the source width and the downstream
            // UNION merges two different `_join_pk` strides.
            let left_null_ri = cb.map_reindex(left_null, &left_join_cols, &left_target_tcs, left_reindex_prog.clone());
            (left_not_null, Some(cb.null_extend(left_null_ri, &right_col_tcs)))
        }
    };
    let reindex_a = cb.map_reindex(input_a_match, &left_join_cols, &left_target_tcs, left_reindex_prog);

    let trace_a = cb.integrate_trace(reindex_a);
    let trace_b = cb.integrate_trace(reindex_b);
    let join_ab = cb.join_with_trace_node(reindex_a, trace_b); // ΔA ⋈ z^{-1}(I(B))
    let join_ba = cb.join_with_trace_node(reindex_b, trace_a); // ΔB ⋈ z^{-1}(I(A))

    // Normalize both terms onto [PK cols, A cols, B cols] and union them.
    let inner_merged = normalize_to_ab(&mut cb, join_ab, join_ba, k, left_n, right_n);

    let merged = if is_left_join {
        // Decomposed LEFT OUTER JOIN: inner ∪ (anti_join × null_right) ∪ null-key bypass.
        // This handles both ΔA and ΔB correctly.

        // Key-only B: strip payload, keep only join key PK for distinct tracking
        let key_only_b = cb.map_key_only(reindex_b);
        let distinct_b = cb.distinct(key_only_b);
        let trace_db = cb.integrate_trace(distinct_b);

        // ΔA null-fill path: left rows whose join key has no match in I(distinct(B))
        let antijoin_a = cb.anti_join_with_trace_node(reindex_a, trace_db);
        let null_filled_a = cb.null_extend(antijoin_a, &right_col_tcs);

        // ΔB correction path: when B key appears/disappears, adjust null-fills
        // join_dt(distinct_b, trace_a) gives (key, A_payload) for affected left rows
        // distinct_b emits +1 when key appears → negate → -1 → retract null-fill
        // distinct_b emits -1 when key disappears → negate → +1 → emit null-fill
        let correction_raw = cb.join_with_trace_node(distinct_b, trace_a);
        let correction = cb.negate(correction_raw);
        let null_filled_correction = cb.null_extend(correction, &right_col_tcs);

        let mut all_null_fills = cb.union(null_filled_a, null_filled_correction);
        // NULL-key left rows bypass the match entirely (SQL 3VL) but are still
        // emitted once as (left payload, NULL right) via this reindexed branch.
        if let Some(f) = left_null_filled {
            all_null_fills = cb.union(all_null_fills, f);
        }
        cb.union(inner_merged, all_null_fills)
    } else {
        inner_merged
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
    for col in &left_schema.columns {
        out_cols.push(col.clone());
    }
    for col in &right_schema.columns {
        let mut c = col.clone();
        if is_left_join {
            c.is_nullable = true;
        }
        out_cols.push(c);
    }

    // Residual ON predicates: splice one linear Filter over the normalized join
    // output `merged` ([_join_pk × k, A, B]) before projection/sink. A residual ⇒
    // INNER (LEFT was rejected above), so `merged == inner_merged` and `out_cols`
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
/// `is_left_join` selects INNER vs LEFT OUTER. INNER emits only the matched pairs.
/// LEFT additionally emits one `(a, NULL)` row per left row with no range match,
/// computed two ways by `n_eq`:
///   - **Band (`n_eq ≥ 1`):** the Z-set difference `A − distinct(π_A(inner))` (the
///     matched left rows are read straight off the inner output) — partition-local
///     on the eq-scatter.
///   - **Pure range (`n_eq == 0`):** match existence collapses to a threshold test
///     `a.x OP MAX/MIN(b.range_col)`, so the null-fill is the subtraction
///     `A − (A ⋈ {m})` against the one-row threshold `m`, computed by an INLINE
///     shard-free reduce over the broadcast ΔB — no catalog helpers, no sentinel,
///     no `distinct`.
///
/// Both ride the single pair-PK output exchange.
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
    is_left_join: bool,
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

    // Pure-range LEFT: the inline threshold null-fill reduces the range column with
    // MIN/MAX (an 8-byte accumulator), then reindexes the result onto the range slot
    // type. MIN/MAX preserves the source integer type, so any ≤8-byte integer range
    // column yields a result the reindex consumes directly. A 16-byte U128/UUID/I128
    // range column has no 8-byte accumulator. Reject up front rather than failing the
    // compile. (Band LEFT, n_eq ≥ 1, uses the cap-free `A − distinct(π_A(inner))`
    // set-difference null-fill instead, so it has no range-type restriction.)
    if is_left_join && n_eq == 0 {
        let range_tc = range.tc;
        if FixedInt::from_type_code(range_tc).is_none() {
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
    let input_b = cb.input_delta_tagged(right_tid);

    // NULL exclusion (SQL 3VL) over ALL key cols (eq + range) when any is nullable.
    // The inner match drops NULL-key rows on both sides. For a LEFT join the
    // unfiltered left input is kept as `input_a_raw`: the null-fill taps it so a
    // left row with a NULL eq/range column (never an inner match, never in `D`) is
    // still null-filled (§4). `input_b` keeps its plain filter — an unmatched B row
    // never reaches the inner output, hence never `D`.
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
    let input_b = if right_key_nullable {
        cb.filter(
            input_b,
            Some(multi_null_filter_prog(&right_reindex_cols, right_schema, false)?),
        )
    } else {
        input_b
    };

    let reindex_a = cb.map_reindex(
        input_a,
        &left_reindex_cols,
        &left_target_tcs,
        build_reindex_program(left_schema),
    );
    let reindex_b = cb.map_reindex(
        input_b,
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
    // LEFT join: every B column can be NULL in a null-fill row → mark it nullable,
    // or a client decoding a NULL in a NOT NULL column panics (matches the equi path).
    let combined_coldef = |idx: usize| -> ColumnDef {
        if idx < left_n {
            left_schema.columns[idx].clone()
        } else {
            let mut c = right_schema.columns[idx - left_n].clone();
            if is_left_join {
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

    // ---- LEFT null-fill ----
    // Both forms produce `nf_keyed`, the null-fill source keyed by the preserved-row
    // identity `[a.pk…, A]` (the IDENTICAL encoding `a_all` uses), so the shared tail
    // below can `null_extend` → re-key onto the pair-PK → project, riding the existing
    // pair-PK output exchange with the inner pairs (no extra exchange).
    let sink_input = if !is_left_join {
        projected
    } else {
        let zero_a = vec![0u8; pa];

        let nf_keyed = if n_eq == 0 {
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
            let red = cb.reduce_multi_local(mbh, &[], &[(agg_func, 1)]); // [_group_pk:U128, m:Tc]
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
            if left_key_nullable {
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
            }
        } else {
            // ── Band set-difference form (plan §3) ──────────────────────────────
            // nf = A − distinct(π_A(inner)). The matched left rows (with full A
            // payload) are read straight off the inner output, so this is a pure
            // Z-set difference; the only non-linear node is the `distinct`, which
            // absorbs the within-epoch ΔA/Δmatched simultaneity. Every node is
            // co-located on the eq-prefix worker (a.pk unique → never spans workers).
            //
            // A reindex Map's OUTPUT schema is always [new-PK, <every input column>]
            // (`reindex_output_schema`), so the only clean [a.pk, A] is re-key (copy
            // the whole union payload, mirroring the inner rekey) THEN project the A
            // region. A's PK cols are the leading `pa` slots of `pair_pk_cols`.
            let rekey_a = cb.map_reindex(
                merged,
                &pair_pk_cols[..pa],
                &zero_a,
                build_reindex_program(&union_schema),
            );
            let a_cols: Vec<usize> = (pa + k..pa + k + left_n).collect();
            let proj_a = cb.map(rekey_a, &a_cols); // [a.pk, A]
            let matched = cb.distinct(proj_a); // partition-local distinct (§3)

            // A = every left row (incl. NULL-key rows the inner match filtered out),
            // re-keyed to a.pk with the IDENTICAL [a.pk, A] encoding as proj_a so a
            // matched row's +1 (A) and −1 (D) are byte-identical and cancel (§4, §8).
            let a_all = cb.map_reindex(
                input_a_raw,
                &left_schema.pk_cols,
                &zero_a,
                build_reindex_program(left_schema),
            );
            let neg = cb.negate(matched);
            cb.union(a_all, neg) // A − D, keyed [a.pk, A]
        };

        // Shared tail: attach the NULL B columns, then re-key onto the pair-PK.
        let right_col_tcs: Vec<u64> = right_schema.columns.iter().map(|c| c.type_code as u64).collect();
        let nullfill = cb.null_extend(nf_keyed, &right_col_tcs); // [a.pk×pa, A, NULL B]

        // Re-key nullfill ([a.pk×pa (PK), A, B]) onto the pair-PK. a.pk from the PK
        // region (0..pa); b.pk from the NULL B payload (pa + left_n + b_pk) → packs to
        // the synthetic 0 → null-fill PK = (a.pk, 0…). The reindex output schema is
        // fixed to [pair-PK, a.pk×pa, A, B]; copy ONLY the A and B columns at their
        // schema positions (src = dst = pa + ci) and leave the a.pk×pa payload slots
        // unwritten (0) — they are projected away by nf_proj.
        let mut nf_pair_pk_cols: Vec<usize> = (0..pa).collect();
        for &b_pk in &right_schema.pk_cols {
            nf_pair_pk_cols.push(pa + left_n + b_pk);
        }
        let mut eb = ExprBuilder::new();
        for ci in 0..left_n + right_n {
            let tc = combined_coldef(ci).type_code as u32;
            eb.copy_col(tc, (pa + ci) as u32, (pa + ci) as u32);
        }
        let nf_rekey = cb.map_reindex(nullfill, &nf_pair_pk_cols, &zero_tcs, eb.build(0));
        // nf_rekey payload = [a.pk×pa (0), A, B]; user cols sit `pa − k` past where the
        // inner rekey ([_join_pk × k, …]) puts them. Same user-column SET as the inner
        // (shift each inner payload index by pa − k), so both branches agree on final_cols.
        let nf_projection: Vec<usize> = final_projection.iter().map(|&idx| idx - k + pa).collect();
        let nf_proj = cb.map(nf_rekey, &nf_projection);

        cb.union(projected, nf_proj)
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
