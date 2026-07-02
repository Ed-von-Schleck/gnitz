//! `[NOT] EXISTS (SELECT …)` / `x [NOT] IN (SELECT …)` view compilation: the
//! semi/anti-join as a circuit derivation of the shipped outer-join null-fill
//! machinery — no new engine operators, no new wire opcodes. The dispatcher
//! routes here when a Simple-shaped view WHERE carries exactly one top-level
//! subquery conjunct.
//!
//! Per preserved-row identity `x` (weight `w_A ≥ 0` in the outer input, summed
//! matched inner weight `S ≥ 0`), the anti-join is the weight-exact null-fill
//! set `ν(x) = positive_part(A − π_A(inner)) = w_A·[S=0]` and the semi-join its
//! complement `A − ν = w_A·[S>0]`. Both hold for bag-valued outer inputs by the
//! same clamp-the-result argument as the LEFT-join null-fill. The pure-range
//! correlation (`n_eq == 0`) instead collapses match existence to the one-row
//! threshold `m = MAX/MIN(inner.range)`: `matched = A ⋈ {m}` carries each row's
//! weight verbatim (no clamp needed), EXISTS = `matched`, NOT EXISTS =
//! `A − matched`.
//!
//! Divergences from Postgres, deliberate pre-alpha:
//!   - The EXISTS subquery's select list is ignored entirely (SQL defines
//!     EXISTS independent of it); it is never bound or name-checked.
//!   - Correlation names resolve through one flat two-relation scope (inner
//!     names do not shadow outer ones; a name in both relations is ambiguous).
//!
//! **Shape-dependent PK surface** (mirroring the join builders): an equi
//! correlation keys the view by the k synthetic `_join_pk` columns
//! (`[_join_pk × k, outer cols…]` — an equi circuit has no output exchange, so
//! re-keying to the outer PK is physically impossible); band and pure-range
//! correlations re-key onto the outer source PK riding their mandatory output
//! exchange (`[_src_pk × pa, outer cols…]`).

use crate::ast_util::{
    expr_operands, extract_table_name_and_alias, flatten_conjuncts, group_by_is_present, is_wildcard_projection,
    projection_has_aggregate,
};
use crate::bind::{
    bind_single_table, build_alias_map, resolve_qualified_column, resolve_unqualified_column, AliasMap, Binder,
};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::plan::validate::{
    reject_column_overflow, reject_duplicate_column_names, reject_unhonored_query_clauses,
    reject_unhonored_select_clauses, HonoredClauses, HonoredQueryClauses,
};
use crate::plan::view::join::{
    build_join_view_projection, build_pure_range_threshold, normalize_to_ab, side_target_tcs,
};
use crate::plan::view::predicates::{
    and_fold_compile, build_reindex_program, converse_rel, extract_join_predicates, multi_null_filter_prog,
    RangeConjunct,
};
use crate::SqlResult;
use gnitz_core::{CircuitBuilder, ColumnDef, FixedInt, GnitzClient, NodeId, Schema, TypeCode};
use sqlparser::ast::{BinaryOperator, Expr, Ident, Select, SelectItem, SetExpr};

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_create_exists_view(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name: &str,
    sql_text: &str,
    select: &Select,
    subq: &Expr,
    outer_not: bool,
    local_conjuncts: &[&Expr],
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // GROUP BY / aggregates cannot host the subquery in one circuit; a targeted
    // message beats the generic clause guard's.
    if group_by_is_present(&select.group_by) || projection_has_aggregate(select) {
        return Err(GnitzSqlError::Unsupported(
            "EXISTS/IN subqueries are not supported together with GROUP BY/aggregates; \
             put the subquery in an inner view"
                .into(),
        ));
    }
    // The outer SELECT consumes FROM + projection + WHERE (the subquery conjunct
    // and the local conjuncts); reject everything else (HAVING without GROUP BY,
    // PREWHERE, TOP, …) so a dropped clause is a clean error.
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
            distinct: false,
        },
        "CREATE VIEW EXISTS/IN",
    )?;

    // ── Outer relation ────────────────────────────────────────────────────────
    let (outer_name, outer_alias) = extract_table_name_and_alias(&select.from[0].relation, "CREATE VIEW EXISTS/IN")?;
    let (outer_tid, outer_schema) = binder.resolve(client, &outer_name)?;

    // ── Subquery envelope ────────────────────────────────────────────────────
    // For IN, `in_operand` carries the outer operand expression.
    let (body, node_negated, in_operand) = match subq {
        Expr::Exists { subquery, negated } => {
            reject_unhonored_query_clauses(
                subquery,
                HonoredQueryClauses {
                    with: false,
                    limit: false,
                },
                "EXISTS subquery",
            )?;
            (subquery.body.as_ref(), *negated, None)
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => (subquery.as_ref(), *negated, Some(expr.as_ref())),
        other => {
            return Err(GnitzSqlError::Plan(format!(
                "exists builder dispatched on a non-subquery conjunct: {other:?}"
            )))
        }
    };
    let inner_select = match body {
        SetExpr::Select(s) => s.as_ref(),
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "EXISTS/IN subquery: only a plain SELECT body is supported; compose via views".into(),
            ))
        }
    };
    // `NOT (EXISTS …)` folds into the node's own flag, so it behaves like
    // `NOT EXISTS …` (and a double negation cancels).
    let negated = node_negated ^ outer_not;

    reject_unhonored_select_clauses(
        inner_select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
            distinct: false,
        },
        "EXISTS/IN subquery",
    )?;
    if inner_select.from.len() != 1 || !inner_select.from[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "EXISTS/IN subquery: only a single FROM table without JOINs is supported; compose via views".into(),
        ));
    }
    let (inner_name, inner_alias) = extract_table_name_and_alias(&inner_select.from[0].relation, "EXISTS/IN subquery")?;
    let (inner_tid, inner_schema) = binder.resolve(client, &inner_name)?;

    // The 2-term DBSP join formula assumes the two input deltas are never
    // simultaneously active — the same-source shape would drop the bilinear
    // ΔT⋈ΔT cross-term (the self-join guard's rationale).
    if outer_tid == inner_tid {
        return Err(GnitzSqlError::Unsupported(
            "EXISTS/IN subquery over the view's own FROM relation is not supported; \
             wrap one side in a view"
                .into(),
        ));
    }
    if outer_alias.eq_ignore_ascii_case(&inner_alias) {
        return Err(GnitzSqlError::Bind(format!(
            "relation alias '{outer_alias}' is used by both the view FROM and its subquery; rename one"
        )));
    }

    // Two-relation alias map for correlation extraction (outer at offset 0,
    // inner after it) and an outer-only map for the user projection — the inner
    // alias is unresolvable there, which is correct SQL scoping for free.
    let outer_n = outer_schema.columns.len();
    let alias_map = build_alias_map(&[
        (&outer_alias, outer_tid, &outer_schema),
        (&inner_alias, inner_tid, &inner_schema),
    ]);
    let outer_alias_map = build_alias_map(&[(&outer_alias, outer_tid, &outer_schema)]);

    // ── Outer local conjuncts → one filter over the raw outer input ─────────
    let local_prog = and_fold_compile(
        local_conjuncts.iter().copied(),
        |e| bind_single_table(e, &outer_schema),
        &outer_schema,
    )?;

    // ── Inner WHERE: split conjuncts by side ─────────────────────────────────
    // outer == 0            → inner pre-filter (constant conjuncts included);
    // outer > 0, inner == 0 → reject (hoisting would be semantically wrong for
    //                         NOT EXISTS, so no silent rewrite);
    // both sides            → correlation predicate.
    let mut inner_conjuncts: Vec<&Expr> = Vec::new();
    if let Some(w) = &inner_select.selection {
        flatten_conjuncts(w, &mut inner_conjuncts);
    }
    let mut prefilter: Vec<&Expr> = Vec::new();
    let mut corr_acc: Option<Expr> = None;
    for &c in &inner_conjuncts {
        let (mut outer_refs, mut inner_refs) = (0usize, 0usize);
        count_side_refs(c, &alias_map, outer_n, &mut outer_refs, &mut inner_refs)?;
        if outer_refs == 0 {
            prefilter.push(c);
        } else if inner_refs == 0 {
            return Err(GnitzSqlError::Unsupported(
                "EXISTS/IN subquery WHERE conjunct references only the outer relation; \
                 hoist it into the view's own WHERE clause"
                    .into(),
            ));
        } else {
            and_into(&mut corr_acc, c.clone());
        }
    }
    let prefilter_prog = and_fold_compile(
        prefilter.iter().copied(),
        |e| bind_single_table(e, &inner_schema),
        &inner_schema,
    )?;

    // ── IN: synthesize the (outer_col = inner_col) pair as a qualified equality
    // conjunct so it runs through the same validate_join_key_pair path (float
    // rejection, cross-width promotion) as every join key. ──────────────────
    if let Some(outer_operand) = in_operand {
        if matches!(outer_operand, Expr::Tuple(_)) {
            return Err(GnitzSqlError::Unsupported(
                "IN (SELECT …): tuple operands are not supported".into(),
            ));
        }
        let outer_col = match bind_single_table(outer_operand, &outer_schema)? {
            BoundExpr::ColRef(c) => c,
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "IN (SELECT …): the outer operand must be a plain column".into(),
                ))
            }
        };
        let inner_proj_expr = match inner_select.projection.as_slice() {
            [SelectItem::UnnamedExpr(e)] | [SelectItem::ExprWithAlias { expr: e, .. }] => e,
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "IN (SELECT …): the subquery must select exactly one plain column".into(),
                ))
            }
        };
        let inner_col = match bind_single_table(inner_proj_expr, &inner_schema)? {
            BoundExpr::ColRef(c) => c,
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "IN (SELECT …): the subquery must select exactly one plain column".into(),
                ))
            }
        };
        // NOT IN and the anti-join coincide only when neither side can be NULL
        // (a NULL outer operand or any NULL inner value makes SQL's NOT IN
        // UNKNOWN → row excluded, which the anti-join cannot see).
        if negated && (outer_schema.columns[outer_col].is_nullable || inner_schema.columns[inner_col].is_nullable) {
            return Err(GnitzSqlError::Unsupported(
                "NOT IN (SELECT …) requires the outer operand and the subquery column to be \
                 NOT NULL (SQL's NULL semantics diverge from the anti-join otherwise); \
                 use NOT EXISTS with an explicit equality instead"
                    .into(),
            ));
        }
        let pair = Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new(outer_alias.clone()),
                Ident::new(outer_schema.columns[outer_col].name.clone()),
            ])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new(inner_alias.clone()),
                Ident::new(inner_schema.columns[inner_col].name.clone()),
            ])),
        };
        and_into(&mut corr_acc, pair);
    }

    // ── Correlation classification via the join ON-clause vocabulary ────────
    let corr_expr = corr_acc.ok_or_else(|| {
        GnitzSqlError::Unsupported(
            "uncorrelated EXISTS (no conjunct pairing an outer and an inner column) is not supported".into(),
        )
    })?;
    let (left_cols, right_cols, target_tcs, range, residual) =
        extract_join_predicates(&corr_expr, &outer_schema, &inner_schema, &alias_map).map_err(|e| match e {
            // The anchor guard fires when every correlation conjunct fell into
            // the residual; rephrase its JOIN-ON message for this surface.
            GnitzSqlError::Bind(msg) if msg.contains("at least one equijoin or range") => GnitzSqlError::Unsupported(
                "EXISTS/IN correlation must include at least one equality or range conjunct \
                 between an outer and an inner column"
                    .into(),
            ),
            e => e,
        })?;
    if !residual.is_empty() {
        // Same boundary as the outer-join + residual rejection: match existence
        // is decided from the inner output (or the threshold witness), so a
        // residual would have to participate in that decision.
        return Err(GnitzSqlError::Unsupported(
            "EXISTS/IN correlation contains a conjunct the semi-join cannot consume \
             (a non-equality/non-range comparison, a second range conjunct, or an OR \
             group spanning both relations); it would have to participate in the \
             match-existence decision. Filter inside the subquery or a wrapping view."
                .into(),
        ));
    }

    let inner_n = inner_schema.columns.len();
    let ctx = ExistsCtx {
        select,
        outer_tid,
        inner_tid,
        outer_schema: &outer_schema,
        inner_schema: &inner_schema,
        outer_alias_map: &outer_alias_map,
        negated,
        local_prog,
        prefilter_prog,
        left_cols: &left_cols,
        right_cols: &right_cols,
        target_tcs: &target_tcs,
    };
    match range {
        None => {
            // Widest intermediate is a join term: [k pk, A, B].
            reject_column_overflow("EXISTS view intermediate", left_cols.len() + outer_n + inner_n)?;
            build_equi_exists(client, schema_name, view_name, sql_text, ctx)
        }
        Some(range) => {
            if left_cols.is_empty() && FixedInt::from_type_code(range.tc).is_none() {
                let range_tc = range.tc;
                return Err(GnitzSqlError::Unsupported(format!(
                    "pure-range EXISTS/IN correlation needs a ≤8-byte integer range column \
                     (got {range_tc:?}); its threshold null-fill reduces the range column with \
                     MIN/MAX, which has no 16-byte accumulator — use a narrower range column \
                     or add an equality conjunct"
                )));
            }
            // Widest intermediate is the band re-key: [a.pk, k pk, A, B].
            reject_column_overflow(
                "EXISTS view intermediate",
                outer_schema.pk_cols.len() + left_cols.len() + 1 + outer_n + inner_n,
            )?;
            build_range_exists(client, schema_name, view_name, sql_text, ctx, range)
        }
    }
}

/// Everything the circuit builders below need, bundled so each takes one
/// argument instead of thirteen.
struct ExistsCtx<'a> {
    select: &'a Select,
    outer_tid: u64,
    inner_tid: u64,
    outer_schema: &'a Schema,
    inner_schema: &'a Schema,
    outer_alias_map: &'a AliasMap,
    negated: bool,
    local_prog: Option<gnitz_core::ExprProgram>,
    prefilter_prog: Option<gnitz_core::ExprProgram>,
    /// Equality-prefix correlation pairs (outer-relative / inner-relative) with
    /// their per-pair promoted common type.
    left_cols: &'a [usize],
    right_cols: &'a [usize],
    target_tcs: &'a [TypeCode],
}

impl ExistsCtx<'_> {
    /// The common circuit prologue: both tagged inputs with their local /
    /// pre-filter conjuncts applied. Returns `(cb, a_local, b_local)`.
    fn open_circuit(&self, view_id: u64) -> (CircuitBuilder, NodeId, NodeId) {
        let mut cb = CircuitBuilder::new(view_id, 0); // no single primary source
        let input_a_raw = cb.input_delta_tagged(self.outer_tid);
        let a_local = match &self.local_prog {
            Some(p) => cb.filter(input_a_raw, Some(p.clone())),
            None => input_a_raw,
        };
        let input_b_raw = cb.input_delta_tagged(self.inner_tid);
        let b_local = match &self.prefilter_prog {
            Some(p) => cb.filter(input_b_raw, Some(p.clone())),
            None => input_b_raw,
        };
        (cb, a_local, b_local)
    }
}

/// The shared semi/anti composition over the weight-exact null-fill set
/// `ν = positive_part(A − π_A(inner)) = w_A·[S=0]`: the anti-join is ν itself
/// (exactly the unmatched rows, weight w_A each); the semi-join its complement
/// `A − ν = w_A·[S>0]` — matched rows keep weight w_A, unmatched cancel to 0 at
/// consolidation. `a_all` may alias a node with other consumers (the equi
/// builder's `reindex_a`), so it rides the non-destructive PORT_IN_B operand;
/// negate(ν) is fresh.
fn semi_or_anti(cb: &mut CircuitBuilder, a_all: NodeId, pi_a: NodeId, negated: bool) -> NodeId {
    let nu = cb.positive_diff(a_all, pi_a); // ν, keyed like a_all
    if negated {
        nu
    } else {
        let neg = cb.negate(nu);
        cb.union(neg, a_all)
    }
}

/// Equi correlation (no range conjunct): the derivation of the equi outer-join
/// circuit with the inner side's columns never materialized. Output keyed by
/// the k synthetic `_join_pk` columns (no output exchange — the join-shard
/// scatter co-locates `a_all`, `π_A(inner)` and the traces per `_join_pk`
/// worker, exactly like the LEFT-join null-fill).
fn build_equi_exists(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name: &str,
    sql_text: &str,
    ctx: ExistsCtx<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let k = ctx.left_cols.len();
    let a_n = ctx.outer_schema.columns.len();
    let b_n = ctx.inner_schema.columns.len();

    let left_target_tcs = side_target_tcs(ctx.left_cols, ctx.outer_schema, ctx.target_tcs);
    let right_target_tcs = side_target_tcs(ctx.right_cols, ctx.inner_schema, ctx.target_tcs);
    // A NULL equi key matches nothing (SQL 3VL): gate NULL keys out of the
    // match on both sides. The unfiltered `a_local` still reaches `a_all`, so a
    // NULL-keyed outer row is never in `π_A(inner)` and lands in ν at full
    // multiplicity — anti includes it (no match exists), semi cancels it to 0.
    let left_key_nullable = ctx.left_cols.iter().any(|&c| ctx.outer_schema.columns[c].is_nullable);
    let right_key_nullable = ctx.right_cols.iter().any(|&c| ctx.inner_schema.columns[c].is_nullable);

    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
    let (mut cb, a_local, b_local) = ctx.open_circuit(view_id);

    let b_match = if right_key_nullable {
        cb.filter(
            b_local,
            Some(multi_null_filter_prog(ctx.right_cols, ctx.inner_schema, false)?),
        )
    } else {
        b_local
    };
    let reindex_b = cb.map_reindex(
        b_match,
        ctx.right_cols,
        &right_target_tcs,
        build_reindex_program(ctx.inner_schema),
    );
    let a_match = if left_key_nullable {
        cb.filter(
            a_local,
            Some(multi_null_filter_prog(ctx.left_cols, ctx.outer_schema, false)?),
        )
    } else {
        a_local
    };
    let reindex_a = cb.map_reindex(
        a_match,
        ctx.left_cols,
        &left_target_tcs,
        build_reindex_program(ctx.outer_schema),
    );

    let trace_a = cb.integrate_trace(reindex_a);
    let trace_b = cb.integrate_trace(reindex_b);
    let join_ab = cb.join_with_trace_node(reindex_a, trace_b); // [k pk, A, B]
    let join_ba = cb.join_with_trace_node(reindex_b, trace_a); // [k pk, B, A]

    // π_A(inner): project each term straight to [_join_pk × k, A] — B's columns
    // are never carried (no normalize_to_ab).
    let pa_ab = cb.map(join_ab, &(k..k + a_n).collect::<Vec<_>>());
    let pa_ba = cb.map(join_ba, &(k + b_n..k + b_n + a_n).collect::<Vec<_>>());
    let pi_a = cb.union(pa_ab, pa_ba);

    // `a_all` re-keys the FULL (locally filtered, NULL keys included) outer
    // input with the identical encoding, reusing `reindex_a` when the key is
    // non-nullable.
    let a_all = if left_key_nullable {
        cb.map_reindex(
            a_local,
            ctx.left_cols,
            &left_target_tcs,
            build_reindex_program(ctx.outer_schema),
        )
    } else {
        reindex_a
    };
    let sink_pre = semi_or_anti(&mut cb, a_all, pi_a, ctx.negated); // keyed [_join_pk, A]

    // View PK = the k synthetic `_join_pk` columns (the pairs' promoted types,
    // non-nullable); payload = the user projection over the outer relation only.
    let mut out_pk_cols: Vec<ColumnDef> = Vec::with_capacity(k);
    for (i, &t) in ctx.target_tcs.iter().enumerate() {
        let name = if k == 1 {
            "_join_pk".to_string()
        } else {
            format!("_join_pk_{i}")
        };
        out_pk_cols.push(ColumnDef::new(name, t, false));
    }
    finish_exists_view(
        client,
        schema_name,
        view_name,
        sql_text,
        &ctx,
        cb,
        sink_pre,
        &out_pk_cols,
        false,
        view_id,
    )
}

/// Range correlation — band (`n_eq ≥ 1` + one range conjunct) or pure range
/// (`n_eq == 0`). Both re-key onto the outer source PK and ride the mandatory
/// range-join output exchange (view PK = `_src_pk × pa`).
fn build_range_exists(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name: &str,
    sql_text: &str,
    ctx: ExistsCtx<'_>,
    range: RangeConjunct,
) -> Result<SqlResult, GnitzSqlError> {
    let a_n = ctx.outer_schema.columns.len();
    let b_n = ctx.inner_schema.columns.len();
    let n_eq = ctx.left_cols.len();
    let k = n_eq + 1; // reindex slots: eq prefix + range slot
    let pa = ctx.outer_schema.pk_cols.len();
    let zero_a = vec![0u8; pa];

    // Reindex columns and per-slot common type: eq pairs first, range slot last.
    let left_reindex_cols: Vec<usize> = ctx
        .left_cols
        .iter()
        .copied()
        .chain(std::iter::once(range.left_col))
        .collect();
    let right_reindex_cols: Vec<usize> = ctx
        .right_cols
        .iter()
        .copied()
        .chain(std::iter::once(range.right_col))
        .collect();
    let all_tcs: Vec<TypeCode> = ctx
        .target_tcs
        .iter()
        .copied()
        .chain(std::iter::once(range.tc))
        .collect();
    let left_target_tcs = side_target_tcs(&left_reindex_cols, ctx.outer_schema, &all_tcs);
    let right_target_tcs = side_target_tcs(&right_reindex_cols, ctx.inner_schema, &all_tcs);
    let rel_ab = converse_rel(range.op);
    let rel_ba = range.op;

    // NULL exclusion (SQL 3VL) over ALL key cols (eq + range): the match drops
    // NULL-key rows on both sides; `a_local` (NULL keys included) still feeds
    // `a_all` / the anti NULL-key branch, so those rows count as unmatched.
    let left_key_nullable = left_reindex_cols
        .iter()
        .any(|&c| ctx.outer_schema.columns[c].is_nullable);
    let right_key_nullable = right_reindex_cols
        .iter()
        .any(|&c| ctx.inner_schema.columns[c].is_nullable);

    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
    let (mut cb, a_local, b_local) = ctx.open_circuit(view_id);

    let input_a = if left_key_nullable {
        cb.filter(
            a_local,
            Some(multi_null_filter_prog(&left_reindex_cols, ctx.outer_schema, false)?),
        )
    } else {
        a_local
    };
    let input_b = if right_key_nullable {
        cb.filter(
            b_local,
            Some(multi_null_filter_prog(&right_reindex_cols, ctx.inner_schema, false)?),
        )
    } else {
        b_local
    };
    let reindex_a = cb.map_reindex(
        input_a,
        &left_reindex_cols,
        &left_target_tcs,
        build_reindex_program(ctx.outer_schema),
    );
    let reindex_b = cb.map_reindex(
        input_b,
        &right_reindex_cols,
        &right_target_tcs,
        build_reindex_program(ctx.inner_schema),
    );

    let sink_pre = if n_eq == 0 {
        // ── Pure range: the one-row threshold `m = MAX/MIN(b.range)` decides
        // match existence (`∃b. a.x OP b.y ⟺ a.x OP m`), replicated on every
        // worker by the broadcast relay — no inner join terms at all. The input
        // is broadcast, so each worker trims to its owned slice before
        // integrating (PartitionFilter), exactly like the pure-range LEFT join.
        let int_a = cb.partition_filter(reindex_a);
        let trace_a = cb.integrate_trace(int_a);
        let thr = build_pure_range_threshold(
            &mut cb,
            ctx.outer_schema,
            range.tc,
            range.op,
            reindex_b,
            int_a,
            trace_a,
            ctx.negated, // the `int_a` passthrough is only needed for A − matched
        );
        if !ctx.negated {
            // EXISTS: `matched = A ⋈ {m}` carries each row's weight verbatim
            // (one-row `m` cannot multiply) — weight-exact with no clamp.
            // NULL-range-key rows are absent from `int_a` → correctly excluded.
            thr.matched
        } else {
            // NOT EXISTS: A − matched. An empty/all-NULL inner side gives
            // `m = ∅`, so `A − ∅ = A` includes every outer row for free.
            let a_pass = thr.a_pass.expect("a_pass requested for the anti polarity");
            let neg = cb.negate(thr.matched);
            let nf_match = cb.union(a_pass, neg); // A − matched, keyed [a.pk…, A]
            if left_key_nullable {
                // NULL-range-key rows never reach `int_a`/`trace_a` (3VL) and
                // never match the threshold: their own branch off the
                // NULL-gate-unfiltered `a_local`, re-keyed to a.pk and routed
                // ONCE by a local partition_filter (broadcast would emit W×
                // copies the output shard would sum).
                let anull = cb.filter(
                    a_local,
                    Some(multi_null_filter_prog(&left_reindex_cols, ctx.outer_schema, true)?),
                );
                let anull_keyed = cb.map_reindex(
                    anull,
                    &ctx.outer_schema.pk_cols,
                    &zero_a,
                    build_reindex_program(ctx.outer_schema),
                );
                let anull_owned = cb.partition_filter(anull_keyed);
                cb.union(nf_match, anull_owned)
            } else {
                nf_match
            }
        }
    } else {
        // ── Band: the eq-prefix scatter co-locates both sides per eq value, so
        // the inner join, its π_A re-key, `a_all`, and the clamp are all
        // partition-local (the LEFT-join band null-fill shapes).
        let trace_a = cb.integrate_trace(reindex_a);
        let trace_b = cb.integrate_trace(reindex_b);
        let join_ab = cb.join_with_trace_range_node(reindex_a, trace_b, n_eq as u8, rel_ab); // ΔA ⋈θ I(B)
        let join_ba = cb.join_with_trace_range_node(reindex_b, trace_a, n_eq as u8, rel_ba); // ΔB ⋈θ I(A)
        let merged = normalize_to_ab(&mut cb, join_ab, join_ba, k, a_n, b_n); // [_join_pk × k, A, B]

        // π_A(inner) keyed by the outer source PK: a reindex Map's output is
        // [new-PK, <every input col>], so re-key `merged` by A's PK columns
        // (sitting at k + pk within the union layout), then project the A region.
        let mut union_cols: Vec<ColumnDef> = Vec::with_capacity(k + a_n + b_n);
        for (i, &t) in all_tcs.iter().enumerate() {
            union_cols.push(ColumnDef::new(format!("_join_pk_{i}"), t, false));
        }
        union_cols.extend(ctx.outer_schema.columns.iter().cloned());
        union_cols.extend(ctx.inner_schema.columns.iter().cloned());
        let union_schema = Schema {
            columns: union_cols,
            pk_cols: (0..k).collect(),
        };
        let a_pk_in_union: Vec<usize> = ctx.outer_schema.pk_cols.iter().map(|&p| k + p).collect();
        let rekey_a = cb.map_reindex(merged, &a_pk_in_union, &zero_a, build_reindex_program(&union_schema));
        let proj_a = cb.map(rekey_a, &(pa + k..pa + k + a_n).collect::<Vec<_>>()); // π_A(inner), [a.pk…, A]

        let a_all = cb.map_reindex(
            a_local,
            &ctx.outer_schema.pk_cols,
            &zero_a,
            build_reindex_program(ctx.outer_schema),
        );
        semi_or_anti(&mut cb, a_all, proj_a, ctx.negated) // keyed [a.pk…, A]
    };

    // View PK = the outer source PK, exposed under fresh `_src_pk_{i}` names
    // (the outer PK columns also appear verbatim in the payload, and reusing
    // their names would trip the duplicate-name guard on explicit projections).
    let src_pk_coldefs: Vec<ColumnDef> = ctx
        .outer_schema
        .pk_cols
        .iter()
        .enumerate()
        .map(|(i, &c)| {
            ColumnDef::new(
                format!("_src_pk_{i}"),
                ctx.outer_schema.columns[c].type_code.reindex_output_type(),
                false,
            )
        })
        .collect();
    finish_exists_view(
        client,
        schema_name,
        view_name,
        sql_text,
        &ctx,
        cb,
        sink_pre,
        &src_pk_coldefs,
        true,
        view_id,
    )
}

/// The shared create-view tail: build the user projection over the outer
/// relation (`pk_coldefs` are the view's leading PK columns), append the final
/// Map only when the projection is not the identity, shard on exactly the view
/// PK columns in order when the circuit carries an output exchange (band /
/// pure-range — the compound-key alignment invariant shared with the range
/// join builder; equi has no exchange), then sink and register the view.
#[allow(clippy::too_many_arguments)]
fn finish_exists_view(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name: &str,
    sql_text: &str,
    ctx: &ExistsCtx<'_>,
    mut cb: CircuitBuilder,
    sink_pre: NodeId,
    pk_coldefs: &[ColumnDef],
    shard: bool,
    view_id: u64,
) -> Result<SqlResult, GnitzSqlError> {
    let a_n = ctx.outer_schema.columns.len();
    let npk = pk_coldefs.len();
    let (final_cols, final_projection) = build_join_view_projection(
        &ctx.select.projection,
        ctx.outer_alias_map,
        pk_coldefs,
        a_n,
        npk,
        |i| ctx.outer_schema.columns[i].clone(),
        "EXISTS view",
    )?;
    let is_identity = final_projection.len() == a_n && final_projection.iter().enumerate().all(|(i, &p)| p == i + npk);
    let mut sink_input = if is_identity {
        sink_pre
    } else {
        cb.map(sink_pre, &final_projection)
    };
    if shard {
        sink_input = cb.shard(sink_input, &(0..npk).collect::<Vec<_>>());
    }
    cb.sink(sink_input);
    let circuit = cb.build();

    if !is_wildcard_projection(&ctx.select.projection) {
        reject_duplicate_column_names(final_cols.iter().map(|c| c.name.as_str()), "EXISTS view")?;
    }
    let view_pk: Vec<u32> = (0..npk as u32).collect();
    client
        .create_view_with_circuit(schema_name, view_name, sql_text, circuit, &final_cols, &view_pk)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

/// AND a conjunct into the accumulating correlation tree.
fn and_into(acc: &mut Option<Expr>, e: Expr) {
    *acc = Some(match acc.take() {
        None => e,
        Some(prev) => Expr::BinaryOp {
            left: Box::new(prev),
            op: BinaryOperator::And,
            right: Box::new(e),
        },
    });
}

/// Count the outer/inner column references of one inner-WHERE conjunct,
/// resolved through the two-relation alias map (unqualified names resolve
/// across both relations; a name in both is ambiguous and errors). Nested
/// subquery expressions are rejected — they cannot be composed here. Every
/// other construct references no columns of its own and its operands are
/// walked; an unsupported construct raises its precise error at binding.
fn count_side_refs(
    e: &Expr,
    alias_map: &AliasMap,
    outer_n: usize,
    outer: &mut usize,
    inner: &mut usize,
) -> Result<(), GnitzSqlError> {
    let mut bump = |g: usize| {
        if g < outer_n {
            *outer += 1;
        } else {
            *inner += 1;
        }
    };
    match e {
        Expr::Identifier(id) => bump(resolve_unqualified_column(&id.value, alias_map)?),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            bump(resolve_qualified_column(&parts[0].value, &parts[1].value, alias_map)?)
        }
        Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::Subquery(_) | Expr::AnyOp { .. } | Expr::AllOp { .. } => {
            return Err(GnitzSqlError::Unsupported(
                "nested subqueries inside an EXISTS/IN subquery are not supported; compose via views".into(),
            ))
        }
        _ => {
            for operand in expr_operands(e) {
                count_side_refs(operand, alias_map, outer_n, outer, inner)?;
            }
        }
    }
    Ok(())
}
