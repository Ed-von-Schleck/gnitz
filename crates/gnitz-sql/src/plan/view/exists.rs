//! `[NOT] EXISTS (SELECT …)` / `x [NOT] IN (SELECT …)` view compilation: the
//! semi/anti-join as a circuit derivation of the shipped outer-join null-fill
//! machinery — no new engine operators, no new wire opcodes. The dispatcher
//! routes here when a Simple-shaped view carries exactly one subquery: a clean
//! top-level WHERE conjunct becomes the semi/anti *filter* view
//! (`emit_exists_pieces`); any other boolean position — under OR/NOT, inside
//! CASE, or projected — becomes the *mark* view (`emit_mark_pieces`), which
//! splits the outer relation into its matched/unmatched branches and binds the
//! WHERE + projection per branch with the subquery as a `0/1` constant.
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
    body_is_grouped, expr_operands, extract_table_name_and_alias, find_subquery, flatten_conjuncts,
    is_wildcard_projection, projection_item_expr, wildcard_name_is_visible, WildcardRewrite,
};
use crate::bind::{
    bind_single_table, bind_single_table_mark, build_alias_map, resolve_qualified_column, resolve_unqualified_column,
    AliasMap, Binder,
};
use crate::codec::project_schema::{compile_projection_map, resolve_proj_col_with, ProjItem};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::lower::compile_filter_program;
use crate::plan::validate::{
    plain_select_body, reject_column_overflow, reject_duplicate_column_names, reject_unhonored_select_clauses,
    HonoredClauses,
};
use crate::plan::view::join::{
    band_union_schema, build_join_view_projection, build_pure_range_threshold, emit_equi_join_terms,
    is_identity_projection, join_pk_coldefs, normalize_to_ab, range_gate_reindex_prologue, range_slots,
    side_target_tcs, union_null_key_rows, EquiSide, EquiTerms, RangeSlots,
};
use crate::plan::view::predicates::{and_fold_compile, build_reindex_program, extract_join_predicates, RangeConjunct};
use crate::plan::view::EmitPieces;
use gnitz_core::{CircuitBuilder, ColumnDef, FixedInt, GnitzClient, NodeId, Schema, TypeCode};
use sqlparser::ast::{BinaryOperator, Expr, Ident, Select, SelectItem};
use std::rc::Rc;

/// Emit an EXISTS/IN semi/anti-join *filter* view's pieces for a pre-allocated
/// `view_id` — the fast path for a single top-level `[NOT] EXISTS`/`[NOT] IN`
/// WHERE conjunct. The remaining local conjuncts pre-filter the raw outer input.
pub(crate) fn emit_exists_pieces(
    client: &mut GnitzClient,
    view_id: u64,
    select: &Select,
    subq: &Expr,
    outer_not: bool,
    local_conjuncts: &[&Expr],
    binder: &mut Binder<'_>,
) -> Result<EmitPieces, GnitzSqlError> {
    let outer = resolve_exists_outer(client, select, binder)?;
    let (ctx, range) = resolve_correlation(
        client,
        select,
        subq,
        outer_not,
        local_conjuncts,
        /* is_mark = */ false,
        binder,
        outer,
    )?;
    emit_correlated(view_id, ctx, range, /* is_mark = */ false)
}

/// Emit a *mark* view's pieces: exactly one subquery in an arbitrary boolean
/// position (under OR/NOT, inside CASE, or projected). The shared correlation
/// core computes the matched/unmatched split of the outer relation; each branch
/// then binds the original WHERE + projection with the subquery node resolved to
/// its constant truth value there (`bind_single_table_mark`), and ordinary
/// expression evaluation consumes it.
pub(crate) fn emit_mark_pieces(
    client: &mut GnitzClient,
    view_id: u64,
    select: &Select,
    binder: &mut Binder<'_>,
) -> Result<EmitPieces, GnitzSqlError> {
    // The dispatcher counted exactly one subquery across the WHERE + projection.
    let subq = select
        .selection
        .iter()
        .chain(select.projection.iter().filter_map(projection_item_expr))
        .find_map(find_subquery)
        .ok_or_else(|| GnitzSqlError::Plan("mark builder dispatched with no EXISTS/IN subquery".into()))?;
    let outer = resolve_exists_outer(client, select, binder)?;
    // No outer local pre-filter: the whole WHERE is applied post-mark, per branch.
    let (ctx, range) = resolve_correlation(
        client,
        select,
        subq,
        /* outer_not = */ false,
        &[],
        /* is_mark = */ true,
        binder,
        outer,
    )?;
    emit_correlated(view_id, ctx, range, /* is_mark = */ true)
}

/// The resolved outer relation of an EXISTS/IN view — `(tid, schema, alias)`.
type ResolvedOuter = (u64, Rc<Schema>, String);

/// Validate the outer SELECT's envelope and resolve its FROM relation. Hoisted
/// out of `resolve_correlation` so a caller can substitute a different outer
/// (the shared correlation core takes the pre-resolved triple as a parameter).
fn resolve_exists_outer(
    client: &mut GnitzClient,
    select: &Select,
    binder: &mut Binder<'_>,
) -> Result<ResolvedOuter, GnitzSqlError> {
    // GROUP BY / aggregates cannot host the subquery in one circuit; a targeted
    // message beats the generic clause guard's.
    if body_is_grouped(select) {
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
    let (outer_name, outer_alias) = extract_table_name_and_alias(&select.from[0].relation, "CREATE VIEW EXISTS/IN")?;
    let (outer_tid, outer_schema) = binder.resolve(client, &outer_name)?;
    Ok((outer_tid, outer_schema, outer_alias))
}

/// Shared front half: resolve the inner relation against the pre-resolved
/// `outer`, extract the correlation predicates + inner pre-filter, and compile
/// the outer local pre-filter.
#[allow(clippy::too_many_arguments)]
fn resolve_correlation<'a>(
    client: &mut GnitzClient,
    select: &'a Select,
    subq: &Expr,
    outer_not: bool,
    local_conjuncts: &[&Expr],
    is_mark: bool,
    binder: &mut Binder<'_>,
    outer: ResolvedOuter,
) -> Result<(ExistsCtx<'a>, Option<RangeConjunct>), GnitzSqlError> {
    let (outer_tid, outer_schema, outer_alias) = outer;

    // ── Subquery envelope ────────────────────────────────────────────────────
    // For IN, `in_operand` carries the outer operand expression. Both node kinds
    // carry a full `Query` (`InSubquery` since sqlparser 0.60), so one shared
    // guard rejects the envelope (LIMIT / ORDER BY / WITH / …).
    let (subquery, ctx, node_negated, in_operand) = match subq {
        Expr::Exists { subquery, negated } => (subquery, "EXISTS subquery", *negated, None),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => (subquery, "IN subquery", *negated, Some(expr.as_ref())),
        other => {
            return Err(GnitzSqlError::Plan(format!(
                "exists builder dispatched on a non-subquery conjunct: {other:?}"
            )))
        }
    };
    let inner_select = plain_select_body(subquery, ctx)?;
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
    let (inner_tid, inner_schema, inner_alias) = resolve_single_plain_from(
        client,
        binder,
        &inner_select.from,
        "EXISTS/IN subquery: only a single FROM table without JOINs is supported; compose via views",
        "EXISTS/IN subquery",
    )?;

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
    reject_alias_collision(&outer_alias, &inner_alias)?;

    // Two-relation alias map for correlation extraction (outer at offset 0,
    // inner after it) and an outer-only map for the user projection — the inner
    // alias is unresolvable there, which is correct SQL scoping for free.
    let outer_n = outer_schema.columns.len();
    let outer_alias_map = build_alias_map(&[(&outer_alias, outer_tid, &outer_schema)]);
    let alias_map = build_alias_map(&[
        (&outer_alias, outer_tid, &outer_schema),
        (&inner_alias, inner_tid, &inner_schema),
    ]);

    // ── Inner WHERE: split conjuncts by side ─────────────────────────────────
    let (prefilter, mut corr_acc) = split_correlation_conjuncts(
        inner_select.selection.as_ref(),
        &alias_map,
        outer_n,
        "EXISTS/IN subquery WHERE conjunct references only the outer relation; \
         hoist it into the view's own WHERE clause",
    )?;
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
        let in_nullable = outer_schema.columns[outer_col].is_nullable || inner_schema.columns[inner_col].is_nullable;
        // Mark position: an IN mark can be negated or projected, so a NULL key's
        // `mark = 0` (≈ FALSE) would leak where SQL means UNKNOWN. Reject any
        // nullable operand regardless of polarity — strictly stronger than the
        // conjunct check below (whose `negated &&` gate stays, since a positive
        // nullable IN is two-valued-safe as a top-level AND conjunct).
        if is_mark && in_nullable {
            return Err(GnitzSqlError::Unsupported(
                "IN (SELECT …) in a mark position (under OR/NOT, in CASE, or projected) requires \
                 the outer operand and the subquery column to be NOT NULL — SQL's 3VL diverges \
                 from the two-valued mark; use a top-level AND `x IN (SELECT …)` conjunct, or \
                 NOT EXISTS with an explicit equality"
                    .into(),
            ));
        }
        // NOT IN and the anti-join coincide only when neither side can be NULL
        // (a NULL outer operand or any NULL inner value makes SQL's NOT IN
        // UNKNOWN → row excluded, which the anti-join cannot see).
        if negated && in_nullable {
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

    // The outer local conjuncts pre-filter the raw outer input.
    let local_prog = and_fold_compile(
        local_conjuncts.iter().copied(),
        |e| bind_single_table(e, &outer_schema),
        &outer_schema,
    )?;

    Ok((
        ExistsCtx {
            select,
            outer_tid,
            inner_tid,
            outer_schema,
            inner_schema,
            outer_alias_map,
            negated,
            local_prog,
            prefilter_prog,
            left_cols,
            right_cols,
            target_tcs,
        },
        range,
    ))
}

/// Dispatch to the equi / range circuit builder.
fn emit_correlated(
    view_id: u64,
    ctx: ExistsCtx<'_>,
    range: Option<RangeConjunct>,
    is_mark: bool,
) -> Result<EmitPieces, GnitzSqlError> {
    let outer_n = ctx.outer_schema.columns.len();
    let inner_n = ctx.inner_schema.columns.len();
    match range {
        None => {
            // Widest intermediate is a join term: [k pk, A, B].
            reject_column_overflow("EXISTS view intermediate", ctx.left_cols.len() + outer_n + inner_n)?;
            emit_equi_exists(view_id, ctx, is_mark)
        }
        Some(range) => {
            if ctx.left_cols.is_empty() && FixedInt::from_type_code(range.tc).is_none() {
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
                ctx.outer_schema.pk_cols.len() + ctx.left_cols.len() + 1 + outer_n + inner_n,
            )?;
            emit_range_exists(view_id, ctx, range, is_mark)
        }
    }
}

/// Everything the circuit builders below need, bundled so each takes one
/// argument instead of thirteen: the resolved relations, the correlation
/// predicates, and the compiled pre-filters.
struct ExistsCtx<'a> {
    select: &'a Select,
    outer_tid: u64,
    inner_tid: u64,
    outer_schema: Rc<Schema>,
    inner_schema: Rc<Schema>,
    /// Outer-only alias map for the user projection — the inner alias is
    /// unresolvable there, which is correct SQL scoping for free.
    outer_alias_map: AliasMap,
    negated: bool,
    /// Outer local conjuncts → one filter over the raw outer input (empty on the
    /// mark path, whose whole WHERE is applied post-mark).
    local_prog: Option<gnitz_core::ExprProgram>,
    prefilter_prog: Option<gnitz_core::ExprProgram>,
    /// Equality-prefix correlation pairs (outer-relative / inner-relative) with
    /// their per-pair promoted common type.
    left_cols: Vec<usize>,
    right_cols: Vec<usize>,
    target_tcs: Vec<TypeCode>,
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

/// The shared semi + anti composition over the weight-exact null-fill set
/// `ν = positive_part(A − π_A(inner)) = w_A·[S=0]`: the anti-join is ν itself
/// (exactly the unmatched rows, weight w_A each); the semi-join its complement
/// `A − ν = w_A·[S>0]` — matched rows keep weight w_A, unmatched cancel to 0 at
/// consolidation. Returns `(semi, anti)` over ONE shared ν, so the mark view
/// (which consumes both) carries a single stateful `positive_part` clamp.
/// `a_all` may alias a node with other consumers (the equi builder's
/// `reindex_a`), so it rides the non-destructive PORT_IN_B operand; negate(ν)
/// is fresh, and ν itself fans out non-destructively (negate reads by-ref).
fn semi_and_anti(cb: &mut CircuitBuilder, a_all: NodeId, pi_a: NodeId) -> (NodeId, NodeId) {
    let nu = cb.positive_diff(a_all, pi_a); // ν, keyed like a_all
    let neg = cb.negate(nu);
    (cb.union(neg, a_all), nu)
}

/// One polarity of [`semi_and_anti`] for the filter view (the anti case builds
/// no dangling semi nodes).
fn semi_or_anti(cb: &mut CircuitBuilder, a_all: NodeId, pi_a: NodeId, negated: bool) -> NodeId {
    if negated {
        cb.positive_diff(a_all, pi_a)
    } else {
        semi_and_anti(cb, a_all, pi_a).0
    }
}

/// Equi correlation (no range conjunct): the derivation of the equi outer-join
/// circuit with the inner side's columns never materialized. Output keyed by
/// the k synthetic `_join_pk` columns (no output exchange — the join-shard
/// scatter co-locates `a_all`, `π_A(inner)` and the traces per `_join_pk`
/// worker, exactly like the LEFT-join null-fill).
fn emit_equi_exists(view_id: u64, ctx: ExistsCtx<'_>, is_mark: bool) -> Result<EmitPieces, GnitzSqlError> {
    let k = ctx.left_cols.len();
    let a_n = ctx.outer_schema.columns.len();
    let b_n = ctx.inner_schema.columns.len();

    let left_target_tcs = side_target_tcs(&ctx.left_cols, &ctx.outer_schema, &ctx.target_tcs);
    let right_target_tcs = side_target_tcs(&ctx.right_cols, &ctx.inner_schema, &ctx.target_tcs);

    let (mut cb, a_local, b_local) = ctx.open_circuit(view_id);

    // The symmetric 2-term join over the NULL-gated sides (SQL 3VL: a NULL equi
    // key matches nothing). The unfiltered `a_local` still reaches `a_all`, so a
    // NULL-keyed outer row is never in `π_A(inner)` and lands in ν at full
    // multiplicity — anti includes it (no match exists), semi cancels it to 0.
    let EquiTerms {
        reindex_a,
        a_nullable: left_key_nullable,
        join_ab, // [k pk, A, B]
        join_ba, // [k pk, B, A]
        ..
    } = emit_equi_join_terms(
        &mut cb,
        EquiSide {
            input: a_local,
            cols: &ctx.left_cols,
            target_tcs: &left_target_tcs,
            schema: &ctx.outer_schema,
            keep: &(0..a_n).collect::<Vec<_>>(),
        },
        EquiSide {
            input: b_local,
            cols: &ctx.right_cols,
            target_tcs: &right_target_tcs,
            schema: &ctx.inner_schema,
            keep: &(0..b_n).collect::<Vec<_>>(),
        },
    )?;

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
            &ctx.left_cols,
            &left_target_tcs,
            build_reindex_program(&ctx.outer_schema),
        )
    } else {
        reindex_a
    };
    // View PK = the k synthetic `_join_pk` columns (the pairs' promoted types,
    // non-nullable, hidden); payload = the user projection over the outer
    // relation only.
    let out_pk_cols = join_pk_coldefs(&ctx.target_tcs);
    if is_mark {
        // Both branches over one shared ν, keyed [_join_pk, A].
        let (matched, unmatched) = semi_and_anti(&mut cb, a_all, pi_a);
        finish_mark_pieces(&ctx, cb, matched, unmatched, &out_pk_cols, false)
    } else {
        let sink_pre = semi_or_anti(&mut cb, a_all, pi_a, ctx.negated); // keyed [_join_pk, A]
        finish_exists_pieces(&ctx, cb, sink_pre, &out_pk_cols, false)
    }
}

/// Range correlation — band (`n_eq ≥ 1` + one range conjunct) or pure range
/// (`n_eq == 0`). Both re-key onto the outer source PK and ride the mandatory
/// range-join output exchange (view PK = `_src_pk × pa`).
fn emit_range_exists(
    view_id: u64,
    ctx: ExistsCtx<'_>,
    range: RangeConjunct,
    is_mark: bool,
) -> Result<EmitPieces, GnitzSqlError> {
    let a_n = ctx.outer_schema.columns.len();
    let b_n = ctx.inner_schema.columns.len();
    let n_eq = ctx.left_cols.len();
    let k = n_eq + 1; // reindex slots: eq prefix + range slot
    let pa = ctx.outer_schema.pk_cols.len();
    let zero_a = vec![0u8; pa];

    // Reindex-slot layout (eq pairs first, range slot last) and the §3 term
    // relations, shared with the range join builder.
    let slots = range_slots(
        &ctx.left_cols,
        &ctx.right_cols,
        &ctx.target_tcs,
        &range,
        &ctx.outer_schema,
        &ctx.inner_schema,
    );

    let (mut cb, a_local, b_local) = ctx.open_circuit(view_id);

    // NULL exclusion + both reindexes (the shared range prologue, SQL 3VL over
    // ALL key cols — eq + range): the match drops NULL-key rows on both sides;
    // `a_local` (NULL keys included) still feeds `a_all` / the anti NULL-key
    // branch, so those rows count as unmatched.
    let (reindex_a, reindex_b, left_key_nullable) =
        range_gate_reindex_prologue(&mut cb, a_local, b_local, &slots, &ctx.outer_schema, &ctx.inner_schema)?;
    let RangeSlots {
        left_reindex_cols,
        all_tcs,
        rel_ab,
        rel_ba,
        ..
    } = slots;

    // Per correlation shape, compute the branch node(s): `(primary, None)` for
    // the filter view, or the `(matched, Some(unmatched))` pair for the mark view.
    let (primary, unmatched): (NodeId, Option<NodeId>) = if n_eq == 0 {
        // ── Pure range: the one-row threshold `m = MAX/MIN(b.range)` decides
        // match existence (`∃b. a.x OP b.y ⟺ a.x OP m`), replicated on every
        // worker by the broadcast relay — no inner join terms at all. The input
        // is broadcast, so each worker trims to its owned slice before
        // integrating (PartitionFilter), exactly like the pure-range LEFT join.
        // (The compiler makes the trim a keep-all identity for an all-replicated
        // view, which runs correct-local over the full broadcast on every worker.)
        let int_a = cb.partition_filter(reindex_a);
        let trace_a = cb.integrate_trace(int_a);
        // `a_pass` (int_a re-keyed) is needed for the `A − matched` (anti) set —
        // by the negated Filter view and by both branches of a Mark view.
        let want_a_pass = ctx.negated || is_mark;
        let thr = build_pure_range_threshold(
            &mut cb,
            &ctx.outer_schema,
            range.tc,
            range.op,
            reindex_b,
            int_a,
            trace_a,
            want_a_pass,
        );
        // `unmatched = A − matched` = (a_pass − matched) ∪ NULL-range-key rows.
        // Built only when needed (negated Filter, or Mark). An empty/all-NULL inner
        // side gives `m = ∅`, so `A − ∅ = A` includes every outer row for free.
        let build_unmatched = |cb: &mut CircuitBuilder| -> Result<NodeId, GnitzSqlError> {
            let a_pass = thr.a_pass.expect("a_pass requested for the anti/mark branch");
            let neg = cb.negate(thr.matched);
            let nf_match = cb.union(a_pass, neg); // A − matched, keyed [a.pk…, A]
            if left_key_nullable {
                // NULL-range-key rows never reach `int_a`/`trace_a` (3VL) and
                // never match the threshold: their own branch off the
                // NULL-gate-unfiltered `a_local` (`union_null_key_rows`).
                union_null_key_rows(cb, nf_match, a_local, &left_reindex_cols, &ctx.outer_schema)
            } else {
                Ok(nf_match)
            }
        };
        if is_mark {
            // `matched = A ⋈ {m}` carries each row's weight verbatim (one-row `m`
            // cannot multiply); NULL-range-key rows are absent from `int_a` (→ they
            // are unmatched, added by `build_unmatched`).
            let unmatched = build_unmatched(&mut cb)?;
            (thr.matched, Some(unmatched))
        } else if !ctx.negated {
            (thr.matched, None)
        } else {
            (build_unmatched(&mut cb)?, None)
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
        let union_schema = band_union_schema(&all_tcs, &ctx.outer_schema, &ctx.inner_schema);
        let a_pk_in_union: Vec<usize> = ctx.outer_schema.pk_cols.iter().map(|&p| k + p).collect();
        let rekey_a = cb.map_reindex(merged, &a_pk_in_union, &zero_a, build_reindex_program(&union_schema));
        let proj_a = cb.map(rekey_a, &(pa + k..pa + k + a_n).collect::<Vec<_>>()); // π_A(inner), [a.pk…, A]

        let a_all = cb.map_reindex(
            a_local,
            &ctx.outer_schema.pk_cols,
            &zero_a,
            build_reindex_program(&ctx.outer_schema),
        );
        if is_mark {
            // Both branches over one shared ν, keyed [a.pk…, A].
            let (matched, unmatched) = semi_and_anti(&mut cb, a_all, proj_a);
            (matched, Some(unmatched))
        } else {
            (semi_or_anti(&mut cb, a_all, proj_a, ctx.negated), None) // keyed [a.pk…, A]
        }
    };

    // View PK = the outer source PK, hidden (the outer PK columns also ride the
    // payload verbatim, so nothing is lost; hidden columns are exempt from the
    // duplicate-name guard and name resolution, so the names can stay verbatim
    // too — matching `place_pk_front`'s hidden auto-prepend convention).
    let src_pk_coldefs: Vec<ColumnDef> = ctx
        .outer_schema
        .pk_cols
        .iter()
        .map(|&c| {
            ColumnDef::new(
                ctx.outer_schema.columns[c].name.clone(),
                ctx.outer_schema.columns[c].type_code.reindex_output_type(),
                false,
            )
            .hidden()
        })
        .collect();
    match unmatched {
        None => finish_exists_pieces(&ctx, cb, primary, &src_pk_coldefs, true),
        Some(unmatched) => finish_mark_pieces(&ctx, cb, primary, unmatched, &src_pk_coldefs, true),
    }
}

/// The shared emit tail: build the user projection over the outer relation
/// (`pk_coldefs` are the view's leading PK columns), append the final Map only
/// when the projection is not the identity, shard on exactly the view PK columns
/// in order when the circuit carries an output exchange (band / pure-range — the
/// compound-key alignment invariant shared with the range join builder; equi has
/// no exchange), then sink and return the pieces.
fn finish_exists_pieces(
    ctx: &ExistsCtx<'_>,
    mut cb: CircuitBuilder,
    sink_pre: NodeId,
    pk_coldefs: &[ColumnDef],
    shard: bool,
) -> Result<EmitPieces, GnitzSqlError> {
    let a_n = ctx.outer_schema.columns.len();
    let npk = pk_coldefs.len();
    let (final_cols, final_projection) = build_join_view_projection(
        &ctx.select.projection,
        &ctx.outer_alias_map,
        pk_coldefs,
        a_n,
        npk,
        |i| ctx.outer_schema.columns[i].clone(),
        "EXISTS view",
    )?;
    let is_identity = is_identity_projection(&final_projection, a_n, npk);
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
        reject_duplicate_column_names(&final_cols, "EXISTS view")?;
    }
    let view_pk: Vec<u32> = (0..npk as u32).collect();
    Ok((circuit, final_cols, view_pk))
}

/// The mark-view emit tail. `matched` (subquery has inner matches) and
/// `unmatched` are keyed by the correlation PK (`pk_coldefs`), payload = the
/// outer columns. Each branch binds the view's original WHERE (filter) and
/// projection (map) with the subquery node resolved to its constant truth value
/// on that branch, then the two are unioned — every outer row lands in exactly
/// one branch, so the result is the outer relation filtered/projected under the
/// per-row subquery value, weight-exact.
fn finish_mark_pieces(
    ctx: &ExistsCtx<'_>,
    mut cb: CircuitBuilder,
    matched: NodeId,
    unmatched: NodeId,
    pk_coldefs: &[ColumnDef],
    shard: bool,
) -> Result<EmitPieces, GnitzSqlError> {
    let npk = pk_coldefs.len();
    // The branch node's physical schema: the correlation-PK region, then the
    // outer columns at `npk..npk+outer_n`.
    let mut cols: Vec<ColumnDef> = pk_coldefs.to_vec();
    cols.extend(ctx.outer_schema.columns.iter().cloned());
    let branch_schema = Schema {
        columns: cols,
        pk_cols: (0..npk).collect(),
    };

    // The subquery node's truth value per branch: on `matched` the subquery has
    // inner matches, so the `[NOT] EXISTS`/`[NOT] IN` node itself is `!negated`.
    let (mark_matched, mark_unmatched) = if ctx.negated { (0, 1) } else { (1, 0) };
    let (m_out, out_cols) = emit_mark_branch(ctx, &mut cb, matched, mark_matched, &branch_schema, npk)?;
    let (u_out, _) = emit_mark_branch(ctx, &mut cb, unmatched, mark_unmatched, &branch_schema, npk)?;
    let mut out = cb.union(m_out, u_out);
    if shard {
        out = cb.shard(out, &(0..npk).collect::<Vec<_>>());
    }
    cb.sink(out);
    let circuit = cb.build();

    if !is_wildcard_projection(&ctx.select.projection) {
        reject_duplicate_column_names(&out_cols, "mark view")?;
    }
    let view_pk: Vec<u32> = (0..npk as u32).collect();
    Ok((circuit, out_cols, view_pk))
}

/// Emit one mark branch: bind the WHERE with the subquery at `mark` and apply it
/// as a filter (a WHERE that folds to a true constant keeps every row), then a
/// map producing the view output. The correlation-PK region is carried verbatim;
/// the map program writes only the payload slots (`items[npk..]`). Both branches
/// produce identical `out_cols` — only the folded constant differs.
fn emit_mark_branch(
    ctx: &ExistsCtx<'_>,
    cb: &mut CircuitBuilder,
    node: NodeId,
    mark: i64,
    branch_schema: &Schema,
    npk: usize,
) -> Result<(NodeId, Vec<ColumnDef>), GnitzSqlError> {
    let filtered = match &ctx.select.selection {
        Some(w) => {
            let bound = bind_single_table_mark(w, branch_schema, mark)?;
            match compile_filter_program(&bound, branch_schema)? {
                Some(prog) => cb.filter(node, Some(prog)),
                None => node, // WHERE folded to a true constant — keep every row
            }
        }
        None => node,
    };
    let (items, out_cols) = build_mark_projection(&ctx.select.projection, branch_schema, npk, ctx, mark)?;
    let program = compile_projection_map(&items[npk..], branch_schema)?;
    Ok((cb.map_expr(filtered, program), out_cols))
}

/// Build one branch's projection `(items, out_cols)` over `branch_schema`
/// (`[corr_pk…, outer cols…]`): the leading `npk` correlation-PK columns are
/// carried verbatim, `*`/`t.*` expands to the outer columns only (single outer
/// relation, not the synthetic PK), and every other item resolves through
/// `resolve_proj_col_with` with the subquery bound to `mark` — a projected
/// `EXISTS(…)` becomes a computed constant column.
fn build_mark_projection(
    projection: &[SelectItem],
    branch_schema: &Schema,
    npk: usize,
    ctx: &ExistsCtx<'_>,
    mark: i64,
) -> Result<(Vec<ProjItem>, Vec<ColumnDef>), GnitzSqlError> {
    let outer_n = ctx.outer_schema.columns.len();
    let mut items: Vec<ProjItem> = Vec::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    // Leading correlation-PK region, carried verbatim.
    for i in 0..npk {
        items.push(ProjItem::PassThrough { src_col: i });
        out_cols.push(branch_schema.columns[i].clone());
    }
    for (idx, item) in projection.iter().enumerate() {
        if projection_item_expr(item).is_none() {
            // `*` / `t.*` → the visible outer columns only (single outer
            // relation). A hidden outer key (EXISTS over a view with a synthetic
            // PK) is not re-admitted into this view's payload. `EXCEPT`/
            // `EXCLUDE`/`RENAME` rewrite by name; `REPLACE`/`ILIKE` reject.
            let rw = WildcardRewrite::for_item(
                item,
                |n| wildcard_name_is_visible(&branch_schema.columns[npk..npk + outer_n], n),
                "mark view",
            )?;
            for i in npk..npk + outer_n {
                let col = &branch_schema.columns[i];
                if col.is_hidden {
                    continue;
                }
                let Some(out) = rw.rewrite_column(col) else { continue };
                items.push(ProjItem::PassThrough { src_col: i });
                out_cols.push(out);
            }
            continue;
        }
        let (it, col) = resolve_proj_col_with(item, idx, branch_schema, |e| {
            bind_single_table_mark(e, branch_schema, mark)
        })?;
        items.push(it);
        out_cols.push(col);
    }
    Ok((items, out_cols))
}

/// Reject the same alias naming both the view's FROM relation and its
/// subquery's — one flat two-relation scope cannot disambiguate them. Shared by
/// the EXISTS/IN and scalar-subquery builders.
pub(crate) fn reject_alias_collision(outer_alias: &str, inner_alias: &str) -> Result<(), GnitzSqlError> {
    if outer_alias.eq_ignore_ascii_case(inner_alias) {
        return Err(GnitzSqlError::Bind(format!(
            "relation alias '{outer_alias}' is used by both the view FROM and its subquery; rename one"
        )));
    }
    Ok(())
}

/// Resolve a subquery's single plain FROM to `(tid, schema, alias)`: exactly
/// one relation, no JOINs, no derived table (the front door pre-compiles
/// derived tables only in the view's top-level FROM, so the lenient alias
/// extractor would mis-resolve one here). `shape_err` is the caller's full
/// unsupported-shape sentence; `ctx` names the surface in the name-extraction
/// error. Alias-clash checks stay in the callers (they interpolate different
/// relation names). Shared by the EXISTS/IN and scalar-subquery builders.
pub(crate) fn resolve_single_plain_from(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    from: &[sqlparser::ast::TableWithJoins],
    shape_err: &str,
    ctx: &str,
) -> Result<(u64, Rc<Schema>, String), GnitzSqlError> {
    if from.len() != 1
        || !from[0].joins.is_empty()
        || matches!(&from[0].relation, sqlparser::ast::TableFactor::Derived { .. })
    {
        return Err(GnitzSqlError::Unsupported(shape_err.into()));
    }
    let (name, alias) = extract_table_name_and_alias(&from[0].relation, ctx)?;
    let (tid, schema) = binder.resolve(client, &name)?;
    Ok((tid, schema, alias))
}

/// Split a subquery's WHERE conjuncts by which relation they reference,
/// resolved through the two-relation `alias_map` (outer at offset 0, width
/// `outer_n`):
///   outer == 0            → inner pre-filter (constant conjuncts included);
///   outer > 0, inner == 0 → reject with the caller's `hoist_err` (hoisting
///                           would be semantically wrong for NOT EXISTS, so no
///                           silent rewrite);
///   both sides            → AND-folded into the correlation accumulator.
/// Returns `(prefilters, correlation)`. Shared by the EXISTS/IN and
/// scalar-subquery builders; everything downstream of the split (the IN-pair
/// injection, envelope checks, classification policy) stays in the callers.
pub(crate) fn split_correlation_conjuncts<'e>(
    selection: Option<&'e Expr>,
    alias_map: &AliasMap,
    outer_n: usize,
    hoist_err: &str,
) -> Result<(Vec<&'e Expr>, Option<Expr>), GnitzSqlError> {
    let mut conjuncts: Vec<&Expr> = Vec::new();
    if let Some(w) = selection {
        flatten_conjuncts(w, &mut conjuncts);
    }
    let mut prefilters: Vec<&Expr> = Vec::new();
    let mut corr_acc: Option<Expr> = None;
    for &c in &conjuncts {
        let (mut outer_refs, mut inner_refs) = (0usize, 0usize);
        count_side_refs(c, alias_map, outer_n, &mut outer_refs, &mut inner_refs)?;
        if outer_refs == 0 {
            prefilters.push(c);
        } else if inner_refs == 0 {
            return Err(GnitzSqlError::Unsupported(hoist_err.into()));
        } else {
            and_into(&mut corr_acc, c.clone());
        }
    }
    Ok((prefilters, corr_acc))
}

/// AND-fold a conjunct list into one expression tree, left to right; `None`
/// for an empty list.
pub(crate) fn fold_and(conjuncts: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    let mut acc: Option<Expr> = None;
    for c in conjuncts {
        and_into(&mut acc, c);
    }
    acc
}

/// AND a conjunct into the accumulating correlation tree.
pub(crate) fn and_into(acc: &mut Option<Expr>, e: Expr) {
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
pub(crate) fn count_side_refs(
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
