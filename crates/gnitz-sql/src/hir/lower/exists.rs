//! The EXISTS/IN semi-, anti-, and mark-join lowering shells, driving the
//! AST-free join primitives in `join.rs` (`emit_equi_join_terms`, the range
//! prologue, `positive_diff`, the pure-range threshold) from the decorrelated HIR
//! `Join{Semi|Anti|Mark}` node (`SegInput` layouts, `JoinClass`, `ProjEntry`).
//! Equi correlations key the view by the synthetic `_join_pk` (no output
//! exchange); band and pure-range correlations re-key onto the outer source PK and
//! ride the mandatory output exchange.

use super::super::{ColId, EqPair, HirExpr, HirRange, HirRef, ProjEntry, RelExpr};
use super::join::{
    band_pi_preserved, band_union_schema, build_pure_range_threshold, emit_equi_join_terms, join_pk_coldefs,
    normalize_to_ab, pure_range_unmatched, range_prologue, resolve_eq_cols, side_target_tcs, EquiSide, RangePrologue,
};
use super::prims::{build_reindex_program, rekey_on_source_pk};
use super::{
    apply_projection, collect_live_cols, emit_filter, key_region_layout, resolve_collisions, resolve_input,
    resolve_projection, seginput_of_get, split_filter, CutMemo, SegInput,
};
use crate::codec::project_schema::{compile_projection_map, ProjItem};
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::hir::guards::reject_pure_range_threshold_tc;
use crate::hir::physical;
use crate::hir::JoinType;
use crate::ir::BExpr;
use crate::validate::reject_column_overflow;
use gnitz_core::{CircuitBuilder, ColumnDef, GnitzClient, NodeId, Schema};
use std::collections::HashSet;
use std::rc::Rc;

/// The shared semi + anti composition over the weight-exact null-fill set
/// `ν = positive_part(A − π_A(inner)) = w_A·[S=0]`: the anti-join is ν itself; the
/// semi-join its complement `A − ν`. Returns `(semi, anti)` over one shared ν.
fn semi_and_anti(cb: &mut CircuitBuilder, a_all: NodeId, pi_a: NodeId) -> (NodeId, NodeId) {
    let nu = cb.positive_diff(a_all, pi_a); // ν, keyed like a_all
    let neg = cb.negate(nu);
    (cb.union(neg, a_all), nu)
}

/// One polarity of [`semi_and_anti`] for the filter view (the anti case builds no
/// dangling semi nodes).
fn semi_or_anti(cb: &mut CircuitBuilder, a_all: NodeId, pi_a: NodeId, negated: bool) -> NodeId {
    if negated {
        cb.positive_diff(a_all, pi_a)
    } else {
        semi_and_anti(cb, a_all, pi_a).0
    }
}

/// The resolved core of one EXISTS/IN circuit: the two inputs, the tagged deltas
/// with prefilters applied, and the filter/mark result nodes plus the output PK
/// columns and whether an output exchange is needed.
struct ExistsCircuit {
    cb: CircuitBuilder,
    /// The semi/anti node (filter) or the matched branch (mark).
    primary: NodeId,
    /// The unmatched branch (mark only).
    unmatched: Option<NodeId>,
    out_pk_cols: Vec<ColumnDef>,
    shard: bool,
    left_in: SegInput,
}

/// Resolve the decorrelated `Join{Semi|Anti|Mark}` node into a built circuit up to
/// (but not including) the projection tail. `left_prefilter` is the outer-local
/// WHERE (the left-input prefilter for a semi/anti view; empty for a mark view,
/// whose WHERE is applied post-mark per branch).
#[allow(clippy::too_many_arguments)]
fn emit_exists_circuit(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    source: &Rc<RelExpr>,
    left_prefilter: &[HirExpr],
    items: &[ProjEntry],
    view_id: u64,
    is_mark: bool,
) -> Result<ExistsCircuit, GnitzSqlError> {
    let RelExpr::Join {
        left, right, kind, on, ..
    } = source.as_ref()
    else {
        unreachable!("emit_exists_circuit receives a Join");
    };
    let class = on.class()?;
    let negated = matches!(kind, JoinType::Anti);

    // Left-input live set (projection + prefilter + correlation-key left cols) for
    // a cut left input (a nested subquery); a bare Get reads everything.
    let mut live: HashSet<ColId> = HashSet::new();
    collect_live_cols(items.iter().map(|i| &i.expr).chain(left_prefilter), &mut live);
    for p in &class.eq {
        live.insert(p.left);
    }
    if let Some(r) = &class.range {
        live.insert(r.left);
    }

    let left_in = resolve_input(client, chain, memo, left, &live)?;
    let (inner_preds, inner_src) = split_filter(right);
    let right_in = seginput_of_get(inner_src)
        .ok_or_else(|| GnitzSqlError::Plan("internal: EXISTS/IN inner relation is not a base Get".into()))?;

    // Self-collision: EXISTS/IN over the outer's own relation must read the inner
    // as a distinct source (single-source-per-epoch) — wrap the colliding inner
    // side in a pass-through segment (the shared source-collision rule).
    let mut inputs = [left_in, right_in];
    resolve_collisions(client, chain, &mut inputs, &[left, right], false)?;
    let [left_in, right_in] = inputs;

    let a_n = left_in.schema.columns.len();
    let b_n = right_in.schema.columns.len();
    match &class.range {
        None => reject_column_overflow("EXISTS view intermediate", class.eq.len() + a_n + b_n)?,
        Some(range) => {
            if class.eq.is_empty() {
                reject_pure_range_threshold_tc(
                    range.tc,
                    "EXISTS/IN pure-range correlation",
                    "use a narrower range column or add an equality conjunct",
                )?;
            }
            reject_column_overflow(
                "EXISTS view intermediate",
                left_in.schema.pk_cols.len() + class.eq.len() + 1 + a_n + b_n,
            )?;
        }
    }

    // Tagged delta per side with its local / inner-local WHERE fused as a prefilter
    // (before the join terms) — through the shared `emit_filter` home.
    let mut cb = CircuitBuilder::new(view_id, 0);
    let a_delta = cb.input_delta_tagged(left_in.tid);
    let a_local = emit_filter(
        &mut cb,
        a_delta,
        left_prefilter,
        &left_in.layout,
        &left_in.schema.columns,
    )?;
    let b_delta = cb.input_delta_tagged(right_in.tid);
    let b_local = emit_filter(
        &mut cb,
        b_delta,
        inner_preds,
        &right_in.layout,
        &right_in.schema.columns,
    )?;

    let (primary, unmatched, out_pk_cols, shard) = if let Some(range) = &class.range {
        let (p, u, pk) = exists_range_core(
            &mut cb, &left_in, &right_in, a_local, b_local, &class.eq, range, is_mark, negated,
        )?;
        (p, u, pk, true)
    } else {
        let (p, u, pk) = exists_equi_core(
            &mut cb, &left_in, &right_in, a_local, b_local, &class.eq, is_mark, negated,
        )?;
        (p, u, pk, false)
    };
    Ok(ExistsCircuit {
        cb,
        primary,
        unmatched,
        out_pk_cols,
        shard,
        left_in,
    })
}

/// Equi correlation: the symmetric 2-term join over
/// the NULL-gated sides, `π_A(inner)`, and `a_all` (the full outer re-keyed,
/// reusing `reindex_a` when the key is NOT NULL). Output keyed by `_join_pk`, no
/// output exchange. Returns `(primary, unmatched?, _join_pk cols)`.
#[allow(clippy::too_many_arguments)]
fn exists_equi_core(
    cb: &mut CircuitBuilder,
    left_in: &SegInput,
    right_in: &SegInput,
    a_local: NodeId,
    b_local: NodeId,
    eq: &[EqPair],
    is_mark: bool,
    negated: bool,
) -> Result<(NodeId, Option<NodeId>, Vec<ColumnDef>), GnitzSqlError> {
    let k = eq.len();
    let a_n = left_in.schema.columns.len();
    let b_n = right_in.schema.columns.len();
    let (left_cols, right_cols, target_tcs) = resolve_eq_cols(eq, left_in, right_in)?;
    let left_target_tcs = side_target_tcs(&left_cols, &left_in.schema.columns, &target_tcs);
    let right_target_tcs = side_target_tcs(&right_cols, &right_in.schema.columns, &target_tcs);
    let keep_a: Vec<usize> = (0..a_n).collect();
    let keep_b: Vec<usize> = (0..b_n).collect();

    let terms = emit_equi_join_terms(
        cb,
        EquiSide {
            input: a_local,
            cols: &left_cols,
            target_tcs: &left_target_tcs,
            coldefs: &left_in.schema.columns,
            keep: &keep_a,
        },
        EquiSide {
            input: b_local,
            cols: &right_cols,
            target_tcs: &right_target_tcs,
            coldefs: &right_in.schema.columns,
            keep: &keep_b,
        },
    )?;

    // π_A(inner): project each term straight to [_join_pk × k, A].
    let pa_ab = cb.map(terms.join_ab, &(k..k + a_n).collect::<Vec<_>>());
    let pa_ba = cb.map(terms.join_ba, &(k + b_n..k + b_n + a_n).collect::<Vec<_>>());
    let pi_a = cb.union(pa_ab, pa_ba);

    // a_all re-keys the full (locally filtered, NULL keys included) outer input,
    // reusing reindex_a on a NOT NULL key (the NULL gate was a no-op there).
    let a_all = if terms.a_nullable {
        cb.map_reindex(
            a_local,
            &left_cols,
            &left_target_tcs,
            build_reindex_program(left_in.schema.columns.len()),
        )
    } else {
        terms.reindex_a
    };

    let out_pk_cols = join_pk_coldefs(&target_tcs);
    if is_mark {
        let (matched, unmatched) = semi_and_anti(cb, a_all, pi_a);
        Ok((matched, Some(unmatched), out_pk_cols))
    } else {
        Ok((semi_or_anti(cb, a_all, pi_a, negated), None, out_pk_cols))
    }
}

/// Range correlation — band (`n_eq ≥ 1`) or pure range (`n_eq == 0`). Both re-key
/// onto the outer source PK and ride the range output exchange. Returns
/// `(primary, unmatched?, _src_pk cols)`.
#[allow(clippy::too_many_arguments)]
fn exists_range_core(
    cb: &mut CircuitBuilder,
    left_in: &SegInput,
    right_in: &SegInput,
    a_local: NodeId,
    b_local: NodeId,
    eq: &[EqPair],
    range: &HirRange,
    is_mark: bool,
    negated: bool,
) -> Result<(NodeId, Option<NodeId>, Vec<ColumnDef>), GnitzSqlError> {
    let a_n = left_in.schema.columns.len();
    let n_eq = eq.len();
    let k = n_eq + 1;

    let RangePrologue {
        reindex_a,
        reindex_b,
        left_key_nullable,
        left_reindex_cols,
        all_tcs,
        rel_ab,
        rel_ba,
    } = range_prologue(cb, left_in, right_in, a_local, b_local, eq, range)?;

    let (primary, unmatched): (NodeId, Option<NodeId>) = if n_eq == 0 {
        // Pure range: the one-row threshold m = MAX/MIN(b.range) decides existence.
        let int_a = cb.partition_filter(reindex_a);
        let trace_a = cb.integrate_trace(int_a);
        let want_a_pass = negated || is_mark;
        let thr = build_pure_range_threshold(
            cb,
            &left_in.schema,
            range.tc,
            range.op,
            reindex_b,
            int_a,
            trace_a,
            want_a_pass,
        );
        // `A − matched` (∪ NULL-range-key rows) — the anti / mark unmatched branch.
        let mut unmatched = || {
            pure_range_unmatched(
                cb,
                &thr,
                left_key_nullable,
                a_local,
                &left_reindex_cols,
                &left_in.schema,
            )
        };
        if is_mark {
            (thr.matched, Some(unmatched()?))
        } else if !negated {
            (thr.matched, None)
        } else {
            (unmatched()?, None)
        }
    } else {
        // Band: the eq-prefix scatter co-locates both sides, so the inner join, its
        // π_A re-key, a_all, and the clamp are all partition-local.
        let trace_a = cb.integrate_trace(reindex_a);
        let trace_b = cb.integrate_trace(reindex_b);
        let join_ab = cb.join_with_trace_range_node(reindex_a, trace_b, n_eq as u8, rel_ab);
        let join_ba = cb.join_with_trace_range_node(reindex_b, trace_a, n_eq as u8, rel_ba);
        let merged = normalize_to_ab(cb, join_ab, join_ba, k, a_n, right_in.schema.columns.len());

        // π_A(inner) keyed by the outer source PK.
        let union_schema = band_union_schema(&all_tcs, &left_in.schema, &right_in.schema);
        let a_pk_in_union: Vec<usize> = left_in.schema.pk_cols.iter().map(|&p| k + p).collect();
        let proj_a = band_pi_preserved(cb, merged, union_schema.columns.len(), &a_pk_in_union, k, 0, a_n);
        let a_all = rekey_on_source_pk(cb, a_local, &left_in.schema);
        if is_mark {
            let (matched, unmatched) = semi_and_anti(cb, a_all, proj_a);
            (matched, Some(unmatched))
        } else {
            (semi_or_anti(cb, a_all, proj_a, negated), None)
        }
    };

    // View PK = the outer source PK, hidden (it also rides the payload verbatim).
    let src_pk_coldefs: Vec<ColumnDef> = left_in
        .schema
        .pk_cols
        .iter()
        .map(|&c| {
            ColumnDef::new(
                left_in.schema.columns[c].name.clone(),
                left_in.schema.columns[c].type_code.reindex_output_type(),
                false,
            )
            .hidden()
        })
        .collect();
    Ok((primary, unmatched, src_pk_coldefs))
}

/// Lower a decorrelated `Project(Filter?(Join{Semi|Anti}))` to circuit pieces.
pub(crate) fn lower_semi_anti_view(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    fpreds: &[HirExpr],
    source: &Rc<RelExpr>,
    view_id: u64,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let ExistsCircuit {
        mut cb,
        primary,
        out_pk_cols,
        shard,
        left_in,
        ..
    } = emit_exists_circuit(client, chain, memo, source, fpreds, items, view_id, false)?;

    let npk = out_pk_cols.len();
    let a_n = left_in.schema.columns.len();
    let mut proj_layout: Vec<ColId> = vec![ColId::NONE; npk];
    proj_layout.extend(left_in.layout.iter().copied());
    let (final_projection, final_cols) = resolve_projection(items, &proj_layout, out_pk_cols)?;
    let mut sink_input = apply_projection(&mut cb, primary, &final_projection, a_n, npk);
    if shard {
        sink_input = cb.shard(sink_input, &(0..npk).collect::<Vec<_>>());
    }
    cb.sink(sink_input);
    let circuit = cb.build();
    let view_pk: Vec<u32> = (0..npk as u32).collect();
    Ok(((circuit, final_cols, view_pk), key_region_layout(npk, items)))
}

/// Lower a decorrelated `Project(Filter?(Join{Mark}))` to circuit pieces: the
/// matched / unmatched branches each bind the (post-mark) WHERE + projection with
/// the mark column substituted by its `0/1` constant, then union.
#[allow(clippy::too_many_arguments)]
pub(crate) fn lower_mark_view(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    fpreds: &[HirExpr],
    source: &Rc<RelExpr>,
    view_id: u64,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let mark_id = {
        let RelExpr::Join { mark, .. } = source.as_ref() else {
            unreachable!("lower_mark_view receives a Join");
        };
        mark.as_ref().expect("Mark join carries a mark column").id
    };
    // No left prefilter: the whole WHERE is applied post-mark, per branch.
    let ExistsCircuit {
        mut cb,
        primary: matched,
        unmatched,
        out_pk_cols,
        shard,
        left_in,
    } = emit_exists_circuit(client, chain, memo, source, &[], items, view_id, true)?;
    let unmatched = unmatched.expect("mark core returns an unmatched branch");

    let npk = out_pk_cols.len();
    // The branch physical schema: the PK region, then the outer columns.
    let mut branch_cols: Vec<ColumnDef> = out_pk_cols.clone();
    branch_cols.extend(left_in.schema.columns.iter().cloned());
    let branch_schema = Schema {
        columns: branch_cols,
        pk_cols: (0..npk).collect(),
    };
    let mut branch_layout: Vec<ColId> = vec![ColId::NONE; npk];
    branch_layout.extend(left_in.layout.iter().copied());

    let branch = MarkBranch {
        mark_id,
        where_preds: fpreds,
        items,
        layout: &branch_layout,
        schema: &branch_schema,
        pk_cols: &out_pk_cols,
    };
    let (m_out, out_cols) = branch.emit(&mut cb, matched, 1)?;
    let (u_out, _) = branch.emit(&mut cb, unmatched, 0)?;
    let mut out = cb.union(m_out, u_out);
    if shard {
        out = cb.shard(out, &(0..npk).collect::<Vec<_>>());
    }
    cb.sink(out);
    let circuit = cb.build();
    let view_pk: Vec<u32> = (0..npk as u32).collect();
    Ok(((circuit, out_cols, view_pk), key_region_layout(npk, items)))
}

/// Everything a mark branch emits from, shared verbatim by the matched and
/// unmatched branch — only the `0/1` mark constant differs between them.
struct MarkBranch<'a> {
    mark_id: ColId,
    where_preds: &'a [HirExpr],
    items: &'a [ProjEntry],
    layout: &'a [ColId],
    schema: &'a Schema,
    pk_cols: &'a [ColumnDef],
}

impl MarkBranch<'_> {
    /// Emit one branch: substitute the mark column by `mark_val`, apply the WHERE
    /// (folds to a true constant → no filter), then map the projection. The leading
    /// PK region is carried verbatim; the map program writes only the payload.
    fn emit(
        &self,
        cb: &mut CircuitBuilder,
        node: NodeId,
        mark_val: i64,
    ) -> Result<(NodeId, Vec<ColumnDef>), GnitzSqlError> {
        let subst = |e: &HirExpr| subst_mark_lit(e, self.mark_id, mark_val);
        let subst_preds: Vec<HirExpr> = self.where_preds.iter().map(subst).collect();
        let filtered = emit_filter(cb, node, subst_preds.iter(), self.layout, &self.schema.columns)?;

        let mut out_cols: Vec<ColumnDef> = self.pk_cols.to_vec();
        let mut proj_items: Vec<ProjItem> = Vec::with_capacity(self.items.len());
        for item in self.items {
            proj_items.push(ProjItem::from_bound(physical::resolve_refs(
                &subst(&item.expr),
                self.layout,
            )?));
            out_cols.push(item.out.def.clone());
        }
        let program = compile_projection_map(&proj_items, self.schema)?;
        Ok((cb.map_expr(filtered, program), out_cols))
    }
}

/// Substitute `ColRef(Col(mark_id))` with `LitInt(val)` throughout an expression —
/// the mark column's per-branch constant. (The mark is non-nullable `0/1`, so it
/// never appears under `IS [NOT] NULL`; every other leaf passes through.)
fn subst_mark_lit(e: &HirExpr, mark_id: ColId, val: i64) -> HirExpr {
    match e.try_expand_leaves::<std::convert::Infallible>(
        &|r| {
            Ok(match r {
                HirRef::Col(id) if *id == mark_id => BExpr::LitInt(val),
                _ => BExpr::ColRef(r.clone()),
            })
        },
        &|r, want_null| {
            Ok(if want_null {
                BExpr::IsNull(r.clone())
            } else {
                BExpr::IsNotNull(r.clone())
            })
        },
    ) {
        Ok(out) => out,
        Err(never) => match never {},
    }
}
