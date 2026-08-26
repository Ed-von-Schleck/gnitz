//! The EXISTS/IN semi-, anti-, and mark-join lowering shells, driving the
//! AST-free join primitives in `join.rs` (`emit_equi_join_terms`, the range
//! prologue, `positive_diff`, the pure-range threshold) from the decorrelated HIR
//! `Join{Semi|Anti|Mark}` node (`SegInput` layouts, `JoinClass`, `ProjEntry`).
//! Equi correlations key the view by the synthetic `_join_pk` (no output
//! exchange); band and pure-range correlations re-key onto the outer source PK and
//! ride the mandatory output exchange.

use super::super::{ColId, EqPair, HirExpr, HirRange, HirRef, JoinClass, ProjEntry, RelExpr};
use super::join::{
    band_pi_preserved, band_union_schema, build_pure_range_threshold, emit_equi_join_terms, join_pk_coldefs,
    normalize_to_ab, pure_range_unmatched, range_prologue, resolve_eq_cols, side_target_tcs, EquiKeys, EquiSide,
    RangePrologue,
};
use super::prims::rekey_on_source_pk;
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
use gnitz_core::{CircuitBuilder, ColumnDef, NodeId, Schema};
use std::collections::HashSet;
use std::rc::Rc;

/// The branch(es) an EXISTS/IN view emits, as one composition over the weight-exact
/// null-fill set `ν = positive_part(A − π_A(inner)) = w_A·[S=0]` — the unmatched
/// outer rows at their true multiplicity. The three decorrelation kinds are three
/// compositions of that one ν, so they are stated here together rather than as a
/// per-kind branch tree at each call site:
///
/// ```text
/// Anti = ν            Semi = A − ν            Mark = (A − ν, ν)
/// ```
///
/// Returns `(primary, unmatched?)`, `unmatched` being `Some` exactly for `Mark`.
/// Node allocation order is fixed across all three (`positive_diff`, then — unless
/// Anti short-circuits — `negate` + `union`), so adding a kind never renumbers
/// another kind's circuit.
fn exists_branches(cb: &mut CircuitBuilder, a_all: NodeId, pi_a: NodeId, kind: JoinType) -> (NodeId, Option<NodeId>) {
    let nu = cb.positive_diff(a_all, pi_a); // ν, keyed like a_all
    if kind == JoinType::Anti {
        return (nu, None);
    }
    let neg = cb.negate(nu);
    let semi = cb.union(neg, a_all); // A − ν
    (semi, (kind == JoinType::Mark).then_some(nu))
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
fn emit_exists_circuit(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    source: &Rc<RelExpr>,
    left_prefilter: &[HirExpr],
    items: &[ProjEntry],
    view_id: u64,
) -> Result<ExistsCircuit, GnitzSqlError> {
    let RelExpr::Join {
        left, right, kind, on, ..
    } = source.as_ref()
    else {
        unreachable!("emit_exists_circuit receives a Join");
    };
    let class = on.class()?;

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

    let left_in = resolve_input(chain, memo, left, &live)?;
    let (inner_preds, inner_src) = split_filter(right);
    let right_in = seginput_of_get(inner_src)
        .ok_or_else(|| GnitzSqlError::Internal("EXISTS/IN inner relation is not a base Get".into()))?;

    // Self-collision: EXISTS/IN over the outer's own relation must read the inner
    // as a distinct source (single-source-per-epoch) — wrap the colliding inner
    // side in a pass-through segment (the shared source-collision rule).
    let mut inputs = [left_in, right_in];
    resolve_collisions(chain, &mut inputs, &[left, right], false)?;
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

    let core = ExistsCore {
        left_in: &left_in,
        right_in: &right_in,
        a_local,
        b_local,
        class,
        kind: *kind,
    };
    // A range correlation re-keys onto the outer source PK and rides the mandatory
    // range output exchange; an equi one is keyed by `_join_pk` and needs none.
    let (primary, unmatched, out_pk_cols, shard) = match &class.range {
        Some(range) => {
            let (p, u, pk) = core.range(&mut cb, range)?;
            (p, u, pk, true)
        }
        None => {
            let (p, u, pk) = core.equi(&mut cb)?;
            (p, u, pk, false)
        }
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

/// The resolved inputs an EXISTS/IN circuit core emits from: both sides (with their
/// local WHEREs already fused into `a_local` / `b_local`), the classified
/// correlation, and the decorrelation `kind` that selects the branch composition.
/// Bundled because the equi and range cores take the same six values and differ
/// only in which half of `class` they read.
struct ExistsCore<'a> {
    left_in: &'a SegInput,
    right_in: &'a SegInput,
    a_local: NodeId,
    b_local: NodeId,
    class: &'a JoinClass,
    kind: JoinType,
}

impl ExistsCore<'_> {
    /// Equi correlation: the symmetric 2-term join over
    /// the NULL-gated sides, `π_A(inner)`, and `a_all` (the full outer re-keyed,
    /// reusing `reindex_a` when the key is NOT NULL). Output keyed by `_join_pk`, no
    /// output exchange. Returns `(primary, unmatched?, _join_pk cols)`.
    fn equi(&self, cb: &mut CircuitBuilder) -> Result<(NodeId, Option<NodeId>, Vec<ColumnDef>), GnitzSqlError> {
        let ExistsCore {
            left_in,
            right_in,
            a_local,
            b_local,
            class,
            kind,
        } = *self;
        let eq: &[EqPair] = &class.eq;
        let k = eq.len();
        let a_n = left_in.schema.columns.len();
        let b_n = right_in.schema.columns.len();
        let EquiKeys {
            left: left_cols,
            right: right_cols,
            tcs: target_tcs,
        } = resolve_eq_cols(eq, left_in, right_in)?;
        let left_target_tcs = side_target_tcs(&left_cols, &left_in.schema.columns, &target_tcs);
        let right_target_tcs = side_target_tcs(&right_cols, &right_in.schema.columns, &target_tcs);
        let keep_a: Vec<usize> = (0..a_n).collect();
        let keep_b: Vec<usize> = (0..b_n).collect();

        let side_a = EquiSide {
            input: a_local,
            cols: &left_cols,
            target_tcs: &left_target_tcs,
            coldefs: &left_in.schema.columns,
            keep: &keep_a,
        };
        let side_b = EquiSide {
            input: b_local,
            cols: &right_cols,
            target_tcs: &right_target_tcs,
            coldefs: &right_in.schema.columns,
            keep: &keep_b,
        };
        let terms = emit_equi_join_terms(cb, side_a, side_b)?;

        // π_A(inner): project each term straight to [_join_pk × k, A].
        let pa_ab = cb.map(terms.join_ab, &(k..k + a_n).collect::<Vec<_>>());
        let pa_ba = cb.map(terms.join_ba, &(k + b_n..k + b_n + a_n).collect::<Vec<_>>());
        let pi_a = cb.union(pa_ab, pa_ba);

        // A_all: the full (locally filtered, NULL keys included) outer input,
        // re-keyed — reusing `reindex_a` on a NOT NULL key, where the gate was a
        // no-op.
        let a_all = terms.a_all(cb, side_a);
        let (primary, unmatched) = exists_branches(cb, a_all, pi_a, kind);
        Ok((primary, unmatched, join_pk_coldefs(&target_tcs)))
    }

    /// Range correlation — band (`n_eq ≥ 1`) or pure range (`n_eq == 0`). Both re-key
    /// onto the outer source PK and ride the range output exchange. Returns
    /// `(primary, unmatched?, _src_pk cols)`.
    fn range(
        &self,
        cb: &mut CircuitBuilder,
        range: &HirRange,
    ) -> Result<(NodeId, Option<NodeId>, Vec<ColumnDef>), GnitzSqlError> {
        let ExistsCore {
            left_in,
            right_in,
            a_local,
            b_local,
            class,
            kind,
        } = *self;
        let eq: &[EqPair] = &class.eq;
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
            let int_a = cb.worker_filter(reindex_a);
            let trace_a = cb.integrate_trace(int_a);
            // Only the branches that subtract from A (`A − matched`) need the passthrough.
            let want_a_pass = kind != JoinType::Semi;
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
            // The threshold decides existence directly, so these are the three
            // compositions `exists_branches` states over ν — spelled here against
            // `matched` / `A − matched` because the threshold has no `π_A(inner)`.
            match kind {
                JoinType::Anti => (unmatched()?, None),
                JoinType::Mark => (thr.matched, Some(unmatched()?)),
                _ => (thr.matched, None),
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
            exists_branches(cb, a_all, proj_a, kind)
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
}

/// Lower a decorrelated `Project(Filter?(Join{Semi|Anti}))` to circuit pieces.
pub(crate) fn lower_semi_anti_view(
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
    } = emit_exists_circuit(chain, memo, source, fpreds, items, view_id)?;

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
pub(crate) fn lower_mark_view(
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
    } = emit_exists_circuit(chain, memo, source, &[], items, view_id)?;
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
    let out_cols = branch.out_cols();
    let m_out = branch.emit(&mut cb, matched, 1)?;
    let u_out = branch.emit(&mut cb, unmatched, 0)?;
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
    /// The branch output schema: the carried PK region then the projected items.
    /// Branch-invariant by construction — only the `0/1` mark constant differs
    /// between the two branches, and it appears nowhere in the column defs.
    fn out_cols(&self) -> Vec<ColumnDef> {
        let mut cols: Vec<ColumnDef> = self.pk_cols.to_vec();
        cols.extend(self.items.iter().map(|i| i.out.def.clone()));
        cols
    }

    /// Emit one branch: substitute the mark column by `mark_val`, apply the WHERE
    /// (folds to a true constant → no filter), then map the projection. The leading
    /// PK region is carried verbatim; the map program writes only the payload.
    fn emit(&self, cb: &mut CircuitBuilder, node: NodeId, mark_val: i64) -> Result<NodeId, GnitzSqlError> {
        let subst = |e: &HirExpr| subst_mark_lit(e, self.mark_id, mark_val);
        let subst_preds: Vec<HirExpr> = self.where_preds.iter().map(subst).collect();
        let filtered = emit_filter(cb, node, subst_preds.iter(), self.layout, &self.schema.columns)?;

        let proj_items: Vec<ProjItem> = self
            .items
            .iter()
            .map(|item| {
                Ok(ProjItem::from_bound(physical::resolve_refs(
                    &subst(&item.expr),
                    self.layout,
                )?))
            })
            .collect::<Result<_, GnitzSqlError>>()?;
        let program = compile_projection_map(&proj_items, self.schema)?;
        Ok(cb.map_expr(filtered, program))
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
        // The mark is non-nullable `0/1`, so the substitution never has to fold a
        // null test — every leaf's `IS [NOT] NULL` passes through unchanged.
        &|r, want_null| Ok(crate::bind::fold_null_test(true, r.clone(), want_null)),
    ) {
        Ok(out) => out,
        Err(never) => match never {},
    }
}
