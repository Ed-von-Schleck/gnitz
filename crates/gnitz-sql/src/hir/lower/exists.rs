//! The EXISTS/IN semi-, anti-, and mark-join lowering shells, driving the
//! AST-free join primitives in [`super::joincore`] (the equi and range prologues,
//! `positive_diff`, the pure-range threshold) from the decorrelated HIR
//! `Join{Semi|Anti|Mark}` node.
//! Equi correlations key the view by the synthetic `_join_pk` (no output
//! exchange); band and pure-range correlations re-key onto the outer source PK and
//! ride the mandatory output exchange.

use super::super::split_filter;
use super::super::{ColId, HirExpr, HirRange, HirRef, JoinClass, ProjEntry, RelExpr};
use super::joincore::{
    band_pi_preserved, build_pure_range_threshold, equi_prologue, pure_range_unmatched, range_prologue,
    rekey_pure_range_a, src_pk_coldefs, EquiInput,
};
use super::prims::rekey_aux_on_source_pk;
use super::{
    collect_live_cols, emit_filter, join_sides, key_region_layout, project_tail, resolve_collisions, resolve_in_place,
    resolve_input, CutMemo, Demand, Frame, JoinSide,
};
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::hir::guards::reject_pure_range_threshold_tc;
use crate::hir::JoinType;
use crate::ir::BExpr;
use gnitz_core::{CircuitBuilder, ColumnDef, NodeId};
use std::collections::HashSet;
use std::rc::Rc;

/// What an EXISTS/IN circuit hands its tail: one branch for semi/anti, two for a
/// mark join. The kind decides which, so the tail reads the shape off the type
/// rather than off an `Option` it must agree with.
enum ExistsBranches {
    SemiAnti(NodeId),
    Mark { matched: NodeId, unmatched: NodeId },
}

/// The branch(es) an EXISTS/IN view emits, as one composition over the weight-exact
/// null-fill set `ν = positive_part(A − π_A(inner)) = w_A·[S=0]` — the unmatched
/// outer rows at their true multiplicity. The three decorrelation kinds are three
/// compositions of that one ν, stated here together rather than as a per-kind
/// branch tree at each call site:
///
/// ```text
/// Anti = ν            Semi = A − ν            Mark = (A − ν, ν)
/// ```
fn exists_branches(cb: &mut CircuitBuilder, a_all: NodeId, pi_a: NodeId, kind: JoinType) -> ExistsBranches {
    let nu = cb.positive_diff(a_all, pi_a); // ν, keyed like a_all
    if kind == JoinType::Anti {
        return ExistsBranches::SemiAnti(nu);
    }
    let neg = cb.negate(nu);
    let semi = cb.union(neg, a_all); // A − ν
    match kind {
        JoinType::Mark(_) => ExistsBranches::Mark { matched: semi, unmatched: nu },
        _ => ExistsBranches::SemiAnti(semi),
    }
}

/// The resolved core of one EXISTS/IN circuit: the two inputs, the tagged deltas
/// with prefilters applied, and the filter/mark result nodes plus the output PK
/// columns and whether an output exchange is needed.
struct ExistsCircuit {
    cb: CircuitBuilder,
    branches: ExistsBranches,
    out_pk_cols: Vec<ColumnDef>,
    shard: bool,
    /// The outer side under the keep rule — its kept ids and defs are the payload
    /// every branch below the circuit projects from.
    left: JoinSide,
}

/// Resolve the decorrelated `Join{Semi|Anti|Mark}` node into a built circuit up to
/// (but not including) the projection tail.
fn emit_exists_circuit(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    source: &Rc<RelExpr>,
    where_preds: &[HirExpr],
    items: &[ProjEntry],
) -> Result<ExistsCircuit, GnitzSqlError> {
    let RelExpr::Join { left, right, kind, on, .. } = source.as_ref() else {
        unreachable!("emit_exists_circuit receives a Join");
    };
    let class = on.class()?;
    // A semi/anti view fuses its WHERE into the outer delta before the join terms;
    // a mark view applies it per branch after the mark. Derived from `kind` here,
    // so the two shells cannot hand it to the wrong stage.
    let (left_prefilter, post_mark) = match kind {
        JoinType::Mark(_) => (&[][..], where_preds),
        _ => (where_preds, &[][..]),
    };

    // Left-input live set (projection + WHERE + correlation-key left cols) for a
    // cut left input (a nested subquery); a bare Get reads everything.
    let mut live: HashSet<ColId> = HashSet::new();
    Demand { items, where_preds }.refs(&mut live);
    live.extend(class.key_cols(true));

    let left_in = resolve_input(chain, memo, left, &live)?;
    let (inner_preds, inner_src) = split_filter(right);
    let right_in = resolve_in_place(chain, memo, inner_src)?
        .ok_or_else(|| GnitzSqlError::Internal("EXISTS/IN inner relation is not read in place".into()))?;

    // Self-collision: EXISTS/IN over the outer's own relation must read the inner
    // as a distinct source (single-source-per-epoch) — wrap the colliding inner
    // side in a pass-through segment (the shared source-collision rule). The
    // wrapper is the INNER side, so its demand is the correlation's right keys
    // plus the inner-local WHERE.
    let mut right_live: HashSet<ColId> = HashSet::new();
    collect_live_cols(inner_preds, &mut right_live);
    right_live.extend(class.key_cols(false));
    let mut inputs = [left_in, right_in];
    resolve_collisions(chain, &mut inputs, &[live, right_live], false)?;

    // A prefilter is consumed before the reindex, so only a post-mark WHERE joins
    // the projection in the keep demand.
    let down = Demand { items, where_preds: post_mark };
    let sides = join_sides(down, class, *kind, inputs);

    // A pure-range correlation (no equality prefix) decides existence from a
    // MIN/MAX threshold, which has only an 8-byte accumulator.
    if let Some(range) = class.range.filter(|_| class.eq.is_empty()) {
        reject_pure_range_threshold_tc(
            range.tc,
            "EXISTS/IN pure-range correlation",
            "use a narrower range column or add an equality conjunct",
        )?;
    }

    // Tagged delta per side with its local / inner-local WHERE fused as a prefilter
    // (before the join terms) — through the shared `emit_filter` home.
    let mut cb = CircuitBuilder::new(0);
    let a_delta = cb.input_delta_tagged(sides[0].seg.tid);
    let a_local = emit_filter(
        &mut cb,
        a_delta,
        left_prefilter,
        &Frame::of(&sides[0].seg.layout, &sides[0].seg.schema),
    )?;
    let b_delta = cb.input_delta_tagged(sides[1].seg.tid);
    let b_local = emit_filter(
        &mut cb,
        b_delta,
        inner_preds,
        &Frame::of(&sides[1].seg.layout, &sides[1].seg.schema),
    )?;

    let core = ExistsCore {
        sides: &sides,
        a_local,
        b_local,
        class,
        kind: *kind,
    };
    // A range correlation re-keys onto the outer source PK and rides the mandatory
    // range output exchange; an equi one is keyed by `_join_pk` and needs none.
    let (branches, out_pk_cols, shard) = match &class.range {
        Some(range) => {
            let (b, pk) = core.range(&mut cb, range)?;
            (b, pk, true)
        }
        None => {
            let (b, pk) = core.equi(&mut cb)?;
            (b, pk, false)
        }
    };
    let [left, _right] = sides;
    Ok(ExistsCircuit { cb, branches, out_pk_cols, shard, left })
}

/// The resolved inputs an EXISTS/IN circuit core emits from: both sides (with their
/// local WHEREs already fused into `a_local` / `b_local`), the classified
/// correlation, and the decorrelation `kind` that selects the branch composition.
/// Bundled because the equi and range cores take the same six values and differ
/// only in which half of `class` they read.
struct ExistsCore<'a> {
    sides: &'a [JoinSide; 2],
    a_local: NodeId,
    b_local: NodeId,
    class: &'a JoinClass,
    kind: JoinType,
}

impl ExistsCore<'_> {
    /// Equi correlation: the symmetric 2-term join over
    /// the NULL-gated sides, `π_A(inner)`, and `a_all` (the full outer re-keyed,
    /// reusing `reindex_a` when the key is NOT NULL). Output keyed by `_join_pk`, no
    /// output exchange. Returns `(branches, _join_pk cols)`.
    fn equi(&self, cb: &mut CircuitBuilder) -> Result<(ExistsBranches, Vec<ColumnDef>), GnitzSqlError> {
        let ExistsCore { sides, a_local, b_local, class, kind } = *self;
        let (a_n, b_n) = (sides[0].n(), sides[1].n());
        let terms = equi_prologue(
            cb,
            &class.eq,
            EquiInput { side: &sides[0], node: a_local },
            EquiInput { side: &sides[1], node: b_local },
        )?;
        let k = terms.k();

        // π_A(inner): project each term straight to [_join_pk × k, A].
        let pa_ab = cb.map(terms.join_ab, &(k..k + a_n).collect::<Vec<_>>());
        let pa_ba = cb.map(terms.join_ba, &(k + b_n..k + b_n + a_n).collect::<Vec<_>>());
        let pi_a = cb.union(pa_ab, pa_ba);

        // A_all: the full (locally filtered, NULL keys included) outer input,
        // re-keyed — reusing `reindex_a` on a NOT NULL key, where the gate was a
        // no-op.
        let a_all = terms.a_all(cb);
        Ok((exists_branches(cb, a_all, pi_a, kind), terms.out_pk_coldefs()))
    }

    /// Range correlation — band (`n_eq ≥ 1`) or pure range (`n_eq == 0`). Both re-key
    /// onto the outer source PK and ride the range output exchange. Returns
    /// `(branches, _src_pk cols)`.
    fn range(
        &self,
        cb: &mut CircuitBuilder,
        range: &HirRange,
    ) -> Result<(ExistsBranches, Vec<ColumnDef>), GnitzSqlError> {
        let ExistsCore { sides, a_local, b_local, class, kind } = *self;
        let left = &sides[0];
        let a_n = left.n();

        let pro = range_prologue(cb, sides, a_local, b_local, &class.eq, range)?;

        let branches = if pro.n_eq() == 0 {
            // Pure range: the one-row threshold m = MAX/MIN(b.range) decides existence.
            let int_a = cb.worker_filter(pro.reindex_a);
            let trace_a = cb.integrate_trace(int_a);
            let matched = build_pure_range_threshold(cb, left, range, pro.reindex_b, int_a, trace_a);
            // `A − matched` (∪ NULL-range-key rows) — the anti / mark unmatched
            // branch; only a branch that subtracts from A emits the passthrough.
            let mut unmatched = || {
                let a_pass = rekey_pure_range_a(cb, int_a, left.pa(), a_n);
                pure_range_unmatched(
                    cb,
                    matched,
                    a_pass,
                    pro.left_key_nullable,
                    a_local,
                    &pro.left_reindex_cols,
                    left,
                )
            };
            // The threshold decides existence directly, so these are the three
            // compositions `exists_branches` states over ν — spelled here against
            // `matched` / `A − matched` because the threshold has no `π_A(inner)`.
            match kind {
                JoinType::Anti => ExistsBranches::SemiAnti(unmatched()?),
                JoinType::Mark(_) => ExistsBranches::Mark { matched, unmatched: unmatched()? },
                _ => ExistsBranches::SemiAnti(matched),
            }
        } else {
            // Band: the eq-prefix scatter co-locates both sides, so the inner join, its
            // π_A re-key, a_all, and the clamp are all partition-local.
            let trace_a = cb.integrate_trace(pro.reindex_a);
            let trace_b = cb.integrate_trace(pro.reindex_b);
            let merged = pro.range_merged(cb, trace_a, trace_b, a_n, sides[1].n());

            // π_A(inner) keyed by the outer source PK.
            let proj_a = band_pi_preserved(cb, merged, pro.k(), 0, left);
            let a_all = rekey_aux_on_source_pk(cb, a_local, &left.seg.schema, &left.keep);
            exists_branches(cb, a_all, proj_a, kind)
        };

        // View PK = the outer source PK, hidden (it also rides the payload verbatim).
        Ok((branches, src_pk_coldefs(&left.seg.schema)))
    }
}

/// Lower a decorrelated `Project(Filter?(Join{Semi|Anti}))` to circuit pieces.
pub(crate) fn lower_semi_anti_view(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    fpreds: &[HirExpr],
    source: &Rc<RelExpr>,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let ExistsCircuit {
        mut cb,
        branches,
        out_pk_cols,
        shard,
        left,
    } = emit_exists_circuit(chain, memo, source, fpreds, items)?;
    let ExistsBranches::SemiAnti(primary) = branches else {
        unreachable!("a Semi/Anti kind composes one branch");
    };

    let npk = out_pk_cols.len();
    let frame = outer_frame(out_pk_cols, &left);
    let (mut sink_input, final_cols) = project_tail(&mut cb, primary, items, &frame)?;
    if shard {
        sink_input = cb.shard(sink_input, &(0..npk).collect::<Vec<_>>());
    }
    cb.sink(sink_input);
    let circuit = cb.build();
    Ok((
        (circuit, final_cols, npk),
        key_region_layout(npk, items.iter().map(|i| i.out.id)),
    ))
}

/// Lower a decorrelated `Project(Filter?(Join{Mark}))` to circuit pieces: the
/// matched / unmatched branches each bind the WHERE + projection with the mark
/// column substituted by its `0/1` constant, then union.
pub(crate) fn lower_mark_view(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    fpreds: &[HirExpr],
    source: &Rc<RelExpr>,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let RelExpr::Join { kind: JoinType::Mark(mark_id), .. } = source.as_ref() else {
        unreachable!("lower_mark_view receives a Mark Join");
    };
    let ExistsCircuit {
        mut cb,
        branches,
        out_pk_cols,
        shard,
        left,
    } = emit_exists_circuit(chain, memo, source, fpreds, items)?;
    let ExistsBranches::Mark { matched, unmatched } = branches else {
        unreachable!("a Mark kind composes two branches");
    };

    let npk = out_pk_cols.len();
    let branch = MarkBranch {
        mark_id: *mark_id,
        where_preds: fpreds,
        items,
        // The same `[out PK][kept outer]` frame the semi/anti tail projects
        // against; both branches carry it, only the mark constant differing.
        frame: outer_frame(out_pk_cols, &left),
    };
    let (m_out, out_cols) = branch.emit(&mut cb, matched, 1)?;
    let (u_out, _) = branch.emit(&mut cb, unmatched, 0)?;
    let mut out = cb.union(m_out, u_out);
    if shard {
        out = cb.shard(out, &(0..npk).collect::<Vec<_>>());
    }
    cb.sink(out);
    let circuit = cb.build();
    Ok((
        (circuit, out_cols, npk),
        key_region_layout(npk, items.iter().map(|i| i.out.id)),
    ))
}

/// The frame every EXISTS/IN tail projects against: the view's output PK region,
/// then the outer side's kept payload.
fn outer_frame(out_pk_cols: Vec<ColumnDef>, left: &JoinSide) -> Frame {
    Frame::keyed(out_pk_cols, left.ids().zip(left.coldefs.iter().cloned()))
}

/// Everything a mark branch emits from, shared verbatim by the matched and
/// unmatched branch — only the `0/1` mark constant differs between them.
struct MarkBranch<'a> {
    mark_id: ColId,
    where_preds: &'a [HirExpr],
    items: &'a [ProjEntry],
    frame: Frame,
}

impl MarkBranch<'_> {
    /// Emit one branch: substitute the mark column by `mark_val`, apply the WHERE
    /// (folds to a true constant → no filter), then the shared projection tail.
    /// The output columns are branch-invariant — the constant appears in no def —
    /// so the caller may take them from either branch.
    fn emit(
        &self,
        cb: &mut CircuitBuilder,
        node: NodeId,
        mark_val: i64,
    ) -> Result<(NodeId, Vec<ColumnDef>), GnitzSqlError> {
        let subst = |e: &HirExpr| subst_mark_lit(e, self.mark_id, mark_val);
        let preds: Vec<HirExpr> = self.where_preds.iter().map(subst).collect();
        let items: Vec<ProjEntry> = self
            .items
            .iter()
            .map(|it| ProjEntry {
                expr: subst(&it.expr),
                out: it.out.clone(),
            })
            .collect();
        let filtered = emit_filter(cb, node, preds.iter(), &self.frame)?;
        project_tail(cb, filtered, &items, &self.frame)
    }
}

/// Substitute `ColRef(Col(mark_id))` with `LitInt(val)` throughout an expression —
/// the mark column's per-branch constant; every other leaf passes through.
fn subst_mark_lit(e: &HirExpr, mark_id: ColId, val: i64) -> HirExpr {
    match e.try_rebuild::<HirRef, std::convert::Infallible>(&|r| {
        Ok(match r {
            HirRef::Col(id) if *id == mark_id => BExpr::LitInt(val),
            _ => BExpr::ColRef(r.clone()),
        })
    }) {
        Ok(out) => out,
        Err(never) => match never {},
    }
}
