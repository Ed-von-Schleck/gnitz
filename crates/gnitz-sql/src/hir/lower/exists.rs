//! The EXISTS/IN shell: a decorrelated `Join{Semi|Anti|Mark}` over the
//! [`super::joincore`] primitives.

use super::super::split_filter;
use super::super::{ColId, HirExpr, HirRange, HirRef, JoinClass, ProjEntry, RelExpr};
use super::joincore::{band_nu_operands, equi_prologue, range_prologue, src_pk_coldefs};
use super::{
    emit_filter, join_sides, materialize, project_tail, resolve_in_place, resolve_input, CutMemo, Demand, JoinSide,
};
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::hir::guards::reject_pure_range_threshold_tc;
use crate::hir::physical::Frame;
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
    Mark {
        mark_id: ColId,
        matched: NodeId,
        unmatched: NodeId,
    },
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
        JoinType::Mark(mark_id) => ExistsBranches::Mark { mark_id, matched: semi, unmatched: nu },
        _ => ExistsBranches::SemiAnti(semi),
    }
}

/// Lower a decorrelated `Project(Filter?(Join{Semi|Anti|Mark}))` to circuit pieces.
pub(super) fn lower_exists_view(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    where_preds: &[HirExpr],
    source: &Rc<RelExpr>,
) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::Join { left, right, kind, on, .. } = source.as_ref() else {
        unreachable!("lower_exists_view receives a Join");
    };
    let class = on.class()?;
    // A semi/anti view fuses its WHERE into the outer delta before the join terms;
    // a mark view applies it per branch after the mark. Derived from `kind` here,
    // so the two stages cannot be handed the wrong one.
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
    let mut right_in = resolve_in_place(chain, memo, inner_src)?
        .ok_or_else(|| GnitzSqlError::Internal("EXISTS/IN inner relation is not read in place".into()))?;
    // Self-collision: EXISTS/IN over the outer's own relation must read the inner
    // as a distinct source (single-source-per-epoch). The materialized inner
    // applies its own WHERE, so it carries the correlation keys alone.
    let inner_preds = if left_in.tid == right_in.tid {
        let keys: HashSet<ColId> = class.key_cols(false).collect();
        right_in = materialize(chain, memo, right, &keys)?;
        &[][..]
    } else {
        inner_preds
    };

    // A prefilter is consumed before the reindex, so only a post-mark WHERE joins
    // the projection in the keep demand.
    let down = Demand { items, where_preds: post_mark };
    let sides = join_sides(down, class, *kind, [left_in, right_in]);

    // A pure-range correlation (no equality prefix) decides existence from a
    // MIN/MAX threshold row, which is carried only at a ≤8-byte width.
    if let Some(range) = class.range.filter(|_| class.eq.is_empty()) {
        reject_pure_range_threshold_tc(
            range.tc,
            "EXISTS/IN pure-range correlation",
            "use a narrower range column or add an equality conjunct",
        )?;
    }

    // Delta per side with its local / inner-local WHERE fused as a prefilter
    // (before the join terms).
    let mut cb = CircuitBuilder::new();
    let a_delta = cb.input_delta(sides[0].seg.tid, None);
    let a_local = emit_filter(&mut cb, a_delta, left_prefilter, &sides[0].seg.frame)?;
    let b_delta = cb.input_delta(sides[1].seg.tid, None);
    let b_local = emit_filter(&mut cb, b_delta, inner_preds, &sides[1].seg.frame)?;

    let core = ExistsCore {
        sides: &sides,
        a_local,
        b_local,
        class,
        kind: *kind,
    };
    let (branches, out_pk_cols) = match &class.range {
        Some(range) => core.range(&mut cb, range)?,
        None => core.equi(&mut cb)?,
    };

    // The `[out PK][kept outer]` frame every branch projects against.
    let frame = Frame::keyed(out_pk_cols, sides[0].ids().zip(sides[0].coldefs.iter().cloned()));
    let (node, out) = match branches {
        ExistsBranches::SemiAnti(node) => project_tail(&mut cb, node, items, &frame)?,
        ExistsBranches::Mark { mark_id, matched, unmatched } => {
            let branch = MarkBranch { mark_id, where_preds, items, frame };
            let (m, out) = branch.emit(&mut cb, matched, 1)?;
            let (u, _) = branch.emit(&mut cb, unmatched, 0)?;
            (cb.union(m, u), out)
        }
    };
    // A range correlation re-keys onto the outer source PK and rides the range
    // output exchange; an equi one is keyed by `_join_pk` and needs none.
    let node = match class.range {
        Some(_) => cb.shard(node, &(0..out.npk() as u32).collect::<Vec<_>>()),
        None => node,
    };
    cb.sink(node);
    Ok(EmitPieces { circuit: cb.build(), out })
}

/// What the equi and range EXISTS/IN cores both read: the sides, their prefiltered
/// deltas, the correlation and the decorrelation kind.
struct ExistsCore<'a> {
    sides: &'a [JoinSide; 2],
    a_local: NodeId,
    b_local: NodeId,
    class: &'a JoinClass,
    kind: JoinType,
}

impl ExistsCore<'_> {
    /// Equi correlation, keyed by `_join_pk`: `(branches, _join_pk cols)`.
    fn equi(&self, cb: &mut CircuitBuilder) -> Result<(ExistsBranches, Vec<ColumnDef>), GnitzSqlError> {
        let ExistsCore { sides, a_local, b_local, class, kind } = *self;
        let (a_n, b_n) = (sides[0].n(), sides[1].n());
        let terms = equi_prologue(cb, &class.eq, sides, [a_local, b_local])?;
        let k = terms.k();

        // π_A(inner): project each term straight to [_join_pk × k, A].
        let pa_ab = cb.map(terms.join_ab, &(k..k + a_n).map(|c| c as u32).collect::<Vec<_>>());
        let pa_ba = cb.map(
            terms.join_ba,
            &(k + b_n..k + b_n + a_n).map(|c| c as u32).collect::<Vec<_>>(),
        );
        let pi_a = cb.union(pa_ab, pa_ba);

        // A_all: the full (locally filtered, NULL keys included) outer input,
        // re-keyed.
        let a_all = terms.p_all(cb, true);
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

        let pro = range_prologue(cb, sides, [a_local, b_local], &class.eq, range)?;

        let branches = if pro.n_eq() == 0 {
            // Pure range: the one-row threshold m = MAX/MIN(b.range) decides existence.
            let int_a = cb.worker_filter(pro.sides[0].reindex);
            let trace_a = cb.integrate_trace(int_a);
            let matched = pro.pure_range_matched(cb, int_a, trace_a);
            // `A − matched` (∪ NULL-range-key rows) — the anti / mark unmatched
            // branch; only a branch that subtracts from A emits the passthrough.
            let mut unmatched = || pro.pure_range_unmatched(cb, matched, int_a, a_local);
            // The threshold decides existence directly, so these are the three
            // compositions `exists_branches` states over ν — spelled here against
            // `matched` / `A − matched` because the threshold has no `π_A(inner)`.
            match kind {
                JoinType::Anti => ExistsBranches::SemiAnti(unmatched()?),
                JoinType::Mark(mark_id) => ExistsBranches::Mark {
                    mark_id,
                    matched,
                    unmatched: unmatched()?,
                },
                _ => ExistsBranches::SemiAnti(matched),
            }
        } else {
            // Band: the eq-prefix scatter co-locates both sides, so the inner join, its
            // π_A re-key, a_all, and the clamp are all partition-local.
            let trace_a = cb.integrate_trace(pro.sides[0].reindex);
            let trace_b = cb.integrate_trace(pro.sides[1].reindex);
            let merged = pro.range_merged(cb, trace_a, trace_b, a_n, sides[1].n());
            let (a_all, pi_a) = band_nu_operands(cb, merged, pro.k(), 0, left, a_local);
            exists_branches(cb, a_all, pi_a, kind)
        };

        // View PK = the outer source PK, hidden (it also rides the payload verbatim).
        Ok((branches, src_pk_coldefs(&left.seg.frame.schema)))
    }
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
    /// One branch with the mark column fixed to `mark_val`. The output frame is the
    /// same for both branches.
    fn emit(&self, cb: &mut CircuitBuilder, node: NodeId, mark_val: i64) -> Result<(NodeId, Frame), GnitzSqlError> {
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
    let Ok(out) = e.try_rebuild::<HirRef, std::convert::Infallible>(&mut |r| {
        Ok(match r {
            HirRef::Col(id) if *id == mark_id => BExpr::LitInt(val),
            _ => BExpr::ColRef(r.clone()),
        })
    });
    out
}
