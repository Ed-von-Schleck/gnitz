//! The one join shell: every `Join` step, decorrelated kinds included, over the
//! circuit primitives in [`super::joincore`], its output key read off [`OutKey`].

use super::super::guards::reject_pair_pk_overflow;
use super::super::{ColId, HirExpr, JoinClass, JoinShape, JoinType, OutKey, ProjEntry, RelExpr};
use super::exists;
use super::joincore::{
    ba_to_ab_cols, emit_range_null_fill_tail, equi_prologue, join_pk_coldefs, pair_pk_coldefs, pair_pk_slots,
    range_prologue, src_pk_coldefs, EquiTerms,
};
use super::prims::{rekey_on_source_pk, self_derived_key};
use super::{emit_filter, emit_join_inputs, project_tail, CutMemo, Demand, JoinSide};
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::hir::physical::Frame;
use crate::ir::BExpr;

use gnitz_core::{Circuit, ColumnDef, NodeId, ReindexRole};
use gnitz_wire::JoinKind;
use std::borrow::Cow;

/// One branch an emitter hands the shell: its node over the join frame, and a mark
/// branch's `0/1` constant.
pub(super) type Branch = (NodeId, Option<i64>);

/// Lower a `Project(Filter?(Join))` tree's join to circuit pieces, whose output
/// leads with the join step's [`OutKey`] slots, then the projected payload in item
/// order.
pub(super) fn lower_join_view(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    where_preds: &[HirExpr],
    join: &RelExpr,
) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::Join { left, right, kind, on: class } = join else {
        unreachable!("lower_join_view receives a Join");
    };
    let kind = *kind;
    let down = Demand { items, where_preds };

    let mut cb = Circuit::default();
    let (inputs, sides) = emit_join_inputs(chain, memo, &mut cb, down, [left, right], kind, class)?;

    let out_key = class.out_key(kind);
    let out_pk = match out_key {
        OutKey::JoinKey => join_pk_coldefs(&class.eq.iter().map(|p| p.tc).collect::<Vec<_>>()),
        OutKey::PairPk => {
            let surface = match class.shape() {
                JoinShape::Cross => "CROSS JOIN",
                _ => "range JOIN",
            };
            reject_pair_pk_overflow(surface, sides[0].pa(), sides[1].pa())?;
            pair_pk_coldefs(&sides[0].frame.schema, &sides[1].frame.schema)
        }
        OutKey::OuterPk { .. } => src_pk_coldefs(&sides[0].frame.schema),
    };

    let unique = [class.side_unique(left, true), class.side_unique(right, false)];
    let branches = match (class.shape(), kind.is_decorrelated()) {
        (JoinShape::Equi, false) => emit_equi(&mut cb, inputs, class, kind, &sides, unique)?,
        (JoinShape::Band | JoinShape::PureRange, false) => emit_range(&mut cb, inputs, class, kind, &sides, unique)?,
        (JoinShape::Cross, false) => emit_cross(&mut cb, inputs, &sides),
        (JoinShape::Equi, true) => exists::equi(&mut cb, inputs, class, kind, &sides, unique[1])?,
        (JoinShape::Band, true) => exists::band(&mut cb, inputs, class, kind, &sides, unique[1])?,
        (JoinShape::PureRange, true) => exists::pure_range(&mut cb, inputs, class, kind, &sides)?,
        (JoinShape::Cross, true) => unreachable!("reject_keyless_non_inner refuses a keyless decorrelated join"),
    };

    // The frame every branch filters and projects against.
    let frame = join_frame(out_pk, &sides, kind);
    let mut acc: Option<(NodeId, Frame)> = None;
    for (node, mark) in branches {
        let (preds, items): (Cow<'_, [HirExpr]>, Cow<'_, [ProjEntry]>) = match (kind, mark) {
            (JoinType::Mark(id), Some(val)) => (
                where_preds.iter().map(|e| subst_mark_lit(e, id, val)).collect(),
                items
                    .iter()
                    .map(|it| ProjEntry {
                        expr: subst_mark_lit(&it.expr, id, val),
                        out: it.out.clone(),
                    })
                    .collect(),
            ),
            _ => (Cow::Borrowed(where_preds), Cow::Borrowed(items)),
        };
        let filtered = emit_filter(&mut cb, node, preds.iter(), &frame)?;
        let (node, out) = project_tail(&mut cb, filtered, &items, &frame)?;
        acc = Some(match acc {
            None => (node, out),
            Some((prior, out)) => (cb.union(node, prior), out),
        });
    }
    let (node, out) = acc.expect("a join step emits at least one branch");
    let node = match out_key.exchanged() {
        true => cb.shard(node, &(0..out.npk() as u32).collect::<Vec<_>>()),
        false => node,
    };
    cb.sink(node);
    Ok(EmitPieces { circuit: cb, out })
}

/// Substitute `ColRef(mark_id)` with `LitInt(val)` throughout an expression —
/// the mark column's per-branch constant; every other leaf passes through.
fn subst_mark_lit(e: &HirExpr, mark_id: ColId, val: i64) -> HirExpr {
    let Ok(out) = e.try_rebuild::<ColId, std::convert::Infallible>(&mut |id| {
        Ok(match *id == mark_id {
            true => BExpr::LitInt(val),
            false => BExpr::ColRef(*id),
        })
    });
    out
}

// ── Equi emission ───────────────────────────────────────────────────────────────

fn emit_equi(
    cb: &mut Circuit,
    inputs: [NodeId; 2],
    class: &JoinClass,
    kind: JoinType,
    sides: &[JoinSide; 2],
    unique: [bool; 2],
) -> Result<Vec<Branch>, GnitzSqlError> {
    let terms = equi_prologue(cb, class, sides, inputs, kind, unique[1])?;
    let inner_merged = terms.merged(cb);
    Ok(vec![(
        emit_equi_null_fill(cb, inner_merged, kind, &terms, unique),
        None,
    )])
}

/// `inner ∪ ν_A ∪ ν_B` — the equi outer join's null-fill, unioned onto the inner
/// output for each preserved side; `inner_merged` unchanged for INNER.
///
/// `inner_merged` feeds both `π_P` and these unions, so it rides the
/// non-destructive second union operand throughout (`op_union` empties the first).
fn emit_equi_null_fill(
    cb: &mut Circuit,
    inner_merged: NodeId,
    kind: JoinType,
    terms: &EquiTerms<'_>,
    unique: [bool; 2],
) -> NodeId {
    if kind == JoinType::Inner {
        return inner_merged;
    }
    let k = terms.k();
    let (pl, pr) = (terms.kept_n(true), terms.kept_n(false));
    // Each preserved side P: the other side supplies the NULL region. Every branch
    // is built before the first union, so each `π_P` reads `inner_merged` ahead of
    // the union that could move it.
    let branches: Vec<NodeId> = [true, false]
        .into_iter()
        .filter(|&preserved_is_left| kind.preserves(preserved_is_left))
        .map(|preserved_is_left| {
            let other_unique = unique[usize::from(preserved_is_left)];
            let nu_p = terms.nu(cb, inner_merged, preserved_is_left, other_unique);
            // A side that keeps nothing contributes no NULL region at all.
            let o_tcs = terms.kept_type_codes(!preserved_is_left);
            let ext = if o_tcs.is_empty() {
                nu_p
            } else {
                cb.null_extend(nu_p, &o_tcs)
            };
            if preserved_is_left {
                ext // [_join_pk, A, NULL-B] — canonical
            } else {
                cb.map(ext, &ba_to_ab_cols(k, pl, pr).collect::<Vec<_>>())
            }
        })
        .collect();
    branches
        .into_iter()
        .fold(inner_merged, |merged, branch| cb.union(branch, merged))
}

// ── Range / band emission ───────────────────────────────────────────────────────

fn emit_range(
    cb: &mut Circuit,
    inputs: [NodeId; 2],
    class: &JoinClass,
    kind: JoinType,
    sides: &[JoinSide; 2],
    unique: [bool; 2],
) -> Result<Vec<Branch>, GnitzSqlError> {
    let pro = range_prologue(cb, class, sides, inputs, kind)?;
    let merged = pro.merged(cb);
    // Every branch below lands on `[pair-PK, kept-A, kept-B]`.
    let rekey = pro.pair_keyed(cb, merged);
    let node = match (kind, class.shape()) {
        (JoinType::Inner, _) => rekey,
        (_, JoinShape::PureRange) => {
            // Pure-range threshold subtraction: `A − matched` against `m = MAX/MIN(b.range)`.
            let (owned, matched) = pro.threshold(cb);
            let nu_a = cb.difference(owned, matched);
            let branch = emit_range_null_fill_tail(cb, sides, nu_a, true);
            cb.union(branch, rekey)
        }
        _ => {
            // Band ν_A (preserves_left) and/or ν_B (preserves_right).
            let mut acc = rekey;
            for preserved_is_left in [true, false] {
                if !kind.preserves(preserved_is_left) {
                    continue;
                }
                let (_, nu) = pro.nu(cb, merged, preserved_is_left, unique[usize::from(preserved_is_left)]);
                let branch = emit_range_null_fill_tail(cb, sides, nu, preserved_is_left);
                acc = cb.union(branch, acc);
            }
            acc
        }
    };
    Ok(vec![(node, None)])
}

// ── Cross emission ──────────────────────────────────────────────────────────────

/// The keyless join `A × B`, INNER only. Each side keys its trace on its own
/// source PK, which takes no part in the match — it only partitions the trace the
/// broadcast delta is paired against.
fn emit_cross(cb: &mut Circuit, [input_a, input_b]: [NodeId; 2], sides: &[JoinSide; 2]) -> Vec<Branch> {
    let (left, right) = (&sides[0], &sides[1]);
    let (pl, pr) = (left.n(), right.n());
    let (pa, pb) = (left.pa(), right.pa());

    let reindex_a = rekey_on_source_pk(cb, input_a, left, ReindexRole::ScatterKey);
    let reindex_b = rekey_on_source_pk(cb, input_b, right, ReindexRole::ScatterKey);
    let int_a = cb.worker_filter(reindex_a);
    let int_b = cb.worker_filter(reindex_b);
    let trace_a = cb.integrate_trace(int_a);
    let trace_b = cb.integrate_trace(int_b);
    let join_ab = cb.join(reindex_a, trace_b, JoinKind::Cross); // [a.pk × pa, A, B]
    let join_ba = cb.join(reindex_b, trace_a, JoinKind::Cross); // [b.pk × pb, B, A]

    // Re-key both terms onto the pair-PK: their key regions are two different
    // source keys, and a union needs one schema. `keep` is the reindex's OUTPUT
    // payload order, so BA's `[B, A]` → `[A, B]` rides the same node.
    let ab = cb.map_reindex(
        join_ab,
        &self_derived_key(&pair_pk_slots(sides, pa, pa + pl)),
        &(pa as u32..(pa + pl + pr) as u32).collect::<Vec<_>>(),
        ReindexRole::Auxiliary,
    );
    let ba_keep: Vec<u32> = ba_to_ab_cols(pb, pl, pr).collect();
    let ba = cb.map_reindex(
        join_ba,
        &self_derived_key(&pair_pk_slots(sides, pb + pr, pb)),
        &ba_keep,
        ReindexRole::Auxiliary,
    );
    vec![(cb.union(ab, ba), None)] // [pair-PK, A, B]
}

// ── shared helpers ──────────────────────────────────────────────────────────────

/// The frame a join step's filters and projection read: `pk_cols`, then both
/// sides' kept payload with the null-providing side widened per `kind`, through
/// the same [`JoinType::widen_sides`] the logical join output uses.
fn join_frame(pk_cols: Vec<ColumnDef>, sides: &[JoinSide; 2], kind: JoinType) -> Frame {
    let (mut l, mut r) = (sides[0].coldefs.clone(), sides[1].coldefs.clone());
    kind.widen_sides(l.iter_mut(), r.iter_mut());
    Frame::keyed(
        pk_cols,
        sides[0].ids().chain(sides[1].ids()).zip(l.into_iter().chain(r)),
    )
}
