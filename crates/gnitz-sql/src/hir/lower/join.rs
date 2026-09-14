//! The FROM-clause join emission shell — equi, range/band and cross. It reads the
//! demand, residual and output projection off the HIR and drives the AST-free
//! circuit primitives in [`super::joincore`]; the decorrelated semi/anti/mark
//! shell is `exists.rs`, over the same primitives.

use super::super::guards::{reject_pair_pk_overflow, reject_pure_range_outer};
use super::super::{ColId, EqPair, HirExpr, HirRange, JoinClass, JoinShape, JoinType, ProjEntry, RelExpr};
use super::joincore::{
    ba_to_ab_cols, band_nu_operands, emit_equi_null_fill, emit_range_null_fill_tail, equi_prologue, pair_pk_coldefs,
    pair_pk_slots, range_prologue,
};
use super::prims::{rekey_on_source_pk, self_derived_key};
use super::{
    collect_live_cols, emit_filter, join_sides, materialize, project_tail, resolve_input, CutMemo, Demand, JoinSide,
};
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::hir::physical::Frame;

use gnitz_core::{CircuitBuilder, ColumnDef, NodeId, ReindexRole};
use std::collections::HashSet;

/// Lower a `Project(Filter?(Join))` tree's join to circuit pieces, whose output
/// leads with the `k` hidden `_join_pk` / `_pair_pk` slots, then the projected
/// payload in item order.
pub(super) fn lower_join_view(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    where_preds: &[HirExpr],
    join: &RelExpr,
) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::Join { left, right, kind, on, .. } = join else {
        unreachable!("lower_join_view receives a Join");
    };
    let class = on.class()?;
    let down = Demand { items, where_preds };

    // The live-column set flowing into this join's inputs: everything consumed
    // downstream (this join's output projection + WHERE) plus this join's own ON.
    // A cut input is pruned to the cols in this set (the chain-liveness rule — the
    // registered segment schema is `join_pk + pruned projection`). For a cut
    // sub-join, `down.items` is already its pruned identity, so this closes over
    // the whole suffix transitively.
    let mut live: HashSet<ColId> = HashSet::new();
    down.refs(&mut live);
    class_referenced(class, &mut live);

    // Segment-cut then source-collision: a repeated tid (self-join) cuts the right
    // side to a second relation, so the two delta inputs are distinct sources.
    let mut inputs = [
        resolve_input(chain, memo, left, &live)?,
        resolve_input(chain, memo, right, &live)?,
    ];
    if inputs[0].tid == inputs[1].tid {
        inputs[1] = materialize(chain, memo, right, &live)?;
    }

    // Both sides' facts once, through the shared keep rule: what survives into
    // each trace, its defs, and where its pinned PK sits inside the kept payload.
    let sides = join_sides(down, class, *kind, inputs);

    let mut cb = CircuitBuilder::new();
    let a = cb.input_delta(sides[0].seg.tid, None);
    let b = cb.input_delta(sides[1].seg.tid, None);
    let (node, frame) = match class.shape() {
        JoinShape::Equi => emit_equi(&mut cb, [a, b], down, class, *kind, &sides)?,
        JoinShape::Range => emit_range(&mut cb, [a, b], down, class, *kind, &sides)?,
        JoinShape::Cross => emit_cross(&mut cb, [a, b], down, class, &sides)?,
    };

    let (node, out) = project_tail(&mut cb, node, down.items, &frame)?;
    // A range or keyless step re-keys onto the source-PK pair, which the output
    // exchange routes.
    let node = match class.shape() {
        JoinShape::Equi => node,
        _ => cb.shard(node, &(0..out.npk() as u32).collect::<Vec<_>>()),
    };
    cb.sink(node);
    Ok(EmitPieces { circuit: cb.build(), out })
}

// ── Equi emission ───────────────────────────────────────────────────────────────

fn emit_equi(
    cb: &mut CircuitBuilder,
    inputs: [NodeId; 2],
    down: Demand<'_>,
    class: &JoinClass,
    kind: JoinType,
    sides: &[JoinSide; 2],
) -> Result<(NodeId, Frame), GnitzSqlError> {
    let terms = equi_prologue(cb, &class.eq, sides, inputs)?;
    let inner_merged = terms.merged(cb);
    let merged = emit_equi_null_fill(cb, inner_merged, kind, &terms);

    // The merged frame over the KEPT columns: `k` `_join_pk` slots, then the
    // pruned payload (outer nullability applied), so a reference resolves to
    // `k + kept_index`.
    let frame = join_frame(terms.out_pk_coldefs(), sides, kind);

    // One residual/WHERE filter over the normalized output (at most one source is
    // non-empty — the rewrite folds the WHERE into the INNER residual and leaves an
    // OUTER WHERE as the post-null-fill filter). const-elision is harmless (INNER 3VL).
    let merged = emit_filter(cb, merged, class.residual.iter().chain(down.where_preds), &frame)?;
    Ok((merged, frame))
}

// ── Range / band emission ───────────────────────────────────────────────────────

fn emit_range(
    cb: &mut CircuitBuilder,
    [input_a_raw, input_b_raw]: [NodeId; 2],
    down: Demand<'_>,
    class: &JoinClass,
    kind: JoinType,
    sides: &[JoinSide; 2],
) -> Result<(NodeId, Frame), GnitzSqlError> {
    let eq: &[EqPair] = &class.eq;
    let range: &HirRange = class.range.as_ref().expect("emit_range receives a range class");
    let (left, right) = (&sides[0], &sides[1]);
    let (pl, pr) = (left.n(), right.n());

    let n_eq = eq.len();
    let k = n_eq + 1;

    reject_pair_pk_overflow("range JOIN", left.pa(), right.pa())?;

    if n_eq == 0 {
        reject_pure_range_outer(kind, range.tc)?;
    }

    let pro = range_prologue(cb, sides, [input_a_raw, input_b_raw], eq, range)?;
    let (reindex_a, reindex_b) = (pro.sides[0].reindex, pro.sides[1].reindex);
    let (int_a, int_b) = if n_eq == 0 {
        (cb.worker_filter(reindex_a), cb.worker_filter(reindex_b))
    } else {
        (reindex_a, reindex_b)
    };
    let trace_a = cb.integrate_trace(int_a);
    let trace_b = cb.integrate_trace(int_b);
    let merged = pro.range_merged(cb, trace_a, trace_b, pl, pr);

    // The `[key region, kept-A, kept-B]` payload behind either key region: the
    // per-term `_join_pk × k` the residual reads, and the `_pair_pk` the outer
    // WHERE and the output projection read after the re-key. The payload defs
    // carry the outer nullability in both — a residual is INNER-only
    // (`reject_outer_with_residual`), where widening is a no-op.
    let join_pk_frame = join_frame(pro.out_pk_coldefs(), sides, kind);
    let pair_pk_frame = join_frame(
        pair_pk_coldefs(&left.seg.frame.schema, &right.seg.frame.schema),
        sides,
        kind,
    );

    // Residual filter over the pre-rekey `[_join_pk × k, kept-A, kept-B]`.
    let merged = emit_filter(cb, merged, &class.residual, &join_pk_frame)?;

    // Re-key onto the source-PK pair `_pair_pk`, dropping the per-term `_join_pk`
    // slots: both consumers read the payload alone, and each PK column also rides
    // at the front of its side's kept payload.
    let pair_pk_cols = pair_pk_slots(sides, k, k + pl);
    let rekey = cb.map_reindex(
        merged,
        &self_derived_key(&pair_pk_cols),
        &(k as u32..(k + pl + pr) as u32).collect::<Vec<_>>(),
        ReindexRole::Auxiliary,
    );

    // Both branches below land on `[pair-PK, kept-A, kept-B]`, the frame the
    // output projection reads.
    if kind == JoinType::Inner {
        return Ok((rekey, pair_pk_frame));
    }
    let unioned = if n_eq == 0 {
        // Pure-range threshold subtraction: `A − matched` against `m = MAX/MIN(b.range)`.
        let matched = pro.pure_range_matched(cb, int_a, trace_a);
        let nu_a = pro.pure_range_unmatched(cb, matched, int_a, input_a_raw)?;
        let branch = emit_range_null_fill_tail(cb, sides, nu_a, true);
        cb.union(branch, rekey)
    } else {
        // Band ν_A (preserves_left) and/or ν_B (preserves_right) — the two
        // mirror sides of `ν_P = positive_part(P_all − π_P(inner))`.
        let mut acc = rekey;
        for (preserved_is_left, payload_off, raw, side) in
            [(true, 0, input_a_raw, left), (false, pl, input_b_raw, right)]
        {
            if !kind.preserves(preserved_is_left) {
                continue;
            }
            let (all, pi) = band_nu_operands(cb, merged, k, payload_off, side, raw);
            let nu = cb.positive_diff(all, pi);
            let branch = emit_range_null_fill_tail(cb, sides, nu, preserved_is_left);
            acc = cb.union(branch, acc);
        }
        acc
    };

    // One linear 3VL WHERE over the full-width `[pair-PK, kept-A, kept-B]`.
    let filtered = emit_filter(cb, unioned, down.where_preds, &pair_pk_frame)?;
    Ok((filtered, pair_pk_frame))
}

// ── Cross emission ──────────────────────────────────────────────────────────────

/// The keyless join `A × B`, INNER only. Each side keys its trace on its own
/// source PK, which takes no part in the match — it only partitions the trace the
/// broadcast delta is paired against.
fn emit_cross(
    cb: &mut CircuitBuilder,
    [input_a_raw, input_b_raw]: [NodeId; 2],
    down: Demand<'_>,
    class: &JoinClass,
    sides: &[JoinSide; 2],
) -> Result<(NodeId, Frame), GnitzSqlError> {
    let (left, right) = (&sides[0], &sides[1]);
    let (pl, pr) = (left.n(), right.n());
    let (pa, pb) = (left.pa(), right.pa());

    reject_pair_pk_overflow("CROSS JOIN", pa, pb)?;

    let reindex_a = rekey_on_source_pk(cb, input_a_raw, left, ReindexRole::ScatterKey);
    let reindex_b = rekey_on_source_pk(cb, input_b_raw, right, ReindexRole::ScatterKey);
    let int_a = cb.worker_filter(reindex_a);
    let int_b = cb.worker_filter(reindex_b);
    let trace_a = cb.integrate_trace(int_a);
    let trace_b = cb.integrate_trace(int_b);
    let join_ab = cb.join_with_trace_cross_node(reindex_a, trace_b); // [a.pk × pa, A, B]
    let join_ba = cb.join_with_trace_cross_node(reindex_b, trace_a); // [b.pk × pb, B, A]

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
    let merged = cb.union(ab, ba); // [pair-PK, A, B]

    // `reject_keyless_non_inner` admits only an INNER keyless step, which widens
    // neither side.
    let frame = join_frame(
        pair_pk_coldefs(&left.seg.frame.schema, &right.seg.frame.schema),
        sides,
        JoinType::Inner,
    );
    let filtered = emit_filter(cb, merged, class.residual.iter().chain(down.where_preds), &frame)?;
    Ok((filtered, frame))
}

// ── keep-set + shared helpers ───────────────────────────────────────────────────

/// Collect every `ColId` a classified join's ON references (eq/range key pairs +
/// residual conjuncts) into the live set.
fn class_referenced(class: &JoinClass, live: &mut HashSet<ColId>) {
    live.extend(class.key_cols(true).chain(class.key_cols(false)));
    collect_live_cols(&class.residual, live);
}

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
