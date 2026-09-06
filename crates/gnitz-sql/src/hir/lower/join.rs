//! The FROM-clause join emission shell — equi, range/band and cross. It reads the
//! demand, residual and output projection off the HIR and drives the AST-free
//! circuit primitives in [`super::joincore`]; the decorrelated semi/anti/mark
//! shells are `exists.rs`, over the same primitives.

use super::super::guards::{reject_pair_pk_overflow, reject_pure_range_outer};
use super::super::{widen_if, ColId, EqPair, HirExpr, HirRange, JoinClass, JoinShape, JoinType, ProjEntry, RelExpr};
use super::joincore::{
    ba_to_ab_cols, band_pi_preserved, build_pure_range_threshold, emit_equi_null_fill, emit_range_null_fill_tail,
    equi_prologue, pair_pk_coldefs, pair_pk_slots, pure_range_unmatched, range_prologue, rekey_pure_range_a, EquiInput,
};
use super::prims::{rekey_aux_on_source_pk, rekey_scatter_on_source_pk, self_derived_key};
use super::{
    collect_live_cols, join_sides, key_region_layout, project_tail, resolve_collisions, resolve_input, CutMemo, Demand,
    Frame, JoinSide,
};
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};

use gnitz_core::{CircuitBuilder, ColumnDef, ReindexRole};
use std::collections::HashSet;

/// Lower a `Project(Filter?(Join))` tree's join to circuit pieces,
/// plus the output `ColId` layout: `[k hidden `_join_pk` / `_pair_pk` slots]`
/// followed by the projected payload in item order.
pub(crate) fn lower_join_view(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    project_items: &[ProjEntry],
    where_preds: &[HirExpr],
    join: &RelExpr,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let down = Demand { items: project_items, where_preds };
    let pieces = emit_step(chain, memo, down, join)?;
    // The emitted pk-list is the synthetic key region; its width is the number of
    // identity-free slots the layout leads with.
    let layout = key_region_layout(pieces.2, project_items.iter().map(|i| i.out.id));
    Ok((pieces, layout))
}

/// Emit one join step, resolving (and cutting/wrapping) its two inputs first.
fn emit_step(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    down: Demand<'_>,
    join: &RelExpr,
) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::Join { left, right, kind, on, .. } = join else {
        unreachable!("emit_step receives a Join");
    };
    let class = on.class()?;

    // The live-column set flowing into this join's inputs: everything consumed
    // downstream (this join's output projection + WHERE) plus this join's own ON.
    // A cut input is pruned to the cols in this set (the chain-liveness rule — the
    // registered segment schema is `join_pk + pruned projection`). For a cut
    // sub-join, `down.items` is already its pruned identity, so this closes over
    // the whole suffix transitively.
    let mut live: HashSet<ColId> = HashSet::new();
    down.refs(&mut live);
    class_referenced(class, &mut live);

    // Segment-cut then source-collision, both through the shared rules: a repeated
    // tid (self-join) wraps the later side in a pass-through segment so the two
    // delta inputs are distinct sources. The wrapped `Get` reuses its ColIds,
    // re-ordered to the wrapper's PK-front schema — `resolve_refs` absorbs it.
    let mut inputs = [
        resolve_input(chain, memo, left, &live)?,
        resolve_input(chain, memo, right, &live)?,
    ];
    resolve_collisions(chain, &mut inputs, &[live.clone(), live], false)?;

    // Both sides' facts once, through the shared keep rule: what survives into
    // each trace, its defs, and where its pinned PK sits inside the kept payload.
    let sides = join_sides(down, class, *kind, inputs);

    match class.shape() {
        JoinShape::Range => emit_range(down, class, *kind, &sides),
        JoinShape::Cross => emit_cross(down, class, &sides),
        JoinShape::Equi => emit_equi(down, class, *kind, &sides),
    }
}

// ── Equi emission ───────────────────────────────────────────────────────────────

fn emit_equi(
    down: Demand<'_>,
    class: &JoinClass,
    kind: JoinType,
    sides: &[JoinSide; 2],
) -> Result<EmitPieces, GnitzSqlError> {
    let (left, right) = (&sides[0], &sides[1]);

    let mut cb = CircuitBuilder::new(0);
    let input_a_raw = cb.input_delta_tagged(left.seg.tid);
    let input_b_raw = cb.input_delta_tagged(right.seg.tid);

    let terms = equi_prologue(
        &mut cb,
        &class.eq,
        EquiInput { side: left, node: input_a_raw },
        EquiInput { side: right, node: input_b_raw },
    )?;
    let k = terms.k();
    let inner_merged = terms.merged(&mut cb);
    let merged = emit_equi_null_fill(&mut cb, inner_merged, kind, &terms);

    // The merged frame over the KEPT columns: `k` `_join_pk` slots, then the
    // pruned payload's ids paired with their defs (outer nullability applied), so
    // a reference resolves to `k + kept_index`.
    let frame = Frame::keyed(
        terms.out_pk_coldefs(),
        left.ids()
            .chain(right.ids())
            .zip(combined_payload_coldefs(&left.coldefs, &right.coldefs, kind)),
    );

    // One residual/WHERE filter over the normalized output (at most one source is
    // non-empty — the rewrite folds the WHERE into the INNER residual and leaves an
    // OUTER WHERE as the post-null-fill filter). const-elision is harmless (INNER 3VL).
    let merged = super::emit_filter(&mut cb, merged, class.residual.iter().chain(down.where_preds), &frame)?;

    // Output projection over the merged frame.
    let (sink_input, final_cols) = project_tail(&mut cb, merged, down.items, &frame)?;
    cb.sink(sink_input);
    let circuit = cb.build();
    Ok((circuit, final_cols, k))
}

// ── Range / band emission ───────────────────────────────────────────────────────

fn emit_range(
    down: Demand<'_>,
    class: &JoinClass,
    kind: JoinType,
    sides: &[JoinSide; 2],
) -> Result<EmitPieces, GnitzSqlError> {
    let eq: &[EqPair] = &class.eq;
    let range: &HirRange = class.range.as_ref().expect("emit_range receives a range class");
    let (left, right) = (&sides[0], &sides[1]);
    let (pl, pr) = (left.n(), right.n());

    let n_eq = eq.len();
    let k = n_eq + 1;
    let (pa, pb) = (left.pa(), right.pa());
    let pair_pk = pa + pb;

    reject_pair_pk_overflow("range JOIN", pa, pb)?;
    // The widest node: the pre-rekey `[_join_pk × k, kept-A, kept-B]` or the
    // post-rekey `[_pair_pk, kept-A, kept-B]`, whichever key region is wider.
    crate::validate::reject_column_overflow("range JOIN view intermediate", k.max(pair_pk) + pl + pr)?;

    if n_eq == 0 {
        reject_pure_range_outer(kind, range.tc)?;
    }

    let mut cb = CircuitBuilder::new(0);
    let input_a_raw = cb.input_delta_tagged(left.seg.tid);
    let input_b_raw = cb.input_delta_tagged(right.seg.tid);

    let pro = range_prologue(&mut cb, sides, input_a_raw, input_b_raw, eq, range)?;
    let (int_a, int_b) = if n_eq == 0 {
        (cb.worker_filter(pro.reindex_a), cb.worker_filter(pro.reindex_b))
    } else {
        (pro.reindex_a, pro.reindex_b)
    };
    let trace_a = cb.integrate_trace(int_a);
    let trace_b = cb.integrate_trace(int_b);
    let merged = pro.range_merged(&mut cb, trace_a, trace_b, pl, pr);

    // The `[key region, kept-A, kept-B]` payload behind either key region: the
    // per-term `_join_pk × k` the residual reads, and the `_pair_pk` the outer
    // WHERE and the output projection read after the re-key. The payload defs
    // carry the outer nullability in both — a residual is INNER-only
    // (`reject_outer_with_residual`), where widening is a no-op.
    let payload = || {
        left.ids()
            .chain(right.ids())
            .zip(combined_payload_coldefs(&left.coldefs, &right.coldefs, kind))
    };
    let join_pk_frame = Frame::keyed(pro.out_pk_coldefs(), payload());
    let pair_pk_frame = Frame::keyed(pair_pk_coldefs(&left.seg.schema, &right.seg.schema), payload());

    // Residual filter over the pre-rekey `[_join_pk × k, kept-A, kept-B]`.
    let merged = super::emit_filter(&mut cb, merged, &class.residual, &join_pk_frame)?;

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
    let projected = if kind == JoinType::Inner {
        rekey
    } else {
        let unioned = if n_eq == 0 {
            // Pure-range threshold subtraction: `A − matched` against `m = MAX/MIN(b.range)`.
            let matched = build_pure_range_threshold(&mut cb, left, range, pro.reindex_b, int_a, trace_a);
            let a_pass = rekey_pure_range_a(&mut cb, int_a, pa, pl);
            let nu_a = pure_range_unmatched(
                &mut cb,
                matched,
                a_pass,
                pro.left_key_nullable,
                input_a_raw,
                &pro.left_reindex_cols,
                left,
            )?;
            let branch = emit_range_null_fill_tail(&mut cb, sides, nu_a, true);
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
                let pi = band_pi_preserved(&mut cb, merged, k, payload_off, side);
                let all = rekey_aux_on_source_pk(&mut cb, raw, &side.seg.schema, &side.keep);
                let nu = cb.positive_diff(all, pi);
                let branch = emit_range_null_fill_tail(&mut cb, sides, nu, preserved_is_left);
                acc = cb.union(branch, acc);
            }
            acc
        };

        // One linear 3VL WHERE over the full-width `[pair-PK, kept-A, kept-B]`.
        super::emit_filter(&mut cb, unioned, down.where_preds, &pair_pk_frame)?
    };

    let (sink_input, final_cols) = project_tail(&mut cb, projected, down.items, &pair_pk_frame)?;
    let sharded = cb.shard(sink_input, &(0..pair_pk).collect::<Vec<_>>());
    cb.sink(sharded);
    let circuit = cb.build();
    Ok((circuit, final_cols, pair_pk))
}

// ── Cross emission ──────────────────────────────────────────────────────────────

/// The keyless join `A × B`, INNER only. Each side keys its trace on its own
/// source PK, which takes no part in the match — it only partitions the trace the
/// broadcast delta is paired against.
fn emit_cross(down: Demand<'_>, class: &JoinClass, sides: &[JoinSide; 2]) -> Result<EmitPieces, GnitzSqlError> {
    let (left, right) = (&sides[0], &sides[1]);
    let (pl, pr) = (left.n(), right.n());
    let (pa, pb) = (left.pa(), right.pa());
    let pair_pk = pa + pb;

    reject_pair_pk_overflow("CROSS JOIN", pa, pb)?;
    // The widest node: the post-rekey `[_pair_pk, kept-A, kept-B]` (each term's
    // own key region is one side's PK alone, and so narrower).
    crate::validate::reject_column_overflow("CROSS JOIN view intermediate", pair_pk + pl + pr)?;

    let mut cb = CircuitBuilder::new(0);
    let input_a_raw = cb.input_delta_tagged(left.seg.tid);
    let input_b_raw = cb.input_delta_tagged(right.seg.tid);

    let reindex_a = rekey_scatter_on_source_pk(&mut cb, input_a_raw, &left.seg.schema, &left.keep);
    let reindex_b = rekey_scatter_on_source_pk(&mut cb, input_b_raw, &right.seg.schema, &right.keep);
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
    let ba_keep: Vec<u32> = ba_to_ab_cols(pb, pl, pr).map(|c| c as u32).collect();
    let ba = cb.map_reindex(
        join_ba,
        &self_derived_key(&pair_pk_slots(sides, pb + pr, pb)),
        &ba_keep,
        ReindexRole::Auxiliary,
    );
    let merged = cb.union(ab, ba); // [pair-PK, A, B]

    // `reject_keyless_non_inner` admits only an INNER keyless step, which widens
    // neither side.
    let frame = Frame::keyed(
        pair_pk_coldefs(&left.seg.schema, &right.seg.schema),
        left.ids()
            .chain(right.ids())
            .zip(combined_payload_coldefs(&left.coldefs, &right.coldefs, JoinType::Inner)),
    );
    let filtered = super::emit_filter(&mut cb, merged, class.residual.iter().chain(down.where_preds), &frame)?;
    let (sink_input, final_cols) = project_tail(&mut cb, filtered, down.items, &frame)?;

    let sharded = cb.shard(sink_input, &(0..pair_pk).collect::<Vec<_>>());
    cb.sink(sharded);
    Ok((cb.build(), final_cols, pair_pk))
}

// ── keep-set + shared helpers ───────────────────────────────────────────────────

/// Collect every `ColId` a classified join's ON references (eq/range key pairs +
/// residual conjuncts) into the live set.
fn class_referenced(class: &JoinClass, live: &mut HashSet<ColId>) {
    live.extend(class.key_cols(true).chain(class.key_cols(false)));
    collect_live_cols(&class.residual, live);
}

/// The combined `[A cols, B cols]` output payload of one join step, with the
/// outer-join nullability adjustment applied through [`widen_if`] — the same
/// primitive `RelExpr::cols` widens the *logical* join output with, so the physical
/// and logical schemas cannot disagree (a client decoding a NULL in a NOT NULL
/// column would panic). Shared by the equi and range join lowering.
fn combined_payload_coldefs(left: &[ColumnDef], right: &[ColumnDef], join_type: JoinType) -> Vec<ColumnDef> {
    let mut cols = left.to_vec();
    widen_if(cols.iter_mut(), join_type.preserves_right());
    let mut rcols = right.to_vec();
    widen_if(rcols.iter_mut(), join_type.preserves_left());
    cols.extend(rcols);
    cols
}
