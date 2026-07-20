//! The join emission shell. Reproduces `plan::view::join::emit_join` /
//! `emit_range_join` node-for-node by calling the same AST-free primitives, but
//! sources the keep-set, residual filter, and output projection from the HIR
//! (`JoinClass`, `ColId`s, `ProjEntry`) instead of the AST (`AliasMap`,
//! `SelectItem`, `Expr`). It also hosts the segment-cut and source-collision
//! rules.

use super::super::{slot_of, ColId, ColIdGen, EqPair, HirExpr, HirRange, HirRef, JoinClass, ProjEntry, RelExpr};
use super::{resolve_collisions, resolve_input, CutMemo, SegInput};
use crate::error::GnitzSqlError;
use crate::hir::physical;
use crate::ir::BExpr;
use crate::lower::compile_filter_program;
use crate::plan::view::join::{
    band_union_schema, build_pure_range_threshold, combined_payload_coldefs, emit_equi_join_terms, emit_equi_null_fill,
    emit_range_null_fill_tail, is_identity_projection, join_pk_coldefs, normalize_to_ab, pair_pk_coldefs, prune_schema,
    range_gate_reindex_prologue, range_slots, reject_pair_pk_overflow, reject_pure_range_outer, side_target_tcs,
    union_null_key_rows, EquiNullFill, EquiSide, JoinType, RangeSlots,
};
use crate::plan::view::predicates::{build_reindex_program, rekey_on_source_pk, RangeConjunct};
use crate::plan::view::{EmitPieces, ViewChain};
use gnitz_core::{CircuitBuilder, ColumnDef, GnitzClient, NodeId, TypeCode};
use std::collections::HashSet;

/// The downstream demand on a join's output: the final projection and the
/// post-join WHERE (outer joins only; empty for INNER — the rewrite folded the
/// WHERE into the residual).
#[derive(Clone, Copy)]
struct Demand<'a> {
    items: &'a [ProjEntry],
    where_preds: &'a [HirExpr],
}

impl Demand<'_> {
    /// Every `ColId` the demand references, into `out`.
    fn collect_refs(&self, out: &mut HashSet<ColId>) {
        for e in self.items.iter().map(|i| &i.expr).chain(self.where_preds) {
            e.for_each_ref(&mut |HirRef::Col(id)| {
                out.insert(*id);
            });
        }
    }
}

/// Lower a `Project(Filter?(Join))` tree's join to circuit pieces for `view_id`,
/// plus the output `ColId` layout: `[k hidden `_join_pk` / `_pair_pk` slots]`
/// followed by the projected payload in item order.
#[allow(clippy::too_many_arguments)]
pub(crate) fn lower_join_view(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    ids: &mut ColIdGen,
    memo: &mut CutMemo,
    project_items: &[ProjEntry],
    where_preds: &[HirExpr],
    join: &RelExpr,
    view_id: u64,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let down = Demand {
        items: project_items,
        where_preds,
    };
    let pieces = emit_step(client, chain, ids, memo, down, join, view_id)?;
    // The emitted pk-list is the synthetic key region; its width is the number of
    // hidden placeholder ids the layout leads with.
    let (_, _, ref pk_cols) = pieces;
    let mut layout = ids.placeholders(pk_cols.len());
    layout.extend(project_items.iter().map(|i| i.out.id));
    Ok((pieces, layout))
}

/// Emit one join step, resolving (and cutting/wrapping) its two inputs first.
fn emit_step(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    ids: &mut ColIdGen,
    memo: &mut CutMemo,
    down: Demand<'_>,
    join: &RelExpr,
    view_id: u64,
) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::Join {
        left,
        right,
        kind,
        classified,
        ..
    } = join
    else {
        unreachable!("emit_step receives a Join");
    };
    let class = classified.as_ref().expect("join classified before lowering");

    // The live-column set flowing into this join's inputs: everything consumed
    // downstream (this join's output projection + WHERE) plus this join's own ON.
    // A cut input is pruned to the cols in this set (the chain-liveness rule — the
    // registered segment schema is `join_pk + pruned projection`). For a cut
    // sub-join, `down.items` is already its pruned identity, so this closes over
    // the whole suffix transitively.
    let mut live: HashSet<ColId> = HashSet::new();
    down.collect_refs(&mut live);
    class_referenced(class, &mut live);

    // Segment-cut then source-collision, both through the shared rules: a repeated
    // tid (self-join) wraps the later side in a pass-through segment so the two
    // delta inputs are distinct sources. The wrapped `Get` reuses its ColIds,
    // re-ordered to the wrapper's PK-front schema — `resolve_refs` absorbs it.
    let mut inputs = [
        resolve_input(client, chain, ids, memo, left, &live)?,
        resolve_input(client, chain, ids, memo, right, &live)?,
    ];
    resolve_collisions(client, chain, ids, &mut inputs, &[left, right], false)?;
    let [left_in, right_in] = inputs;

    if class.range.is_some() {
        emit_range(ids, down, class, *kind, &left_in, &right_in, view_id)
    } else {
        emit_equi(ids, down, class, *kind, &left_in, &right_in, view_id)
    }
}

// ── Equi emission (reproduces emit_join's equi path) ────────────────────────────

fn emit_equi(
    ids: &mut ColIdGen,
    down: Demand<'_>,
    class: &JoinClass,
    kind: JoinType,
    left_in: &SegInput,
    right_in: &SegInput,
    view_id: u64,
) -> Result<EmitPieces, GnitzSqlError> {
    let left_schema = &left_in.schema;
    let right_schema = &right_in.schema;
    let left_n = left_schema.columns.len();
    let right_n = right_schema.columns.len();

    // Key columns: the eq pairs resolved to positions in each side's layout.
    let left_join_cols: Vec<usize> = class
        .eq
        .iter()
        .map(|p| slot_of(&left_in.layout, p.left))
        .collect::<Result<_, _>>()?;
    let right_join_cols: Vec<usize> = class
        .eq
        .iter()
        .map(|p| slot_of(&right_in.layout, p.right))
        .collect::<Result<_, _>>()?;
    let target_tcs: Vec<TypeCode> = class.eq.iter().map(|p| p.tc).collect();
    let k = left_join_cols.len();
    let left_target_tcs = side_target_tcs(&left_join_cols, &left_schema.columns, &target_tcs);
    let right_target_tcs = side_target_tcs(&right_join_cols, &right_schema.columns, &target_tcs);

    // Reindex-payload keep set (equi_keep_combined + the ">= col 0" guard).
    let keep = keep_set(down, class, kind, left_in, right_in);
    let keep_l: Vec<usize> = (0..left_n).filter(|&i| keep[i]).collect();
    let keep_r: Vec<usize> = (0..right_n).filter(|&i| keep[left_n + i]).collect();
    let pl = keep_l.len();
    let pr = keep_r.len();
    let pruned_left_schema = prune_schema(left_schema, &keep_l);
    let pruned_right_schema = prune_schema(right_schema, &keep_r);
    crate::plan::validate::reject_column_overflow("JOIN view output", k + pl + pr)?;

    let mut cb = CircuitBuilder::new(view_id, 0);
    let input_a_raw = cb.input_delta_tagged(left_in.tid);
    let input_b_raw = cb.input_delta_tagged(right_in.tid);

    let terms = emit_equi_join_terms(
        &mut cb,
        EquiSide {
            input: input_a_raw,
            cols: &left_join_cols,
            target_tcs: &left_target_tcs,
            coldefs: &left_schema.columns,
            keep: &keep_l,
        },
        EquiSide {
            input: input_b_raw,
            cols: &right_join_cols,
            target_tcs: &right_target_tcs,
            coldefs: &right_schema.columns,
            keep: &keep_r,
        },
    )?;
    let inner_merged = normalize_to_ab(&mut cb, terms.join_ab, terms.join_ba, k, pl, pr);

    let merged = emit_equi_null_fill(
        &mut cb,
        EquiNullFill {
            inner_merged,
            kind,
            k,
            pl,
            pr,
            left: EquiSide {
                input: input_a_raw,
                cols: &left_join_cols,
                target_tcs: &left_target_tcs,
                coldefs: &left_schema.columns,
                keep: &keep_l,
            },
            right: EquiSide {
                input: input_b_raw,
                cols: &right_join_cols,
                target_tcs: &right_target_tcs,
                coldefs: &right_schema.columns,
                keep: &keep_r,
            },
            terms: &terms,
            pruned_left: &pruned_left_schema.columns,
            pruned_right: &pruned_right_schema.columns,
        },
    );

    // Virtual combined output schema: k `_join_pk` cols + kept-A + kept-B (with the
    // outer nullability applied to the payload).
    let mut out_cols: Vec<ColumnDef> = join_pk_coldefs(&target_tcs);
    out_cols.extend(combined_payload_coldefs(
        &pruned_left_schema,
        &pruned_right_schema,
        kind,
    ));

    // The merged-layout ColId vector: k hidden placeholders then the kept payload
    // ids — resolve_refs against it yields `k + pruned_index` for both the residual
    // filter and the output projection.
    let mut merged_layout = ids.placeholders(k);
    merged_layout.extend(keep_l.iter().map(|&i| left_in.layout[i]));
    merged_layout.extend(keep_r.iter().map(|&i| right_in.layout[i]));

    // One residual/WHERE filter over the normalized output (at most one source is
    // non-empty — classify_join_where). const-elision is harmless (INNER 3VL).
    let merged = apply_filter(
        &mut cb,
        merged,
        &class.residual,
        down.where_preds,
        &merged_layout,
        &out_cols,
    )?;

    // Output projection (build_join_view_projection): each item is a column ref;
    // its slot is resolve_refs against the merged layout, its def is the ProjEntry's.
    let mut final_cols: Vec<ColumnDef> = out_cols[..k].to_vec();
    let mut final_projection: Vec<usize> = Vec::with_capacity(down.items.len());
    for item in down.items {
        final_projection.push(slot_of_expr(&item.expr, &merged_layout)?);
        final_cols.push(item.out.def.clone());
    }

    let sink_input = if is_identity_projection(&final_projection, pl + pr, k) {
        merged
    } else {
        cb.map(merged, &final_projection)
    };
    cb.sink(sink_input);
    let circuit = cb.build();
    let view_pk: Vec<u32> = (0..k as u32).collect();
    Ok((circuit, final_cols, view_pk))
}

// ── Range / band emission (reproduces emit_range_join) ──────────────────────────

fn emit_range(
    ids: &mut ColIdGen,
    down: Demand<'_>,
    class: &JoinClass,
    kind: JoinType,
    left_in: &SegInput,
    right_in: &SegInput,
    view_id: u64,
) -> Result<EmitPieces, GnitzSqlError> {
    let eq: &[EqPair] = &class.eq;
    let range: &HirRange = class.range.as_ref().expect("emit_range receives a range class");
    let left_schema = &left_in.schema;
    let right_schema = &right_in.schema;
    let left_n = left_schema.columns.len();
    let right_n = right_schema.columns.len();

    let left_cols: Vec<usize> = eq
        .iter()
        .map(|p| slot_of(&left_in.layout, p.left))
        .collect::<Result<_, _>>()?;
    let right_cols: Vec<usize> = eq
        .iter()
        .map(|p| slot_of(&right_in.layout, p.right))
        .collect::<Result<_, _>>()?;
    let eq_tcs: Vec<TypeCode> = eq.iter().map(|p| p.tc).collect();
    let range_conj = RangeConjunct {
        left_col: slot_of(&left_in.layout, range.left)?,
        right_col: slot_of(&right_in.layout, range.right)?,
        op: range.op,
        tc: range.tc,
    };
    let n_eq = left_cols.len();
    let k = n_eq + 1;
    let pa = left_schema.pk_cols.len();
    let pb = right_schema.pk_cols.len();
    let pair_pk = pa + pb;

    reject_pair_pk_overflow(pa, pb)?;
    crate::plan::validate::reject_column_overflow("range JOIN view intermediate", pair_pk + k + left_n + right_n)?;

    if n_eq == 0 {
        reject_pure_range_outer(kind, range.tc)?;
    }

    let slots = range_slots(
        &left_cols,
        &right_cols,
        &eq_tcs,
        &range_conj,
        &left_schema.columns,
        &right_schema.columns,
    );

    let mut cb = CircuitBuilder::new(view_id, 0);
    let input_a_raw = cb.input_delta_tagged(left_in.tid);
    let input_b_raw = cb.input_delta_tagged(right_in.tid);

    let (reindex_a, reindex_b, left_key_nullable) = range_gate_reindex_prologue(
        &mut cb,
        input_a_raw,
        input_b_raw,
        &slots,
        &left_schema.columns,
        &right_schema.columns,
    )?;
    let RangeSlots {
        left_reindex_cols,
        all_tcs,
        rel_ab,
        rel_ba,
        ..
    } = slots;

    let (int_a, int_b) = if n_eq == 0 {
        (cb.partition_filter(reindex_a), cb.partition_filter(reindex_b))
    } else {
        (reindex_a, reindex_b)
    };
    let trace_a = cb.integrate_trace(int_a);
    let trace_b = cb.integrate_trace(int_b);
    let join_ab = cb.join_with_trace_range_node(reindex_a, trace_b, n_eq as u8, rel_ab);
    let join_ba = cb.join_with_trace_range_node(reindex_b, trace_a, n_eq as u8, rel_ba);

    let merged = normalize_to_ab(&mut cb, join_ab, join_ba, k, left_n, right_n);

    // Residual filter over the pre-rekey `[_join_pk × k, A, B]` layout at base k.
    let union_schema = band_union_schema(&all_tcs, left_schema, right_schema);
    let mut band_layout = ids.placeholders(k);
    band_layout.extend(left_in.layout.iter().copied());
    band_layout.extend(right_in.layout.iter().copied());
    let merged = if class.residual.is_empty() {
        merged
    } else {
        let folded = physical::fold_preds(&class.residual, &band_layout)?.expect("non-empty residual");
        let prog = compile_filter_program(&folded, &union_schema.columns)?.expect("residual is not a bare constant");
        cb.filter(merged, Some(prog))
    };

    // Re-key onto the source-PK pair `_pair_pk`.
    let mut pair_pk_cols: Vec<usize> = Vec::with_capacity(pair_pk);
    for &a_pk in &left_schema.pk_cols {
        pair_pk_cols.push(k + a_pk);
    }
    for &b_pk in &right_schema.pk_cols {
        pair_pk_cols.push(k + left_n + b_pk);
    }
    let zero_tcs = vec![0u8; pair_pk];
    let rekey = cb.map_reindex(
        merged,
        &pair_pk_cols,
        &zero_tcs,
        build_reindex_program(&union_schema.columns),
    );

    let payload_offset = pair_pk + k;
    let pair_pk_coldefs: Vec<ColumnDef> = pair_pk_coldefs(left_schema, right_schema);

    // Output projection: INNER reads off `rekey` (payload at `payload_offset`, and
    // always maps to drop the per-term `_join_pk` slots); OUTER reads off the
    // full-width post-null-fill union (payload at `pair_pk`).
    let user_offset = if kind == JoinType::Inner {
        payload_offset
    } else {
        pair_pk
    };
    let mut proj_layout = ids.placeholders(user_offset);
    proj_layout.extend(left_in.layout.iter().copied());
    proj_layout.extend(right_in.layout.iter().copied());
    let mut final_cols: Vec<ColumnDef> = pair_pk_coldefs.clone();
    let mut final_projection: Vec<usize> = Vec::with_capacity(down.items.len());
    for item in down.items {
        final_projection.push(slot_of_expr(&item.expr, &proj_layout)?);
        final_cols.push(item.out.def.clone());
    }

    let sink_input = if kind == JoinType::Inner {
        cb.map(rekey, &final_projection)
    } else {
        let nf_tail = |cb: &mut CircuitBuilder, nf_keyed: NodeId, preserved_is_left: bool| -> NodeId {
            emit_range_null_fill_tail(cb, left_schema, right_schema, nf_keyed, preserved_is_left)
        };

        // Full-width inner pairs `[pair-PK, A, B]` — drop the per-term `_join_pk` slots.
        let inner_full = cb.map(
            rekey,
            &(payload_offset..payload_offset + left_n + right_n).collect::<Vec<_>>(),
        );
        let zero_a = vec![0u8; pa];
        let unioned = if n_eq == 0 {
            // Pure-range threshold subtraction: `A − matched` against `m = MAX/MIN(b.range)`.
            let thr = build_pure_range_threshold(
                &mut cb,
                left_schema,
                range.tc,
                range.op,
                reindex_b,
                int_a,
                trace_a,
                true,
            );
            let matched = thr.matched;
            let a_pass = thr.a_pass.expect("a_pass requested");
            let neg = cb.negate(matched);
            let nf_match = cb.union(a_pass, neg);
            let nf_keyed = if left_key_nullable {
                union_null_key_rows(&mut cb, nf_match, input_a_raw, &left_reindex_cols, left_schema)?
            } else {
                nf_match
            };
            let branch = nf_tail(&mut cb, nf_keyed, true);
            cb.union(branch, inner_full)
        } else {
            // Band ν_A (preserves_left) and/or ν_B (preserves_right).
            let zero_b = vec![0u8; pb];
            let mut acc = inner_full;
            if kind.preserves_left() {
                let rekey_a = cb.map_reindex(
                    merged,
                    &pair_pk_cols[..pa],
                    &zero_a,
                    build_reindex_program(&union_schema.columns),
                );
                let proj_a = cb.map(rekey_a, &(pa + k..pa + k + left_n).collect::<Vec<_>>()); // π_A(inner)
                let a_all = rekey_on_source_pk(&mut cb, input_a_raw, left_schema);
                let nu_a = cb.positive_diff(a_all, proj_a);
                let branch = nf_tail(&mut cb, nu_a, true);
                acc = cb.union(branch, acc);
            }
            if kind.preserves_right() {
                let rekey_b = cb.map_reindex(
                    merged,
                    &pair_pk_cols[pa..],
                    &zero_b,
                    build_reindex_program(&union_schema.columns),
                );
                let proj_b = cb.map(
                    rekey_b,
                    &(pb + k + left_n..pb + k + left_n + right_n).collect::<Vec<_>>(),
                ); // π_B(inner)
                let b_all = rekey_on_source_pk(&mut cb, input_b_raw, right_schema);
                let nu_b = cb.positive_diff(b_all, proj_b);
                let branch = nf_tail(&mut cb, nu_b, false);
                acc = cb.union(branch, acc);
            }
            acc
        };

        // One linear 3VL WHERE over the full-width `[pair-PK, A, B]` (base pair_pk).
        let combined_payload = combined_payload_coldefs(left_schema, right_schema, kind);
        let combined_cols: Vec<ColumnDef> = pair_pk_coldefs.iter().cloned().chain(combined_payload).collect();
        let mut where_layout = ids.placeholders(pair_pk);
        where_layout.extend(left_in.layout.iter().copied());
        where_layout.extend(right_in.layout.iter().copied());
        let filtered = apply_filter(&mut cb, unioned, &[], down.where_preds, &where_layout, &combined_cols)?;

        if is_identity_projection(&final_projection, left_n + right_n, pair_pk) {
            filtered
        } else {
            cb.map(filtered, &final_projection)
        }
    };

    let pair_pk_idxs: Vec<usize> = (0..pair_pk).collect();
    let sharded = cb.shard(sink_input, &pair_pk_idxs);
    cb.sink(sharded);
    let circuit = cb.build();
    let view_pk: Vec<u32> = (0..pair_pk as u32).collect();
    Ok((circuit, final_cols, view_pk))
}

// ── keep-set + shared helpers ───────────────────────────────────────────────────

/// The four-contributor reindex-payload keep set (equi_keep_combined) plus the
/// ">= col 0" guard. A wildcard projection is already expanded into `ProjEntry`
/// column refs by bind, so Rule 1 marks each visible projected col (a hidden col
/// on a `SELECT *` is dropped — result-identical, unpinned).
fn keep_set(down: Demand<'_>, class: &JoinClass, kind: JoinType, left_in: &SegInput, right_in: &SegInput) -> Vec<bool> {
    let left_layout = &left_in.layout;
    let right_layout = &right_in.layout;
    let left_n = left_layout.len();
    let right_n = right_layout.len();
    let mut keep = vec![false; left_n + right_n];
    let mark = |keep: &mut Vec<bool>, id: ColId| {
        if let Some(p) = left_layout.iter().position(|c| *c == id) {
            keep[p] = true;
        } else if let Some(p) = right_layout.iter().position(|c| *c == id) {
            keep[left_n + p] = true;
        }
    };
    // Rules 1 + 2: projection, residual ON + top-level WHERE.
    for e in down
        .items
        .iter()
        .map(|i| &i.expr)
        .chain(down.where_preds)
        .chain(&class.residual)
    {
        e.for_each_ref(&mut |HirRef::Col(id)| mark(&mut keep, *id));
    }
    // Rule 3: preserved side's nullable join-key components.
    if kind.preserves_left() {
        for p in &class.eq {
            if let Some(pos) = left_layout.iter().position(|c| *c == p.left) {
                if left_in.schema.columns[pos].is_nullable {
                    keep[pos] = true;
                }
            }
        }
    }
    if kind.preserves_right() {
        for p in &class.eq {
            if let Some(pos) = right_layout.iter().position(|c| *c == p.right) {
                if right_in.schema.columns[pos].is_nullable {
                    keep[left_n + pos] = true;
                }
            }
        }
    }
    // Rule 4: a side that keeps nothing still retains its column 0.
    if left_n > 0 && !keep[..left_n].iter().any(|&b| b) {
        keep[0] = true;
    }
    if right_n > 0 && !keep[left_n..].iter().any(|&b| b) {
        keep[left_n] = true;
    }
    keep
}

/// Collect every `ColId` a classified join's ON references (eq/range key pairs +
/// residual conjuncts) into the live set.
fn class_referenced(class: &JoinClass, live: &mut HashSet<ColId>) {
    for p in &class.eq {
        live.insert(p.left);
        live.insert(p.right);
    }
    if let Some(r) = &class.range {
        live.insert(r.left);
        live.insert(r.right);
    }
    for e in &class.residual {
        e.for_each_ref(&mut |HirRef::Col(id)| {
            live.insert(*id);
        });
    }
}

/// The physical position of a bare `ColRef` against a layout.
fn slot_of_expr(e: &HirExpr, layout: &[ColId]) -> Result<usize, GnitzSqlError> {
    match e {
        BExpr::ColRef(HirRef::Col(id)) => slot_of(layout, *id),
        _ => Err(GnitzSqlError::Plan(
            "internal: HIR join projection item is not a column reference".into(),
        )),
    }
}

/// Emit the residual/WHERE filter over `merged` (at most one source non-empty).
fn apply_filter(
    cb: &mut CircuitBuilder,
    merged: NodeId,
    residual: &[HirExpr],
    where_preds: &[HirExpr],
    layout: &[ColId],
    cols: &[ColumnDef],
) -> Result<NodeId, GnitzSqlError> {
    super::emit_filter(cb, merged, residual.iter().chain(where_preds), layout, cols)
}
