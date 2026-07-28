//! Equi-, range/band-, semi/anti-, and mark-join emission. The `lower_*_view`
//! shells source the keep-set, residual filter, and output projection from the
//! HIR (`JoinClass`, `ColId`s, `ProjEntry`); the AST-free circuit primitives they
//! drive — reindex each side onto the join key, run the symmetric 2-term DBSP
//! join against the other side's trace, normalize onto `[A cols, B cols]`, and
//! attach the outer null-fill — live in the second half of this module, shared
//! with `exists.rs`. Also hosts the `JoinType`-driven emit geometry and the
//! key-arity / outer-residual / pure-range guards.

use super::super::guards::{converse_rel, reject_pair_pk_overflow, reject_pure_range_outer};
use super::super::{slot_of, widen_if, ColId, EqPair, HirExpr, HirRange, JoinClass, JoinType, ProjEntry, RelExpr};
use super::prims::{
    build_reindex_program, build_reindex_program_keep, multi_null_filter_prog, null_gate, pure_range_m_output_cols,
    rekey_on_source_pk, schema_type_codes,
};
use super::{
    apply_projection, collect_live_cols, key_region_layout, resolve_collisions, resolve_input, resolve_projection,
    CutMemo, SegInput,
};
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};

use gnitz_core::{CircuitBuilder, ColumnDef, GnitzClient, NodeId, RangeRel, ReduceOutKey, Schema, TypeCode};
use gnitz_wire::{AGG_MAX, AGG_MIN};
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
        collect_live_cols(self.items.iter().map(|i| &i.expr).chain(self.where_preds), out);
    }
}

/// Lower a `Project(Filter?(Join))` tree's join to circuit pieces for `view_id`,
/// plus the output `ColId` layout: `[k hidden `_join_pk` / `_pair_pk` slots]`
/// followed by the projected payload in item order.
pub(crate) fn lower_join_view(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
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
    let pieces = emit_step(client, chain, memo, down, join, view_id)?;
    // The emitted pk-list is the synthetic key region; its width is the number of
    // identity-free slots the layout leads with.
    let layout = key_region_layout(pieces.2.len(), project_items);
    Ok((pieces, layout))
}

/// Emit one join step, resolving (and cutting/wrapping) its two inputs first.
fn emit_step(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    down: Demand<'_>,
    join: &RelExpr,
    view_id: u64,
) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::Join {
        left, right, kind, on, ..
    } = join
    else {
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
    down.collect_refs(&mut live);
    class_referenced(class, &mut live);

    // Segment-cut then source-collision, both through the shared rules: a repeated
    // tid (self-join) wraps the later side in a pass-through segment so the two
    // delta inputs are distinct sources. The wrapped `Get` reuses its ColIds,
    // re-ordered to the wrapper's PK-front schema — `resolve_refs` absorbs it.
    let mut inputs = [
        resolve_input(client, chain, memo, left, &live)?,
        resolve_input(client, chain, memo, right, &live)?,
    ];
    resolve_collisions(client, chain, &mut inputs, &[left, right], false)?;
    let [left_in, right_in] = inputs;

    if class.range.is_some() {
        emit_range(down, class, *kind, &left_in, &right_in, view_id)
    } else {
        emit_equi(down, class, *kind, &left_in, &right_in, view_id)
    }
}

/// A join's equality pairs resolved against both inputs: each side's key-column
/// positions in its own layout, and the per-pair promoted type codes.
pub(super) struct EquiKeys {
    pub(super) left: Vec<usize>,
    pub(super) right: Vec<usize>,
    pub(super) tcs: Vec<TypeCode>,
}

/// Resolve a join's equality pairs to [`EquiKeys`] — the shared key prologue of
/// every equi/range emit, join and EXISTS/IN alike.
pub(super) fn resolve_eq_cols(
    eq: &[EqPair],
    left_in: &SegInput,
    right_in: &SegInput,
) -> Result<EquiKeys, GnitzSqlError> {
    Ok(EquiKeys {
        left: eq
            .iter()
            .map(|p| slot_of(&left_in.layout, p.left))
            .collect::<Result<_, _>>()?,
        right: eq
            .iter()
            .map(|p| slot_of(&right_in.layout, p.right))
            .collect::<Result<_, _>>()?,
        tcs: eq.iter().map(|p| p.tc).collect(),
    })
}

// ── Equi emission ───────────────────────────────────────────────────────────────

fn emit_equi(
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
    let EquiKeys {
        left: left_join_cols,
        right: right_join_cols,
        tcs: target_tcs,
    } = resolve_eq_cols(&class.eq, left_in, right_in)?;
    let k = left_join_cols.len();
    let left_target_tcs = side_target_tcs(&left_join_cols, &left_schema.columns, &target_tcs);
    let right_target_tcs = side_target_tcs(&right_join_cols, &right_schema.columns, &target_tcs);

    // Reindex-payload keep set (equi_keep_combined + the ">= col 0" guard).
    let keep = keep_set(down, class, kind, left_in, right_in);
    let keep_l: Vec<usize> = (0..left_n).filter(|&i| keep[i]).collect();
    let keep_r: Vec<usize> = (0..right_n).filter(|&i| keep[left_n + i]).collect();
    let pl = keep_l.len();
    let pr = keep_r.len();
    let pruned_left = kept_coldefs(left_schema, &keep_l);
    let pruned_right = kept_coldefs(right_schema, &keep_r);
    crate::validate::reject_column_overflow("JOIN view output", k + pl + pr)?;

    let mut cb = CircuitBuilder::new(view_id, 0);
    let input_a_raw = cb.input_delta_tagged(left_in.tid);
    let input_b_raw = cb.input_delta_tagged(right_in.tid);

    let left_side = EquiSide {
        input: input_a_raw,
        cols: &left_join_cols,
        target_tcs: &left_target_tcs,
        coldefs: &left_schema.columns,
        keep: &keep_l,
    };
    let right_side = EquiSide {
        input: input_b_raw,
        cols: &right_join_cols,
        target_tcs: &right_target_tcs,
        coldefs: &right_schema.columns,
        keep: &keep_r,
    };

    let terms = emit_equi_join_terms(&mut cb, left_side, right_side)?;
    let inner_merged = normalize_to_ab(&mut cb, terms.join_ab, terms.join_ba, k, pl, pr);

    let merged = emit_equi_null_fill(
        &mut cb,
        EquiNullFill {
            inner_merged,
            kind,
            k,
            left: left_side,
            right: right_side,
            terms: &terms,
        },
    );

    // Virtual combined output schema: k `_join_pk` cols + kept-A + kept-B (with the
    // outer nullability applied to the payload).
    let mut out_cols: Vec<ColumnDef> = join_pk_coldefs(&target_tcs);
    out_cols.extend(combined_payload_coldefs(&pruned_left, &pruned_right, kind));

    // The merged-layout ColId vector: k identity-free key slots then the kept
    // payload ids — resolve_refs against it yields `k + pruned_index` for both the
    // residual filter and the output projection.
    let mut merged_layout = vec![ColId::NONE; k];
    merged_layout.extend(keep_l.iter().map(|&i| left_in.layout[i]));
    merged_layout.extend(keep_r.iter().map(|&i| right_in.layout[i]));

    // One residual/WHERE filter over the normalized output (at most one source is
    // non-empty — the rewrite folds the WHERE into the INNER residual and leaves an
    // OUTER WHERE as the post-null-fill filter). const-elision is harmless (INNER 3VL).
    let merged = super::emit_filter(
        &mut cb,
        merged,
        class.residual.iter().chain(down.where_preds),
        &merged_layout,
        &out_cols,
    )?;

    // Output projection: each item is a column ref against the merged layout.
    let (final_projection, final_cols) = resolve_projection(down.items, &merged_layout, out_cols[..k].to_vec())?;
    let sink_input = apply_projection(&mut cb, merged, &final_projection, pl + pr, k);
    cb.sink(sink_input);
    let circuit = cb.build();
    let view_pk: Vec<u32> = (0..k as u32).collect();
    Ok((circuit, final_cols, view_pk))
}

// ── Range / band emission ───────────────────────────────────────────────────────

fn emit_range(
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

    let n_eq = eq.len();
    let k = n_eq + 1;
    let pa = left_schema.pk_cols.len();
    let pb = right_schema.pk_cols.len();
    let pair_pk = pa + pb;

    reject_pair_pk_overflow(pa, pb)?;
    crate::validate::reject_column_overflow("range JOIN view intermediate", pair_pk + k + left_n + right_n)?;

    if n_eq == 0 {
        reject_pure_range_outer(kind, range.tc)?;
    }

    let mut cb = CircuitBuilder::new(view_id, 0);
    let input_a_raw = cb.input_delta_tagged(left_in.tid);
    let input_b_raw = cb.input_delta_tagged(right_in.tid);

    let RangePrologue {
        reindex_a,
        reindex_b,
        left_key_nullable,
        left_reindex_cols,
        all_tcs,
        rel_ab,
        rel_ba,
    } = range_prologue(&mut cb, left_in, right_in, input_a_raw, input_b_raw, eq, range)?;

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

    // The `[key region, A, B]` layout at any key width — the residual, the output
    // projection, and the outer WHERE each read the same payload behind a different
    // leading key region (`_join_pk × k`, `_pair_pk + _join_pk`, `_pair_pk`).
    let side_layout = |npk: usize| -> Vec<ColId> {
        let mut l = vec![ColId::NONE; npk];
        l.extend(left_in.layout.iter().copied());
        l.extend(right_in.layout.iter().copied());
        l
    };

    // Residual filter over the pre-rekey `[_join_pk × k, A, B]` layout at base k.
    let union_schema = band_union_schema(&all_tcs, left_schema, right_schema);
    let merged = super::emit_filter(&mut cb, merged, &class.residual, &side_layout(k), &union_schema.columns)?;

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
        build_reindex_program(union_schema.columns.len()),
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
    let (final_projection, final_cols) =
        resolve_projection(down.items, &side_layout(user_offset), pair_pk_coldefs.clone())?;

    let sink_input = if kind == JoinType::Inner {
        cb.map(rekey, &final_projection)
    } else {
        // Full-width inner pairs `[pair-PK, A, B]` — drop the per-term `_join_pk` slots.
        let inner_full = cb.map(
            rekey,
            &(payload_offset..payload_offset + left_n + right_n).collect::<Vec<_>>(),
        );
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
            let nu_a = pure_range_unmatched(
                &mut cb,
                &thr,
                left_key_nullable,
                input_a_raw,
                &left_reindex_cols,
                left_schema,
            )?;
            let branch = emit_range_null_fill_tail(&mut cb, left_schema, right_schema, nu_a, true);
            cb.union(branch, inner_full)
        } else {
            // Band ν_A (preserves_left) and/or ν_B (preserves_right) — the two
            // mirror sides of `ν_P = positive_part(P_all − π_P(inner))`.
            let mut acc = inner_full;
            for (preserved_is_left, pk_slots, payload_off, payload_n, raw, schema) in [
                (true, &pair_pk_cols[..pa], 0, left_n, input_a_raw, left_schema),
                (false, &pair_pk_cols[pa..], left_n, right_n, input_b_raw, right_schema),
            ] {
                if !kind.preserves(preserved_is_left) {
                    continue;
                }
                let pi = band_pi_preserved(
                    &mut cb,
                    merged,
                    union_schema.columns.len(),
                    pk_slots,
                    k,
                    payload_off,
                    payload_n,
                );
                let all = rekey_on_source_pk(&mut cb, raw, schema);
                let nu = cb.positive_diff(all, pi);
                let branch = emit_range_null_fill_tail(&mut cb, left_schema, right_schema, nu, preserved_is_left);
                acc = cb.union(branch, acc);
            }
            acc
        };

        // One linear 3VL WHERE over the full-width `[pair-PK, A, B]` (base pair_pk).
        let combined_payload = combined_payload_coldefs(&left_schema.columns, &right_schema.columns, kind);
        let combined_cols: Vec<ColumnDef> = pair_pk_coldefs.iter().cloned().chain(combined_payload).collect();
        let filtered = super::emit_filter(
            &mut cb,
            unioned,
            down.where_preds,
            &side_layout(pair_pk),
            &combined_cols,
        )?;

        apply_projection(&mut cb, filtered, &final_projection, left_n + right_n, pair_pk)
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
    let mut referenced: HashSet<ColId> = HashSet::new();
    collect_live_cols(
        down.items
            .iter()
            .map(|i| &i.expr)
            .chain(down.where_preds)
            .chain(&class.residual),
        &mut referenced,
    );
    for id in referenced {
        mark(&mut keep, id);
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

/// This side's kept payload column defs, in ascending source order.
fn kept_coldefs(schema: &Schema, keep: &[usize]) -> Vec<ColumnDef> {
    keep.iter().map(|&i| schema.columns[i].clone()).collect()
}

/// Per-side carried reindex target type for each join key slot: `T_i` only when
/// the side's own self-derived reindex output type differs from `T_i` (a
/// cross-width/cross-sign promotion on that side), else `0` (self-derive). The
/// `0` keeps same-type / U128-vs-UUID / string circuits byte-identical to the
/// pre-promotion serialization. The encode rule lives in `carried_reindex_tc`,
/// the round-trip inverse of the engine's `resolve_reindex_type`. Shared by the
/// equi builder (`slot_tcs = target_tcs`), the range builder
/// (`slot_tcs = all_tcs`, the eq prefix plus the range slot), and the
/// EXISTS/IN semi-join builder.
pub(crate) fn side_target_tcs(cols: &[usize], coldefs: &[ColumnDef], slot_tcs: &[TypeCode]) -> Vec<u8> {
    cols.iter()
        .zip(slot_tcs)
        .map(|(&c, &t)| coldefs[c].type_code.carried_reindex_tc(t))
        .collect()
}

/// Normalize the two per-term join outputs onto the canonical `[A cols, B cols]`
/// payload layout and union them. Term AB is already canonical
/// (`[_join_pk × k, A, B]`); term BA is `[_join_pk × k, B, A]` and is reordered.
/// Shared verbatim by the equi (`k = eq slots`) and range (`k = n_eq + 1`)
/// builders, and by the band EXISTS/IN semi-join circuit.
pub(crate) fn normalize_to_ab(
    cb: &mut CircuitBuilder,
    join_ab: gnitz_core::NodeId,
    join_ba: gnitz_core::NodeId,
    k: usize,
    left_n: usize,
    right_n: usize,
) -> gnitz_core::NodeId {
    // Path AB projection: identity (already canonical: [PK cols, A_cols, B_cols])
    let proj_ab: Vec<usize> = (k..k + left_n + right_n).collect();
    let proj_ab_node = cb.map(join_ab, &proj_ab);

    // Path BA projection: reorder [A_cols, B_cols]
    let mut proj_ba: Vec<usize> = Vec::new();
    for i in 0..left_n {
        proj_ba.push(k + right_n + i);
    }
    for i in 0..right_n {
        proj_ba.push(k + i);
    }
    let proj_ba_node = cb.map(join_ba, &proj_ba);

    cb.union(proj_ab_node, proj_ba_node)
}

/// One side of the symmetric equi-join term emission: its (unfiltered) input
/// node, the join key columns with their per-slot carried target types, the
/// source schema, and the kept payload columns (`0..n` for the unpruned
/// layout — `build_reindex_program_keep(0..n)` is byte-identical to
/// `build_reindex_program`).
#[derive(Clone, Copy)]
pub(crate) struct EquiSide<'a> {
    pub(crate) input: gnitz_core::NodeId,
    pub(crate) cols: &'a [usize],
    pub(crate) target_tcs: &'a [u8],
    pub(crate) coldefs: &'a [ColumnDef],
    pub(crate) keep: &'a [usize],
}

impl EquiSide<'_> {
    /// This side's kept-payload width in the merged layout.
    fn kept_n(&self) -> usize {
        self.keep.len()
    }

    /// The type codes of this side's kept payload columns — what `null_extend`
    /// needs to synthesize the OTHER side's NULL region. Derived from `keep` +
    /// `coldefs` so it cannot drift from the reindex program built from the same
    /// pair.
    fn kept_type_codes(&self) -> Vec<u64> {
        self.keep.iter().map(|&i| self.coldefs[i].type_code as u64).collect()
    }

    /// `P_all`: this side's *unfiltered* input re-keyed onto the join key with the
    /// same reindex program the join terms used — reusing the already-emitted
    /// reindex when the key is non-nullable (there the NULL gate was a no-op, so
    /// the gated and raw inputs are the same node).
    fn all(&self, cb: &mut CircuitBuilder, nullable: bool, reindex: gnitz_core::NodeId) -> gnitz_core::NodeId {
        if nullable {
            cb.map_reindex(
                self.input,
                self.cols,
                self.target_tcs,
                build_reindex_program_keep(self.keep),
            )
        } else {
            reindex
        }
    }
}

/// The nodes [`emit_equi_join_terms`] emits, plus each side's key-nullability
/// fact (from the shared `null_gate`, so a caller's `a_all`/`b_all` reuse
/// decision cannot drift from the gate that was actually emitted).
pub(crate) struct EquiTerms {
    pub(crate) reindex_a: gnitz_core::NodeId,
    pub(crate) reindex_b: gnitz_core::NodeId,
    pub(crate) a_nullable: bool,
    pub(crate) b_nullable: bool,
    pub(crate) join_ab: gnitz_core::NodeId,
    pub(crate) join_ba: gnitz_core::NodeId,
}

/// The leading output PK columns of a range/band join: A's then B's source-PK
/// column types (the re-key self-derive output type), non-nullable and
/// `_pair_pk_{slot}`-numbered across both sides. Hidden — the synthetic pair-PK
/// is a physical PK column, not a presentation column (`SELECT *` omits it; it is
/// not name-resolvable). One home for both range emitters.
fn pair_pk_coldefs(left_schema: &Schema, right_schema: &Schema) -> Vec<ColumnDef> {
    left_schema
        .pk_cols
        .iter()
        .map(|&c| (left_schema, c))
        .chain(right_schema.pk_cols.iter().map(|&c| (right_schema, c)))
        .enumerate()
        .map(|(slot, (schema, c))| {
            ColumnDef::new(
                format!("_pair_pk_{slot}"),
                schema.columns[c].type_code.reindex_output_type(),
                false,
            )
            .hidden()
        })
        .collect()
}

/// The shared null-fill tail of a range/band outer join: null-extend `ν_P` with
/// the O-side NULL columns, re-key onto the pair-PK `[a.pk…, b.pk…]` (the other
/// side's PK rides in the NULL-O payload, packing to the synthetic 0), and
/// project to the FULL combined width `[pair-PK, A, B]`.
///
/// `preserved_is_left` selects the canonical (P=A) vs reordered (P=B) layout:
/// `null_extend` appends and `map_reindex` locks its payload to input order, so
/// only the final `cb.map` can place the NULL-O columns before P (the P=B case).
/// All the geometry derives from the two schemas, so both range emitters call
/// this with nothing else to keep in sync.
fn emit_range_null_fill_tail(
    cb: &mut CircuitBuilder,
    left_schema: &Schema,
    right_schema: &Schema,
    nf_keyed: gnitz_core::NodeId,
    preserved_is_left: bool,
) -> gnitz_core::NodeId {
    let (left_n, right_n) = (left_schema.columns.len(), right_schema.columns.len());
    let (pa, pb) = (left_schema.pk_cols.len(), right_schema.pk_cols.len());
    let pair_pk = pa + pb;
    let zero_tcs = vec![0u8; pair_pk];

    let (p_pk, p_n) = if preserved_is_left { (pa, left_n) } else { (pb, right_n) };
    let o_col_tcs = schema_type_codes(if preserved_is_left {
        &right_schema.columns
    } else {
        &left_schema.columns
    });
    let nullfill = cb.null_extend(nf_keyed, &o_col_tcs); // [P.pk × p_pk, P, NULL-O]

    // Pair-PK [a.pk…, b.pk…]: preserved side's pk from the PK region (0..p_pk),
    // other side's pk from the NULL-O payload (p_pk + p_n + pk → synthetic 0).
    let mut nf_pair_pk_cols: Vec<usize> = Vec::with_capacity(pair_pk);
    if preserved_is_left {
        nf_pair_pk_cols.extend(0..pa);
        for &b_pk in &right_schema.pk_cols {
            nf_pair_pk_cols.push(p_pk + p_n + b_pk);
        }
    } else {
        for &a_pk in &left_schema.pk_cols {
            nf_pair_pk_cols.push(p_pk + p_n + a_pk);
        }
        nf_pair_pk_cols.extend(0..pb);
    }

    // Keep the [P, NULL-O] payload and DROP the leading P.pk × p_pk columns —
    // they are the null-fill's own key source, already consumed by
    // `nf_pair_pk_cols` above, and nothing downstream reads them. The payload is
    // P's columns followed by O's, in input order — no reorder, since
    // map_reindex locks the output payload to input order.
    let keep: Vec<usize> = (p_pk..p_pk + p_n + o_col_tcs.len()).collect();
    let nf_rekey = cb.map_reindex(nullfill, &nf_pair_pk_cols, &zero_tcs, build_reindex_program_keep(&keep));

    // nf_rekey output: [pair-PK, <[P, NULL-O] in input order>] — the payload sits
    // directly behind the pair-PK. Map EVERY combined column (canonical [A, B]
    // index) to its slot, canonicalizing P=B, so the branch is full width
    // `[pair-PK, A, B]` — the layout the post-union WHERE and the final user
    // projection both read.
    let nf_full_projection: Vec<usize> = (0..left_n + right_n)
        .map(|ci| {
            if preserved_is_left {
                pair_pk + ci // [A, B] contiguous after the pair-PK
            } else if ci < left_n {
                pair_pk + right_n + ci // A column → trailing NULL-A region
            } else {
                pair_pk + (ci - left_n) // B column → leading B region
            }
        })
        .collect();
    cb.map(nf_rekey, &nf_full_projection)
}

/// The inputs of the equi outer null-fill: the inner join output it subtracts
/// from, the two sides as `emit_equi_join_terms` saw them, and the terms it
/// emitted. The merged layout `[k _join_pk][kept-A][kept-B]` is derived from the
/// sides (`k` is the key arity, each width its `keep` length), so there is nothing
/// here to keep in sync with the emit above.
struct EquiNullFill<'a> {
    inner_merged: gnitz_core::NodeId,
    kind: JoinType,
    k: usize,
    left: EquiSide<'a>,
    right: EquiSide<'a>,
    terms: &'a EquiTerms,
}

/// Emit `inner ∪ ν_A ∪ ν_B` — the outer join's null-fill, unioned onto the inner
/// output for each preserved side. Returns `inner_merged` unchanged for an INNER
/// join. One home for the outer-join null-fill.
///
/// Per preserved side P, `ν_P = positive_part(P_all − π_P(inner))`: `π_P(inner)`
/// re-keys the inner output back to P's identity, carrying the matched
/// multiplicity `m = w_P·S` there (the bilinear join's consolidation sums the
/// per-match `w_P·w_o`), and `P_all` re-keys the *unfiltered* P input with the
/// same reindex program — so a matched row's `+w_P` and `−m` cancel before the
/// clamp. The join-shard scatter co-locates `P_all` and the inner output on the
/// `_join_pk` worker, so the difference is partition-local — no exchange.
/// `positive_part` subtracts the RAW matched multiplicity (weight-exact for a
/// bag-valued preserved side) and absorbs the within-epoch ΔP/Δπ_P(inner)
/// simultaneity and the cross-epoch transient.
///
/// `null_extend` is append-only ⇒ canonical `[_join_pk, A, B]` for P = A; for
/// P = B the appended NULL-A lands after B and needs the `normalize_to_ab` BA
/// reorder to become `[_join_pk, NULL-A, B]`.
///
/// `inner_merged` feeds both `π_P` and these unions, so it rides the
/// non-destructive `PORT_IN_B` operand throughout (`op_union` empties
/// `PORT_IN_A`; each null-fill is a fresh single-consumer node, as is the
/// accumulator after the first union). A non-Inner join preserves ≥ 1 side, so at
/// least one null-fill is always folded in.
fn emit_equi_null_fill(cb: &mut CircuitBuilder, nf: EquiNullFill<'_>) -> gnitz_core::NodeId {
    let EquiNullFill {
        inner_merged,
        kind,
        k,
        left,
        right,
        terms,
    } = nf;
    if kind == JoinType::Inner {
        return inner_merged;
    }
    let (pl, pr) = (left.kept_n(), right.kept_n());
    let mut merged = inner_merged;
    // Each preserved side P: `p0` locates its kept payload in the merged layout
    // (`k` for A, `k + pl` for B) and the other side supplies the NULL region.
    for (preserved_is_left, side, other, nullable, reindex, p0, p_n) in [
        (true, left, right, terms.a_nullable, terms.reindex_a, k, pl),
        (false, right, left, terms.b_nullable, terms.reindex_b, k + pl, pr),
    ] {
        if !kind.preserves(preserved_is_left) {
            continue;
        }
        let p_all = side.all(cb, nullable, reindex);
        let proj_p = cb.map(inner_merged, &(p0..p0 + p_n).collect::<Vec<_>>()); // π_P(inner) = [_join_pk, P]
        let nu_p = cb.positive_diff(p_all, proj_p); // max(0, P − π_P(inner))
        let ext = cb.null_extend(nu_p, &other.kept_type_codes());
        let branch = if preserved_is_left {
            ext // [_join_pk, A, NULL-B] — canonical
        } else {
            // The appended NULL-A (pl cols) lands after B (pr cols); reorder it to
            // the canonical `[_join_pk, NULL-A, B]`.
            let reorder: Vec<usize> = (k + pr..k + pr + pl).chain(k..k + pr).collect();
            cb.map(ext, &reorder)
        };
        merged = cb.union(branch, merged);
    }
    merged
}

/// `π_P(inner)` for a band join: re-key the inner output onto the preserved side's
/// source PK, then project that side's payload back out. The two operands of
/// `ν_P = positive_part(P_all − π_P(inner))` must be keyed byte-identically or the
/// clamp silently mis-weights, so both range outer null-fills and the band EXISTS/IN
/// semi-join build their π here.
///
/// `pk_slots` are the preserved side's PK positions in the pre-rekey
/// `[_join_pk × k, A, B]` layout; the re-key prepends them, making the layout
/// `[P.pk × p, _join_pk × k, A, B]`, so the side's payload starts at
/// `p + k + payload_off` (`0` for the left side, `left_n` for the right).
pub(crate) fn band_pi_preserved(
    cb: &mut CircuitBuilder,
    merged: NodeId,
    union_n: usize,
    pk_slots: &[usize],
    k: usize,
    payload_off: usize,
    payload_n: usize,
) -> NodeId {
    let p = pk_slots.len();
    let rekey = cb.map_reindex(merged, pk_slots, &vec![0u8; p], build_reindex_program(union_n));
    let base = p + k + payload_off;
    cb.map(rekey, &(base..base + payload_n).collect::<Vec<_>>())
}

/// The pure-range unmatched set `A − matched`, union the NULL-range-key rows a
/// threshold comparison can never match. The pure-range LEFT JOIN null-fill and the
/// NOT EXISTS / mark-unmatched branch are the same node sequence, so they share it.
pub(crate) fn pure_range_unmatched(
    cb: &mut CircuitBuilder,
    thr: &PureRangeThreshold,
    key_nullable: bool,
    raw: NodeId,
    reindex_cols: &[usize],
    schema: &Schema,
) -> Result<NodeId, GnitzSqlError> {
    let a_pass = thr.a_pass.expect("a_pass requested for the unmatched branch");
    let neg = cb.negate(thr.matched);
    let nf_match = cb.union(a_pass, neg);
    if key_nullable {
        union_null_key_rows(cb, nf_match, raw, reindex_cols, schema)
    } else {
        Ok(nf_match)
    }
}

/// Emit the symmetric 2-term equi join over two NULL-gated, reindexed sides —
/// B-first interleaved gates/reindexes, then both traces, then
/// `join_ab`/`join_ba` — the sequence shared by the join lowering and the equi
/// EXISTS/IN builder. A NULL equi-join key must match nothing
/// (SQL 3VL: NULL = anything, including NULL = NULL, is unknown); `map_reindex`
/// would promote a NULL integer key to synthetic PK 0 and a NULL string to the
/// empty-content hash 0, colliding with a real 0/"" key and with every other
/// NULL — so `null_gate` drops NULL-keyed rows from the match on both sides
/// (and leaves a NOT NULL side untouched, zero overhead).
pub(crate) fn emit_equi_join_terms(
    cb: &mut CircuitBuilder,
    a: EquiSide<'_>,
    b: EquiSide<'_>,
) -> Result<EquiTerms, GnitzSqlError> {
    let (b_gated, b_nullable) = null_gate(cb, b.input, b.cols, b.coldefs)?;
    let reindex_b = cb.map_reindex(b_gated, b.cols, b.target_tcs, build_reindex_program_keep(b.keep));
    let (a_gated, a_nullable) = null_gate(cb, a.input, a.cols, a.coldefs)?;
    let reindex_a = cb.map_reindex(a_gated, a.cols, a.target_tcs, build_reindex_program_keep(a.keep));
    let trace_a = cb.integrate_trace(reindex_a);
    let trace_b = cb.integrate_trace(reindex_b);
    let join_ab = cb.join_with_trace_node(reindex_a, trace_b); // ΔA ⋈ z^{-1}(I(B))
    let join_ba = cb.join_with_trace_node(reindex_b, trace_a); // ΔB ⋈ z^{-1}(I(A))
    Ok(EquiTerms {
        reindex_a,
        reindex_b,
        a_nullable,
        b_nullable,
        join_ab,
        join_ba,
    })
}

/// What both range builders carry out of [`range_prologue`]: the two reindexed
/// sides, the LEFT key's nullability (the NULL-key branch's gate), the left key
/// slots (re-read by that branch), the per-slot common types, and the §3 term
/// relations (`rel_ab = converse(op)` for term AB, `rel_ba = op` for term BA).
pub(crate) struct RangePrologue {
    pub(crate) reindex_a: NodeId,
    pub(crate) reindex_b: NodeId,
    pub(crate) left_key_nullable: bool,
    pub(crate) left_reindex_cols: Vec<usize>,
    pub(crate) all_tcs: Vec<TypeCode>,
    pub(crate) rel_ab: RangeRel,
    pub(crate) rel_ba: RangeRel,
}

/// The prologue every range/band builder opens with, in one place: resolve the eq
/// pairs and the range conjunct to each side's key slots (`[eq slots…, range slot]`
/// — the reindex-column order the whole range geometry is stated against), NULL-gate
/// both sides over ALL of them (SQL 3VL), and reindex each onto its key. The callers
/// keep their unfiltered inputs for the preserved side's null-fill; partition
/// filters, traces, and join terms stay with them (those diverge per shape).
pub(crate) fn range_prologue(
    cb: &mut CircuitBuilder,
    left_in: &SegInput,
    right_in: &SegInput,
    a_input: NodeId,
    b_input: NodeId,
    eq: &[EqPair],
    range: &HirRange,
) -> Result<RangePrologue, GnitzSqlError> {
    let EquiKeys {
        left: left_cols,
        right: right_cols,
        tcs: eq_tcs,
    } = resolve_eq_cols(eq, left_in, right_in)?;
    let (left_coldefs, right_coldefs) = (&left_in.schema.columns, &right_in.schema.columns);

    let left_reindex_cols: Vec<usize> = left_cols
        .into_iter()
        .chain(std::iter::once(slot_of(&left_in.layout, range.left)?))
        .collect();
    let right_reindex_cols: Vec<usize> = right_cols
        .into_iter()
        .chain(std::iter::once(slot_of(&right_in.layout, range.right)?))
        .collect();
    let all_tcs: Vec<TypeCode> = eq_tcs.into_iter().chain(std::iter::once(range.tc)).collect();
    let left_target_tcs = side_target_tcs(&left_reindex_cols, left_coldefs, &all_tcs);
    let right_target_tcs = side_target_tcs(&right_reindex_cols, right_coldefs, &all_tcs);

    let (a_gated, left_key_nullable) = null_gate(cb, a_input, &left_reindex_cols, left_coldefs)?;
    let (b_gated, _) = null_gate(cb, b_input, &right_reindex_cols, right_coldefs)?;
    let reindex_a = cb.map_reindex(
        a_gated,
        &left_reindex_cols,
        &left_target_tcs,
        build_reindex_program(left_coldefs.len()),
    );
    let reindex_b = cb.map_reindex(
        b_gated,
        &right_reindex_cols,
        &right_target_tcs,
        build_reindex_program(right_coldefs.len()),
    );
    Ok(RangePrologue {
        reindex_a,
        reindex_b,
        left_key_nullable,
        left_reindex_cols,
        all_tcs,
        rel_ab: converse_rel(range.op),
        rel_ba: range.op,
    })
}

/// The pure-range threshold pipeline's outputs: `matched = A ⋈ {m}` and (when
/// requested) the bare `int_a` passthrough, both re-keyed onto `[a.pk…, A]`.
pub(crate) struct PureRangeThreshold {
    pub(crate) matched: gnitz_core::NodeId,
    pub(crate) a_pass: Option<gnitz_core::NodeId>,
}

/// Build the inline pure-range (`n_eq == 0`) threshold pipeline: the one-row
/// `m = MAX/MIN(b.range_col)` reduce and its trace, the two matched terms
/// (`Δint_a ⋈ trace_m`, `Δreindex_m ⋈ trace_a`) with their A-side projections,
/// and the shared `[a.pk…, A]` re-key applied to the matched union and (when
/// `want_a_pass`) the `int_a` passthrough. Shared by the pure-range LEFT JOIN
/// null-fill (`A − matched`, `want_a_pass = true`) and the pure-range EXISTS
/// builder (EXISTS = `matched` alone; NOT EXISTS = `A − matched`). Node
/// allocation order is load-bearing: the LEFT JOIN keeps compiling
/// byte-identically through this extraction.
///
/// `m` is computed LOCALLY on every worker over the broadcast `reindex_b` (NOT
/// a partition-filtered slice): a global extremum over a fully-broadcast input
/// is identical on every worker, so no scatter/gather is needed. `map_hash_row`
/// moves `reindex_b`'s `Tc` PK into a payload column, DECODING OPK → native AND
/// carrying the `Tc` type verbatim, so the reduce aggregates the native value
/// (col 1), not the OPK bytes (which would invert signed MIN/MAX), and — because
/// non-float MIN/MAX carries its source type (`agg_output_type`) — emits its
/// result already typed `Tc`; `reindex_m` self-derives the correct OPK order
/// straight off the reduce. `global_ground = false`: a ground row would seed
/// `(m=NULL)` into the threshold trace and break the `A − ∅ = A` subtraction
/// that needs the trace empty over an empty other side.
///
/// The two matched terms carry the MATCH op (= the inner op), mirroring
/// `join_ab`/`join_ba` EXACTLY (`rel_ab = converse(op)`, `rel_ba = op`) with
/// only the trace swapped to `trace_m`/`reindex_m`. The `a`-side term taps the
/// filtered `int_a` against the replicated `trace_m`; the `m`-side term taps
/// the replicated `reindex_m` against the filtered `trace_a`, so neither
/// duplicates under broadcast. All range nodes carry n_eq == 0 — load-bearing
/// for the view-level `circuit_range_join_n_eq` discriminator, which reads an
/// ARBITRARY `DeltaTraceRange.n_eq` and is correct only because they all agree.
///
/// Both `matched_raw` and the `int_a` passthrough are `[Tc PK, A]` (a.pk lives
/// INSIDE A, not in the PK). Re-keying BOTH onto `[a.pk…, A]` with one shared
/// reindex program makes a matched `a`'s `+a` (passthrough) and `−a` (matched,
/// negated) byte-identical so they cancel in-epoch. The passthrough re-keys
/// `int_a` (the broadcast ΔA minus its non-owned / NULL-key rows), NOT the raw
/// input: a pure-range relay broadcasts `a`, so re-keying the raw input would
/// emit `W×` copies the output shard would sum.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_pure_range_threshold(
    cb: &mut CircuitBuilder,
    left_schema: &Schema,
    range_tc: TypeCode,
    range_op: RangeRel,
    reindex_b: gnitz_core::NodeId,
    int_a: gnitz_core::NodeId,
    trace_a: gnitz_core::NodeId,
    want_a_pass: bool,
) -> PureRangeThreshold {
    let left_n = left_schema.columns.len();
    let pa = left_schema.pk_cols.len();
    let k = 1usize; // pure range: no eq prefix, one range slot
    let zero_a = vec![0u8; pa];
    let rel_ab = converse_rel(range_op);
    let rel_ba = range_op;
    let want_max = matches!(range_op, RangeRel::Lt | RangeRel::Le);
    let agg_func = if want_max { AGG_MAX } else { AGG_MIN };
    let m_schema = Schema {
        columns: pure_range_m_output_cols(range_tc),
        pk_cols: vec![0],
    };

    let mbh = cb.map_hash_row(reindex_b, &[0], &[], 0);
    // Empty group set (one global threshold over the broadcast other side) → the
    // synthetic `_group_pk` fold, like every other empty-group reduce.
    let red = cb.reduce_multi_local(mbh, &[], &[(agg_func, 1)], false, ReduceOutKey::SyntheticFold); // [_group_pk:U128, m:Tc]
    let carried_m = range_tc.carried_reindex_tc(range_tc);
    let reindex_m = cb.map_reindex(red, &[1], &[carried_m], build_reindex_program(m_schema.columns.len()));
    let trace_m = cb.integrate_trace(reindex_m);

    let j_am = cb.join_with_trace_range_node(int_a, trace_m, 0, rel_ab);
    let j_ma = cb.join_with_trace_range_node(reindex_m, trace_a, 0, rel_ba);
    // Range-join output = [_join_pk × k, delta payload, trace payload]; project
    // each to the A columns. `j_am`: A is the delta payload at `k..k+left_n`.
    // `j_ma`: A is the trace payload, after Δm's payload (`m`'s 2 columns).
    let m_payload = m_schema.columns.len(); // m's output arity ([_group_pk, m])
    let m_am = cb.map(j_am, &(k..k + left_n).collect::<Vec<_>>());
    let m_ma = cb.map(j_ma, &(k + m_payload..k + m_payload + left_n).collect::<Vec<_>>());
    let matched_raw = cb.union(m_am, m_ma); // [_join_pk(PK), A]

    let jp = ColumnDef::new("_jp", range_tc, false);
    let nf_raw_schema = Schema {
        columns: std::iter::once(jp).chain(left_schema.columns.iter().cloned()).collect(),
        pk_cols: vec![0],
    };
    let a_pk_in_raw: Vec<usize> = left_schema.pk_cols.iter().map(|&p| 1 + p).collect();
    let a_cols: Vec<usize> = (pa + 1..pa + 1 + left_n).collect();
    // One shared reindex program drives both re-keys, so the IDENTICAL encoding
    // is structural — `matched` and `a_pass` differ only in their input node.
    let nf_reindex_prog = build_reindex_program(nf_raw_schema.columns.len());
    let rekey_a = |cb: &mut CircuitBuilder, input: gnitz_core::NodeId| {
        let keyed = cb.map_reindex(input, &a_pk_in_raw, &zero_a, nf_reindex_prog.clone());
        cb.map(keyed, &a_cols) // [a.pk…, A]
    };
    let matched = rekey_a(cb, matched_raw);
    let a_pass = want_a_pass.then(|| rekey_a(cb, int_a));
    PureRangeThreshold { matched, a_pass }
}

/// The k synthetic `_join_pk` output PK columns of an equi join / equi EXISTS
/// view, hidden (a physical PK column, not a presentation column — `SELECT *`
/// omits it and it is not name-resolvable). Each slot carries its pair's common
/// type `T_i` — the single persisted stride both sides' reindex Maps and every
/// cross-process consumer re-derive. The first column keeps the name
/// `_join_pk` at k = 1 (preserving the shippable single-key catalog name);
/// composite keys use `_join_pk_{i}`.
pub(crate) fn join_pk_coldefs(target_tcs: &[TypeCode]) -> Vec<ColumnDef> {
    let k = target_tcs.len();
    target_tcs
        .iter()
        .enumerate()
        .map(|(i, &t)| {
            let name = if k == 1 {
                "_join_pk".to_string()
            } else {
                format!("_join_pk_{i}")
            };
            ColumnDef::new(name, t, false).hidden()
        })
        .collect()
}

/// The band re-key's union-layout schema `[_join_pk × k, A cols, B cols]`
/// (`k = all_tcs.len()` synthetic slots as the PK region, both sides' columns
/// verbatim behind them) — the schema the band builders compile their re-key
/// reindex programs and residual filters against.
pub(crate) fn band_union_schema(all_tcs: &[TypeCode], left: &Schema, right: &Schema) -> Schema {
    let k = all_tcs.len();
    let mut cols: Vec<ColumnDef> = Vec::with_capacity(k + left.columns.len() + right.columns.len());
    for (i, &t) in all_tcs.iter().enumerate() {
        cols.push(ColumnDef::new(format!("_join_pk_{i}"), t, false));
    }
    cols.extend(left.columns.iter().cloned());
    cols.extend(right.columns.iter().cloned());
    Schema {
        columns: cols,
        pk_cols: (0..k).collect(),
    }
}

/// Union the pure-range NULL-range-key rows into `nf_match` (`A − matched`):
/// NULL-key rows never reach the integrated trace (3VL) and never match the
/// threshold, so they get their own branch off the NULL-gate-unfiltered
/// `source`, re-keyed to the preserved side's source PK and routed ONCE by a
/// local `partition_filter` (no exchange) — broadcast would emit W× copies the
/// output shard would sum. (The compiler makes the filter a keep-all identity
/// for an all-replicated view, which runs correct-local over the full broadcast
/// on every worker.) `source` is the caller's semantic preserved input (the raw
/// input for the LEFT join; the locally pre-filtered outer for EXISTS).
fn union_null_key_rows(
    cb: &mut CircuitBuilder,
    nf_match: gnitz_core::NodeId,
    source: gnitz_core::NodeId,
    cols: &[usize],
    schema: &Schema,
) -> Result<gnitz_core::NodeId, GnitzSqlError> {
    let anull = cb.filter(source, Some(multi_null_filter_prog(cols, &schema.columns, true)?));
    let anull_keyed = rekey_on_source_pk(cb, anull, schema);
    let anull_owned = cb.partition_filter(anull_keyed);
    Ok(cb.union(nf_match, anull_owned))
}
