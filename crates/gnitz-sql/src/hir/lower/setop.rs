//! The set-operation and DISTINCT emission shell. Each side is content-hashed to
//! a synthetic PK (reusing `hash_shard_side`) and combined with the retained
//! join-free `union`/`negate`/`positive_diff`/`distinct` arithmetic
//! (`set_op_leaves`). A pure pass-through plain side reads its base relation
//! directly (its source columns hash by value); every other side — a computed
//! projection, a combine — is cut to a hidden segment by the shared rule, so the
//! set identity is materialized before it is hashed. The shared source-collision
//! rule then wraps a repeated relation (`t EXCEPT t`) in a pass-through segment.

use super::super::{slot_of, ColId, ColIdGen, HirCol, RelExpr, SetOpKind};
use super::{cut_segment, emit_filter, resolve_collisions, resolve_input, CutMemo, SegInput};
use crate::error::GnitzSqlError;
use crate::hir::physical;
use crate::ir::{BExpr, BoundExpr};
use crate::plan::validate::{reject_duplicate_column_names, reject_float_key};
use crate::plan::view::set_op::{hash_shard_side, set_op_leaves};
use crate::plan::view::{EmitPieces, ViewChain};
use gnitz_core::{CircuitBuilder, ColumnDef, GnitzClient, NodeId, TypeCode};
use std::collections::HashSet;
use std::rc::Rc;

/// Lower a `SetOp` body to circuit pieces + output `ColId` layout.
pub(crate) fn lower_setop(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    ids: &mut ColIdGen,
    memo: &mut CutMemo,
    setop: &RelExpr,
    view_id: u64,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let RelExpr::SetOp {
        op,
        all,
        left,
        right,
        out,
    } = setop
    else {
        unreachable!("lower_setop receives a SetOp");
    };

    // A float set-identity column breaks content-hash equality (IEEE-754).
    for c in out {
        reject_float_key(&c.out.def, "set operation")?;
    }

    // Promotion targets: `0` keeps the side's own type, else the promoted common type.
    let side_targets = |scols: &[HirCol]| -> Vec<u8> {
        out.iter()
            .zip(scols)
            .map(|(o, s)| {
                if s.def.type_code == o.out.def.type_code {
                    0
                } else {
                    o.out.def.type_code as u8
                }
            })
            .collect()
    };
    let left_tt = side_targets(&left.cols());
    let right_tt = side_targets(&right.cols());

    // UNION ALL keeps both copies of an identical row, so the right side hashes on
    // a distinct branch id; every deduplicating op uses branch 0.
    let right_branch_id = matches!((op, all), (SetOpKind::Union, true)) as u8;

    let mut cb = CircuitBuilder::new(view_id, 0);
    let side_ids: [Vec<ColId>; 2] = [
        out.iter().map(|c| c.left).collect(),
        out.iter().map(|c| c.right).collect(),
    ];
    let (l_node, l_slots, r_node, r_slots) = lower_sides(
        &mut cb,
        client,
        chain,
        ids,
        memo,
        [left, right],
        &side_ids,
        // UNION / UNION ALL are exempt from the collision rule: they are linear
        // merges the dag drives by cloning one epoch's delta to both sides.
        matches!(op, SetOpKind::Union),
    )?;

    let left_node = hash_shard_side(&mut cb, l_node, &l_slots, &left_tt, 0);
    let right_node = hash_shard_side(&mut cb, r_node, &r_slots, &right_tt, right_branch_id);

    let out_node = match op {
        SetOpKind::Union if *all => cb.union(left_node, right_node),
        SetOpKind::Union => {
            let merged = cb.union(left_node, right_node);
            cb.distinct(merged)
        }
        SetOpKind::Intersect => {
            // INTERSECT = min(a, b) = a − positive_part(a − b).
            let (a, b) = set_op_leaves(&mut cb, *all, left_node, right_node);
            let pos = cb.positive_diff(a, b);
            let neg = cb.negate(pos);
            cb.union(neg, a)
        }
        SetOpKind::Except => {
            // EXCEPT = positive_part(a − b).
            let (a, b) = set_op_leaves(&mut cb, *all, left_node, right_node);
            cb.positive_diff(a, b)
        }
    };
    cb.sink(out_node);
    hashed_out(cb, ids, "_set_pk", out.iter().map(|c| &c.out), "set operation view")
}

/// Lower a `SELECT DISTINCT` body (`Distinct(Project(...))`) — dedup over the
/// projected content via the synthetic hash key.
pub(crate) fn lower_distinct(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    ids: &mut ColIdGen,
    memo: &mut CutMemo,
    input: &Rc<RelExpr>,
    view_id: u64,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let side_cols = input.cols();
    // A float set-identity column breaks content-hash equality (IEEE-754).
    for c in &side_cols {
        reject_float_key(&c.def, "SELECT DISTINCT")?;
    }
    let side_ids: Vec<ColId> = side_cols.iter().map(|c| c.id).collect();
    let mut cb = CircuitBuilder::new(view_id, 0);
    let (node, slots) = lower_set_side(&mut cb, client, chain, ids, memo, input, &side_ids)?;
    let sharded = hash_shard_side(&mut cb, node, &slots, &[], 0);
    let distinct_node = cb.distinct(sharded);
    cb.sink(distinct_node);
    hashed_out(cb, ids, "_distinct_pk", side_cols.iter(), "SELECT DISTINCT view")
}

/// Finish a content-hashed body: build the circuit and pair the synthetic hidden
/// U128 hash PK with the body's output columns, returning the pieces plus the
/// output `ColId` layout. One home for the `_set_pk` / `_distinct_pk` convention.
fn hashed_out<'a>(
    cb: CircuitBuilder,
    ids: &mut ColIdGen,
    pk_name: &str,
    cols: impl Iterator<Item = &'a HirCol>,
    dup_ctx: &str,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let circuit = cb.build();
    let (mut out_cols, mut layout) = (
        vec![ColumnDef::new(pk_name, TypeCode::U128, false).hidden()],
        vec![ids.next()],
    );
    for c in cols {
        out_cols.push(c.def.clone());
        layout.push(c.id);
    }
    reject_duplicate_column_names(&out_cols, dup_ctx)?;
    Ok(((circuit, out_cols, vec![0]), layout))
}

/// Lower both sides of a set operation, then apply the source-collision rule over
/// their **resolved** tids — so a side that already became its own segment is
/// correctly seen as distinct and never wrapped redundantly.
#[allow(clippy::too_many_arguments)]
fn lower_sides(
    cb: &mut CircuitBuilder,
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    ids: &mut ColIdGen,
    memo: &mut CutMemo,
    sides: [&Rc<RelExpr>; 2],
    side_ids: &[Vec<ColId>; 2],
    exempt: bool,
) -> Result<(NodeId, Vec<usize>, NodeId, Vec<usize>), GnitzSqlError> {
    let mut inputs = [
        resolve_set_input(client, chain, ids, memo, sides[0], &side_ids[0])?,
        resolve_set_input(client, chain, ids, memo, sides[1], &side_ids[1])?,
    ];
    resolve_collisions(client, chain, ids, &mut inputs, &sides, exempt)?;
    let [l, r] = inputs;
    let (l_node, l_slots) = emit_side(cb, &l, sides[0], &side_ids[0])?;
    let (r_node, r_slots) = emit_side(cb, &r, sides[1], &side_ids[1])?;
    Ok((l_node, l_slots, r_node, r_slots))
}

/// Resolve one set-op side to a `SegInput`. A pure pass-through plain side reads
/// its base relation directly (its source columns hash by value); every other
/// shape — a computed projection, a combine — is cut to a hidden segment through
/// the shared rule, so the set identity is materialized before it is hashed.
fn resolve_set_input(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    ids: &mut ColIdGen,
    memo: &mut CutMemo,
    side: &Rc<RelExpr>,
    side_ids: &[ColId],
) -> Result<SegInput, GnitzSqlError> {
    if let Some(get) = passthrough_get(side) {
        return resolve_input(client, chain, ids, memo, get, &side_ids.iter().copied().collect());
    }
    let live: HashSet<ColId> = side_ids.iter().copied().collect();
    cut_segment(client, chain, ids, memo, side, &live)
}

/// The base `Get` of a pure pass-through side (`Project(Filter?(Get))` whose every
/// item is a bare column ref), else `None` — a computed item has no set identity
/// until it is materialized, so it must go through a segment.
fn passthrough_get(side: &Rc<RelExpr>) -> Option<&Rc<RelExpr>> {
    let RelExpr::Project { input, items } = side.as_ref() else {
        return None;
    };
    if !items.iter().all(|e| matches!(&e.expr, BExpr::ColRef(_))) {
        return None;
    }
    let inner = match input.as_ref() {
        RelExpr::Filter { input, .. } => input,
        _ => input,
    };
    matches!(inner.as_ref(), RelExpr::Get { .. }).then_some(inner)
}

/// Emit one resolved side's delta input and return it with the slots
/// `hash_shard_side` projects, in set-op column order.
///
/// The two side kinds resolve their slots against different `ColId` spaces, which
/// is why this is one function rather than a shared `slot_of` loop. A **segment**
/// side's layout carries the projection's *output* ids, so a slot is
/// `slot_of(out_id)`. A **pass-through** side reads the base relation directly, so
/// its layout carries the *source* ids and a slot comes from resolving the
/// projection item's expression — which is also where the side's own WHERE is
/// inlined (the segment path already materialized it).
fn emit_side(
    cb: &mut CircuitBuilder,
    seg: &SegInput,
    side: &Rc<RelExpr>,
    side_ids: &[ColId],
) -> Result<(NodeId, Vec<usize>), GnitzSqlError> {
    let inp = cb.input_delta_tagged(seg.tid);
    let Some(_) = passthrough_get(side) else {
        let slots = side_ids
            .iter()
            .map(|id| slot_of(&seg.layout, *id))
            .collect::<Result<_, _>>()?;
        return Ok((inp, slots));
    };
    let RelExpr::Project { input, items } = side.as_ref() else {
        unreachable!("a pass-through side is a Project");
    };
    let (where_preds, _) = super::split_filter(input);
    let node = emit_filter(cb, inp, where_preds, &seg.layout, &seg.schema)?;
    // Every item is a bare column ref (that is what makes the side pass-through),
    // so each resolves to one source slot.
    let slots = items
        .iter()
        .map(|e| match physical::resolve_refs(&e.expr, &seg.layout)? {
            BoundExpr::ColRef(s) => Ok(s),
            _ => Err(GnitzSqlError::Plan(
                "internal: pass-through side item is not a column ref".into(),
            )),
        })
        .collect::<Result<_, _>>()?;
    Ok((node, slots))
}

/// Lower a DISTINCT input to a `(node, slots)` pair — the single-side form of
/// [`lower_sides`] (DISTINCT has no second side and so no collision to resolve).
fn lower_set_side(
    cb: &mut CircuitBuilder,
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    ids: &mut ColIdGen,
    memo: &mut CutMemo,
    side: &Rc<RelExpr>,
    side_ids: &[ColId],
) -> Result<(NodeId, Vec<usize>), GnitzSqlError> {
    let seg = resolve_set_input(client, chain, ids, memo, side, side_ids)?;
    emit_side(cb, &seg, side, side_ids)
}
