//! The set-operation and DISTINCT emission shell. Each side is content-hashed to
//! a synthetic PK (reusing `hash_shard_side`) and combined with the retained
//! join-free `union`/`negate`/`positive_diff`/`distinct` arithmetic
//! (`set_op_leaves`). A pure pass-through plain side reads its base relation
//! directly (its source columns hash by value); every other side — a computed
//! projection, a combine — is cut to a hidden segment by the shared rule, so the
//! set identity is materialized before it is hashed. The shared source-collision
//! rule then wraps a repeated relation (`t EXCEPT t`) in a pass-through segment.

use super::super::{slot_of, ColId, HirCol, HirExpr, ProjEntry, RelExpr, SetOpKind};
use super::{cut_segment, emit_filter, resolve_collisions, seginput_of_get, CutMemo, SegInput};
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::hir::physical;
use crate::ir::{BExpr, BoundExpr};
use crate::validate::{reject_duplicate_column_names, reject_float_key};
use gnitz_core::{CircuitBuilder, ColumnDef, GnitzClient, NodeId, TypeCode};
use std::collections::HashSet;
use std::rc::Rc;

/// Lower a `SetOp` body to circuit pieces + output `ColId` layout.
pub(crate) fn lower_setop(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
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

    // Promotion targets, stamped per pair by `RelExpr::set_op`.
    let left_tt: Vec<u8> = out.iter().map(|c| c.left_target).collect();
    let right_tt: Vec<u8> = out.iter().map(|c| c.right_target).collect();

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
    hashed_out(cb, "_set_pk", out.iter().map(|c| &c.out), "set operation view")
}

/// Lower a `SELECT DISTINCT` body (`Distinct(Project(...))`) — dedup over the
/// projected content via the synthetic hash key.
pub(crate) fn lower_distinct(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
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
    let (seg, kind) = resolve_set_input(client, chain, memo, input, &side_ids)?;
    let (node, slots) = emit_side(&mut cb, &seg, &kind, &side_ids)?;
    let sharded = hash_shard_side(&mut cb, node, &slots, &[], 0);
    let distinct_node = cb.distinct(sharded);
    cb.sink(distinct_node);
    hashed_out(cb, "_distinct_pk", side_cols.iter(), "SELECT DISTINCT view")
}

/// Finish a content-hashed body: build the circuit and pair the synthetic hidden
/// U128 hash PK with the body's output columns, returning the pieces plus the
/// output `ColId` layout. One home for the `_set_pk` / `_distinct_pk` convention.
fn hashed_out<'a>(
    cb: CircuitBuilder,
    pk_name: &str,
    cols: impl Iterator<Item = &'a HirCol>,
    dup_ctx: &str,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let circuit = cb.build();
    let (mut out_cols, mut layout) = (
        vec![ColumnDef::new(pk_name, TypeCode::U128, false).hidden()],
        vec![ColId::NONE],
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
fn lower_sides(
    cb: &mut CircuitBuilder,
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    sides: [&Rc<RelExpr>; 2],
    side_ids: &[Vec<ColId>; 2],
    exempt: bool,
) -> Result<(NodeId, Vec<usize>, NodeId, Vec<usize>), GnitzSqlError> {
    let (l_seg, l_kind) = resolve_set_input(client, chain, memo, sides[0], &side_ids[0])?;
    let (r_seg, r_kind) = resolve_set_input(client, chain, memo, sides[1], &side_ids[1])?;
    // The collision rule may re-point a side at a pass-through wrapper segment; the
    // wrapper's layout still carries the source ids, so the addressing kind holds.
    let mut inputs = [l_seg, r_seg];
    resolve_collisions(client, chain, &mut inputs, &sides, exempt)?;
    let [l_seg, r_seg] = inputs;
    let (l_node, l_slots) = emit_side(cb, &l_seg, &l_kind, &side_ids[0])?;
    let (r_node, r_slots) = emit_side(cb, &r_seg, &r_kind, &side_ids[1])?;
    Ok((l_node, l_slots, r_node, r_slots))
}

/// How a resolved set-op side addresses its set-identity columns — **not** the
/// same `ColId` space for the two shapes, so the discrimination is made once at
/// resolve time and carried, rather than re-derived (and re-validated) at emit.
enum SetSideKind<'a> {
    /// A pure pass-through plain side, read straight off its base relation: the
    /// layout carries the *source* ids, so a slot comes from resolving the
    /// projection item's own expression — and the side's WHERE is still to be
    /// inlined at emit (the segment shape already materialized its own).
    PassThrough {
        items: &'a [ProjEntry],
        where_preds: &'a [HirExpr],
    },
    /// Every other shape — a computed projection, a combine — cut to a hidden
    /// segment so the set identity is materialized before it is hashed. Its layout
    /// carries the projection's *output* ids, so a slot is `slot_of(out_id)`.
    Segment,
}

/// Resolve one set-op side to its delta source plus the addressing kind, making
/// the pass-through-vs-segment decision once.
fn resolve_set_input<'a>(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    side: &'a Rc<RelExpr>,
    side_ids: &[ColId],
) -> Result<(SegInput, SetSideKind<'a>), GnitzSqlError> {
    if let Some((get, items, where_preds)) = passthrough_parts(side) {
        // A pass-through side's base is a `Get`, so it is read in place and can
        // never be cut — no live set is consulted.
        let seg = seginput_of_get(get).expect("a pass-through side's base is a Get");
        return Ok((seg, SetSideKind::PassThrough { items, where_preds }));
    }
    let live: HashSet<ColId> = side_ids.iter().copied().collect();
    Ok((cut_segment(client, chain, memo, side, &live)?, SetSideKind::Segment))
}

/// A pure pass-through side (`Project(Filter?(Get))` whose every item is a bare
/// column ref), split into its base `Get`, projection items, and WHERE — else
/// `None`, since a computed item has no set identity until it is materialized.
/// Returning the parts (rather than just the `Get`) is what lets the emit below
/// consume the shape without re-matching it.
fn passthrough_parts(side: &Rc<RelExpr>) -> Option<(&Rc<RelExpr>, &[ProjEntry], &[HirExpr])> {
    let RelExpr::Project { input, items } = side.as_ref() else {
        return None;
    };
    if !items.iter().all(|e| matches!(&e.expr, BExpr::ColRef(_))) {
        return None;
    }
    let (where_preds, inner) = super::split_filter(input);
    matches!(inner.as_ref(), RelExpr::Get { .. }).then_some((inner, items, where_preds))
}

/// Emit one resolved side's delta input and return it with the slots
/// `hash_shard_side` projects, in set-op column order.
fn emit_side(
    cb: &mut CircuitBuilder,
    seg: &SegInput,
    kind: &SetSideKind<'_>,
    side_ids: &[ColId],
) -> Result<(NodeId, Vec<usize>), GnitzSqlError> {
    let inp = cb.input_delta_tagged(seg.tid);
    match kind {
        SetSideKind::Segment => {
            let slots = side_ids
                .iter()
                .map(|id| slot_of(&seg.layout, *id))
                .collect::<Result<_, _>>()?;
            Ok((inp, slots))
        }
        SetSideKind::PassThrough { items, where_preds } => {
            let node = emit_filter(cb, inp, *where_preds, &seg.layout, &seg.schema.columns)?;
            // Every item is a bare column ref — that is what makes the side
            // pass-through — so each resolves to exactly one source slot.
            let slots = items
                .iter()
                .map(|e| match physical::resolve_refs(&e.expr, &seg.layout)? {
                    BoundExpr::ColRef(s) => Ok(s),
                    _ => Err(GnitzSqlError::Internal(
                        "pass-through side item is not a column ref".into(),
                    )),
                })
                .collect::<Result<_, _>>()?;
            Ok((node, slots))
        }
    }
}

/// Hash the projected columns to a synthetic content PK — widening
/// each column whose `target_tcs` entry is non-zero into the promoted layout so
/// both set-op sides share one physical representation — then shard by that PK.
fn hash_shard_side(
    cb: &mut CircuitBuilder,
    filtered: gnitz_core::NodeId,
    proj_indices: &[usize],
    target_tcs: &[u8],
    branch_id: u8,
) -> gnitz_core::NodeId {
    // Reindex by a hash of the projected columns, so set membership
    // (EXCEPT/INTERSECT/UNION-distinct) is decided by the projected row content,
    // not by the source table's PK: two rows from different tables sharing a PK
    // but differing in payload must not match.
    let reindexed = cb.map_hash_row(filtered, proj_indices, target_tcs, branch_id);
    // Repartition by the synthetic hash PK (column 0) so that under
    // multiple workers each row lands on the worker that owns its new PK's
    // shard, co-locating matching rows for the downstream set arithmetic and
    // placing each output row on its owning worker for the sink/scan. The hash
    // is computed in-circuit, so the master cannot pre-shard the source by it;
    // this in-circuit exchange is mandatory. Single-worker mode elides the IPC.
    cb.shard(reindexed, &[0])
}

/// The two leaf nodes feeding an INTERSECT/EXCEPT weight-clamp arm. The distinct
/// form clamps each side to {0,1} via `distinct` so the arithmetic is set-valued;
/// `all` keeps the raw per-row bag counts.
fn set_op_leaves(
    cb: &mut CircuitBuilder,
    all: bool,
    left: gnitz_core::NodeId,
    right: gnitz_core::NodeId,
) -> (gnitz_core::NodeId, gnitz_core::NodeId) {
    if all {
        (left, right)
    } else {
        (cb.distinct(left), cb.distinct(right))
    }
}
