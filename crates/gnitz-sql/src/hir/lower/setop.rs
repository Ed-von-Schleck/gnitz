//! The set-operation and DISTINCT shell: each side, opened through the spine, is
//! content-hashed to a synthetic PK and combined by weight arithmetic.

use super::super::{slots_of, ColId, HirCol, RelExpr, SetOpKind};
use super::prims::self_derived_key;
use super::spine::{open, open_pair, Top};
use super::CutMemo;
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::hir::physical::Frame;
use crate::validate::reject_float_keys;
use gnitz_core::{CircuitBuilder, ColumnDef, ReindexSlot, TypeCode};
use std::collections::HashSet;
use std::rc::Rc;

/// Lower a `SetOp` body to circuit pieces.
pub(super) fn lower_setop(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    setop: &RelExpr,
) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::SetOp { op, all, left, right, out } = setop else {
        unreachable!("lower_setop receives a SetOp");
    };

    // A float set-identity column breaks content-hash equality (IEEE-754).
    reject_float_keys(out.iter().map(|c| &c.out.def), "set operation")?;

    // UNION ALL keeps both copies of an identical row, so the right side hashes on
    // a distinct branch id; every deduplicating op uses branch 0.
    let right_branch_id = matches!((op, all), (SetOpKind::Union, true)) as u8;

    let ids: [Vec<ColId>; 2] = [
        out.iter().map(|c| c.left).collect(),
        out.iter().map(|c| c.right).collect(),
    ];
    let live = |ids: &[ColId]| ids.iter().copied().collect::<HashSet<ColId>>();
    let (live_l, live_r) = (live(&ids[0]), live(&ids[1]));
    // UNION / UNION ALL are linear merges the dag drives by cloning one epoch's
    // delta to both sides; every other operator needs two distinct sources.
    let sides = open_pair(chain, memo, [left, right], [&live_l, &live_r], *op != SetOpKind::Union)?;
    let mut cb = CircuitBuilder::new();
    let mut hashed = Vec::with_capacity(2);
    for (i, side) in sides.into_iter().enumerate() {
        let (node, frame) = side.emit(&mut cb, Top::Slots, "set operation input")?;
        // Each side's hashed columns, carrying the promotion target
        // `RelExpr::set_op` stamped per pair.
        let key: Vec<ReindexSlot> = slots_of(&frame.layout, &ids[i])?
            .into_iter()
            .zip(out)
            .map(|(s, c)| (s as u32, if i == 0 { c.left_target } else { c.right_target }))
            .collect();
        let branch_id = if i == 0 { 0 } else { right_branch_id };
        hashed.push(hash_shard_side(&mut cb, node, &key, branch_id));
    }
    let (left_node, right_node) = (hashed[0], hashed[1]);

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
    Ok(EmitPieces {
        circuit: cb.build(),
        out: hashed_out("_set_pk", out.iter().map(|c| &c.out)),
    })
}

/// Lower a `SELECT DISTINCT` body (`Distinct(Project(...))`) — dedup over the
/// projected content via the synthetic hash key.
pub(super) fn lower_distinct(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    input: &Rc<RelExpr>,
) -> Result<EmitPieces, GnitzSqlError> {
    let side_cols = input.cols();
    // A float set-identity column breaks content-hash equality (IEEE-754).
    reject_float_keys(side_cols.iter().map(|c| &c.def), "SELECT DISTINCT")?;
    let ids: Vec<ColId> = side_cols.iter().map(|c| c.id).collect();
    let spine = open(chain, memo, input, &ids.iter().copied().collect())?;
    let mut cb = CircuitBuilder::new();
    let (node, frame) = spine.emit(&mut cb, Top::Slots, "SELECT DISTINCT input")?;
    let slots = slots_of(&frame.layout, &ids)?;
    let sharded = hash_shard_side(&mut cb, node, &self_derived_key(&slots), 0);
    let distinct_node = cb.distinct(sharded);
    cb.sink(distinct_node);
    Ok(EmitPieces {
        circuit: cb.build(),
        out: hashed_out("_distinct_pk", side_cols.iter()),
    })
}

/// A content-hashed body's output frame: the synthetic hidden U128 hash PK, then
/// the body's output columns. One home for the `_set_pk` / `_distinct_pk`
/// convention.
///
/// No duplicate-name guard: bind runs one over the projection these columns come
/// from, and neither shell renames anything.
fn hashed_out<'a>(pk_name: &str, cols: impl Iterator<Item = &'a HirCol>) -> Frame {
    Frame::keyed(
        vec![ColumnDef::new(pk_name, TypeCode::U128, false).hidden()],
        cols.map(|c| (c.id, c.def.clone())),
    )
}

/// Hash the projected columns to a synthetic content PK — widening each column
/// carrying a promotion target into the promoted layout so both set-op sides
/// share one physical representation — then shard by that PK.
fn hash_shard_side(
    cb: &mut CircuitBuilder,
    filtered: gnitz_core::NodeId,
    cols: &[ReindexSlot],
    branch_id: u8,
) -> gnitz_core::NodeId {
    // Reindex by a hash of the projected columns, so set membership
    // (EXCEPT/INTERSECT/UNION-distinct) is decided by the projected row content,
    // not by the source table's PK: two rows from different tables sharing a PK
    // but differing in payload must not match.
    let reindexed = cb.map_hash_row(filtered, cols, branch_id);
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
