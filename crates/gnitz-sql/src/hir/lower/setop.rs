//! The set-operation and DISTINCT shell: each leaf, opened through the spine, is
//! content-hashed to a synthetic PK and combined by weight arithmetic.

use super::super::{slots_of, ColId, HirCol, RelExpr, SetOpKind};
use super::spine::{open, Top};
use super::CutMemo;
use crate::error::GnitzSqlError;
use crate::hir::chain::{EmitPieces, ViewChain};
use crate::hir::physical::Frame;
use gnitz_core::{Circuit, ColumnDef, NodeId, ReindexSlot, TypeCode};
use std::rc::Rc;

/// Lower a `SetOp` body: its tree of directly nested set operations is one
/// circuit, every leaf hashed at the root's column types.
pub(super) fn lower_setop(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    rel: &Rc<RelExpr>,
    out: &[HirCol],
) -> Result<EmitPieces, GnitzSqlError> {
    let mut cb = Circuit::default();
    let top = combine(chain, memo, &mut cb, rel, out)?;
    Ok(hashed_pieces(cb, top, "_set_pk", out))
}

/// `rel` as weight arithmetic over its hashed leaves.
fn combine(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    cb: &mut Circuit,
    rel: &Rc<RelExpr>,
    out: &[HirCol],
) -> Result<NodeId, GnitzSqlError> {
    let RelExpr::SetOp { op, all, left, right, .. } = rel.as_ref() else {
        return hashed_side(chain, memo, cb, rel, out, "set operation input");
    };
    if *op == SetOpKind::Union {
        let mut operands = Vec::new();
        union_operands(rel, !all, &mut operands);
        let mut sum = combine(chain, memo, cb, operands[0], out)?;
        for operand in &operands[1..] {
            let term = combine(chain, memo, cb, operand, out)?;
            sum = cb.union(sum, term);
        }
        return Ok(if *all { sum } else { cb.distinct(sum) });
    }
    let a = combine(chain, memo, cb, left, out)?;
    let b = combine(chain, memo, cb, right, out)?;
    let is_set = |r: &RelExpr| r.unique_key().is_some();
    // B ≥ 0, so positive_part(A − B) and min(A, B) are sets once A is; min also
    // once B is.
    let result_is_set = is_set(left) || (*op == SetOpKind::Intersect && is_set(right));
    let a = if *all || result_is_set { a } else { cb.distinct(a) };
    let except = cb.positive_diff(a, b);
    Ok(if *op == SetOpKind::Except {
        except
    } else {
        // min(A, B) = A − positive_part(A − B)
        cb.difference(a, except)
    })
}

/// The operands of the union `rel`, through every union nested directly under
/// it that its own clamp also covers: under a DISTINCT that is every one of
/// them, since `distinct(distinct(x) + y) = distinct(x + y)` over weights ≥ 0.
fn union_operands<'a>(rel: &'a Rc<RelExpr>, distinct: bool, operands: &mut Vec<&'a Rc<RelExpr>>) {
    match rel.as_ref() {
        RelExpr::SetOp {
            op: SetOpKind::Union, all, left, right, ..
        } if *all || distinct => {
            union_operands(left, distinct, operands);
            union_operands(right, distinct, operands);
        }
        _ => operands.push(rel),
    }
}

/// Lower a `SELECT DISTINCT` body: dedup over its input's content hash.
pub(super) fn lower_distinct(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    input: &Rc<RelExpr>,
) -> Result<EmitPieces, GnitzSqlError> {
    let cols = input.cols();
    let mut cb = Circuit::default();
    let side = hashed_side(chain, memo, &mut cb, input, &cols, "SELECT DISTINCT input")?;
    let top = cb.distinct(side);
    Ok(hashed_pieces(cb, top, "_distinct_pk", &cols))
}

/// `side` opened and keyed by a hash of its columns, each widened to its
/// positional `out` column's type, then exchanged on that hash.
fn hashed_side(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    cb: &mut Circuit,
    side: &Rc<RelExpr>,
    out: &[HirCol],
    what: &str,
) -> Result<NodeId, GnitzSqlError> {
    let ids: Vec<ColId> = side.cols().iter().map(|c| c.id).collect();
    let (node, frame) = open(chain, memo, side, &ids.iter().copied().collect())?.emit(cb, Top::Slots, what)?;
    let key: Vec<ReindexSlot> = slots_of(&frame.layout, &ids)?
        .into_iter()
        .zip(out)
        .map(|(s, o)| {
            let tc = o.def.ty.tc;
            (s as u32, (frame.schema.columns[s].ty.tc != tc).then_some(tc))
        })
        .collect();
    Ok(cb.map_hash_row(node, &key))
}

/// The sunk circuit and its frame: the hidden U128 content-hash PK, then `cols`.
fn hashed_pieces(mut cb: Circuit, top: NodeId, pk_name: &str, cols: &[HirCol]) -> EmitPieces {
    cb.sink(top);
    EmitPieces {
        circuit: cb,
        out: Frame::keyed(
            vec![ColumnDef::new(pk_name, TypeCode::U128, false).hidden()],
            cols.iter().map(|c| (c.id, c.def.clone())),
        ),
        // A content hash identifies its own row.
        pk_repeats: false,
    }
}
