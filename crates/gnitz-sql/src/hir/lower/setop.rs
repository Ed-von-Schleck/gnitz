//! The set-operation and DISTINCT shell: each leaf, opened through the spine, is
//! content-hashed to a synthetic PK and combined by weight arithmetic.

use super::super::{ColId, HirCol, RelExpr, SetOpKind};
use super::spine::{open, Top};
use super::{EmitPieces, ViewChain};
use crate::error::GnitzSqlError;
use crate::hir::physical::Frame;
use gnitz_wire::{Circuit, ColumnDef, NodeId, ReindexSlot, TypeCode};
use std::rc::Rc;

/// Lower a `SetOp` body: its tree of directly nested set operations is one
/// circuit, every leaf hashed at the root's column types.
pub(super) fn lower_setop(
    chain: &mut ViewChain,
    rel: &Rc<RelExpr>,
    out: &[HirCol],
) -> Result<EmitPieces, GnitzSqlError> {
    let mut cb = Circuit::default();
    combine(chain, &mut cb, rel, out)?;
    // An ALL root can hold an element at weight above 1; any other clamps to a set.
    let bag = matches!(rel.as_ref(), RelExpr::SetOp { all: true, .. });
    hashed_pieces(cb, "_set_pk", out, bag)
}

/// `rel` as weight arithmetic over its hashed leaves.
fn combine(
    chain: &mut ViewChain,
    cb: &mut Circuit,
    rel: &Rc<RelExpr>,
    out: &[HirCol],
) -> Result<NodeId, GnitzSqlError> {
    let RelExpr::SetOp { op, all, left, right, .. } = rel.as_ref() else {
        return hashed_side(chain, cb, rel, out);
    };
    if *op == SetOpKind::Union {
        let mut operands = Vec::new();
        union_operands(rel, !all, &mut operands);
        let mut sum = combine(chain, cb, operands[0], out)?;
        for operand in &operands[1..] {
            let term = combine(chain, cb, operand, out)?;
            sum = cb.union(sum, term);
        }
        return Ok(if *all { sum } else { cb.distinct(sum) });
    }
    // A backfill feeds the sources in scan order, and the first is combined with
    // nothing: the operand that outputs its rows on its own goes second.
    let b = combine(chain, cb, right, out)?;
    let a = combine(chain, cb, left, out)?;
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
pub(super) fn lower_distinct(chain: &mut ViewChain, input: &Rc<RelExpr>) -> Result<EmitPieces, GnitzSqlError> {
    let cols = input.cols();
    let mut cb = Circuit::default();
    let side = hashed_side(chain, &mut cb, input, &cols)?;
    cb.distinct(side);
    hashed_pieces(cb, "_distinct_pk", &cols, false)
}

/// `side` opened and keyed by a hash of its columns, each widened to its
/// positional `out` column's type, then exchanged on that hash.
fn hashed_side(
    chain: &mut ViewChain,
    cb: &mut Circuit,
    side: &Rc<RelExpr>,
    out: &[HirCol],
) -> Result<NodeId, GnitzSqlError> {
    let ids: Vec<ColId> = side.cols().iter().map(|c| c.id).collect();
    let (node, frame) = open(chain, side, &ids.iter().copied().collect())?.emit(cb, Top::Slots)?;
    let key: Vec<ReindexSlot> = frame
        .slots(ids.iter().copied())?
        .into_iter()
        .zip(out)
        .map(|(s, o)| (s as u32, o.def.ty.tc))
        .collect();
    Ok(cb.map_hash_row(node, &key))
}

/// The circuit and its frame: the hidden U128 content-hash PK, then `cols`. A
/// content hash identifies its own element, so the PK repeats only where an
/// element's weight can exceed 1 — `bag`.
fn hashed_pieces(cb: Circuit, pk_name: &str, cols: &[HirCol], bag: bool) -> Result<EmitPieces, GnitzSqlError> {
    Ok(EmitPieces {
        circuit: cb,
        out: Frame::keyed(
            vec![ColumnDef::new(pk_name, TypeCode::U128, false).hidden()],
            cols.iter().map(|c| (Some(c.id), c.def.clone())),
        )?,
        pk_repeats: bag,
    })
}
