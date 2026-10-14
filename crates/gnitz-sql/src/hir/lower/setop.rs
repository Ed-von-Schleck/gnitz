//! The set-operation and DISTINCT shell: each leaf, opened through the spine, is
//! keyed by its element's identity and combined by weight arithmetic. A base
//! table's PK is that identity, so a leaf carrying one keeps it; any other is
//! keyed by a hash of its row.

use super::super::{ColId, HirCol, RelExpr, SetOpKind};
use super::spine::{open, Spine, Top};
use super::{EmitPieces, SegSource, ViewChain};
use crate::error::GnitzSqlError;
use crate::hir::physical::Frame;
use gnitz_wire::{Circuit, ColumnDef, NodeId, ReadBound, ReindexSlot, RelClass, TypeCode};
use std::rc::Rc;

/// Lower a `SetOp` body: its tree of directly nested set operations is one
/// circuit over its leaves, each opened once.
pub(super) fn lower_setop(
    chain: &mut ViewChain,
    rel: &Rc<RelExpr>,
    out: &[HirCol],
) -> Result<EmitPieces, GnitzSqlError> {
    // The leaves, in the order `combine` emits them.
    let mut sides: Vec<&Rc<RelExpr>> = Vec::new();
    combine(&mut Circuit::default(), rel, &mut |cb, side| {
        sides.push(side);
        Ok(cb.input_delta(0, ReadBound::None))
    })?;
    let spines = sides
        .iter()
        .map(|side| open(chain, side, &side.cols().iter().map(|c| c.id).collect()))
        .collect::<Result<Vec<_>, _>>()?;
    // An ALL root can hold an element at weight above 1; any other clamps to a set.
    let bag = matches!(rel.as_ref(), RelExpr::SetOp { all: true, .. });
    let unique = unique_on_key(rel);
    // A content hash keys a set by its elements; a source key does only where it stays unique.
    let key = source_key(&sides, &spines, out).filter(|_| bag || unique);

    let mut cb = Circuit::default();
    let mut spines = spines.into_iter();
    let mut leaf = |cb: &mut Circuit, side: &Rc<RelExpr>| {
        let spine = spines.next().expect("one spine per leaf, opened in `combine` order");
        match &key {
            Some(key) => keyed_side(cb, spine, side, key),
            None => hashed_side(cb, spine, side, out),
        }
    };
    combine(&mut cb, rel, &mut leaf)?;
    match key {
        Some(key) => keyed_pieces(cb, out, &key, !unique),
        None => hashed_pieces(cb, "_set_pk", out, bag),
    }
}

/// `rel` as weight arithmetic over its leaves, each emitted by `leaf`.
fn combine<'r>(
    cb: &mut Circuit,
    rel: &'r Rc<RelExpr>,
    leaf: &mut impl FnMut(&mut Circuit, &'r Rc<RelExpr>) -> Result<NodeId, GnitzSqlError>,
) -> Result<NodeId, GnitzSqlError> {
    let RelExpr::SetOp { op, all, left, right, .. } = rel.as_ref() else {
        return leaf(cb, rel);
    };
    if *op == SetOpKind::Union {
        let mut operands = Vec::new();
        union_operands(rel, !all, &mut operands);
        let mut sum = combine(cb, operands[0], leaf)?;
        for operand in &operands[1..] {
            let term = combine(cb, operand, leaf)?;
            sum = cb.union(sum, term);
        }
        return Ok(if *all { sum } else { cb.distinct(sum) });
    }
    // A backfill feeds the sources in scan order, and the first is combined with
    // nothing: the operand that outputs its rows on its own goes second.
    let b = combine(cb, right, leaf)?;
    let a = combine(cb, left, leaf)?;
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
    let spine = open(chain, input, &cols.iter().map(|c| c.id).collect())?;
    let mut cb = Circuit::default();
    let side = hashed_side(&mut cb, spine, input, &cols)?;
    cb.distinct(side);
    hashed_pieces(cb, "_distinct_pk", &cols, false)
}

/// The positions of `out` holding every leaf's source PK, in PK order, where each
/// leaf is a base table read in place at `out`'s column types.
fn source_key(sides: &[&Rc<RelExpr>], spines: &[Spine<'_>], out: &[HirCol]) -> Option<Vec<usize>> {
    let mut key: Option<Vec<usize>> = None;
    for (side, spine) in sides.iter().zip(spines) {
        let SegSource::Catalog(desc) = spine.source() else {
            return None;
        };
        if desc.class != RelClass::Table {
            return None;
        }
        let cols = side.cols();
        if cols.iter().zip(out).any(|(c, o)| c.def.ty != o.def.ty) {
            return None;
        }
        // Read in place, a leaf's unique key is its source PK.
        let at = side
            .unique_key()?
            .iter()
            .map(|k| cols.iter().position(|c| c.id == *k))
            .collect::<Option<Vec<usize>>>()?;
        match &key {
            Some(seen) if *seen != at => return None,
            _ => key = Some(at),
        }
    }
    key
}

/// `side` keyed by its source PK, its other columns behind it in output order,
/// then exchanged on that key.
fn keyed_side(cb: &mut Circuit, spine: Spine<'_>, side: &Rc<RelExpr>, key: &[usize]) -> Result<NodeId, GnitzSqlError> {
    // The output projection pins the source PK to the front.
    let (node, frame) = spine.emit(cb, Top::Output)?;
    let slots = frame.slots(side.cols().iter().map(|c| c.id))?;
    if key
        .iter()
        .map(|&p| slots[p] as u32)
        .ne(frame.schema.pk_cols().iter().copied())
    {
        return Err(GnitzSqlError::Internal(
            "a source-keyed set-op leaf does not carry its source PK".into(),
        ));
    }
    Ok(cb.shard(node))
}

/// The circuit and its frame over source-keyed leaves: `out`'s key columns, then
/// the rest in output order.
fn keyed_pieces(cb: Circuit, out: &[HirCol], key: &[usize], pk_repeats: bool) -> Result<EmitPieces, GnitzSqlError> {
    let slot = |c: &HirCol| (Some(c.id), c.def.clone());
    let payload = (0..out.len()).filter(|p| !key.contains(p)).map(|p| slot(&out[p]));
    Ok(EmitPieces {
        circuit: cb,
        out: Frame::new(key.iter().map(|&p| slot(&out[p])).chain(payload), key.len())?,
        pk_repeats,
    })
}

/// Whether `rel`'s result holds each key of its leaves' unique keys once.
fn unique_on_key(rel: &RelExpr) -> bool {
    match rel {
        RelExpr::SetOp { op: SetOpKind::Union, .. } => false,
        RelExpr::SetOp { op: SetOpKind::Except, left, .. } => unique_on_key(left),
        RelExpr::SetOp {
            op: SetOpKind::Intersect, left, right, ..
        } => unique_on_key(left) || unique_on_key(right),
        leaf => leaf.unique_key().is_some(),
    }
}

/// `side` keyed by a hash of its columns, each widened to its positional `out`
/// column's type, then exchanged on that hash.
fn hashed_side(
    cb: &mut Circuit,
    spine: Spine<'_>,
    side: &Rc<RelExpr>,
    out: &[HirCol],
) -> Result<NodeId, GnitzSqlError> {
    let ids: Vec<ColId> = side.cols().iter().map(|c| c.id).collect();
    let (node, frame) = spine.emit(cb, Top::Slots)?;
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
