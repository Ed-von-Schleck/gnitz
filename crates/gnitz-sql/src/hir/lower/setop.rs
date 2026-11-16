//! The set-operation and DISTINCT shell: each leaf, opened through the spine, is
//! keyed by its element's identity and combined by weight arithmetic.

use super::super::physical::Frame;
use super::super::{HirCol, ProjEntry, RelExpr, SetOpKind};
use super::spine::{Top, open};
use super::{EmitPieces, ViewChain, project_front};
use crate::error::GnitzSqlError;
use gnitz_wire::{Circuit, ColumnDef, NARROW_PK_MAX_BYTES, NodeId, NullKeys, ReadBound, ReindexSlot, TypeCode};
use std::rc::Rc;

/// A leaf emitted into the circuit: its columns, its node, and the frame that
/// node's rows are addressed by.
struct Leaf {
    cols: Vec<HirCol>,
    node: NodeId,
    frame: Frame,
}

/// What every leaf is keyed by.
enum ElementKey {
    /// The PK every leaf carries: each leaf's columns in `order`, the first `npk` the key.
    Carried { order: Vec<usize>, npk: usize },
    /// Every column, as this frame keys them.
    Natural(Frame),
    /// A hash of every column.
    Hash,
}

/// Lower a `Distinct` or `SetOp` to circuit pieces: its tree of directly nested
/// clamps and set operations is one circuit over its leaves, each opened and
/// emitted once.
pub(super) fn lower_setop(chain: &mut ViewChain, rel: &Rc<RelExpr>) -> Result<EmitPieces, GnitzSqlError> {
    let out = rel.cols();
    let unique = rel.unique_key();
    let mut cb = Circuit::default();
    // The leaves, in the order `combine` reads them, which is scan order.
    let mut leaves: Vec<Leaf> = Vec::new();
    combine(&mut Circuit::default(), rel, false, &mut |dry, side| {
        let cols = side.cols().into_owned();
        let spine = open(chain, side, &cols.iter().map(|c| c.id).collect())?;
        let (node, frame) = spine.emit(&mut cb, Top::Slots)?;
        leaves.push(Leaf { cols, node, frame });
        Ok(dry.input_delta(0, ReadBound::None))
    })?;
    // A set's key covers its unique key, so its PK repeats only where it is a bag.
    let covers = |key: &Vec<usize>| {
        unique
            .as_ref()
            .is_none_or(|u| u.iter().all(|id| key.iter().any(|&p| out[p].id == *id)))
    };
    let key = match carried_key(&leaves, &out).filter(covers) {
        Some(key) => ElementKey::Carried {
            npk: key.len(),
            order: key
                .iter()
                .copied()
                .chain((0..out.len()).filter(|p| !key.contains(p)))
                .collect(),
        },
        None => natural_frame(&out).map_or(ElementKey::Hash, ElementKey::Natural),
    };

    let mut next = leaves.iter();
    combine(&mut cb, rel, false, &mut |cb, _| {
        let leaf = next.next().expect("one leaf per `combine` read, in the same order");
        if let ElementKey::Carried { order, .. } = &key {
            let items = ProjEntry::passthroughs(order.iter().map(|&p| leaf.cols[p].clone()));
            let (node, _) = project_front(cb, leaf.node, &items, &leaf.frame)?;
            return Ok(cb.shard(node));
        }
        let slots: Vec<ReindexSlot> = leaf
            .frame
            .slots(leaf.cols.iter().map(|c| c.id))?
            .into_iter()
            .zip(out.iter())
            .map(|(s, o)| (s as u32, o.def.ty.tc))
            .collect();
        Ok(match key {
            // A leaf row NULL where `out` is NOT NULL is of no element of the result.
            ElementKey::Natural(_) => {
                let node = cb.map_reindex(leaf.node, &slots, &[], NullKeys::Drop);
                cb.shard(node)
            }
            _ => cb.map_hash_row(leaf.node, &slots),
        })
    })?;

    let frame = match key {
        ElementKey::Carried { order, npk } => Frame::new(order.iter().map(|&p| out[p].slot()), npk)?,
        ElementKey::Natural(frame) => frame,
        ElementKey::Hash => Frame::keyed(
            vec![ColumnDef::new("_set_pk", TypeCode::U128, false).hidden()],
            out.iter().map(HirCol::slot),
        )?,
    };
    Ok(EmitPieces {
        circuit: cb,
        out: frame,
        pk_repeats: unique.is_none(),
    })
}

/// `rel` as weight arithmetic over its leaves, each emitted by `leaf` — up to each
/// weight's sign where `sign`, whose reader reads only whether it is positive.
fn combine<'r>(
    cb: &mut Circuit,
    rel: &'r Rc<RelExpr>,
    sign: bool,
    leaf: &mut impl FnMut(&mut Circuit, &'r Rc<RelExpr>) -> Result<NodeId, GnitzSqlError>,
) -> Result<NodeId, GnitzSqlError> {
    match rel.as_ref() {
        RelExpr::Distinct { input } => {
            let x = combine(cb, input, true, leaf)?;
            Ok(if sign { x } else { cb.distinct(x) })
        }
        // A sum of weights ≥ 0 is positive where an operand is.
        RelExpr::SetOp { op: SetOpKind::Union, left, right, .. } => {
            let a = combine(cb, left, sign, leaf)?;
            let b = combine(cb, right, sign, leaf)?;
            Ok(cb.union(a, b))
        }
        RelExpr::SetOp { op, left, right, .. } => {
            let intersect = *op == SetOpKind::Intersect;
            // Beside an exact set, positive_part(A − B) reads only B's sign and
            // min(A, B) only the other operand's; min is positive where both are.
            let b_sign = left.unique_key().is_some() || (intersect && sign);
            let a_sign = intersect && (sign || (right.unique_key().is_some() && !b_sign));
            // A backfill feeds the sources in scan order, the first against nothing:
            // the operand that outputs its rows on its own goes second.
            let b = combine(cb, right, b_sign, leaf)?;
            let a = combine(cb, left, a_sign, leaf)?;
            let except = cb.positive_diff(a, b);
            // min(A, B) = A − positive_part(A − B)
            Ok(if intersect { cb.difference(a, except) } else { except })
        }
        _ => leaf(cb, rel),
    }
}

/// The positions in its columns of each leaf's PK slots, where every leaf names
/// them all, at the same positions and at `out`'s column types.
fn carried_key(leaves: &[Leaf], out: &[HirCol]) -> Option<Vec<usize>> {
    let mut key: Option<Vec<usize>> = None;
    for leaf in leaves {
        if leaf.cols.iter().zip(out).any(|(c, o)| c.def.ty != o.def.ty) {
            return None;
        }
        let at = leaf
            .frame
            .schema
            .pk_cols()
            .iter()
            .map(|&s| {
                let id = leaf.frame.layout[s as usize]?;
                leaf.cols.iter().position(|c| c.id == id)
            })
            .collect::<Option<Vec<usize>>>()?;
        match &key {
            Some(seen) if *seen != at => return None,
            _ => key = Some(at),
        }
    }
    key
}

/// `out` keyed by all of its columns, where they can be its own key.
fn natural_frame(out: &[HirCol]) -> Option<Frame> {
    let frame = Frame::new(out.iter().map(HirCol::slot), out.len()).ok()?;
    // The reindex packs each column at its own type.
    let packs = out.iter().all(|c| c.def.ty.tc.packs_at(c.def.ty.tc));
    // A wider key leaves every key kernel's narrow path.
    let narrow = frame.schema.layout().pk_stride() <= NARROW_PK_MAX_BYTES;
    (packs && narrow).then_some(frame)
}
