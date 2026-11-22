//! The input spine every consumer opens its inputs with: the linear levels —
//! filters and projections — over a relation read in place fuse into the
//! consumer's circuit, and a node below them that no relation is read in place
//! for is cut to a hidden segment: whole where the body reads it more than once,
//! else pruned to what its one reader reads. The same levels over a combine are
//! emitted over its node.

use super::super::physical::{self, Frame, Relabel};
use super::super::{ColId, ColSet, HirExpr, ProjEntry, RelExpr};
use super::{SegInput, ViewChain};
use crate::access::candidates;
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BoundExpr};
use gnitz_core::RelDescriptor;
use gnitz_wire::{Circuit, NodeId, ReadBound, ReindexSlot};
use std::borrow::Cow;
use std::rc::Rc;
use std::sync::Arc;

/// A single input as its consumer's circuit reads it: a source — a relation read
/// in place, or a hidden segment — under the linear levels fused above it,
/// outermost first, each with the columns read of it.
pub(super) struct Spine<'a> {
    pub(super) seg: SegInput,
    pub(super) levels: Vec<(Level<'a>, ColSet)>,
}

/// One linear level: a `Filter` or a `Project`.
pub(super) enum Level<'a> {
    Where(Cow<'a, [HirExpr]>),
    Project(Cow<'a, [ProjEntry]>),
}

impl Level<'_> {
    /// This level reading the column `id` as the constant `value`.
    pub(super) fn substituted(&self, id: ColId, value: &HirExpr) -> Level<'static> {
        let mut leaf = |c: &ColId| match *c == id {
            true => value.clone(),
            false => BExpr::ColRef(*c),
        };
        match self {
            Level::Where(preds) => Level::Where(preds.iter().map(|p| p.rebuild(&mut leaf)).collect()),
            Level::Project(items) => Level::Project(
                (items.iter())
                    .map(|it| ProjEntry {
                        expr: it.expr.rebuild(&mut leaf),
                        out: it.out.clone(),
                    })
                    .collect(),
            ),
        }
    }
}

/// `rel`'s linear levels down to the first node that is none, outermost first,
/// each with what is read of its output when `live` is read of `rel`.
pub(super) struct Peeled<'a> {
    pub(super) levels: Vec<(Level<'a>, ColSet)>,
    /// The node below the levels.
    pub(super) bottom: &'a Rc<RelExpr>,
    /// What is read of `bottom`.
    pub(super) read: ColSet,
    /// The node a reader cuts unless `bottom` is a relation read in place, with
    /// the number of levels above it: `rel` itself, or the deepest level's input the
    /// body reads more than once — a subtree its readers share, so nothing above it
    /// is cut with it.
    pub(super) cut: (usize, &'a Rc<RelExpr>),
}

impl<'a> Peeled<'a> {
    /// These levels opened for their consumer. They fuse when the node below them
    /// is a relation read in place; otherwise the cut node is cut, and only the
    /// levels above it fuse.
    pub(super) fn open(self, chain: &mut ViewChain) -> Result<Spine<'a>, GnitzSqlError> {
        let Peeled {
            mut levels,
            bottom,
            read,
            cut: (above, node),
        } = self;
        let seg = match bottom.as_ref() {
            RelExpr::Get { desc, cols } => SegInput {
                tid: desc.tid,
                pk_repeats: desc.pk_repeats,
                catalog: Some(Arc::clone(desc)),
                frame: Frame::scan(desc, cols),
            },
            // What is read of `rel`, the outermost level's set, prunes only `rel`
            // itself: a shared node is cut whole.
            _ => {
                let live = levels.first().map_or(read, |(_, r)| r.clone());
                levels.truncate(above);
                chain.cut(node, &live)?
            }
        };
        Ok(Spine { seg, levels })
    }
}

/// `rel`'s linear levels, for a reader of `live` of it. The cut is the deepest
/// node below `rel` more than one node reads.
pub(super) fn peel<'a>(chain: &ViewChain, rel: &'a Rc<RelExpr>, live: &ColSet) -> Peeled<'a> {
    fn reads<'e>(exprs: impl IntoIterator<Item = &'e HirExpr>, into: &mut ColSet) {
        for e in exprs {
            e.for_each_ref(&mut |id| into.insert(*id));
        }
    }
    let mut bottom = rel;
    let mut levels: Vec<(Level<'a>, ColSet)> = Vec::new();
    let mut read = live.clone();
    let mut cut = (0, rel);
    loop {
        // Each level with what is read of its input.
        let (level, input, below) = match bottom.as_ref() {
            RelExpr::Filter { input, preds } => {
                let mut below = read.clone();
                reads(preds.iter(), &mut below);
                (Level::Where(Cow::Borrowed(preds)), input, below)
            }
            RelExpr::Project { input, items } => {
                let mut below = ColSet::default();
                reads(
                    items.iter().filter(|it| read.contains(it.out.id)).map(|it| &it.expr),
                    &mut below,
                );
                (Level::Project(Cow::Borrowed(items)), input, below)
            }
            _ => break,
        };
        levels.push((level, std::mem::replace(&mut read, below)));
        bottom = input;
        if chain.shared.contains_key(&Rc::as_ptr(input)) {
            cut = (levels.len(), input);
        }
    }
    Peeled { levels, bottom, read, cut }
}

/// Where the levels' rows start.
pub(super) enum Head {
    /// The source `tid`, its delta co-located by `scatter` ahead of the circuit —
    /// broadcast by an empty key, left where its store places it by none — and its
    /// scan bounded by the leading WHERE levels over the relation `catalog` reads in
    /// place.
    Source {
        tid: u64,
        catalog: Option<Arc<RelDescriptor>>,
        scatter: Option<Vec<ReindexSlot>>,
    },
    /// A node the circuit already holds.
    Node(NodeId),
}

impl Spine<'_> {
    /// The slot of the opened source `id` is a verbatim copy of, or `None` where it
    /// is computed. A bare column reference copies its column verbatim, whether
    /// [`emit_levels`] relabels it or emits it; anything else ends the chain.
    pub(super) fn source_slot(&self, mut id: ColId) -> Option<u32> {
        for (level, _) in &self.levels {
            if let Level::Project(items) = level {
                id = items.iter().find(|it| it.out.id == id)?.expr.as_col()?;
            }
        }
        Some(self.seg.frame.slot(id).ok()? as u32)
    }

    /// Emit the source, co-located by `scatter`, and the levels into `cb`.
    pub(super) fn emit(
        self,
        cb: &mut Circuit,
        top: Relabel,
        scatter: Option<Vec<ReindexSlot>>,
    ) -> Result<(NodeId, Frame), GnitzSqlError> {
        let Spine { seg, levels } = self;
        let SegInput { tid, catalog, frame, .. } = seg;
        emit_levels(cb, &levels, top, frame, Head::Source { tid, catalog, scatter })
    }
}

/// Emit `levels`, innermost first, over `head`, whose rows `frame` addresses. A
/// rename-only projection relabels the frame instead of emitting a node: the
/// outermost one as `top` admits, every inner one as a slot per column. The WHERE
/// levels between two emitted nodes are one filter: a relabel moves no slot.
pub(super) fn emit_levels(
    cb: &mut Circuit,
    levels: &[(Level<'_>, ColSet)],
    top: Relabel,
    mut frame: Frame,
    mut head: Head,
) -> Result<(NodeId, Frame), GnitzSqlError> {
    let outermost = levels.iter().position(|(l, _)| matches!(l, Level::Project(_)));
    let mut pending: Vec<BoundExpr> = Vec::new();
    for (i, (level, read)) in levels.iter().enumerate().rev() {
        let items = match level {
            Level::Where(preds) => {
                pending.extend(frame.resolve_preds(preds)?);
                continue;
            }
            Level::Project(items) => items,
        };
        let items: Cow<'_, [ProjEntry]> = match items.iter().all(|it| read.contains(it.out.id)) {
            true => Cow::Borrowed(items),
            false => items.iter().filter(|it| read.contains(it.out.id)).cloned().collect(),
        };
        let how = if outermost == Some(i) { top } else { Relabel::Slots };
        if let Some(relabeled) = frame.relabeled(&items, how)? {
            frame = relabeled;
            continue;
        }
        let input = settle(cb, head, &frame, std::mem::take(&mut pending))?;
        let (map, out) = physical::projection_map(&items, &frame)?;
        head = Head::Node(map.map_or(input, |map| cb.map_expr(input, map)));
        frame = out;
    }
    let node = settle(cb, head, &frame, pending)?;
    Ok((node, frame))
}

/// `head` as a node, filtered by `conjuncts` over `frame`: a source's delta node
/// first, bounded by what they give a relation read in place.
fn settle(cb: &mut Circuit, head: Head, frame: &Frame, conjuncts: Vec<BoundExpr>) -> Result<NodeId, GnitzSqlError> {
    let node = match head {
        Head::Node(node) => node,
        Head::Source { tid, catalog, scatter } => {
            // The bound only narrows the scan; every conjunct is still filtered below.
            let bound = catalog.and_then(|d| {
                candidates(&conjuncts, &frame.schema, &d.indexes)
                    .into_iter()
                    .map(|c| c.bound)
                    // Every conjunct is filtered below, so no candidate past the scan serves.
                    .take_while(|b| !matches!(b, ReadBound::None))
                    // A backfill never routes, and the engine may trade an index walk for the
                    // full scan, which it cannot do for a PK range.
                    .min_by_key(|b| match b {
                        ReadBound::PkSet(_) => 0,
                        ReadBound::Range(r) if !r.walks_pk(frame.schema.pk_cols()) => 1,
                        _ => 2,
                    })
            });
            cb.input_scattered(tid, bound.unwrap_or(ReadBound::None), scatter)
                .map_err(GnitzSqlError::rejected)?
        }
    };
    Ok(
        match crate::expr_lower::compile_filter_program(&conjuncts, frame.schema.columns())? {
            Some(prog) => cb.filter(node, prog.to_blob_bytes()),
            None => node,
        },
    )
}
