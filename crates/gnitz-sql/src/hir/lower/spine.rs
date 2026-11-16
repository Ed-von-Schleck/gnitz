//! The input spine every consumer opens its inputs with: the linear levels —
//! filters, projections and aliases — over a relation read in place fuse into
//! the consumer's circuit, and a node below them that no relation is read in
//! place for is cut to a hidden segment.

use super::super::physical::Frame;
use super::super::{ColId, ColSet, HirExpr, ProjEntry, RelExpr};
use super::{EmitPieces, SegInput, SegSource, ViewChain, collect_live_cols, cut_segment, filter, project_front};
use crate::access::candidates;
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use crate::ir::BoundExpr;
use gnitz_core::Schema;
use gnitz_wire::ReadBound;
use gnitz_wire::{Circuit, NodeId, ReindexSlot};
use std::borrow::Cow;
use std::rc::Rc;
use std::sync::Arc;

/// A single input as its consumer's circuit reads it: a source — a relation read
/// in place, or a hidden segment — under the linear levels fused above it,
/// outermost first, each with the columns read of it.
pub(super) struct Spine<'a> {
    seg: SegInput,
    levels: Vec<(Level<'a>, ColSet)>,
    /// The key the source's delta is co-located by ahead of the circuit, in the
    /// source's own columns; `None` leaves it where the source's store places it.
    scatter: Option<Vec<ReindexSlot>>,
}

/// One linear level: a `Filter`, or a `Project` — an `Alias` is the projection
/// of each input column under its new identity.
pub(super) enum Level<'a> {
    Where(Cow<'a, [HirExpr]>),
    Project(Cow<'a, [ProjEntry]>),
}

impl<'a> Level<'a> {
    /// What is read of this level's input when `above` is read of its output.
    fn reads(&self, above: &ColSet) -> ColSet {
        let mut below = ColSet::default();
        match self {
            Level::Where(preds) => {
                below.clone_from(above);
                collect_live_cols(preds.iter(), &mut below);
            }
            Level::Project(items) => collect_live_cols(
                items.iter().filter(|it| above.contains(it.out.id)).map(|it| &it.expr),
                &mut below,
            ),
        }
        below
    }

    /// This level with every `ColRef(id)` leaf replaced by `f(id)`.
    pub(super) fn rebuilt(&self, f: &mut impl FnMut(ColId) -> HirExpr) -> Level<'a> {
        let mut leaf = |id: &ColId| f(*id);
        match self {
            Level::Where(preds) => Level::Where(Cow::Owned(preds.iter().map(|p| p.rebuild(&mut leaf)).collect())),
            Level::Project(items) => Level::Project(Cow::Owned(
                items
                    .iter()
                    .map(|it| ProjEntry {
                        expr: it.expr.rebuild(&mut leaf),
                        out: it.out.clone(),
                    })
                    .collect(),
            )),
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
    /// What is read of `rel`.
    live: ColSet,
    /// The node a reader not reading `bottom` in place cuts, with the number of
    /// levels above it: `rel` itself, or the input of the deepest `Alias` — a
    /// subtree every `Alias` of it shares, so nothing above it is cut with it.
    cut: (usize, &'a Rc<RelExpr>),
}

impl<'a> Peeled<'a> {
    /// Whether the bottom node is this body's own, under no `Alias`.
    pub(super) fn owns_bottom(&self) -> bool {
        self.cut.0 == 0
    }

    /// These levels opened for their consumer. They fuse when the node below them
    /// is a relation read in place; otherwise [`Self::cut`] is cut, and only the
    /// levels above it fuse.
    pub(super) fn open(self, chain: &mut ViewChain) -> Result<Spine<'a>, GnitzSqlError> {
        let Peeled {
            mut levels,
            bottom,
            live,
            cut: (above, node),
            ..
        } = self;
        let seg = match bottom.as_ref() {
            RelExpr::Get { desc, cols } => SegInput {
                src: SegSource::Catalog(Arc::clone(desc)),
                frame: Frame::scan(desc, cols),
            },
            // Every reader of a shared subtree reads all of it: its segment is one.
            _ if above > 0 => {
                levels.truncate(above);
                cut_segment(chain, node, &node.cols().iter().map(|c| c.id).collect())?
            }
            _ => {
                levels.clear();
                cut_segment(chain, node, &live)?
            }
        };
        Ok(Spine { seg, levels, scatter: None })
    }
}

/// `rel`'s linear levels, for a reader of `live` of it.
pub(super) fn peel<'a>(rel: &'a Rc<RelExpr>, live: &ColSet) -> Peeled<'a> {
    let mut bottom = rel;
    let mut levels: Vec<(Level<'a>, ColSet)> = Vec::new();
    let mut read = live.clone();
    let mut cut = (0, rel);
    loop {
        let (level, input) = match bottom.as_ref() {
            RelExpr::Filter { input, preds } => (Level::Where(Cow::Borrowed(preds)), input),
            RelExpr::Project { input, items } => (Level::Project(Cow::Borrowed(items)), input),
            RelExpr::Alias { input, cols } => {
                cut = (levels.len() + 1, input);
                let items = (input.cols().iter())
                    .zip(cols)
                    .map(|(i, out)| ProjEntry {
                        expr: BExpr::ColRef(i.id),
                        out: out.clone(),
                    })
                    .collect();
                (Level::Project(Cow::Owned(items)), input)
            }
            _ => break,
        };
        let below = level.reads(&read);
        levels.push((level, std::mem::replace(&mut read, below)));
        bottom = input;
    }
    Peeled {
        levels,
        bottom,
        read,
        live: live.clone(),
        cut,
    }
}

/// What the consumer needs of the top projection: its exact columns (a view's
/// output, a top-N or window carrying every input column), or only a slot per column it
/// reads (a reduce, a set operation's leaves).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Top {
    Output,
    Slots,
}

/// `schema`'s PK as a reindex key: each column at its key slot's type.
pub(super) fn pk_slots(schema: &Schema) -> Vec<ReindexSlot> {
    (schema.pk_cols().iter())
        .map(|&c| (c, schema.columns()[c as usize].ty.tc.reindex_output_type()))
        .collect()
}

/// Open `rel` for a consumer reading `live` of it.
pub(super) fn open<'a>(chain: &mut ViewChain, rel: &'a Rc<RelExpr>, live: &ColSet) -> Result<Spine<'a>, GnitzSqlError> {
    peel(rel, live).open(chain)
}

impl Spine<'_> {
    pub(super) fn tid(&self) -> u64 {
        self.seg.src.tid()
    }

    /// The opened source's, since no fused level re-keys.
    pub(super) fn pk_repeats(&self) -> bool {
        self.seg.src.pk_repeats()
    }

    /// A hidden segment read whole, with nothing fused above it.
    pub(super) fn segment(seg: SegInput) -> Spine<'static> {
        Spine { seg, levels: Vec::new(), scatter: None }
    }

    /// The source's delta co-located by `key`, its own columns, ahead of the
    /// circuit; broadcast by an empty one.
    pub(super) fn scatter(&mut self, key: Vec<ReindexSlot>) {
        self.scatter = Some(key);
    }

    /// The source's PK as a scatter key.
    pub(super) fn pk_key(&self) -> Vec<ReindexSlot> {
        pk_slots(&self.seg.frame.schema)
    }

    /// The column of the opened relation each of `ids` is a verbatim copy of, or
    /// `None` where one is computed. A bare column reference copies its column
    /// verbatim, whether [`Self::emit`] relabels it or emits it; anything else
    /// ends that column's chain.
    pub(super) fn source_cols(&self, ids: impl IntoIterator<Item = ColId>) -> Option<Vec<u32>> {
        ids.into_iter()
            .map(|mut id| {
                for (level, _) in &self.levels {
                    id = match level {
                        Level::Where(_) => id,
                        Level::Project(items) => items.iter().find(|it| it.out.id == id)?.expr.as_col()?,
                    };
                }
                self.seg.frame.slot(id).ok().map(|slot| slot as u32)
            })
            .collect()
    }

    /// Emit the source and the levels into `cb`. A rename-only projection
    /// relabels the frame instead of emitting a node, except the outermost one
    /// under `Top::Output`.
    pub(super) fn emit(self, cb: &mut Circuit, top: Top) -> Result<(NodeId, Frame), GnitzSqlError> {
        let Spine { seg, levels, scatter } = self;
        let SegInput { src, frame } = seg;
        let read = |cb: &mut Circuit, frame: &Frame, leading| source(cb, &src, frame, leading, scatter);
        emit_levels(cb, &levels, top, frame, read)
    }
}

/// `levels` over `node`, whose rows `frame` addresses, as the consumer's output.
pub(super) fn emit_over(
    cb: &mut Circuit,
    levels: &[(Level<'_>, ColSet)],
    node: NodeId,
    frame: Frame,
) -> Result<(NodeId, Frame), GnitzSqlError> {
    let start = |cb: &mut Circuit, frame: &Frame, preds: Vec<BoundExpr>| filter(cb, node, &preds, &frame.schema);
    emit_levels(cb, levels, Top::Output, frame, start)
}

/// Emit `levels`, innermost first, over the node `start` emits filtered by the
/// leading WHERE levels — a source they bound — whose rows `frame` addresses. The
/// WHERE levels between two emitted nodes are one filter: a relabel moves no slot.
fn emit_levels(
    cb: &mut Circuit,
    levels: &[(Level<'_>, ColSet)],
    top: Top,
    mut frame: Frame,
    start: impl FnOnce(&mut Circuit, &Frame, Vec<BoundExpr>) -> Result<NodeId, GnitzSqlError>,
) -> Result<(NodeId, Frame), GnitzSqlError> {
    let output_at = match top {
        Top::Output => levels.iter().position(|(l, _)| !matches!(l, Level::Where(_))),
        Top::Slots => None,
    };
    let mut start = Some(start);
    let mut node = None;
    let mut pending: Vec<BoundExpr> = Vec::new();
    let mut at = |cb: &mut Circuit, node: Option<NodeId>, frame: &Frame, pending: &mut Vec<BoundExpr>| {
        let preds = std::mem::take(pending);
        match node {
            Some(at) => filter(cb, at, &preds, &frame.schema),
            None => start.take().expect("the start is emitted once")(cb, frame, preds),
        }
    };
    for (i, (level, read)) in levels.iter().enumerate().rev() {
        match level {
            Level::Where(preds) => pending.extend(frame.resolve_preds(preds)?),
            Level::Project(items) => {
                let items: Cow<'_, [ProjEntry]> = match items.iter().all(|it| read.contains(it.out.id)) {
                    true => Cow::Borrowed(items),
                    false => items.iter().filter(|it| read.contains(it.out.id)).cloned().collect(),
                };
                if output_at != Some(i)
                    && let Some(relabeled) = frame.relabeled(&items)?
                {
                    frame = relabeled;
                    continue;
                }
                let input = at(cb, node, &frame, &mut pending)?;
                let (out_node, out) = project_front(cb, input, &items, &frame)?;
                node = Some(out_node);
                frame = out;
            }
        }
    }
    let node = at(cb, node, &frame, &mut pending)?;
    Ok((node, frame))
}

/// `src`'s delta node, co-located by `scatter` and bounded by what `conjuncts`
/// give a catalog source, then filtered by them.
fn source(
    cb: &mut Circuit,
    src: &SegSource,
    frame: &Frame,
    conjuncts: Vec<BoundExpr>,
    scatter: Option<Vec<ReindexSlot>>,
) -> Result<NodeId, GnitzSqlError> {
    // The bound only narrows the scan; every conjunct is still filtered below.
    let bound = match src {
        SegSource::Segment { .. } => ReadBound::None,
        SegSource::Catalog(d) => {
            candidates(&conjuncts, &frame.schema, &d.indexes)
                .into_iter()
                .map(|c| c.bound)
                // A backfill never routes, and the engine may trade an index walk for the
                // full scan, which it cannot do for a PK range.
                .min_by_key(|b| match b {
                    ReadBound::PkSet(_) => 0,
                    ReadBound::Range(r) if !r.walks_pk(frame.schema.pk_cols()) => 1,
                    _ => 2,
                })
                .unwrap_or(ReadBound::None)
        }
    };
    let node = cb.input_scattered(src.tid(), bound, scatter);
    filter(cb, node, &conjuncts, &frame.schema)
}

/// Lower a linear body: `spine`'s levels over its one source. The emitted circuit
/// carries no exchange: a filter or map neither re-keys nor redistributes its
/// source.
pub(super) fn lower_linear(spine: Spine<'_>) -> Result<EmitPieces, GnitzSqlError> {
    let mut cb = Circuit::default();
    let pk_repeats = spine.pk_repeats();
    let (_, out) = spine.emit(&mut cb, Top::Output)?;
    Ok(EmitPieces { circuit: cb, out, pk_repeats })
}
