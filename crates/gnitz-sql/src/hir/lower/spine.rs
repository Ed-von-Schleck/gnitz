//! The input spine every consumer opens its inputs with: the filters and
//! projections over a relation read in place fuse into the consumer's circuit.

use super::super::physical::{Frame, Rename};
use super::super::{ColId, HirExpr, ProjEntry, RelExpr};
use super::{
    collect_live_cols, cut_segment, filter, lowered_whole, project_front, resolve_in_place, EmitPieces, SegInput,
    SegSource, ViewChain,
};
use crate::access::candidates;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::Schema;
use gnitz_wire::ReadBound;
use gnitz_wire::{Circuit, NodeId, ReindexSlot};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// A single input as its consumer's circuit reads it: a source — a relation read
/// in place, or a hidden segment — under the inline `Filter` / `Project` levels
/// fused above it, innermost first, each with the columns read of it.
pub(super) struct Spine<'a> {
    seg: SegInput,
    levels: Vec<(Level<'a>, HashSet<ColId>)>,
    /// The key the source's delta is co-located by ahead of the circuit, in the
    /// source's own columns; `None` leaves it where the source's store places it.
    scatter: Option<Vec<ReindexSlot>>,
}

enum Level<'a> {
    Where(&'a [HirExpr]),
    Project(&'a [ProjEntry]),
}

impl Level<'_> {
    /// What is read of this level's input when `above` is read of its output.
    fn reads(&self, above: &HashSet<ColId>) -> HashSet<ColId> {
        let mut below = HashSet::new();
        match self {
            Level::Where(preds) => {
                below.extend(above.iter().copied());
                collect_live_cols(*preds, &mut below);
            }
            Level::Project(items) => collect_live_cols(
                items.iter().filter(|it| above.contains(&it.out.id)).map(|it| &it.expr),
                &mut below,
            ),
        }
        below
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

/// Open `rel` for a consumer reading `live` of it. Its `Filter` / `Project` levels
/// fuse when the node below them is read in place; otherwise that node is cut with
/// whatever its shell lowers whole, and only the levels above the cut fuse.
pub(super) fn open<'a>(
    chain: &mut ViewChain,
    rel: &'a Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<Spine<'a>, GnitzSqlError> {
    // Peel the levels top-down, each paired with what is read of its output;
    // `read` ends as what is read of the node below them.
    let mut nodes: Vec<&'a Rc<RelExpr>> = vec![rel];
    let mut levels: Vec<(Level<'a>, HashSet<ColId>)> = Vec::new();
    let mut read = live.clone();
    loop {
        let (level, input) = match nodes[nodes.len() - 1].as_ref() {
            RelExpr::Filter { input, preds } => (Level::Where(preds), input),
            RelExpr::Project { input, items } => (Level::Project(items), input),
            _ => break,
        };
        let below = level.reads(&read);
        levels.push((level, std::mem::replace(&mut read, below)));
        nodes.push(input);
    }
    let bottom = levels.len();
    let (seg, fused) = match resolve_in_place(chain, nodes[bottom])? {
        Some(seg) => (seg, bottom),
        None => {
            let cut = (bottom.saturating_sub(2)..bottom)
                .find(|&i| lowered_whole(nodes[i]))
                .unwrap_or(bottom);
            let cut_read = levels.get(cut).map_or(&read, |(_, r)| r);
            (cut_segment(chain, nodes[cut], cut_read)?, cut)
        }
    };
    levels.truncate(fused);
    levels.reverse();
    Ok(Spine { seg, levels, scatter: None })
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
        let mut cols: HashMap<ColId, u32> = self
            .seg
            .frame
            .layout
            .iter()
            .enumerate()
            .filter_map(|(slot, id)| Some(((*id)?, slot as u32)))
            .collect();
        for (level, read) in &self.levels {
            let Level::Project(items) = level else { continue };
            cols = items
                .iter()
                .filter(|it| read.contains(&it.out.id))
                .filter_map(|it| Some((it.out.id, *cols.get(&it.expr.as_col()?)?)))
                .collect();
        }
        ids.into_iter().map(|id| cols.get(&id).copied()).collect()
    }

    /// Emit the levels into `cb`. A rename-only projection relabels the frame
    /// instead of emitting a node, except the outermost one under `Top::Output`.
    pub(super) fn emit(self, cb: &mut Circuit, top: Top) -> Result<(NodeId, Frame), GnitzSqlError> {
        let Spine { seg, levels, mut scatter } = self;
        let SegInput { src, mut frame } = seg;
        let output_at = match top {
            Top::Output => levels.iter().rposition(|(l, _)| matches!(l, Level::Project(_))),
            Top::Slots => None,
        };
        let mut leading: Vec<Vec<BoundExpr>> = Vec::new();
        let mut node: Option<NodeId> = None;
        for (i, (level, read)) in levels.into_iter().enumerate() {
            match level {
                Level::Where(preds) => {
                    let preds = frame.resolve_preds(preds)?;
                    match node {
                        None => leading.push(preds),
                        Some(at) => node = Some(filter(cb, at, &preds, &frame.schema)?),
                    }
                }
                Level::Project(items) => {
                    let items: Vec<ProjEntry> = items.iter().filter(|it| read.contains(&it.out.id)).cloned().collect();
                    if let Some(rename) = Rename::of(&items).filter(|_| output_at != Some(i)) {
                        frame = frame.renamed(&rename)?;
                        continue;
                    }
                    let at = match node {
                        Some(at) => at,
                        None => source(cb, &src, &frame, std::mem::take(&mut leading), scatter.take())?,
                    };
                    let (at, out) = project_front(cb, at, &items, &frame)?;
                    node = Some(at);
                    frame = out;
                }
            }
        }
        let node = match node {
            Some(at) => at,
            None => source(cb, &src, &frame, leading, scatter.take())?,
        };
        Ok((node, frame))
    }
}

/// `src`'s delta node, co-located by `scatter` and bounded by what `leading`
/// gives a catalog source, then one filter per leading WHERE.
fn source(
    cb: &mut Circuit,
    src: &SegSource,
    frame: &Frame,
    leading: Vec<Vec<BoundExpr>>,
    scatter: Option<Vec<ReindexSlot>>,
) -> Result<NodeId, GnitzSqlError> {
    // The bound only narrows the scan; every leading conjunct is still filtered below.
    let bound = match src {
        SegSource::Segment { .. } => ReadBound::None,
        SegSource::Catalog(d) => {
            let conjuncts = leading.concat();
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
    let mut node = cb.input_scattered(src.tid(), bound, scatter);
    for preds in &leading {
        node = filter(cb, node, preds, &frame.schema)?;
    }
    Ok(node)
}

/// Lower a linear body — the inline levels of `rel`, whose outermost projection is
/// `items`, over one source. The emitted circuit carries no exchange: a filter or
/// map neither re-keys nor redistributes its source.
pub(super) fn lower_linear(
    chain: &mut ViewChain,
    rel: &Rc<RelExpr>,
    items: &[ProjEntry],
) -> Result<EmitPieces, GnitzSqlError> {
    let live = items.iter().map(|it| it.out.id).collect();
    let spine = open(chain, rel, &live)?;
    let mut cb = Circuit::default();
    let pk_repeats = spine.pk_repeats();
    let (_, out) = spine.emit(&mut cb, Top::Output)?;
    Ok(EmitPieces { circuit: cb, out, pk_repeats })
}
