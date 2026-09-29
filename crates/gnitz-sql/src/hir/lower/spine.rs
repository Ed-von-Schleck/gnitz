//! The input spine every consumer opens its inputs with: the filters and
//! projections over a relation read in place fuse into the consumer's circuit.

use super::super::physical::{Frame, Rename};
use super::super::{as_col, ColId, HirExpr, ProjEntry, RelExpr};
use super::{
    collect_live_cols, cut_segment, filter, lowered_whole, project_front, resolve_in_place, EmitPieces, SegInput,
    SegSource, ViewChain,
};
use crate::access::candidates;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{Circuit, NodeId, ReindexRole, ReindexSlot};
use gnitz_wire::ReadBound;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// A single input as its consumer's circuit reads it: a source — a relation read
/// in place, or a hidden segment — under the inline `Filter` / `Project` levels
/// fused above it, innermost first, each with the columns read of it.
pub(crate) struct Spine<'a> {
    seg: SegInput,
    levels: Vec<(Level<'a>, HashSet<ColId>)>,
}

enum Level<'a> {
    Where(&'a [HirExpr]),
    Project(&'a [ProjEntry]),
}

/// What the consumer needs of the top projection: its exact columns (a view's
/// output, a top-N carrying every input column), or only a slot per column it
/// reads (a reduce, a content hash).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Top {
    Output,
    Slots,
}

/// Where an opened input's columns come from in the relation the master
/// scatters: the source, the source column behind each surviving column, and the
/// source PK, which the levels carry verbatim at the front of the key region.
pub(crate) struct SourceOrigin {
    pub(super) src: SegSource,
    /// Keyed by the id a column carries above the fused levels; a computed
    /// column is absent.
    cols: HashMap<ColId, u32>,
    pk: Vec<u32>,
}

impl SourceOrigin {
    /// True iff `id` names a verbatim copy of a source column.
    pub(crate) fn resolves(&self, id: ColId) -> bool {
        self.cols.contains_key(&id)
    }

    /// Slot `slot` of `frame` as a column of the source relation.
    fn slot(&self, frame: &Frame, slot: usize) -> Option<u32> {
        let by_id = frame
            .layout
            .get(slot)
            .copied()
            .flatten()
            .and_then(|id| self.cols.get(&id))
            .copied();
        // An auto-prepended PK column carries no id to look up.
        let by_pk = frame
            .schema
            .pk_cols
            .iter()
            .position(|&c| c as usize == slot)
            .and_then(|j| self.pk.get(j))
            .copied();
        debug_assert!(
            by_id.is_none() || by_pk.is_none() || by_id == by_pk,
            "slot {slot} resolves to source column {by_id:?} by id and {by_pk:?} by PK position",
        );
        by_id.or(by_pk)
    }

    /// `key`, restated over the source relation. Each slot keeps its type.
    pub(crate) fn scatter_role(&self, frame: &Frame, key: &[ReindexSlot]) -> Option<ReindexRole> {
        let source_key = key
            .iter()
            .map(|&(c, tc)| Some((self.slot(frame, c as usize)?, tc)))
            .collect::<Option<Vec<ReindexSlot>>>()?;
        Some(ReindexRole::ScatterKey { source: self.src.tid(), source_key })
    }
}

/// Open `rel` for a consumer reading `live` of it. Its `Filter` / `Project` levels
/// fuse when the node below them is read in place; otherwise that node is cut with
/// whatever its shell lowers whole, and only the levels above the cut fuse.
pub(crate) fn open<'a>(
    chain: &mut ViewChain,
    rel: &'a Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<Spine<'a>, GnitzSqlError> {
    let mut nodes: Vec<&'a Rc<RelExpr>> = vec![rel];
    let mut last: &'a Rc<RelExpr> = rel;
    while let RelExpr::Project { input, .. } | RelExpr::Filter { input, .. } = last.as_ref() {
        nodes.push(input);
        last = input;
    }
    let bottom = nodes.len() - 1;
    // What is read of each node, top-down.
    let mut read = vec![live.clone()];
    for node in &nodes[..bottom] {
        let above = &read[read.len() - 1];
        let mut below = HashSet::new();
        match node.as_ref() {
            RelExpr::Filter { preds, .. } => {
                below.extend(above.iter().copied());
                collect_live_cols(preds, &mut below);
            }
            RelExpr::Project { items, .. } => collect_live_cols(
                items.iter().filter(|it| above.contains(&it.out.id)).map(|it| &it.expr),
                &mut below,
            ),
            _ => unreachable!("only a Filter or Project was peeled"),
        }
        read.push(below);
    }
    let (seg, fused) = match resolve_in_place(chain, nodes[bottom])? {
        Some(seg) => (seg, bottom),
        None => {
            let cut = (bottom.saturating_sub(2)..bottom)
                .find(|&i| lowered_whole(nodes[i]))
                .unwrap_or(bottom);
            (cut_segment(chain, nodes[cut], &read[cut])?, cut)
        }
    };
    let levels = nodes[..fused]
        .iter()
        .copied()
        .zip(read)
        .rev()
        .map(|(node, read)| {
            let level = match &**node {
                RelExpr::Filter { preds, .. } => Level::Where(preds),
                RelExpr::Project { items, .. } => Level::Project(items),
                _ => unreachable!("only a Filter or Project was peeled"),
            };
            (level, read)
        })
        .collect();
    Ok(Spine { seg, levels })
}

impl Spine<'_> {
    pub(crate) fn tid(&self) -> u64 {
        self.seg.src.tid()
    }

    /// The opened source's, since no fused level re-keys.
    pub(crate) fn pk_repeats(&self) -> bool {
        self.seg.src.pk_repeats()
    }

    /// A hidden segment read whole, with nothing fused above it.
    pub(crate) fn segment(seg: SegInput) -> Spine<'static> {
        Spine { seg, levels: Vec::new() }
    }

    /// Where this spine's columns come from in the opened relation. A bare
    /// column reference copies its column verbatim, whether [`Self::emit`]
    /// relabels it or emits it; anything else ends that column's chain.
    pub(crate) fn origin(&self) -> SourceOrigin {
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
                .filter_map(|it| Some((it.out.id, *cols.get(&as_col(&it.expr)?)?)))
                .collect();
        }
        SourceOrigin {
            src: self.seg.src.clone(),
            cols,
            pk: self.seg.frame.schema.pk_cols.clone(),
        }
    }

    /// True iff a key over `keys` can be stated over the opened relation.
    pub(crate) fn keys_reach_source(&self, keys: impl Iterator<Item = ColId>) -> bool {
        let origin = self.origin();
        keys.into_iter().all(|id| origin.resolves(id))
    }

    /// Emit the levels into `cb`. A rename-only projection relabels the frame
    /// instead of emitting a node, except the outermost one under `Top::Output`.
    pub(crate) fn emit(self, cb: &mut Circuit, top: Top) -> Result<(NodeId, Frame), GnitzSqlError> {
        let Spine { seg, levels } = self;
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
                        None => source(cb, &src, &frame, std::mem::take(&mut leading))?,
                    };
                    let (at, out) = project_front(cb, at, &items, &frame)?;
                    node = Some(at);
                    frame = out;
                }
            }
        }
        let node = match node {
            Some(at) => at,
            None => source(cb, &src, &frame, leading)?,
        };
        Ok((node, frame))
    }
}

/// `src`'s delta node, bounded by what `leading` gives a catalog source, then one
/// filter per leading WHERE.
fn source(
    cb: &mut Circuit,
    src: &SegSource,
    frame: &Frame,
    leading: Vec<Vec<BoundExpr>>,
) -> Result<NodeId, GnitzSqlError> {
    // This path compiles no residual.
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
                    ReadBound::Range(r) if !r.walks_pk(&frame.schema.pk_cols) => 1,
                    _ => 2,
                })
                .unwrap_or(ReadBound::None)
        }
    };
    let mut node = cb.input_delta(src.tid(), bound);
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
    let (node, out) = spine.emit(&mut cb, Top::Output)?;
    Ok(EmitPieces { circuit: cb, top: node, out, pk_repeats })
}
