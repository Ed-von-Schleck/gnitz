//! The input spine every consumer opens its inputs with: the filters and
//! projections over a relation read in place fuse into the consumer's circuit.

use super::super::chain::{admit, EmitPieces, ViewChain};
use super::super::physical::{self, Frame};
use super::super::{as_col, ColId, HirExpr, ProjEntry, RelExpr};
use super::{
    collect_live_cols, cut_segment, filter, lowered_whole, materialize, project_front, resolve_in_place, CutMemo,
    SegInput,
};
use crate::access::candidates;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{Circuit, NodeId, RelDescriptor, Schema};
use gnitz_wire::ReadBound;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;

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

/// Open `rel` for a consumer reading `live` of it. Its `Filter` / `Project` levels
/// fuse when the node below them is read in place; otherwise that node is cut with
/// whatever its shell lowers whole, and only the levels above the cut fuse.
pub(crate) fn open<'a>(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
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
    let (seg, fused) = match resolve_in_place(chain, memo, nodes[bottom])? {
        Some(seg) => (seg, bottom),
        None => {
            let cut = (bottom.saturating_sub(2)..bottom)
                .find(|&i| lowered_whole(nodes[i]))
                .unwrap_or(bottom);
            (cut_segment(chain, memo, nodes[cut], &read[cut])?, cut)
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

/// Two inputs opened side by side; under `distinct_sources` a right input reading
/// the left's source is re-read as a second relation.
pub(crate) fn open_pair<'a>(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    [left, right]: [&'a Rc<RelExpr>; 2],
    [live_l, live_r]: [&HashSet<ColId>; 2],
    distinct_sources: bool,
) -> Result<[Spine<'a>; 2], GnitzSqlError> {
    let l = open(chain, memo, left, live_l)?;
    let r = open(chain, memo, right, live_r)?;
    if distinct_sources && l.tid() == r.tid() {
        return Ok([l, Spine::segment(materialize(chain, memo, right, live_r)?)]);
    }
    Ok([l, r])
}

impl Spine<'_> {
    pub(crate) fn tid(&self) -> u64 {
        self.seg.tid
    }

    pub(crate) fn replicated(&self) -> bool {
        self.seg.desc.as_ref().is_some_and(|d| d.replicated)
    }

    /// A hidden segment read whole, with nothing fused above it.
    pub(crate) fn segment(seg: SegInput) -> Spine<'static> {
        Spine { seg, levels: Vec::new() }
    }

    /// Emit the levels into `cb`. A rename-only projection relabels the frame
    /// instead of emitting a node, except the outermost one under `Top::Output`;
    /// `what` names an emitted projection whose schema is inadmissible.
    pub(crate) fn emit(self, cb: &mut Circuit, top: Top, what: &str) -> Result<(NodeId, Frame), GnitzSqlError> {
        let Spine { seg, levels } = self;
        let SegInput { tid, mut frame, desc } = seg;
        let output_at = match top {
            Top::Output => levels.iter().rposition(|(l, _)| matches!(l, Level::Project(_))),
            Top::Slots => None,
        };
        let mut leading: Vec<Vec<BoundExpr>> = Vec::new();
        let mut node: Option<NodeId> = None;
        for (i, (level, read)) in levels.into_iter().enumerate() {
            match level {
                Level::Where(preds) => {
                    let preds = physical::resolve_preds(preds, &frame.layout)?;
                    match node {
                        None => leading.push(preds),
                        Some(at) => node = Some(filter(cb, at, &preds, &frame.schema)?),
                    }
                }
                Level::Project(items) => {
                    let items: Vec<ProjEntry> = items.iter().filter(|it| read.contains(&it.out.id)).cloned().collect();
                    let output = output_at == Some(i);
                    if !output {
                        if let Some(relabeled) = relabel(&frame, &items) {
                            frame = relabeled;
                            continue;
                        }
                    }
                    let at = match node {
                        Some(at) => at,
                        None => source(cb, tid, &desc, &frame, std::mem::take(&mut leading))?,
                    };
                    let (at, out) = project_front(cb, at, &items, &frame)?;
                    admit(&out.schema, what)?;
                    node = Some(at);
                    frame = out;
                }
            }
        }
        let node = match node {
            Some(at) => at,
            None => source(cb, tid, &desc, &frame, leading)?,
        };
        Ok((node, frame))
    }
}

/// `tid`'s delta node, bounded by what `leading` gives a catalog source, then one
/// filter per leading WHERE.
fn source(
    cb: &mut Circuit,
    tid: u64,
    desc: &Option<Arc<RelDescriptor>>,
    frame: &Frame,
    leading: Vec<Vec<BoundExpr>>,
) -> Result<NodeId, GnitzSqlError> {
    // This path compiles no residual.
    let bound = desc.as_ref().map_or(ReadBound::None, |d| {
        let conjuncts = leading.concat();
        candidates(&conjuncts, &frame.schema, &d.indexes)
            .into_iter()
            .map(|c| c.bound)
            // The cell decodes under the per-request key cap.
            .filter(|b| !matches!(b, ReadBound::PkSet(keys) if !keys.fits_one_request()))
            // A backfill never routes, and the engine may trade an index walk for the
            // full scan, which it cannot do for a PK range.
            .min_by_key(|b| match b {
                ReadBound::PkSet(_) => 0,
                ReadBound::IndexRange { .. } => 1,
                _ => 2,
            })
            .unwrap_or(ReadBound::None)
    });
    let mut node = cb.input_delta(tid, bound);
    for preds in &leading {
        node = filter(cb, node, preds, &frame.schema)?;
    }
    Ok(node)
}

/// `items` as a relabel of `frame` when each names a distinct column: such a
/// projection moves no data. A slot no item reads loses its identity.
fn relabel(frame: &Frame, items: &[ProjEntry]) -> Option<Frame> {
    let mut layout = vec![ColId::NONE; frame.layout.len()];
    let mut columns = frame.schema.columns.clone();
    for it in items {
        let slot = frame.layout.iter().position(|&c| Some(c) == as_col(&it.expr))?;
        if layout[slot] != ColId::NONE {
            return None;
        }
        layout[slot] = it.out.id;
        columns[slot] = it.out.def.clone();
    }
    Some(Frame {
        layout,
        schema: Arc::new(Schema {
            columns,
            pk_cols: frame.schema.pk_cols.clone(),
        }),
    })
}

/// Lower a linear body — the inline levels of `rel`, whose outermost projection is
/// `items`, over one source. The emitted circuit carries no exchange: a filter or
/// map neither re-keys nor redistributes its source.
pub(super) fn lower_linear(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    rel: &Rc<RelExpr>,
    items: &[ProjEntry],
) -> Result<EmitPieces, GnitzSqlError> {
    let live = items.iter().map(|it| it.out.id).collect();
    let spine = open(chain, memo, rel, &live)?;
    let mut cb = Circuit::default();
    let (node, out) = spine.emit(&mut cb, Top::Output, "view output")?;
    cb.sink(node);
    Ok(EmitPieces { circuit: cb, out })
}
