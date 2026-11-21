//! HIR → circuit lowering: one circuit per combine-class node and the linear
//! nodes around it, with nested combine subtrees cut to hidden segments.
//!
//! The rules every shell obeys, one home each:
//!
//! * input spine and the linear levels above a core — [`spine`];
//! * segment cut — [`ViewChain::cut`], [`ViewChain::materialize`];
//! * source collision — `join::emit_join_inputs`, the one bilinear shell;
//! * reduce layout — `Frame::group_aggs`, `Frame::keyed_by`;
//! * addressing — `physical::Frame`.
//!
//! A segment is named by its position in the chain, which commit substitutes for
//! a real id, so lowering reaches no server.

mod join;
mod reduce;
mod setop;
mod spine;

use super::physical::Frame;
use super::{ColSet, HirCol, ProjEntry, RelExpr, col_by_id};
use crate::error::GnitzSqlError;
use gnitz_core::{PlannedView, RelDescriptor, pending_id};
use gnitz_wire::Circuit;
use rustc_hash::{FxHashMap, FxHashSet};
use spine::{Head, Level, Top};
use std::rc::Rc;
use std::sync::Arc;

/// What every view emitter returns: the circuit, whose last node is its output,
/// and its output frame — the view's schema, and the layout a cut segment
/// exposes to its parent.
struct EmitPieces {
    circuit: Circuit,
    out: Frame,
    /// Whether two output rows may share the frame's leading key, or one may stand
    /// at weight above 1.
    pk_repeats: bool,
}

/// A source a spine reads, with the frame its references resolve against.
#[derive(Clone)]
struct SegInput {
    tid: u64,
    /// Whether two of the source's rows may share its leading key, or one may stand
    /// at weight above 1.
    pk_repeats: bool,
    /// The relation read in place, whose index list bounds the scan; `None` for a
    /// hidden segment, which has no catalog rows until the chain commits.
    catalog: Option<Arc<RelDescriptor>>,
    frame: Frame,
}

/// The hidden segments one CREATE VIEW body lowers to, in dependency order, and
/// the subtrees already cut to one of them.
struct ViewChain {
    segments: Vec<PlannedView>,
    /// The nodes of the body more than one node reads, by `Rc` — a CTE named twice,
    /// a window input read directly and again — each with its segment once cut.
    shared: FxHashMap<*const RelExpr, Option<SegInput>>,
}

impl ViewChain {
    /// The chain of the body `root`, which outlives it.
    fn new(root: &RelExpr) -> ViewChain {
        type Shared = FxHashMap<*const RelExpr, Option<SegInput>>;
        fn walk(rel: &RelExpr, seen: &mut FxHashSet<*const RelExpr>, shared: &mut Shared) {
            let (a, b) = match rel {
                RelExpr::Unit | RelExpr::Get { .. } => return,
                RelExpr::Filter { input, .. }
                | RelExpr::Project { input, .. }
                | RelExpr::Reduce { input, .. }
                | RelExpr::Distinct { input }
                | RelExpr::Ordered { input, .. } => (input, None),
                RelExpr::Join { left, right, .. } | RelExpr::SetOp { left, right, .. } => (left, Some(right)),
            };
            for input in std::iter::once(a).chain(b) {
                if seen.insert(Rc::as_ptr(input)) {
                    walk(input, seen, shared);
                } else {
                    shared.insert(Rc::as_ptr(input), None);
                }
            }
        }
        let mut shared = Shared::default();
        walk(root, &mut FxHashSet::default(), &mut shared);
        ViewChain { segments: Vec::new(), shared }
    }

    /// `subtree` cut to a hidden segment pruned to `live`. Every reader of a shared
    /// subtree reads all of it: its segment is one, cut on its first read.
    fn cut(&mut self, subtree: &Rc<RelExpr>, live: &ColSet) -> Result<SegInput, GnitzSqlError> {
        let key = Rc::as_ptr(subtree);
        match self.shared.get(&key) {
            None => self.materialize(subtree, live),
            Some(Some(cut)) => Ok(cut.clone()),
            Some(None) => {
                let whole: ColSet = subtree.cols().iter().map(|c| c.id).collect();
                let seg = self.materialize(subtree, &whole)?;
                self.shared.insert(key, Some(seg.clone()));
                Ok(seg)
            }
        }
    }

    /// `rel` cut to a fresh hidden segment pruned to `live`, never one [`Self::cut`]
    /// already made. A `Project` keeps its live items; a clamp, set operation, top-N
    /// or window — or anything read whole — is cut with every column and no map over
    /// it; anything else is wrapped in a projection of its live columns. Its id exists
    /// only once it is pushed, so a circuit names only segments before it.
    fn materialize(&mut self, rel: &Rc<RelExpr>, live: &ColSet) -> Result<SegInput, GnitzSqlError> {
        let body = match rel.as_ref() {
            RelExpr::Project { input, items } => {
                let narrowed: Vec<ProjEntry> = items.iter().filter(|it| live.contains(it.out.id)).cloned().collect();
                match narrowed.len() == items.len() {
                    true => Rc::clone(rel),
                    false => RelExpr::project(Rc::clone(input), narrowed),
                }
            }
            RelExpr::Distinct { .. } | RelExpr::SetOp { .. } | RelExpr::Ordered { .. } => Rc::clone(rel),
            _ if rel.cols().iter().all(|c| live.contains(c.id)) => Rc::clone(rel),
            // The HIR spelling of `SELECT <live cols> FROM <rel>`.
            _ => {
                let items = ProjEntry::passthroughs(rel.cols().iter().filter(|c| live.contains(c.id)).cloned());
                RelExpr::project(Rc::clone(rel), items)
            }
        };
        let EmitPieces { circuit, out: frame, pk_repeats } = lower_body(self, &body)?;
        let tid = pending_id(self.segments.len() as u64);
        self.segments.push(PlannedView {
            circuit,
            schema: Arc::clone(&frame.schema),
            pk_repeats,
        });
        Ok(SegInput { tid, pk_repeats, catalog: None, frame })
    }
}

/// Lower a bound view body to its segments and the user-named view, which comes
/// after every one. The view lists its visible columns in `names` order, each under
/// the name its column of `names` gives it: the final frame's regions under the
/// SELECT list's numbering.
pub(crate) fn lower(rel: Rc<RelExpr>, names: &[HirCol]) -> Result<(Vec<PlannedView>, PlannedView), GnitzSqlError> {
    // A top-N carries its input's columns key first: the SELECT list is a
    // projection over it. A window body always ends in a projection.
    let rel = match rel.as_ref() {
        RelExpr::Ordered { .. } => RelExpr::project(Rc::clone(&rel), ProjEntry::passthroughs(rel.cols().into_owned())),
        _ => rel,
    };
    let mut chain = ViewChain::new(&rel);
    let top = lower_body(&mut chain, &rel)?;
    let visible = names.iter().filter(|c| !c.def.is_hidden).map(|c| c.id);
    let name = |id| col_by_id(names, id).map(|c| c.def.name.clone());
    let (schema, _) = top.out.schema_in_order(visible, name)?;
    let view = PlannedView {
        circuit: top.circuit,
        schema,
        pk_repeats: top.pk_repeats,
    };
    Ok((chain.segments, view))
}

/// Lower a complete view body to circuit pieces: its linear levels over one
/// combine, in that combine's circuit, or over one source.
fn lower_body(chain: &mut ViewChain, rel: &Rc<RelExpr>) -> Result<EmitPieces, GnitzSqlError> {
    let live: ColSet = match rel.as_ref() {
        RelExpr::Project { items, .. } => items.iter().map(|it| it.out.id).collect(),
        _ => rel.cols().iter().map(|c| c.id).collect(),
    };
    let peeled = spine::peel(chain, rel, &live);
    let core = peeled.bottom;
    // The bottom node is this body's own where it is under no shared subtree.
    let (mut pieces, top) = match (peeled.cut.0 == 0, core.as_ref()) {
        (true, RelExpr::Join { left, right, kind, on }) => {
            return join::lower_join_view(chain, peeled, [left, right], *kind, on);
        }
        // A reduce read only through renames stays the circuit's output, whose store
        // holds its history, so its rows are not copied into a second one: the
        // columns no level names are stored hidden beside the others.
        (true, RelExpr::Reduce { input, group_cols, aggs, kind }) => {
            let renames = peeled.levels.iter().all(|(l, _)| matches!(l, Level::Project(_)));
            let pieces = reduce::lower_reduce(chain, input, group_cols, aggs, *kind)?;
            (pieces, if renames { Top::Store } else { Top::Output })
        }
        (true, RelExpr::Ordered { input, partition, order, op }) => {
            (reduce::lower_ordered(chain, input, partition, order, op)?, Top::Output)
        }
        (true, RelExpr::Distinct { .. } | RelExpr::SetOp { .. }) => (setop::lower_setop(chain, core)?, Top::Output),
        // A linear body: the levels over its one source. The circuit carries no
        // exchange: a filter or map neither re-keys nor redistributes its source.
        _ => {
            let spine = peeled.open(chain)?;
            let pk_repeats = spine.seg.pk_repeats;
            let mut circuit = Circuit::default();
            let (_, out) = spine.emit(&mut circuit, Top::Output, None)?;
            return Ok(EmitPieces { circuit, out, pk_repeats });
        }
    };
    // The body's circuit ends in its core node, which the levels read.
    let node = Head::Node(pieces.circuit.out());
    pieces.out = spine::emit_levels(&mut pieces.circuit, &peeled.levels, top, pieces.out, node)?.1;
    Ok(pieces)
}
