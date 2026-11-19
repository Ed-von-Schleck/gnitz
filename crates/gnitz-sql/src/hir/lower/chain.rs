//! The CREATE VIEW segment sink: the hidden segments a body's lowering cuts,
//! collected ahead of the user-named view. A segment is named by its position, which
//! commit substitutes for a real id, so lowering reaches no server.

use super::super::RelExpr;
use super::super::physical::Frame;
use super::{SegInput, SegSource};
use gnitz_core::{PlannedView, Schema, pending_id};
use gnitz_wire::Circuit;
use rustc_hash::FxHashSet;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

/// What every view emitter returns: the circuit, whose last node is its output,
/// and its output frame — the view's schema, and the layout a cut segment
/// exposes to its parent.
pub(super) struct EmitPieces {
    pub(super) circuit: Circuit,
    pub(super) out: Frame,
    /// Whether two output rows may share the frame's leading key, or one may stand
    /// at weight above 1.
    pub(super) pk_repeats: bool,
}

/// The hidden segments one CREATE VIEW body lowers to, in dependency order, and
/// the subtrees already cut to one of them.
pub(super) struct ViewChain {
    segments: Vec<PlannedView>,
    /// Each subtree a spine has cut, by `Rc`. A subtree the body reads more than
    /// once — a CTE named twice, a window input read directly and again — is cut
    /// once, with all its columns.
    pub(super) cuts: HashMap<*const RelExpr, SegInput>,
    /// The nodes of the body more than one node reads.
    shared: FxHashSet<*const RelExpr>,
}

impl ViewChain {
    /// The chain of the body `root`, which outlives it.
    pub(super) fn new(root: &RelExpr) -> ViewChain {
        fn walk(rel: &RelExpr, seen: &mut FxHashSet<*const RelExpr>, shared: &mut FxHashSet<*const RelExpr>) {
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
                    shared.insert(Rc::as_ptr(input));
                }
            }
        }
        let mut shared = FxHashSet::default();
        walk(root, &mut FxHashSet::default(), &mut shared);
        ViewChain {
            segments: Vec::new(),
            cuts: HashMap::new(),
            shared,
        }
    }

    /// Whether more than one node of the body reads `rel`.
    pub(super) fn is_shared(&self, rel: &Rc<RelExpr>) -> bool {
        self.shared.contains(&Rc::as_ptr(rel))
    }

    /// Push a hidden segment and return it as a source. Its id exists only once
    /// it is pushed, so a circuit names only segments before it.
    pub(super) fn add_segment(&mut self, p: EmitPieces) -> SegInput {
        let EmitPieces { circuit, out: frame, pk_repeats } = p;
        let src = SegSource::Segment {
            tid: pending_id(self.segments.len() as u64),
            pk_repeats,
        };
        self.segments.push(PlannedView {
            circuit,
            schema: Arc::clone(&frame.schema),
            pk_repeats,
        });
        SegInput { src, frame }
    }

    /// The segments, and the user-named view `p` under its final `schema`, which
    /// comes after every one.
    pub(super) fn finish(self, p: EmitPieces, schema: Arc<Schema>) -> (Vec<PlannedView>, PlannedView) {
        let view = PlannedView {
            circuit: p.circuit,
            schema,
            pk_repeats: p.pk_repeats,
        };
        (self.segments, view)
    }
}
