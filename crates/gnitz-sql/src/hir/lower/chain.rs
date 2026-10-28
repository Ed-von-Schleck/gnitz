//! The CREATE VIEW segment sink: the hidden segments a body's lowering cuts,
//! collected ahead of the user-named view. A segment is named by its position, which
//! commit substitutes for a real id, so lowering reaches no server.

use super::super::physical::Frame;
use super::super::RelExpr;
use super::{SegInput, SegSource};
use gnitz_core::{segment_id, PlannedView, Schema};
use gnitz_wire::Circuit;
use std::collections::HashMap;
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
#[derive(Default)]
pub(super) struct ViewChain {
    segments: Vec<PlannedView>,
    /// Each subtree bind shares as one `Rc` (a CTE or window input read through
    /// several `Alias`es), cut once.
    pub(super) cuts: HashMap<*const RelExpr, SegInput>,
}

impl ViewChain {
    /// Push a hidden segment and return it as a source. Its id exists only once
    /// it is pushed, so a circuit names only segments before it.
    pub(super) fn add_segment(&mut self, p: EmitPieces) -> SegInput {
        let EmitPieces { circuit, out: frame, pk_repeats } = p;
        let src = SegSource::Segment {
            tid: segment_id(self.segments.len() as u64),
            pk_repeats,
        };
        self.segments.push(PlannedView {
            circuit,
            schema: Arc::clone(&frame.schema),
            pk_repeats,
        });
        SegInput { src, frame }
    }

    pub(super) fn has_segments(&self) -> bool {
        !self.segments.is_empty()
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
