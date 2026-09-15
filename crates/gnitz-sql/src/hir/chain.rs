//! The CREATE VIEW segment sink: `ViewChain` collects the hidden segments the
//! HIR lowering cuts (in dependency order) plus the user-named final view, and
//! commits them as one atomic `create_view_chain` bundle. `EmitPieces` is the
//! per-segment emit product.

use super::lower::SegInput;
use super::physical::Frame;
use crate::error::GnitzSqlError;
use gnitz_core::{segment_id, Circuit, PlannedView, Schema};
use std::sync::Arc;

/// What every view emitter returns: the circuit and its output frame, whose
/// schema is the view's and whose layout is what a cut segment exposes to its
/// parent.
pub(crate) struct EmitPieces {
    pub circuit: Circuit,
    pub out: Frame,
}

/// `Schema::validate_parts`, with a rejection naming the stage `what`.
pub(crate) fn admit(schema: &Schema, what: &str) -> Result<(), GnitzSqlError> {
    Schema::validate_parts(&schema.pk_cols, &schema.columns)
        .map_err(|e| GnitzSqlError::Unsupported(format!("{what}: {e}")))
}

/// The in-flight CREATE VIEW bundle: the hidden segments compiled so far plus
/// the user-named final view, which every hidden segment names as its owner (the
/// DROP-cascade convention). Ends as one atomic `create_view_chain` bundle
/// (hiddens then final).
///
/// A segment is named by its position, as a symbolic id substituted for a real
/// one at commit. Compiling a body therefore reaches no server and repeats
/// exactly, which the resolve loop needs to re-run a pass.
pub(crate) struct ViewChain {
    pub(crate) segments: Vec<PlannedView>,
}

impl ViewChain {
    pub(crate) fn new() -> Self {
        ViewChain { segments: Vec::new() }
    }

    /// Push one emitted circuit: the one path every circuit, hidden or final,
    /// reaches — so no emitter has to remember the checks, and none can be added
    /// that escapes them.
    fn push(
        &mut self,
        circuit: Circuit,
        schema: Schema,
        capacity_bytes: Option<u64>,
        delta_bytes: Option<u64>,
        what: &str,
    ) -> Result<(), GnitzSqlError> {
        admit(&schema, what)?;
        self.segments.push(PlannedView {
            circuit,
            output_columns: schema.columns,
            pk_cols: schema.pk_cols,
            capacity_bytes,
            delta_bytes,
        });
        Ok(())
    }

    /// Add one hidden segment: run `emit` (the emitter may push its own upstream
    /// segments first — it gets `self` back), push the emitted pieces, and return
    /// the segment as a `SegInput`.
    ///
    /// The segment's id is handed out only after its push, so a circuit can only
    /// name segments pushed before it: the chain lands in dependency order.
    pub(crate) fn add_segment(
        &mut self,
        emit: impl FnOnce(&mut ViewChain) -> Result<EmitPieces, GnitzSqlError>,
    ) -> Result<SegInput, GnitzSqlError> {
        let EmitPieces { circuit, out } = emit(self)?;
        // Capacity and delta feeds belong to the user-named view alone.
        self.push(circuit, Schema::clone(&out.schema), None, None, "view segment output")?;
        Ok(SegInput {
            tid: segment_id(self.segments.len() as u64 - 1),
            frame: out,
            // A chain-minted id, not a catalog one: no kind, no index bound.
            desc: None,
        })
    }

    /// Push the user-named view, always the chain's last element.
    pub(crate) fn push_final(
        &mut self,
        pieces: EmitPieces,
        capacity_bytes: Option<u64>,
        delta_bytes: Option<u64>,
    ) -> Result<(), GnitzSqlError> {
        let EmitPieces { circuit, out } = pieces;
        self.push(
            circuit,
            Arc::unwrap_or_clone(out.schema),
            capacity_bytes,
            delta_bytes,
            "view output",
        )
    }
}
