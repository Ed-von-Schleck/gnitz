//! A compiled projection run client-side over a batch: the finalize map of an ad-hoc
//! fold, a FROM-less SELECT's constant row, and an `INSERT … RETURNING` list over the
//! rows it wrote.

use std::sync::Arc;

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::LogicalProgram;
use gnitz_zset::algebra::MapPlan;

use crate::error::GnitzSqlError;

/// A projection program resolved over a source schema, and the output schema it fills.
/// The output keeps its source's key region; its column list may name those key columns
/// anywhere.
#[derive(Clone)]
pub(crate) struct ClientMap {
    src: Arc<Schema>,
    out_schema: Arc<Schema>,
    /// `None` when the plan shipped no program: the output is the source's own regions.
    plan: Option<MapPlan>,
}

impl ClientMap {
    /// `program` from `src` onto `out_schema`: a [`crate::hir::RowsReply`]'s, as the client
    /// runs it over its source.
    pub(crate) fn new(
        program: Option<LogicalProgram>,
        src: Arc<Schema>,
        out_schema: Arc<Schema>,
    ) -> Result<Self, GnitzSqlError> {
        debug_assert!(
            src.layout().same_key_types(out_schema.layout()),
            "a client map keeps its source's key"
        );
        let plan = program
            .map(|p| MapPlan::inheriting(p, src.layout(), *out_schema.layout()))
            .transpose()?;
        // Each copy keeps its source's width: only a set operation's common type widens a
        // column, and an ad-hoc read has none.
        debug_assert!(
            plan.iter().flat_map(MapPlan::copies).all(|c| c.src.size() == c.width),
            "a client map copies each column at its own width"
        );
        Ok(ClientMap { src, out_schema, plan })
    }

    /// The schema of the batches the map reads.
    pub(crate) fn src(&self) -> &Arc<Schema> {
        &self.src
    }

    /// The map over every row of `src`, each row keeping its PK and weight.
    pub(crate) fn apply(&mut self, src: ZSetBatch) -> ZSetBatch {
        match &mut self.plan {
            None => src.with_schema(Arc::clone(&self.out_schema)),
            Some(plan) => ZSetBatch::of(&self.out_schema, plan.evaluate_map_batch(&src.view())),
        }
    }
}

#[cfg(test)]
#[path = "tests/client_map.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/client_map.rs"]
mod bench;
