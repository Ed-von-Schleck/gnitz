//! A compiled projection run client-side over a batch: the finalize map of an ad-hoc
//! fold, a FROM-less SELECT's constant row, and an `INSERT … RETURNING` list over the
//! rows it wrote.

use std::sync::Arc;

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::LogicalProgram;
use gnitz_zset::algebra::MapPlan;

use crate::error::GnitzSqlError;
use crate::hir::RowsReply;

/// A projection program resolved over a source schema, and the output schema it fills.
/// The output keeps its source's key region; its column list may name those key columns
/// anywhere.
pub(crate) struct ClientMap {
    src: Arc<Schema>,
    out_schema: Arc<Schema>,
    /// `None` when the plan shipped no program: the output is the source's own regions.
    plan: Option<MapPlan>,
}

impl ClientMap {
    /// `rows` as the client runs them over their source, and their ORDER BY over the
    /// output.
    pub(crate) fn over(rows: RowsReply) -> Result<(Self, Vec<gnitz_wire::OrderKey>), GnitzSqlError> {
        let RowsReply { src, schema, program, order, .. } = rows;
        Ok((ClientMap::new(program, src, schema)?, order))
    }

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
            .map(|p| p.resolve_map(src.layout(), out_schema.layout()))
            .transpose()?
            .map(|ev| MapPlan::inheriting(ev, *out_schema.layout()));
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

    pub(crate) fn out_schema(&self) -> &Arc<Schema> {
        &self.out_schema
    }

    /// The map over every row of `src`, each row keeping its PK and weight.
    pub(crate) fn apply(&mut self, src: ZSetBatch) -> ZSetBatch {
        match &mut self.plan {
            None => src,
            Some(plan) => plan.evaluate_map_batch(&src.view()).into(),
        }
    }
}

#[cfg(test)]
#[path = "tests/client_map.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/client_map.rs"]
mod bench;
