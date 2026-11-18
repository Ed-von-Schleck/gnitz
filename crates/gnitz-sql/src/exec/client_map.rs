//! A compiled projection run client-side over a batch: the finalize map of an ad-hoc
//! fold, a FROM-less SELECT's constant row, and an `INSERT … RETURNING` list over the
//! rows it wrote.

use std::sync::Arc;

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::{ColumnLocator, LogicalProgram, MapEval};

use crate::error::GnitzSqlError;
use crate::hir::RowsReply;

/// A projection program resolved over a source schema, and the output schema it fills.
/// The output keeps its source's key region; its column list may name those key columns
/// anywhere.
pub(crate) struct ClientMap {
    src: Arc<Schema>,
    out_schema: Arc<Schema>,
    /// `None` when the plan shipped no program: the output is the source's own regions.
    ev: Option<MapEval>,
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
        let key = |s: &Schema| s.layout().pk_columns().map(|c| c.type_code).collect::<Vec<_>>();
        debug_assert_eq!(key(&src), key(&out_schema), "a client map keeps its source's key");
        let ev = program
            .map(|p| p.resolve_map(src.layout(), out_schema.layout()))
            .transpose()?;
        // Each copy keeps its source's width: only a set operation's common type widens a
        // column, and an ad-hoc read has none.
        debug_assert!(
            ev.iter()
                .flat_map(|ev| ev.moves().copies())
                .all(|c| c.src.size() == c.width),
            "a client map copies each column at its own width"
        );
        Ok(ClientMap { src, out_schema, ev })
    }

    /// The schema of the batches the map reads.
    pub(crate) fn src(&self) -> &Arc<Schema> {
        &self.src
    }

    pub(crate) fn out_schema(&self) -> &Arc<Schema> {
        &self.out_schema
    }

    /// The map over every row of `src`, each row keeping its PK and weight.
    pub(crate) fn apply(&mut self, mut src: ZSetBatch) -> ZSetBatch {
        let Some(ev) = &mut self.ev else {
            return src;
        };
        let n = src.len();
        // Every slot starts zero for the evaluator, the key decode or a copy to write into.
        let mut out = ZSetBatch::zeroed(&self.out_schema, n);
        out.copy_keys_and_weights(&src);
        // A copied German cell keeps its offset into the source heap, and a computed
        // string is re-encoded into the output's: the source heap survives only when a
        // copy reads it, and is cloned to start the output's when computed strings follow.
        let (computes_a_string, copies_a_string) = (ev.emits_a_string(), ev.moves().string_slots() != 0);
        if computes_a_string && copies_a_string {
            out.set_heap(src.heap().to_vec());
        }
        ev.write_computed(&src, 0, n, &mut out, 0);
        // A key column's copy decodes it out of the key region; a key is never NULL.
        for c in ev.moves().copies() {
            match c.src {
                ColumnLocator::Pk { byte_off, type_code, .. } => gnitz_wire::decode_pk_cells(
                    src.pk_region(),
                    src.pk_stride(),
                    byte_off as usize,
                    type_code,
                    out.col_mut(c.slot),
                ),
                ColumnLocator::Payload { slot, .. } => out.col_mut(c.slot).copy_from_slice(src.col(slot as usize)),
            }
        }
        if copies_a_string && !computes_a_string {
            out.set_heap(src.take_heap());
        }
        out
    }
}

#[cfg(test)]
#[path = "tests/client_map.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/client_map.rs"]
mod bench;
