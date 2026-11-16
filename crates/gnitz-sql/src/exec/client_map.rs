//! A compiled projection run client-side over a batch: the finalize map of an ad-hoc
//! fold, a FROM-less SELECT's constant row, and an `INSERT … RETURNING` list over the
//! rows it wrote.

use std::sync::Arc;

use gnitz_core::{PayloadColumn, Schema, ZSetBatch};
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
    pub(crate) fn apply(&mut self, src: ZSetBatch) -> ZSetBatch {
        let Some(ev) = &mut self.ev else {
            return src;
        };
        let n = src.len();
        let mut out = ZSetBatch::new(&self.out_schema);
        out.nulls = vec![0; n];
        // A slot copied from a source payload region takes that region whole in
        // `move_payload`; every other slot is zero-filled for the evaluator or the key
        // decode to write into.
        let moves: Vec<(usize, usize)> = ev
            .moves()
            .copies()
            .iter()
            .filter_map(|c| match c.src {
                ColumnLocator::Payload { slot, .. } => Some((c.slot, slot as usize)),
                ColumnLocator::Pk { .. } => None,
            })
            .collect();
        let slots = out.payload.iter_mut().zip(self.out_schema.payload_columns());
        for (slot, (col, (_, _, def))) in slots.enumerate() {
            if moves.iter().all(|&(to, _)| to != slot) {
                col.bytes = vec![0; n * def.ty.tc.wire_stride()];
            }
        }
        // A copied German cell keeps its offset into the source arena, and a computed
        // string is re-encoded into the output's: the source arena survives only when a
        // copy reads it, and is cloned to start the output's when computed strings follow.
        let (computes_a_string, copies_a_string) = (ev.emits_a_string(), ev.moves().string_slots() != 0);
        if computes_a_string && copies_a_string {
            out.blob = src.blob.clone();
        }
        ev.write_computed(&src, 0, n, &mut out, 0);
        // A key column's copy decodes it out of the key region; a key is never NULL.
        for c in ev.moves().copies() {
            if let ColumnLocator::Pk { byte_off, type_code, .. } = c.src {
                gnitz_wire::decode_pk_cells(
                    src.pks.region(),
                    src.pks.stride(),
                    byte_off as usize,
                    c.width,
                    type_code.is_signed_int(),
                    &mut out.payload[c.slot].bytes,
                );
            }
        }
        // The evaluator is done with `src`; the parts the output reads move into it.
        let ZSetBatch { pks, weights, mut payload, blob, .. } = src;
        move_payload(&mut out.payload, &mut payload, &moves);
        if copies_a_string && !computes_a_string {
            out.blob = blob;
        }
        out.pks = pks;
        out.weights = weights;
        out
    }
}

/// Each `(to, from)` payload region of `src` into `dst`: moved, or cloned while a
/// later pair still reads it. The destination keeps its own type.
fn move_payload(dst: &mut [PayloadColumn], src: &mut [PayloadColumn], moves: &[(usize, usize)]) {
    for (pos, &(to, from)) in moves.iter().enumerate() {
        let read_again = moves[pos + 1..].iter().any(|&(_, f)| f == from);
        dst[to].bytes = if read_again {
            src[from].bytes.clone()
        } else {
            std::mem::take(&mut src[from].bytes)
        };
    }
}

#[cfg(test)]
#[path = "tests/client_map.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/client_map.rs"]
mod bench;
