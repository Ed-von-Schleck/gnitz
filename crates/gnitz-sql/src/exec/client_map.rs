//! A compiled projection run client-side over a batch: the finalize map of an ad-hoc
//! fold, and an `INSERT … RETURNING` list over the rows it wrote.

use std::sync::Arc;

use gnitz_core::{PayloadColumn, Schema, ZSetBatch};
use gnitz_expr::{ColumnLocator, LogicalProgram, MapEval};

use crate::error::GnitzSqlError;

/// A projection program resolved over a source schema, and the output schema it fills:
/// the source's key, then what the program writes.
pub(crate) struct ClientMap {
    out_schema: Arc<Schema>,
    ev: MapEval,
    /// Some computed slot is a German string, whose cell may spill into the output heap.
    computes_a_string: bool,
}

impl ClientMap {
    pub(crate) fn new(program: LogicalProgram, src: &Schema, out_schema: Arc<Schema>) -> Result<Self, GnitzSqlError> {
        let key = |s: &Schema| {
            s.pk_cols
                .iter()
                .map(|&c| s.columns[c as usize].ty.tc)
                .collect::<Vec<_>>()
        };
        if key(src) != key(&out_schema) {
            return Err(GnitzSqlError::Internal(
                "a client map must keep its source's key".into(),
            ));
        }
        let ev = program.resolve_map(src, out_schema.as_ref())?;
        if ev.copies().iter().any(|c| c.src.size() != c.width) {
            return Err(GnitzSqlError::Internal("a projection copies a promoted column".into()));
        }
        let computes_a_string = out_schema
            .payload_columns()
            .any(|(pi, _, c)| c.ty.tc.is_german_string() && !ev.copies().iter().any(|k| k.slot == pi));
        Ok(ClientMap { out_schema, ev, computes_a_string })
    }

    pub(crate) fn out_schema(&self) -> &Arc<Schema> {
        &self.out_schema
    }

    /// The map over every row of `src`, each row keeping its PK and weight.
    pub(crate) fn apply(&mut self, src: ZSetBatch) -> ZSetBatch {
        if self.ev.is_identity() {
            return src;
        }
        let n = src.len();
        let mut out = ZSetBatch::new(&self.out_schema);
        out.nulls = vec![0; n];
        out.payload = ZSetBatch::filler_columns(&self.out_schema, n);
        if self.computes_a_string {
            // Appended to, so copied: a copied German cell keeps its offset into it.
            out.blob = src.blob.clone();
        }
        self.ev.write_computed(&src, 0, n, &mut out, 0);
        // A key column's copy decodes it out of the key region; a key is never NULL.
        let mut moves = Vec::new();
        for c in self.ev.copies() {
            match c.src {
                ColumnLocator::Payload { slot, .. } => moves.push((c.slot, slot as usize)),
                ColumnLocator::Pk { byte_off, type_code, .. } => gnitz_wire::decode_pk_cells(
                    src.pks.region(),
                    src.pks.stride(),
                    byte_off as usize,
                    c.width,
                    type_code.is_signed_int(),
                    &mut out.payload[c.slot].bytes,
                ),
            }
        }
        // The evaluator is done with `src`; its parts move into the output.
        let ZSetBatch { pks, weights, mut payload, blob, .. } = src;
        move_payload(&mut out.payload, &mut payload, &moves);
        if !self.computes_a_string {
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
