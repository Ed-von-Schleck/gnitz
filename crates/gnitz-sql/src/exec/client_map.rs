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
    /// `None` when the output is the source's own layout: no program, or one that
    /// reproduces its input.
    ev: Option<MapEval>,
}

impl ClientMap {
    pub(crate) fn new(
        program: Option<LogicalProgram>,
        src: &Schema,
        out_schema: Arc<Schema>,
    ) -> Result<Self, GnitzSqlError> {
        let key = |s: &Schema| {
            s.pk_cols()
                .iter()
                .map(|&c| s.columns()[c as usize].ty.tc)
                .collect::<Vec<_>>()
        };
        debug_assert_eq!(key(src), key(&out_schema), "a client map keeps its source's key");
        let ev = program
            .map(|p| p.resolve_map(src.layout(), out_schema.layout()))
            .transpose()?
            .filter(|ev| !ev.is_identity());
        if ev.iter().flat_map(|ev| ev.copies()).any(|c| c.src.size() != c.width) {
            return Err(GnitzSqlError::Internal("a projection copies a promoted column".into()));
        }
        Ok(ClientMap { out_schema, ev })
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
        out.payload = ZSetBatch::filler_columns(&self.out_schema, n);
        let computes_a_string = ev.emits_a_string();
        if computes_a_string {
            // Appended to, so copied: a copied German cell keeps its offset into it.
            out.blob = src.blob.clone();
        }
        ev.write_computed(&src, 0, n, &mut out, 0);
        // A key column's copy decodes it out of the key region; a key is never NULL.
        let mut moves = Vec::new();
        for c in ev.copies() {
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
        if !computes_a_string {
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
