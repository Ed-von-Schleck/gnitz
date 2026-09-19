//! A compiled projection run client-side over a batch: the finalize map of an ad-hoc
//! fold, and an `INSERT … RETURNING` list over the rows it wrote.

use std::sync::Arc;

use gnitz_core::{PayloadColumn, Schema, ZSetBatch};
use gnitz_expr::{ColumnLocator, Evaluator, LogicalProgram};

use crate::error::GnitzSqlError;

/// A projection program resolved over a source schema, and the output schema it fills:
/// the source's key, then what the program writes.
pub(crate) struct ClientMap {
    out_schema: Arc<Schema>,
    ev: Evaluator,
    /// The map's verbatim column copies as `(output payload slot, source column)`: a payload
    /// column moves its region whole, a PK column is decoded row by row.
    copies: Vec<(usize, ColumnLocator)>,
    /// The map reproduces its input.
    identity: bool,
}

impl ClientMap {
    pub(crate) fn new(program: LogicalProgram, src: &Schema, out_schema: Arc<Schema>) -> Result<Self, GnitzSqlError> {
        let key = |s: &Schema| {
            s.pk_cols
                .iter()
                .map(|&c| s.columns[c as usize].type_code)
                .collect::<Vec<_>>()
        };
        if key(src) != key(&out_schema) {
            return Err(GnitzSqlError::Internal(
                "a client map must keep its source's key".into(),
            ));
        }
        let identity = program.is_identity_map(src, out_schema.as_ref());
        let ev = program.resolve_map(src, out_schema.as_ref())?;
        let copies = ev
            .copies()
            .iter()
            .map(|&(loc, out, width)| {
                if loc.size() == width as usize {
                    Ok((out as usize, loc))
                } else {
                    Err(GnitzSqlError::Internal("a projection copies a promoted column".into()))
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(ClientMap { out_schema, ev, copies, identity })
    }

    pub(crate) fn out_schema(&self) -> &Arc<Schema> {
        &self.out_schema
    }

    /// Whether the map reproduces its input, so [`Self::apply`] hands the batch back.
    #[cfg(test)]
    pub(crate) fn is_identity(&self) -> bool {
        self.identity
    }

    /// The map over every row of `src`, each row keeping its PK and weight.
    pub(crate) fn apply(&self, src: ZSetBatch) -> ZSetBatch {
        if self.identity {
            return src;
        }
        let (ev, n) = (&self.ev, src.len());
        let mut out = ZSetBatch::new(&self.out_schema);
        out.nulls = vec![0; n];
        let str_emits = !ev.str_emits().is_empty();
        if str_emits {
            // Appended to, so copied: a copied German cell keeps its offset into it.
            out.blob = src.blob.clone();
        }
        for &(_, pi, stride) in ev.scalar_emits() {
            out.payload[pi as usize].bytes = vec![0; n * stride as usize];
        }
        for &(_, pi) in ev.str_emits() {
            out.payload[pi as usize].bytes = vec![0; n * 16];
        }
        {
            let ZSetBatch { nulls, payload, blob, .. } = &mut out;
            let nb = gnitz_wire::as_le_bytes_mut(nulls);
            ev.null_perm()
                .write_rows(gnitz_wire::as_le_bytes(&src.nulls), 0, nb, 0, n);
            if ev.emits_anything() {
                ev.eval_morsels(&src, 0, n, |row0, mo| {
                    for &(reg, pi, stride) in ev.scalar_emits() {
                        mo.emit_scalar_cells(
                            reg as usize,
                            &mut payload[pi as usize].bytes,
                            nb,
                            row0,
                            pi as usize,
                            stride as usize,
                        );
                    }
                    for &(reg, pi) in ev.str_emits() {
                        mo.emit_str_cells(
                            reg as usize,
                            &mut payload[pi as usize].bytes,
                            nb,
                            blob,
                            row0,
                            pi as usize,
                        );
                    }
                });
            }
        }
        // A key column's copy decodes it out of the key region; a key is never NULL.
        let mut moves = Vec::with_capacity(self.copies.len());
        for &(to, loc) in &self.copies {
            match loc {
                ColumnLocator::Payload { slot, .. } => moves.push((to, slot as usize)),
                ColumnLocator::Pk { .. } => {
                    let mut bytes = Vec::with_capacity(n * loc.size());
                    let mut scratch = [0u8; 16];
                    for r in 0..n {
                        bytes.extend_from_slice(loc.native_le_bytes(&src, r, &mut scratch));
                    }
                    out.payload[to].bytes = bytes;
                }
            }
        }
        // The evaluator is done with `src`; its parts move into the output.
        let ZSetBatch { pks, weights, mut payload, blob, .. } = src;
        move_payload(&mut out.payload, &mut payload, &moves);
        if !str_emits {
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
