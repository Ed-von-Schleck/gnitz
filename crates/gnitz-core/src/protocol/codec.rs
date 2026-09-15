//! The client's two adapters to the shared meta-schema block codec
//! (`gnitz_wire::schema_block`): `Schema` → block bytes, block bytes →
//! `Schema`. The block's layout and every rule about what makes one admissible
//! live in `gnitz-wire`, so the engine cannot enforce a different set.

use super::error::ProtocolError;
use super::types::{type_code_from_u64, ColType, ColumnDef, Schema};
use gnitz_wire::schema_block::{ColMeta, SchemaBlock, SchemaBlockCol};
use gnitz_wire::PK_LIST_MAX_COLS;
use std::sync::Arc;

/// Encode the meta-schema WAL block for `schema` under table id `tid` — the
/// exact bytes a schema-bearing push embeds. Shared by the plain-push encoder,
/// the always-schema-bearing `PUSH_TXN` per-family block, and the ScanSpec
/// request's reply schema.
///
/// `pub` for `gnitz-mirror`, which encodes a registered view's schema through
/// it, and for the engine's cross-side wire tests.
pub fn encode_schema_block(schema: &Schema, tid: u32) -> Vec<u8> {
    let cols: Vec<SchemaBlockCol> = schema
        .columns
        .iter()
        .enumerate()
        .map(|(ci, col)| SchemaBlockCol {
            type_code: col.type_code as u8,
            // Position-in-PK-tuple is carried so compound `PRIMARY KEY (b, a)`
            // decodes back to the user-declared order.
            meta: ColMeta {
                nullable: col.is_nullable,
                hidden: col.is_hidden,
                serial: col.is_serial,
                scale: col.scale,
                pk_pos: schema.pk_cols.iter().position(|&p| p as usize == ci).map(|p| p as u8),
            },
            name: col.name.as_bytes(),
        })
        .collect();
    gnitz_wire::schema_block::encode(tid, &cols)
}

/// A `SCAN_SPEC` reply schema together with the wire block that ships it, both
/// a pure function of `(schema, target_id)`. A caller that repeats a read
/// against one relation — a mirror poll — builds this once and pays neither the
/// encode nor a schema clone per call.
pub struct ReplySchema {
    schema: Arc<Schema>,
    block: Vec<u8>,
}

impl ReplySchema {
    pub fn new(schema: Arc<Schema>, target_id: u64) -> Self {
        let block = encode_schema_block(&schema, target_id as u32);
        ReplySchema { schema, block }
    }

    /// The decode hint, cheap to hand to a pending slot.
    pub(crate) fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// The encoded block the request carries.
    pub(crate) fn block(&self) -> &[u8] {
        &self.block
    }
}

/// Reconstruct a `Schema` from meta-schema block bytes.
///
/// `PK_LIST_MAX_COLS` is the client's own PK-arity limit — the capacity of the
/// persisted PK-list codec a key must round-trip through — not the engine's,
/// which is wider for its internal secondary-index schema.
///
/// `pub` for the same reason as [`encode_schema_block`].
pub fn schema_from_block(block: &[u8]) -> Result<Schema, ProtocolError> {
    let sb = SchemaBlock::decode(block, false, PK_LIST_MAX_COLS).map_err(|e| ProtocolError::DecodeError(e.into()))?;
    let mut columns = Vec::with_capacity(sb.num_columns());
    for c in sb.columns() {
        let name =
            std::str::from_utf8(c.name).map_err(|e| ProtocolError::DecodeError(format!("utf8 in column name: {e}")))?;
        // The shared decoder already rejected an unknown code; this re-reads it
        // as the client's typed `TypeCode` rather than trusting a cast.
        let ty = ColType {
            tc: type_code_from_u64(c.type_code as u64)?,
            scale: c.meta.scale,
        };
        let mut col = ColumnDef::typed(name, ty, c.meta.nullable);
        if c.meta.hidden {
            col = col.hidden();
        }
        if c.meta.serial {
            col = col.serial();
        }
        columns.push(col);
    }
    Schema::from_parts(columns, sb.pk_indices().to_vec()).map_err(ProtocolError::DecodeError)
}

#[cfg(test)]
#[path = "tests/codec.rs"]
mod tests;
