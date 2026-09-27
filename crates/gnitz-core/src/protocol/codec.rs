//! The client's two adapters to the shared meta-schema record codec
//! (`gnitz_wire::schema_block`): `Schema` → record bytes, record bytes →
//! `Schema`. The record's layout lives in `gnitz-wire`, so the engine cannot
//! encode a different one.

use super::error::ProtocolError;
use super::types::{ColumnDef, Schema};
use gnitz_wire::schema_block::{ColMeta, SchemaBlockCol};
use std::sync::Arc;

/// Encode the meta-schema record for `schema` — the exact bytes a
/// schema-bearing push or ScanSpec request embeds.
///
/// `pub` for `gnitz-mirror`, which uses the bytes as a view's schema identity,
/// and for the engine's cross-side wire tests.
pub fn encode_schema_block(schema: &Schema) -> Vec<u8> {
    let cols: Vec<SchemaBlockCol> = schema
        .columns
        .iter()
        .map(|col| SchemaBlockCol {
            ty: col.ty,
            meta: ColMeta {
                nullable: col.is_nullable,
                hidden: col.is_hidden,
            },
            name: col.name.as_bytes(),
        })
        .collect();
    gnitz_wire::schema_block::encode(&cols, &schema.pk_cols)
}

/// A `SCAN_SPEC` reply schema together with the wire record that ships it, both
/// a pure function of the schema. A caller that repeats a read against one
/// relation — a mirror poll — builds this once and pays neither the encode nor a
/// schema clone per call.
pub struct ReplySchema {
    schema: Arc<Schema>,
    block: Vec<u8>,
}

impl ReplySchema {
    pub fn new(schema: Arc<Schema>) -> Self {
        let block = encode_schema_block(&schema);
        ReplySchema { schema, block }
    }

    /// The decode hint, cheap to hand to a pending slot.
    pub(crate) fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// The encoded record the request carries.
    pub(crate) fn block(&self) -> &[u8] {
        &self.block
    }
}

/// Reconstruct a `Schema` from meta-schema record bytes.
///
/// `pub` for the same reason as [`encode_schema_block`].
pub fn schema_from_block(block: &[u8]) -> Result<Schema, ProtocolError> {
    let sb = gnitz_wire::schema_block::decode(block).map_err(ProtocolError::DecodeError)?;
    let mut columns = Vec::with_capacity(sb.num_columns());
    for c in sb.columns() {
        let name =
            std::str::from_utf8(c.name).map_err(|e| ProtocolError::DecodeError(format!("utf8 in column name: {e}")))?;
        let mut col = ColumnDef::typed(name, c.ty, c.meta.nullable);
        if c.meta.hidden {
            col = col.hidden();
        }
        columns.push(col);
    }
    Schema::from_parts(columns, sb.pk_indices().to_vec()).map_err(ProtocolError::DecodeError)
}

#[cfg(test)]
#[path = "tests/codec.rs"]
mod tests;
