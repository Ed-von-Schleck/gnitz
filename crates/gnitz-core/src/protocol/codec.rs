//! The client's two adapters to the shared meta-schema record codec
//! (`gnitz_wire::schema_block`): `Schema` → record bytes, record bytes →
//! `Schema`. The record's layout lives in `gnitz-wire`, so the engine cannot
//! encode a different one.

use super::error::ProtocolError;
use super::types::{ColumnDef, Schema};
use gnitz_wire::schema_block::{ColMeta, SchemaBlockCol};

/// Encode the meta-schema record for `schema` — the exact bytes a
/// schema-bearing push embeds.
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

/// Reconstruct a `Schema` from meta-schema record bytes.
///
/// `pub` for the same reason as [`encode_schema_block`].
pub fn schema_from_block(block: &[u8]) -> Result<Schema, ProtocolError> {
    let mut columns = Vec::new();
    let pk = gnitz_wire::schema_block::decode(block, |c| {
        let name = std::str::from_utf8(c.name).map_err(|e| format!("utf8 in column name: {e}"))?;
        let mut col = ColumnDef::typed(name, c.ty, c.meta.nullable);
        if c.meta.hidden {
            col = col.hidden();
        }
        columns.push(col);
        Ok(())
    })
    .map_err(ProtocolError::DecodeError)?;
    Schema::from_parts(columns, pk.as_slice().to_vec()).map_err(ProtocolError::DecodeError)
}

#[cfg(test)]
#[path = "tests/codec.rs"]
mod tests;
