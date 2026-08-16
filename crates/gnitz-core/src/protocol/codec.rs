//! The client's two adapters to the shared meta-schema block codec
//! (`gnitz_wire::schema_block`): `Schema` → block bytes, block bytes →
//! `Schema`. The block's layout and every rule about what makes one admissible
//! live in `gnitz-wire`, so the engine cannot enforce a different set.

use super::error::ProtocolError;
use super::types::{type_code_from_u64, ColumnDef, Schema};
use gnitz_wire::schema_block::{SchemaBlock, SchemaBlockCol};
use gnitz_wire::{col_meta_hidden, col_meta_nullable, col_meta_serial, pack_col_meta_flags, PK_LIST_MAX_COLS};

/// Encode the meta-schema WAL block for `schema` under table id `tid` — the
/// exact bytes a schema-bearing push embeds. Shared by the plain-push encoder,
/// the always-schema-bearing `FLAG_PUSH_TXN` per-family block, and the ScanSpec
/// request's reply schema.
///
/// `pub` for the engine's cross-side wire tests (which take gnitz-core as a
/// dev-dependency) — the only coverage that the two crates' adapters produce
/// and accept the same bytes. No production caller outside this crate.
pub fn encode_schema_block(schema: &Schema, tid: u32) -> Vec<u8> {
    let cols: Vec<SchemaBlockCol> = schema
        .columns
        .iter()
        .enumerate()
        .map(|(ci, col)| SchemaBlockCol {
            type_code: col.type_code as u8,
            // Position-in-PK-tuple is carried so compound `PRIMARY KEY (b, a)`
            // decodes back to the user-declared order.
            flags: pack_col_meta_flags(
                col.is_nullable,
                col.is_hidden,
                col.is_serial,
                schema.pk_cols.iter().position(|&p| p == ci).map(|p| p as u8),
            ),
            name: col.name.as_bytes(),
        })
        .collect();
    gnitz_wire::schema_block::encode(tid, &cols, true)
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
        let mut col = ColumnDef::new(
            name,
            type_code_from_u64(c.type_code as u64)?,
            col_meta_nullable(c.flags),
        );
        if col_meta_hidden(c.flags) {
            col = col.hidden();
        }
        if col_meta_serial(c.flags) {
            col = col.serial();
        }
        columns.push(col);
    }
    let pk_cols: Vec<usize> = sb.pk_indices().iter().map(|&i| i as usize).collect();
    Schema::from_parts(columns, pk_cols).map_err(|e| ProtocolError::DecodeError(e.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::TypeCode;

    /// Every fact a `Schema` carries must survive the block round-trip: column
    /// types, nullability, names, the `hidden`/`serial` markers, and the
    /// declared PK order (which is not column order here).
    #[test]
    fn schema_survives_the_block_roundtrip() {
        let original = Schema {
            columns: vec![
                ColumnDef::new("id", TypeCode::U64, false).hidden().serial(),
                ColumnDef::new("name", TypeCode::String, true),
                ColumnDef::new("score", TypeCode::F64, false),
                ColumnDef::new("tag", TypeCode::I32, true),
                ColumnDef::new("uuid", TypeCode::U128, false),
            ],
            pk_cols: vec![0],
        };
        let block = encode_schema_block(&original, 0);
        assert_eq!(schema_from_block(&block).unwrap(), original);
    }

    #[test]
    fn compound_pk_decodes_in_declared_order() {
        let original = Schema {
            columns: vec![
                ColumnDef::new("a", TypeCode::U64, false),
                ColumnDef::new("b", TypeCode::I32, false),
                ColumnDef::new("v", TypeCode::String, true),
            ],
            // `PRIMARY KEY (b, a)` — the reverse of column order.
            pk_cols: vec![1, 0],
        };
        let block = encode_schema_block(&original, 9);
        assert_eq!(schema_from_block(&block).unwrap().pk_cols, vec![1, 0]);
    }

    /// A column name that spills the German-string inline cell must come back
    /// whole, from the block's blob heap.
    #[test]
    fn a_long_column_name_survives_the_blob_heap() {
        let long = "a_column_name_well_past_the_inline_cell";
        let original = Schema {
            columns: vec![
                ColumnDef::new("id", TypeCode::U64, false),
                ColumnDef::new(long, TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        };
        let block = encode_schema_block(&original, 1);
        assert_eq!(schema_from_block(&block).unwrap().columns[1].name, long);
    }

    /// The wire caps the client enforces on a decoded block. The rejections
    /// themselves are `gnitz-wire`'s; what this pins is that the client asks for
    /// *its* limits — in particular `PK_LIST_MAX_COLS`, not the engine's wider
    /// `MAX_PK_COLUMNS`.
    #[test]
    fn a_pk_wider_than_the_client_codec_is_rejected() {
        let n = PK_LIST_MAX_COLS + 1;
        let cols: Vec<SchemaBlockCol> = (0..n)
            .map(|i| SchemaBlockCol {
                type_code: TypeCode::U64 as u8,
                flags: pack_col_meta_flags(false, false, false, Some(i as u8)),
                name: b"k",
            })
            .collect();
        let block = gnitz_wire::schema_block::encode(1, &cols, true);
        assert!(matches!(schema_from_block(&block), Err(ProtocolError::DecodeError(_))));
    }

    #[test]
    fn a_truncated_block_is_a_decode_error_not_a_panic() {
        let schema = Schema {
            columns: vec![ColumnDef::new("id", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        let block = encode_schema_block(&schema, 1);
        for cut in [0, 8, block.len() / 2, block.len() - 1] {
            assert!(schema_from_block(&block[..cut]).is_err(), "cut at {cut}");
        }
    }

    // ── type_code_from_u64 error paths ──────────────────────────────────────

    #[test]
    fn test_unknown_type_code_zero() {
        assert!(matches!(type_code_from_u64(0), Err(ProtocolError::UnknownTypeCode(0))));
    }

    #[test]
    fn test_unknown_type_code_16() {
        // 16 is the first unassigned type-code value after I128 (15).
        assert!(matches!(
            type_code_from_u64(16),
            Err(ProtocolError::UnknownTypeCode(16))
        ));
    }

    #[test]
    fn test_unknown_type_code_max() {
        assert!(matches!(
            type_code_from_u64(u64::MAX),
            Err(ProtocolError::UnknownTypeCode(_))
        ));
    }
}
