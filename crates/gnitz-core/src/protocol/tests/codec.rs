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
    let block = gnitz_wire::schema_block::encode(1, &cols);
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
fn test_unknown_type_code_after_last() {
    let next = TypeCode::ALL.len() as u64 + 1;
    assert!(!TypeCode::ALL.iter().any(|&tc| tc as u64 == next));
    assert!(matches!(type_code_from_u64(next), Err(ProtocolError::UnknownTypeCode(n)) if n == next));
}

#[test]
fn test_unknown_type_code_max() {
    assert!(matches!(
        type_code_from_u64(u64::MAX),
        Err(ProtocolError::UnknownTypeCode(_))
    ));
}
