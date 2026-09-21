use super::*;
use crate::protocol::types::TypeCode;
use gnitz_wire::PK_LIST_MAX_COLS;

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
    let block = encode_schema_block(&original);
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
    let block = encode_schema_block(&original);
    assert_eq!(schema_from_block(&block).unwrap().pk_cols, vec![1, 0]);
}

/// A long column name must come back whole: the record carries each name as its
/// own length-prefixed section, with no inline/spill split.
#[test]
fn a_long_column_name_survives_the_record() {
    let long = "a_column_name_well_past_the_inline_cell";
    let original = Schema {
        columns: vec![
            ColumnDef::new("id", TypeCode::U64, false),
            ColumnDef::new(long, TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    let block = encode_schema_block(&original);
    assert_eq!(schema_from_block(&block).unwrap().columns[1].name, long);
}

/// The client's PK arity cap is `PK_LIST_MAX_COLS` — a key it accepts must
/// round-trip through the persisted PK-list word — so it must refuse a record
/// the shared codec, capped wider, admits.
#[test]
fn a_pk_wider_than_the_client_codec_is_rejected() {
    let n = PK_LIST_MAX_COLS + 1;
    let cols: Vec<SchemaBlockCol> = (0..n)
        .map(|_| SchemaBlockCol {
            type_code: TypeCode::U64 as u8,
            meta: ColMeta::default(),
            name: b"k",
        })
        .collect();
    let pk: Vec<u32> = (0..n as u32).collect();
    let block = gnitz_wire::schema_block::encode(&cols, &pk);
    assert!(gnitz_wire::schema_block::decode(&block).is_ok(), "the codec admits it");
    assert!(matches!(schema_from_block(&block), Err(ProtocolError::DecodeError(_))));
}

/// The record's decode bounds only its own arrays; an empty, out-of-range or
/// duplicate PK list is a schema rule, refused here.
#[test]
fn an_empty_out_of_range_or_duplicate_pk_is_rejected() {
    let cols: Vec<SchemaBlockCol> = (0..3)
        .map(|_| SchemaBlockCol {
            type_code: TypeCode::U64 as u8,
            meta: ColMeta::default(),
            name: b"k",
        })
        .collect();
    for pk in [&[][..], &[3], &[1, 1]] {
        let block = gnitz_wire::schema_block::encode(&cols, pk);
        assert!(
            gnitz_wire::schema_block::decode(&block).is_ok(),
            "{pk:?}: the codec admits it"
        );
        assert!(
            matches!(schema_from_block(&block), Err(ProtocolError::DecodeError(_))),
            "{pk:?}"
        );
    }
}

#[test]
fn a_truncated_record_is_a_decode_error_not_a_panic() {
    let schema = Schema {
        columns: vec![ColumnDef::new("id", TypeCode::U64, false)],
        pk_cols: vec![0],
    };
    let block = encode_schema_block(&schema);
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
