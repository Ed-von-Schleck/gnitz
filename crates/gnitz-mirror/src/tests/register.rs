use super::*;
use gnitz_core::protocol::types::{ColumnDef, TypeCode};
use gnitz_wire::MAX_COLUMNS;

fn u64_cols(n: usize) -> Vec<ColumnDef> {
    (0..n)
        .map(|i| ColumnDef::new(format!("c{i}"), TypeCode::U64, false))
        .collect()
}

/// `Schema.columns` is a `Vec` behind a `pub` field, so a schema wider than the
/// engine's cap reaches here and must come back as an `Err`.
#[test]
fn a_schema_wider_than_the_engine_column_limit_is_an_error() {
    let schema = Schema {
        columns: u64_cols(MAX_COLUMNS + 1),
        pk_cols: vec![0],
    };
    assert!(descriptor_of(&schema).is_err());
}

/// A client `Schema` converted directly and the same schema's encoded record
/// decoded back must agree: a mirrored view is registered through one and read
/// through the other.
#[test]
fn converting_a_schema_agrees_with_decoding_its_record() {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("a", TypeCode::U64, false),
            ColumnDef::new("b", TypeCode::I32, false),
            ColumnDef::new("v", TypeCode::String, true),
        ],
        // `PRIMARY KEY (b, a)` — the reverse of column order.
        pk_cols: vec![1, 0],
    };
    let direct = descriptor_of(&schema).expect("a valid schema converts");
    let record = gnitz_core::protocol::codec::encode_schema_block(&schema);
    let decoded = descriptor_of_block(&record).expect("its record decodes");

    assert_eq!(direct.pk_indices(), decoded.pk_indices());
    assert_eq!(direct.num_columns(), decoded.num_columns());
    for i in 0..direct.num_columns() {
        let (a, b) = (&direct.columns[i], &decoded.columns[i]);
        assert_eq!((a.type_code, a.nullable), (b.type_code, b.nullable), "column {i}");
    }
}
