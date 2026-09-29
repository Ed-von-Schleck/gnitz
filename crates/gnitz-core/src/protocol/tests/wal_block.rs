use super::*;
use crate::test_support::{decode_wal_block, encode_wal_block};
use crate::{BatchAppender, Schema, ZSetBatch};
use gnitz_wire::{ColumnDef, TypeCode};

fn kv_schema(v: TypeCode) -> Schema {
    Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", v, false),
        ],
        pk_cols: vec![0],
    }
}

/// A German cell whose heap offset overruns the heap is a `DecodeError`, and
/// leaves the sink as it was although the block's rows were already appended.
#[test]
fn a_string_cell_pointing_past_the_blob_is_refused_whole() {
    let schema = kv_schema(TypeCode::String);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch, &schema)
        .add_row(1, 1)
        .str_val("a value well past the inline cell");
    let mut forged = batch.clone();
    // A German cell's bytes 8..16 are its heap offset; the block copies it verbatim.
    forged.payload[0].bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes());

    let mut sink = batch.clone();
    assert!(matches!(
        decode_wal_block_into(&mut sink, &encode_wal_block(&forged), &schema),
        Err(ProtocolError::DecodeError(_))
    ));
    assert_eq!(sink, batch);
}

/// A null bit under a NOT NULL column is refused at decode, naming the column.
#[test]
fn a_null_bit_under_a_not_null_column_is_a_decode_error() {
    let schema = kv_schema(TypeCode::I64);
    let mut b = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut b, &schema);
        a.add_row(1, 1).i64_val(7);
        a.add_row(2, 1).i64_val(8);
    }
    b.nulls[1] = 1;
    match decode_wal_block(&encode_wal_block(&b), &schema) {
        Err(ProtocolError::DecodeError(m)) => assert!(m.contains("NOT NULL column 'v'"), "{m}"),
        other => panic!("expected a NOT NULL decode error, got {other:?}"),
    }
}
