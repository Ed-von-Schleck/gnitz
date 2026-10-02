use super::*;
use crate::test_support::{decode_wal_block, encode_wal_block, kv_schema};
use crate::{BatchAppender, ZSetBatch};
use gnitz_wire::TypeCode;

/// A German cell whose heap offset overruns the heap is a `DecodeError`.
#[test]
fn a_string_cell_pointing_past_the_blob_is_refused() {
    let schema = kv_schema(TypeCode::String);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch)
        .add_row(1, 1)
        .str_val("a value well past the inline cell");
    let mut forged = batch.clone();
    // A German cell's bytes 8..16 are its heap offset; the block copies it verbatim.
    forged.payload[0].bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes());

    match decode_wal_block(&encode_wal_block(&forged), &schema) {
        Err(ProtocolError::DecodeError(m)) => assert!(m.contains("not in canonical form"), "{m}"),
        other => panic!("expected a decode error, got {other:?}"),
    }
}

/// A block of zero rows is refused: an empty batch ships no block.
#[test]
fn a_zero_row_block_is_refused() {
    let schema = kv_schema(TypeCode::I64);
    match decode_wal_block(&encode_wal_block(&ZSetBatch::new(&schema)), &schema) {
        Err(ProtocolError::DecodeError(m)) => assert!(m.contains("block holds no rows"), "{m}"),
        other => panic!("expected a decode error, got {other:?}"),
    }
}

/// A region list that does not lay out its schema — a region short, or one
/// not as long as its rows — is a `DecodeError`.
#[test]
fn a_region_list_that_does_not_lay_out_its_schema_is_refused() {
    let schema = kv_schema(TypeCode::I64);
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch).add_row(1, 1).i64_val(7);
    let regions = batch.wire_regions();
    let decode = |regions: &[&[u8]]| decode_regions_into(&mut ZSetBatch::new(&schema), regions, &schema);
    decode(&regions).expect("the batch's own regions decode");

    let short = &regions[..regions.len() - 1];
    let mut ragged = regions.to_vec();
    ragged[gnitz_wire::REG_PAYLOAD_START] = &[0; 4];
    for (case, list) in [
        ("a region short", short),
        ("a ragged region", &ragged[..]),
        ("no regions", &[]),
    ] {
        assert!(matches!(decode(list), Err(ProtocolError::DecodeError(_))), "{case}");
    }
}

/// A null bit under a NOT NULL column is refused at decode, naming the column.
#[test]
fn a_null_bit_under_a_not_null_column_is_a_decode_error() {
    let schema = kv_schema(TypeCode::I64);
    let mut b = ZSetBatch::new(&schema);
    {
        let mut a = BatchAppender::new(&mut b);
        a.add_row(1, 1).i64_val(7);
        a.add_row(2, 1).i64_val(8);
    }
    b.nulls[1] = 1;
    match decode_wal_block(&encode_wal_block(&b), &schema) {
        Err(ProtocolError::DecodeError(m)) => assert!(m.contains("NOT NULL column 'v'"), "{m}"),
        other => panic!("expected a NOT NULL decode error, got {other:?}"),
    }
}
