use crate::runtime::wire::{
    encode_wire, encode_wire_into, decode_wire, decode_wire_with_schema,
    wire_size, schema_to_batch, batch_to_schema, peek_target_id,
    build_schema_wire_block,
    STATUS_OK, STATUS_ERROR,
};
use crate::schema::{SchemaDescriptor, SchemaColumn, type_code, encode_german_string, decode_german_string};
use crate::storage::{Batch, ConsolidatedBatch};

fn simple_schema() -> SchemaDescriptor {
    let mut sd = SchemaDescriptor {
        num_columns: 2,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    };
    sd.columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
    sd.columns[1] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
    sd
}

fn string_schema() -> SchemaDescriptor {
    let mut sd = SchemaDescriptor {
        num_columns: 3,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    };
    sd.columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
    sd.columns[1] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
    sd.columns[2] = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0 };
    sd
}

fn make_simple_batch(pk: u64, val: u64) -> ConsolidatedBatch {
    let sd = simple_schema();
    let mut b = Batch::with_schema(sd, 1);
    b.extend_pk_lo(&pk.to_le_bytes());
    b.extend_pk_hi(&0u64.to_le_bytes());
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &val.to_le_bytes());
    b.count = 1;
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

#[test]
fn test_encode_decode_roundtrip_no_data() {
    let wire = encode_wire(
        42, 7, 0x100, 10, 20, 3, 0,
        STATUS_OK, b"",
        None, None, None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.target_id, 42);
    assert_eq!(decoded.control.client_id, 7);
    assert_eq!(decoded.control.flags & 0xFFFF, 0x100);
    assert_eq!(decoded.control.seek_pk_lo, 10);
    assert_eq!(decoded.control.seek_pk_hi, 20);
    assert_eq!(decoded.control.seek_col_idx, 3);
    assert_eq!(decoded.control.request_id, 0);
    assert_eq!(decoded.control.status, STATUS_OK);
    assert!(decoded.control.error_msg.is_empty());
    assert!(decoded.schema.is_none());
    assert!(decoded.data_batch.is_none());
}

#[test]
fn test_encode_decode_roundtrip_with_schema() {
    let sd = simple_schema();
    let names: Vec<&[u8]> = vec![b"id", b"value"];
    let wire = encode_wire(
        1, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&sd), Some(&names), None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert!(decoded.schema.is_some());
    let s = decoded.schema.unwrap();
    assert_eq!(s.num_columns, 2);
    assert_eq!(s.pk_index, 0);
    assert_eq!(s.columns[0].type_code, type_code::U64);
    assert_eq!(s.columns[1].type_code, type_code::U64);
    assert!(decoded.data_batch.is_none());
}

#[test]
fn test_encode_decode_roundtrip_with_data() {
    let sd = simple_schema();
    let batch = make_simple_batch(100, 999);
    let names: Vec<&[u8]> = vec![b"id", b"val"];
    let wire = encode_wire(
        5, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&sd), Some(&names), Some(&batch),
    );
    let decoded = decode_wire(&wire).unwrap();
    assert!(decoded.schema.is_some());
    assert!(decoded.data_batch.is_some());
    let db = decoded.data_batch.as_ref().unwrap();
    assert_eq!(db.count, 1);
    let pk = u64::from_le_bytes(db.pk_lo_data()[0..8].try_into().unwrap());
    assert_eq!(pk, 100);
    let val = u64::from_le_bytes(db.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(val, 999);
    assert!(db.sorted);
    assert!(db.consolidated);
}

#[test]
fn test_encode_decode_error_msg() {
    let wire = encode_wire(
        0, 0, 0, 0, 0, 0, 0,
        STATUS_ERROR, b"something went wrong",
        None, None, None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.status, STATUS_ERROR);
    assert_eq!(decoded.control.error_msg, b"something went wrong");
}

#[test]
fn test_encode_decode_request_id_nonzero() {
    let req_id: u64 = 0xDEAD_BEEF_CAFE_F00D;
    let wire = encode_wire(
        42, 7, 0, 0, 0, 0, req_id,
        STATUS_OK, b"",
        None, None, None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.request_id, req_id);
}

#[test]
fn test_encode_decode_request_id_max() {
    let wire = encode_wire(
        1, 2, 3, 4, 5, 6, u64::MAX,
        STATUS_OK, b"",
        None, None, None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.request_id, u64::MAX);
}

#[test]
fn test_encode_decode_request_id_with_error() {
    let req_id: u64 = 0x1122_3344_5566_7788;
    let wire = encode_wire(
        10, 20, 30, 40, 50, 60, req_id,
        STATUS_ERROR, b"boom",
        None, None, None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.status, STATUS_ERROR);
    assert_eq!(decoded.control.request_id, req_id);
    assert_eq!(decoded.control.error_msg, b"boom");
}

#[test]
fn test_wire_size_includes_request_id() {
    let sz = wire_size(STATUS_OK, b"", None, None, None, None);
    let wire = encode_wire(
        0, 0, 0, 0, 0, 0, 0xAAAA_BBBB_CCCC_DDDD,
        STATUS_OK, b"",
        None, None, None,
    );
    assert_eq!(sz, wire.len());
    let sz2 = wire_size(STATUS_ERROR, b"err", None, None, None, None);
    let wire2 = encode_wire(
        0, 0, 0, 0, 0, 0, u64::MAX,
        STATUS_ERROR, b"err",
        None, None, None,
    );
    assert_eq!(sz2, wire2.len());
}

#[test]
fn test_schema_roundtrip_with_names() {
    let sd = string_schema();
    let names: Vec<&[u8]> = vec![b"pk_col", b"int_col", b"name_col"];
    let batch = schema_to_batch(&sd, &names);
    let (sd2, names2) = batch_to_schema(&batch).unwrap();
    assert_eq!(sd2.num_columns, 3);
    assert_eq!(sd2.pk_index, 0);
    assert_eq!(sd2.columns[0].type_code, type_code::U64);
    assert_eq!(sd2.columns[1].type_code, type_code::U64);
    assert_eq!(sd2.columns[2].type_code, type_code::STRING);
    assert_eq!(sd2.columns[2].size, 16);
    assert_eq!(names2[0], b"pk_col");
    assert_eq!(names2[1], b"int_col");
    assert_eq!(names2[2], b"name_col");
}

#[test]
fn test_flag_has_data_requires_schema() {
    let sd = simple_schema();
    let batch = make_simple_batch(1, 2);
    let names: Vec<&[u8]> = vec![b"a", b"b"];
    let mut wire = encode_wire(
        0, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&sd), Some(&names), Some(&batch),
    );
    let ctrl_size = u32::from_le_bytes(wire[16..20].try_into().unwrap()) as usize;
    wire.truncate(ctrl_size);
    let result = decode_wire(&wire);
    assert!(result.is_err());
}

#[test]
fn test_schema_roundtrip_long_names() {
    let sd = simple_schema();
    let long_name = b"this_is_a_very_long_column_name_exceeding_twelve_bytes";
    let names: Vec<&[u8]> = vec![b"pk", long_name];
    let batch = schema_to_batch(&sd, &names);
    let (sd2, names2) = batch_to_schema(&batch).unwrap();
    assert_eq!(sd2.num_columns, 2);
    assert_eq!(names2[0], b"pk");
    assert_eq!(names2[1], long_name);
}

#[test]
fn test_encode_decode_string_column() {
    let sd = string_schema();
    let mut batch = Batch::with_schema(sd, 2);

    batch.extend_pk_lo(&1u64.to_le_bytes());
    batch.extend_pk_hi(&0u64.to_le_bytes());
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &42u64.to_le_bytes());
    let st1 = encode_german_string(b"hello", &mut batch.blob);
    batch.extend_col(1, &st1);
    batch.count += 1;

    batch.extend_pk_lo(&2u64.to_le_bytes());
    batch.extend_pk_hi(&0u64.to_le_bytes());
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &99u64.to_le_bytes());
    let long_str = b"this is a long string that exceeds twelve bytes";
    let st2 = encode_german_string(long_str, &mut batch.blob);
    batch.extend_col(1, &st2);
    batch.count += 1;

    let names: Vec<&[u8]> = vec![b"id", b"val", b"name"];
    let wire = encode_wire(
        10, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&sd), Some(&names), Some(&batch),
    );
    let decoded = decode_wire(&wire).unwrap();
    let db = decoded.data_batch.as_ref().unwrap();
    assert_eq!(db.count, 2);

    let mut s1 = [0u8; 16];
    s1.copy_from_slice(&db.col_data(1)[0..16]);
    let str1 = decode_german_string(&s1, &db.blob);
    assert_eq!(str1, b"hello");

    let mut s2 = [0u8; 16];
    s2.copy_from_slice(&db.col_data(1)[16..32]);
    let str2 = decode_german_string(&s2, &db.blob);
    assert_eq!(str2, long_str);
}

#[test]
fn wire_size_matches_encode() {
    let sz = wire_size(STATUS_OK, b"", None, None, None, None);
    let wire = encode_wire(42, 7, 0x100, 10, 20, 3, 0, STATUS_OK, b"", None, None, None);
    assert_eq!(sz, wire.len(), "wire_size mismatch (no data)");

    let sd = simple_schema();
    let names: Vec<&[u8]> = vec![b"id", b"val"];
    let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), None, None);
    let wire = encode_wire(1, 0, 0, 0, 0, 0, 0, STATUS_OK, b"", Some(&sd), Some(&names), None);
    assert_eq!(sz, wire.len(), "wire_size mismatch (schema only)");

    let batch = make_simple_batch(100, 999);
    let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch), None);
    let wire = encode_wire(5, 0, 0, 0, 0, 0, 0, STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch));
    assert_eq!(sz, wire.len(), "wire_size mismatch (with data)");

    let sz = wire_size(STATUS_ERROR, b"something went wrong", None, None, None, None);
    let wire = encode_wire(0, 0, 0, 0, 0, 0, 0, STATUS_ERROR, b"something went wrong", None, None, None);
    assert_eq!(sz, wire.len(), "wire_size mismatch (error msg)");
}

#[test]
fn encode_wire_into_roundtrip() {
    let sd = simple_schema();
    let batch = make_simple_batch(100, 999);
    let names: Vec<&[u8]> = vec![b"id", b"val"];

    let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch), None);
    let mut buf = vec![0u8; sz];
    let written = encode_wire_into(
        &mut buf, 0,
        5, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&sd), Some(&names), Some(&batch), None,
    );
    assert_eq!(written, sz);

    let decoded = decode_wire(&buf).unwrap();
    assert_eq!(decoded.control.target_id, 5);
    assert!(decoded.schema.is_some());
    let db = decoded.data_batch.as_ref().unwrap();
    assert_eq!(db.count, 1);
    let pk = u64::from_le_bytes(db.pk_lo_data()[0..8].try_into().unwrap());
    assert_eq!(pk, 100);
    let val = u64::from_le_bytes(db.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(val, 999);
}

#[test]
fn encode_wire_into_matches_encode_wire() {
    let sd = simple_schema();
    let batch = make_simple_batch(42, 123);
    let names: Vec<&[u8]> = vec![b"id", b"val"];

    let wire = encode_wire(
        10, 3, 0x200, 5, 6, 7, 0,
        STATUS_OK, b"",
        Some(&sd), Some(&names), Some(&batch),
    );

    let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch), None);
    let mut buf = vec![0u8; sz];
    let written = encode_wire_into(
        &mut buf, 0,
        10, 3, 0x200, 5, 6, 7, 0,
        STATUS_OK, b"",
        Some(&sd), Some(&names), Some(&batch), None,
    );
    assert_eq!(written, wire.len());
    assert_eq!(buf, wire, "encode_wire_into should produce identical bytes");
}

/// Verify that prebuilt schema bytes produce bit-identical output to the inline path.
/// This is the core invariant of the schema wire block cache.
#[test]
fn prebuilt_schema_block_matches_inline_encode() {
    let sd = simple_schema();
    let names: Vec<&[u8]> = vec![b"id", b"val"];
    let batch = make_simple_batch(7, 42);
    let target_id: u64 = 99;

    // Inline path (no prebuilt).
    let inline = encode_wire(
        target_id, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&sd), Some(&names), Some(&batch),
    );

    // Prebuilt path.
    let prebuilt = build_schema_wire_block(&sd, &names, target_id as u32);
    let sz = wire_size(STATUS_OK, b"", Some(&sd), None, Some(&batch), Some(&prebuilt));
    let mut buf = vec![0u8; sz];
    encode_wire_into(
        &mut buf, 0,
        target_id, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&sd), None, Some(&batch), Some(&prebuilt),
    );

    assert_eq!(buf, inline, "prebuilt schema block must produce identical wire bytes");
}

/// A cached schema block with no col_names is valid and round-trips correctly.
#[test]
fn prebuilt_schema_block_no_col_names_roundtrips() {
    let sd = simple_schema();
    let prebuilt = build_schema_wire_block(&sd, &[], 5);
    let sz = wire_size(STATUS_OK, b"", Some(&sd), None, None, Some(&prebuilt));
    let mut buf = vec![0u8; sz];
    encode_wire_into(
        &mut buf, 0,
        5, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&sd), None, None, Some(&prebuilt),
    );
    let decoded = decode_wire(&buf).unwrap();
    assert!(decoded.schema.is_some(), "schema block must be present");
    assert_eq!(decoded.schema.unwrap().num_columns, sd.num_columns);
}

#[test]
fn test_decode_truncated_control_block_returns_err() {
    let wire = encode_wire(
        1, 2, 3, 4, 5, 6, 7,
        STATUS_OK, b"",
        None, None, None,
    );
    let truncated = &wire[..wire.len().saturating_sub(32)];
    let result = decode_wire(truncated);
    assert!(result.is_err(),
        "decode should reject truncated control block, got Ok");
}

#[test]
fn test_decode_wire_with_schema_rejects_nullable_mismatch() {
    let mut server_sd = SchemaDescriptor {
        num_columns: 2,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    };
    server_sd.columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
    server_sd.columns[1] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 1, _pad: 0 };

    let mut client_sd = server_sd;
    client_sd.columns[1].nullable = 0;

    let batch = make_simple_batch(1, 42);
    let names: Vec<&[u8]> = vec![b"id", b"val"];
    let wire = encode_wire(
        1, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&client_sd), Some(&names), Some(&batch),
    );
    let result = decode_wire_with_schema(&wire, &server_sd);
    assert!(result.is_err(),
        "decode_wire_with_schema should reject nullable mismatch but accepted it");
}

#[test]
fn test_decode_wire_with_schema_accepts_nullable_match() {
    let mut server_sd = SchemaDescriptor {
        num_columns: 2,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    };
    server_sd.columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
    server_sd.columns[1] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 1, _pad: 0 };

    let batch = make_simple_batch(1, 42);
    let names: Vec<&[u8]> = vec![b"id", b"val"];
    let wire = encode_wire(
        1, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&server_sd), Some(&names), Some(&batch),
    );
    let result = decode_wire_with_schema(&wire, &server_sd);
    assert!(result.is_ok(),
        "decode_wire_with_schema should accept matching nullable");
}

/// decode_control_block reads error_msg from the blob arena when the string
/// exceeds the 12-byte inline threshold. Validates the fast directory-read
/// path handles the blob region offset correctly.
#[test]
fn decode_control_block_long_error_msg_uses_blob() {
    let long_msg = b"this error message is definitely longer than twelve bytes";
    let wire = encode_wire(
        7, 3, 0, 0, 0, 0, 0xABCD,
        STATUS_ERROR, long_msg,
        None, None, None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.target_id, 7);
    assert_eq!(decoded.control.client_id, 3);
    assert_eq!(decoded.control.request_id, 0xABCD);
    assert_eq!(decoded.control.status, STATUS_ERROR);
    assert_eq!(decoded.control.error_msg, long_msg.as_ref());
}

/// When error_msg is null (STATUS_OK with no message), the null bitmap bit
/// is set and decode_control_block must return an empty Vec without touching
/// the error_msg region.
#[test]
fn decode_control_block_null_error_msg() {
    let wire = encode_wire(
        42, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        None, None, None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert!(decoded.control.error_msg.is_empty());
}

/// All control fields round-trip correctly through the fast decode path.
#[test]
fn decode_control_block_all_fields_round_trip() {
    let wire = encode_wire(
        0xDEAD, 0xBEEF, 0xCAFE, 0x1111, 0x2222, 0x3333, 0x4444,
        STATUS_OK, b"",
        None, None, None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.target_id,    0xDEAD);
    assert_eq!(decoded.control.client_id,    0xBEEF);
    assert_eq!(decoded.control.flags & 0xFFFF, 0xCAFE);
    assert_eq!(decoded.control.seek_pk_lo,   0x1111);
    assert_eq!(decoded.control.seek_pk_hi,   0x2222);
    assert_eq!(decoded.control.seek_col_idx, 0x3333);
    assert_eq!(decoded.control.request_id,   0x4444);
}

#[test]
fn peek_target_id_on_valid_wire() {
    let wire = encode_wire(
        99, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        None, None, None,
    );
    let tid = peek_target_id(&wire).unwrap();
    assert_eq!(tid, 99);
}

#[test]
fn peek_target_id_rejects_short_data() {
    // Any slice shorter than WAL_HEADER_SIZE (48 bytes) must fail.
    let result = peek_target_id(&[0u8; 10]);
    assert!(result.is_err(), "peek_target_id should reject slices < WAL_HEADER_SIZE");
}

#[test]
fn decode_wire_truncated_schema_block_returns_err() {
    let sd = simple_schema();
    let names: Vec<&[u8]> = vec![b"id", b"val"];
    // Encode with schema but no data.
    let wire = encode_wire(
        1, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&sd), Some(&names), None,
    );
    // The control block occupies [0..ctrl_size). Truncate in the middle of the
    // schema block (keep only the first WAL_HEADER_SIZE bytes of it so the
    // header is readable but the body is missing).
    let ctrl_size = u32::from_le_bytes(wire[16..20].try_into().unwrap()) as usize;
    let truncated = &wire[..ctrl_size + gnitz_wire::WAL_HEADER_SIZE / 2];
    let result = decode_wire(truncated);
    assert!(result.is_err(), "decode_wire should reject truncated schema block");
}

#[test]
fn decode_wire_truncated_data_block_returns_err() {
    let sd = simple_schema();
    let batch = make_simple_batch(1, 42);
    let names: Vec<&[u8]> = vec![b"id", b"val"];
    let wire = encode_wire(
        1, 0, 0, 0, 0, 0, 0,
        STATUS_OK, b"",
        Some(&sd), Some(&names), Some(&batch),
    );
    // Find where the data block starts: after control + schema blocks.
    let ctrl_size = u32::from_le_bytes(wire[16..20].try_into().unwrap()) as usize;
    let schema_size = u32::from_le_bytes(wire[ctrl_size + 16..ctrl_size + 20].try_into().unwrap()) as usize;
    let data_start = ctrl_size + schema_size;
    // Truncate in the middle of the data block.
    let truncated = &wire[..data_start + gnitz_wire::WAL_HEADER_SIZE / 2];
    let result = decode_wire(truncated);
    assert!(result.is_err(), "decode_wire should reject truncated data block");
}
