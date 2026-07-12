use crate::runtime::wire::{
    batch_to_schema, build_schema_wire_block, decode_wire, encode_ctrl_block_direct, encode_wire, encode_wire_into,
    peek_client_control, peek_control_block, schema_to_batch, wire_size, CTRL_BLOCK_SIZE_NO_BLOB, STATUS_ERROR,
    STATUS_OK,
};
use crate::schema::{encode_german_string, try_decode_german_string, type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, Layout};

fn simple_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0],
    )
}

fn string_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    )
}

fn make_simple_batch(pk: u64, val: u64) -> Batch {
    let sd = simple_schema();
    let mut b = Batch::with_schema(sd, 1);
    b.extend_pk(pk as u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &val.to_le_bytes());
    b.count = 1;
    b.certify_layout(Layout::Consolidated, &sd);
    b
}

#[test]
fn test_encode_decode_roundtrip_no_data() {
    let wire = encode_wire(
        42,
        7,
        0x100,
        10u128 | (20u128 << 64),
        3,
        0,
        STATUS_OK,
        b"",
        None,
        None,
        None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.target_id, 42);
    assert_eq!(decoded.control.client_id, 7);
    assert_eq!(decoded.control.flags & 0xFFFF, 0x100);
    assert_eq!(decoded.control.seek_pk, 10u128 | (20u128 << 64));
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
    let wire = encode_wire(1, 0, 0, 0u128, 0, 0, STATUS_OK, b"", Some(&sd), Some(&names), None);
    let decoded = decode_wire(&wire).unwrap();
    assert!(decoded.schema.is_some());
    let s = decoded.schema.unwrap();
    assert_eq!(s.num_columns(), 2);
    assert_eq!(s.pk_indices(), &[0]);
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
        5,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        Some(&names),
        Some(&batch),
    );
    let decoded = decode_wire(&wire).unwrap();
    assert!(decoded.schema.is_some());
    assert!(decoded.data_batch.is_some());
    let db = decoded.data_batch.as_ref().unwrap();
    assert_eq!(db.count, 1);
    let pk = db.get_pk(0) as u64;
    assert_eq!(pk, 100);
    let val = u64::from_le_bytes(db.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(val, 999);
    assert!(db.is_sorted());
    assert!(db.is_consolidated());
}

#[test]
fn test_encode_decode_error_msg() {
    let wire = encode_wire(
        0,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_ERROR,
        b"something went wrong",
        None,
        None,
        None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.status, STATUS_ERROR);
    assert_eq!(decoded.control.error_msg, b"something went wrong");
}

#[test]
fn test_encode_decode_request_id_nonzero() {
    let req_id: u64 = 0xDEAD_BEEF_CAFE_F00D;
    let wire = encode_wire(42, 7, 0, 0u128, 0, req_id, STATUS_OK, b"", None, None, None);
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.request_id, req_id);
}

#[test]
fn test_encode_decode_request_id_max() {
    let wire = encode_wire(
        1,
        2,
        3,
        4u128 | (5u128 << 64),
        6,
        u64::MAX,
        STATUS_OK,
        b"",
        None,
        None,
        None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.request_id, u64::MAX);
}

#[test]
fn test_encode_decode_request_id_with_error() {
    let req_id: u64 = 0x1122_3344_5566_7788;
    let wire = encode_wire(
        10,
        20,
        30,
        40u128 | (50u128 << 64),
        60,
        req_id,
        STATUS_ERROR,
        b"boom",
        None,
        None,
        None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.status, STATUS_ERROR);
    assert_eq!(decoded.control.request_id, req_id);
    assert_eq!(decoded.control.error_msg, b"boom");
}

#[test]
fn test_wire_size_includes_request_id() {
    let sz = wire_size(STATUS_OK, b"", None, None, None, None, &[]);
    let wire = encode_wire(
        0,
        0,
        0,
        0u128,
        0,
        0xAAAA_BBBB_CCCC_DDDD,
        STATUS_OK,
        b"",
        None,
        None,
        None,
    );
    assert_eq!(sz, wire.len());
    let sz2 = wire_size(STATUS_ERROR, b"err", None, None, None, None, &[]);
    let wire2 = encode_wire(0, 0, 0, 0u128, 0, u64::MAX, STATUS_ERROR, b"err", None, None, None);
    assert_eq!(sz2, wire2.len());
}

#[test]
fn test_schema_roundtrip_with_names() {
    let sd = string_schema();
    let names: Vec<&[u8]> = vec![b"pk_col", b"int_col", b"name_col"];
    let batch = schema_to_batch(&sd, &names, 0);
    let (sd2, names2) = batch_to_schema(&batch).unwrap();
    assert_eq!(sd2.num_columns(), 3);
    assert_eq!(sd2.pk_indices(), &[0]);
    assert_eq!(sd2.columns[0].type_code, type_code::U64);
    assert_eq!(sd2.columns[1].type_code, type_code::U64);
    assert_eq!(sd2.columns[2].type_code, type_code::STRING);
    assert_eq!(sd2.columns[2].size(), 16);
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
        0,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        Some(&names),
        Some(&batch),
    );
    let ctrl_size = u32::from_le_bytes(
        wire[gnitz_wire::WAL_OFF_SIZE..gnitz_wire::WAL_OFF_SIZE + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    wire.truncate(ctrl_size);
    let result = decode_wire(&wire);
    assert!(result.is_err());
}

#[test]
fn test_schema_roundtrip_long_names() {
    let sd = simple_schema();
    let long_name = b"this_is_a_very_long_column_name_exceeding_twelve_bytes";
    let names: Vec<&[u8]> = vec![b"pk", long_name];
    let batch = schema_to_batch(&sd, &names, 0);
    let (sd2, names2) = batch_to_schema(&batch).unwrap();
    assert_eq!(sd2.num_columns(), 2);
    assert_eq!(names2[0], b"pk");
    assert_eq!(names2[1], long_name);
}

/// Compound-PK order (including a non-identity `pk_indices` permutation) must
/// survive the `schema_to_batch` → `batch_to_schema` wire round-trip. The
/// catalog-restart peer of this — `schema_roundtrip_catalog_preserves_pk_order`
/// in `catalog/tests/compound_pk_smoke.rs` — exercises the same property through
/// the catalog API.
#[test]
fn schema_roundtrip_wire_preserves_pk_order() {
    let u64c = SchemaColumn::new(type_code::U64, 0);
    let u32c = SchemaColumn::new(type_code::U32, 0);
    let cases: &[(&[SchemaColumn], &[u32])] = &[
        (&[u64c, u64c], &[0, 1]),
        (&[u64c, u64c], &[1, 0]),
        (&[u32c, u32c, u32c, u32c], &[0, 1, 2, 3]),
    ];
    for &(cols, pk_indices) in cases {
        let original = SchemaDescriptor::new(cols, pk_indices);
        let batch = schema_to_batch(&original, &[], 0);
        let (decoded, _names) = batch_to_schema(&batch).unwrap();
        assert!(
            original == decoded,
            "pk_indices {pk_indices:?} did not survive wire round-trip",
        );
    }
}

#[test]
fn test_encode_decode_string_column() {
    let sd = string_schema();
    let mut batch = Batch::with_schema(sd, 2);

    batch.extend_pk(1u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &42u64.to_le_bytes());
    let st1 = encode_german_string(b"hello", &mut batch.blob);
    batch.extend_col(1, &st1);
    batch.count += 1;

    batch.extend_pk(2u128);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.extend_col(0, &99u64.to_le_bytes());
    let long_str = b"this is a long string that exceeds twelve bytes";
    let st2 = encode_german_string(long_str, &mut batch.blob);
    batch.extend_col(1, &st2);
    batch.count += 1;

    let names: Vec<&[u8]> = vec![b"id", b"val", b"name"];
    let wire = encode_wire(
        10,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        Some(&names),
        Some(&batch),
    );
    let decoded = decode_wire(&wire).unwrap();
    let db = decoded.data_batch.as_ref().unwrap();
    assert_eq!(db.count, 2);

    let mut s1 = [0u8; 16];
    s1.copy_from_slice(&db.col_data(1)[0..16]);
    let str1 = try_decode_german_string(&s1, &db.blob).unwrap();
    assert_eq!(str1, b"hello");

    let mut s2 = [0u8; 16];
    s2.copy_from_slice(&db.col_data(1)[16..32]);
    let str2 = try_decode_german_string(&s2, &db.blob).unwrap();
    assert_eq!(str2, long_str);
}

#[test]
fn wire_size_matches_encode() {
    let sz = wire_size(STATUS_OK, b"", None, None, None, None, &[]);
    let wire = encode_wire(
        42,
        7,
        0x100,
        10u128 | (20u128 << 64),
        3,
        0,
        STATUS_OK,
        b"",
        None,
        None,
        None,
    );
    assert_eq!(sz, wire.len(), "wire_size mismatch (no data)");

    let sd = simple_schema();
    let names: Vec<&[u8]> = vec![b"id", b"val"];
    let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), None, None, &[]);
    let wire = encode_wire(1, 0, 0, 0u128, 0, 0, STATUS_OK, b"", Some(&sd), Some(&names), None);
    assert_eq!(sz, wire.len(), "wire_size mismatch (schema only)");

    let batch = make_simple_batch(100, 999);
    let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch), None, &[]);
    let wire = encode_wire(
        5,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        Some(&names),
        Some(&batch),
    );
    assert_eq!(sz, wire.len(), "wire_size mismatch (with data)");

    let sz = wire_size(STATUS_ERROR, b"something went wrong", None, None, None, None, &[]);
    let wire = encode_wire(
        0,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_ERROR,
        b"something went wrong",
        None,
        None,
        None,
    );
    assert_eq!(sz, wire.len(), "wire_size mismatch (error msg)");
}

#[test]
fn encode_wire_into_roundtrip() {
    let sd = simple_schema();
    let batch = make_simple_batch(100, 999);
    let names: Vec<&[u8]> = vec![b"id", b"val"];

    let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch), None, &[]);
    let mut buf = vec![0u8; sz];
    let written = encode_wire_into(
        &mut buf,
        0,
        5,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        Some(&names),
        Some(&batch),
        None,
        &[],
    );
    assert_eq!(written, sz);

    let decoded = decode_wire(&buf).unwrap();
    assert_eq!(decoded.control.target_id, 5);
    assert!(decoded.schema.is_some());
    let db = decoded.data_batch.as_ref().unwrap();
    assert_eq!(db.count, 1);
    let pk = db.get_pk(0) as u64;
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
        10,
        3,
        0x200,
        5u128 | (6u128 << 64),
        7,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        Some(&names),
        Some(&batch),
    );

    let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch), None, &[]);
    let mut buf = vec![0u8; sz];
    let written = encode_wire_into(
        &mut buf,
        0,
        10,
        3,
        0x200,
        5u128 | (6u128 << 64),
        7,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        Some(&names),
        Some(&batch),
        None,
        &[],
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
        target_id,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        Some(&names),
        Some(&batch),
    );

    // Prebuilt path.
    let prebuilt = build_schema_wire_block(&sd, &names, 0, target_id as u32);
    let sz = wire_size(STATUS_OK, b"", Some(&sd), None, Some(&batch), Some(&prebuilt), &[]);
    let mut buf = vec![0u8; sz];
    encode_wire_into(
        &mut buf,
        0,
        target_id,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        None,
        Some(&batch),
        Some(&prebuilt),
        &[],
    );

    assert_eq!(buf, inline, "prebuilt schema block must produce identical wire bytes");
}

/// A cached schema block with no col_names is valid and round-trips correctly.
#[test]
fn prebuilt_schema_block_no_col_names_roundtrips() {
    let sd = simple_schema();
    let prebuilt = build_schema_wire_block(&sd, &[], 0, 5);
    let sz = wire_size(STATUS_OK, b"", Some(&sd), None, None, Some(&prebuilt), &[]);
    let mut buf = vec![0u8; sz];
    encode_wire_into(
        &mut buf,
        0,
        5,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        None,
        None,
        Some(&prebuilt),
        &[],
    );
    let decoded = decode_wire(&buf).unwrap();
    assert!(decoded.schema.is_some(), "schema block must be present");
    assert_eq!(decoded.schema.unwrap().num_columns(), sd.num_columns());
}

#[test]
fn test_decode_truncated_control_block_returns_err() {
    let wire = encode_wire(1, 2, 3, 4u128 | (5u128 << 64), 6, 7, STATUS_OK, b"", None, None, None);
    let truncated = &wire[..wire.len().saturating_sub(32)];
    let result = decode_wire(truncated);
    assert!(result.is_err(), "decode should reject truncated control block, got Ok");
}

/// `decode_wire` decodes from the wire block's embedded schema; schema
/// validation against the catalog happens separately in the executor.
#[test]
fn test_decode_wire_uses_embedded_schema() {
    let sd = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0],
    );

    let batch = make_simple_batch(1, 42);
    let names: Vec<&[u8]> = vec![b"id", b"val"];
    let wire = encode_wire(
        1,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        Some(&names),
        Some(&batch),
    );
    let result = decode_wire(&wire);
    assert!(result.is_ok(), "decode_wire should succeed for a valid frame");
    let decoded = result.unwrap();
    assert!(decoded.schema.is_some());
    assert!(decoded.data_batch.is_some());
}

/// decode_control_block reads error_msg from the blob arena when the string
/// exceeds the 12-byte inline threshold. Validates the fast directory-read
/// path handles the blob region offset correctly.
#[test]
fn decode_control_block_long_error_msg_uses_blob() {
    let long_msg = b"this error message is definitely longer than twelve bytes";
    let wire = encode_wire(7, 3, 0, 0u128, 0, 0xABCD, STATUS_ERROR, long_msg, None, None, None);
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
    let wire = encode_wire(42, 0, 0, 0u128, 0, 0, STATUS_OK, b"", None, None, None);
    let decoded = decode_wire(&wire).unwrap();
    assert!(decoded.control.error_msg.is_empty());
}

/// All control fields round-trip correctly through the fast decode path.
#[test]
fn decode_control_block_all_fields_round_trip() {
    let wire = encode_wire(
        0xDEAD,
        0xBEEF,
        0xCAFE,
        0x1111u128 | (0x2222u128 << 64),
        0x3333,
        0x4444,
        STATUS_OK,
        b"",
        None,
        None,
        None,
    );
    let decoded = decode_wire(&wire).unwrap();
    assert_eq!(decoded.control.target_id, 0xDEAD);
    assert_eq!(decoded.control.client_id, 0xBEEF);
    assert_eq!(decoded.control.flags & 0xFFFF, 0xCAFE);
    assert_eq!(decoded.control.seek_pk, 0x1111u128 | (0x2222u128 << 64));
    assert_eq!(decoded.control.seek_col_idx, 0x3333);
    assert_eq!(decoded.control.request_id, 0x4444);
}

#[test]
fn peek_client_control_on_valid_wire() {
    let wire = encode_wire(99, 0xCAFE_BABE, 0, 0u128, 0, 0, STATUS_OK, b"", None, None, None);
    let ctrl = peek_client_control(&wire).unwrap();
    assert_eq!(ctrl.target_id, 99);
    assert_eq!(ctrl.client_id, 0xCAFE_BABE);
}

#[test]
fn peek_client_control_rejects_short_data() {
    // Any slice shorter than WAL_HEADER_SIZE must fail.
    let result = peek_client_control(&[0u8; 10]);
    assert!(
        result.is_err(),
        "peek_client_control should reject slices < WAL_HEADER_SIZE"
    );
}

#[test]
fn decode_wire_truncated_schema_block_returns_err() {
    let sd = simple_schema();
    let names: Vec<&[u8]> = vec![b"id", b"val"];
    // Encode with schema but no data.
    let wire = encode_wire(1, 0, 0, 0u128, 0, 0, STATUS_OK, b"", Some(&sd), Some(&names), None);
    // The control block occupies [0..ctrl_size). Truncate in the middle of the
    // schema block (keep only the first WAL_HEADER_SIZE bytes of it so the
    // header is readable but the body is missing).
    let ctrl_size = u32::from_le_bytes(
        wire[gnitz_wire::WAL_OFF_SIZE..gnitz_wire::WAL_OFF_SIZE + 4]
            .try_into()
            .unwrap(),
    ) as usize;
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
        1,
        0,
        0,
        0u128,
        0,
        0,
        STATUS_OK,
        b"",
        Some(&sd),
        Some(&names),
        Some(&batch),
    );
    // Find where the data block starts: after control + schema blocks.
    let ctrl_size = u32::from_le_bytes(
        wire[gnitz_wire::WAL_OFF_SIZE..gnitz_wire::WAL_OFF_SIZE + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let schema_size = u32::from_le_bytes(
        wire[ctrl_size + gnitz_wire::WAL_OFF_SIZE..ctrl_size + gnitz_wire::WAL_OFF_SIZE + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let data_start = ctrl_size + schema_size;
    // Truncate in the middle of the data block.
    let truncated = &wire[..data_start + gnitz_wire::WAL_HEADER_SIZE / 2];
    let result = decode_wire(truncated);
    assert!(result.is_err(), "decode_wire should reject truncated data block");
}

/// The ctrl-block round-trip through the shared `gnitz_wire::control` codec:
/// every variable field distinct and non-zero so a swapped offset corrupts the
/// bytes detectably; `offset = 64` so writes accidentally indexing through
/// `out[offset + OFF_X..]` rather than the sub-slice are also caught. Runs
/// with and without the engine's checksum stamp — `peek_control_block` never
/// verifies the checksum, so both frames must decode identically.
#[test]
fn encode_ctrl_block_direct_roundtrips() {
    const OFFSET: usize = 64;
    let target_id: u64 = 0x1111_2222_3333_4444;
    let client_id: u64 = 0x5555_6666_7777_8888;
    let wire_flags: u64 = 0x9999_AAAA_BBBB_CCCC;
    let seek_pk: u128 = (0xDDDD_EEEE_FFFF_0011u128 << 64) | 0x2233_4455_6677_8899u128;
    let seek_col_idx: u64 = 0xAA_BB_CC_DD_EE_FF_00_11;
    let request_id: u64 = 0x1234_5678_9ABC_DEF0;
    let status: u32 = 0xDEAD_BEEF;

    for &checksum in &[false, true] {
        let mut buf = vec![0u8; OFFSET + CTRL_BLOCK_SIZE_NO_BLOB];
        let n = encode_ctrl_block_direct(
            &mut buf,
            OFFSET,
            target_id,
            client_id,
            wire_flags,
            seek_pk,
            seek_col_idx,
            request_id,
            status,
            b"",
            &[],
            checksum,
        );
        assert_eq!(
            n, CTRL_BLOCK_SIZE_NO_BLOB,
            "encoder size mismatch (checksum={checksum})"
        );
        let dec = peek_control_block(&buf[OFFSET..OFFSET + n]).expect("decode");
        assert_eq!(dec.target_id, target_id);
        assert_eq!(dec.client_id, client_id);
        assert_eq!(dec.flags, wire_flags);
        assert_eq!(dec.seek_pk, seek_pk);
        assert_eq!(dec.seek_col_idx, seek_col_idx);
        assert_eq!(dec.request_id, request_id);
        assert_eq!(dec.status, status);
        assert_eq!(dec.block_size, n);
    }
}

// ---------------------------------------------------------------------------
// Scan chunking tests
// ---------------------------------------------------------------------------

use crate::runtime::wire::{
    decode_wire_ipc, decode_wire_ipc_with_schema, encode_wire_into_range, wire_flags_set_schema_version,
    wire_size_range, SchemaWithVersion, FLAG_CONTINUATION,
};

fn make_wire_safe_batch(n: usize) -> Batch {
    let sd = simple_schema();
    let mut b = Batch::with_schema(sd, n.max(1));
    for i in 0..n {
        b.extend_pk(i as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &(i as u64).to_le_bytes());
        b.count += 1;
    }
    b
}

/// `wire_size_range` must match the actual encoded byte count.
#[test]
fn wire_size_range_matches_encoded_size() {
    let sd = simple_schema();
    let batch = make_wire_safe_batch(8);

    for count in [0usize, 1, 4, 8] {
        let sz = wire_size_range(STATUS_OK, &[], Some(&sd), None, &batch, count, None);
        let mut buf = vec![0u8; sz];
        let written = encode_wire_into_range(&mut buf, 0, 1, 0, 0, 0, STATUS_OK, Some(&sd), &batch, 0, count, None);
        assert_eq!(written, sz, "wire_size_range mismatch for count={count}");
    }
}

/// `wire_size_range` with count=1 minus count=0 gives positive per-row delta.
#[test]
fn wire_size_range_positive_per_row_delta() {
    let sd = simple_schema();
    let batch = make_wire_safe_batch(1);
    let sz0 = wire_size_range(STATUS_OK, &[], Some(&sd), None, &batch, 0, None);
    let sz1 = wire_size_range(STATUS_OK, &[], Some(&sd), None, &batch, 1, None);
    assert!(sz1 > sz0, "adding 1 row must increase wire size");
}

/// `encode_wire_into_range` round-trips a sub-range of a batch correctly.
#[test]
fn encode_range_roundtrip() {
    let sd = simple_schema();
    let batch = make_wire_safe_batch(8);

    // Encode rows [2, 5) into a wire frame with the schema block.
    let sz = wire_size_range(STATUS_OK, &[], Some(&sd), None, &batch, 3, None);
    let mut buf = vec![0u8; sz];
    encode_wire_into_range(&mut buf, 0, 1, 0, 0, 0, STATUS_OK, Some(&sd), &batch, 2, 3, None);

    // Decode and verify row values.
    let decoded = decode_wire_ipc(&buf).expect("decode_wire_ipc");
    let b = decoded.data_batch.expect("data_batch");
    assert_eq!(b.count, 3);
    for i in 0..3usize {
        assert_eq!(b.get_pk(i), (i + 2) as u128);
    }
}

/// A continuation frame (FLAG_HAS_DATA, no FLAG_HAS_SCHEMA) can be decoded
/// using `decode_wire_ipc_with_schema` with a versioned schema hint.
#[test]
fn continuation_frame_decoded_with_schema_hint() {
    let sd = simple_schema();
    let batch = make_wire_safe_batch(4);

    // Encode a continuation frame: no schema, FLAG_CONTINUATION set.
    // Embed server_version=7 in wire_flags bits 24-39.
    let server_version: u16 = 7;
    let frame_flags = wire_flags_set_schema_version(FLAG_CONTINUATION, server_version);
    let sz = wire_size_range(STATUS_OK, &[], None, None, &batch, 4, None);
    let mut buf = vec![0u8; sz];
    encode_wire_into_range(&mut buf, 0, 1, 0, frame_flags, 0, STATUS_OK, None, &batch, 0, 4, None);

    // decode_wire_ipc must fail (no schema in frame, no hint).
    assert!(
        decode_wire_ipc(&buf).is_err(),
        "decode_wire_ipc should fail for continuation frame without schema"
    );

    // decode_wire_ipc_with_schema with matching version must succeed.
    let decoded = decode_wire_ipc_with_schema(
        &buf,
        SchemaWithVersion {
            descriptor: &sd,
            version: server_version,
        },
    )
    .expect("decode_wire_ipc_with_schema");
    let b = decoded.data_batch.expect("data_batch");
    assert_eq!(b.count, 4);
    for i in 0..4usize {
        assert_eq!(b.get_pk(i), i as u128);
    }

    // decode_wire_ipc_with_schema with mismatched version must fail.
    let err = decode_wire_ipc_with_schema(
        &buf,
        SchemaWithVersion {
            descriptor: &sd,
            version: server_version + 1,
        },
    );
    assert!(err.is_err(), "version mismatch must return Err");
}

/// The blob fallback path (an error message present) round-trips through the
/// shared codec at a non-zero offset.
#[test]
fn encode_ctrl_block_direct_error_path_roundtrips() {
    const OFFSET: usize = 32;
    let err = b"something went wrong";
    let mut buf = vec![0u8; OFFSET + 1024];
    let n = encode_ctrl_block_direct(
        &mut buf,
        OFFSET,
        7,
        11,
        13,
        17u128,
        19,
        23,
        STATUS_ERROR,
        err,
        &[],
        true,
    );
    let dec = peek_control_block(&buf[OFFSET..OFFSET + n]).expect("decode");
    assert_eq!(dec.status, STATUS_ERROR);
    assert_eq!(dec.error_msg, err);
    assert_eq!(dec.block_size, n);
}

/// Round-trip the new `seek_pk_extra` blob through encode + decode.
///
/// Exercises three cases: empty (the universal hot path), short
/// (≤ German-string inline threshold, no blob spill), and long (spills
/// into the shared blob region alongside `error_msg`). The decoder must
/// resolve a single shared blob slice and feed it to both German-string
/// decodes, so the combined case is the load-bearing one.
#[test]
fn ctrl_block_seek_pk_extra_roundtrip() {
    // Case 1: both empty — the empty seek_pk_extra path the direct/IPC
    // equivalence test already covers, asserted here from the decoder side.
    let mut buf = vec![0u8; 1024];
    let n = encode_ctrl_block_direct(
        &mut buf, 0, /*target*/ 1, /*client*/ 2, /*flags*/ 3, /*seek_pk*/ 4u128,
        /*seek_col_idx*/ 5, /*request_id*/ 6, STATUS_OK, b"", b"", true,
    );
    let dec = peek_control_block(&buf[..n]).expect("decode empty");
    assert_eq!(dec.error_msg, Vec::<u8>::new());
    assert_eq!(dec.seek_pk_extra, Vec::<u8>::new());
    assert_eq!(dec.seek_pk, 4u128);

    // Case 2: seek_pk_extra short (≤12 bytes, inline; no blob spill).
    let short = b"abcd"; // 4 bytes, fits inline
    let mut buf = vec![0u8; 1024];
    let n = encode_ctrl_block_direct(&mut buf, 0, 1, 2, 3, 4u128, 5, 6, STATUS_OK, b"", short, true);
    let dec = peek_control_block(&buf[..n]).expect("decode short");
    assert_eq!(dec.error_msg, Vec::<u8>::new());
    assert_eq!(dec.seek_pk_extra, short);

    // Case 3: both non-empty, both long enough to spill into the shared
    // blob region. The decoder must resolve REGION_BLOB once and pass the
    // same slice to both German-string decodes.
    let err = b"this error message is definitely longer than twelve bytes";
    let extra = b"and so is this wide-pk-extra blob payload past 12B";
    let mut buf = vec![0u8; 1024];
    let n = encode_ctrl_block_direct(&mut buf, 0, 1, 2, 3, 4u128, 5, 6, STATUS_ERROR, err, extra, true);
    let dec = peek_control_block(&buf[..n]).expect("decode long");
    assert_eq!(dec.error_msg, err);
    assert_eq!(dec.seek_pk_extra, extra);
    assert_eq!(dec.status, STATUS_ERROR);
}
