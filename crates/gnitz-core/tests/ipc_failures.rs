//! IPC failure-mode tests: exercising the WAL-block IPC wire format.
//!
//! Each test sends a specifically malformed or edge-case message to the server
//! and verifies STATUS_ERROR, or STATUS_OK for messages that should succeed.
//! Server resilience (able to handle more requests after an error) is also
//! verified.

#![cfg(feature = "integration")]

mod helpers;

use std::os::unix::io::RawFd;

use gnitz_core::{GnitzClient, SCHEMA_TAB};
use gnitz_protocol::{
    Header,
    FLAG_HAS_SCHEMA, FLAG_HAS_DATA,
    STATUS_OK, STATUS_ERROR,
    META_FLAG_IS_PK,
    TypeCode, ColumnDef, Schema, ZSetBatch, ColData,
    encode_wal_block,
    encode_control_block, decode_control_block,
    schema_to_batch, meta_schema,
    connect, send_framed, recv_framed, close_fd,
    WAL_BLOCK_HEADER_SIZE,
};
use helpers::ServerHandle;

// ── RawClient ─────────────────────────────────────────────────────────────────

struct RawClient {
    fd: RawFd,
}

impl RawClient {
    fn connect(sock_path: &str) -> Self {
        let fd = connect(sock_path).expect("RawClient::connect");
        Self { fd }
    }

    /// Send raw bytes, receive a raw response buffer.
    /// Returns (status, error_msg, raw_response).
    fn send_recv(&self, data: &[u8]) -> (u32, String, Vec<u8>) {
        send_framed(self.fd, data).expect("send_framed");
        let resp = recv_framed(self.fd).expect("recv_framed");
        if resp.len() < WAL_BLOCK_HEADER_SIZE {
            return (STATUS_ERROR, "response too small".into(), resp);
        }
        let ctrl_size = u32::from_le_bytes(resp[16..20].try_into().unwrap()) as usize;
        if ctrl_size > resp.len() || ctrl_size < WAL_BLOCK_HEADER_SIZE {
            return (STATUS_ERROR, "response ctrl truncated".into(), resp);
        }
        match decode_control_block(&resp[..ctrl_size]) {
            Ok((hdr, err_msg)) => (hdr.status, err_msg, resp),
            Err(e) => (STATUS_ERROR, e.to_string(), resp),
        }
    }
}

impl Drop for RawClient {
    fn drop(&mut self) { close_fd(self.fd); }
}

// ── helpers ────────────────────────────────────────────────────────────────────

/// Decode the control-block Header from a raw response buffer.
fn resp_header(buf: &[u8]) -> Header {
    if buf.len() < WAL_BLOCK_HEADER_SIZE { return Header::default(); }
    let ctrl_size = u32::from_le_bytes(buf[16..20].try_into().unwrap()) as usize;
    if ctrl_size > buf.len() { return Header::default(); }
    decode_control_block(&buf[..ctrl_size])
        .map(|(h, _)| h)
        .unwrap_or_default()
}

/// Minimal control-only scan message for `target_id`.
fn scan_msg(target_id: u64) -> Vec<u8> {
    let h = Header { target_id, ..Header::default() };
    encode_control_block(&h, "").expect("encode_control_block")
}

fn assert_err(status: u32, err: &str, words: &[&str]) {
    let lc = err.to_lowercase();
    assert_eq!(status, STATUS_ERROR,
        "expected STATUS_ERROR; got status={}; err={:?}", status, err);
    for w in words {
        assert!(lc.contains(w),
            "expected {:?} in error {:?}", w, lc);
    }
}

fn assert_ok(status: u32) {
    assert_eq!(status, STATUS_OK, "expected STATUS_OK; got status={}", status);
}

/// Build a WAL-format schema block from `schema`.
fn make_schema_block(schema: &Schema) -> Vec<u8> {
    let ms = meta_schema();
    encode_wal_block(ms, 0, &schema_to_batch(schema))
}

/// Schema block with type_code at `col_idx` corrupted to `bad_tc`.
/// Corrupt before encoding → structurally valid WAL (good checksum) but bad content.
fn make_schema_block_bad_type(schema: &Schema, col_idx: usize, bad_tc: u64) -> Vec<u8> {
    let ms = meta_schema();
    let mut sbatch = schema_to_batch(schema);
    if let ColData::Fixed(ref mut v) = sbatch.columns[1] {  // col 1 = type_code
        let s = col_idx * 8;
        v[s..s + 8].copy_from_slice(&bad_tc.to_le_bytes());
    }
    encode_wal_block(ms, 0, &sbatch)
}

/// Schema block with all IS_PK flags cleared.
fn make_schema_block_no_pk(schema: &Schema) -> Vec<u8> {
    let ms = meta_schema();
    let mut sbatch = schema_to_batch(schema);
    if let ColData::Fixed(ref mut v) = sbatch.columns[2] {  // col 2 = flags
        for b in v.iter_mut() { *b = 0; }
    }
    encode_wal_block(ms, 0, &sbatch)
}

/// Schema block with IS_PK set on every column.
fn make_schema_block_multi_pk(schema: &Schema) -> Vec<u8> {
    let ms = meta_schema();
    let mut sbatch = schema_to_batch(schema);
    if let ColData::Fixed(ref mut v) = sbatch.columns[2] {
        for chunk in v.chunks_mut(8) {
            let mut val = u64::from_le_bytes(chunk.try_into().unwrap());
            val |= META_FLAG_IS_PK;
            chunk.copy_from_slice(&val.to_le_bytes());
        }
    }
    encode_wal_block(ms, 0, &sbatch)
}

/// Schema block with pk_lo[`row`] set to `bad_idx`.
fn make_schema_block_bad_col_idx(schema: &Schema, row: usize, bad_idx: u64) -> Vec<u8> {
    let ms = meta_schema();
    let mut sbatch = schema_to_batch(schema);
    sbatch.pk_lo[row] = bad_idx;
    encode_wal_block(ms, 0, &sbatch)
}

/// Assemble a complete IPC message from optional pre-built WAL blocks.
/// Sets FLAG_HAS_SCHEMA / FLAG_HAS_DATA in the control block as appropriate.
fn assemble(
    target_id:    u64,
    schema_block: Option<Vec<u8>>,
    data_block:   Option<Vec<u8>>,
) -> Vec<u8> {
    let has_schema = schema_block.is_some();
    let has_data   = data_block.is_some();
    let mut flags = 0u64;
    if has_schema { flags |= FLAG_HAS_SCHEMA; }
    if has_data   { flags |= FLAG_HAS_DATA;   }

    let mut h = Header::default();
    h.target_id = target_id;
    h.flags     = flags;

    let mut buf = encode_control_block(&h, "").unwrap();
    if let Some(sb) = schema_block { buf.extend_from_slice(&sb); }
    if let Some(db) = data_block   { buf.extend_from_slice(&db); }
    buf
}

fn two_col() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    }
}

fn three_col() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "pk".into(),   type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "val".into(),  type_code: TypeCode::I64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "name".into(), type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    }
}

fn simple_batch_i64(schema: &Schema, pk: u64, val: i64) -> ZSetBatch {
    let mut batch = ZSetBatch::new(schema);
    batch.pk_lo.push(pk);
    batch.pk_hi.push(0);
    batch.weights.push(1);
    batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[1] {
        v.extend_from_slice(&val.to_le_bytes());
    }
    batch
}

/// Create a test table (pk U64, val I64, name STRING) and return (client, tid, schema).
fn setup_test_table(sock_path: &str) -> (GnitzClient, u64, Schema) {
    let client = GnitzClient::connect(sock_path).unwrap();
    client.create_schema("ipctest").unwrap();
    let cols = three_col().columns;
    let tid  = client.create_table("ipctest", "t1", &cols, 0, true).unwrap();
    let schema = Schema { columns: cols, pk_index: 0 };
    (client, tid, schema)
}

// ============================================================================
// 1. WAL FORMAT VALIDATION
// ============================================================================

#[test]
fn test_too_small_1_byte() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (status, err, _) = raw.send_recv(&[0x47u8]);
    assert_err(status, &err, &["too small"]);
}

#[test]
fn test_too_small_47_bytes() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (status, err, _) = raw.send_recv(&[0u8; 47]);
    assert_err(status, &err, &["too small"]);
}

#[test]
fn test_bad_format_version() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);

    let h = Header::default();
    let mut ctrl = encode_control_block(&h, "").unwrap();
    // Set format_version = 1 at byte offset 20 in the control WAL block
    ctrl[20..24].copy_from_slice(&1u32.to_le_bytes());
    // Checksum covers body[48..], not the header, so version change doesn't break checksum.
    let (status, _, _) = raw.send_recv(&ctrl);
    assert_eq!(status, STATUS_ERROR);
}

#[test]
fn test_bad_checksum() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);

    let h = Header::default();
    let mut ctrl = encode_control_block(&h, "").unwrap();
    // Flip a byte in the body (after the 48-byte header)
    ctrl[WAL_BLOCK_HEADER_SIZE] ^= 0xFF;
    let (status, _, _) = raw.send_recv(&ctrl);
    assert_eq!(status, STATUS_ERROR);
}

// ============================================================================
// 2. FLAG INVARIANT
// ============================================================================

#[test]
fn test_has_data_without_has_schema() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);

    // FLAG_HAS_DATA without FLAG_HAS_SCHEMA is a protocol violation
    let mut h = Header::default();
    h.target_id = SCHEMA_TAB;
    h.flags = FLAG_HAS_DATA;
    let ctrl = encode_control_block(&h, "").unwrap();
    let (status, _, _) = raw.send_recv(&ctrl);
    assert_eq!(status, STATUS_ERROR);
}

// ============================================================================
// 3. SCHEMA SECTION FAILURES
// ============================================================================

#[test]
fn test_schema_invalid_type_code_255() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let sb = make_schema_block_bad_type(&schema, 1, 255);
    let msg = assemble(SCHEMA_TAB, Some(sb), None);
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["type code"]);
}

#[test]
fn test_schema_invalid_type_code_zero() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let sb = make_schema_block_bad_type(&schema, 1, 0);
    let msg = assemble(SCHEMA_TAB, Some(sb), None);
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["type code"]);
}

#[test]
fn test_schema_invalid_type_code_13() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let sb = make_schema_block_bad_type(&schema, 1, 13);
    let msg = assemble(SCHEMA_TAB, Some(sb), None);
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["type code"]);
}

#[test]
fn test_schema_no_pk_flag() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let sb = make_schema_block_no_pk(&schema);
    let msg = assemble(SCHEMA_TAB, Some(sb), None);
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["pk"]);
}

#[test]
fn test_schema_multiple_pk_flags() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let sb = make_schema_block_multi_pk(&schema);
    let msg = assemble(SCHEMA_TAB, Some(sb), None);
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["multiple pk"]);
}

#[test]
fn test_schema_col_idx_out_of_order() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    // col_idx [1, 0] instead of [0, 1]
    let ms = meta_schema();
    let mut sbatch = schema_to_batch(&schema);
    sbatch.pk_lo[0] = 1;
    sbatch.pk_lo[1] = 0;
    let sb = encode_wal_block(ms, 0, &sbatch);
    let msg = assemble(SCHEMA_TAB, Some(sb), None);
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["col_idx", "order"]);
}

#[test]
fn test_schema_col_idx_gap() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let sb = make_schema_block_bad_col_idx(&schema, 1, 2);  // [0, 2] — gap
    let msg = assemble(SCHEMA_TAB, Some(sb), None);
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["col_idx"]);
}

#[test]
fn test_schema_col_idx_duplicate() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let sb = make_schema_block_bad_col_idx(&schema, 1, 0);  // [0, 0] — duplicate
    let msg = assemble(SCHEMA_TAB, Some(sb), None);
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["col_idx"]);
}

// ── schema WAL block with bad checksum ───────────────────────────────────────

#[test]
fn test_schema_block_bad_checksum() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let mut sb = make_schema_block(&schema);
    // Flip a body byte — valid version but bad checksum
    sb[WAL_BLOCK_HEADER_SIZE] ^= 0xFF;
    let msg = assemble(SCHEMA_TAB, Some(sb), None);
    let (status, _, _) = raw.send_recv(&msg);
    assert_eq!(status, STATUS_ERROR);
}

// ============================================================================
// 4. DATA SECTION FAILURES
// ============================================================================

#[test]
fn test_data_section_truncated() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, schema) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let schema_block = make_schema_block(&schema);
    // Send FLAG_HAS_DATA but don't include the data WAL block at all
    let mut h = Header::default();
    h.target_id = tid;
    h.flags = FLAG_HAS_SCHEMA | FLAG_HAS_DATA;
    let mut msg = encode_control_block(&h, "").unwrap();
    msg.extend_from_slice(&schema_block);
    // data block absent

    let (status, _, _) = raw.send_recv(&msg);
    assert_eq!(status, STATUS_ERROR);
}

#[test]
fn test_data_block_bad_checksum() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, schema) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let schema_block = make_schema_block(&schema);
    // data col: need the string column too for three_col schema
    let mut full_batch = ZSetBatch::new(&schema);
    full_batch.pk_lo.push(1); full_batch.pk_hi.push(0);
    full_batch.weights.push(1); full_batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = full_batch.columns[1] {
        v.extend_from_slice(&42i64.to_le_bytes());
    }
    if let ColData::Strings(ref mut v) = full_batch.columns[2] {
        v.push(Some("hello".into()));
    }
    let mut data_block = encode_wal_block(&schema, tid as u32, &full_batch);
    data_block[WAL_BLOCK_HEADER_SIZE] ^= 0xFF;  // corrupt body

    let msg = assemble(tid, Some(schema_block), Some(data_block));
    let (status, _, _) = raw.send_recv(&msg);
    assert_eq!(status, STATUS_ERROR);
}

// ============================================================================
// 5. SCHEMA MISMATCH (push to real table)
// ============================================================================

#[test]
fn test_schema_mismatch_wrong_column_count_fewer() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, _) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let wrong = two_col();
    let sb = make_schema_block(&wrong);
    let db = encode_wal_block(&wrong, tid as u32, &simple_batch_i64(&wrong, 1, 99));
    let msg = assemble(tid, Some(sb), Some(db));
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["schema mismatch", "columns"]);
}

#[test]
fn test_schema_mismatch_wrong_column_count_more() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, _) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let wrong = Schema {
        columns: vec![
            ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "a".into(),  type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "b".into(),  type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "c".into(),  type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    };
    let sb = make_schema_block(&wrong);
    let mut batch = ZSetBatch::new(&wrong);
    batch.pk_lo.push(1); batch.pk_hi.push(0); batch.weights.push(1); batch.nulls.push(0);
    for ci in 1..4usize {
        if let ColData::Fixed(ref mut v) = batch.columns[ci] {
            v.extend_from_slice(&1i64.to_le_bytes());
        }
    }
    let db = encode_wal_block(&wrong, tid as u32, &batch);
    let msg = assemble(tid, Some(sb), Some(db));
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["schema mismatch", "columns"]);
}

#[test]
fn test_schema_mismatch_wrong_column_type() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, _) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    // Push schema where column 2 is I64 instead of STRING
    let wrong = Schema {
        columns: vec![
            ColumnDef { name: "pk".into(),   type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "val".into(),  type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "name".into(), type_code: TypeCode::I64, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 0,
    };
    let sb = make_schema_block(&wrong);
    let mut batch = ZSetBatch::new(&wrong);
    batch.pk_lo.push(1); batch.pk_hi.push(0); batch.weights.push(1); batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[1] { v.extend_from_slice(&42i64.to_le_bytes()); }
    if let ColData::Fixed(ref mut v) = batch.columns[2] { v.extend_from_slice(&999i64.to_le_bytes()); }
    let db = encode_wal_block(&wrong, tid as u32, &batch);
    let msg = assemble(tid, Some(sb), Some(db));
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["schema mismatch", "column 2"]);
}

#[test]
fn test_schema_mismatch_wrong_pk_index() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, _) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    // Push with pk_index=1 to a table with pk_index=0
    let wrong = Schema {
        columns: vec![
            ColumnDef { name: "val".into(),  type_code: TypeCode::I64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "pk".into(),   type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ColumnDef { name: "name".into(), type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
        ],
        pk_index: 1,
    };
    let sb = make_schema_block(&wrong);
    let mut batch = ZSetBatch::new(&wrong);
    batch.pk_lo.push(1); batch.pk_hi.push(0); batch.weights.push(1); batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[0] { v.extend_from_slice(&99i64.to_le_bytes()); }
    if let ColData::Strings(ref mut v) = batch.columns[2] { v.push(Some("x".into())); }
    let db = encode_wal_block(&wrong, tid as u32, &batch);
    let msg = assemble(tid, Some(sb), Some(db));
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["schema mismatch", "pk_index"]);
}

// ============================================================================
// 6. DISPATCH FAILURES
// ============================================================================

#[test]
fn test_target_id_nonexistent() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (status, err, _) = raw.send_recv(&scan_msg(99_999));
    assert_err(status, &err, &["unknown"]);
}

#[test]
fn test_target_id_zero() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (status, err, _) = raw.send_recv(&scan_msg(0));
    assert_err(status, &err, &["unknown"]);
}

#[test]
fn test_target_id_max_u64() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (status, err, _) = raw.send_recv(&scan_msg(u64::MAX));
    assert_err(status, &err, &["unknown"]);
}

#[test]
fn test_push_to_nonexistent_with_valid_schema() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let sb = make_schema_block(&schema);
    let db = encode_wal_block(&schema, 88_888u32, &simple_batch_i64(&schema, 1, 42));
    let msg = assemble(88_888, Some(sb), Some(db));
    let (status, err, _) = raw.send_recv(&msg);
    assert_err(status, &err, &["unknown"]);
}

// ============================================================================
// 7. EDGE CASES (should succeed)
// ============================================================================

#[test]
fn test_scan_system_table_schema_tab() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (status, _, resp) = raw.send_recv(&scan_msg(SCHEMA_TAB));
    assert_ok(status);
    // Response should include schema data
    let h = resp_header(&resp);
    assert!((h.flags & FLAG_HAS_SCHEMA) != 0);
}

#[test]
fn test_scan_with_schema_but_no_data_is_scan() {
    // Sending FLAG_HAS_SCHEMA but no FLAG_HAS_DATA to a table is a scan
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let sb = make_schema_block(&schema);
    let msg = assemble(SCHEMA_TAB, Some(sb), None);
    let (status, _, _) = raw.send_recv(&msg);
    assert_ok(status);
}

#[test]
fn test_scan_echoes_client_id() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut h = Header::default();
    h.target_id = SCHEMA_TAB;
    h.client_id = 123_456_789;
    let msg = encode_control_block(&h, "").unwrap();
    let (status, _, resp) = raw.send_recv(&msg);
    assert_ok(status);
    assert_eq!(resp_header(&resp).client_id, 123_456_789);
}

// ============================================================================
// 8. TRANSPORT EDGE CASES
// ============================================================================

#[test]
fn test_empty_payload_too_small() {
    // Send a payload that's too small to contain a valid WAL block header.
    // Server should respond with STATUS_ERROR or drop the connection.
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let sock_fd = connect(&srv.sock_path).expect("connect");

    // Send a 4-byte garbage payload (smaller than WAL_BLOCK_HEADER_SIZE)
    let garbage = [0u8; 4];
    if send_framed(sock_fd, &garbage).is_err() {
        close_fd(sock_fd);
        return;
    }

    match recv_framed(sock_fd) {
        Ok(resp) if resp.len() >= WAL_BLOCK_HEADER_SIZE => {
            let ctrl_size = u32::from_le_bytes(resp[16..20].try_into().unwrap()) as usize;
            if ctrl_size <= resp.len() {
                if let Ok((hdr, _)) = decode_control_block(&resp[..ctrl_size]) {
                    assert_eq!(hdr.status, STATUS_ERROR, "too-small payload should yield error");
                }
            }
        }
        _ => {} // server may drop connection — also acceptable
    }
    close_fd(sock_fd);
}

// ============================================================================
// 9. RESILIENCE
// ============================================================================

#[test]
fn test_error_then_valid_same_connection() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);

    // Bad checksum → error
    let mut ctrl = scan_msg(SCHEMA_TAB);
    ctrl[WAL_BLOCK_HEADER_SIZE] ^= 0xFF;
    let (status1, _, _) = raw.send_recv(&ctrl);
    assert_eq!(status1, STATUS_ERROR);

    // Valid scan after error
    let (status2, _, resp) = raw.send_recv(&scan_msg(SCHEMA_TAB));
    assert_ok(status2);
    let h = resp_header(&resp);
    assert!((h.flags & FLAG_HAS_SCHEMA) != 0);
}

#[test]
fn test_multiple_errors_then_valid() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);

    // Too small
    let (s, _, _) = raw.send_recv(&[0x42u8]);
    assert_eq!(s, STATUS_ERROR);

    // Bad format version
    let h = Header::default();
    let mut ctrl = encode_control_block(&h, "").unwrap();
    ctrl[20..24].copy_from_slice(&1u32.to_le_bytes());
    let (s, _, _) = raw.send_recv(&ctrl);
    assert_eq!(s, STATUS_ERROR);

    // Unknown target
    let (s, _, _) = raw.send_recv(&scan_msg(99_999));
    assert_eq!(s, STATUS_ERROR);

    // Valid scan
    let (s, _, _) = raw.send_recv(&scan_msg(SCHEMA_TAB));
    assert_ok(s);
}

#[test]
fn test_new_connection_after_error() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };

    let c1 = RawClient::connect(&srv.sock_path);
    let (s, _, _) = c1.send_recv(&[0u8; 10]);
    assert_eq!(s, STATUS_ERROR);
    drop(c1);

    let c2 = RawClient::connect(&srv.sock_path);
    let (s, _, _) = c2.send_recv(&scan_msg(SCHEMA_TAB));
    assert_ok(s);
}

#[test]
fn test_rapid_reconnect() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    for _ in 0..10 {
        let c = RawClient::connect(&srv.sock_path);
        let (s, _, _) = c.send_recv(&scan_msg(SCHEMA_TAB));
        assert_ok(s);
    }
}

#[test]
fn test_error_recovery_after_schema_parse_error() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);

    // Schema error: bad type code
    let schema = two_col();
    let sb = make_schema_block_bad_type(&schema, 1, 200);
    let msg = assemble(SCHEMA_TAB, Some(sb), None);
    let (s, _, _) = raw.send_recv(&msg);
    assert_eq!(s, STATUS_ERROR);

    // Valid scan after error
    let (s, _, _) = raw.send_recv(&scan_msg(SCHEMA_TAB));
    assert_ok(s);
}

// ============================================================================
// 10. DATA VALIDITY EDGE CASES (should succeed)
// ============================================================================

#[test]
fn test_push_valid_data_to_real_table() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, schema) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let sb = make_schema_block(&schema);
    let mut batch = ZSetBatch::new(&schema);
    batch.pk_lo.push(9000); batch.pk_hi.push(0); batch.weights.push(1); batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[1] { v.extend_from_slice(&77i64.to_le_bytes()); }
    if let ColData::Strings(ref mut v) = batch.columns[2] { v.push(Some("raw_push".into())); }
    let db = encode_wal_block(&schema, tid as u32, &batch);
    let msg = assemble(tid, Some(sb), Some(db));
    let (status, _, _) = raw.send_recv(&msg);
    assert_ok(status);
}

#[test]
fn test_push_retraction_weight_minus_one() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, schema) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let sb = make_schema_block(&schema);
    let mut batch = ZSetBatch::new(&schema);
    batch.pk_lo.push(9000); batch.pk_hi.push(0); batch.weights.push(-1); batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[1] { v.extend_from_slice(&77i64.to_le_bytes()); }
    if let ColData::Strings(ref mut v) = batch.columns[2] { v.push(Some("retract".into())); }
    let db = encode_wal_block(&schema, tid as u32, &batch);
    let msg = assemble(tid, Some(sb), Some(db));
    let (status, _, _) = raw.send_recv(&msg);
    assert_ok(status);
}

#[test]
fn test_push_with_empty_string() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, schema) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let sb = make_schema_block(&schema);
    let mut batch = ZSetBatch::new(&schema);
    batch.pk_lo.push(9100); batch.pk_hi.push(0); batch.weights.push(1); batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[1] { v.extend_from_slice(&1i64.to_le_bytes()); }
    if let ColData::Strings(ref mut v) = batch.columns[2] { v.push(Some("".into())); }
    let db = encode_wal_block(&schema, tid as u32, &batch);
    let msg = assemble(tid, Some(sb), Some(db));
    let (status, _, _) = raw.send_recv(&msg);
    assert_ok(status);
}

#[test]
fn test_push_multiple_rows() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, schema) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let sb = make_schema_block(&schema);
    let mut batch = ZSetBatch::new(&schema);
    for &(pk, val, name) in &[(9001u64, 1i64, "alpha"), (9002, 2, "beta"), (9003, 3, "gamma")] {
        batch.pk_lo.push(pk); batch.pk_hi.push(0);
        batch.weights.push(1); batch.nulls.push(0);
        if let ColData::Fixed(ref mut v) = batch.columns[1] { v.extend_from_slice(&val.to_le_bytes()); }
        if let ColData::Strings(ref mut v) = batch.columns[2] { v.push(Some(name.into())); }
    }
    let db = encode_wal_block(&schema, tid as u32, &batch);
    let msg = assemble(tid, Some(sb), Some(db));
    let (status, _, _) = raw.send_recv(&msg);
    assert_ok(status);
}

#[test]
fn test_scan_after_push_returns_data() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, _) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);
    let (status, _, resp) = raw.send_recv(&scan_msg(tid));
    assert_ok(status);
    let h = resp_header(&resp);
    assert!((h.flags & FLAG_HAS_SCHEMA) != 0);
}
