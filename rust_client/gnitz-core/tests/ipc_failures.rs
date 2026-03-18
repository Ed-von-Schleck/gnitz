//! IPC failure-mode tests: ported from py_client/tests/test_ipc_failures.py
//!
//! Each test sends a specifically malformed or edge-case message to the server
//! and verifies STATUS_ERROR with a descriptive message, or STATUS_OK for
//! messages that should succeed.  Server resilience (able to handle more
//! requests after an error) is also verified.

#![cfg(feature = "integration")]

mod helpers;

use std::os::unix::io::RawFd;

use gnitz_core::{GnitzClient, SCHEMA_TAB};
use gnitz_protocol::{
    Header, MAGIC_V2, HEADER_SIZE, STATUS_OK, STATUS_ERROR,
    META_FLAG_IS_PK,
    TypeCode, ColumnDef, Schema, ZSetBatch, ColData,
    encode_zset, schema_to_batch, meta_schema,
    connect, send_memfd, recv_memfd, close_fd,
    align_up,
};
use helpers::ServerHandle;

// ── RawClient ─────────────────────────────────────────────────────────────

struct RawClient {
    fd: RawFd,
}

impl RawClient {
    fn connect(sock_path: &str) -> Self {
        let fd = connect(sock_path).expect("RawClient::connect");
        Self { fd }
    }

    fn send_recv(&self, data: &[u8]) -> (Header, Vec<u8>) {
        send_memfd(self.fd, data).expect("send_memfd");
        let resp = recv_memfd(self.fd).expect("recv_memfd");
        let hdr = Header::unpack(&resp).expect("Header::unpack on response");
        (hdr, resp)
    }
}

impl Drop for RawClient {
    fn drop(&mut self) { close_fd(self.fd); }
}

// ── helpers ────────────────────────────────────────────────────────────────

/// Minimal 96-byte scan message.
fn scan_msg(target_id: u64) -> Vec<u8> {
    let mut h = Header::default();
    h.target_id = target_id;
    h.pack().to_vec()
}

fn err_text(hdr: &Header, resp: &[u8]) -> String {
    if hdr.err_len == 0 { return String::new(); }
    let end = (HEADER_SIZE + hdr.err_len as usize).min(resp.len());
    String::from_utf8_lossy(&resp[HEADER_SIZE..end]).to_lowercase()
}

fn assert_err(hdr: &Header, resp: &[u8], words: &[&str]) {
    assert_eq!(hdr.status, STATUS_ERROR,
        "expected STATUS_ERROR (status={}); error text: {:?}", hdr.status, err_text(hdr, resp));
    let msg = err_text(hdr, resp);
    for w in words {
        assert!(msg.contains(w), "expected {:?} in error {:?}", w, msg);
    }
}

fn assert_ok(hdr: &Header) {
    assert_eq!(hdr.status, STATUS_OK, "expected STATUS_OK; got status={}", hdr.status);
}

/// Encode `schema` as META_SCHEMA ZSet section.
fn schema_sec(schema: &Schema) -> (Vec<u8>, u64, usize) {
    let ms  = meta_schema();
    let b   = schema_to_batch(schema);
    let n   = b.len();
    let (data, blob_sz) = encode_zset(ms, &b);
    (data, blob_sz, n)
}

/// Encode `schema` but corrupt type_code at row `col_idx` to `bad_tc`.
fn schema_sec_bad_type(schema: &Schema, col_idx: usize, bad_tc: u64) -> (Vec<u8>, u64, usize) {
    let ms  = meta_schema();
    let mut b = schema_to_batch(schema);
    if let ColData::Fixed(ref mut v) = b.columns[1] {
        let s = col_idx * 8;
        v[s..s + 8].copy_from_slice(&bad_tc.to_le_bytes());
    }
    let n = b.len();
    let (data, blob_sz) = encode_zset(ms, &b);
    (data, blob_sz, n)
}

/// Encode `schema` with no PK flag in any column.
fn schema_sec_no_pk(schema: &Schema) -> (Vec<u8>, u64, usize) {
    let ms  = meta_schema();
    let mut b = schema_to_batch(schema);
    if let ColData::Fixed(ref mut v) = b.columns[2] {
        for byte in v.iter_mut() { *byte = 0; }
    }
    let n = b.len();
    let (data, blob_sz) = encode_zset(ms, &b);
    (data, blob_sz, n)
}

/// Encode `schema` with both columns having IS_PK flag.
fn schema_sec_multi_pk(schema: &Schema) -> (Vec<u8>, u64, usize) {
    let ms  = meta_schema();
    let mut b = schema_to_batch(schema);
    if let ColData::Fixed(ref mut v) = b.columns[2] {
        // Set IS_PK=2 on every column
        for chunk in v.chunks_mut(8) {
            let mut val = u64::from_le_bytes(chunk.try_into().unwrap());
            val |= META_FLAG_IS_PK;
            chunk.copy_from_slice(&val.to_le_bytes());
        }
    }
    let n = b.len();
    let (data, blob_sz) = encode_zset(ms, &b);
    (data, blob_sz, n)
}

/// Encode `schema` with pk_lo[`row`] set to `bad_idx`.
fn schema_sec_bad_col_idx(schema: &Schema, row: usize, bad_idx: u64) -> (Vec<u8>, u64, usize) {
    let ms  = meta_schema();
    let mut b = schema_to_batch(schema);
    b.pk_lo[row] = bad_idx;
    let n = b.len();
    let (data, blob_sz) = encode_zset(ms, &b);
    (data, blob_sz, n)
}

/// Assemble a complete IPC message.
/// Layout: header(96) + pad-to-128 + [schema_sec] + [pad] + [data_sec]
fn assemble(
    target_id: u64,
    schema: Option<(&[u8], u64, usize)>,
    data:   Option<(&[u8], u64, usize, usize)>,  // (bytes, blob_sz, count, pk_index)
) -> Vec<u8> {
    let body_start = align_up(HEADER_SIZE, 64); // = 128 (err_len=0)

    let (sc, sblob) = schema.map(|(_, b, n)| (n as u64, b)).unwrap_or((0, 0));
    let (dc, dblob, pk_idx) = data
        .map(|(_, b, n, p)| (n as u64, b, p as u64))
        .unwrap_or((0, 0, 0));

    let mut h = Header::default();
    h.target_id      = target_id;
    h.schema_count   = sc;
    h.schema_blob_sz = sblob;
    h.data_count     = dc;
    h.data_blob_sz   = dblob;
    h.p4             = pk_idx;

    let mut msg = h.pack().to_vec();
    msg.resize(body_start, 0); // pad to body_start

    if let Some((sbytes, _, _)) = schema {
        let schema_end = body_start + sbytes.len();
        msg.extend_from_slice(sbytes);
        if let Some((dbytes, _, _, _)) = data {
            let data_start = align_up(schema_end, 64);
            msg.resize(data_start, 0);
            msg.extend_from_slice(dbytes);
        }
    } else if let Some((dbytes, _, _, _)) = data {
        msg.extend_from_slice(dbytes);
    }

    msg
}

fn two_col() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false },
            ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false },
        ],
        pk_index: 0,
    }
}

fn three_col() -> Schema {
    Schema {
        columns: vec![
            ColumnDef { name: "pk".into(),   type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "val".into(),  type_code: TypeCode::I64,    is_nullable: false },
            ColumnDef { name: "name".into(), type_code: TypeCode::String, is_nullable: true  },
        ],
        pk_index: 0,
    }
}

/// Simple I64-payload batch for two_col() schema.
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
// 1. HEADER VALIDATION
// ============================================================================

#[test]
fn test_header_wrong_magic_all_zeros() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (hdr, resp) = raw.send_recv(&[0u8; HEADER_SIZE]);
    assert_err(&hdr, &resp, &["magic"]);
}

#[test]
fn test_header_wrong_magic_random() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut h = Header::default();
    h.magic     = 0xDEAD_BEEF_CAFE_BABEu64;
    h.target_id = SCHEMA_TAB;
    let (hdr, resp) = raw.send_recv(&h.pack());
    assert_err(&hdr, &resp, &["magic"]);
}

#[test]
fn test_header_wrong_magic_off_by_one() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut h = Header::default();
    h.magic     = MAGIC_V2.wrapping_add(1);
    h.target_id = SCHEMA_TAB;
    let (hdr, resp) = raw.send_recv(&h.pack());
    assert_err(&hdr, &resp, &["magic"]);
}

#[test]
fn test_header_payload_too_small_1_byte() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (hdr, resp) = raw.send_recv(&[0x47u8]);
    assert_err(&hdr, &resp, &["too small"]);
}

#[test]
fn test_header_payload_too_small_50_bytes() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (hdr, resp) = raw.send_recv(&[0u8; 50]);
    assert_err(&hdr, &resp, &["too small"]);
}

#[test]
fn test_header_payload_too_small_95_bytes() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (hdr, resp) = raw.send_recv(&[0u8; 95]);
    assert_err(&hdr, &resp, &["too small"]);
}

#[test]
fn test_header_exactly_96_bytes_valid_scan() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let msg = scan_msg(SCHEMA_TAB);
    assert_eq!(msg.len(), HEADER_SIZE);
    let (hdr, _) = raw.send_recv(&msg);
    assert_ok(&hdr);
}

#[test]
fn test_header_valid_magic_garbage_rest_is_scan() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    // Valid magic + target_id, garbage in reserved area [80..96]
    let mut msg = scan_msg(SCHEMA_TAB);
    msg[80..96].copy_from_slice(&(0u8..16).collect::<Vec<_>>());
    let (hdr, _) = raw.send_recv(&msg);
    assert_ok(&hdr);
}

// ============================================================================
// 2. ERROR STRING BOUNDS
// ============================================================================

#[test]
fn test_err_len_exceeds_max() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut h = Header::default();
    h.target_id = SCHEMA_TAB;
    h.err_len   = 70_000;
    let mut msg = h.pack().to_vec();
    msg.extend_from_slice(&vec![0u8; 70_000]);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["safety limit"]);
}

#[test]
fn test_err_len_truncated() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut h = Header::default();
    h.target_id = SCHEMA_TAB;
    h.err_len   = 1000;
    let mut msg = h.pack().to_vec();
    // Only 104 bytes of payload (200 - 96), not 1000
    msg.extend_from_slice(&[0u8; 200 - HEADER_SIZE]);
    assert_eq!(msg.len(), 200);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["truncated"]);
}

#[test]
fn test_err_len_one_byte_over() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut h = Header::default();
    h.target_id = SCHEMA_TAB;
    h.err_len   = 11;
    let mut msg = h.pack().to_vec();
    msg.extend_from_slice(&[b'A'; 10]); // only 10 bytes, not 11
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["truncated"]);
}

// ============================================================================
// 3. PROTOCOL INVARIANT
// ============================================================================

#[test]
fn test_data_count_without_schema() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut h = Header::default();
    h.target_id   = SCHEMA_TAB;
    h.data_count  = 5;
    h.schema_count = 0;
    let mut msg = h.pack().to_vec();
    msg.extend_from_slice(&[0u8; 2048]);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["data_rows", "schema_rows"]);
}

#[test]
fn test_data_count_large_without_schema() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut h = Header::default();
    h.target_id   = SCHEMA_TAB;
    h.data_count  = 1_000_000;
    h.schema_count = 0;
    let mut msg = h.pack().to_vec();
    msg.extend_from_slice(&[0u8; 256]);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["data_rows", "schema_rows"]);
}

// ============================================================================
// 4. SCHEMA SECTION FAILURES
// ============================================================================

#[test]
fn test_schema_section_truncated_no_body() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut h = Header::default();
    h.target_id    = SCHEMA_TAB;
    h.schema_count = 3;
    // No body at all — structural data would start at 128
    let (hdr, resp) = raw.send_recv(&h.pack());
    assert_err(&hdr, &resp, &["truncated"]);
}

#[test]
fn test_schema_invalid_type_code_255() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let (sec, blob, count) = schema_sec_bad_type(&schema, 1, 255);
    let msg = assemble(SCHEMA_TAB, Some((&sec, blob, count)), None);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["type code"]);
}

#[test]
fn test_schema_invalid_type_code_zero() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let (sec, blob, count) = schema_sec_bad_type(&schema, 1, 0);
    let msg = assemble(SCHEMA_TAB, Some((&sec, blob, count)), None);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["type code"]);
}

#[test]
fn test_schema_invalid_type_code_13() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let (sec, blob, count) = schema_sec_bad_type(&schema, 1, 13);
    let msg = assemble(SCHEMA_TAB, Some((&sec, blob, count)), None);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["type code"]);
}

#[test]
fn test_schema_no_pk_flag() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let (sec, blob, count) = schema_sec_no_pk(&schema);
    let msg = assemble(SCHEMA_TAB, Some((&sec, blob, count)), None);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["pk"]);
}

#[test]
fn test_schema_multiple_pk_flags() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let (sec, blob, count) = schema_sec_multi_pk(&schema);
    let msg = assemble(SCHEMA_TAB, Some((&sec, blob, count)), None);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["multiple pk"]);
}

#[test]
fn test_schema_col_idx_out_of_order() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    // col_idx values [1, 0] instead of [0, 1]
    let ms  = meta_schema();
    let mut b = schema_to_batch(&schema);
    b.pk_lo[0] = 1;
    b.pk_lo[1] = 0;
    let n = b.len();
    let (sec, blob_sz) = encode_zset(ms, &b);
    let msg = assemble(SCHEMA_TAB, Some((&sec, blob_sz, n)), None);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["col_idx", "order"]);
}

#[test]
fn test_schema_col_idx_gap() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    // col_idx [0, 2] — gap at 1
    let (sec, blob, count) = schema_sec_bad_col_idx(&schema, 1, 2);
    let msg = assemble(SCHEMA_TAB, Some((&sec, blob, count)), None);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["col_idx"]);
}

#[test]
fn test_schema_col_idx_duplicate() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    // col_idx [0, 0] — duplicate
    let (sec, blob, count) = schema_sec_bad_col_idx(&schema, 1, 0);
    let msg = assemble(SCHEMA_TAB, Some((&sec, blob, count)), None);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["col_idx"]);
}

// ============================================================================
// 5. DATA SECTION FAILURES
// ============================================================================

#[test]
fn test_data_section_truncated_no_data_body() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let (ssec, sblob, scount) = schema_sec(&schema);
    // data_count=10 but no data bytes appended
    let mut h = Header::default();
    h.target_id      = SCHEMA_TAB;
    h.schema_count   = scount as u64;
    h.schema_blob_sz = sblob;
    h.data_count     = 10;
    let body_start = align_up(HEADER_SIZE, 64);
    let mut msg = h.pack().to_vec();
    msg.resize(body_start, 0);
    msg.extend_from_slice(&ssec);
    // no data section
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["truncated"]);
}

#[test]
fn test_data_section_truncated_column_buffer() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let (ssec, sblob, scount) = schema_sec(&schema);

    // Build correct data section for 5 rows, then truncate it
    let mut batch = ZSetBatch::new(&schema);
    for i in 0u64..5 {
        batch.pk_lo.push(i);
        batch.pk_hi.push(0);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(ref mut v) = batch.columns[1] {
            v.extend_from_slice(&(i as i64 * 10).to_le_bytes());
        }
    }
    let (dsec, dblob) = encode_zset(&schema, &batch);
    let truncated = &dsec[..dsec.len() / 2];

    let msg = assemble(
        SCHEMA_TAB,
        Some((&ssec, sblob, scount)),
        Some((truncated, dblob, 5, 0)),
    );
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["truncated"]);
}

#[test]
fn test_data_string_offset_beyond_blob() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = three_col();
    let (ssec, sblob, scount) = schema_sec(&schema);

    let mut batch = ZSetBatch::new(&schema);
    batch.pk_lo.push(1);
    batch.pk_hi.push(0);
    batch.weights.push(1);
    batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[1] {
        v.extend_from_slice(&42i64.to_le_bytes());
    }
    if let ColData::Strings(ref mut v) = batch.columns[2] {
        v.push(Some("hello".to_string()));
    }
    let (mut dsec, dblob) = encode_zset(&schema, &batch);

    // Find string column offset and corrupt string offset to huge value
    use gnitz_protocol::layout as codec_layout;
    let lay = codec_layout(&schema, 1);
    let str_col_off = lay.col_offsets[2]; // col 2 = name/string
    dsec[str_col_off..str_col_off + 4].copy_from_slice(&99_999u32.to_le_bytes());

    let msg = assemble(
        SCHEMA_TAB,
        Some((&ssec, sblob, scount)),
        Some((&dsec, dblob, 1, 0)),
    );
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["blob arena"]);
}

#[test]
fn test_data_string_length_overflows_blob() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = three_col();
    let (ssec, sblob, scount) = schema_sec(&schema);

    let mut batch = ZSetBatch::new(&schema);
    batch.pk_lo.push(1);
    batch.pk_hi.push(0);
    batch.weights.push(1);
    batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[1] {
        v.extend_from_slice(&42i64.to_le_bytes());
    }
    if let ColData::Strings(ref mut v) = batch.columns[2] {
        v.push(Some("hi".to_string()));
    }
    let (mut dsec, dblob) = encode_zset(&schema, &batch);

    use gnitz_protocol::layout as codec_layout;
    let lay = codec_layout(&schema, 1);
    let str_col_off = lay.col_offsets[2];
    // offset=0, length=dblob+100 → extends past blob end
    dsec[str_col_off..str_col_off + 4].copy_from_slice(&0u32.to_le_bytes());
    let bad_len = (dblob as u32).saturating_add(100);
    dsec[str_col_off + 4..str_col_off + 8].copy_from_slice(&bad_len.to_le_bytes());

    let msg = assemble(
        SCHEMA_TAB,
        Some((&ssec, sblob, scount)),
        Some((&dsec, dblob, 1, 0)),
    );
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["blob arena"]);
}

// ============================================================================
// 6. SCHEMA MISMATCH (push to real table)
// ============================================================================

#[test]
fn test_schema_mismatch_wrong_column_count_fewer() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, _) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    // Push 2-col schema to a 3-col table
    let wrong = two_col();
    let (ssec, sblob, scount) = schema_sec(&wrong);
    let batch = simple_batch_i64(&wrong, 1, 99);
    let (dsec, dblob) = encode_zset(&wrong, &batch);
    let msg = assemble(tid, Some((&ssec, sblob, scount)), Some((&dsec, dblob, 1, 0)));
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["schema mismatch", "columns"]);
}

#[test]
fn test_schema_mismatch_wrong_column_count_more() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, _) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    // Push 4-col schema to a 3-col table
    let wrong = Schema {
        columns: vec![
            ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false },
            ColumnDef { name: "a".into(),  type_code: TypeCode::I64, is_nullable: false },
            ColumnDef { name: "b".into(),  type_code: TypeCode::I64, is_nullable: false },
            ColumnDef { name: "c".into(),  type_code: TypeCode::I64, is_nullable: false },
        ],
        pk_index: 0,
    };
    let (ssec, sblob, scount) = schema_sec(&wrong);
    let mut batch = ZSetBatch::new(&wrong);
    batch.pk_lo.push(1); batch.pk_hi.push(0); batch.weights.push(1); batch.nulls.push(0);
    for ci in 1..4usize {
        if let ColData::Fixed(ref mut v) = batch.columns[ci] { v.extend_from_slice(&1i64.to_le_bytes()); }
    }
    let (dsec, dblob) = encode_zset(&wrong, &batch);
    let msg = assemble(tid, Some((&ssec, sblob, scount)), Some((&dsec, dblob, 1, 0)));
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["schema mismatch", "columns"]);
}

#[test]
fn test_schema_mismatch_wrong_column_type() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, _) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    // Push schema where column 2 is I64 instead of STRING
    let wrong = Schema {
        columns: vec![
            ColumnDef { name: "pk".into(),   type_code: TypeCode::U64, is_nullable: false },
            ColumnDef { name: "val".into(),  type_code: TypeCode::I64, is_nullable: false },
            ColumnDef { name: "name".into(), type_code: TypeCode::I64, is_nullable: true  },
        ],
        pk_index: 0,
    };
    let (ssec, sblob, scount) = schema_sec(&wrong);
    let mut batch = ZSetBatch::new(&wrong);
    batch.pk_lo.push(1); batch.pk_hi.push(0); batch.weights.push(1); batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[1] { v.extend_from_slice(&42i64.to_le_bytes()); }
    if let ColData::Fixed(ref mut v) = batch.columns[2] { v.extend_from_slice(&999i64.to_le_bytes()); }
    let (dsec, dblob) = encode_zset(&wrong, &batch);
    let msg = assemble(tid, Some((&ssec, sblob, scount)), Some((&dsec, dblob, 1, 0)));
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["schema mismatch", "column 2"]);
}

#[test]
fn test_schema_mismatch_wrong_pk_index() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, _) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    // Push with pk_index=1 to table with pk_index=0
    let wrong = Schema {
        columns: vec![
            ColumnDef { name: "val".into(),  type_code: TypeCode::I64,    is_nullable: false },
            ColumnDef { name: "pk".into(),   type_code: TypeCode::U64,    is_nullable: false },
            ColumnDef { name: "name".into(), type_code: TypeCode::String, is_nullable: true  },
        ],
        pk_index: 1,
    };
    let (ssec, sblob, scount) = schema_sec(&wrong);
    let mut batch = ZSetBatch::new(&wrong);
    batch.pk_lo.push(1); batch.pk_hi.push(0); batch.weights.push(1); batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[0] { v.extend_from_slice(&99i64.to_le_bytes()); }
    if let ColData::Strings(ref mut v) = batch.columns[2] { v.push(Some("x".into())); }
    let (dsec, dblob) = encode_zset(&wrong, &batch);
    let msg = assemble(tid, Some((&ssec, sblob, scount)), Some((&dsec, dblob, 1, 1)));
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["schema mismatch", "pk_index"]);
}

// ============================================================================
// 7. DISPATCH FAILURES
// ============================================================================

#[test]
fn test_target_id_nonexistent() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (hdr, resp) = raw.send_recv(&scan_msg(99_999));
    assert_err(&hdr, &resp, &["unknown"]);
}

#[test]
fn test_target_id_zero() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (hdr, resp) = raw.send_recv(&scan_msg(0));
    assert_err(&hdr, &resp, &["unknown"]);
}

#[test]
fn test_target_id_max_u64() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (hdr, resp) = raw.send_recv(&scan_msg(u64::MAX));
    assert_err(&hdr, &resp, &["unknown"]);
}

#[test]
fn test_push_to_nonexistent_with_valid_schema() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let (ssec, sblob, scount) = schema_sec(&schema);
    let batch = simple_batch_i64(&schema, 1, 42);
    let (dsec, dblob) = encode_zset(&schema, &batch);
    let msg = assemble(88_888, Some((&ssec, sblob, scount)), Some((&dsec, dblob, 1, 0)));
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["unknown"]);
}

// ============================================================================
// 8. EDGE CASES (should succeed)
// ============================================================================

#[test]
fn test_scan_system_table_schema_tab() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (hdr, _) = raw.send_recv(&scan_msg(SCHEMA_TAB));
    assert_ok(&hdr);
    assert!(hdr.schema_count > 0);
}

#[test]
fn test_scan_with_schema_section_but_no_data_is_scan() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let (ssec, sblob, scount) = schema_sec(&schema);
    // schema_count set but data_count=0 → treated as scan
    let msg = assemble(SCHEMA_TAB, Some((&ssec, sblob, scount)), None);
    let (hdr, _) = raw.send_recv(&msg);
    assert_ok(&hdr);
}

#[test]
fn test_nonzero_reserved_bytes_ignored() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut msg = scan_msg(SCHEMA_TAB);
    for b in &mut msg[80..96] { *b = 0xFF; }
    let (hdr, _) = raw.send_recv(&msg);
    assert_ok(&hdr);
}

#[test]
fn test_scan_echoes_client_id() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut h = Header::default();
    h.target_id = SCHEMA_TAB;
    h.client_id = 123_456_789;
    let (hdr, _) = raw.send_recv(&h.pack());
    assert_ok(&hdr);
    assert_eq!(hdr.client_id, 123_456_789);
}

#[test]
fn test_scan_returns_schema_section() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (hdr, _) = raw.send_recv(&scan_msg(SCHEMA_TAB));
    assert_ok(&hdr);
    assert!(hdr.schema_count > 0);
    assert!(hdr.schema_blob_sz > 0);
}

// ============================================================================
// 9. TRANSPORT EDGE CASES
// ============================================================================

#[test]
fn test_empty_memfd_too_small() {
    // Zero-byte memfd → server reports "too small"
    use std::ffi::CString;
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let sock_fd = connect(&srv.sock_path).expect("connect");

    let result = unsafe {
        let name = CString::new("gnitz_empty").unwrap();
        let mem_fd = libc::memfd_create(name.as_ptr(), 0);
        if mem_fd < 0 {
            close_fd(sock_fd);
            return; // skip if memfd_create fails
        }

        // Send the zero-size memfd via SCM_RIGHTS
        let mut dummy = [b'G'; 1];
        let mut iov = libc::iovec {
            iov_base: dummy.as_mut_ptr() as *mut libc::c_void,
            iov_len:  1,
        };
        let cmsg_space = libc::CMSG_SPACE(std::mem::size_of::<libc::c_int>() as u32) as usize;
        let mut ctrl = vec![0u8; cmsg_space];
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov        = &mut iov as *mut libc::iovec;
        msg.msg_iovlen     = 1;
        msg.msg_control    = ctrl.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cmsg_space as _;
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_len   = libc::CMSG_LEN(std::mem::size_of::<libc::c_int>() as u32) as _;
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type  = libc::SCM_RIGHTS;
        *(libc::CMSG_DATA(cmsg) as *mut libc::c_int) = mem_fd;
        let ret = libc::sendmsg(sock_fd, &msg, 0);
        libc::close(mem_fd);
        ret
    };

    if result < 0 {
        close_fd(sock_fd);
        return; // skip on send failure
    }

    match recv_memfd(sock_fd) {
        Ok(resp) => {
            if resp.len() >= HEADER_SIZE {
                if let Ok(hdr) = Header::unpack(&resp) {
                    assert_eq!(hdr.status, STATUS_ERROR, "empty memfd should yield error");
                }
            }
        }
        Err(_) => {} // server may drop connection — also acceptable
    }
    close_fd(sock_fd);
}

// ============================================================================
// 10. RESILIENCE
// ============================================================================

#[test]
fn test_error_then_valid_same_connection() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);

    // Error: wrong magic
    let (hdr1, _) = raw.send_recv(&[0u8; HEADER_SIZE]);
    assert_eq!(hdr1.status, STATUS_ERROR);

    // Valid scan after error
    let (hdr2, _) = raw.send_recv(&scan_msg(SCHEMA_TAB));
    assert_ok(&hdr2);
    assert!(hdr2.schema_count > 0);
}

#[test]
fn test_multiple_errors_then_valid() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);

    let (h, _) = raw.send_recv(&[0u8; HEADER_SIZE]);  // bad magic
    assert_eq!(h.status, STATUS_ERROR);

    let (h, _) = raw.send_recv(&[0x42u8]);             // too small
    assert_eq!(h.status, STATUS_ERROR);

    let (h, _) = raw.send_recv(&scan_msg(99_999));     // unknown target
    assert_eq!(h.status, STATUS_ERROR);

    let (h, _) = raw.send_recv(&scan_msg(SCHEMA_TAB)); // valid
    assert_ok(&h);
}

#[test]
fn test_new_connection_after_error() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };

    // Connection 1: bad message
    let c1 = RawClient::connect(&srv.sock_path);
    let (h, _) = c1.send_recv(&[0u8; HEADER_SIZE]);
    assert_eq!(h.status, STATUS_ERROR);
    drop(c1);

    // Connection 2: should work fine
    let c2 = RawClient::connect(&srv.sock_path);
    let (h, _) = c2.send_recv(&scan_msg(SCHEMA_TAB));
    assert_ok(&h);
}

#[test]
fn test_rapid_reconnect() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    for _ in 0..10 {
        let c = RawClient::connect(&srv.sock_path);
        let (h, _) = c.send_recv(&scan_msg(SCHEMA_TAB));
        assert_ok(&h);
    }
}

#[test]
fn test_error_recovery_after_schema_parse_error() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);

    // Schema error: bad type code
    let schema = two_col();
    let (sec, blob, count) = schema_sec_bad_type(&schema, 1, 200);
    let msg = assemble(SCHEMA_TAB, Some((&sec, blob, count)), None);
    let (h, _) = raw.send_recv(&msg);
    assert_eq!(h.status, STATUS_ERROR);

    // Valid scan after error
    let (h, _) = raw.send_recv(&scan_msg(SCHEMA_TAB));
    assert_ok(&h);
}

// ============================================================================
// 11. DATA VALIDITY EDGE CASES
// ============================================================================

#[test]
fn test_push_valid_data_to_real_table() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, schema) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let (ssec, sblob, scount) = schema_sec(&schema);
    let mut batch = ZSetBatch::new(&schema);
    batch.pk_lo.push(9000); batch.pk_hi.push(0); batch.weights.push(1); batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[1] { v.extend_from_slice(&77i64.to_le_bytes()); }
    if let ColData::Strings(ref mut v) = batch.columns[2] { v.push(Some("raw_push".into())); }
    let (dsec, dblob) = encode_zset(&schema, &batch);
    let msg = assemble(tid, Some((&ssec, sblob, scount)), Some((&dsec, dblob, 1, 0)));
    let (hdr, _) = raw.send_recv(&msg);
    assert_ok(&hdr);
}

#[test]
fn test_push_retraction_weight_minus_one() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, schema) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let (ssec, sblob, scount) = schema_sec(&schema);
    let mut batch = ZSetBatch::new(&schema);
    batch.pk_lo.push(9000); batch.pk_hi.push(0); batch.weights.push(-1); batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[1] { v.extend_from_slice(&77i64.to_le_bytes()); }
    if let ColData::Strings(ref mut v) = batch.columns[2] { v.push(Some("retract".into())); }
    let (dsec, dblob) = encode_zset(&schema, &batch);
    let msg = assemble(tid, Some((&ssec, sblob, scount)), Some((&dsec, dblob, 1, 0)));
    let (hdr, _) = raw.send_recv(&msg);
    assert_ok(&hdr);
}

#[test]
fn test_push_with_empty_string() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, schema) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let (ssec, sblob, scount) = schema_sec(&schema);
    let mut batch = ZSetBatch::new(&schema);
    batch.pk_lo.push(9100); batch.pk_hi.push(0); batch.weights.push(1); batch.nulls.push(0);
    if let ColData::Fixed(ref mut v) = batch.columns[1] { v.extend_from_slice(&1i64.to_le_bytes()); }
    if let ColData::Strings(ref mut v) = batch.columns[2] { v.push(Some("".into())); }
    let (dsec, dblob) = encode_zset(&schema, &batch);
    let msg = assemble(tid, Some((&ssec, sblob, scount)), Some((&dsec, dblob, 1, 0)));
    let (hdr, _) = raw.send_recv(&msg);
    assert_ok(&hdr);
}

#[test]
fn test_push_multiple_rows() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, schema) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);

    let (ssec, sblob, scount) = schema_sec(&schema);
    let mut batch = ZSetBatch::new(&schema);
    for &(pk, val, name) in &[(9001u64, 1i64, "alpha"), (9002, 2, "beta"), (9003, 3, "gamma")] {
        batch.pk_lo.push(pk); batch.pk_hi.push(0); batch.weights.push(1); batch.nulls.push(0);
        if let ColData::Fixed(ref mut v) = batch.columns[1] { v.extend_from_slice(&val.to_le_bytes()); }
        if let ColData::Strings(ref mut v) = batch.columns[2] { v.push(Some(name.into())); }
    }
    let (dsec, dblob) = encode_zset(&schema, &batch);
    let msg = assemble(tid, Some((&ssec, sblob, scount)), Some((&dsec, dblob, 3, 0)));
    let (hdr, _) = raw.send_recv(&msg);
    assert_ok(&hdr);
}

#[test]
fn test_scan_after_push_returns_data() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (_client, tid, _) = setup_test_table(&srv.sock_path);
    let raw = RawClient::connect(&srv.sock_path);
    let (hdr, _) = raw.send_recv(&scan_msg(tid));
    assert_ok(&hdr);
    assert!(hdr.schema_count > 0);
}

// ============================================================================
// 12. COMPOUND FAILURES
// ============================================================================

#[test]
fn test_wrong_magic_detected_before_data_count_error() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let mut h = Header::default();
    h.magic      = 0;
    h.target_id  = SCHEMA_TAB;
    h.data_count = 5;
    let (hdr, resp) = raw.send_recv(&h.pack());
    assert_err(&hdr, &resp, &["magic"]);
}

#[test]
fn test_truncated_detected_before_bad_magic_on_short_payload() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let (hdr, resp) = raw.send_recv(&[0xFFu8; 50]);
    assert_err(&hdr, &resp, &["too small"]);
}

#[test]
fn test_valid_schema_but_data_count_lies() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let (ssec, sblob, scount) = schema_sec(&schema);
    // data_count=100 but no data bytes at all
    let mut h = Header::default();
    h.target_id      = SCHEMA_TAB;
    h.schema_count   = scount as u64;
    h.schema_blob_sz = sblob;
    h.data_count     = 100;
    let body_start = align_up(HEADER_SIZE, 64);
    let mut msg = h.pack().to_vec();
    msg.resize(body_start, 0);
    msg.extend_from_slice(&ssec);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["truncated"]);
}

#[test]
fn test_schema_count_lies_high() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let raw = RawClient::connect(&srv.sock_path);
    let schema = two_col();
    let (sec, blob, _real_count) = schema_sec(&schema);
    // Tell server there are 1000 rows but actual batch has 2
    let msg = assemble(SCHEMA_TAB, Some((&sec, blob, 1000)), None);
    let (hdr, resp) = raw.send_recv(&msg);
    assert_err(&hdr, &resp, &["truncated"]);
}
