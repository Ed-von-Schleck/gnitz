//! IPC control-block wire layout.
//!
//! The control WAL block carries the per-message header. Both `gnitz-engine`
//! (server) and `gnitz-core` (client) build/parse this block; the column
//! indices, payload indices, and null-bit positions live here so the two
//! implementations cannot drift.
//!
//! Schema (10 columns, pk_index = 0):
//!   col  0: msg_idx       U64   (PK placeholder; always 0)
//!   col  1: status        U64
//!   col  2: client_id     U64
//!   col  3: target_id     U64
//!   col  4: flags         U64
//!   col  5: seek_pk       U128
//!   col  6: seek_col_idx  U64
//!   col  7: request_id    U64    -- reactor reply-routing key
//!   col  8: error_msg     STRING (nullable)
//!   col  9: seek_pk_extra BLOB   (nullable) -- PK region bytes 16.. for a wide PK
//!
//! Reserved request_id values:
//!   0          -- "unsolicited"/"untagged" (pre-reactor reply path)
//!   u64::MAX   -- broadcast reply (one reply per worker per broadcast)
//!   other      -- master-allocated, monotonic per request

use crate::catalog::col;
use crate::{
    encode_german_string, try_decode_german_string, TypeCode, WireSysCol, IPC_CONTROL_TID, SHORT_STRING_THRESHOLD,
    WAL_FORMAT_VERSION, WAL_HEADER_SIZE, WAL_OFF_COUNT, WAL_OFF_NUM_REGIONS, WAL_OFF_SIZE, WAL_OFF_TID,
    WAL_OFF_VERSION,
};

pub const CONTROL_COLS: &[WireSysCol] = &[
    col("msg_idx", TypeCode::U64, false),
    col("status", TypeCode::U64, false),
    col("client_id", TypeCode::U64, false),
    col("target_id", TypeCode::U64, false),
    col("flags", TypeCode::U64, false),
    col("seek_pk", TypeCode::U128, false),
    col("seek_col_idx", TypeCode::U64, false),
    col("request_id", TypeCode::U64, false),
    col("error_msg", TypeCode::String, true),
    col("seek_pk_extra", TypeCode::Blob, true),
];

pub const NUM_COLUMNS: usize = CONTROL_COLS.len();

const fn col_index(name: &str) -> usize {
    crate::col_index_in(CONTROL_COLS, name)
}

pub const COL_MSG_IDX: usize = col_index("msg_idx");
pub const COL_STATUS: usize = col_index("status");
pub const COL_CLIENT_ID: usize = col_index("client_id");
pub const COL_TARGET_ID: usize = col_index("target_id");
pub const COL_FLAGS: usize = col_index("flags");
pub const COL_SEEK_PK: usize = col_index("seek_pk");
pub const COL_SEEK_COL_IDX: usize = col_index("seek_col_idx");
pub const COL_REQUEST_ID: usize = col_index("request_id");
pub const COL_ERROR_MSG: usize = col_index("error_msg");
pub const COL_SEEK_PK_EXTRA: usize = col_index("seek_pk_extra");

const fn payload_index(col_idx: usize) -> usize {
    assert!(col_idx != COL_MSG_IDX, "PK column has no payload index");
    // COL_MSG_IDX == 0 (first column), so all non-PK indices are > 0
    col_idx - 1
}

pub const PAYLOAD_STATUS: usize = payload_index(COL_STATUS);
pub const PAYLOAD_CLIENT_ID: usize = payload_index(COL_CLIENT_ID);
pub const PAYLOAD_TARGET_ID: usize = payload_index(COL_TARGET_ID);
pub const PAYLOAD_FLAGS: usize = payload_index(COL_FLAGS);
pub const PAYLOAD_SEEK_PK: usize = payload_index(COL_SEEK_PK);
pub const PAYLOAD_SEEK_COL_IDX: usize = payload_index(COL_SEEK_COL_IDX);
pub const PAYLOAD_REQUEST_ID: usize = payload_index(COL_REQUEST_ID);
pub const PAYLOAD_ERROR_MSG: usize = payload_index(COL_ERROR_MSG);
pub const PAYLOAD_SEEK_PK_EXTRA: usize = payload_index(COL_SEEK_PK_EXTRA);

/// Null-bit position for `error_msg` in the row null bitmap.
/// Equals the payload index of the column.
pub const NULL_BIT_ERROR_MSG: u64 = 1u64 << PAYLOAD_ERROR_MSG;

/// Null-bit position for `seek_pk_extra` in the row null bitmap.
/// Equals the payload index of the column.
pub const NULL_BIT_SEEK_PK_EXTRA: u64 = 1u64 << PAYLOAD_SEEK_PK_EXTRA;

/// WAL region count for a CONTROL_SCHEMA block (V3 format):
/// 3 fixed regions (pk 16B, weight, null_bmp) + (NUM_COLUMNS - 1) payload columns
/// + 1 blob region.
pub const NUM_REGIONS: usize = 3 + (NUM_COLUMNS - 1) + 1;

// Region indices. V3 format: 3 system regions (pk=0, weight=1, null_bmp=2)
// followed by payload columns in schema order, then blob last.
pub const REGION_NULL_BMP: usize = 2;
pub const REGION_STATUS: usize = 3 + PAYLOAD_STATUS;
pub const REGION_CLIENT_ID: usize = 3 + PAYLOAD_CLIENT_ID;
pub const REGION_TARGET_ID: usize = 3 + PAYLOAD_TARGET_ID;
pub const REGION_FLAGS: usize = 3 + PAYLOAD_FLAGS;
pub const REGION_SEEK_PK: usize = 3 + PAYLOAD_SEEK_PK;
pub const REGION_SEEK_COL_IDX: usize = 3 + PAYLOAD_SEEK_COL_IDX;
pub const REGION_REQUEST_ID: usize = 3 + PAYLOAD_REQUEST_ID;
pub const REGION_ERROR_MSG: usize = 3 + PAYLOAD_ERROR_MSG;
pub const REGION_SEEK_PK_EXTRA: usize = 3 + PAYLOAD_SEEK_PK_EXTRA;
pub const REGION_BLOB: usize = NUM_REGIONS - 1;

// ---------------------------------------------------------------------------
// Control-block codec — the one encoder/decoder both ends run.
//
// The control block is a 1-row WAL block over CONTROL_COLS. Every region is a
// fixed-width scalar except the trailing blob region, which carries the
// German-string spill of `error_msg` / `seek_pk_extra` when either exceeds the
// 12-byte inline threshold. All fixed regions are 8-aligned by construction,
// so every region offset — and the whole no-blob block image — is a
// compile-time constant.
// ---------------------------------------------------------------------------

/// Byte size of region `r` for a 1-row control block (blob counted as empty).
const fn ctrl_region_size(r: usize) -> usize {
    match r {
        0 => CONTROL_COLS[COL_MSG_IDX].type_code.wire_stride(), // pk (OPK)
        1 | 2 => 8,                                             // weight, null_bmp
        REGION_BLOB => 0,                                       // blob (no-blob image)
        // payload region: payload index r-3 → column index r-3+1 (PK is col 0)
        _ => CONTROL_COLS[r - 2].type_code.wire_stride(),
    }
}

/// Absolute offset of region `r` within the control WAL block. Mirrors
/// `wal` encode's directory walk: directory immediately follows the header,
/// each region `align8`-padded before its data. The assertion enforces that
/// every region size is a multiple of 8 (so the implicit `align8` is a no-op);
/// a future schema change introducing an unaligned column fails at compile
/// time.
pub const fn ctrl_region_offset(target_region: usize) -> usize {
    let mut pos = WAL_HEADER_SIZE + NUM_REGIONS * 8;
    let mut r = 0;
    while r < target_region {
        let sz = ctrl_region_size(r);
        assert!(
            sz.is_multiple_of(8),
            "ctrl_region_offset assumes every region size is 8-aligned"
        );
        pos += sz;
        r += 1;
    }
    pos
}

/// Total control-block size when neither `error_msg` nor `seek_pk_extra`
/// spills to the blob region — the universal hot path.
pub const CTRL_BLOCK_SIZE_NO_BLOB: usize = ctrl_region_offset(NUM_REGIONS);

const OFF_WEIGHT: usize = ctrl_region_offset(1);
const OFF_NULL_BMP: usize = ctrl_region_offset(REGION_NULL_BMP);
const OFF_STATUS: usize = ctrl_region_offset(REGION_STATUS);
const OFF_CLIENT_ID: usize = ctrl_region_offset(REGION_CLIENT_ID);
const OFF_TARGET_ID: usize = ctrl_region_offset(REGION_TARGET_ID);
const OFF_FLAGS: usize = ctrl_region_offset(REGION_FLAGS);
const OFF_SEEK_PK: usize = ctrl_region_offset(REGION_SEEK_PK);
const OFF_SEEK_COL_IDX: usize = ctrl_region_offset(REGION_SEEK_COL_IDX);
const OFF_REQUEST_ID: usize = ctrl_region_offset(REGION_REQUEST_ID);
const OFF_ERROR_MSG: usize = ctrl_region_offset(REGION_ERROR_MSG);
const OFF_SEEK_PK_EXTRA: usize = ctrl_region_offset(REGION_SEEK_PK_EXTRA);

/// Blob bytes a German string of length `len` spills into the shared blob
/// region: 0 when it fits the 12-byte inline form, its full length otherwise.
const fn german_spill_len(len: usize) -> usize {
    if len > SHORT_STRING_THRESHOLD {
        len
    } else {
        0
    }
}

/// Total encoded size of a control block carrying an `error_msg` /
/// `seek_pk_extra` of the given byte lengths. The blob region is the last
/// region and every fixed region is 8-aligned, so the size is the no-blob
/// image plus the German-string spill.
pub const fn ctrl_block_size(error_msg_len: usize, seek_pk_extra_len: usize) -> usize {
    CTRL_BLOCK_SIZE_NO_BLOB + german_spill_len(error_msg_len) + german_spill_len(seek_pk_extra_len)
}

const fn template_write_u32(buf: &mut [u8; CTRL_BLOCK_SIZE_NO_BLOB], off: usize, v: u32) {
    let b = v.to_le_bytes();
    let mut i = 0;
    while i < 4 {
        buf[off + i] = b[i];
        i += 1;
    }
}

const fn template_write_u64(buf: &mut [u8; CTRL_BLOCK_SIZE_NO_BLOB], off: usize, v: u64) {
    let b = v.to_le_bytes();
    let mut i = 0;
    while i < 8 {
        buf[off + i] = b[i];
        i += 1;
    }
}

/// Pre-encoded no-blob control block: header, directory, weight = 1, both
/// nullable columns NULL, every variable field zero. `encode_ctrl_block`
/// copies this and patches the variable fields. The checksum field stays 0 —
/// callers that checksum their frames stamp it after encoding.
const CTRL_BLOCK_TEMPLATE: [u8; CTRL_BLOCK_SIZE_NO_BLOB] = {
    let mut buf = [0u8; CTRL_BLOCK_SIZE_NO_BLOB];
    template_write_u32(&mut buf, WAL_OFF_TID, IPC_CONTROL_TID);
    template_write_u32(&mut buf, WAL_OFF_COUNT, 1);
    template_write_u32(&mut buf, WAL_OFF_SIZE, CTRL_BLOCK_SIZE_NO_BLOB as u32);
    template_write_u32(&mut buf, WAL_OFF_VERSION, WAL_FORMAT_VERSION);
    template_write_u32(&mut buf, WAL_OFF_NUM_REGIONS, NUM_REGIONS as u32);
    let mut r = 0;
    while r < NUM_REGIONS {
        template_write_u32(&mut buf, WAL_HEADER_SIZE + r * 8, ctrl_region_offset(r) as u32);
        template_write_u32(&mut buf, WAL_HEADER_SIZE + r * 8 + 4, ctrl_region_size(r) as u32);
        r += 1;
    }
    template_write_u64(&mut buf, OFF_WEIGHT, 1); // weight = +1
    template_write_u64(&mut buf, OFF_NULL_BMP, NULL_BIT_ERROR_MSG | NULL_BIT_SEEK_PK_EXTRA);
    buf
};

/// Encode a control WAL block into `out[offset..]`. Returns bytes written
/// (`ctrl_block_size(error_msg.len(), seek_pk_extra.len())`).
///
/// Fast path (both `error_msg` and `seek_pk_extra` empty — the universal
/// case): copy the pre-encoded template and patch the 7 variable fields.
/// Otherwise additionally write the German-string structs, clear the
/// corresponding null bits, and append the blob spill.
///
/// The checksum header field is left 0; callers that checksum their frames
/// (durable WAL writes, TCP responses) stamp it over `[WAL_HEADER_SIZE, n)`
/// after encoding.
#[allow(clippy::too_many_arguments)]
#[inline]
pub fn encode_ctrl_block(
    out: &mut [u8],
    offset: usize,
    target_id: u64,
    client_id: u64,
    wire_flags: u64,
    seek_pk: u128,
    seek_col_idx: u64,
    request_id: u64,
    status: u32,
    error_msg: &[u8],
    seek_pk_extra: &[u8],
) -> usize {
    let total = ctrl_block_size(error_msg.len(), seek_pk_extra.len());
    let buf = &mut out[offset..offset + total];
    buf[..CTRL_BLOCK_SIZE_NO_BLOB].copy_from_slice(&CTRL_BLOCK_TEMPLATE);
    buf[OFF_STATUS..OFF_STATUS + 8].copy_from_slice(&(status as u64).to_le_bytes());
    buf[OFF_CLIENT_ID..OFF_CLIENT_ID + 8].copy_from_slice(&client_id.to_le_bytes());
    buf[OFF_TARGET_ID..OFF_TARGET_ID + 8].copy_from_slice(&target_id.to_le_bytes());
    buf[OFF_FLAGS..OFF_FLAGS + 8].copy_from_slice(&wire_flags.to_le_bytes());
    buf[OFF_SEEK_PK..OFF_SEEK_PK + 16].copy_from_slice(&seek_pk.to_le_bytes());
    buf[OFF_SEEK_COL_IDX..OFF_SEEK_COL_IDX + 8].copy_from_slice(&seek_col_idx.to_le_bytes());
    buf[OFF_REQUEST_ID..OFF_REQUEST_ID + 8].copy_from_slice(&request_id.to_le_bytes());

    if error_msg.is_empty() && seek_pk_extra.is_empty() {
        return CTRL_BLOCK_SIZE_NO_BLOB;
    }

    // Cold path: at least one German-string column is present.
    let mut blob = Vec::with_capacity(total - CTRL_BLOCK_SIZE_NO_BLOB);
    let mut null_word = NULL_BIT_ERROR_MSG | NULL_BIT_SEEK_PK_EXTRA;
    if !error_msg.is_empty() {
        null_word &= !NULL_BIT_ERROR_MSG;
        let st = encode_german_string(error_msg, &mut blob);
        buf[OFF_ERROR_MSG..OFF_ERROR_MSG + 16].copy_from_slice(&st);
    }
    if !seek_pk_extra.is_empty() {
        null_word &= !NULL_BIT_SEEK_PK_EXTRA;
        let st = encode_german_string(seek_pk_extra, &mut blob);
        buf[OFF_SEEK_PK_EXTRA..OFF_SEEK_PK_EXTRA + 16].copy_from_slice(&st);
    }
    buf[OFF_NULL_BMP..OFF_NULL_BMP + 8].copy_from_slice(&null_word.to_le_bytes());
    let blob_dir = WAL_HEADER_SIZE + REGION_BLOB * 8;
    buf[blob_dir + 4..blob_dir + 8].copy_from_slice(&(blob.len() as u32).to_le_bytes());
    buf[CTRL_BLOCK_SIZE_NO_BLOB..CTRL_BLOCK_SIZE_NO_BLOB + blob.len()].copy_from_slice(&blob);
    buf[WAL_OFF_SIZE..WAL_OFF_SIZE + 4].copy_from_slice(&(total as u32).to_le_bytes());
    total
}

/// Decoded control fields from a wire message.
pub struct DecodedControl {
    pub status: u32,
    pub client_id: u64,
    pub target_id: u64,
    pub flags: u64,
    pub seek_pk: u128,
    pub seek_col_idx: u64,
    pub request_id: u64,
    pub error_msg: Vec<u8>,
    /// PK region bytes `16..` for a wide PK; empty for `pk_stride <= 16`.
    /// Read by the worker SEEK dispatch to reconstruct the full wide-PK key.
    pub seek_pk_extra: Vec<u8>,
    /// Total byte length of this control WAL block (read from the WAL size
    /// field at offset WAL_OFF_SIZE). Callers that need to advance past the
    /// ctrl block to find the schema/data blocks use this directly instead of
    /// re-reading the WAL header.
    pub block_size: usize,
}

#[inline(always)]
fn read_u32_le(data: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(data[off..off + 4].try_into().unwrap())
}

#[inline(always)]
fn read_u64_le(data: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(data[off..off + 8].try_into().unwrap())
}

/// Read the (data_offset, data_size) directory entry for region `r` from a
/// WAL block. Panics on slice errors — callers must validate `data.len()`
/// covers the full directory before calling.
#[inline(always)]
fn wal_dir_entry(data: &[u8], r: usize) -> (usize, usize) {
    let base = WAL_HEADER_SIZE + r * 8;
    (read_u32_le(data, base) as usize, read_u32_le(data, base + 4) as usize)
}

/// Bounds-checked read of a u64 from a fixed-width u64 region of a 1-row
/// control block (exactly 8 bytes for 1 row).
#[inline(always)]
fn read_u64_region(data: &[u8], r: usize) -> Result<u64, &'static str> {
    let (off, sz) = wal_dir_entry(data, r);
    if sz < 8 || off.saturating_add(8) > data.len() {
        return Err("control block region out of bounds");
    }
    Ok(read_u64_le(data, off))
}

/// Bounds-checked read of a u128 from a fixed-width u128 region of a 1-row
/// control block (exactly 16 bytes for 1 row).
#[inline(always)]
fn read_u128_region(data: &[u8], r: usize) -> Result<u128, &'static str> {
    let (off, sz) = wal_dir_entry(data, r);
    if sz < 16 || off.saturating_add(16) > data.len() {
        return Err("control block u128 region out of bounds");
    }
    Ok(u128::from_le_bytes(data[off..off + 16].try_into().unwrap()))
}

/// Decode all control fields directly from the WAL block's directory without
/// materializing a batch. Each directory entry stores (data_offset: u32,
/// data_size: u32) at `WAL_HEADER_SIZE + region * 8`. For a 1-row control
/// block every u64 region is exactly 8 bytes, so the fields index directly.
pub fn peek_control_block(data: &[u8]) -> Result<DecodedControl, &'static str> {
    let dir_end = WAL_HEADER_SIZE + NUM_REGIONS * 8;
    if data.len() < dir_end {
        return Err("control block too small");
    }

    // Validate identity fields without computing the checksum.
    if read_u32_le(data, WAL_OFF_TID) != IPC_CONTROL_TID {
        return Err("control block wrong TID");
    }
    if read_u32_le(data, WAL_OFF_VERSION) != WAL_FORMAT_VERSION {
        return Err("control block wrong version");
    }
    if read_u32_le(data, WAL_OFF_COUNT) != 1 {
        return Err("control block must have exactly 1 row");
    }
    if read_u32_le(data, WAL_OFF_NUM_REGIONS) as usize != NUM_REGIONS {
        return Err("control block wrong region count");
    }

    let null_bmp = read_u64_region(data, REGION_NULL_BMP)?;
    let status = read_u64_region(data, REGION_STATUS)? as u32;
    let client_id = read_u64_region(data, REGION_CLIENT_ID)?;
    let target_id = read_u64_region(data, REGION_TARGET_ID)?;
    let flags = read_u64_region(data, REGION_FLAGS)?;
    let seek_pk = read_u128_region(data, REGION_SEEK_PK)?;
    let seek_col_idx = read_u64_region(data, REGION_SEEK_COL_IDX)?;
    let request_id = read_u64_region(data, REGION_REQUEST_ID)?;

    let error_is_null = (null_bmp & NULL_BIT_ERROR_MSG) != 0;
    let seek_extra_is_null = (null_bmp & NULL_BIT_SEEK_PK_EXTRA) != 0;

    // error_msg and seek_pk_extra each own a 16-byte German-string struct in
    // their own fixed region but spill overflow (>12B) into the shared blob
    // region. Resolve that directory entry once, and only if at least one is
    // non-null. When both are null (the universal case) skip the lookup
    // entirely, preserving the hot-path fast case.
    let blob: &[u8] = if !error_is_null || !seek_extra_is_null {
        let (blob_off, blob_sz) = wal_dir_entry(data, REGION_BLOB);
        if blob_sz > 0 && blob_off.saturating_add(blob_sz) <= data.len() {
            &data[blob_off..blob_off + blob_sz]
        } else {
            &[]
        }
    } else {
        &[]
    };

    let read_german = |region: usize, err: &'static str, oob: &'static str| -> Result<Vec<u8>, &'static str> {
        let (off, sz) = wal_dir_entry(data, region);
        if sz < 16 || off.saturating_add(16) > data.len() {
            return Err(err);
        }
        let mut st = [0u8; 16];
        st.copy_from_slice(&data[off..off + 16]);
        try_decode_german_string(&st, blob).ok_or(oob)
    };

    let error_msg = if error_is_null {
        Vec::new()
    } else {
        read_german(
            REGION_ERROR_MSG,
            "error_msg region out of bounds",
            "error_msg string offset out of bounds",
        )?
    };

    let seek_pk_extra = if seek_extra_is_null {
        Vec::new()
    } else {
        read_german(
            REGION_SEEK_PK_EXTRA,
            "seek_pk_extra region out of bounds",
            "seek_pk_extra string offset out of bounds",
        )?
    };

    let block_size = read_u32_le(data, WAL_OFF_SIZE) as usize;
    Ok(DecodedControl {
        status,
        client_id,
        target_id,
        flags,
        seek_pk,
        seek_col_idx,
        request_id,
        error_msg,
        seek_pk_extra,
        block_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The template-and-patch fast path and the blob fallback agree on the
    /// shared fixed-region image: encoding with empty strings then with
    /// spilling strings must differ only in the null word, the two German
    /// structs, the blob directory entry / content, and the size field.
    #[test]
    fn encode_roundtrip_all_paths() {
        // Fast path.
        let mut buf = vec![0u8; ctrl_block_size(0, 0)];
        let n = encode_ctrl_block(&mut buf, 0, 1, 2, 3, 4u128, 5, 6, 7, b"", b"");
        assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB);
        let dec = peek_control_block(&buf[..n]).expect("decode empty");
        assert_eq!(dec.target_id, 1);
        assert_eq!(dec.client_id, 2);
        assert_eq!(dec.flags, 3);
        assert_eq!(dec.seek_pk, 4u128);
        assert_eq!(dec.seek_col_idx, 5);
        assert_eq!(dec.request_id, 6);
        assert_eq!(dec.status, 7);
        assert!(dec.error_msg.is_empty());
        assert!(dec.seek_pk_extra.is_empty());
        assert_eq!(dec.block_size, n);

        // Inline strings (≤ 12 bytes, no blob spill).
        let short = b"abcd";
        let mut buf = vec![0u8; ctrl_block_size(short.len(), short.len())];
        let n = encode_ctrl_block(&mut buf, 0, 1, 2, 3, 4u128, 5, 6, 7, short, short);
        assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB, "inline strings must not grow the block");
        let dec = peek_control_block(&buf[..n]).expect("decode short");
        assert_eq!(dec.error_msg, short);
        assert_eq!(dec.seek_pk_extra, short);

        // Both spill into the shared blob region.
        let err = b"this error message is definitely longer than twelve bytes";
        let extra = b"and so is this wide-pk-extra blob payload past 12B";
        let mut buf = vec![0u8; ctrl_block_size(err.len(), extra.len())];
        let n = encode_ctrl_block(&mut buf, 0, 1, 2, 3, 4u128, 5, 6, 7, err, extra);
        assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB + err.len() + extra.len());
        let dec = peek_control_block(&buf[..n]).expect("decode long");
        assert_eq!(dec.error_msg, err);
        assert_eq!(dec.seek_pk_extra, extra);
        assert_eq!(dec.block_size, n);
    }

    /// A corrupted long-string blob offset must surface an error, not panic.
    #[test]
    fn peek_rejects_oob_error_msg_offset() {
        let long_msg = b"this error message exceeds twelve bytes so it spills into the blob";
        let mut buf = vec![0u8; ctrl_block_size(long_msg.len(), 0)];
        let n = encode_ctrl_block(&mut buf, 0, 1, 2, 0, 0, 0, 0, 1, long_msg, b"");
        buf.truncate(n);
        let (err_off, _) = wal_dir_entry(&buf, REGION_ERROR_MSG);
        buf[err_off + 8..err_off + 16].copy_from_slice(&u64::MAX.to_le_bytes());
        match peek_control_block(&buf) {
            Err("error_msg string offset out of bounds") => {}
            Err(other) => panic!("wrong error: {other}"),
            Ok(_) => panic!("OOB error_msg offset must be rejected"),
        }
    }
}
