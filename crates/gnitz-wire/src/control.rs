//! IPC control-block wire layout.
//!
//! The control WAL block carries the per-message header. Both `gnitz-server`
//! and `gnitz-core` (client) build/parse this block; the column
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
use crate::wal::IPC_CONTROL_TID;
use crate::{
    checksum, encode_german_string_cell, read_u32_le, read_u64_le, try_decode_german_string, TypeCode, WireSysCol,
    REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT, SHORT_STRING_THRESHOLD, WAL_FORMAT_VERSION, WAL_HEADER_SIZE,
    WAL_OFF_CHECKSUM, WAL_OFF_COUNT, WAL_OFF_NUM_REGIONS, WAL_OFF_SIZE, WAL_OFF_TID, WAL_OFF_VERSION,
};

const CONTROL_COLS: &[WireSysCol] = &[
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

const NUM_COLUMNS: usize = CONTROL_COLS.len();

/// WAL region count for a CONTROL_SCHEMA block. The PK is column 0, so the
/// block has `NUM_COLUMNS - 1` payload columns.
const NUM_REGIONS: usize = crate::wal::num_regions(NUM_COLUMNS - 1);

/// Region index of the payload column named `name`: the fixed regions, then the
/// payload columns in schema order (`pay_index_in`, valid here because the PK is
/// the single leading column), then the blob region last. The fixed indices
/// themselves (`REG_PK` / `REG_WEIGHT` / `REG_NULL_BMP`) come straight from
/// `wal`. A column's null bit is its payload index, i.e. `reg - REG_PAYLOAD_START`.
const fn reg(name: &str) -> usize {
    crate::payload_region_in(CONTROL_COLS, name)
}

const REG_STATUS: usize = reg("status");
const REG_CLIENT_ID: usize = reg("client_id");
const REG_TARGET_ID: usize = reg("target_id");
const REG_FLAGS: usize = reg("flags");
const REG_SEEK_PK: usize = reg("seek_pk");
const REG_SEEK_COL_IDX: usize = reg("seek_col_idx");
const REG_REQUEST_ID: usize = reg("request_id");
const REG_ERROR_MSG: usize = reg("error_msg");
const REG_SEEK_PK_EXTRA: usize = reg("seek_pk_extra");
const REG_BLOB: usize = NUM_REGIONS - 1;

/// Null-bit positions of the two nullable columns in the row null bitmap.
const NULL_BIT_ERROR_MSG: u64 = 1u64 << (REG_ERROR_MSG - REG_PAYLOAD_START);
const NULL_BIT_SEEK_PK_EXTRA: u64 = 1u64 << (REG_SEEK_PK_EXTRA - REG_PAYLOAD_START);

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
        // The PK is column 0 (`msg_idx`) — the same fact `reg` above rests on.
        REG_PK => CONTROL_COLS[0].type_code.wire_stride(),
        REG_WEIGHT | REG_NULL_BMP => 8,
        REG_BLOB => 0, // no-blob image
        // Payload region: payload index `r - REG_PAYLOAD_START` → that + 1 as a
        // column index, since the PK is column 0.
        _ => CONTROL_COLS[r - REG_PAYLOAD_START + 1].type_code.wire_stride(),
    }
}

/// Absolute offset of region `r` within the control WAL block. Mirrors
/// `wal` encode's directory walk: directory immediately follows the header,
/// each region `align8`-padded before its data. The assertion enforces that
/// every region size is a multiple of 8 (so the implicit `align8` is a no-op);
/// a future schema change introducing an unaligned column fails at compile
/// time.
const fn ctrl_region_offset(target_region: usize) -> usize {
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

const OFF_WEIGHT: usize = ctrl_region_offset(REG_WEIGHT);
const OFF_NULL_BMP: usize = ctrl_region_offset(REG_NULL_BMP);
const OFF_STATUS: usize = ctrl_region_offset(REG_STATUS);
const OFF_CLIENT_ID: usize = ctrl_region_offset(REG_CLIENT_ID);
const OFF_TARGET_ID: usize = ctrl_region_offset(REG_TARGET_ID);
const OFF_FLAGS: usize = ctrl_region_offset(REG_FLAGS);
const OFF_SEEK_PK: usize = ctrl_region_offset(REG_SEEK_PK);
const OFF_SEEK_COL_IDX: usize = ctrl_region_offset(REG_SEEK_COL_IDX);
const OFF_REQUEST_ID: usize = ctrl_region_offset(REG_REQUEST_ID);
const OFF_ERROR_MSG: usize = ctrl_region_offset(REG_ERROR_MSG);
const OFF_SEEK_PK_EXTRA: usize = ctrl_region_offset(REG_SEEK_PK_EXTRA);

/// Blob bytes a German string of length `len` spills into the shared blob
/// region: 0 when it fits the 12-byte inline form, its full length otherwise.
pub(crate) const fn german_spill_len(len: usize) -> usize {
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

/// `copy_from_slice` is not `const`, so the template below writes its
/// little-endian fields a byte at a time. Generic over the field width, since
/// that is the only thing the u32 and u64 forms differed in.
const fn template_write<const N: usize>(buf: &mut [u8; CTRL_BLOCK_SIZE_NO_BLOB], off: usize, b: [u8; N]) {
    let mut i = 0;
    while i < N {
        buf[off + i] = b[i];
        i += 1;
    }
}

/// Pre-encoded no-blob control block: header, directory, weight = 1, both
/// nullable columns NULL, every variable field zero. `encode_ctrl_block`
/// copies this and patches the variable fields. The checksum field stays 0 in
/// the template; `encode_ctrl_block` stamps it when its caller asks for one.
const CTRL_BLOCK_TEMPLATE: [u8; CTRL_BLOCK_SIZE_NO_BLOB] = {
    let mut buf = [0u8; CTRL_BLOCK_SIZE_NO_BLOB];
    template_write(&mut buf, WAL_OFF_TID, IPC_CONTROL_TID.to_le_bytes());
    template_write(&mut buf, WAL_OFF_COUNT, 1u32.to_le_bytes());
    template_write(&mut buf, WAL_OFF_SIZE, (CTRL_BLOCK_SIZE_NO_BLOB as u32).to_le_bytes());
    template_write(&mut buf, WAL_OFF_VERSION, WAL_FORMAT_VERSION.to_le_bytes());
    template_write(&mut buf, WAL_OFF_NUM_REGIONS, (NUM_REGIONS as u32).to_le_bytes());
    let mut r = 0;
    while r < NUM_REGIONS {
        let dir = crate::wal::dir_entry_offset(r);
        template_write(&mut buf, dir, (ctrl_region_offset(r) as u32).to_le_bytes());
        template_write(&mut buf, dir + 4, (ctrl_region_size(r) as u32).to_le_bytes());
        r += 1;
    }
    template_write(&mut buf, OFF_WEIGHT, 1u64.to_le_bytes()); // weight = +1
    template_write(
        &mut buf,
        OFF_NULL_BMP,
        (NULL_BIT_ERROR_MSG | NULL_BIT_SEEK_PK_EXTRA).to_le_bytes(),
    );
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
/// `checksum` stamps the header's checksum field over the encoded body, as
/// `wal::encode` and `schema_block::encode_into` do for the blocks beside this
/// one: `true` for what [`peek_control_block`] reads back, `false` for
/// [`peek_control_block_ipc`].
#[inline]
pub fn encode_ctrl_block(
    out: &mut [u8],
    offset: usize,
    hdr: &ControlHeader,
    error_msg: &[u8],
    seek_pk_extra: &[u8],
    checksum: bool,
) -> usize {
    let total = ctrl_block_size(error_msg.len(), seek_pk_extra.len());
    let buf = &mut out[offset..offset + total];
    buf[..CTRL_BLOCK_SIZE_NO_BLOB].copy_from_slice(&CTRL_BLOCK_TEMPLATE);
    buf[OFF_STATUS..OFF_STATUS + 8].copy_from_slice(&(hdr.status as u64).to_le_bytes());
    buf[OFF_CLIENT_ID..OFF_CLIENT_ID + 8].copy_from_slice(&hdr.client_id.to_le_bytes());
    buf[OFF_TARGET_ID..OFF_TARGET_ID + 8].copy_from_slice(&hdr.target_id.to_le_bytes());
    buf[OFF_FLAGS..OFF_FLAGS + 8].copy_from_slice(&hdr.flags.to_le_bytes());
    buf[OFF_SEEK_PK..OFF_SEEK_PK + 16].copy_from_slice(&hdr.seek_pk.to_le_bytes());
    buf[OFF_SEEK_COL_IDX..OFF_SEEK_COL_IDX + 8].copy_from_slice(&hdr.seek_col_idx.to_le_bytes());
    buf[OFF_REQUEST_ID..OFF_REQUEST_ID + 8].copy_from_slice(&hdr.request_id.to_le_bytes());

    if error_msg.is_empty() && seek_pk_extra.is_empty() {
        if checksum {
            crate::wal::stamp_checksum(buf, CTRL_BLOCK_SIZE_NO_BLOB);
        }
        return CTRL_BLOCK_SIZE_NO_BLOB;
    }

    // Cold path: at least one German-string column is present. Each cell's
    // spill goes straight into the blob region at the offset the cell names,
    // so nothing is staged and copied twice.
    let mut heap = 0usize;
    let mut null_word = NULL_BIT_ERROR_MSG | NULL_BIT_SEEK_PK_EXTRA;
    let mut write_cell = |buf: &mut [u8], cell_off: usize, s: &[u8]| {
        let (st, spill) = encode_german_string_cell(s, heap);
        buf[cell_off..cell_off + 16].copy_from_slice(&st);
        let at = CTRL_BLOCK_SIZE_NO_BLOB + heap;
        buf[at..at + spill.len()].copy_from_slice(spill);
        heap += spill.len();
    };
    if !error_msg.is_empty() {
        null_word &= !NULL_BIT_ERROR_MSG;
        write_cell(buf, OFF_ERROR_MSG, error_msg);
    }
    if !seek_pk_extra.is_empty() {
        null_word &= !NULL_BIT_SEEK_PK_EXTRA;
        write_cell(buf, OFF_SEEK_PK_EXTRA, seek_pk_extra);
    }
    buf[OFF_NULL_BMP..OFF_NULL_BMP + 8].copy_from_slice(&null_word.to_le_bytes());
    crate::write_u32_le(buf, crate::wal::dir_entry_offset(REG_BLOB) + 4, heap as u32);
    crate::write_u32_le(buf, WAL_OFF_SIZE, total as u32);
    if checksum {
        crate::wal::stamp_checksum(buf, total);
    }
    total
}

/// The control block's seven scalar fields — the routing header both sides of
/// the wire agree on. Named rather than positional: `target_id`/`client_id` are
/// adjacent `u64`s, so a transposed pair in an argument list would encode
/// cleanly and misroute the frame.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ControlHeader {
    pub status: u32,
    pub target_id: u64,
    pub client_id: u64,
    pub flags: u64,
    pub seek_pk: u128,
    pub seek_col_idx: u64,
    /// Master-allocated reply-routing key. Clients send 0; the master sets a
    /// per-request value when fanning out to workers, and workers echo it back
    /// in their W2M reply. Reserved: `0` — unsolicited / untagged;
    /// `u64::MAX` — broadcast reply.
    pub request_id: u64,
}

/// Decoded control fields from a wire message.
#[derive(Default)]
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

/// The directory prefix a canonical control block shares byte-for-byte with
/// [`CTRL_BLOCK_TEMPLATE`]: every fixed region's offset and size, plus the blob
/// region's offset. Only the blob region's *size* varies, which is why the
/// compared span stops just after the blob entry's offset field.
const DIR_FIXED_END: usize = crate::wal::dir_entry_offset(REG_BLOB) + 4;

/// Decode all control fields without materializing a batch.
///
/// The block's directory is a compile-time constant — `encode_ctrl_block` copies
/// [`CTRL_BLOCK_TEMPLATE`] and patches only the blob size — so this checks the
/// directory against that template once and then reads every field at the same
/// `OFF_*` constant the encoder wrote it at. A directory that disagrees with the
/// template is rejected rather than followed: honouring one would let a peer
/// point a scalar field at the blob heap.
///
/// Verifies the block's checksum, like the schema and data blocks beside it —
/// use this on frames that crossed a durability or trust boundary (SAL replay,
/// client ingress). For frames written by `encode_ipc`, which leaves the
/// checksum zero, use [`peek_control_block_ipc`].
pub fn peek_control_block(data: &[u8]) -> Result<DecodedControl, &'static str> {
    peek_control_block_impl(data, true)
}

/// [`peek_control_block`] without checksum verification, for the intra-process
/// frames `encode_ipc` writes: the W2M ring and client egress.
pub fn peek_control_block_ipc(data: &[u8]) -> Result<DecodedControl, &'static str> {
    peek_control_block_impl(data, false)
}

fn peek_control_block_impl(data: &[u8], verify_checksum: bool) -> Result<DecodedControl, &'static str> {
    let dir_end = WAL_HEADER_SIZE + NUM_REGIONS * 8;
    if data.len() < dir_end {
        return Err("control block too small");
    }

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

    // The directory is fixed by the format; only the blob size varies. Checking
    // it against the template is what lets every field below read at its
    // compile-time offset instead of re-deriving one per field.
    if data[WAL_HEADER_SIZE..DIR_FIXED_END] != CTRL_BLOCK_TEMPLATE[WAL_HEADER_SIZE..DIR_FIXED_END] {
        return Err("control block directory is not the canonical layout");
    }

    // `SIZE` frames the rest of the slot and sits outside the block's own
    // checksum, so the exact blob relation is what constrains it.
    let blob_len = read_u32_le(data, DIR_FIXED_END) as usize;
    let block_size = read_u32_le(data, WAL_OFF_SIZE) as usize;
    if block_size != CTRL_BLOCK_SIZE_NO_BLOB + blob_len {
        return Err("control block size disagrees with its blob region");
    }
    if block_size > data.len() {
        return Err("control block truncated");
    }

    if verify_checksum && checksum(&data[WAL_HEADER_SIZE..block_size]) != read_u64_le(data, WAL_OFF_CHECKSUM) {
        return Err("control block checksum mismatch");
    }

    // `block_size <= data.len()` and every fixed region lies within
    // `CTRL_BLOCK_SIZE_NO_BLOB <= block_size`, so each read below is in bounds.
    let null_bmp = read_u64_le(data, OFF_NULL_BMP);
    let status = read_u64_le(data, OFF_STATUS) as u32;
    let client_id = read_u64_le(data, OFF_CLIENT_ID);
    let target_id = read_u64_le(data, OFF_TARGET_ID);
    let flags = read_u64_le(data, OFF_FLAGS);
    let seek_pk = u128::from_le_bytes(data[OFF_SEEK_PK..OFF_SEEK_PK + 16].try_into().unwrap());
    let seek_col_idx = read_u64_le(data, OFF_SEEK_COL_IDX);
    let request_id = read_u64_le(data, OFF_REQUEST_ID);

    let error_is_null = (null_bmp & NULL_BIT_ERROR_MSG) != 0;
    let seek_extra_is_null = (null_bmp & NULL_BIT_SEEK_PK_EXTRA) != 0;

    // error_msg and seek_pk_extra each own a 16-byte German-string struct in
    // their own fixed region but spill overflow (>12B) into the shared blob
    // region, which starts where the no-blob image ends.
    let blob = &data[CTRL_BLOCK_SIZE_NO_BLOB..block_size];

    let read_german = |off: usize, oob: &'static str| -> Result<Vec<u8>, &'static str> {
        try_decode_german_string(&data[off..off + 16], blob).ok_or(oob)
    };

    let error_msg = if error_is_null {
        Vec::new()
    } else {
        read_german(OFF_ERROR_MSG, "error_msg string offset out of bounds")?
    };

    let seek_pk_extra = if seek_extra_is_null {
        Vec::new()
    } else {
        read_german(OFF_SEEK_PK_EXTRA, "seek_pk_extra string offset out of bounds")?
    };

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

impl DecodedControl {
    /// The seven routing scalars as a [`ControlHeader`], for a caller that
    /// re-encodes what it decoded.
    pub fn header(&self) -> ControlHeader {
        ControlHeader {
            status: self.status,
            target_id: self.target_id,
            client_id: self.client_id,
            flags: self.flags,
            seek_pk: self.seek_pk,
            seek_col_idx: self.seek_col_idx,
            request_id: self.request_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A header with a distinct wide value per field, so a transposed pair or a
    /// truncated width fails an assertion rather than round-tripping unnoticed.
    fn probe_header() -> ControlHeader {
        ControlHeader {
            target_id: 0x1111_2222_3333_4444,
            client_id: 0x5555_6666_7777_8888,
            flags: 0x9999_AAAA_BBBB_CCCC,
            seek_pk: (0xDDDD_EEEE_FFFF_0011u128 << 64) | 0x2233_4455_6677_8899u128,
            seek_col_idx: 0xAA_BB_CC_DD_EE_FF_00_11,
            request_id: 0x1234_5678_9ABC_DEF0,
            status: 0xDEAD_BEEF,
        }
    }

    /// Every encode path — template fast path, inline strings, blob spill —
    /// round-trips, at both trust levels and at a non-zero offset (a write
    /// indexing through `out[offset + OFF_X..]` instead of the sub-slice then
    /// lands outside the block).
    #[test]
    fn encode_roundtrip_all_paths() {
        const OFFSET: usize = 64;
        for &checksum in &[false, true] {
            let peek = |b: &[u8]| {
                if checksum {
                    peek_control_block(b)
                } else {
                    peek_control_block_ipc(b)
                }
            };
            let hdr = probe_header();

            // Fast path.
            let mut buf = vec![0u8; OFFSET + ctrl_block_size(0, 0)];
            let n = encode_ctrl_block(&mut buf, OFFSET, &hdr, b"", b"", checksum);
            assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB);
            let dec = peek(&buf[OFFSET..OFFSET + n]).expect("decode empty");
            assert_eq!(dec.header(), hdr, "checksum={checksum}");
            assert!(dec.error_msg.is_empty());
            assert!(dec.seek_pk_extra.is_empty());
            assert_eq!(dec.block_size, n);

            // Inline strings (≤ 12 bytes, no blob spill).
            let short = b"abcd";
            let mut buf = vec![0u8; OFFSET + ctrl_block_size(short.len(), short.len())];
            let n = encode_ctrl_block(&mut buf, OFFSET, &hdr, short, short, checksum);
            assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB, "inline strings must not grow the block");
            let dec = peek(&buf[OFFSET..OFFSET + n]).expect("decode short");
            assert_eq!(dec.error_msg, short);
            assert_eq!(dec.seek_pk_extra, short);

            // Both spill into the shared blob region, so the decoder has to
            // resolve one heap and hand each cell the right slice of it.
            let err = b"this error message is definitely longer than twelve bytes";
            let extra = b"and so is this wide-pk-extra blob payload past 12B";
            let mut buf = vec![0u8; OFFSET + ctrl_block_size(err.len(), extra.len())];
            let n = encode_ctrl_block(&mut buf, OFFSET, &hdr, err, extra, checksum);
            assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB + err.len() + extra.len());
            let dec = peek(&buf[OFFSET..OFFSET + n]).expect("decode long");
            assert_eq!(dec.header(), hdr);
            assert_eq!(dec.error_msg, err);
            assert_eq!(dec.seek_pk_extra, extra);
            assert_eq!(dec.block_size, n);
        }
    }

    /// The directory is part of the format, not something a sender gets to
    /// choose. Re-pointing a scalar field's region at the blob heap — which the
    /// old per-field extent check honoured, since the extent stayed in-block —
    /// must be rejected outright.
    #[test]
    fn peek_rejects_a_directory_that_disagrees_with_the_template() {
        let mut buf = vec![0u8; ctrl_block_size(0, 0)];
        let n = encode_ctrl_block(&mut buf, 0, &probe_header(), b"", b"", false);
        buf.truncate(n);

        // Aim `status` at `client_id`'s bytes: a well-formed, in-block extent.
        let mut forged = buf.clone();
        crate::write_u32_le(
            &mut forged,
            crate::wal::dir_entry_offset(REG_STATUS),
            OFF_CLIENT_ID as u32,
        );
        crate::wal::stamp_checksum(&mut forged, n);
        assert_eq!(
            peek_control_block(&forged).err(),
            Some("control block directory is not the canonical layout")
        );

        // The blob region's *size* is the one entry a sender legitimately varies.
        let long = b"an error message well past the twelve-byte inline threshold";
        let mut ok = vec![0u8; ctrl_block_size(long.len(), 0)];
        let n = encode_ctrl_block(&mut ok, 0, &probe_header(), long, b"", false);
        assert_eq!(peek_control_block_ipc(&ok[..n]).unwrap().error_msg, long);
    }

    /// A corrupted long-string blob offset must surface an error, not panic.
    #[test]
    fn peek_rejects_oob_error_msg_offset() {
        let long_msg = b"this error message exceeds twelve bytes so it spills into the blob";
        let mut buf = vec![0u8; ctrl_block_size(long_msg.len(), 0)];
        let n = encode_ctrl_block(
            &mut buf,
            0,
            &ControlHeader {
                status: 1,
                target_id: 1,
                client_id: 2,
                ..Default::default()
            },
            long_msg,
            b"",
            false,
        );
        buf.truncate(n);
        let (err_off, _) = crate::wal::dir_entry(&buf, REG_ERROR_MSG);
        buf[err_off + 8..err_off + 16].copy_from_slice(&u64::MAX.to_le_bytes());
        match peek_control_block_ipc(&buf) {
            Err("error_msg string offset out of bounds") => {}
            Err(other) => panic!("wrong error: {other}"),
            Ok(_) => panic!("OOB error_msg offset must be rejected"),
        }
    }
}
