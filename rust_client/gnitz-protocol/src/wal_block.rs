// gnitz-protocol/src/wal_block.rs — WAL-block encode/decode (Python wal_columnar.py port)

use std::hash::Hasher;

use crate::error::ProtocolError;
use crate::types::{ColData, Schema, TypeCode, ZSetBatch};

// ── Constants ────────────────────────────────────────────────────────────────

pub const WAL_BLOCK_HEADER_SIZE: usize = 48;
pub const WAL_FORMAT_VERSION:    u32   = 2;
pub const IPC_CONTROL_TID:       u32   = 0xFFFF_FFFF;
const     SHORT_STRING_THRESHOLD: usize = 12;

// ── Internal helpers ─────────────────────────────────────────────────────────

#[inline]
fn align8(n: usize) -> usize {
    (n + 7) & !7
}

/// Append `data` to `buf` at an 8-byte-aligned offset.  Returns `(offset, len)`.
fn append_region(buf: &mut Vec<u8>, data: &[u8]) -> (u32, u32) {
    let aligned = align8(buf.len());
    buf.resize(aligned, 0);
    let off = aligned as u32;
    buf.extend_from_slice(data);
    (off, data.len() as u32)
}

/// WAL column stride: String and U128 each use 16 bytes (German String struct / lo+hi pair).
fn wal_col_stride(tc: TypeCode) -> usize {
    match tc {
        TypeCode::U8  | TypeCode::I8  => 1,
        TypeCode::U16 | TypeCode::I16 => 2,
        TypeCode::U32 | TypeCode::I32 | TypeCode::F32 => 4,
        TypeCode::U64 | TypeCode::I64 | TypeCode::F64 => 8,
        TypeCode::String | TypeCode::U128 => 16,
    }
}

fn xxh3_64(data: &[u8]) -> u64 {
    let mut h = twox_hash::xxh3::Hash64::with_seed(0);
    h.write(data);
    h.finish()
}

// ── German String encode/decode ───────────────────────────────────────────────

/// Encode a Rust `&str` as a 16-byte German String struct, appending long-string
/// data to `blob`.
///
/// Layout:
///   [0..4]  length (u32 LE)
///   [4..8]  prefix — first min(4, length) bytes, zero-padded to 4
///   [8..16] if length ≤ 12 → suffix bytes [4..length], zero-padded to 8
///           if length > 12 → blob arena offset (u64 LE)
fn encode_german_string(s: &str, blob: &mut Vec<u8>) -> [u8; 16] {
    let bytes = s.as_bytes();
    let length = bytes.len();
    let mut st = [0u8; 16];

    st[0..4].copy_from_slice(&(length as u32).to_le_bytes());

    if length == 0 {
        return st;
    }

    let prefix_len = length.min(4);
    st[4..4 + prefix_len].copy_from_slice(&bytes[..prefix_len]);

    if length <= SHORT_STRING_THRESHOLD {
        if length > 4 {
            st[8..8 + (length - 4)].copy_from_slice(&bytes[4..length]);
        }
    } else {
        let offset = blob.len() as u64;
        st[8..16].copy_from_slice(&offset.to_le_bytes());
        blob.extend_from_slice(bytes);
    }

    st
}

/// Decode a 16-byte German String struct into a Rust `String`.
/// `blob` is the shared blob arena for this WAL block.
fn decode_german_string(st: [u8; 16], blob: &[u8]) -> Result<String, ProtocolError> {
    let length = u32::from_le_bytes(st[0..4].try_into().unwrap()) as usize;

    if length == 0 {
        return Ok(String::new());
    }

    if length <= SHORT_STRING_THRESHOLD {
        let take_prefix = length.min(4);
        let mut result = Vec::with_capacity(length);
        result.extend_from_slice(&st[4..4 + take_prefix]);
        if length > 4 {
            result.extend_from_slice(&st[8..8 + (length - 4)]);
        }
        String::from_utf8(result)
            .map_err(|e| ProtocolError::DecodeError(format!("utf8 in German String: {}", e)))
    } else {
        let offset = u64::from_le_bytes(st[8..16].try_into().unwrap()) as usize;
        let end = offset.checked_add(length).ok_or_else(|| {
            ProtocolError::DecodeError("German String blob offset overflow".into())
        })?;
        if end > blob.len() {
            return Err(ProtocolError::DecodeError(format!(
                "German String blob arena out of bounds: offset={}, length={}, blob.len()={}",
                offset, length, blob.len()
            )));
        }
        String::from_utf8(blob[offset..end].to_vec())
            .map_err(|e| ProtocolError::DecodeError(format!("utf8 in German String (long): {}", e)))
    }
}

// ── Region read helpers ───────────────────────────────────────────────────────

fn read_u64_region(
    data: &[u8], off: usize, sz: usize, count: usize,
) -> Result<Vec<u64>, ProtocolError> {
    let expected = count * 8;
    if sz != expected {
        return Err(ProtocolError::DecodeError(format!(
            "u64 region size mismatch: expected {}, got {}", expected, sz
        )));
    }
    let mut v = Vec::with_capacity(count);
    for i in 0..count {
        let base = off + i * 8;
        v.push(u64::from_le_bytes(data[base..base + 8].try_into().unwrap()));
    }
    Ok(v)
}

fn read_i64_region(
    data: &[u8], off: usize, sz: usize, count: usize,
) -> Result<Vec<i64>, ProtocolError> {
    let expected = count * 8;
    if sz != expected {
        return Err(ProtocolError::DecodeError(format!(
            "i64 region size mismatch: expected {}, got {}", expected, sz
        )));
    }
    let mut v = Vec::with_capacity(count);
    for i in 0..count {
        let base = off + i * 8;
        v.push(i64::from_le_bytes(data[base..base + 8].try_into().unwrap()));
    }
    Ok(v)
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Encode a ZSetBatch into a WAL block `Vec<u8>`.
///
/// Region order: pk_lo, pk_hi, weight, null, [non-PK cols in schema order], blob.
/// num_regions = 4 + (ncols - 1) + 1.
pub fn encode_wal_block(schema: &Schema, table_id: u32, batch: &ZSetBatch) -> Vec<u8> {
    let count = batch.len();
    let num_non_pk = schema.columns.len() - 1;
    let num_regions = 4 + num_non_pk + 1;

    // --- Build column region data ---
    let mut blob: Vec<u8> = Vec::new();
    let mut col_region_data: Vec<Vec<u8>> = Vec::with_capacity(num_non_pk);

    for (ci, col) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index {
            continue;
        }
        let payload_idx = if ci < schema.pk_index { ci } else { ci - 1 };

        let region = match col.type_code {
            TypeCode::String => {
                let strings = match &batch.columns[ci] {
                    ColData::Strings(v) => v,
                    _ => panic!("encode_wal_block: expected Strings for String column {}", ci),
                };
                let mut col_bytes = Vec::with_capacity(count * 16);
                for (row, val) in strings.iter().enumerate() {
                    let is_null = (batch.nulls[row] & (1u64 << payload_idx)) != 0;
                    if is_null || val.is_none() {
                        col_bytes.extend_from_slice(&[0u8; 16]);
                    } else {
                        let s = val.as_ref().unwrap();
                        let st = encode_german_string(s, &mut blob);
                        col_bytes.extend_from_slice(&st);
                    }
                }
                col_bytes
            }
            TypeCode::U128 => {
                let vals = match &batch.columns[ci] {
                    ColData::U128s(v) => v,
                    _ => panic!("encode_wal_block: expected U128s for U128 column {}", ci),
                };
                let mut col_bytes = Vec::with_capacity(count * 16);
                for &v in vals {
                    let lo = (v & 0xFFFF_FFFF_FFFF_FFFF) as u64;
                    let hi = (v >> 64) as u64;
                    col_bytes.extend_from_slice(&lo.to_le_bytes());
                    col_bytes.extend_from_slice(&hi.to_le_bytes());
                }
                col_bytes
            }
            _ => {
                let stride = wal_col_stride(col.type_code);
                let fixed = match &batch.columns[ci] {
                    ColData::Fixed(v) => v,
                    _ => panic!("encode_wal_block: expected Fixed for column {}", ci),
                };
                debug_assert_eq!(fixed.len(), count * stride,
                    "col {} Fixed length {} != count*stride {}", ci, fixed.len(), count * stride);
                fixed.clone()
            }
        };
        col_region_data.push(region);
    }

    let blob_size = blob.len();

    // --- Assemble buffer: header + directory + aligned regions ---
    let dir_start = WAL_BLOCK_HEADER_SIZE;
    let dir_size = num_regions * 8;
    let mut buf: Vec<u8> = vec![0u8; dir_start + dir_size];
    let mut dir_entries: Vec<(u32, u32)> = Vec::with_capacity(num_regions);

    // pk_lo
    let r = {
        let mut v = Vec::with_capacity(count * 8);
        for &x in &batch.pk_lo { v.extend_from_slice(&x.to_le_bytes()); }
        v
    };
    dir_entries.push(append_region(&mut buf, &r));

    // pk_hi
    let r = {
        let mut v = Vec::with_capacity(count * 8);
        for &x in &batch.pk_hi { v.extend_from_slice(&x.to_le_bytes()); }
        v
    };
    dir_entries.push(append_region(&mut buf, &r));

    // weight
    let r = {
        let mut v = Vec::with_capacity(count * 8);
        for &x in &batch.weights { v.extend_from_slice(&x.to_le_bytes()); }
        v
    };
    dir_entries.push(append_region(&mut buf, &r));

    // null
    let r = {
        let mut v = Vec::with_capacity(count * 8);
        for &x in &batch.nulls { v.extend_from_slice(&x.to_le_bytes()); }
        v
    };
    dir_entries.push(append_region(&mut buf, &r));

    // non-PK column regions
    for region in &col_region_data {
        dir_entries.push(append_region(&mut buf, region));
    }

    // blob arena (always last region)
    dir_entries.push(append_region(&mut buf, &blob));

    let total_size = buf.len();

    // Write directory entries
    for (i, &(off, sz)) in dir_entries.iter().enumerate() {
        let base = dir_start + i * 8;
        buf[base..base + 4].copy_from_slice(&off.to_le_bytes());
        buf[base + 4..base + 8].copy_from_slice(&sz.to_le_bytes());
    }

    // Backfill header  (lsn=0 at offset 0 is already zero)
    buf[8..12].copy_from_slice(&table_id.to_le_bytes());          // table_id
    buf[12..16].copy_from_slice(&(count as u32).to_le_bytes());   // entry_count
    buf[16..20].copy_from_slice(&(total_size as u32).to_le_bytes()); // total_size
    buf[20..24].copy_from_slice(&WAL_FORMAT_VERSION.to_le_bytes()); // format_version
    // checksum at [24..32] — filled below
    buf[32..36].copy_from_slice(&(num_regions as u32).to_le_bytes()); // num_regions
    // reserved [36..40] stays zero
    buf[40..48].copy_from_slice(&(blob_size as u64).to_le_bytes()); // blob_size

    // Compute and write checksum over buf[48..]
    let checksum = if total_size > WAL_BLOCK_HEADER_SIZE {
        xxh3_64(&buf[WAL_BLOCK_HEADER_SIZE..])
    } else {
        0
    };
    buf[24..32].copy_from_slice(&checksum.to_le_bytes());

    buf
}

/// Decode a WAL block from `data`, expecting columns described by `schema`.
/// Returns `(ZSetBatch, table_id, lsn)`.
pub fn decode_wal_block(
    data: &[u8],
    schema: &Schema,
) -> Result<(ZSetBatch, u32, u64), ProtocolError> {
    if data.len() < WAL_BLOCK_HEADER_SIZE {
        return Err(ProtocolError::DecodeError(format!(
            "WAL block too small: {} < {}", data.len(), WAL_BLOCK_HEADER_SIZE
        )));
    }

    let lsn          = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let table_id     = u32::from_le_bytes(data[8..12].try_into().unwrap());
    let entry_count  = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;
    let total_size   = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;
    let format_ver   = u32::from_le_bytes(data[20..24].try_into().unwrap());
    let exp_checksum = u64::from_le_bytes(data[24..32].try_into().unwrap());
    let num_regions  = u32::from_le_bytes(data[32..36].try_into().unwrap()) as usize;
    // blob_size from header (informational; we use the blob region's directory entry)

    if format_ver != WAL_FORMAT_VERSION {
        return Err(ProtocolError::DecodeError(format!(
            "unsupported WAL format version: expected {}, got {}",
            WAL_FORMAT_VERSION, format_ver
        )));
    }
    if total_size > data.len() {
        return Err(ProtocolError::DecodeError(format!(
            "WAL block total_size {} exceeds data len {}", total_size, data.len()
        )));
    }
    if total_size > WAL_BLOCK_HEADER_SIZE {
        let actual = xxh3_64(&data[WAL_BLOCK_HEADER_SIZE..total_size]);
        if actual != exp_checksum {
            return Err(ProtocolError::DecodeError("WAL checksum mismatch".into()));
        }
    }

    // Validate num_regions
    let expected_num_regions = 4 + (schema.columns.len() - 1) + 1;
    if num_regions != expected_num_regions {
        return Err(ProtocolError::DecodeError(format!(
            "WAL block num_regions mismatch: expected {}, got {}",
            expected_num_regions, num_regions
        )));
    }

    // Parse directory
    let dir_start = WAL_BLOCK_HEADER_SIZE;
    let dir_bytes = num_regions * 8;
    if dir_start + dir_bytes > total_size {
        return Err(ProtocolError::DecodeError("WAL directory out of bounds".into()));
    }
    let mut dir: Vec<(usize, usize)> = Vec::with_capacity(num_regions);
    for i in 0..num_regions {
        let base = dir_start + i * 8;
        let off = u32::from_le_bytes(data[base..base + 4].try_into().unwrap()) as usize;
        let sz  = u32::from_le_bytes(data[base + 4..base + 8].try_into().unwrap()) as usize;
        if off > total_size || off + sz > total_size {
            return Err(ProtocolError::DecodeError(format!(
                "WAL region {} out of bounds (off={}, sz={}, total={})", i, off, sz, total_size
            )));
        }
        dir.push((off, sz));
    }

    let count = entry_count;

    // Empty batch: build typed placeholder columns
    if count == 0 {
        let columns = schema.columns.iter().enumerate().map(|(ci, col)| {
            if ci == schema.pk_index {
                ColData::Fixed(vec![])
            } else {
                match col.type_code {
                    TypeCode::String => ColData::Strings(vec![]),
                    TypeCode::U128   => ColData::U128s(vec![]),
                    _                => ColData::Fixed(vec![]),
                }
            }
        }).collect();
        return Ok((ZSetBatch { pk_lo: vec![], pk_hi: vec![], weights: vec![], nulls: vec![], columns }, table_id, lsn));
    }

    // Read system regions
    let mut region_idx = 0;

    let (pk_lo_off,  pk_lo_sz)  = dir[region_idx]; region_idx += 1;
    let (pk_hi_off,  pk_hi_sz)  = dir[region_idx]; region_idx += 1;
    let (wt_off,     wt_sz)     = dir[region_idx]; region_idx += 1;
    let (null_off,   null_sz)   = dir[region_idx]; region_idx += 1;

    let pk_lo   = read_u64_region(data, pk_lo_off, pk_lo_sz, count)?;
    let pk_hi   = read_u64_region(data, pk_hi_off, pk_hi_sz, count)?;
    let weights = read_i64_region(data, wt_off,    wt_sz,    count)?;
    let nulls   = read_u64_region(data, null_off,  null_sz,  count)?;

    // Blob region (always last)
    let (blob_off, blob_sz) = dir[num_regions - 1];
    let blob = if blob_sz > 0 { &data[blob_off..blob_off + blob_sz] } else { &[] };

    // Read column regions
    let mut columns: Vec<ColData> = Vec::with_capacity(schema.columns.len());

    for (ci, col) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index {
            columns.push(ColData::Fixed(vec![]));
            continue;
        }

        let payload_idx = if ci < schema.pk_index { ci } else { ci - 1 };
        let (reg_off, reg_sz) = dir[region_idx];
        region_idx += 1;

        match col.type_code {
            TypeCode::String => {
                let expected_sz = count * 16;
                if reg_sz != expected_sz {
                    return Err(ProtocolError::DecodeError(format!(
                        "String column {} region size mismatch: expected {}, got {}",
                        ci, expected_sz, reg_sz
                    )));
                }
                let mut vals: Vec<Option<String>> = Vec::with_capacity(count);
                for row in 0..count {
                    let is_null = (nulls[row] & (1u64 << payload_idx)) != 0;
                    if is_null {
                        vals.push(None);
                        continue;
                    }
                    let struct_start = reg_off + row * 16;
                    if struct_start + 16 > data.len() {
                        return Err(ProtocolError::DecodeError(format!(
                            "German String struct out of bounds at row {}, col {}", row, ci
                        )));
                    }
                    let mut st = [0u8; 16];
                    st.copy_from_slice(&data[struct_start..struct_start + 16]);
                    vals.push(Some(decode_german_string(st, blob)?));
                }
                columns.push(ColData::Strings(vals));
            }
            TypeCode::U128 => {
                let expected_sz = count * 16;
                if reg_sz != expected_sz {
                    return Err(ProtocolError::DecodeError(format!(
                        "U128 column {} region size mismatch: expected {}, got {}", ci, expected_sz, reg_sz
                    )));
                }
                let mut vals: Vec<u128> = Vec::with_capacity(count);
                for row in 0..count {
                    let base = reg_off + row * 16;
                    if base + 16 > data.len() {
                        return Err(ProtocolError::DecodeError("U128 out of bounds".into()));
                    }
                    let lo = u64::from_le_bytes(data[base..base + 8].try_into().unwrap());
                    let hi = u64::from_le_bytes(data[base + 8..base + 16].try_into().unwrap());
                    vals.push(lo as u128 | ((hi as u128) << 64));
                }
                columns.push(ColData::U128s(vals));
            }
            _ => {
                let stride = wal_col_stride(col.type_code);
                let expected_sz = count * stride;
                if reg_sz != expected_sz {
                    return Err(ProtocolError::DecodeError(format!(
                        "Fixed column {} region size mismatch: expected {}, got {}", ci, expected_sz, reg_sz
                    )));
                }
                if reg_off + reg_sz > data.len() {
                    return Err(ProtocolError::DecodeError(format!(
                        "Fixed column {} region out of bounds", ci
                    )));
                }
                columns.push(ColData::Fixed(data[reg_off..reg_off + reg_sz].to_vec()));
            }
        }
    }

    Ok((ZSetBatch { pk_lo, pk_hi, weights, nulls, columns }, table_id, lsn))
}

/// Recompute and write the WAL block checksum in-place.
/// Used by tests that need to corrupt block content and fix the checksum.
pub fn recompute_block_checksum(block: &mut Vec<u8>) {
    if block.len() <= WAL_BLOCK_HEADER_SIZE {
        return;
    }
    let total_size = u32::from_le_bytes(block[16..20].try_into().unwrap()) as usize;
    let end = total_size.min(block.len());
    let checksum = xxh3_64(&block[WAL_BLOCK_HEADER_SIZE..end]);
    block[24..32].copy_from_slice(&checksum.to_le_bytes());
}

/// Return the `(offset, size)` of a region from a block's directory.
/// Used by tests to locate regions for corruption.
pub fn get_region_offset_size(block: &[u8], region_idx: usize) -> (usize, usize) {
    let base = WAL_BLOCK_HEADER_SIZE + region_idx * 8;
    let off = u32::from_le_bytes(block[base..base + 4].try_into().unwrap()) as usize;
    let sz  = u32::from_le_bytes(block[base + 4..base + 8].try_into().unwrap()) as usize;
    (off, sz)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnDef, Schema, TypeCode, ZSetBatch, ColData};

    fn u64_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false },
                ColumnDef { name: "v".into(),  type_code: TypeCode::I64, is_nullable: false },
            ],
            pk_index: 0,
        }
    }

    fn str_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U64,    is_nullable: false },
                ColumnDef { name: "s".into(),   type_code: TypeCode::String, is_nullable: true  },
            ],
            pk_index: 0,
        }
    }

    fn u128_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U128, is_nullable: false },
                ColumnDef { name: "v".into(),   type_code: TypeCode::U128, is_nullable: false },
            ],
            pk_index: 0,
        }
    }

    // ── Header roundtrip ───────────────────────────────────────────────────

    #[test]
    fn test_header_roundtrip() {
        let mut buf = vec![0u8; WAL_BLOCK_HEADER_SIZE];
        buf[0..8].copy_from_slice(&123u64.to_le_bytes());
        buf[8..12].copy_from_slice(&456u32.to_le_bytes());
        buf[12..16].copy_from_slice(&10u32.to_le_bytes());
        buf[16..20].copy_from_slice(&200u32.to_le_bytes());
        buf[20..24].copy_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        buf[24..32].copy_from_slice(&789u64.to_le_bytes());
        buf[32..36].copy_from_slice(&5u32.to_le_bytes());
        buf[36..40].copy_from_slice(&0u32.to_le_bytes());
        buf[40..48].copy_from_slice(&100u64.to_le_bytes());

        assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()),   123);
        assert_eq!(u32::from_le_bytes(buf[8..12].try_into().unwrap()),  456);
        assert_eq!(u32::from_le_bytes(buf[12..16].try_into().unwrap()), 10);
        assert_eq!(u32::from_le_bytes(buf[16..20].try_into().unwrap()), 200);
        assert_eq!(u32::from_le_bytes(buf[20..24].try_into().unwrap()), WAL_FORMAT_VERSION);
        assert_eq!(u64::from_le_bytes(buf[24..32].try_into().unwrap()), 789);
        assert_eq!(u32::from_le_bytes(buf[32..36].try_into().unwrap()), 5);
        assert_eq!(u32::from_le_bytes(buf[36..40].try_into().unwrap()), 0);
        assert_eq!(u64::from_le_bytes(buf[40..48].try_into().unwrap()), 100);
    }

    // ── German String ─────────────────────────────────────────────────────

    #[test]
    fn test_german_string_short() {
        for s in &["", "a", "abcd", "abcdefghijkl"] {
            let mut blob = Vec::new();
            let st = encode_german_string(s, &mut blob);
            assert!(blob.is_empty(), "short string '{}' should not use blob", s);
            let decoded = decode_german_string(st, &[]).unwrap();
            assert_eq!(&decoded, s, "roundtrip failed for '{}'", s);
        }
    }

    #[test]
    fn test_german_string_long() {
        for s in &["abcdefghijklm", "hello world 12345", "a".repeat(100).as_str()] {
            let mut blob = Vec::new();
            let st = encode_german_string(s, &mut blob);
            assert!(!blob.is_empty(), "long string should use blob");
            let decoded = decode_german_string(st, &blob).unwrap();
            assert_eq!(&decoded, s, "roundtrip failed for long string");
        }
    }

    #[test]
    fn test_german_string_boundary() {
        // length=12 → inline (SHORT_STRING_THRESHOLD)
        let s12 = "123456789012";
        assert_eq!(s12.len(), 12);
        let mut blob12 = Vec::new();
        let st12 = encode_german_string(s12, &mut blob12);
        assert!(blob12.is_empty());
        assert_eq!(decode_german_string(st12, &[]).unwrap(), s12);

        // length=13 → blob
        let s13 = "1234567890123";
        assert_eq!(s13.len(), 13);
        let mut blob13 = Vec::new();
        let st13 = encode_german_string(s13, &mut blob13);
        assert_eq!(blob13.len(), 13);
        assert_eq!(decode_german_string(st13, &blob13).unwrap(), s13);
    }

    // ── encode/decode roundtrips ──────────────────────────────────────────

    #[test]
    fn test_encode_decode_fixed() {
        let schema = u64_schema();
        let n = 10usize;
        let pk_lo: Vec<u64>   = (0..n as u64).collect();
        let pk_hi: Vec<u64>   = vec![0; n];
        let weights: Vec<i64> = vec![1; n];
        let nulls: Vec<u64>   = vec![0; n];
        let vals: Vec<i64>    = (0..n as i64).map(|x| x * -7).collect();
        let mut val_bytes = Vec::with_capacity(n * 8);
        for &v in &vals { val_bytes.extend_from_slice(&v.to_le_bytes()); }

        let batch = ZSetBatch {
            pk_lo: pk_lo.clone(), pk_hi: pk_hi.clone(),
            weights: weights.clone(), nulls: nulls.clone(),
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(val_bytes.clone())],
        };

        let encoded = encode_wal_block(&schema, 42, &batch);
        let (decoded, tid, _lsn) = decode_wal_block(&encoded, &schema).unwrap();

        assert_eq!(tid, 42);
        assert_eq!(decoded.pk_lo, pk_lo);
        assert_eq!(decoded.pk_hi, pk_hi);
        assert_eq!(decoded.weights, weights);
        assert_eq!(decoded.nulls, nulls);
        match &decoded.columns[1] {
            ColData::Fixed(got) => assert_eq!(got, &val_bytes),
            _ => panic!("expected Fixed"),
        }
    }

    #[test]
    fn test_encode_decode_strings() {
        let schema = str_schema();
        let n = 5usize;
        // Row 2 is null (payload_idx=0 → bit 0)
        let nulls: Vec<u64> = vec![0, 0, 1, 0, 0];
        let vals: Vec<Option<String>> = vec![
            Some("hello".into()),
            Some("hello world 1234".into()), // long string
            None,
            Some("".into()),
            Some("abcdefghijkl".into()), // exactly 12 chars
        ];

        let batch = ZSetBatch {
            pk_lo: (0..n as u64).collect(),
            pk_hi: vec![0; n],
            weights: vec![1; n],
            nulls: nulls.clone(),
            columns: vec![ColData::Fixed(vec![]), ColData::Strings(vals.clone())],
        };

        let encoded = encode_wal_block(&schema, 0, &batch);
        let (decoded, _, _) = decode_wal_block(&encoded, &schema).unwrap();

        assert_eq!(decoded.nulls, nulls);
        match &decoded.columns[1] {
            ColData::Strings(got) => assert_eq!(got, &vals),
            _ => panic!("expected Strings"),
        }
    }

    #[test]
    fn test_encode_decode_u128() {
        let schema = u128_schema();
        let vals: Vec<u128> = vec![0, 1, u128::MAX, 1u128 << 64, (1u128 << 64) + 42];
        let n = vals.len();

        let batch = ZSetBatch {
            pk_lo: vals.iter().map(|&v| v as u64).collect(),
            pk_hi: vals.iter().map(|&v| (v >> 64) as u64).collect(),
            weights: vec![1; n],
            nulls: vec![0; n],
            columns: vec![ColData::Fixed(vec![]), ColData::U128s(vals.clone())],
        };

        let encoded = encode_wal_block(&schema, 1, &batch);
        let (decoded, tid, _) = decode_wal_block(&encoded, &schema).unwrap();
        assert_eq!(tid, 1);
        match &decoded.columns[1] {
            ColData::U128s(got) => assert_eq!(got, &vals),
            _ => panic!("expected U128s"),
        }
    }

    #[test]
    fn test_encode_decode_empty() {
        let schema = u64_schema();
        let empty = ZSetBatch {
            pk_lo: vec![], pk_hi: vec![], weights: vec![], nulls: vec![],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(vec![])],
        };
        let encoded = encode_wal_block(&schema, 7, &empty);
        let (decoded, tid, _) = decode_wal_block(&encoded, &schema).unwrap();
        assert_eq!(tid, 7);
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn test_bad_checksum() {
        let schema = u64_schema();
        let batch = ZSetBatch {
            pk_lo: vec![1], pk_hi: vec![0], weights: vec![1], nulls: vec![0],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(8u64.to_le_bytes().to_vec())],
        };
        let mut encoded = encode_wal_block(&schema, 0, &batch);
        // Flip a byte in the body (after header)
        encoded[WAL_BLOCK_HEADER_SIZE] ^= 0xFF;
        let res = decode_wal_block(&encoded, &schema);
        assert!(matches!(res, Err(ProtocolError::DecodeError(ref s)) if s.contains("checksum")));
    }

    #[test]
    fn test_bad_version() {
        let schema = u64_schema();
        let batch = ZSetBatch {
            pk_lo: vec![1], pk_hi: vec![0], weights: vec![1], nulls: vec![0],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(8u64.to_le_bytes().to_vec())],
        };
        let mut encoded = encode_wal_block(&schema, 0, &batch);
        // Set format_version = 1 (in header, offset 20..24)
        encoded[20..24].copy_from_slice(&1u32.to_le_bytes());
        // Note: checksum covers buf[48..] so changing header bytes does NOT invalidate checksum
        let res = decode_wal_block(&encoded, &schema);
        assert!(matches!(res, Err(ProtocolError::DecodeError(ref s)) if s.contains("version")));
    }
}
