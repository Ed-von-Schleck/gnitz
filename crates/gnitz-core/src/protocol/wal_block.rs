// gnitz-protocol/src/wal_block.rs — WAL-block encode/decode (Python wal_columnar.py port)

use super::error::ProtocolError;
use super::types::{ColData, PkColumn, Schema, TypeCode, ZSetBatch};

use gnitz_wire::{
    WAL_HEADER_SIZE as WAL_BLOCK_HEADER_SIZE,
    WAL_FORMAT_VERSION, SHORT_STRING_THRESHOLD,
    align8,
};

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Append `data` to `buf` at an 8-byte-aligned offset.  Returns `(offset, len)`.
fn append_region(buf: &mut Vec<u8>, data: &[u8]) -> (u32, u32) {
    let aligned = align8(buf.len());
    buf.resize(aligned, 0);
    let off = aligned as u32;
    buf.extend_from_slice(data);
    (off, data.len() as u32)
}

/// Serialize a `&[T]` (T = u64 or i64) directly into `buf` at 8-byte alignment via bulk memcpy.
/// Correct on little-endian (x86_64) — native layout matches to_le_bytes() output.
fn append_64bit_region<T: Copy>(buf: &mut Vec<u8>, vals: &[T]) -> (u32, u32) {
    debug_assert_eq!(std::mem::size_of::<T>(), 8);
    let aligned = align8(buf.len());
    let sz = vals.len() * 8;
    buf.resize(aligned, 0);
    // SAFETY: T is 8 bytes (asserted above); reinterpreting as &[u8] is valid
    // because the output is consumed as opaque bytes, not as typed values.
    let src = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, sz) };
    buf.extend_from_slice(src);
    (aligned as u32, sz as u32)
}

/// Serialize a `PkColumn` into `buf` at 8-byte alignment via bulk memcpy.
/// U64 variant writes 8 bytes/row; U128 variant writes 16 bytes/row.
/// Correct on little-endian (x86_64) — native layout matches to_le_bytes() output.
fn append_pk_region(buf: &mut Vec<u8>, pks: &PkColumn) -> (u32, u32) {
    match pks {
        PkColumn::U64s(v) => {
            // SAFETY: u64 is 8 bytes; native LE layout matches to_le_bytes() on x86_64.
            let src = unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * 8) };
            append_region(buf, src)
        }
        PkColumn::U128s(v) => {
            // SAFETY: u128 is 16 bytes; native LE layout matches to_le_bytes() on x86_64.
            let src = unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * 16) };
            append_region(buf, src)
        }
    }
}

fn xxh3_64(data: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(data)
}

fn encode_german_string(s: &str, blob: &mut Vec<u8>) -> [u8; 16] {
    gnitz_wire::encode_german_string(s.as_bytes(), blob)
}

fn decode_german_string(st: [u8; 16], blob: &[u8]) -> Result<String, ProtocolError> {
    let length = u32::from_le_bytes(st[0..4].try_into().unwrap()) as usize;
    if length == 0 {
        return Ok(String::new());
    }
    // Bounds-check before delegating to wire (which would panic).
    if length > SHORT_STRING_THRESHOLD {
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
    }
    let bytes = gnitz_wire::decode_german_string(&st, blob);
    String::from_utf8(bytes)
        .map_err(|e| ProtocolError::DecodeError(format!("utf8 in German String: {}", e)))
}

// ── Region read helpers ───────────────────────────────────────────────────────

/// Read a region of 64-bit values (u64 or i64) via bulk memcpy. Correct on little-endian.
fn read_64bit_region<T: Copy + Default>(
    data: &[u8], off: usize, sz: usize, count: usize, label: &str,
) -> Result<Vec<T>, ProtocolError> {
    debug_assert_eq!(std::mem::size_of::<T>(), 8);
    let expected = count * 8;
    if sz != expected {
        return Err(ProtocolError::DecodeError(format!(
            "{} region size mismatch: expected {}, got {}", label, expected, sz
        )));
    }
    let src = &data[off..off + expected];
    let mut v: Vec<T> = vec![T::default(); count];
    // SAFETY: src is `expected` bytes (bounds-checked above); v has room for
    // `count` Ts = `expected` bytes.  Both are valid, non-overlapping regions.
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), v.as_mut_ptr() as *mut u8, expected);
    }
    Ok(v)
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Encode a ZSetBatch into a WAL block `Vec<u8>`.
///
/// Region order: pk (pk_stride bytes each), weight, null, [non-PK cols in schema order], blob.
/// num_regions = 3 + (ncols - 1) + 1.
pub fn encode_wal_block(schema: &Schema, table_id: u32, batch: &ZSetBatch) -> Vec<u8> {
    let count = batch.len();
    let ncols = schema.columns.len();
    let num_non_pk = if ncols > 0 { ncols - 1 } else { 0 };
    let num_regions = 3 + num_non_pk + 1;

    // --- Pre-build String/U128 column region data (needs blob arena) ---
    // Fixed columns are appended directly to buf later (no clone).
    let mut blob: Vec<u8> = Vec::new();

    // ColRegion::Prebuilt holds String/U128 temp Vecs; ColRegion::FixedRef marks
    // columns that will borrow from batch.columns directly.
    enum ColRegion {
        Prebuilt(Vec<u8>),
        FixedRef(usize), // schema column index → borrow batch.columns[ci]
    }
    let mut col_regions: Vec<ColRegion> = Vec::with_capacity(num_non_pk);

    for (ci, col) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index {
            continue;
        }
        let payload_idx = if ci < schema.pk_index { ci } else { ci - 1 };

        match col.type_code {
            TypeCode::String => {
                let strings = match &batch.columns[ci] {
                    ColData::Strings(v) => v,
                    _ => panic!("encode_wal_block: expected Strings for String column {}", ci),
                };
                let mut col_bytes = Vec::with_capacity(count * 16);
                for (row, val) in strings.iter().enumerate() {
                    let is_null = (batch.nulls[row] & (1u64 << payload_idx)) != 0;
                    if let (false, Some(s)) = (is_null, val.as_deref()) {
                        col_bytes.extend_from_slice(&encode_german_string(s, &mut blob));
                    } else {
                        col_bytes.extend_from_slice(&[0u8; 16]);
                    }
                }
                col_regions.push(ColRegion::Prebuilt(col_bytes));
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
                col_regions.push(ColRegion::Prebuilt(col_bytes));
            }
            _ => {
                let stride = col.type_code.wire_stride();
                let fixed = match &batch.columns[ci] {
                    ColData::Fixed(v) => v,
                    _ => panic!("encode_wal_block: expected Fixed for column {}", ci),
                };
                assert_eq!(fixed.len(), count * stride,
                    "col {} Fixed length {} != count*stride {}", ci, fixed.len(), count * stride);
                col_regions.push(ColRegion::FixedRef(ci));
            }
        }
    }

    let blob_size = blob.len();

    // --- Assemble buffer: header + directory + aligned regions ---
    let dir_start = WAL_BLOCK_HEADER_SIZE;
    let dir_size = num_regions * 8;
    let mut buf: Vec<u8> = vec![0u8; dir_start + dir_size];
    let mut dir_entries: Vec<(u32, u32)> = Vec::with_capacity(num_regions);

    // System regions — write directly into buf, no temp Vecs
    dir_entries.push(append_pk_region(&mut buf, &batch.pks));
    dir_entries.push(append_64bit_region(&mut buf, &batch.weights));
    dir_entries.push(append_64bit_region(&mut buf, &batch.nulls));

    // Non-PK column regions
    for cr in &col_regions {
        match cr {
            ColRegion::Prebuilt(data) => {
                dir_entries.push(append_region(&mut buf, data));
            }
            ColRegion::FixedRef(ci) => {
                let fixed = match &batch.columns[*ci] {
                    ColData::Fixed(v) => v,
                    _ => unreachable!(),
                };
                dir_entries.push(append_region(&mut buf, fixed));
            }
        }
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
    let expected_num_regions = 3 + (schema.columns.len() - 1) + 1;
    if num_regions != expected_num_regions {
        return Err(ProtocolError::DecodeError(format!(
            "WAL block num_regions mismatch: expected {}, got {}",
            expected_num_regions, num_regions
        )));
    }
    let pk_stride = schema.columns[schema.pk_index].type_code.wire_stride();

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

    if count == 0 {
        return Ok((ZSetBatch::new(schema), table_id, lsn));
    }

    // Read system regions
    let mut region_idx = 0;

    let (pk_off,   pk_sz)   = dir[region_idx]; region_idx += 1;
    let (wt_off,   wt_sz)   = dir[region_idx]; region_idx += 1;
    let (null_off, null_sz) = dir[region_idx]; region_idx += 1;

    let expected_pk_sz = count * pk_stride;
    if pk_sz != expected_pk_sz {
        return Err(ProtocolError::DecodeError(format!(
            "pk region size mismatch: expected {}, got {}", expected_pk_sz, pk_sz
        )));
    }
    let pks: PkColumn = if pk_stride == 8 {
        let mut v = Vec::with_capacity(count);
        for i in 0..count {
            let base = pk_off + i * 8;
            if base + 8 > total_size {
                return Err(ProtocolError::DecodeError("pk region out of bounds".into()));
            }
            v.push(u64::from_le_bytes(data[base..base+8].try_into().unwrap()));
        }
        PkColumn::U64s(v)
    } else {
        let mut v = Vec::with_capacity(count);
        let mut pk_buf = [0u8; 16];
        for i in 0..count {
            let base = pk_off + i * pk_stride;
            if base + pk_stride > total_size {
                return Err(ProtocolError::DecodeError("pk region out of bounds".into()));
            }
            pk_buf[..pk_stride].copy_from_slice(&data[base..base + pk_stride]);
            v.push(u128::from_le_bytes(pk_buf));
        }
        PkColumn::U128s(v)
    };
    let weights: Vec<i64> = read_64bit_region(data, wt_off,   wt_sz,   count, "weights")?;
    let nulls: Vec<u64>   = read_64bit_region(data, null_off, null_sz, count, "nulls")?;

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
                let stride = col.type_code.wire_stride();
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

    Ok((ZSetBatch { pks, weights, nulls, columns }, table_id, lsn))
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
    use crate::protocol::types::{ColumnDef, PkColumn, Schema, TypeCode, ZSetBatch, ColData};

    fn u64_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(),  type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        }
    }

    fn str_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U64,    is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "s".into(),   type_code: TypeCode::String, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 },
            ],
            pk_index: 0,
        }
    }

    fn u128_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
                ColumnDef { name: "v".into(),   type_code: TypeCode::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
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
        let pks: Vec<u128>    = (0..n as u128).collect();
        let weights: Vec<i64> = vec![1; n];
        let nulls: Vec<u64>   = vec![0; n];
        let vals: Vec<i64>    = (0..n as i64).map(|x| x * -7).collect();
        let mut val_bytes = Vec::with_capacity(n * 8);
        for &v in &vals { val_bytes.extend_from_slice(&v.to_le_bytes()); }

        let batch = ZSetBatch {
            pks: PkColumn::U64s(pks.iter().map(|&x| x as u64).collect()),
            weights: weights.clone(), nulls: nulls.clone(),
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(val_bytes.clone())],
        };

        let encoded = encode_wal_block(&schema, 42, &batch);
        let (decoded, tid, _lsn) = decode_wal_block(&encoded, &schema).unwrap();

        assert_eq!(tid, 42);
        assert_eq!(decoded.pks, pks);
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
            pks: PkColumn::U64s((0..n as u64).collect()),
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
            pks: PkColumn::U128s(vals.clone()),
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
            pks: PkColumn::U64s(vec![]), weights: vec![], nulls: vec![],
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
            pks: PkColumn::U64s(vec![1u64]), weights: vec![1], nulls: vec![0],
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
            pks: PkColumn::U64s(vec![1u64]), weights: vec![1], nulls: vec![0],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(8u64.to_le_bytes().to_vec())],
        };
        let mut encoded = encode_wal_block(&schema, 0, &batch);
        // Set format_version = 1 (in header, offset 20..24)
        encoded[20..24].copy_from_slice(&1u32.to_le_bytes());
        // Note: checksum covers buf[48..] so changing header bytes does NOT invalidate checksum
        let res = decode_wal_block(&encoded, &schema);
        assert!(matches!(res, Err(ProtocolError::DecodeError(ref s)) if s.contains("version")));
    }

    // ── pk_stride roundtrips ───────────────────────────────────────────────

    #[test]
    fn pk_stride_wal_roundtrip_u64() {
        let schema = u64_schema();
        let pks = vec![1u128, 2, 3];
        let n = pks.len();
        let batch = ZSetBatch {
            pks: PkColumn::U64s(pks.iter().map(|&x| x as u64).collect()),
            weights: vec![1; n],
            nulls: vec![0; n],
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Fixed(vec![0u8; n * 8]),
            ],
        };
        let encoded = encode_wal_block(&schema, 0, &batch);

        // PK region is region 0 in the directory.
        let (pk_off, pk_sz) = get_region_offset_size(&encoded, 0);
        assert_eq!(pk_sz, n * 8, "U64 PK region must be 8B/row");

        // First PK value in the region is 1u64 LE.
        let expected_first = 1u64.to_le_bytes();
        assert_eq!(&encoded[pk_off..pk_off + 8], &expected_first);

        let (decoded, _, _) = decode_wal_block(&encoded, &schema).unwrap();
        assert_eq!(decoded.pks, pks);
    }

    #[test]
    fn pk_stride_wal_roundtrip_u128() {
        let schema = u128_schema();
        let pks = vec![1u128, 2, 3];
        let n = pks.len();
        let batch = ZSetBatch {
            pks: PkColumn::U128s(pks.clone()),
            weights: vec![1; n],
            nulls: vec![0; n],
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::U128s(pks.clone()),
            ],
        };
        let encoded = encode_wal_block(&schema, 0, &batch);

        let (_, pk_sz) = get_region_offset_size(&encoded, 0);
        assert_eq!(pk_sz, n * 16, "U128 PK region must be 16B/row");

        let (decoded, _, _) = decode_wal_block(&encoded, &schema).unwrap();
        assert_eq!(decoded.pks, pks);
    }

    #[test]
    fn wal_retraction_u64() {
        let schema = u64_schema();
        let pks = vec![10u128, 20, 30];
        let weights = vec![1i64, -1, 3];
        let n = pks.len();
        let batch = ZSetBatch {
            pks: PkColumn::U64s(pks.iter().map(|&x| x as u64).collect()),
            weights: weights.clone(),
            nulls: vec![0; n],
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Fixed(vec![0u8; n * 8]),
            ],
        };
        let encoded = encode_wal_block(&schema, 5, &batch);
        let (decoded, tid, _) = decode_wal_block(&encoded, &schema).unwrap();
        assert_eq!(tid, 5);
        assert_eq!(decoded.pks, pks);
        assert_eq!(decoded.weights, weights, "negative weight must survive encode/decode");
    }

    #[test]
    fn test_batch_appender_round_trip_u64_pk() {
        use crate::protocol::types::BatchAppender;
        let schema = u64_schema();
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1u128, 1).i64_val(10);
            a.add_row(100u128, 1).i64_val(200);
            a.add_row((u32::MAX as u128) + 1, -1).i64_val(300);
        }
        let encoded = encode_wal_block(&schema, 7, &batch);
        let (decoded, tid, _) = decode_wal_block(&encoded, &schema).unwrap();
        assert_eq!(tid, 7);
        assert_eq!(decoded.pks.len(), 3);
        assert_eq!(decoded.pks.get(0), 1u128);
        assert_eq!(decoded.pks.get(1), 100u128);
        assert_eq!(decoded.pks.get(2), (u32::MAX as u128) + 1);
        assert!(matches!(decoded.pks, PkColumn::U64s(_)), "U64 schema must decode to PkColumn::U64s");
    }

    #[test]
    fn test_batch_appender_round_trip_u128_pk() {
        use crate::protocol::types::BatchAppender;
        let schema = u128_schema();
        let pks = vec![0u128, u64::MAX as u128, (u64::MAX as u128) + 1, u128::MAX];
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            for &pk in &pks {
                a.add_row(pk, 1).u128_val(pk as u64, (pk >> 64) as u64);
            }
        }
        let encoded = encode_wal_block(&schema, 9, &batch);
        let (decoded, tid, _) = decode_wal_block(&encoded, &schema).unwrap();
        assert_eq!(tid, 9);
        assert_eq!(decoded.pks.len(), pks.len());
        for (i, &expected) in pks.iter().enumerate() {
            assert_eq!(decoded.pks.get(i), expected);
        }
        assert!(matches!(decoded.pks, PkColumn::U128s(_)), "U128 schema must decode to PkColumn::U128s");
    }

    #[test]
    fn test_xxh3_matches_python_server() {
        // Body bytes captured from a live Python server FLAG_ALLOCATE_SCHEMA_ID response.
        // Python computes checksum 0x741C9E0BA1D8A9FD using XXH3_64bits (gnitz_xxh3_64 C FFI).
        // This test verifies Rust twox_hash::xxh3 produces the same value.
        let body_hex = concat!(
            "9800000008000000a000000008000000a800000008000000",
            "b000000008000000b800000008000000c000000008000000",
            "c800000008000000d000000008000000d800000008000000",
            "e000000008000000e800000008000000f0000000100000",
            "0000100000000000000000000000000000000000000000000",
            "1000000000000008000000000000000000000000000000001",
            "00000000000000030000000000000000000000000000000000",
            "000000000000000000000000000000000000000000000000000000000000000000000000000"
        );
        // Use the raw hex instead
        let body_hex = "9800000008000000a000000008000000a800000008000000b000000008000000b800000008000000c000000008000000c800000008000000d000000008000000d800000008000000e000000008000000e800000008000000f00000001000000000010000000000000000000000000000000000000000000001000000000000008000000000000000000000000000000001000000000000000300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
        let body: Vec<u8> = (0..body_hex.len()).step_by(2)
            .map(|i| u8::from_str_radix(&body_hex[i..i+2], 16).unwrap())
            .collect();
        assert_eq!(body.len(), 208);
        let computed = xxh3_64(&body);
        eprintln!("Rust xxh3_64(body) = 0x{:016X}", computed);
        eprintln!("Expected           = 0x741C9E0BA1D8A9FD");
        assert_eq!(computed, 0x741C9E0BA1D8A9FD_u64,
            "Rust and Python xxh3_64 disagree for same bytes!");
    }
}
