//! WAL-block encode/decode for the client wire codec.

use super::error::ProtocolError;
use super::regions::ViewBuffers;
use super::types::{null_word_get, ColData, PkColumn, Schema, TypeCode, ZSetBatch};
use gnitz_wire::{REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};

// ── Internal helpers ─────────────────────────────────────────────────────────

/// `&str` / `String` convenience wrappers over the wire German-string codec,
/// used only by the round-trip tests (production STRING cells are built by
/// `regions.rs` and read back by `decode_german_col`).
#[cfg(test)]
fn encode_german_str(s: &str, blob: &mut Vec<u8>) -> [u8; 16] {
    gnitz_wire::encode_german_string(s.as_bytes(), blob)
}

#[cfg(test)]
fn decode_german_str(st: [u8; 16], blob: &[u8]) -> Result<String, ProtocolError> {
    let bytes = gnitz_wire::try_decode_german_string(&st, blob)
        .ok_or_else(|| ProtocolError::DecodeError("German String blob arena out of bounds".into()))?;
    String::from_utf8(bytes).map_err(|e| ProtocolError::DecodeError(format!("utf8 in German String: {e}")))
}

/// Decode a STRING/BLOB column region into per-row raw-byte cells: `None` for
/// a null row, else the German string resolved against `blob`. Shared by the
/// STRING and BLOB decode arms (STRING then UTF-8-validates each cell).
/// `reg_off` is the region's directory offset; the caller has validated the
/// region is `count * 16` bytes and in-bounds, so the per-row struct extent
/// needs no re-check.
fn decode_german_col(
    data: &[u8],
    reg_off: usize,
    blob: &[u8],
    nulls: &[u64],
    payload_idx: usize,
    count: usize,
) -> Result<Vec<Option<Vec<u8>>>, ProtocolError> {
    let mut vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(count);
    for (row, &null_word) in nulls.iter().enumerate().take(count) {
        if null_word_get(null_word, payload_idx) {
            vals.push(None);
            continue;
        }
        let struct_start = reg_off + row * 16;
        let mut st = [0u8; 16];
        st.copy_from_slice(&data[struct_start..struct_start + 16]);
        vals.push(Some(gnitz_wire::try_decode_german_string(&st, blob).ok_or_else(
            || ProtocolError::DecodeError("German String blob arena out of bounds".into()),
        )?));
    }
    Ok(vals)
}

// ── Region read helpers ───────────────────────────────────────────────────────

/// Read a region of 64-bit values (u64 or i64) via bulk memcpy. Correct on little-endian.
fn read_64bit_region<T: Copy + Default>(
    data: &[u8],
    off: usize,
    sz: usize,
    count: usize,
    label: &str,
) -> Result<Vec<T>, ProtocolError> {
    debug_assert_eq!(std::mem::size_of::<T>(), 8);
    let expected = count * 8;
    if sz != expected {
        return Err(ProtocolError::DecodeError(format!(
            "{label} region size mismatch: expected {expected}, got {sz}"
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

/// Frame the batch's §6 region list ([`ViewBuffers::regions`]) into a WAL block.
pub fn encode_wal_block(schema: &Schema, table_id: u32, batch: &ZSetBatch) -> Vec<u8> {
    let mut bufs = ViewBuffers::default();
    let regions = bufs.regions(batch, schema);
    // Size the output to exactly one block, then frame in place, so encode never
    // returns BufferTooSmall. checksum = true: client frames always carry a body
    // checksum.
    let mut out = vec![0u8; gnitz_wire::wal::block_size_of(&regions)];
    gnitz_wire::wal::encode(&mut out, 0, table_id, batch.len() as u32, &regions, true)
        .expect("WAL encode: pre-sized buffer");
    out
}

/// Decode a WAL block from `data`, expecting columns described by `schema`.
/// Returns `(ZSetBatch, table_id)`.
///
/// Skips checksum verification — the production form: every client decode
/// runs over a trusted stream (OS-delivered Unix socket bytes or a TLS
/// record layer) where transport integrity is already guaranteed. Use
/// [`decode_wal_block_verified`] where the checksum itself is under test.
pub fn decode_wal_block(data: &[u8], schema: &Schema) -> Result<(ZSetBatch, u32), ProtocolError> {
    decode_wal_block_impl(data, schema, false)
}

/// Like [`decode_wal_block`] but verifies the block's XXH3 body checksum.
/// Kept `pub` for the cross-codec round-trip tests (here and in the engine's
/// wire tests, which use gnitz-core as a dev-dependency) — the only coverage
/// that client-encoded checksums are correct.
pub fn decode_wal_block_verified(data: &[u8], schema: &Schema) -> Result<(ZSetBatch, u32), ProtocolError> {
    decode_wal_block_impl(data, schema, true)
}

fn decode_wal_block_impl(
    data: &[u8],
    schema: &Schema,
    verify_checksum: bool,
) -> Result<(ZSetBatch, u32), ProtocolError> {
    // Shared framer validates format: version, `total_size` in-bounds, body
    // checksum, region count ≤ cap, and every region's [off, off+sz) extent
    // within the block. On success `dir` can index each region unchecked.
    let mut region_offsets = [0u64; gnitz_wire::MAX_WIRE_REGIONS];
    let mut region_sizes = [0u32; gnitz_wire::MAX_WIRE_REGIONS];
    let header = gnitz_wire::wal::validate_and_parse(data, &mut region_offsets, &mut region_sizes, verify_checksum)?;
    let table_id = header.table_id;
    let num_regions = header.num_regions as usize;

    // Client's half of the split: schema conformance.
    let expected_num_regions = gnitz_wire::wal::num_regions(schema.num_payload_cols());
    if num_regions != expected_num_regions {
        return Err(ProtocolError::DecodeError(format!(
            "WAL block num_regions mismatch: expected {expected_num_regions}, got {num_regions}"
        )));
    }
    let pk_stride = schema.pk_stride();

    // Index the framer-filled offset/size arrays directly — no per-decode heap
    // Vec (the engine's `decode_mem_batch_inner`/`decode_schema_block` do the same).
    let dir = |r: usize| (region_offsets[r] as usize, region_sizes[r] as usize);

    let count = header.entry_count as usize;

    if count == 0 {
        return Ok((ZSetBatch::new(schema), table_id));
    }

    // Read system regions, by the shared §6 index — the same rule the encoder
    // builds the list with, rather than a counter that happens to agree.
    let (pk_off, pk_sz) = dir(REG_PK);
    let (wt_off, wt_sz) = dir(REG_WEIGHT);
    let (null_off, null_sz) = dir(REG_NULL_BMP);

    let expected_pk_sz = count * pk_stride;
    if pk_sz != expected_pk_sz {
        return Err(ProtocolError::DecodeError(format!(
            "pk region size mismatch: expected {expected_pk_sz}, got {pk_sz}"
        )));
    }
    // Gate on PK column count, not stride: single-PK stays on the existing
    // numeric fast arms (byte-for-byte unchanged); a compound key never
    // decodes to a numeric variant.
    // The PK region at rest is OPK (order-preserving big-endian). Decode each
    // variant back to the native LE values the in-memory `PkColumn` holds, so
    // re-encoding the batch (which OPK-encodes assuming LE input) does not
    // double-encode.
    let pks: PkColumn = if schema.pk_count() >= 2 {
        // `stride` is a u8: reject (rather than silently truncate) any wire
        // schema whose packed PK region exceeds the field width.
        if pk_stride > u8::MAX as usize {
            return Err(ProtocolError::DecodeError(format!(
                "compound pk_stride {pk_stride} exceeds 255"
            )));
        }
        let col_info: Vec<(usize, u8)> = schema.pk_col_codes().collect();
        let mut decoded = Vec::with_capacity(pk_sz);
        let mut le_row = [0u8; gnitz_wire::MAX_PK_BYTES];
        for row in 0..count {
            let base = pk_off + row * pk_stride;
            let src = &data[base..base + pk_stride];
            let mut off = 0;
            for &(cs, tc) in &col_info {
                gnitz_wire::decode_pk_column(&src[off..off + cs], tc, &mut le_row[off..off + cs]);
                off += cs;
            }
            decoded.extend_from_slice(&le_row[..pk_stride]);
        }
        PkColumn::Bytes {
            stride: pk_stride as u8,
            buf: decoded,
        }
    } else if pk_stride <= 8 {
        // Narrow single-column PK: OPK → native LE via decode_pk_column. The
        // per-row extent is dominated by the `pk_sz == count * pk_stride` check
        // above plus the directory's `pk_off + pk_sz <= total_size`, so no
        // per-row bounds check is needed.
        let pk_tc = schema.columns[schema.pk_indices()[0]].type_code as u8;
        let mut v = Vec::with_capacity(count);
        for i in 0..count {
            v.push(gnitz_wire::pk_native_key(data, pk_off + i * pk_stride, pk_stride, pk_tc) as u64);
        }
        PkColumn::U64s(v)
    } else {
        // Lone 16-byte PK: U128/UUID (unsigned) or I128 (signed). decode_pk_column
        // reverses both — a plain byte-swap for the unsigned types (identical to the
        // prior from_be_bytes) and a byte-swap plus sign-flip for I128. Per-row
        // extent dominated by the region-size + directory checks (as above).
        let pk_tc = schema.columns[schema.pk_indices()[0]].type_code as u8;
        let mut v = Vec::with_capacity(count);
        for i in 0..count {
            v.push(gnitz_wire::pk_native_key(data, pk_off + i * 16, 16, pk_tc));
        }
        PkColumn::U128s(v)
    };
    let weights: Vec<i64> = read_64bit_region(data, wt_off, wt_sz, count, "weights")?;
    let nulls: Vec<u64> = read_64bit_region(data, null_off, null_sz, count, "nulls")?;

    // Blob region (always last)
    let (blob_off, blob_sz) = dir(num_regions - 1);
    let blob = if blob_sz > 0 {
        &data[blob_off..blob_off + blob_sz]
    } else {
        &[]
    };

    // Read column regions. The payload iterator supplies the slot, so payload
    // slot `pi` ↔ region `REG_PAYLOAD_START + pi` is stated, not produced as a
    // side effect of a counter; PK slots keep the empty `Fixed` placeholder
    // `ZSetBatch::filler_columns` builds.
    let mut columns = ZSetBatch::filler_columns(schema, 0);
    for (pi, ci, col) in schema.payload_columns() {
        let (reg_off, reg_sz) = dir(REG_PAYLOAD_START + pi);
        // One width rule for every column kind: `wire_stride` is 16 for STRING,
        // BLOB and the three 16-byte int types alike.
        let expected_sz = count * col.type_code.wire_stride();
        if reg_sz != expected_sz {
            return Err(ProtocolError::DecodeError(format!(
                "column {ci} region size mismatch: expected {expected_sz}, got {reg_sz}"
            )));
        }
        columns[ci] = match col.type_code {
            TypeCode::String => {
                // Raw German-string cells, then UTF-8-validate each into a String.
                let raw = decode_german_col(data, reg_off, blob, &nulls, pi, count)?;
                let mut vals: Vec<Option<String>> = Vec::with_capacity(count);
                for cell in raw {
                    vals.push(match cell {
                        None => None,
                        Some(bytes) => Some(
                            String::from_utf8(bytes)
                                .map_err(|e| ProtocolError::DecodeError(format!("utf8 in German String: {e}")))?,
                        ),
                    });
                }
                ColData::Strings(vals)
            }
            TypeCode::Blob => ColData::Bytes(decode_german_col(data, reg_off, blob, &nulls, pi, count)?),
            _ if col.type_code.is_wide_int() => {
                // Native u128 LE layout is byte-identical to the region; the
                // extent is dominated by the size + directory checks, so bulk-
                // copy the whole region rather than reading each row.
                let mut vals: Vec<u128> = vec![0u128; count];
                // SAFETY: `reg_off + count*16 <= total_size <= data.len()`
                // (region-size check + directory bound); `vals` has room for
                // `count*16` bytes. Distinct, non-overlapping regions.
                unsafe {
                    std::ptr::copy_nonoverlapping(data[reg_off..].as_ptr(), vals.as_mut_ptr() as *mut u8, count * 16);
                }
                ColData::U128s(vals)
            }
            _ => ColData::Fixed(data[reg_off..reg_off + reg_sz].to_vec()),
        };
    }

    Ok((
        ZSetBatch {
            pks,
            weights,
            nulls,
            columns,
        },
        table_id,
    ))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::{ColData, ColumnDef, PkColumn, Schema, TypeCode, ZSetBatch};
    use gnitz_wire::{
        WAL_FORMAT_VERSION, WAL_HEADER_SIZE as WAL_BLOCK_HEADER_SIZE, WAL_OFF_CHECKSUM, WAL_OFF_COUNT,
        WAL_OFF_NUM_REGIONS, WAL_OFF_SIZE, WAL_OFF_TID, WAL_OFF_VERSION,
    };

    /// Return the `(offset, size)` of a region from a block's directory.
    fn get_region_offset_size(block: &[u8], region_idx: usize) -> (usize, usize) {
        gnitz_wire::wal::dir_entry(block, region_idx)
    }

    fn u64_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        }
    }

    fn str_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, true),
            ],
            pk_cols: vec![0],
        }
    }

    fn u128_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U128, false),
                ColumnDef::new("v", TypeCode::U128, false),
            ],
            pk_cols: vec![0],
        }
    }

    // ── Header roundtrip ───────────────────────────────────────────────────

    #[test]
    fn test_header_roundtrip() {
        let mut buf = [0u8; WAL_BLOCK_HEADER_SIZE];
        buf[WAL_OFF_TID..WAL_OFF_TID + 4].copy_from_slice(&456u32.to_le_bytes());
        buf[WAL_OFF_COUNT..WAL_OFF_COUNT + 4].copy_from_slice(&10u32.to_le_bytes());
        buf[WAL_OFF_SIZE..WAL_OFF_SIZE + 4].copy_from_slice(&200u32.to_le_bytes());
        buf[WAL_OFF_VERSION..WAL_OFF_VERSION + 4].copy_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        buf[WAL_OFF_CHECKSUM..WAL_OFF_CHECKSUM + 8].copy_from_slice(&789u64.to_le_bytes());
        buf[WAL_OFF_NUM_REGIONS..WAL_OFF_NUM_REGIONS + 4].copy_from_slice(&5u32.to_le_bytes());

        assert_eq!(
            u32::from_le_bytes(buf[WAL_OFF_TID..WAL_OFF_TID + 4].try_into().unwrap()),
            456
        );
        assert_eq!(
            u32::from_le_bytes(buf[WAL_OFF_COUNT..WAL_OFF_COUNT + 4].try_into().unwrap()),
            10
        );
        assert_eq!(
            u32::from_le_bytes(buf[WAL_OFF_SIZE..WAL_OFF_SIZE + 4].try_into().unwrap()),
            200
        );
        assert_eq!(
            u32::from_le_bytes(buf[WAL_OFF_VERSION..WAL_OFF_VERSION + 4].try_into().unwrap()),
            WAL_FORMAT_VERSION
        );
        assert_eq!(
            u64::from_le_bytes(buf[WAL_OFF_CHECKSUM..WAL_OFF_CHECKSUM + 8].try_into().unwrap()),
            789
        );
        assert_eq!(
            u32::from_le_bytes(buf[WAL_OFF_NUM_REGIONS..WAL_OFF_NUM_REGIONS + 4].try_into().unwrap()),
            5
        );
    }

    // ── German String ─────────────────────────────────────────────────────

    #[test]
    fn test_german_string_short() {
        for s in &["", "a", "abcd", "abcdefghijkl"] {
            let mut blob = Vec::new();
            let st = encode_german_str(s, &mut blob);
            assert!(blob.is_empty(), "short string '{s}' should not use blob");
            let decoded = decode_german_str(st, &[]).unwrap();
            assert_eq!(&decoded, s, "roundtrip failed for '{s}'");
        }
    }

    #[test]
    fn test_german_string_long() {
        for s in &["abcdefghijklm", "hello world 12345", "a".repeat(100).as_str()] {
            let mut blob = Vec::new();
            let st = encode_german_str(s, &mut blob);
            assert!(!blob.is_empty(), "long string should use blob");
            let decoded = decode_german_str(st, &blob).unwrap();
            assert_eq!(&decoded, s, "roundtrip failed for long string");
        }
    }

    #[test]
    fn test_german_string_boundary() {
        // length=12 → inline (SHORT_STRING_THRESHOLD)
        let s12 = "123456789012";
        assert_eq!(s12.len(), 12);
        let mut blob12 = Vec::new();
        let st12 = encode_german_str(s12, &mut blob12);
        assert!(blob12.is_empty());
        assert_eq!(decode_german_str(st12, &[]).unwrap(), s12);

        // length=13 → blob
        let s13 = "1234567890123";
        assert_eq!(s13.len(), 13);
        let mut blob13 = Vec::new();
        let st13 = encode_german_str(s13, &mut blob13);
        assert_eq!(blob13.len(), 13);
        assert_eq!(decode_german_str(st13, &blob13).unwrap(), s13);
    }

    #[test]
    fn test_german_string_out_of_bounds_offset_errors() {
        // A long-string struct (len > 12) whose blob offset overruns the arena must
        // decode to a DecodeError, not panic: the offset bounds check now lives
        // entirely in the wire `try_decode_german_string`. Match the variant, not
        // the message text.
        let mut st = [0u8; 16];
        st[0..4].copy_from_slice(&100u32.to_le_bytes()); // len = 100 (> 12 → reads blob)
        st[8..16].copy_from_slice(&0u64.to_le_bytes()); // offset 0
        let blob = vec![0u8; 50]; // shorter than len → out of bounds
        let result = decode_german_str(st, &blob);
        assert!(
            matches!(result, Err(ProtocolError::DecodeError(_))),
            "out-of-bounds long-string offset must return DecodeError, got {result:?}"
        );
    }

    // ── encode/decode roundtrips ──────────────────────────────────────────

    #[test]
    fn test_encode_decode_fixed() {
        let schema = u64_schema();
        let n = 10usize;
        let pks: Vec<u128> = (0..n as u128).collect();
        let weights: Vec<i64> = vec![1; n];
        let nulls: Vec<u64> = vec![0; n];
        let vals: Vec<i64> = (0..n as i64).map(|x| x * -7).collect();
        let mut val_bytes = Vec::with_capacity(n * 8);
        for &v in &vals {
            val_bytes.extend_from_slice(&v.to_le_bytes());
        }

        let batch = ZSetBatch {
            pks: PkColumn::U64s(pks.iter().map(|&x| x as u64).collect()),
            weights: weights.clone(),
            nulls: nulls.clone(),
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(val_bytes.clone())],
        };

        let encoded = encode_wal_block(&schema, 42, &batch);
        let (decoded, tid) = decode_wal_block_verified(&encoded, &schema).unwrap();

        assert_eq!(tid, 42);
        assert_eq!(decoded.pks.to_vec_u128(), pks);
        assert!(
            matches!(decoded.pks, PkColumn::U64s(_)),
            "U64 schema must decode to PkColumn::U64s (no variant drift)"
        );
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
        let (decoded, _) = decode_wal_block_verified(&encoded, &schema).unwrap();

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
        let (decoded, tid) = decode_wal_block_verified(&encoded, &schema).unwrap();
        assert_eq!(tid, 1);
        assert!(
            matches!(decoded.pks, PkColumn::U128s(_)),
            "U128 schema must decode to PkColumn::U128s (no variant drift)"
        );
        match &decoded.columns[1] {
            ColData::U128s(got) => assert_eq!(got, &vals),
            _ => panic!("expected U128s"),
        }
    }

    /// A schema whose lone PK column is the signed-128 join-key type `I128`,
    /// with a second I128 payload column (the `SELECT _join_pk AS dup` shape).
    fn i128_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::I128, false),
                ColumnDef::new("v", TypeCode::I128, false),
            ],
            pk_cols: vec![0],
        }
    }

    /// Wide single-column I128 PK + payload round-trip. Pre-fix, the PK decode took
    /// the raw `from_be_bytes` "always unsigned" shortcut, so a signed key came back
    /// off by 2^127 (e.g. -1 → 2^127 - 1). The §4.6.1 OPK round-trip fixes that; the
    /// payload arm grouping fixes the `Fixed`-fallthrough. Both must surface the
    /// exact signed bits across the sign and 2^63/2^64 width boundaries.
    #[test]
    fn test_encode_decode_i128_signed_roundtrip() {
        // for_type must pick U128s (16-byte storage), not the U64s truncation that
        // produced "weights length != row count".
        assert!(matches!(PkColumn::for_type(TypeCode::I128), PkColumn::U128s(_)));

        let schema = i128_schema();
        let signed: Vec<i128> = vec![
            i128::MIN,
            -1,
            0,
            1,
            i128::MAX,
            1i128 << 63,
            (1i128 << 63) - 1,
            1i128 << 64,
            (1i128 << 64) - 1,
        ];
        // PkColumn / ColData::U128s hold the native bits as u128.
        let bits: Vec<u128> = signed.iter().map(|&x| x as u128).collect();
        let n = bits.len();

        let batch = ZSetBatch {
            pks: PkColumn::U128s(bits.clone()),
            weights: vec![1; n],
            nulls: vec![0; n],
            columns: vec![ColData::Fixed(vec![]), ColData::U128s(bits.clone())],
        };

        let encoded = encode_wal_block(&schema, 3, &batch);
        let (decoded, tid) = decode_wal_block_verified(&encoded, &schema).unwrap();
        assert_eq!(tid, 3);

        // (a) No PK-variant drift: a lone 16-byte PK stays U128s.
        let pk_bits = match &decoded.pks {
            PkColumn::U128s(v) => v,
            other => panic!("I128 PK must decode to U128s, got {other:?}"),
        };
        // (b) Each signed value survives — reinterpret the recovered bits as i128.
        let got_pk: Vec<i128> = pk_bits.iter().map(|&x| x as i128).collect();
        assert_eq!(got_pk, signed, "I128 PK sign-flip must round-trip");

        // The I128 payload arm must decode to U128s with the exact native bits.
        match &decoded.columns[1] {
            ColData::U128s(got) => {
                let got_signed: Vec<i128> = got.iter().map(|&x| x as i128).collect();
                assert_eq!(got_signed, signed, "I128 payload must round-trip");
            }
            other => panic!("I128 payload must decode to U128s, got {other:?}"),
        }
    }

    #[test]
    fn test_encode_decode_empty() {
        let schema = u64_schema();
        let empty = ZSetBatch {
            pks: PkColumn::U64s(vec![]),
            weights: vec![],
            nulls: vec![],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(vec![])],
        };
        let encoded = encode_wal_block(&schema, 7, &empty);
        let (decoded, tid) = decode_wal_block_verified(&encoded, &schema).unwrap();
        assert_eq!(tid, 7);
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn test_bad_checksum() {
        let schema = u64_schema();
        let batch = ZSetBatch {
            pks: PkColumn::U64s(vec![1u64]),
            weights: vec![1],
            nulls: vec![0],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(8u64.to_le_bytes().to_vec())],
        };
        let mut encoded = encode_wal_block(&schema, 0, &batch);
        // Flip a byte in the body (after header)
        encoded[WAL_BLOCK_HEADER_SIZE] ^= 0xFF;
        let res = decode_wal_block_verified(&encoded, &schema);
        assert!(matches!(res, Err(ProtocolError::DecodeError(ref s)) if s.contains("checksum")));
    }

    #[test]
    fn test_bad_version() {
        let schema = u64_schema();
        let batch = ZSetBatch {
            pks: PkColumn::U64s(vec![1u64]),
            weights: vec![1],
            nulls: vec![0],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(8u64.to_le_bytes().to_vec())],
        };
        let mut encoded = encode_wal_block(&schema, 0, &batch);
        // Set format_version = 1.
        encoded[WAL_OFF_VERSION..WAL_OFF_VERSION + 4].copy_from_slice(&1u32.to_le_bytes());
        // Note: checksum covers only the body, so changing header bytes does NOT invalidate it
        let res = decode_wal_block_verified(&encoded, &schema);
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
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(vec![0u8; n * 8])],
        };
        let encoded = encode_wal_block(&schema, 0, &batch);

        // PK region is region 0 in the directory.
        let (pk_off, pk_sz) = get_region_offset_size(&encoded, 0);
        assert_eq!(pk_sz, n * 8, "U64 PK region must be 8B/row");

        // PK region is OPK-at-rest: an unsigned U64 encodes to big-endian.
        let expected_first = 1u64.to_be_bytes();
        assert_eq!(&encoded[pk_off..pk_off + 8], &expected_first);

        let (decoded, _) = decode_wal_block_verified(&encoded, &schema).unwrap();
        assert_eq!(decoded.pks.to_vec_u128(), pks);
        assert!(
            matches!(decoded.pks, PkColumn::U64s(_)),
            "U64 schema must decode to PkColumn::U64s (no variant drift)"
        );
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
            columns: vec![ColData::Fixed(vec![]), ColData::U128s(pks.clone())],
        };
        let encoded = encode_wal_block(&schema, 0, &batch);

        let (_, pk_sz) = get_region_offset_size(&encoded, 0);
        assert_eq!(pk_sz, n * 16, "U128 PK region must be 16B/row");

        let (decoded, _) = decode_wal_block_verified(&encoded, &schema).unwrap();
        assert_eq!(decoded.pks.to_vec_u128(), pks);
        assert!(
            matches!(decoded.pks, PkColumn::U128s(_)),
            "U128 schema must decode to PkColumn::U128s (no variant drift)"
        );
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
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(vec![0u8; n * 8])],
        };
        let encoded = encode_wal_block(&schema, 5, &batch);
        let (decoded, tid) = decode_wal_block_verified(&encoded, &schema).unwrap();
        assert_eq!(tid, 5);
        assert_eq!(decoded.pks.to_vec_u128(), pks);
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
        let (decoded, tid) = decode_wal_block_verified(&encoded, &schema).unwrap();
        assert_eq!(tid, 7);
        assert_eq!(decoded.pks.len(), 3);
        assert_eq!(decoded.pks.get(0), 1u128);
        assert_eq!(decoded.pks.get(1), 100u128);
        assert_eq!(decoded.pks.get(2), (u32::MAX as u128) + 1);
        assert!(
            matches!(decoded.pks, PkColumn::U64s(_)),
            "U64 schema must decode to PkColumn::U64s"
        );
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
                a.add_row(pk, 1).u128_val(pk);
            }
        }
        let encoded = encode_wal_block(&schema, 9, &batch);
        let (decoded, tid) = decode_wal_block_verified(&encoded, &schema).unwrap();
        assert_eq!(tid, 9);
        assert_eq!(decoded.pks.len(), pks.len());
        for (i, &expected) in pks.iter().enumerate() {
            assert_eq!(decoded.pks.get(i), expected);
        }
        assert!(
            matches!(decoded.pks, PkColumn::U128s(_)),
            "U128 schema must decode to PkColumn::U128s"
        );
    }

    #[test]
    fn test_xxh3_pins_known_vector() {
        // A pinned XXH3-64 test vector: `gnitz_wire::checksum` (the one the client
        // now stamps every frame with) of these 208 body bytes must equal this
        // constant. Guards against a silent hash-library or parameterization swap
        // that would break wire-checksum interop.
        let body_hex = "9800000008000000a000000008000000a800000008000000b000000008000000b800000008000000c000000008000000c800000008000000d000000008000000d800000008000000e000000008000000e800000008000000f00000001000000000010000000000000000000000000000000000000000000001000000000000008000000000000000000000000000000001000000000000000300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
        let body: Vec<u8> = (0..body_hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&body_hex[i..i + 2], 16).unwrap())
            .collect();
        assert_eq!(body.len(), 208);
        assert_eq!(
            gnitz_wire::checksum(&body),
            0x741C9E0BA1D8A9FD_u64,
            "xxh3_64 test vector drifted"
        );
    }

    // ── wide compound-PK roundtrips ────────────────────────────────────────
    //
    // The SQL planner still rejects compound PRIMARY KEY, so these schemas are
    // hand-built via the pk_cols field. The wide-PK ZSetBatch is a struct
    // literal (ZSetBatch::new debug-asserts pk_count == 1).

    /// `(U64, U128)` compound PK + one I64 payload column. pk_stride == 24.
    fn wide24_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("pk0", TypeCode::U64, false),
                ColumnDef::new("pk1", TypeCode::U128, false),
                ColumnDef::new("v", TypeCode::I64, true),
            ],
            pk_cols: vec![0, 1],
        }
    }

    /// Pack one `(u64, u128)` PK tuple into 24 on-wire LE bytes.
    fn pk24(a: u64, b: u128) -> [u8; 24] {
        let mut out = [0u8; 24];
        out[0..8].copy_from_slice(&a.to_le_bytes());
        out[8..24].copy_from_slice(&b.to_le_bytes());
        out
    }

    #[test]
    fn pk_stride_wal_roundtrip_bytes_24() {
        let schema = wide24_schema();
        assert_eq!(schema.pk_stride(), 24);
        assert_eq!(schema.pk_count(), 2);

        let tuples = [
            pk24(1, 100),
            pk24(u64::MAX, u128::MAX),
            pk24(7, (1u128 << 64) + 9),
            pk24(0, 0),
        ];
        let n = tuples.len();
        let mut pk_buf = Vec::with_capacity(n * 24);
        for t in &tuples {
            pk_buf.extend_from_slice(t);
        }

        let weights = vec![1i64, -1, 3, 1];
        // Row 2 has a NULL payload (payload bit 0).
        let nulls = vec![0u64, 0, 1, 0];
        let payload: Vec<i64> = vec![10, 20, 30, 40];
        let mut payload_bytes = Vec::with_capacity(n * 8);
        for &v in &payload {
            payload_bytes.extend_from_slice(&v.to_le_bytes());
        }

        let batch = ZSetBatch {
            pks: PkColumn::Bytes {
                stride: 24,
                buf: pk_buf.clone(),
            },
            weights: weights.clone(),
            nulls: nulls.clone(),
            columns: vec![
                ColData::Fixed(vec![]), // pk0 placeholder
                ColData::Fixed(vec![]), // pk1 placeholder
                ColData::Fixed(payload_bytes.clone()),
            ],
        };

        let encoded = encode_wal_block(&schema, 77, &batch);

        // PK region (region 0) must be count * 24 bytes.
        let (_, pk_sz) = get_region_offset_size(&encoded, 0);
        assert_eq!(pk_sz, n * 24, "wide PK region must be 24B/row");

        let (decoded, tid) = decode_wal_block_verified(&encoded, &schema).unwrap();
        assert_eq!(tid, 77);
        assert_eq!(decoded.weights, weights);
        assert_eq!(decoded.nulls, nulls);
        match &decoded.pks {
            PkColumn::Bytes { stride, buf } => {
                assert_eq!(*stride, 24);
                assert_eq!(buf, &pk_buf, "wide PK bytes must roundtrip exactly");
                assert_eq!(decoded.pks.len(), n);
            }
            _ => panic!("compound PK must decode to PkColumn::Bytes"),
        }
        match &decoded.columns[2] {
            ColData::Fixed(got) => assert_eq!(got, &payload_bytes),
            _ => panic!("expected Fixed payload"),
        }
    }

    #[test]
    fn pk_stride_wal_roundtrip_bytes_64() {
        // Maximum user pk_stride: four U128 PK columns = 64 bytes/row.
        let schema = Schema {
            columns: vec![
                ColumnDef::new("a", TypeCode::U128, false),
                ColumnDef::new("b", TypeCode::U128, false),
                ColumnDef::new("c", TypeCode::U128, false),
                ColumnDef::new("d", TypeCode::U128, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            pk_cols: vec![0, 1, 2, 3],
        };
        assert_eq!(schema.pk_stride(), 64);

        let rows: [[u128; 4]; 3] = [
            [0, 1, 2, 3],
            [u128::MAX, 1u128 << 64, 42, (1u128 << 64) + 7],
            [9, 8, 7, 6],
        ];
        let n = rows.len();
        let mut pk_buf = Vec::with_capacity(n * 64);
        for r in &rows {
            for &cval in r {
                pk_buf.extend_from_slice(&cval.to_le_bytes());
            }
        }
        let payload: Vec<i64> = vec![-1, 0, 123];
        let mut payload_bytes = Vec::with_capacity(n * 8);
        for &v in &payload {
            payload_bytes.extend_from_slice(&v.to_le_bytes());
        }

        let batch = ZSetBatch {
            pks: PkColumn::Bytes {
                stride: 64,
                buf: pk_buf.clone(),
            },
            weights: vec![1; n],
            nulls: vec![0; n],
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Fixed(vec![]),
                ColData::Fixed(vec![]),
                ColData::Fixed(vec![]),
                ColData::Fixed(payload_bytes.clone()),
            ],
        };

        let encoded = encode_wal_block(&schema, 3, &batch);
        let (_, pk_sz) = get_region_offset_size(&encoded, 0);
        assert_eq!(pk_sz, n * 64, "wide PK region must be 64B/row");

        let (decoded, tid) = decode_wal_block_verified(&encoded, &schema).unwrap();
        assert_eq!(tid, 3);
        match &decoded.pks {
            PkColumn::Bytes { stride, buf } => {
                assert_eq!(*stride, 64);
                assert_eq!(buf, &pk_buf);
            }
            _ => panic!("compound PK must decode to PkColumn::Bytes"),
        }
        match &decoded.columns[4] {
            ColData::Fixed(got) => assert_eq!(got, &payload_bytes),
            _ => panic!("expected Fixed payload"),
        }
    }

    #[test]
    fn wide_pk_extend_from_concatenates() {
        let mk = |a: u64, b: u128| {
            let mut buf = Vec::new();
            buf.extend_from_slice(&pk24(a, b));
            ZSetBatch {
                pks: PkColumn::Bytes { stride: 24, buf },
                weights: vec![1],
                nulls: vec![0],
                columns: vec![
                    ColData::Fixed(vec![]),
                    ColData::Fixed(vec![]),
                    ColData::Fixed(5i64.to_le_bytes().to_vec()),
                ],
            }
        };
        let mut a = mk(1, 11);
        let b = mk(2, 22);
        a.extend_from_owned(b);

        assert_eq!(a.pks.len(), 2);
        let mut expected = Vec::new();
        expected.extend_from_slice(&pk24(1, 11));
        expected.extend_from_slice(&pk24(2, 22));
        match &a.pks {
            PkColumn::Bytes { stride, buf } => {
                assert_eq!(*stride, 24);
                assert_eq!(buf, &expected);
            }
            _ => panic!("expected Bytes"),
        }
        assert_eq!(a.weights, vec![1, 1]);
        match &a.columns[2] {
            ColData::Fixed(got) => assert_eq!(got.len(), 16),
            _ => panic!("expected Fixed"),
        }
    }
}
