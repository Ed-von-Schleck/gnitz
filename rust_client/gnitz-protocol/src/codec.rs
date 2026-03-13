use crate::error::ProtocolError;
use crate::header::{IPC_STRING_STRIDE, IPC_NULL_STRING_OFFSET, META_FLAG_NULLABLE, META_FLAG_IS_PK, ALIGNMENT};
use crate::types::{ColData, ColumnDef, Schema, TypeCode, ZSetBatch, meta_schema};

pub fn align_up(val: usize, align: usize) -> usize {
    (val + align - 1) & !(align - 1)
}

pub struct Layout {
    pub pk_lo_off:   usize,
    pub pk_hi_off:   usize,
    pub weight_off:  usize,
    pub null_off:    usize,
    /// One entry per schema column. Entry at pk_index = 0 (unused).
    pub col_offsets: Vec<usize>,
    /// One entry per schema column. Entry at pk_index = 0 (unused).
    pub col_strides: Vec<usize>,
    /// Start of string blob arena; equals total fixed-section size.
    pub blob_off:    usize,
}

/// Compute the wire layout for `count` rows under `schema`.
/// Mirrors Python's `_walk_layout`.
pub fn layout(schema: &Schema, count: usize) -> Layout {
    let struct_sz = count * 8;

    let pk_lo_off = 0;
    let mut cur = align_up(struct_sz, ALIGNMENT);

    let pk_hi_off = cur;
    cur = align_up(cur + struct_sz, ALIGNMENT);

    let weight_off = cur;
    cur = align_up(cur + struct_sz, ALIGNMENT);

    let null_off = cur;
    cur = align_up(cur + struct_sz, ALIGNMENT);

    let mut col_offsets = Vec::with_capacity(schema.columns.len());
    let mut col_strides = Vec::with_capacity(schema.columns.len());

    for (ci, col) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index {
            col_offsets.push(0);
            col_strides.push(0);
        } else if col.type_code == TypeCode::String {
            col_offsets.push(cur);
            col_strides.push(IPC_STRING_STRIDE);
            cur = align_up(cur + count * IPC_STRING_STRIDE, ALIGNMENT);
        } else {
            let stride = col.type_code.wire_stride();
            col_offsets.push(cur);
            col_strides.push(stride);
            cur = align_up(cur + stride * count, ALIGNMENT);
        }
    }

    Layout {
        pk_lo_off,
        pk_hi_off,
        weight_off,
        null_off,
        col_offsets,
        col_strides,
        blob_off: cur,
    }
}

/// Encode a ZSetBatch to wire bytes. Returns (section_bytes, blob_total_bytes).
/// Empty batch → (empty vec, 0).
pub fn encode_zset(schema: &Schema, batch: &ZSetBatch) -> (Vec<u8>, u64) {
    let count = batch.len();
    if count == 0 {
        return (Vec::new(), 0);
    }

    let lay = layout(schema, count);

    // First pass: collect blob bytes from string columns.
    // string_blobs[ci] = Vec<(global_offset: u32, length: u32)>
    let mut string_blobs: Vec<Option<Vec<(u32, u32)>>> = vec![None; schema.columns.len()];
    let mut blob_parts: Vec<Vec<u8>> = Vec::new();
    let mut blob_total: usize = 0;

    for (ci, col) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index || col.type_code != TypeCode::String {
            continue;
        }
        let strings = match &batch.columns[ci] {
            ColData::Strings(v) => v,
            _ => panic!("expected ColData::Strings for String column {}", ci),
        };
        let mut entries = Vec::with_capacity(count);
        for val in strings {
            match val {
                None => entries.push((IPC_NULL_STRING_OFFSET, 0u32)),
                Some(s) => {
                    let bytes = s.as_bytes();
                    let off = blob_total as u32;
                    let len = bytes.len() as u32;
                    entries.push((off, len));
                    blob_parts.push(bytes.to_vec());
                    blob_total += bytes.len();
                }
            }
        }
        string_blobs[ci] = Some(entries);
    }

    let total_size = lay.blob_off + blob_total;
    let mut buf = vec![0u8; total_size];

    // Write pk_lo, pk_hi, weights, nulls
    for row in 0..count {
        let base = lay.pk_lo_off + row * 8;
        buf[base..base + 8].copy_from_slice(&batch.pk_lo[row].to_le_bytes());

        let base = lay.pk_hi_off + row * 8;
        buf[base..base + 8].copy_from_slice(&batch.pk_hi[row].to_le_bytes());

        let base = lay.weight_off + row * 8;
        buf[base..base + 8].copy_from_slice(&batch.weights[row].to_le_bytes());

        let base = lay.null_off + row * 8;
        buf[base..base + 8].copy_from_slice(&batch.nulls[row].to_le_bytes());
    }

    // Write payload columns
    for (ci, col) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index {
            continue;
        }
        let off = lay.col_offsets[ci];

        match col.type_code {
            TypeCode::String => {
                let entries = string_blobs[ci].as_ref().unwrap();
                for (row, &(str_off, str_len)) in entries.iter().enumerate() {
                    let base = off + row * IPC_STRING_STRIDE;
                    buf[base..base + 4].copy_from_slice(&str_off.to_le_bytes());
                    buf[base + 4..base + 8].copy_from_slice(&str_len.to_le_bytes());
                }
            }
            TypeCode::U128 => {
                let vals = match &batch.columns[ci] {
                    ColData::U128s(v) => v,
                    _ => panic!("expected ColData::U128s for U128 column {}", ci),
                };
                for (row, &val) in vals.iter().enumerate() {
                    let lo = (val & 0xFFFF_FFFF_FFFF_FFFF) as u64;
                    let hi = (val >> 64) as u64;
                    let base = off + row * 16;
                    buf[base..base + 8].copy_from_slice(&lo.to_le_bytes());
                    buf[base + 8..base + 16].copy_from_slice(&hi.to_le_bytes());
                }
            }
            _ => {
                let stride = col.type_code.wire_stride();
                let fixed = match &batch.columns[ci] {
                    ColData::Fixed(v) => v,
                    _ => panic!("expected ColData::Fixed for column {}", ci),
                };
                buf[off..off + stride * count].copy_from_slice(fixed);
            }
        }
    }

    // Write blob arena
    let mut blob_pos = lay.blob_off;
    for part in &blob_parts {
        buf[blob_pos..blob_pos + part.len()].copy_from_slice(part);
        blob_pos += part.len();
    }

    (buf, blob_total as u64)
}

/// Decode a ZSet section from `data[offset..]`.
pub fn decode_zset(
    data: &[u8],
    offset: usize,
    schema: &Schema,
    count: usize,
    _blob_sz: usize,
) -> Result<ZSetBatch, ProtocolError> {
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
        return Ok(ZSetBatch {
            pk_lo: vec![],
            pk_hi: vec![],
            weights: vec![],
            nulls: vec![],
            columns,
        });
    }

    let lay = layout(schema, count);
    let base = offset;

    // Validate minimum buffer length
    let min_needed = base + lay.blob_off;
    if data.len() < min_needed {
        return Err(ProtocolError::DecodeError(format!(
            "buffer too short: {} < {}", data.len(), min_needed
        )));
    }

    macro_rules! read_u64_at {
        ($off:expr) => {
            u64::from_le_bytes(data[$off..$off + 8].try_into().unwrap())
        };
    }
    macro_rules! read_i64_at {
        ($off:expr) => {
            i64::from_le_bytes(data[$off..$off + 8].try_into().unwrap())
        };
    }

    let mut pk_lo   = Vec::with_capacity(count);
    let mut pk_hi   = Vec::with_capacity(count);
    let mut weights = Vec::with_capacity(count);
    let mut nulls   = Vec::with_capacity(count);

    for row in 0..count {
        pk_lo  .push(read_u64_at!(base + lay.pk_lo_off  + row * 8));
        pk_hi  .push(read_u64_at!(base + lay.pk_hi_off  + row * 8));
        weights.push(read_i64_at!(base + lay.weight_off + row * 8));
        nulls  .push(read_u64_at!(base + lay.null_off   + row * 8));
    }

    let blob_base = base + lay.blob_off;

    let mut columns: Vec<ColData> = Vec::with_capacity(schema.columns.len());

    for (ci, col) in schema.columns.iter().enumerate() {
        if ci == schema.pk_index {
            columns.push(ColData::Fixed(vec![]));
            continue;
        }

        let col_off = lay.col_offsets[ci];
        // payload_idx: column's bit position in the null bitmap
        let payload_idx = if ci < schema.pk_index { ci } else { ci - 1 };

        match col.type_code {
            TypeCode::String => {
                let mut vals: Vec<Option<String>> = Vec::with_capacity(count);
                for row in 0..count {
                    let is_null = (nulls[row] & (1u64 << payload_idx)) != 0;
                    let entry_base = base + col_off + row * IPC_STRING_STRIDE;
                    let str_off = u32::from_le_bytes(data[entry_base..entry_base + 4].try_into().unwrap());
                    let str_len = u32::from_le_bytes(data[entry_base + 4..entry_base + 8].try_into().unwrap());
                    if is_null || str_off == IPC_NULL_STRING_OFFSET {
                        vals.push(None);
                    } else {
                        let abs_start = blob_base + str_off as usize;
                        let abs_end   = abs_start + str_len as usize;
                        if abs_end > data.len() {
                            return Err(ProtocolError::DecodeError(format!(
                                "string out of bounds at row {}, ci {}", row, ci
                            )));
                        }
                        let s = std::str::from_utf8(&data[abs_start..abs_end])
                            .map_err(|e| ProtocolError::DecodeError(format!("utf8: {}", e)))?;
                        vals.push(Some(s.to_string()));
                    }
                }
                columns.push(ColData::Strings(vals));
            }
            TypeCode::U128 => {
                let mut vals: Vec<u128> = Vec::with_capacity(count);
                for row in 0..count {
                    let entry_base = base + col_off + row * 16;
                    let lo = u64::from_le_bytes(data[entry_base..entry_base + 8].try_into().unwrap());
                    let hi = u64::from_le_bytes(data[entry_base + 8..entry_base + 16].try_into().unwrap());
                    vals.push(lo as u128 | ((hi as u128) << 64));
                }
                columns.push(ColData::U128s(vals));
            }
            _ => {
                let stride = col.type_code.wire_stride();
                let start = base + col_off;
                let end   = start + count * stride;
                columns.push(ColData::Fixed(data[start..end].to_vec()));
            }
        }
    }

    Ok(ZSetBatch { pk_lo, pk_hi, weights, nulls, columns })
}

/// Convert a Schema to a META_SCHEMA-shaped ZSetBatch (one row per column).
/// Mirrors Python's `schema_to_batch`.
pub fn schema_to_batch(schema: &Schema) -> ZSetBatch {
    let ncols = schema.columns.len();
    let mut pk_lo   = Vec::with_capacity(ncols);
    let mut pk_hi   = Vec::with_capacity(ncols);
    let mut weights = Vec::with_capacity(ncols);
    let mut nulls   = Vec::with_capacity(ncols);

    // META_SCHEMA: col_idx(pk=0), type_code(U64), flags(U64), name(String)
    let mut type_code_bytes = Vec::with_capacity(ncols * 8);
    let mut flags_bytes     = Vec::with_capacity(ncols * 8);
    let mut names: Vec<Option<String>> = Vec::with_capacity(ncols);

    for (ci, col) in schema.columns.iter().enumerate() {
        pk_lo.push(ci as u64);
        pk_hi.push(0u64);
        weights.push(1i64);
        nulls.push(0u64);

        type_code_bytes.extend_from_slice(&(col.type_code as u64).to_le_bytes());

        let mut flags: u64 = 0;
        if col.is_nullable        { flags |= META_FLAG_NULLABLE; }
        if ci == schema.pk_index  { flags |= META_FLAG_IS_PK; }
        flags_bytes.extend_from_slice(&flags.to_le_bytes());

        names.push(Some(col.name.clone()));
    }

    ZSetBatch {
        pk_lo,
        pk_hi,
        weights,
        nulls,
        columns: vec![
            ColData::Fixed(vec![]),          // col 0 = col_idx (pk placeholder)
            ColData::Fixed(type_code_bytes), // col 1 = type_code
            ColData::Fixed(flags_bytes),     // col 2 = flags
            ColData::Strings(names),         // col 3 = name
        ],
    }
}

/// Reconstruct a Schema from a META_SCHEMA-shaped ZSetBatch.
/// Mirrors Python's `batch_to_schema`.
pub fn batch_to_schema(batch: &ZSetBatch) -> Result<Schema, ProtocolError> {
    let count = batch.len();
    let mut columns: Vec<ColumnDef> = Vec::with_capacity(count);
    let mut pk_index: Option<usize> = None;

    let type_code_fixed = match &batch.columns[1] {
        ColData::Fixed(v) => v,
        _ => return Err(ProtocolError::DecodeError("col 1 (type_code) must be Fixed".into())),
    };
    let flags_fixed = match &batch.columns[2] {
        ColData::Fixed(v) => v,
        _ => return Err(ProtocolError::DecodeError("col 2 (flags) must be Fixed".into())),
    };
    let names_col = match &batch.columns[3] {
        ColData::Strings(v) => v,
        _ => return Err(ProtocolError::DecodeError("col 3 (name) must be Strings".into())),
    };

    for i in 0..count {
        let col_idx = batch.pk_lo[i];
        if col_idx != i as u64 {
            return Err(ProtocolError::DecodeError(format!(
                "schema batch col_idx out of order: expected {}, got {}", i, col_idx
            )));
        }

        let type_code_raw = u64::from_le_bytes(
            type_code_fixed[i * 8..(i + 1) * 8].try_into().unwrap()
        );
        let flags = u64::from_le_bytes(
            flags_fixed[i * 8..(i + 1) * 8].try_into().unwrap()
        );
        let name = match &names_col[i] {
            Some(s) => s.clone(),
            None => return Err(ProtocolError::DecodeError(format!("null name at col {}", i))),
        };

        let tc = TypeCode::try_from_u64(type_code_raw)?;
        let is_nullable = (flags & META_FLAG_NULLABLE) != 0;
        let is_pk       = (flags & META_FLAG_IS_PK)    != 0;

        if is_pk {
            pk_index = Some(i);
        }

        columns.push(ColumnDef { name, type_code: tc, is_nullable });
    }

    let pk_index = pk_index.ok_or_else(|| {
        ProtocolError::DecodeError("no PK column found in schema batch".into())
    })?;

    Ok(Schema { columns, pk_index })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColData, Schema, ColumnDef, TypeCode, ZSetBatch};

    // ── align_up ────────────────────────────────────────────────────────────

    #[test]
    fn test_align_up() {
        assert_eq!(align_up(0,   64), 0);
        assert_eq!(align_up(1,   64), 64);
        assert_eq!(align_up(63,  64), 64);
        assert_eq!(align_up(64,  64), 64);
        assert_eq!(align_up(65,  64), 128);
        assert_eq!(align_up(127, 64), 128);
        assert_eq!(align_up(128, 64), 128);
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    // ── roundtrip: all fixed types ───────────────────────────────────────────

    #[test]
    fn test_roundtrip_all_fixed_types() {
        // Schema: pk=U64 (col 0), then U8, I8, U16, I16, U32, I32, F32, U64, I64, F64
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false },
                ColumnDef { name: "u8".into(),  type_code: TypeCode::U8,  is_nullable: false },
                ColumnDef { name: "i8".into(),  type_code: TypeCode::I8,  is_nullable: false },
                ColumnDef { name: "u16".into(), type_code: TypeCode::U16, is_nullable: false },
                ColumnDef { name: "i16".into(), type_code: TypeCode::I16, is_nullable: false },
                ColumnDef { name: "u32".into(), type_code: TypeCode::U32, is_nullable: false },
                ColumnDef { name: "i32".into(), type_code: TypeCode::I32, is_nullable: false },
                ColumnDef { name: "f32".into(), type_code: TypeCode::F32, is_nullable: false },
                ColumnDef { name: "u64".into(), type_code: TypeCode::U64, is_nullable: false },
                ColumnDef { name: "i64".into(), type_code: TypeCode::I64, is_nullable: false },
                ColumnDef { name: "f64".into(), type_code: TypeCode::F64, is_nullable: false },
            ],
            pk_index: 0,
        };

        let n = 5usize;
        let pk_lo: Vec<u64>   = (0..n as u64).collect();
        let pk_hi: Vec<u64>   = vec![0; n];
        let weights: Vec<i64> = vec![1; n];
        let nulls: Vec<u64>   = vec![0; n];

        let u8_vals:  Vec<u8>  = vec![1, 2, 3, 4, 5];
        let i8_vals:  Vec<i8>  = vec![-1, -2, 0, 127, -128];
        let u16_vals: Vec<u16> = vec![100, 200, 300, 400, 500];
        let i16_vals: Vec<i16> = vec![-100, 0, 100, -32768, 32767];
        let u32_vals: Vec<u32> = vec![0, 1, u32::MAX, 42, 999];
        let i32_vals: Vec<i32> = vec![i32::MIN, -1, 0, 1, i32::MAX];
        let f32_vals: Vec<f32> = vec![0.0, 1.5, -1.5, f32::MAX, f32::MIN_POSITIVE];
        let u64_vals: Vec<u64> = vec![0, 1, u64::MAX / 2, u64::MAX - 1, 12345678];
        let i64_vals: Vec<i64> = vec![i64::MIN, -1, 0, 1, i64::MAX];
        let f64_vals: Vec<f64> = vec![0.0, 1.0, -1.0, f64::MAX, f64::MIN_POSITIVE];

        // Build Fixed bytes
        fn mk_fixed<T: Copy, const S: usize>(vals: &[T], to_le: impl Fn(T) -> [u8; S]) -> Vec<u8> {
            let mut v = vec![];
            for &x in vals { v.extend_from_slice(&to_le(x)); }
            v
        }

        let cols = vec![
            ColData::Fixed(vec![]),                                     // pk placeholder
            ColData::Fixed(u8_vals.clone()),
            ColData::Fixed(i8_vals.iter().map(|&x| x as u8).collect()),
            ColData::Fixed(mk_fixed(&u16_vals, |x: u16| x.to_le_bytes())),
            ColData::Fixed(mk_fixed(&i16_vals, |x: i16| x.to_le_bytes())),
            ColData::Fixed(mk_fixed(&u32_vals, |x: u32| x.to_le_bytes())),
            ColData::Fixed(mk_fixed(&i32_vals, |x: i32| x.to_le_bytes())),
            ColData::Fixed(mk_fixed(&f32_vals, |x: f32| x.to_le_bytes())),
            ColData::Fixed(mk_fixed(&u64_vals, |x: u64| x.to_le_bytes())),
            ColData::Fixed(mk_fixed(&i64_vals, |x: i64| x.to_le_bytes())),
            ColData::Fixed(mk_fixed(&f64_vals, |x: f64| x.to_le_bytes())),
        ];

        let batch = ZSetBatch { pk_lo, pk_hi, weights, nulls, columns: cols };
        let (encoded, _blob_sz) = encode_zset(&schema, &batch);
        let decoded = decode_zset(&encoded, 0, &schema, n, 0).unwrap();

        // Verify structural fields
        assert_eq!(decoded.pk_lo,   batch.pk_lo);
        assert_eq!(decoded.pk_hi,   batch.pk_hi);
        assert_eq!(decoded.weights, batch.weights);
        assert_eq!(decoded.nulls,   batch.nulls);

        // Verify column data by comparing raw bytes
        for ci in 1..schema.columns.len() {
            match (&decoded.columns[ci], &batch.columns[ci]) {
                (ColData::Fixed(got), ColData::Fixed(expected)) => {
                    assert_eq!(got, expected, "column {} raw bytes differ", ci);
                }
                _ => panic!("unexpected ColData variant at col {}", ci),
            }
        }
    }

    // ── roundtrip: U128 ─────────────────────────────────────────────────────

    #[test]
    fn test_roundtrip_u128() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),      type_code: TypeCode::U128, is_nullable: false },
                ColumnDef { name: "payload".into(), type_code: TypeCode::U128, is_nullable: false },
            ],
            pk_index: 0,
        };

        let large_vals: Vec<u128> = vec![
            u128::MAX,
            0,
            1u128 << 64,
            (1u128 << 64) + 1,
            0xDEAD_BEEF_CAFE_BABE_1234_5678_9ABC_DEF0,
        ];
        let n = large_vals.len();

        // pk stored as pk_lo/pk_hi
        let pk_lo: Vec<u64> = large_vals.iter().map(|&v| v as u64).collect();
        let pk_hi: Vec<u64> = large_vals.iter().map(|&v| (v >> 64) as u64).collect();
        let weights: Vec<i64> = vec![1; n];
        let nulls: Vec<u64>   = vec![0; n];

        let batch = ZSetBatch {
            pk_lo,
            pk_hi,
            weights,
            nulls,
            columns: vec![
                ColData::Fixed(vec![]),              // pk placeholder
                ColData::U128s(large_vals.clone()),  // payload col
            ],
        };

        let (encoded, _blob) = encode_zset(&schema, &batch);
        let decoded = decode_zset(&encoded, 0, &schema, n, 0).unwrap();

        match &decoded.columns[1] {
            ColData::U128s(got) => assert_eq!(got, &large_vals),
            _ => panic!("expected U128s"),
        }
        assert_eq!(decoded.pk_lo, batch.pk_lo);
        assert_eq!(decoded.pk_hi, batch.pk_hi);
    }

    // ── roundtrip: strings ───────────────────────────────────────────────────

    #[test]
    fn test_roundtrip_strings() {
        // Schema: pk=U64, str_nullable(String), str_nonnull(String)
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),           type_code: TypeCode::U64,    is_nullable: false },
                ColumnDef { name: "str_nullable".into(), type_code: TypeCode::String, is_nullable: true  },
                ColumnDef { name: "str_nonnull".into(),  type_code: TypeCode::String, is_nullable: false },
            ],
            pk_index: 0,
        };

        let n = 5;
        let pk_lo: Vec<u64>   = (0..n as u64).collect();
        let pk_hi: Vec<u64>   = vec![0; n];
        let weights: Vec<i64> = vec![1; n];

        // Null bitmap: col1 (payload_idx=0) is nullable, col2 (payload_idx=1) is not.
        // Row 1: col1 is null → bit 0 set.
        let nulls: Vec<u64> = vec![0, 1, 0, 0, 0];

        let col1: Vec<Option<String>> = vec![
            Some("hello".into()),
            None,                     // row 1 null via null bitmap
            Some("".into()),          // empty string
            Some("日本語".into()),    // multi-byte UTF-8
            Some("world".into()),
        ];
        let col2: Vec<Option<String>> = vec![
            Some("a".into()),
            Some("b".into()),
            Some("c".into()),
            Some("d".into()),
            Some("e".into()),
        ];

        let batch = ZSetBatch {
            pk_lo,
            pk_hi,
            weights,
            nulls,
            columns: vec![
                ColData::Fixed(vec![]),    // pk placeholder
                ColData::Strings(col1.clone()),
                ColData::Strings(col2.clone()),
            ],
        };

        let (encoded, _blob) = encode_zset(&schema, &batch);
        let decoded = decode_zset(&encoded, 0, &schema, n, 0).unwrap();

        match &decoded.columns[1] {
            ColData::Strings(got) => assert_eq!(got, &col1),
            _ => panic!("expected Strings at col 1"),
        }
        match &decoded.columns[2] {
            ColData::Strings(got) => assert_eq!(got, &col2),
            _ => panic!("expected Strings at col 2"),
        }
        assert_eq!(decoded.nulls, batch.nulls);
    }

    // ── roundtrip: mixed ─────────────────────────────────────────────────────

    #[test]
    fn test_roundtrip_mixed() {
        // Schema: pk=U64, I64, STRING (nullable), U128
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U64,    is_nullable: false },
                ColumnDef { name: "i64".into(), type_code: TypeCode::I64,    is_nullable: false },
                ColumnDef { name: "str".into(), type_code: TypeCode::String, is_nullable: true  },
                ColumnDef { name: "u128".into(),type_code: TypeCode::U128,   is_nullable: false },
            ],
            pk_index: 0,
        };

        let n = 10;
        let pk_lo: Vec<u64>   = (0..n as u64).collect();
        let pk_hi: Vec<u64>   = vec![0; n];
        let weights: Vec<i64> = vec![1, -1, 2, -2, 1, 1, -3, 0, 1, 1];
        // col1=I64 (payload_idx=0), col2=String (payload_idx=1), col3=U128 (payload_idx=2)
        // Null str at rows 2, 5, 8 → bit 1 set in those nulls
        let nulls: Vec<u64>   = vec![0, 0, 2, 0, 0, 2, 0, 0, 2, 0];

        let i64_vals: Vec<i64> = (0..n as i64).map(|x| x * -100).collect();
        let str_vals: Vec<Option<String>> = vec![
            Some("alpha".into()), Some("beta".into()), None, Some("delta".into()),
            Some("epsilon".into()), None, Some("zeta".into()), Some("eta".into()),
            None, Some("iota".into()),
        ];
        let u128_vals: Vec<u128> = (0..n as u128).map(|x| x * (1u128 << 64) + x).collect();

        let mut i64_bytes = Vec::with_capacity(n * 8);
        for &v in &i64_vals { i64_bytes.extend_from_slice(&v.to_le_bytes()); }

        let batch = ZSetBatch {
            pk_lo,
            pk_hi,
            weights,
            nulls,
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Fixed(i64_bytes.clone()),
                ColData::Strings(str_vals.clone()),
                ColData::U128s(u128_vals.clone()),
            ],
        };

        let (encoded, _blob) = encode_zset(&schema, &batch);
        let decoded = decode_zset(&encoded, 0, &schema, n, 0).unwrap();

        assert_eq!(decoded.pk_lo,   batch.pk_lo);
        assert_eq!(decoded.weights, batch.weights);
        assert_eq!(decoded.nulls,   batch.nulls);

        match &decoded.columns[1] {
            ColData::Fixed(got) => assert_eq!(got, &i64_bytes),
            _ => panic!("expected Fixed at col 1"),
        }
        match &decoded.columns[2] {
            ColData::Strings(got) => assert_eq!(got, &str_vals),
            _ => panic!("expected Strings at col 2"),
        }
        match &decoded.columns[3] {
            ColData::U128s(got) => assert_eq!(got, &u128_vals),
            _ => panic!("expected U128s at col 3"),
        }
    }

    // ── schema/meta roundtrip ────────────────────────────────────────────────

    fn schemas_equal(a: &Schema, b: &Schema) -> bool {
        if a.pk_index != b.pk_index || a.columns.len() != b.columns.len() {
            return false;
        }
        for (ca, cb) in a.columns.iter().zip(b.columns.iter()) {
            if ca.name != cb.name || ca.type_code != cb.type_code || ca.is_nullable != cb.is_nullable {
                return false;
            }
        }
        true
    }

    #[test]
    fn test_schema_meta_roundtrip() {
        let original = Schema {
            columns: vec![
                ColumnDef { name: "id".into(),     type_code: TypeCode::U64,    is_nullable: false },
                ColumnDef { name: "name".into(),   type_code: TypeCode::String, is_nullable: true  },
                ColumnDef { name: "score".into(),  type_code: TypeCode::F64,    is_nullable: false },
                ColumnDef { name: "tag".into(),    type_code: TypeCode::I32,    is_nullable: true  },
                ColumnDef { name: "uuid".into(),   type_code: TypeCode::U128,   is_nullable: false },
            ],
            pk_index: 0,
        };

        let ms = meta_schema();
        let meta_batch = schema_to_batch(&original);
        let (encoded, blob_sz) = encode_zset(ms, &meta_batch);
        let count = meta_batch.len();
        let decoded_batch = decode_zset(&encoded, 0, ms, count, blob_sz as usize).unwrap();
        let reconstructed = batch_to_schema(&decoded_batch).unwrap();

        assert!(schemas_equal(&original, &reconstructed),
            "schema mismatch after meta roundtrip");
    }
}
