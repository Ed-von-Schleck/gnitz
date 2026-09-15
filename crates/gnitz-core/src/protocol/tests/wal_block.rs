use super::*;
use crate::protocol::types::{ColumnDef, PkColumn, Schema, TypeCode, ZSetBatch};
use crate::test_support::payload_of;
use gnitz_wire::WAL_OFF_VERSION;

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

/// A 16-byte-int payload column region from its values.
fn wide_col(vals: &[u128]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// Read a 16-byte-int payload column back out as values.
fn wide_vals(col: &[u8]) -> Vec<u128> {
    col.as_chunks::<16>()
        .0
        .iter()
        .map(|c| u128::from_le_bytes(*c))
        .collect()
}

/// A STRING/BLOB column region from its values, spilling into `blob`; `None` is
/// a NULL cell, which the region zero-fills.
fn german_col(vals: &[Option<&str>], blob: &mut Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(vals.len() * 16);
    for v in vals {
        out.extend_from_slice(&gnitz_wire::encode_german_string(v.unwrap_or("").as_bytes(), blob));
    }
    out
}

/// Read a STRING column region back out, `None` where the null bit is set.
fn german_vals(batch: &ZSetBatch, pi: usize) -> Vec<Option<String>> {
    (0..batch.len())
        .map(|row| {
            (!gnitz_wire::null_word_get(batch.nulls[row], pi)).then(|| {
                let cell = &batch.payload[pi].bytes[row * 16..(row + 1) * 16];
                String::from_utf8(gnitz_wire::german_string_content(cell, &batch.blob).to_vec()).unwrap()
            })
        })
        .collect()
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

/// A German cell whose blob offset overruns the arena must surface as a
/// `DecodeError` from the production decode path, not a panic. The German
/// codec itself is covered in `gnitz-wire`; what this pins is the client's
/// mapping of its `None` onto a protocol error.
#[test]
fn a_string_cell_pointing_past_the_blob_is_a_decode_error() {
    let schema = str_schema();
    let mut blob = Vec::new();
    let batch = ZSetBatch {
        pks: PkColumn::from_natives(&schema, [1u128]),
        weights: vec![1],
        nulls: vec![0],
        payload: payload_of(
            &schema,
            vec![german_col(&[Some("a value well past the inline cell")], &mut blob)],
        ),
        blob,
    };
    let mut block = encode_wal_block(7, &batch);
    // The lone payload region holds one 16-byte German cell; its bytes
    // 8..16 are the blob offset. Push it past the arena.
    let (off, _) = get_region_offset_size(&block, gnitz_wire::REG_PAYLOAD_START);
    block[off + 8..off + 16].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(matches!(
        decode_wal_block(&block, &schema),
        Err(ProtocolError::DecodeError(_))
    ));
}

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
        pks: PkColumn::from_natives(&schema, pks.iter().copied()),
        weights: weights.clone(),
        nulls: nulls.clone(),
        payload: payload_of(&schema, vec![val_bytes.clone()]),
        blob: vec![],
    };

    let encoded = encode_wal_block(42, &batch);
    let (decoded, tid) = decode_wal_block(&encoded, &schema).unwrap();

    assert_eq!(tid, 42);
    assert_eq!(decoded.pks.to_vec_u128(&schema), pks);
    assert_eq!(decoded.weights, weights);
    assert_eq!(decoded.nulls, nulls);
    {
        let got = &decoded.payload[0].bytes;
        assert_eq!(got, &val_bytes);
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

    let cells: Vec<Option<&str>> = vals.iter().map(|v| v.as_deref()).collect();
    let mut blob = Vec::new();
    let batch = ZSetBatch {
        pks: PkColumn::from_natives(&schema, 0..n as u128),
        weights: vec![1; n],
        nulls: nulls.clone(),
        payload: payload_of(&schema, vec![german_col(&cells, &mut blob)]),
        blob,
    };

    let encoded = encode_wal_block(0, &batch);
    let (decoded, _) = decode_wal_block(&encoded, &schema).unwrap();

    assert_eq!(decoded.nulls, nulls);
    assert_eq!(german_vals(&decoded, 0), vals);
}

#[test]
fn test_encode_decode_u128() {
    let schema = u128_schema();
    let vals: Vec<u128> = vec![0, 1, u128::MAX, 1u128 << 64, (1u128 << 64) + 42];
    let n = vals.len();

    let batch = ZSetBatch {
        pks: PkColumn::from_natives(&schema, vals.iter().copied()),
        weights: vec![1; n],
        nulls: vec![0; n],
        payload: payload_of(&schema, vec![wide_col(&vals)]),
        blob: vec![],
    };

    let encoded = encode_wal_block(1, &batch);
    let (decoded, tid) = decode_wal_block(&encoded, &schema).unwrap();
    assert_eq!(tid, 1);
    assert_eq!(wide_vals(&decoded.payload[0].bytes), vals);
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
    let schema = i128_schema();
    // A lone I128 key is 16 bytes wide, not the 8-byte truncation that once
    // produced "weights length != row count".
    assert_eq!(schema.pk_stride(), 16);
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
    // PK and payload both hold the native bits.
    let bits: Vec<u128> = signed.iter().map(|&x| x as u128).collect();
    let n = bits.len();

    let batch = ZSetBatch {
        pks: PkColumn::from_natives(&schema, bits.iter().copied()),
        weights: vec![1; n],
        nulls: vec![0; n],
        payload: payload_of(&schema, vec![wide_col(&bits)]),
        blob: vec![],
    };

    let encoded = encode_wal_block(3, &batch);
    let (decoded, tid) = decode_wal_block(&encoded, &schema).unwrap();
    assert_eq!(tid, 3);

    // Each signed value survives — reinterpret the recovered bits as i128.
    let got_pk: Vec<i128> = (0..decoded.pks.len())
        .map(|i| decoded.pks.get(&schema, i) as i128)
        .collect();
    assert_eq!(got_pk, signed, "I128 PK sign-flip must round-trip");

    // The I128 payload must decode with the exact native bits.
    let got_signed: Vec<i128> = wide_vals(&decoded.payload[0].bytes)
        .into_iter()
        .map(|x| x as i128)
        .collect();
    assert_eq!(got_signed, signed, "I128 payload must round-trip");
}

#[test]
fn test_encode_decode_empty() {
    let schema = u64_schema();
    let empty = ZSetBatch {
        pks: PkColumn::empty_for_schema(&schema),
        weights: vec![],
        nulls: vec![],
        payload: payload_of(&schema, vec![vec![]]),
        blob: vec![],
    };
    let encoded = encode_wal_block(7, &empty);
    let (decoded, tid) = decode_wal_block(&encoded, &schema).unwrap();
    assert_eq!(tid, 7);
    assert_eq!(decoded.len(), 0);
}

#[test]
fn test_bad_version() {
    let schema = u64_schema();
    let batch = ZSetBatch {
        pks: PkColumn::from_natives(&schema, [1]),
        weights: vec![1],
        nulls: vec![0],
        payload: payload_of(&schema, vec![8u64.to_le_bytes().to_vec()]),
        blob: vec![],
    };
    let mut encoded = encode_wal_block(0, &batch);
    // Set format_version = 1.
    encoded[WAL_OFF_VERSION..WAL_OFF_VERSION + 4].copy_from_slice(&1u32.to_le_bytes());
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
        pks: PkColumn::from_natives(&schema, pks.iter().copied()),
        weights: vec![1; n],
        nulls: vec![0; n],
        payload: payload_of(&schema, vec![vec![0u8; n * 8]]),
        blob: vec![],
    };
    let encoded = encode_wal_block(0, &batch);

    // PK region is region 0 in the directory.
    let (pk_off, pk_sz) = get_region_offset_size(&encoded, 0);
    assert_eq!(pk_sz, n * 8, "U64 PK region must be 8B/row");

    // PK region is OPK-at-rest: an unsigned U64 encodes to big-endian.
    let expected_first = 1u64.to_be_bytes();
    assert_eq!(&encoded[pk_off..pk_off + 8], &expected_first);

    let (decoded, _) = decode_wal_block(&encoded, &schema).unwrap();
    assert_eq!(decoded.pks.to_vec_u128(&schema), pks);
}

#[test]
fn pk_stride_wal_roundtrip_u128() {
    let schema = u128_schema();
    let pks = vec![1u128, 2, 3];
    let n = pks.len();
    let batch = ZSetBatch {
        pks: PkColumn::from_natives(&schema, pks.iter().copied()),
        weights: vec![1; n],
        nulls: vec![0; n],
        payload: payload_of(&schema, vec![wide_col(&pks)]),
        blob: vec![],
    };
    let encoded = encode_wal_block(0, &batch);

    let (_, pk_sz) = get_region_offset_size(&encoded, 0);
    assert_eq!(pk_sz, n * 16, "U128 PK region must be 16B/row");

    let (decoded, _) = decode_wal_block(&encoded, &schema).unwrap();
    assert_eq!(decoded.pks.to_vec_u128(&schema), pks);
}

#[test]
fn wal_retraction_u64() {
    let schema = u64_schema();
    let pks = vec![10u128, 20, 30];
    let weights = vec![1i64, -1, 3];
    let n = pks.len();
    let batch = ZSetBatch {
        pks: PkColumn::from_natives(&schema, pks.iter().copied()),
        weights: weights.clone(),
        nulls: vec![0; n],
        payload: payload_of(&schema, vec![vec![0u8; n * 8]]),
        blob: vec![],
    };
    let encoded = encode_wal_block(5, &batch);
    let (decoded, tid) = decode_wal_block(&encoded, &schema).unwrap();
    assert_eq!(tid, 5);
    assert_eq!(decoded.pks.to_vec_u128(&schema), pks);
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
    let encoded = encode_wal_block(7, &batch);
    let (decoded, tid) = decode_wal_block(&encoded, &schema).unwrap();
    assert_eq!(tid, 7);
    assert_eq!(decoded.pks.len(), 3);
    assert_eq!(decoded.pks.get(&schema, 0), 1u128);
    assert_eq!(decoded.pks.get(&schema, 1), 100u128);
    assert_eq!(decoded.pks.get(&schema, 2), (u32::MAX as u128) + 1);
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
    let encoded = encode_wal_block(9, &batch);
    let (decoded, tid) = decode_wal_block(&encoded, &schema).unwrap();
    assert_eq!(tid, 9);
    assert_eq!(decoded.pks.len(), pks.len());
    for (i, &expected) in pks.iter().enumerate() {
        assert_eq!(decoded.pks.get(&schema, i), expected);
    }
}

// ── wide compound-PK roundtrips ────────────────────────────────────────

/// `(U64, U128)` compound PK + one I64 payload column. pk_stride == 24.
/// Through `from_parts`, so the fixture also witnesses that the client
/// admits this schema.
fn wide24_schema() -> Schema {
    Schema::from_parts(
        vec![
            ColumnDef::new("pk0", TypeCode::U64, false),
            ColumnDef::new("pk1", TypeCode::U128, false),
            ColumnDef::new("v", TypeCode::I64, true),
        ],
        vec![0, 1],
    )
    .expect("arity-2 compound PK of non-nullable integer columns")
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
    let mut pks = PkColumn::empty_for_schema(&schema);
    for t in &tuples {
        pks.push_bytes(&schema, t);
    }
    let pk_region = pks.region().to_vec();

    let weights = vec![1i64, -1, 3, 1];
    // Row 2 has a NULL payload (payload bit 0).
    let nulls = vec![0u64, 0, 1, 0];
    let payload: Vec<i64> = vec![10, 20, 30, 40];
    let mut payload_bytes = Vec::with_capacity(n * 8);
    for &v in &payload {
        payload_bytes.extend_from_slice(&v.to_le_bytes());
    }

    let batch = ZSetBatch {
        pks,
        weights: weights.clone(),
        nulls: nulls.clone(),
        payload: payload_of(&schema, vec![payload_bytes.clone()]),
        blob: vec![],
    };

    let encoded = encode_wal_block(77, &batch);

    // PK region (region 0) must be count * 24 bytes.
    let (_, pk_sz) = get_region_offset_size(&encoded, 0);
    assert_eq!(pk_sz, n * 24, "wide PK region must be 24B/row");

    let (decoded, tid) = decode_wal_block(&encoded, &schema).unwrap();
    assert_eq!(tid, 77);
    assert_eq!(decoded.weights, weights);
    assert_eq!(decoded.nulls, nulls);
    assert_eq!(decoded.pks.stride(), 24);
    assert_eq!(decoded.pks.region(), pk_region, "wide PK bytes must roundtrip exactly");
    assert_eq!(decoded.pks.len(), n);
    {
        let got = &decoded.payload[0].bytes;
        assert_eq!(got, &payload_bytes);
    }
}

#[test]
fn pk_stride_wal_roundtrip_bytes_64() {
    // Maximum user pk_stride: four U128 PK columns = 64 bytes/row. Through
    // `from_parts`, so the fixture proves the client admits that schema
    // rather than only asserting the stride it produces.
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("a", TypeCode::U128, false),
            ColumnDef::new("b", TypeCode::U128, false),
            ColumnDef::new("c", TypeCode::U128, false),
            ColumnDef::new("d", TypeCode::U128, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        vec![0, 1, 2, 3],
    )
    .expect("PK_LIST_MAX_COLS U128 columns is the widest admissible user PK");
    assert_eq!(schema.pk_stride(), 64);

    let rows: [[u128; 4]; 3] = [
        [0, 1, 2, 3],
        [u128::MAX, 1u128 << 64, 42, (1u128 << 64) + 7],
        [9, 8, 7, 6],
    ];
    let n = rows.len();
    let mut pks = PkColumn::empty_for_schema(&schema);
    for r in &rows {
        let mut native = Vec::with_capacity(64);
        for &cval in r {
            native.extend_from_slice(&cval.to_le_bytes());
        }
        pks.push_bytes(&schema, &native);
    }
    let pk_region = pks.region().to_vec();
    let payload: Vec<i64> = vec![-1, 0, 123];
    let mut payload_bytes = Vec::with_capacity(n * 8);
    for &v in &payload {
        payload_bytes.extend_from_slice(&v.to_le_bytes());
    }

    let batch = ZSetBatch {
        pks,
        weights: vec![1; n],
        nulls: vec![0; n],
        payload: payload_of(&schema, vec![payload_bytes.clone()]),
        blob: vec![],
    };

    let encoded = encode_wal_block(3, &batch);
    let (_, pk_sz) = get_region_offset_size(&encoded, 0);
    assert_eq!(pk_sz, n * 64, "wide PK region must be 64B/row");

    let (decoded, tid) = decode_wal_block(&encoded, &schema).unwrap();
    assert_eq!(tid, 3);
    assert_eq!(decoded.pks.stride(), 64);
    assert_eq!(decoded.pks.region(), pk_region);
    {
        let got = &decoded.payload[0].bytes;
        assert_eq!(got, &payload_bytes);
    }
}

#[test]
fn wide_pk_extend_from_concatenates() {
    let schema = wide24_schema();
    let mk = |a: u64, b: u128| {
        let mut pks = PkColumn::empty_for_schema(&schema);
        pks.push_bytes(&schema, &pk24(a, b));
        ZSetBatch {
            pks,
            weights: vec![1],
            nulls: vec![0],
            payload: payload_of(&schema, vec![5i64.to_le_bytes().to_vec()]),
            blob: vec![],
        }
    };
    let mut a = mk(1, 11);
    let b = mk(2, 22);
    a.extend_from_owned(b);

    assert_eq!(a.pks.len(), 2);
    let mut expected = PkColumn::empty_for_schema(&schema);
    expected.push_bytes(&schema, &pk24(1, 11));
    expected.push_bytes(&schema, &pk24(2, 22));
    assert_eq!(a.pks.stride(), 24);
    assert_eq!(a.pks.region(), expected.region());
    assert_eq!(a.weights, vec![1, 1]);
    {
        let got = &a.payload[0].bytes;
        assert_eq!(got.len(), 16);
    }
}

/// A null bit under a NOT NULL column is refused at decode, naming the column.
#[test]
fn a_null_bit_under_a_not_null_column_is_a_decode_error() {
    let schema = u64_schema();
    let mut b = ZSetBatch::new(&schema);
    b.pks = PkColumn::from_natives(&schema, [1u128, 2]);
    b.weights = vec![1, 1];
    b.nulls = vec![0, 1];
    b.payload[0].bytes = [7i64, 8].iter().flat_map(|v| v.to_le_bytes()).collect();
    let block = encode_wal_block(3, &b);
    match decode_wal_block(&block, &schema) {
        Err(ProtocolError::DecodeError(m)) => assert!(m.contains("NOT NULL column 'v'"), "{m}"),
        other => panic!("expected a NOT NULL decode error, got {other:?}"),
    }
}

/// Decoding a batch's own region list is decoding the block framed from it.
#[test]
fn decoding_regions_equals_decoding_the_framed_block() {
    let schema = str_schema();
    let vals = [
        Some("a string long enough to spill past the inline prefix"),
        None,
        Some("short"),
        Some("another long string that spills into the heap"),
    ];
    let mut b = ZSetBatch::new(&schema);
    b.pks = PkColumn::from_natives(&schema, 1u128..=4);
    b.weights = vec![1, 2, 1, 3];
    b.nulls = vals.iter().map(|v| v.is_none() as u64).collect();
    let mut blob = Vec::new();
    b.payload[0].bytes = german_col(&vals, &mut blob);
    b.blob = blob;

    let regions = crate::protocol::regions::regions(&b);
    let mut local = ZSetBatch::new(&schema);
    decode_regions_into(&mut local, &regions, b.len(), &schema).unwrap();
    let (remote, _) = decode_wal_block(&encode_wal_block(9, &b), &schema).unwrap();
    assert_eq!(local, remote);
}
