use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::wide_pk_3xu64_schema;

// Compound-PK regression guard for the precomputed payload→logical
// mapping. 4-column schema with pk_indices=[1, 2]: payload slot 0
// must map to logical column 0, slot 1 to logical column 3 — never
// 0 and 1 (the bug the previous single-PK reimplementation would
// have introduced for any non-leading compound PK).
#[test]
fn batch_builder_physical_col_idx_compound_pk() {
    let cols = [
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::U64, 0),
    ];
    let schema = SchemaDescriptor::new(&cols, &[1, 2]);
    let mut bb = BatchBuilder::new(schema);
    assert_eq!(bb.curr_col, 0);
    assert_eq!(bb.physical_col_idx(), 0);
    bb.curr_col = 1;
    assert_eq!(bb.physical_col_idx(), 3);
}

#[test]
fn widen_pk_be_recovers_unsigned_value() {
    // OPK bytes of an unsigned PK are its big-endian image; widen_pk_be
    // right-aligns them and recovers the native value. A left-align bug
    // would return value·2^k — assert it does not.
    let v: u64 = 0x1122_3344_5566_7788;
    assert_eq!(gnitz_wire::widen_pk_be(&v.to_be_bytes(), 8), v as u128);

    let key = 0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10u128;
    assert_eq!(gnitz_wire::widen_pk_be(&key.to_be_bytes(), 16), key);

    for &v in &[1u64, 5, 42] {
        assert_eq!(gnitz_wire::widen_pk_be(&v.to_be_bytes(), 8), v as u128);
    }
    for &v in &[0u32, 0xFFFF_FFFE, 0xFFFF_FFFF] {
        assert_eq!(gnitz_wire::widen_pk_be(&v.to_be_bytes(), 4), v as u128);
    }
    // Narrow non-power-of-two stride: right-aligned recovery.
    let mut opk12 = [0u8; 12];
    opk12[11] = 0x2A; // value 42 in the low byte of a 12-byte OPK region
    assert_eq!(gnitz_wire::widen_pk_be(&opk12, 12), 42u128);
}

#[test]
fn widen_pk_be_extend_pk_round_trips() {
    // extend_pk writes right-aligned BE; get_pk_bytes is the stored OPK and
    // get_pk recovers the value. extend_pk(widen_pk_be(bytes)) == bytes.
    let schema = crate::test_support::pk_i64_schema(type_code::U64);
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(1);
    b.extend_pk(42);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &0i64.to_le_bytes());
    b.count += 1;
    assert_eq!(b.get_pk_bytes(0), &42u64.to_be_bytes());
    assert_eq!(b.get_pk(0), 42u128);
}

#[test]
fn extend_pk_narrow_strides_round_trip() {
    // For each narrow stride, extend_pk writes the low `stride` bytes of
    // the u128 argument verbatim; get_pk reads them back via widen_pk_be.
    // Unsigned types only: for them OPK *is* right-aligned big-endian, which
    // is what `extend_pk` writes — a signed PK column carries the sign flip
    // and must go through `extend_pk_opk` (`extend_pk` debug-asserts it).
    // Every narrow stride is still covered.
    let cases: &[(u8, u128)] = &[
        // 1-byte stride
        (type_code::U8, 200u128),
        // 2-byte stride
        (type_code::U16, u16::MAX as u128),
        // 4-byte stride
        (type_code::U32, u32::MAX as u128),
        // 8-byte stride
        (type_code::U64, u64::MAX as u128),
    ];
    for &(tc, pk) in cases {
        let schema = crate::test_support::pk_i64_schema(tc);
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(1);
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
        assert_eq!(b.get_pk(0), pk, "type_code {tc} narrow-stride round-trip");
    }
}

#[test]
fn write_to_batch_narrow_pk_odd_rowcount_round_trips() {
    // Regression: `carve_writer_slices` (the writer) must carve at the same
    // 8-byte-aligned region offsets `compute_offsets` (the reader) produces.
    // At an odd row count a narrow `pk_stride` makes `rows * pk_stride`
    // non-8-aligned, so a back-to-back writer carve placed every post-PK
    // region a few bytes earlier than the reader expected — corrupting
    // weight/payload and dropping rows on a plain INSERT + scan. Build a
    // correct source via the extend path (the reader-offset oracle), rebuild
    // it through `write_to_batch` → `carve_writer_slices`, and assert the
    // read-back is byte-exact for every narrow stride at 3 rows.
    let rows: [(u128, i64, i64); 3] = [(1, 1, 30), (2, 1, 10), (3, 1, 20)];
    // U8/U16/U32 = strides 1/2/4 (the buggy non-8-aligned cases at 3 rows);
    // U64 = stride 8 (always aligned) as a control.
    for tc in [type_code::U8, type_code::U16, type_code::U32, type_code::U64] {
        let schema = crate::test_support::pk_i64_schema(tc);
        let stride = schema.pk_stride() as usize;

        let mut src = Batch::empty_with_schema(&schema);
        src.reserve_rows(rows.len());
        for &(pk, w, v) in &rows {
            src.extend_pk(pk);
            src.extend_weight(&w.to_le_bytes());
            src.extend_null_bmp(&0u64.to_le_bytes());
            src.extend_col(0, &v.to_le_bytes());
            src.count += 1;
        }
        let src_mb = src.as_mem_batch();

        let out = write_to_batch(&schema, rows.len(), 0, |w| {
            for i in 0..rows.len() {
                w.write_row(&src_mb, i, src_mb.get_weight(i));
            }
        });

        assert_eq!(out.count, rows.len(), "tc={tc} stride={stride}: row count");
        for (i, &(pk, w, v)) in rows.iter().enumerate() {
            assert_eq!(
                out.get_pk_bytes(i),
                &pk.to_be_bytes()[16 - stride..],
                "tc={tc} stride={stride}: pk row {i}"
            );
            assert_eq!(out.get_weight(i), w, "tc={tc} stride={stride}: weight row {i}");
            let col = out.get_col_ptr(i, 0, 8);
            assert_eq!(
                i64::from_le_bytes(col.try_into().unwrap()),
                v,
                "tc={tc} stride={stride}: payload row {i}"
            );
        }
    }
}

#[test]
fn extend_pk_roundtrip_across_u64_boundary() {
    let schema = crate::test_support::pk_i64_schema(type_code::U128);
    let mut b = Batch::with_capacity(schema, 8);
    let keys: [u128; 5] = [0, 1, u64::MAX as u128, (u64::MAX as u128) + 1, u128::MAX];
    for &pk in &keys {
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    for (i, &pk) in keys.iter().enumerate() {
        assert_eq!(b.get_pk(i), pk, "row {i} pk roundtrip");
    }
    let iter_pks: Vec<u128> = b.pk_iter().collect();
    assert_eq!(iter_pks, keys);
}

fn minimal_u64_with_i64_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

// U64 PK + a *nullable* I64 payload, so a row can carry a set null bit over
// non-zero bytes — the null-canonicalization case the trust strip protects.
fn pk_u64_nullable_i64_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

fn append_test_row(b: &mut Batch, pk: u128, w: i64, val: i64, null_word: u64) {
    b.extend_pk(pk);
    b.extend_weight(&w.to_le_bytes());
    b.extend_null_bmp(&null_word.to_le_bytes());
    b.extend_col(0, &val.to_le_bytes());
    b.count += 1;
}

/// A deliberately corrupt, unsorted batch: a duplicate `(PK, payload)` pair
/// and a `+1`/`-1` ghost pair at non-adjacent positions, plus a NULL payload
/// cell whose underlying bytes are non-zero. Used to exercise both the strip
/// (real consolidation) and the debug verifier that rejects a spoofed-flag
/// version of it.
fn build_corrupt_batch(schema: &SchemaDescriptor) -> Batch {
    let mut b = Batch::with_capacity(*schema, 5);
    append_test_row(&mut b, 5, 1, 100, 0); // dup A
    append_test_row(&mut b, 1, 1, 7, 0); // ghost +
    append_test_row(&mut b, 9, 1, -1, 1); // NULL cell over non-zero (0xFF..) bytes
    append_test_row(&mut b, 5, 1, 100, 0); // dup B (non-adjacent to A)
    append_test_row(&mut b, 1, -1, 7, 0); // ghost - (non-adjacent to +)
    b
}

/// The trust-boundary strip's mechanism, exercised directly on
/// `into_consolidated` (no server). Clearing the flags — what `handle_message`
/// does to every client batch — forces a real sort+fold: the duplicate sums to
/// `+2`, the ghost is dropped, the output is `(PK, payload)`-sorted, and the
/// NULL cell stays NULL. (A spoof that instead *keeps* both flags set on this
/// corrupt data is rejected by the debug consumer-side verifier — see
/// `into_consolidated_spoofed_corrupt_flags_panic_in_debug`.)
#[test]
fn into_consolidated_strip_forces_real_consolidation() {
    let schema = pk_u64_nullable_i64_schema();
    // `build_corrupt_batch` yields a `Raw` batch (the constructor default), so
    // `into_consolidated` runs a real sort+fold — the strip's mechanism.
    let clean = build_corrupt_batch(&schema);
    let clean = clean.into_consolidated(&schema);

    // Ghost eliminated and duplicate folded → 2 surviving rows, (PK,payload)-sorted.
    assert_eq!(clean.count, 2, "ghost dropped, duplicate folded");
    assert_eq!(clean.get_pk(0), 5);
    assert_eq!(clean.get_pk(1), 9);
    // PK 5: the non-adjacent duplicate summed to +2; value intact; not null.
    assert_eq!(clean.get_weight(0), 2, "duplicate (PK,payload) folds to +2");
    assert_eq!(i64::from_le_bytes(clean.get_col_ptr(0, 0, 8).try_into().unwrap()), 100);
    assert_eq!(clean.get_null_word(0) & 1, 0);
    // PK 9: the NULL cell still decodes as NULL (null bit preserved).
    assert_eq!(clean.get_weight(1), 1);
    assert_eq!(clean.get_null_word(1) & 1, 1, "null cell stays NULL");
}

/// The spoof the strip defends against: both flags set on a batch that is
/// neither sorted nor folded. `into_consolidated`'s consolidated short-circuit
/// now verifies the data in debug and panics instead of trusting it verbatim
/// (which would leave the duplicate unmerged and the ghost alive — silent
/// wrong weights).
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "flagged consolidated")]
fn into_consolidated_spoofed_corrupt_flags_panic_in_debug() {
    let schema = pk_u64_nullable_i64_schema();
    let mut corrupt = build_corrupt_batch(&schema);
    corrupt.set_layout_unchecked(Layout::Consolidated);
    let _ = corrupt.into_consolidated(&schema);
}

#[test]
fn pk_stride_u64_roundtrip() {
    let schema = minimal_u64_with_i64_schema();
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(4);
    for &pk in &[0u64, 1, 1 << 32, u64::MAX] {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    assert_eq!(b.strides[REG_PK], 8);
    assert_eq!(b.pk_data().len(), 4 * 8);
    for (i, &pk) in [0u64, 1, 1 << 32, u64::MAX].iter().enumerate() {
        assert_eq!(b.get_pk(i), pk as u128);
    }
}

#[test]
fn pk_stride_u128_roundtrip() {
    let schema = crate::test_support::pk_i64_schema(type_code::U128);
    let pks: &[u128] = &[0, 1, u64::MAX as u128, (u64::MAX as u128) + 1, u128::MAX];
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(pks.len());
    for &pk in pks {
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    assert_eq!(b.strides[REG_PK], 16);
    assert_eq!(b.pk_data().len(), pks.len() * 16);
    for (i, &pk) in pks.iter().enumerate() {
        assert_eq!(b.get_pk(i), pk);
    }
}

#[test]
#[cfg(debug_assertions)]
#[should_panic]
fn extend_pk_u64_batch_rejects_wide_pk() {
    let schema = minimal_u64_with_i64_schema();
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(1);
    b.extend_pk((u64::MAX as u128) + 1);
}

#[test]
fn extend_pk_bytes_then_get_pk_bytes_roundtrip_u64() {
    let schema = minimal_u64_with_i64_schema();
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(4);
    let pks: [[u8; 8]; 4] = [
        [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
        [0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88],
        [0, 0, 0, 0, 0, 0, 0, 0],
        [0xff; 8],
    ];
    for pk in &pks {
        b.extend_pk_bytes(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    for (i, pk) in pks.iter().enumerate() {
        assert_eq!(b.get_pk_bytes(i), pk, "row {i} bytes roundtrip");
        assert_eq!(b.get_pk(i), u64::from_be_bytes(*pk) as u128, "row {i} u128");
    }
}

#[test]
fn extend_pk_bytes_then_get_pk_bytes_roundtrip_u128() {
    let schema = crate::test_support::pk_i64_schema(type_code::U128);
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(3);
    let pks: [[u8; 16]; 3] = [
        [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        ],
        [0; 16],
        [0xff; 16],
    ];
    for pk in &pks {
        b.extend_pk_bytes(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    for (i, pk) in pks.iter().enumerate() {
        assert_eq!(b.get_pk_bytes(i), pk, "row {i} bytes roundtrip");
        assert_eq!(b.get_pk(i), u128::from_be_bytes(*pk), "row {i} u128");
    }
}

#[test]
#[should_panic(expected = "extend_pk_bytes: length must equal pk_stride")]
fn extend_pk_bytes_length_mismatch_panics() {
    let schema = minimal_u64_with_i64_schema();
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(1);
    b.extend_pk_bytes(&[0u8; 7]);
}

#[test]
fn find_lower_bound_bytes_narrow_opk() {
    // Narrow single-PK: find_lower_bound_bytes is now a raw memcmp search
    // over OPK bytes. For an unsigned U64 PK the OPK is big-endian, so the
    // probe key is `to_be_bytes()`. Validate against a linear reference.
    let schema = minimal_u64_with_i64_schema();
    let mut b = Batch::empty_with_schema(&schema);
    let pks: [u64; 5] = [10, 20, 30, 40, 50];
    b.reserve_rows(pks.len());
    for &pk in &pks {
        b.extend_pk(pk as u128); // stored as OPK (BE) for unsigned
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    for probe in [0u64, 5, 10, 15, 20, 25, 30, 40, 50, 51, u64::MAX] {
        let key = probe.to_be_bytes();
        let expected = (0..b.count).find(|&i| b.get_pk_bytes(i) >= &key[..]).unwrap_or(b.count);
        assert_eq!(b.find_lower_bound_bytes(&key), expected, "probe={probe}");
    }
}

#[test]
fn find_lower_bound_bytes_compound_pk_matches_compare_pk_bytes() {
    // Compound PK (3xU64, stride 24): exercises the column-walk path.
    // The result of find_lower_bound_bytes must equal the first index
    // where compare_pk_bytes(row, key) is not Less, for every probe.
    let schema = wide_pk_3xu64_schema();
    assert_eq!(schema.pk_stride(), 24);
    let mut b = Batch::empty_with_schema(&schema);
    // Grouped by 8-byte u64 column boundaries (3xU64 compound PK).
    // Sorted in compare_pk_bytes order (lexicographic over the
    // u64 columns: column 0 high priority, then 1, then 2).
    #[rustfmt::skip]
    let pks: [[u8; 24]; 5] = [
        [0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
        [1,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
        [1,0,0,0,0,0,0,0,  5,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
        [1,0,0,0,0,0,0,0,  5,0,0,0,0,0,0,0,  9,0,0,0,0,0,0,0],
        [2,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
    ];
    b.reserve_rows(pks.len());
    for pk in &pks {
        b.extend_pk_bytes(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    #[rustfmt::skip]
    let probes: &[[u8; 24]] = &[
        [0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
        [0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  1,0,0,0,0,0,0,0],
        [1,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
        [1,0,0,0,0,0,0,0,  5,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
        [1,0,0,0,0,0,0,0,  5,0,0,0,0,0,0,0,  9,0,0,0,0,0,0,0],
        [1,0,0,0,0,0,0,0,  5,0,0,0,0,0,0,0,  10,0,0,0,0,0,0,0],
        [3,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
    ];
    for key in probes {
        // Expected: first row where compare_pk_bytes(row, key) is not Less.
        let expected = (0..b.count)
            .find(|&i| crate::schema::key::compare_pk_bytes(b.get_pk_bytes(i), key) != std::cmp::Ordering::Less)
            .unwrap_or(b.count);
        let got = b.find_lower_bound_bytes(key);
        assert_eq!(got, expected, "probe={key:?}");
    }
}

#[test]
fn found_row_append_narrow_byte_roundtrip() {
    // Narrow single-PK round-trip through append_row_from_source_bytes fed by
    // a found-row ColumnarSource view. The byte-typed PK path must preserve
    // byte-for-byte equivalence with the old extend_pk(pk) path: the stored
    // PK region bytes are the same LE bytes extend_pk would have written.
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("appendrow_byte_test");
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let mut pt = crate::storage::Table::new(
        tdir.to_str().unwrap(),
        schema,
        100,
        crate::storage::RecoverySource::Rederive { resume_at: None },
    )
    .unwrap();

    // Ingest one row so retract_pk can set a found-row.
    let mut src = Batch::with_capacity(schema, 1);
    let pk_val: u64 = 0xDEAD_BEEFu64;
    src.extend_pk(pk_val as u128);
    src.extend_weight(&1i64.to_le_bytes());
    src.extend_null_bmp(&0u64.to_le_bytes());
    src.extend_col(0, &0x4242i64.to_le_bytes());
    src.count += 1;
    pt.ingest_owned_batch(src).unwrap();

    // retract_pk returns the stored row as an owned ColumnarSource that
    // append_row_from_source_bytes copies in.
    let (_w, found) = pt.retract_pk(pk_val as u128);
    let found_row = found.expect("retract_pk located the stored row");

    let mut dst = Batch::with_capacity(schema, 1);
    // PK region is OPK (big-endian) at rest; the lookup key must match.
    let pk_bytes = pk_val.to_be_bytes();
    dst.append_row_from_source_bytes(&pk_bytes, -1, &found_row.run, found_row.row, None);
    assert_eq!(dst.count, 1);
    assert_eq!(dst.get_pk_bytes(0), &pk_bytes);
    assert_eq!(dst.get_pk(0), pk_val as u128);
    assert_eq!(dst.get_weight(0), -1);
    // Payload (column 0, I64) preserved from ptable.
    let payload = dst.get_col_ptr(0, 0, 8);
    assert_eq!(i64::from_le_bytes(payload.try_into().unwrap()), 0x4242i64);
}

#[test]
fn extend_pk_bytes_roundtrip_compound_pk() {
    // 3-column compound PK (U64 + U64 + U64) — pk_stride = 24, exercising
    // the wide-stride byte API. extend_pk panics for this stride, so the
    // test must use extend_pk_bytes / get_pk_bytes throughout.
    let schema = wide_pk_3xu64_schema();
    assert_eq!(schema.pk_stride(), 24);
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(3);
    let pks: [[u8; 24]; 3] = [
        [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x21, 0x22,
            0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
        ],
        [0u8; 24],
        [0xffu8; 24],
    ];
    for pk in &pks {
        b.extend_pk_bytes(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    assert_eq!(b.pk_data().len(), pks.len() * 24);
    for (i, pk) in pks.iter().enumerate() {
        assert_eq!(b.get_pk_bytes(i), pk, "row {i} bytes roundtrip");
    }
}

/// Narrow non-power-of-two stride 12 ((U64, U32)) must round-trip through
/// the u128-keyed extend_pk path (low 12 bytes verbatim), not panic.
#[test]
fn extend_pk_stride12_roundtrip() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );
    assert_eq!(schema.pk_stride(), 12);
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(3);
    // (U64, U32) packed little-endian into the low 12 bytes of a u128. The
    // `| (0 << 64)` keeps the (low, high) structure visible across all three.
    #[allow(clippy::identity_op)]
    let pks: [u128; 3] = [
        1u128 | (0u128 << 64),
        1u128 | ((u32::MAX as u128) << 64),
        7u128 | (7u128 << 64),
    ];
    for &pk in &pks {
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    for (i, &pk) in pks.iter().enumerate() {
        assert_eq!(b.get_pk(i), pk, "stride-12 row {i} roundtrip");
    }
}

/// extend_pk on a stride-12 batch must reject a u128 with bits set above the
/// 12-byte (96-bit) window in debug builds (silent truncation guard).
#[test]
#[should_panic(expected = "does not fit 12 bytes")]
fn extend_pk_stride12_high_bits_panics() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(1);
    // Bit 100 is set — above the 96-bit stride window.
    b.extend_pk(1u128 << 100);
}

// ── Consumer skip-point flag verifiers (debug_verify_sorted /
//    debug_verify_consolidated) ─────────────────────────────────────────
//
// Build a single-col-U64-PK / I64-payload batch from (pk, weight, payload)
// triples and stamp a (possibly lying) layout directly via the test-only
// `set_layout_unchecked`. This hands a *lying* tag to a consumer skip-point so
// the debug verifier can be caught tripping.
fn flagged_batch(rows: &[(u128, i64, i64)], sorted: bool, consolidated: bool) -> (Batch, SchemaDescriptor) {
    let schema = crate::test_support::pk_i64_schema(type_code::U64);
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(rows.len());
    for &(pk, w, v) in rows {
        append_test_row(&mut b, pk, w, v, 0);
    }
    let layout = if consolidated {
        Layout::Consolidated
    } else if sorted {
        Layout::Sorted
    } else {
        Layout::Raw
    };
    b.set_layout_unchecked(layout);
    (b, schema)
}

// B: into_consolidated's `already_sorted` fold path trusts `sorted`.
// Descending PKs (2,1) but sorted=true, consolidated=false → B, not A.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "flagged sorted")]
fn into_consolidated_panics_on_lying_sorted() {
    let (b, schema) = flagged_batch(&[(2, 1, 0), (1, 1, 0)], true, false);
    let _ = b.into_consolidated(&schema);
}

// C: consolidate_if_needed's re-sort skip trusts `sorted`.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "flagged sorted")]
fn consolidate_if_needed_panics_on_lying_sorted() {
    let (b, schema) = flagged_batch(&[(2, 1, 0), (1, 1, 0)], true, false);
    let _ = Batch::consolidate_if_needed(&b, &schema);
}

// A: into_consolidated's consolidated short-circuit trusts `consolidated`.
// Adjacent-equal (PK=1, payload=5) duplicate but consolidated=true.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "not strictly")]
fn into_consolidated_panics_on_lying_consolidated_dup() {
    let (b, schema) = flagged_batch(&[(1, 1, 5), (1, 1, 5)], true, true);
    let _ = b.into_consolidated(&schema);
}

// D: consolidate_if_needed's consolidated short-circuit trusts `consolidated`.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "not strictly")]
fn consolidate_if_needed_panics_on_lying_consolidated_dup() {
    let (b, schema) = flagged_batch(&[(1, 1, 5), (1, 1, 5)], true, true);
    let _ = Batch::consolidate_if_needed(&b, &schema);
}

// Ghost clause: strictly ordered, but a net-zero row under consolidated=true.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "ghost not eliminated")]
fn into_consolidated_panics_on_consolidated_ghost() {
    let (b, schema) = flagged_batch(&[(1, 1, 0), (2, 0, 0), (3, 1, 0)], true, true);
    let _ = b.into_consolidated(&schema);
}

// A genuinely sorted+consolidated batch passes A, C, and D with no panic.
#[test]
fn honest_sorted_consolidated_batch_passes_verifiers() {
    let (b, schema) = flagged_batch(&[(1, 1, 0), (2, 1, 0), (3, 1, 0)], true, true);
    assert!(
        Batch::consolidate_if_needed(&b, &schema).is_none(),
        "C+D: honest sorted/consolidated batch borrows original"
    );
    let cb = b.into_consolidated(&schema);
    assert_eq!(cb.count, 3, "A: honest consolidated batch passes through");
}

// Layout lifecycle: constructors default `Raw`; `extend_*` never raises;
// `certify_layout` raises; any append downgrades to `Raw`; `clear()`
// resets to `Raw`.
#[test]
fn layout_lifecycle_default_raise_and_lower() {
    let schema = crate::test_support::pk_i64_schema(type_code::U64);
    let mut b = Batch::with_capacity(schema, 4);
    assert_eq!(b.layout(), Layout::Raw, "constructor defaults Raw");
    append_test_row(&mut b, 1, 1, 10, 0);
    append_test_row(&mut b, 2, 1, 20, 0);
    assert_eq!(b.layout(), Layout::Raw, "extend_* never raises the layout");

    // Genuinely (PK, payload)-sorted, ghost-free → certify Consolidated.
    b.certify_layout(Layout::Consolidated, &schema);
    assert!(b.is_sorted() && b.is_consolidated());

    // Any append downgrades all the way to Raw (the W2M-class fail-safe).
    let mut src = Batch::with_capacity(schema, 1);
    append_test_row(&mut src, 3, 1, 30, 0);
    b.append_batch(&src, 0, 1);
    assert_eq!(b.layout(), Layout::Raw, "append downgrades to Raw");

    b.clear();
    assert_eq!(b.layout(), Layout::Raw, "clear resets to Raw");
}

// An empty batch reads sorted + consolidated regardless of its (Raw) tag — the
// `count == 0` special-case inside the accessors, so the constructor flip to
// `Raw` needs no per-reader audit.
#[test]
fn empty_batch_reads_sorted_and_consolidated() {
    let schema = crate::test_support::pk_i64_schema(type_code::U64);
    let b = Batch::with_capacity(schema, 4);
    assert_eq!(b.count, 0);
    assert_eq!(b.layout(), Layout::Raw);
    assert!(b.is_sorted(), "empty batch is structurally sorted");
    assert!(b.is_consolidated(), "empty batch is structurally consolidated");
}

// ── `Batch` and batch-pool behaviour (no LSM tier involved) ──────────

fn make_schema_cols(cols: &[(u8, u8)], pk_index: u32) -> SchemaDescriptor {
    let mut columns = [SchemaColumn::EMPTY; crate::schema::MAX_COLUMNS];
    for (i, &(tc, nullable)) in cols.iter().enumerate() {
        columns[i] = SchemaColumn::new(tc, nullable);
    }
    SchemaDescriptor::new(&columns[..cols.len()], &[pk_index])
}

#[test]
fn owned_batch_roundtrip() {
    let schema = crate::test_support::make_schema_u64_i64();
    let batch = crate::test_support::make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
    assert_eq!(batch.count, 2);
    assert_eq!(batch.get_pk(0), 10);
    assert_eq!(batch.get_pk(1), 20);

    let mb = batch.as_mem_batch();
    assert_eq!(mb.count, 2);
    assert_eq!(gnitz_wire::widen_pk_be(mb.get_pk_bytes(0), mb.pk_stride as usize), 10);
    assert_eq!(mb.get_weight(1), 1);
}

#[test]
fn batch_append_batch() {
    let schema = crate::test_support::make_schema_u64_i64();
    let src = crate::test_support::make_batch(&schema, &[(10, 1, 100), (20, 1, 200), (30, 1, 300)]);
    let mut dst = Batch::with_capacity(schema, 8);

    dst.append_batch(&src, 0, 3);
    assert_eq!(dst.count, 3);
    assert_eq!(dst.get_pk(0), 10);
    assert_eq!(dst.get_pk(2), 30);
    assert_eq!(dst.get_weight(1), 1);

    dst.clear();
    dst.append_batch(&src, 1, 2);
    assert_eq!(dst.count, 1);
    assert_eq!(dst.get_pk(0), 20);
}

/// Regression: bulk append into an `empty_with_schema()` batch must not spin
/// when n far exceeds its initial (zero) capacity.
#[test]
fn batch_append_batch_from_empty_exceeds_initial_capacity() {
    let schema = crate::test_support::make_schema_u64_i64();
    let rows: Vec<(u64, i64, i64)> = (1u64..=200).map(|i| (i, 1i64, (i * 10) as i64)).collect();
    let src = crate::test_support::make_batch(&schema, &rows);

    let mut dst = Batch::empty_with_schema(&schema);
    dst.append_batch(&src, 0, src.count);

    assert_eq!(dst.count, 200);
    for i in 0..200usize {
        assert_eq!(dst.get_pk(i), (i + 1) as u128);
    }
}

#[test]
fn batch_region_access() {
    let schema = crate::test_support::make_schema_u64_i64();
    let batch = crate::test_support::make_batch(&schema, &[(10, 1, 100)]);

    // 2 columns: PK (U64) + payload (I64) — regions pk(0), weight(1),
    // null(2), col0(3), blob(4).
    assert_eq!(batch.num_regions_total(), 5);
    assert_eq!(batch.region_slice(0).len(), 8);
    assert_eq!(batch.region_slice(3).len(), 8);
    assert!(batch.region_slice(4).is_empty(), "no strings ⇒ empty heap");
}

#[test]
fn batch_clear() {
    let schema = crate::test_support::make_schema_u64_i64();
    let mut batch = crate::test_support::make_batch(&schema, &[(10, 1, 100)]);
    assert_eq!(batch.count, 1);

    batch.clear();
    assert_eq!(batch.count, 0);
    assert!(batch.is_sorted());
    assert!(batch.is_consolidated());
}

#[test]
fn test_append_row_simple_nullable_string() {
    let schema = make_schema_cols(&[(type_code::U64, 0), (type_code::STRING, 1)], 0);
    let mut batch = Batch::with_capacity(schema, 4);

    let s = b"not null";
    let lo = [0i64]; // not used for STRING
    let hi = [0u64];
    let ptrs = [s.as_ptr()];
    let lens = [s.len() as u32];
    unsafe {
        batch.append_row_simple(1, 1, 0, &lo, &hi, &ptrs, &lens);
    }

    // Row 1: null string (null_word bit 0 set).
    let null_ptr: *const u8 = std::ptr::null();
    unsafe {
        batch.append_row_simple(2, 1, 1, &[0i64], &[0u64], &[null_ptr], &[0u32]);
    }

    assert_eq!(batch.count, 2);
    assert_eq!(crate::test_support::read_german_string(&batch, 0, 0), b"not null");
    assert_eq!(batch.get_null_word(0) & 1, 0);
    assert_eq!(batch.get_null_word(1) & 1, 1);
    let raw = batch.get_col_ptr(1, 0, 16);
    assert!(raw.iter().all(|&b| b == 0), "null cell is zeroed");
}

#[test]
fn test_append_row_simple_multi_string() {
    let schema = make_schema_cols(
        &[(type_code::U64, 0), (type_code::STRING, 0), (type_code::STRING, 0)],
        0,
    );
    let mut batch = Batch::with_capacity(schema, 4);

    let cases: &[(&[u8], &[u8])] = &[
        (b"Alice", b"short"),
        (b"Bob has a long name!", b"Also quite a long description"),
        (b"", b"nonempty"),
        (b"mix", b"another long one for blob storage"),
    ];

    for (pk, (name, desc)) in cases.iter().enumerate() {
        let ptrs = [name.as_ptr(), desc.as_ptr()];
        let lens = [name.len() as u32, desc.len() as u32];
        unsafe {
            batch.append_row_simple(pk as u128, 1, 0, &[0i64, 0], &[0u64, 0], &ptrs, &lens);
        }
    }

    assert_eq!(batch.count, 4);
    for (i, (name, desc)) in cases.iter().enumerate() {
        assert_eq!(
            crate::test_support::read_german_string(&batch, 0, i),
            *name,
            "row {i} name mismatch"
        );
        assert_eq!(
            crate::test_support::read_german_string(&batch, 1, i),
            *desc,
            "row {i} desc mismatch"
        );
    }
}

// 3.14 / 2.718 are test-fixture values exercising f32/f64 round-trip, not
// approximations of PI/E.
#[test]
#[allow(clippy::approx_constant)]
fn test_append_row_simple_all_types() {
    // U64 pk, then one of each remaining type at payload index 0..=11.
    let schema = make_schema_cols(
        &[
            (type_code::U64, 0),    // pk
            (type_code::U8, 0),     // pi 0
            (type_code::I8, 0),     // pi 1
            (type_code::U16, 0),    // pi 2
            (type_code::I16, 0),    // pi 3
            (type_code::U32, 0),    // pi 4
            (type_code::I32, 0),    // pi 5
            (type_code::F32, 0),    // pi 6
            (type_code::U64, 0),    // pi 7
            (type_code::I64, 0),    // pi 8
            (type_code::F64, 0),    // pi 9
            (type_code::STRING, 0), // pi 10
            (type_code::U128, 0),   // pi 11
        ],
        0,
    );
    let mut batch = Batch::with_capacity(schema, 1);

    let n = 12;
    let mut lo = vec![0i64; n];
    let mut hi = vec![0u64; n];
    let mut ptrs = vec![std::ptr::null::<u8>(); n];
    let mut lens = vec![0u32; n];

    lo[0] = 42;
    lo[1] = -7;
    lo[2] = 1000;
    lo[3] = -500;
    lo[4] = 70000;
    lo[5] = -12345;
    // Floats travel as f64 bit patterns (the float2longlong convention).
    lo[6] = f64::to_bits(3.14f64) as i64;
    lo[7] = 0x1234_5678_9ABC_DEF0u64 as i64;
    lo[8] = -99999;
    lo[9] = f64::to_bits(2.718281828f64) as i64;
    let s = b"hello world!";
    ptrs[10] = s.as_ptr();
    lens[10] = s.len() as u32;
    lo[11] = 0xDEADBEEFu64 as i64;
    hi[11] = 0xCAFEBABE;

    unsafe {
        batch.append_row_simple(100, 1, 0, &lo, &hi, &ptrs, &lens);
    }

    assert_eq!(batch.count, 1);
    assert_eq!(batch.get_col_ptr(0, 0, 1), &[42]);
    assert_eq!(batch.get_col_ptr(0, 1, 1), &[(-7i8) as u8]);
    assert_eq!(batch.get_col_ptr(0, 2, 2), &1000u16.to_le_bytes());
    assert_eq!(batch.get_col_ptr(0, 3, 2), &(-500i16).to_le_bytes());
    assert_eq!(batch.get_col_ptr(0, 4, 4), &70000u32.to_le_bytes());
    assert_eq!(batch.get_col_ptr(0, 5, 4), &(-12345i32).to_le_bytes());
    let f32_val = f32::from_le_bytes(batch.get_col_ptr(0, 6, 4).try_into().unwrap());
    assert!((f32_val - 3.14f32).abs() < 1e-5, "f32: {f32_val}");
    assert_eq!(batch.get_col_ptr(0, 7, 8), &0x1234_5678_9ABC_DEF0u64.to_le_bytes());
    assert_eq!(batch.get_col_ptr(0, 8, 8), &(-99999i64).to_le_bytes());
    let f64_val = f64::from_le_bytes(batch.get_col_ptr(0, 9, 8).try_into().unwrap());
    assert!((f64_val - 2.718281828).abs() < 1e-9, "f64: {f64_val}");
    assert_eq!(crate::test_support::read_german_string(&batch, 10, 0), b"hello world!");
    let u128_bytes = batch.get_col_ptr(0, 11, 16);
    assert_eq!(u64::from_le_bytes(u128_bytes[0..8].try_into().unwrap()), 0xDEADBEEF);
    assert_eq!(u64::from_le_bytes(u128_bytes[8..16].try_into().unwrap()), 0xCAFEBABE);
}

/// An I128 payload column (a cross-sign `_join_pk` surfaced into a payload
/// slot) round-trips through the lo/hi split in `append_row_simple`.
#[test]
fn test_append_row_simple_i128_payload() {
    let schema = make_schema_cols(&[(type_code::U64, 0), (type_code::I128, 0)], 0);
    let mut batch = Batch::with_capacity(schema, 1);
    // A negative value with bits in both halves (bit 127 set). The I128 is
    // the only payload column, so lo/hi are indexed at pi = 0.
    let v: i128 = -0x0123_4567_89AB_CDEF_1122_3344_5566_7788;
    let bits = v as u128;
    unsafe {
        batch.append_row_simple(
            7,
            1,
            0,
            &[(bits as u64) as i64],
            &[(bits >> 64) as u64],
            &[std::ptr::null::<u8>()],
            &[0u32],
        );
    }
    assert_eq!(batch.count, 1);
    let got = i128::from_le_bytes(batch.get_col_ptr(0, 0, 16).try_into().unwrap());
    assert_eq!(got, v, "I128 payload must round-trip through the lo/hi split");
}

/// `append_row` substitutes an empty string when the declared length would
/// read past the end of `blob_src`. Matches the string relocator, and keeps
/// malformed wire data from emitting unrelated bytes from the blob start.
#[test]
fn test_append_row_blob_length_header() {
    let schema = make_schema_cols(&[(type_code::U64, 0), (type_code::STRING, 0)], 0);
    let mut batch = Batch::with_capacity(schema, 1);

    // German String with length=20 but only 5 blob bytes available.
    let mut gs_struct = [0u8; 16];
    gs_struct[0..4].copy_from_slice(&20u32.to_le_bytes()); // declared length
    gs_struct[4..8].copy_from_slice(&0u32.to_le_bytes()); // prefix bytes
    gs_struct[8..16].copy_from_slice(&0u64.to_le_bytes()); // blob offset

    unsafe {
        batch.append_row(1, 1, 0, &[gs_struct.as_ptr()], &[16u32], b"hello");
    }

    // Empty-string fallback: not the declared 20, not a truncated 5.
    let stored_len = u32::from_le_bytes(batch.get_col_ptr(0, 0, 16)[0..4].try_into().unwrap());
    assert_eq!(stored_len, 0, "malformed long-string should fall back to empty");
    assert!(batch.blob.is_empty(), "no bytes copied into the blob arena");
}

/// `append_row_from_source` must not panic on an out-of-range blob offset.
#[test]
fn test_append_row_from_source_corrupted_blob() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    );

    let mut src = Batch::with_capacity(schema, 1);
    // German string: length=20, prefix="ABCD", offset=9999 (out of bounds).
    let mut str_struct = [0u8; 16];
    str_struct[0..4].copy_from_slice(&20u32.to_le_bytes());
    str_struct[4..8].copy_from_slice(b"ABCD");
    str_struct[8..16].copy_from_slice(&9999u64.to_le_bytes());

    src.ensure_row_capacity();
    src.extend_pk(42u128);
    src.extend_weight(&1i64.to_le_bytes());
    src.extend_null_bmp(&0u64.to_le_bytes());
    src.extend_col(0, &str_struct);
    src.blob = vec![0u8; 10];
    src.count = 1;

    let mut dst = Batch::with_capacity(schema, 1);
    let mut blob_cache = crate::storage::BlobCache::default();
    dst.append_row_from_source(42u128, 1, &src, 0, Some(&mut blob_cache));

    assert_eq!(dst.count, 1);
    let out_len = u32::from_le_bytes(dst.col_data(0)[0..4].try_into().unwrap());
    assert_eq!(out_len, 0, "corrupted blob reference should produce zero-length string");
}

#[test]
fn drop_recycles_buffers() {
    use crate::storage::batch_pool::{acquire_buf, recycle_buf};
    while acquire_buf().capacity() > 0 {}

    let schema = crate::test_support::make_schema_u64_i64();
    let batch = crate::test_support::make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
    let data_cap = batch.data_capacity();
    assert!(data_cap > 0);
    drop(batch);

    let mut found = false;
    let mut drained = Vec::new();
    loop {
        let buf = acquire_buf();
        if buf.capacity() == 0 {
            break;
        }
        found |= buf.capacity() >= data_cap;
        drained.push(buf);
    }
    assert!(found, "pool should contain the recycled data buffer");
    for buf in drained {
        recycle_buf(buf);
    }
}

#[test]
fn clone_drops_independently() {
    use crate::storage::batch_pool::acquire_buf;
    while acquire_buf().capacity() > 0 {}

    let schema = crate::test_support::make_schema_u64_i64();
    let batch = crate::test_support::make_batch(&schema, &[(1, 1, 10)]);
    let cloned = batch.clone();
    drop(batch);
    drop(cloned);

    // Two batches, each with a data + blob buffer; some may be zero-cap.
    let mut count = 0;
    while acquire_buf().capacity() > 0 {
        count += 1;
    }
    assert!(count >= 2, "expected at least 2 recycled buffers, got {count}");
}

// ── Gather / widen: blob arms, layout propagation, empty shape ──────────

/// A long (> 12 byte) value must resolve in the gathered output under BOTH blob
/// arms — sharing carries the source heap verbatim, relocating rewrites each
/// surviving cell into a fresh one.
#[test]
fn from_ranges_resolves_long_values_under_both_blob_arms() {
    use crate::test_support::{make_batch_bytes, make_schema_pk_u64_payload_blob, read_german_string};
    let schema = make_schema_pk_u64_payload_blob();

    // Sharing arm: a small heap, where a per-cell rewrite would cost more
    // than the whole-heap memcpy it replaces.
    let long: &[u8] = b"a-fairly-long-blob-value-xyz"; // 28 bytes > 12
    let small = make_batch_bytes(&schema, &[(1, 1, long), (2, 1, b"hi")]);
    assert!(
        !Batch::should_relocate_blob(1, small.count, small.blob.len()),
        "precondition: this shape takes the sharing arm",
    );
    let shared = Batch::from_ranges(&small, &[(0, 1)], &schema);
    assert_eq!(shared.count, 1);
    assert_eq!(read_german_string(&shared, 0, 0), long);
    assert_eq!(
        shared.blob.len(),
        small.blob.len(),
        "the sharing arm carries the heap whole"
    );

    // Relocating arm: a wide heap where one survivor out of a hundred would
    // otherwise carry every dropped row's span.
    let vals: Vec<Vec<u8>> = (0..100u64).map(|i| vec![b'a' + (i % 26) as u8; 1024]).collect();
    let rows: Vec<(u64, i64, &[u8])> = vals.iter().enumerate().map(|(i, v)| (i as u64, 1, &v[..])).collect();
    let wide = make_batch_bytes(&schema, &rows);
    assert!(
        Batch::should_relocate_blob(1, wide.count, wide.blob.len()),
        "precondition: this shape takes the relocating arm",
    );
    let relocated = Batch::from_ranges(&wide, &[(7, 8)], &schema);
    assert_eq!(relocated.count, 1);
    assert_eq!(read_german_string(&relocated, 0, 0), vals[7]);
    assert!(
        relocated.blob.len() < wide.blob.len(),
        "the relocating arm carries only the survivor's span",
    );
}

/// `inherit_layout` is a bare field store with no debug verification, so a
/// gather that dropped the tag is caught nowhere at the producer — only
/// downstream, at the next skip-point that trusts the claim.
#[test]
fn from_ranges_inherits_its_source_layout() {
    let schema = crate::test_support::make_schema_u64_i64();
    let src = crate::test_support::make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    assert!(src.is_consolidated(), "precondition: the source claims consolidated");

    let subset = Batch::from_ranges(&src, &[(0, 1), (2, 3)], &schema);
    assert_eq!(subset.count, 2);
    assert!(
        subset.is_consolidated(),
        "a disjoint ascending subset keeps order, weights and distinctness",
    );
    assert!(subset.is_sorted());
}

/// Regression: the widen must carry the input's blob heap, or a long
/// (> 12 byte) string in the output resolves against an empty heap and reads
/// back as garbage.
#[test]
fn widened_with_null_tail_carries_the_blob() {
    use crate::test_support::{make_batch_bytes, make_schema_pk_u64_payload_string, read_german_string};
    let in_schema = make_schema_pk_u64_payload_string();
    let long: &[u8] = b"a-fairly-long-string-value"; // 26 bytes > 12
    let b = make_batch_bytes(&in_schema, &[(1, 1, long)]);

    // One appended I64 column — the shape a LEFT JOIN null-fill widens to.
    let out_schema = crate::schema::null_extend_output_schema(&in_schema, &[type_code::I64])
        .expect("two columns is well inside MAX_COLUMNS");
    let out = b.widened_with_null_tail(&in_schema, &out_schema);

    assert_eq!(out.count, 1);
    assert!(!out.blob.is_empty(), "output blob must be propagated");
    assert_eq!(
        read_german_string(&out, 0, 0),
        long,
        "long string must resolve to the original"
    );
    assert!(
        gnitz_wire::null_word_get(out.get_null_word(0), 1),
        "the appended column reads NULL",
    );
}

/// Every operator's empty early-return goes through one of these two, so both
/// must carry the schema's PK stride rather than a fixed width: a placeholder
/// whose shape disagrees with its register's is one a later reader trusts.
#[test]
fn empty_constructors_carry_the_schema_pk_stride() {
    let narrow = crate::test_support::make_schema_u64_i64(); // U64 PK → stride 8
    let empty = Batch::empty_with_schema(&narrow);
    assert_eq!(empty.count, 0);
    assert_eq!(empty.pk_stride(), 8);
    assert_eq!(empty.empty_like().pk_stride(), 8);

    let wide = Batch::empty_with_schema(&wide_pk_3xu64_schema()); // 3×U64 → stride 24
    assert_eq!(wide.pk_stride(), 24);
    assert_eq!(wide.empty_like().pk_stride(), 24);
}

#[test]
fn empty_batch_drop_is_noop() {
    use crate::storage::batch_pool::acquire_buf;
    while acquire_buf().capacity() > 0 {}

    let batch = Batch::empty_with_schema(&SchemaDescriptor::default());
    assert_eq!(batch.data_capacity(), 0);
    drop(batch);

    assert_eq!(acquire_buf().capacity(), 0, "empty batch should not pollute pool");
}
