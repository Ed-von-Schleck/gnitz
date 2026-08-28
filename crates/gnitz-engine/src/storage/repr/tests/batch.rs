use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::{pk_payload_schema, wide_pk_3xu64_schema};

// With a non-leading compound PK the payload slots are renumbered around every
// PK position, so `payload_idx = ci - 1` does not hold. For pk_indices=[1, 2]
// over four columns, payload slot 0 maps to logical column 0 and slot 1 to
// logical column 3 — never to 0 and 1.
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
fn write_to_batch_narrow_pk_odd_rowcount_round_trips() {
    // The writer's carve must land on the same 8-aligned region offsets the
    // reader computes. At an odd row count a narrow `pk_stride` makes
    // `rows * pk_stride` non-8-aligned, which is where the two can disagree.
    // Build a source via the extend path (the reader-offset oracle), rebuild it
    // through `write_to_batch`, and assert the read-back is byte-exact.
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
/// NULL cell stays NULL. A spoof that instead *keeps* the flags set on this
/// corrupt data is rejected by the debug consumer-side verifier.
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

#[test]
fn extend_pk_round_trips_at_every_narrow_stride() {
    // (PK column types, expected stride, keys). Every key must fit its stride;
    // `extend_pk` debug-asserts that nothing is set above the window.
    let cases: &[(&[u8], usize, &[u128])] = &[
        (&[type_code::U8], 1, &[0, 1, 200, 255]),
        (&[type_code::U16], 2, &[0, 1, u16::MAX as u128]),
        (&[type_code::U32], 4, &[0, 1, u32::MAX as u128]),
        (&[type_code::U64], 8, &[0, 1, 1 << 32, u64::MAX as u128]),
        (
            &[type_code::U64, type_code::U32],
            12,
            &[1, 1 | ((u32::MAX as u128) << 64), 7 | (7 << 64)],
        ),
        (
            &[type_code::U128],
            16,
            &[0, 1, u64::MAX as u128, (u64::MAX as u128) + 1, u128::MAX],
        ),
    ];

    for &(tcs, stride, keys) in cases {
        let schema = pk_payload_schema(tcs);
        assert_eq!(schema.pk_stride() as usize, stride);
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(keys.len());
        for &pk in keys {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        assert_eq!(b.pk_data().len(), keys.len() * stride, "stride {stride}: region size");
        for (i, &pk) in keys.iter().enumerate() {
            let want = &pk.to_be_bytes()[16 - stride..];
            assert_eq!(b.get_pk_bytes(i), want, "stride {stride} row {i}: stored bytes");
            assert_eq!(b.get_pk(i), pk, "stride {stride} row {i}: value");
        }
    }
}

/// `extend_pk_bytes` stores the key verbatim at any stride, including the wide
/// ones `extend_pk` refuses.
#[test]
fn extend_pk_bytes_stores_the_key_verbatim() {
    for (tcs, stride) in [
        (&[type_code::U64][..], 8usize),
        (&[type_code::U128], 16),
        (&[type_code::U64, type_code::U64, type_code::U64], 24),
    ] {
        let schema = pk_payload_schema(tcs);
        assert_eq!(schema.pk_stride() as usize, stride);
        let keys: [Vec<u8>; 3] = [
            (0..stride as u8).map(|i| i.wrapping_mul(17).wrapping_add(1)).collect(),
            vec![0u8; stride],
            vec![0xffu8; stride],
        ];
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(keys.len());
        for pk in &keys {
            b.extend_pk_bytes(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        assert_eq!(b.pk_data().len(), keys.len() * stride, "stride {stride}: region size");
        for (i, pk) in keys.iter().enumerate() {
            assert_eq!(b.get_pk_bytes(i), &pk[..], "stride {stride} row {i}");
        }
    }
}

#[test]
#[cfg(debug_assertions)]
#[should_panic]
fn extend_pk_u64_batch_rejects_wide_pk() {
    let schema = crate::test_support::make_schema_u64_i64();
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(1);
    b.extend_pk((u64::MAX as u128) + 1);
}

#[test]
#[should_panic(expected = "extend_pk_bytes: length must equal pk_stride")]
fn extend_pk_bytes_length_mismatch_panics() {
    let schema = crate::test_support::make_schema_u64_i64();
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(1);
    b.extend_pk_bytes(&[0u8; 7]);
}

/// `Batch` feeds its own count, stride and PK base into the shared seek kernel
/// (swept exhaustively in `columnar`); both entry points must agree with a
/// linear scan over the same batch, at a narrow and a wide stride.
#[test]
fn pk_seeks_agree_with_a_linear_scan() {
    let narrow: Vec<Vec<u8>> = [10u64, 20, 30, 40, 50]
        .iter()
        .map(|v| v.to_be_bytes().to_vec())
        .collect();
    let narrow_probes: Vec<Vec<u8>> = [0u64, 5, 10, 15, 25, 50, 51, u64::MAX]
        .iter()
        .map(|v| v.to_be_bytes().to_vec())
        .collect();

    // A 3xU64 compound key ascending in compare_pk_bytes order (column 0
    // dominates, then 1, then 2); probes land on and between the stored keys.
    let wide = |a: u64, b: u64, c: u64| {
        crate::test_support::opk_pk(&wide_pk_3xu64_schema(), &[a as u128, b as u128, c as u128])
    };
    let wide_keys = vec![
        wide(0, 0, 0),
        wide(1, 0, 0),
        wide(1, 5, 0),
        wide(1, 5, 9),
        wide(2, 0, 0),
    ];
    let wide_probes = vec![
        wide(0, 0, 0),
        wide(0, 0, 1),
        wide(1, 0, 0),
        wide(1, 5, 0),
        wide(1, 5, 10),
        wide(3, 0, 0),
    ];

    for (schema, keys, probes) in [
        (pk_payload_schema(&[type_code::U64]), narrow, narrow_probes),
        (wide_pk_3xu64_schema(), wide_keys, wide_probes),
    ] {
        let rows: Vec<(&[u8], i64, i64)> = keys.iter().map(|k| (&k[..], 1i64, 0i64)).collect();
        let b = crate::test_support::make_batch_opk(&schema, &rows);

        for key in &probes {
            let want = (0..b.count)
                .find(|&i| crate::schema::key::compare_pk_bytes(b.get_pk_bytes(i), key) != std::cmp::Ordering::Less)
                .unwrap_or(b.count);
            assert_eq!(b.find_lower_bound_bytes(key), want, "probe={key:02x?}");
            // `advance_to` is the galloping form the merge drives; every hint,
            // a run-off past the end included, must land on the same index.
            for hint in 0..=b.count {
                assert_eq!(b.advance_to(key, hint), want, "probe={key:02x?} hint={hint}");
            }
        }
    }
}

/// `append_row_from_source_bytes` copies the PK verbatim and carries the
/// caller's weight, not the source row's.
#[test]
fn append_row_from_source_bytes_copies_pk_weight_and_payload() {
    let schema = pk_payload_schema(&[type_code::U64]);
    let src = crate::test_support::make_batch_opk(&schema, &[(&0xDEAD_BEEFu64.to_be_bytes(), 1, 0x4242)]);

    let mut dst = Batch::with_capacity(schema, 1);
    dst.append_row_from_source_bytes(src.get_pk_bytes(0), -1, &src, 0, None);

    assert_eq!(dst.count, 1);
    assert_eq!(dst.get_pk_bytes(0), &0xDEAD_BEEFu64.to_be_bytes());
    assert_eq!(dst.get_weight(0), -1);
    let payload = dst.get_col_ptr(0, 0, 8);
    assert_eq!(i64::from_le_bytes(payload.try_into().unwrap()), 0x4242);
}

// ── Consumer skip-point flag verifiers (debug_verify_sorted /
//    debug_verify_consolidated) ─────────────────────────────────────────
//
// Build a single-col-U64-PK / I64-payload batch from (pk, weight, payload)
// triples and stamp a (possibly lying) layout directly via the test-only
// `set_layout_unchecked`. This hands a *lying* tag to a consumer skip-point so
// the debug verifier can be caught tripping.
fn flagged_batch(rows: &[(u128, i64, i64)], layout: Layout) -> (Batch, SchemaDescriptor) {
    let schema = crate::test_support::make_schema_u64_i64();
    let mut b = crate::test_support::make_batch_raw(
        &schema,
        &rows.iter().map(|&(pk, w, v)| (pk as u64, w, v)).collect::<Vec<_>>(),
    );
    b.set_layout_unchecked(layout);
    (b, schema)
}

// The `already_sorted` fold path trusts the Sorted tag: descending PKs under
// a Sorted stamp must trip the verifier rather than fold in the wrong order.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "flagged sorted")]
fn into_consolidated_panics_on_lying_sorted() {
    let (b, schema) = flagged_batch(&[(2, 1, 0), (1, 1, 0)], Layout::Sorted);
    let _ = b.into_consolidated(&schema);
}

// The consolidated short-circuit trusts the Consolidated tag: an adjacent-equal
// (PK, payload) duplicate under that stamp must trip the verifier.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "not strictly")]
fn into_consolidated_panics_on_lying_consolidated_dup() {
    let (b, schema) = flagged_batch(&[(1, 1, 5), (1, 1, 5)], Layout::Consolidated);
    let _ = b.into_consolidated(&schema);
}

// Ghost clause: strictly ordered, but carrying a net-zero row under Consolidated.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "ghost not eliminated")]
fn into_consolidated_panics_on_consolidated_ghost() {
    let (b, schema) = flagged_batch(&[(1, 1, 0), (2, 0, 0), (3, 1, 0)], Layout::Consolidated);
    let _ = b.into_consolidated(&schema);
}

// An honest sorted+consolidated batch passes both entry points with no panic.
#[test]
fn honest_sorted_consolidated_batch_passes_verifiers() {
    let (b, schema) = flagged_batch(&[(1, 1, 0), (2, 1, 0), (3, 1, 0)], Layout::Consolidated);
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
    assert_eq!(b.count, 0, "clear drops the rows");
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

/// `append_row_simple` over two nullable STRING columns: inline cells, cells
/// that spill to the blob heap, the empty string, and a NULL whose cell must be
/// zeroed rather than left holding the caller's pointer.
#[test]
fn append_row_simple_writes_string_cells_and_nulls() {
    let schema = make_schema_cols(
        &[(type_code::U64, 0), (type_code::STRING, 1), (type_code::STRING, 1)],
        0,
    );
    let mut batch = Batch::with_capacity(schema, 4);

    // (col 0, col 1). Long values (> 12 bytes) land in the blob heap, short
    // ones stay inline in the 16-byte struct.
    // `None` is a NULL cell.
    type Row<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);
    let cases: &[Row] = &[
        (Some(b"Alice"), Some(b"short")),
        (Some(b"Bob has a long name!"), Some(b"Also quite a long description")),
        (Some(b""), Some(b"nonempty")),
        (None, Some(b"another long one for blob storage")),
    ];

    for (pk, &(a, b)) in cases.iter().enumerate() {
        let null_word = u64::from(a.is_none()) | (u64::from(b.is_none()) << 1);
        let ptrs = [
            a.map_or(std::ptr::null(), |v| v.as_ptr()),
            b.map_or(std::ptr::null(), |v| v.as_ptr()),
        ];
        let lens = [a.map_or(0, |v| v.len() as u32), b.map_or(0, |v| v.len() as u32)];
        unsafe {
            batch.append_row_simple(pk as u128, 1, null_word, &[0i64, 0], &[0u64, 0], &ptrs, &lens);
        }
    }

    assert_eq!(batch.count, cases.len());
    for (i, &(a, b)) in cases.iter().enumerate() {
        for (col, want) in [(0usize, a), (1, b)] {
            assert_eq!(
                batch.get_null_word(i) >> col & 1,
                u64::from(want.is_none()),
                "row {i} col {col} null bit"
            );
            match want {
                Some(v) => assert_eq!(
                    crate::test_support::read_german_string(&batch, col, i),
                    v,
                    "row {i} col {col}"
                ),
                None => assert!(
                    batch.get_col_ptr(i, col, 16).iter().all(|&b| b == 0),
                    "row {i} col {col}: a null cell must be zeroed"
                ),
            }
        }
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
            (type_code::I128, 0),   // pi 11
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
    // A negative 16-byte payload (a cross-sign `_join_pk` surfaced into a
    // payload slot), with bits in both halves.
    let i128_val: i128 = -0x0123_4567_89AB_CDEF_1122_3344_5566_7788;
    lo[11] = (i128_val as u128 as u64) as i64;
    hi[11] = ((i128_val as u128) >> 64) as u64;

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
    let got = i128::from_le_bytes(batch.get_col_ptr(0, 11, 16).try_into().unwrap());
    assert_eq!(got, i128_val, "16-byte payload round-trips through the lo/hi split");
}

/// Dropping a batch returns its data buffer to the thread-local pool, and a
/// clone owns its own buffers — dropping both must not double-free.
#[test]
fn drop_recycles_buffers() {
    use crate::storage::batch_pool::{acquire_buf, recycle_buf};
    while acquire_buf().capacity() > 0 {}

    let schema = crate::test_support::make_schema_u64_i64();
    let batch = crate::test_support::make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
    let data_cap = batch.data_capacity();
    assert!(data_cap > 0);
    let cloned = batch.clone();
    drop(batch);
    drop(cloned);

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

    // One appended nullable I64 column — the shape a LEFT JOIN null-fill widens
    // to: the input schema verbatim, then the fill column.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
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

    let batch = Batch::empty_with_schema(&SchemaDescriptor::minimal_u64());
    assert_eq!(batch.data_capacity(), 0);
    drop(batch);

    assert_eq!(acquire_buf().capacity(), 0, "empty batch should not pollute pool");
}
