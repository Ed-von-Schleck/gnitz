use super::*;
use crate::schema::key::compare_pk_bytes;
use crate::schema::ColumnTable;
use crate::schema::{SchemaColumn, SchemaFacts, TypeCode};
use crate::storage::Batch;
use crate::test_support::{batch_of_pk_bytes, pk_payload_schema, wide_pk_3xu64_schema};

// ---------------------------------------------------------------------------
// Group key
// ---------------------------------------------------------------------------

/// A single natural column keys by its OPK image — at its own width as the
/// output PK — whether it is a PK or a payload column.
#[test]
fn single_natural_col_keys_by_its_opk_from_either_side() {
    // `(identity, out_pk)` of the value `le` held in column 1 of `[U64 pk, <tc>]`,
    // with column 1 either a second PK column or the sole payload column.
    let key = |tc: TypeCode, le: &[u8], col1_is_pk: bool| -> (u128, Vec<u8>) {
        let cols = [SchemaColumn::new(TypeCode::U64, false), SchemaColumn::new(tc, false)];
        let schema = SchemaDescriptor::new(&cols, if col1_is_pk { &[0, 1] } else { &[0] });
        let mut b = Batch::with_capacity(&schema, 1);
        if col1_is_pk {
            let mut native = [0u8; 16];
            native[..le.len()].copy_from_slice(le);
            b.extend_pk_opk(&[0, u128::from_le_bytes(native)]);
        } else {
            b.extend_pk(0u128);
        }
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        if !col1_is_pk {
            b.extend_col(schema.payload_slot(1).unwrap(), le);
        }
        b.count += 1;
        let (key, _) = GroupOutKey::new(&schema, &[1], []).unwrap();
        let mb = b.as_mem_batch();
        (key.identity(&mb, 0), key.out_pk(&mb, 0).bytes().to_vec())
    };

    for (tc, vals) in [
        (TypeCode::I32, vec![1i128, -1, 100, i32::MIN as i128, i32::MAX as i128]),
        (TypeCode::I64, vec![0, -1, i64::MIN as i128, i64::MAX as i128]),
        (TypeCode::U16, vec![0, 1, 0xBEEF, u16::MAX as i128]),
        (TypeCode::U64, vec![0, 1, u64::MAX as i128]),
    ] {
        let width = SchemaColumn::new(tc, false).size() as usize;
        for v in vals {
            let le = &(v as u128).to_le_bytes()[..width];
            // Signed columns are sign-flipped into the OPK image; unsigned ones
            // pass through, so the OPK image is the native value.
            let want = if tc.is_signed_int() {
                (v + (1i128 << (8 * width - 1))) as u128
            } else {
                v as u128
            };
            let want_pk = want.to_be_bytes()[16 - width..].to_vec();
            assert_eq!(
                key(tc, le, true),
                (want, want_pk.clone()),
                "PK-column key for {tc} v={v}"
            );
            assert_eq!(key(tc, le, false), (want, want_pk), "payload-column key for {tc} v={v}");
        }
    }
}

/// A float group column has no key image: its raw bits would split `+0.0` from
/// `-0.0` into two groups.
#[test]
fn group_key_refuses_a_float_column() {
    for tc in [TypeCode::F32, TypeCode::F64] {
        let schema = SchemaDescriptor::new(
            &[SchemaColumn::new(TypeCode::U64, false), SchemaColumn::new(tc, false)],
            &[0],
        );
        let err = GroupKey::new(&schema, &[1])
            .err()
            .expect("a float group column is refused");
        assert!(err.to_string().contains("float"), "{err}");
    }
    let schema = pk_payload_schema(&[TypeCode::U64]);
    assert!(
        GroupKey::new(&schema, &[2]).is_err(),
        "an out-of-range column is refused"
    );
}

// ---------------------------------------------------------------------------
// Group runs
// ---------------------------------------------------------------------------

/// Every position's row, in visit order.
fn visit_order(runs: &GroupRuns, n: usize) -> Vec<usize> {
    (0..n).map(|p| runs.row(p)).collect()
}

/// A whole-PK key's runs over an unsorted batch must reproduce the
/// authoritative `compare_pk_bytes` order. PKs are distinct, so the (unstable)
/// sort yields a unique order, one run per row.
fn assert_canonical_order(schema: &SchemaDescriptor, pk_rows: &[Vec<u8>]) {
    let mut want: Vec<usize> = (0..pk_rows.len()).collect();
    want.sort_by(|&a, &b| compare_pk_bytes(&pk_rows[a], &pk_rows[b]));
    let want_keys: Vec<&[u8]> = want.iter().map(|&i| pk_rows[i].as_slice()).collect();

    let batch = batch_of_pk_bytes(schema, pk_rows);
    let mb = batch.as_mem_batch();
    let key = GroupOutKey::new(schema, schema.pk_cols(), []).unwrap().0;
    let runs = key.runs(&batch);
    let got_keys: Vec<&[u8]> = visit_order(&runs, mb.count)
        .into_iter()
        .map(|i| mb.get_pk_bytes(i))
        .collect();
    assert_eq!(got_keys, want_keys, "PK-keyed run order mismatch");
    assert_eq!(runs.len(), pk_rows.len(), "distinct PKs are distinct groups");
}

#[test]
fn pk_runs_u64_arm_full_and_subwidth() {
    // stride 8 (u64 arm): high-bit / signed-OPK byte shapes must order by raw
    // unsigned byte compare (the OPK sign-flip lives in the bytes).
    assert_canonical_order(
        &pk_payload_schema(&[TypeCode::U64]),
        &[
            vec![0x80, 0, 0, 0, 0, 0, 0, 1],
            vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfd],
            vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            vec![0, 0, 0, 0, 0, 0, 0, 5],
            vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ],
    );
    // stride 4 (u64 arm, sub-width): left-align into the high bytes preserves order.
    assert_canonical_order(
        &pk_payload_schema(&[TypeCode::U32]),
        &[
            vec![0xff, 0, 0, 1],
            vec![0, 0, 0, 9],
            vec![0x80, 0, 0, 0],
            vec![0, 0, 0, 1],
        ],
    );
}

#[test]
fn pk_runs_u128_arm() {
    let mk = |hi: u8, lo: u8| {
        let mut v = vec![0u8; 16];
        v[0] = hi;
        v[15] = lo;
        v
    };
    assert_canonical_order(
        &pk_payload_schema(&[TypeCode::U128]),
        &[mk(0xff, 2), mk(0, 9), mk(0x80, 1), mk(0, 1), mk(0xff, 1)],
    );
}

#[test]
fn pk_runs_u128x2_arm_with_leading16_collision() {
    // stride 24 ([u128;2] arm). Rows sharing their leading 16 bytes and differing
    // only in the tail MUST be ordered by the second limb — a bare u128 prefix
    // would tie them and risk mis-merging distinct compound PKs (weight corruption).
    let shared = [0xab_u8; 16];
    let mk_tail = |tail: u64| {
        let mut v = Vec::with_capacity(24);
        v.extend_from_slice(&shared);
        v.extend_from_slice(&tail.to_be_bytes());
        v
    };
    let mk = |a: u64, b: u64, c: u64| {
        let mut v = Vec::with_capacity(24);
        v.extend_from_slice(&a.to_be_bytes());
        v.extend_from_slice(&b.to_be_bytes());
        v.extend_from_slice(&c.to_be_bytes());
        v
    };
    assert_canonical_order(
        &wide_pk_3xu64_schema(),
        &[
            mk_tail(7),
            mk(0, 0, 0),
            mk_tail(2),
            mk(0xffff_ffff_ffff_ffff, 0, 0),
            mk_tail(5),
        ],
    );
}

#[test]
fn pk_runs_wide_arm() {
    // stride 40 (> 32): the byte-slice fallback, whose `Ord` is `compare_pk_bytes`.
    let schema = pk_payload_schema(&[TypeCode::U64; 5]);
    let mk = |lead: u8, tail: u8| {
        let mut v = vec![0u8; 40];
        v[0] = lead;
        v[39] = tail;
        v
    };
    assert_canonical_order(&schema, &[mk(2, 0), mk(0, 9), mk(0xff, 1), mk(0, 1), mk(2, 3)]);
}

/// Rows sharing a key come back in ascending source-index order: the whole
/// `(key, index)` pair is the sort key, so the index breaks every tie. That
/// is what makes a float SUM over a group reproducible for a fixed access path.
/// A run ends exactly where the key changes.
#[test]
fn sorted_runs_break_ties_on_source_index() {
    // Three keys, four rows each, interleaved.
    let runs = GroupRuns::sorted(12, |i| i % 3);
    assert_eq!(visit_order(&runs, 12), vec![0, 3, 6, 9, 1, 4, 7, 10, 2, 5, 8, 11]);
    assert_eq!(runs.iter().collect::<Vec<_>>(), vec![0..4, 4..8, 8..12]);
    // Every row one key: the identity permutation, one run.
    let runs = GroupRuns::sorted(5, |_| 0u64);
    assert_eq!(visit_order(&runs, 5), vec![0, 1, 2, 3, 4]);
    assert_eq!(runs.iter().collect::<Vec<_>>(), vec![0..5]);
}

/// A consolidated batch grouped by its leading PK column is already in group
/// order: the runs visit positions in place and end where the column changes.
#[test]
fn leading_pk_column_runs_in_place_over_a_consolidated_batch() {
    let schema = pk_payload_schema(&[TypeCode::U64, TypeCode::U64]);
    let pk = |a: u64, b: u64| [a.to_be_bytes(), b.to_be_bytes()].concat();
    let raw = batch_of_pk_bytes(&schema, &[pk(1, 1), pk(1, 5), pk(2, 0), pk(7, 3), pk(7, 4), pk(7, 9)]);
    let mut consolidated = Batch::clone(&raw);
    consolidated.certify_layout(crate::storage::Layout::Consolidated);
    let key = GroupOutKey::new(&schema, &[0], []).unwrap().0;
    for batch in [&consolidated, &raw] {
        let runs = key.runs(batch);
        assert_eq!(visit_order(&runs, 6), vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(runs.iter().collect::<Vec<_>>(), vec![0..2, 2..3, 3..6]);
    }
}

/// Under every key form, `runs` visits groups in ascending `out_pk` order, and a
/// run ends exactly where `out_pk` changes.
#[test]
fn runs_ascend_by_out_pk_under_every_key_form() {
    // `[U32 pk0, I32 pk1, I64 v, U128 w, I64 n NULL]`, PK `(pk0, pk1)`.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0, 1],
    );
    let mut bb = crate::storage::BatchBuilder::new(schema);
    for i in 0..64u64 {
        let m = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40;
        bb.begin_row_opk(&[(m % 5) as u128, (m % 7) as i32 as i64 as u128], 1);
        bb.put_int(((m % 9) as i64 - 4) as u128);
        bb.put_int(u128::from(m % 3) << 100);
        match m % 4 {
            0 => bb.put_null(),
            k => bb.put_int((k as i64 - 2) as u128),
        }
        bb.end_row();
    }
    let batch = bb.finish();
    let mb = batch.as_mem_batch();
    // Leading PK column, non-leading PK column, whole PK, signed payload, wide
    // payload, nullable payload, several columns.
    for cols in [&[0u32][..], &[1], &[0, 1], &[2], &[3], &[4], &[2, 3]] {
        let (key, _) = GroupOutKey::new(&schema, cols, []).unwrap();
        let runs = key.runs(&batch);
        let pk = |pos: usize| key.out_pk(&mb, runs.row(pos)).bytes().to_vec();
        for run in runs.iter() {
            assert!(
                run.clone().all(|p| pk(p) == pk(run.start)),
                "{cols:?}: a run mixes groups"
            );
            if run.end < batch.count {
                assert!(pk(run.start) < pk(run.end), "{cols:?}: runs out of order");
            }
        }
    }
}

/// The empty group set is one run over the whole batch, in place.
#[test]
fn empty_group_set_is_one_run() {
    let schema = pk_payload_schema(&[TypeCode::U64]);
    let batch = batch_of_pk_bytes(&schema, &[3u64.to_be_bytes(), 1u64.to_be_bytes(), 2u64.to_be_bytes()]);
    let runs = GroupOutKey::new(&schema, &[], []).unwrap().0.runs(&batch);
    assert_eq!(visit_order(&runs, 3), vec![0, 1, 2]);
    assert_eq!(runs.iter().collect::<Vec<_>>(), vec![0..3]);
}

/// A shuffled ~1M-row batch of distinct keys, `stride` PK bytes each.
fn bench_rows(n: usize, stride: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let mut v = vec![0u8; stride];
            for chunk in 0..stride.div_ceil(8) {
                let seed = (i as u64)
                    .wrapping_add((chunk as u64).wrapping_mul(0x1000))
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15);
                let start = chunk * 8;
                let end = (start + 8).min(stride);
                v[start..end].copy_from_slice(&seed.to_be_bytes()[..end - start]);
            }
            v
        })
        .collect()
}

/// Regression guard — time `runs` under a whole-PK key over a shuffled
/// ~1M-row batch at each keyed arm. `#[ignore]`; run release:
///   cargo test -p gnitz-store --release reduce_sort -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn reduce_sort_argsort_bench() {
    let n = 1_000_000usize;
    for &stride in &[8usize, 16, 24] {
        let schema = match stride {
            8 => pk_payload_schema(&[TypeCode::U64]),
            16 => pk_payload_schema(&[TypeCode::U128]),
            _ => wide_pk_3xu64_schema(),
        };
        let batch = batch_of_pk_bytes(&schema, &bench_rows(n, stride));
        let key = GroupOutKey::new(&schema, schema.pk_cols(), []).unwrap().0;

        let t = std::time::Instant::now();
        let runs = key.runs(&batch);
        let dt = t.elapsed();
        std::hint::black_box(&runs);

        let mrps = n as f64 / dt.as_secs_f64() / 1e6;
        println!("argsort stride {stride}: {n} rows in {dt:?} = {mrps:.1} M rows/s");
    }
}

/// The group-key sibling: `runs` under a key that is not a PK prefix over a
/// shuffled ~1M-row batch at each key arm — a narrow image (`u64`), a wide image
/// (`u128`), and the multi-column fold. `#[ignore]`; run release, as above.
#[test]
#[ignore]
fn reduce_sort_argsort_delta_bench() {
    let n = 1_000_000usize;
    // (label, schema, group cols) — the three arms. A non-leading PK column is an
    // image; a partial PK is a fold.
    let narrow = wide_pk_3xu64_schema();
    let wide = pk_payload_schema(&[TypeCode::U64, TypeCode::U128]);
    for (label, schema, group_cols) in [
        ("image u64", &narrow, &[1u32][..]),
        ("image u128", &wide, &[1u32][..]),
        ("fold 2-col", &narrow, &[0u32, 1u32][..]),
    ] {
        let batch = batch_of_pk_bytes(schema, &bench_rows(n, schema.pk_stride()));
        let key = GroupOutKey::new(schema, group_cols, []).unwrap().0;

        let t = std::time::Instant::now();
        let runs = key.runs(&batch);
        let dt = t.elapsed();
        std::hint::black_box(&runs);

        let mrps = n as f64 / dt.as_secs_f64() / 1e6;
        println!("argsort_delta {label}: {n} rows in {dt:?} = {mrps:.1} M rows/s");
    }
}

/// A whole-PK key over a PK wider than 16 bytes has, as its `identity`, the
/// XXH3-128 of the PK bytes; its output PK is the PK itself.
#[test]
fn a_wide_whole_pk_identity_is_the_checksum_of_its_bytes() {
    let schema = wide_pk_3xu64_schema();
    let pk = [1u64.to_be_bytes(), 2u64.to_be_bytes(), 3u64.to_be_bytes()].concat();
    let batch = batch_of_pk_bytes(&schema, &[&pk]);
    let mb = batch.as_mem_batch();
    let (key, prefix) = GroupOutKey::new(&schema, schema.pk_cols(), []).unwrap();
    assert_eq!(prefix.finish().pk_stride(), 24);
    assert_eq!(key.identity(&mb, 0), gnitz_wire::checksum_128(&pk));
    assert_eq!(key.out_pk(&mb, 0).bytes(), &pk[..]);
}
