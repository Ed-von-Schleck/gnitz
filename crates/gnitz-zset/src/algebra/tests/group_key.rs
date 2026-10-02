use super::*;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{ColumnTable, SchemaColumn, TypeCode};
use crate::test_support::{
    arb_fold_case, cell, fold_batch, fold_schemas, le_cell, opk_pk, pk_only_schema, pk_payload_schema,
    wide_pk_3xu64_schema,
};
use proptest::prelude::*;

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
        let mut b = BatchBuilder::new(&schema);
        match col1_is_pk {
            true => b.begin_row_opk(&[0, le_cell(le)], 1),
            false => {
                b.begin_row(0, 1);
                b.put_int(le_cell(le));
            }
        }
        b.end_row();
        let b = b.finish();
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
            let want_pk = opk_pk(&pk_only_schema(&[tc]), &[v as u128]);
            let want = (gnitz_wire::widen_pk_be(&want_pk), want_pk);
            assert_eq!(key(tc, le, true), want, "PK-column key for {tc} v={v}");
            assert_eq!(key(tc, le, false), want, "payload-column key for {tc} v={v}");
        }
    }
}

// ---------------------------------------------------------------------------
// Group runs and ordinals
// ---------------------------------------------------------------------------

/// Sorting numbers the groups in ascending key order, and a group's first row is
/// its first in the batch: the whole `(key, index)` pair is the sort key, so the
/// index breaks every tie.
#[test]
fn sorted_ordinals_number_groups_by_key_and_keep_the_first_row() {
    // Three keys, descending, four rows each, interleaved.
    let groups = GroupOrdinals::sorted(12, |i| 2 - i % 3);
    assert_eq!(groups.ord, [2, 1, 0, 2, 1, 0, 2, 1, 0, 2, 1, 0]);
    assert_eq!(groups.first, [2, 1, 0]);
    assert_eq!(groups.by_pk, [0, 1, 2]);
    // Every row one key: one group.
    let groups = GroupOrdinals::sorted(5, |_| 0u64);
    assert_eq!((groups.ord, groups.first, groups.by_pk), (vec![0; 5], vec![0], vec![0]));
}

/// Under every key form — the empty set, the whole PK, each single column
/// (the leading PK column among them) and all of them — over `raw` and its
/// consolidation: `ordinals` gives two rows one ordinal iff they share an
/// `out_pk`, names each group's first row, and lists the groups in strictly
/// ascending `out_pk` order; two rows share an `out_pk` iff they share their
/// group columns' values, and iff they share an identity. `runs`, where it
/// answers, cuts the rows at exactly the ordinals' boundaries, and it answers
/// for a consolidated batch grouped by a PK prefix.
fn assert_groups_follow_out_pk(raw: &Batch) -> Result<(), TestCaseError> {
    let schema = *raw.schema();
    let consolidated = Batch::clone(raw).into_consolidated();
    let pk: Vec<u32> = schema.pk_cols().to_vec();
    let all: Vec<u32> = (0..schema.num_columns() as u32).collect();
    let mut forms = vec![vec![], pk.clone(), all];
    forms.extend((0..schema.num_columns() as u32).map(|c| vec![c]));
    for cols in &forms {
        let (key, _) = GroupOutKey::new(&schema, cols, []).unwrap();
        for batch in [raw, &consolidated] {
            let mb = batch.as_mem_batch();
            let out_pk = |row: usize| key.out_pk(&mb, row).bytes().to_vec();
            let groups = key.ordinals(batch);
            prop_assert_eq!(groups.ord.len(), batch.count, "{:?}", cols);
            for (row, &g) in groups.ord.iter().enumerate() {
                let first = groups.first[g as usize] as usize;
                prop_assert!(first <= row, "{:?}: row {} precedes its group's first", cols, row);
                prop_assert_eq!(out_pk(first), out_pk(row), "{:?}: a group mixes keys", cols);
            }
            let mut seen = groups.by_pk.clone();
            seen.sort_unstable();
            prop_assert_eq!(seen, (0..groups.len() as u32).collect::<Vec<_>>(), "{:?}", cols);
            let pks: Vec<Vec<u8>> = groups
                .by_pk
                .iter()
                .map(|&g| out_pk(groups.first[g as usize] as usize))
                .collect();
            prop_assert!(pks.windows(2).all(|w| w[0] < w[1]), "{:?}: groups out of order", cols);

            let runs = key.runs(batch);
            if let Some(runs) = &runs {
                prop_assert_eq!(runs.len(), groups.len(), "{:?}", cols);
                for (g, run) in runs.iter().enumerate() {
                    prop_assert!(
                        run.clone().all(|row| groups.ord[row] == g as u32),
                        "{:?}: run {} is not group {}",
                        cols,
                        g,
                        g
                    );
                }
            }
            if std::ptr::eq(batch, &consolidated) && (cols[..] == pk[..1] || *cols == pk) {
                prop_assert!(runs.is_some(), "{:?}: a PK prefix of a consolidated batch", cols);
            }

            let group =
                |r: usize| -> Vec<_> { cols.iter().map(|&c| cell(&mb, schema.locate(c as usize), r)).collect() };
            for a in 0..batch.count {
                for b in a + 1..batch.count {
                    let same_out_pk = out_pk(a) == out_pk(b);
                    prop_assert_eq!(group(a) == group(b), same_out_pk, "{:?}: rows {} and {}", cols, a, b);
                    prop_assert_eq!(
                        groups.ord[a] == groups.ord[b],
                        same_out_pk,
                        "{:?}: rows {} and {}",
                        cols,
                        a,
                        b
                    );
                    prop_assert_eq!(
                        key.identity(&mb, a) == key.identity(&mb, b),
                        same_out_pk,
                        "{:?}: rows {} and {}",
                        cols,
                        a,
                        b
                    );
                }
            }
        }
    }
    Ok(())
}

proptest! {
    /// [`assert_groups_follow_out_pk`] over every PK width arm.
    #[test]
    fn groups_follow_out_pk_at_every_pk_width((si, rows) in arb_fold_case()) {
        assert_groups_follow_out_pk(&fold_batch(&fold_schemas()[si], &rows))?;
    }
}

/// [`assert_groups_follow_out_pk`] over a compound signed PK and signed, wide
/// and nullable payload columns, each holding repeated values: 64 rows, so both
/// the hashed and the sorted numbering are taken, by group count.
#[test]
fn groups_follow_out_pk_over_signed_wide_and_nullable_columns() {
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
    let mut bb = BatchBuilder::new(&schema);
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
    assert_groups_follow_out_pk(&bb.finish()).unwrap();
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

/// Regression guard — time `ordinals` over a shuffled ~1M-row batch of distinct
/// keys, which every arm sorts: a whole PK per keyed width, a narrow image
/// (`u64`), a wide image (`u128`), and the multi-column fold. `#[ignore]`; run
/// release:
///   cargo test -p gnitz-zset --release reduce_sort -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn reduce_sort_argsort_bench() {
    let n = 1_000_000usize;
    let (u64pk, u128pk, wide) = (
        pk_payload_schema(&[TypeCode::U64]),
        pk_payload_schema(&[TypeCode::U128]),
        wide_pk_3xu64_schema(),
    );
    let mixed = pk_payload_schema(&[TypeCode::U64, TypeCode::U128]);
    // A non-leading PK column is an image; a partial PK is a fold.
    for (label, schema, group_cols) in [
        ("pk stride 8", &u64pk, &[0u32][..]),
        ("pk stride 16", &u128pk, &[0]),
        ("pk stride 24", &wide, &[0, 1, 2]),
        ("image u64", &wide, &[1]),
        ("image u128", &mixed, &[1]),
        ("fold 2-col", &wide, &[0, 1]),
    ] {
        let mut batch = crate::repr::BatchBuilder::new(schema);
        for pk in bench_rows(n, schema.pk_stride()) {
            batch.begin_row_bytes(&pk, 1);
            (0..schema.num_payload_cols()).for_each(|_| batch.put_int(0));
            batch.end_row();
        }
        let batch = batch.finish();
        let key = GroupOutKey::new(schema, group_cols, []).unwrap().0;

        let t = std::time::Instant::now();
        let groups = key.ordinals(&batch);
        let dt = t.elapsed();
        std::hint::black_box(&groups.ord);

        let mrps = n as f64 / dt.as_secs_f64() / 1e6;
        println!("argsort {label}: {n} rows in {dt:?} = {mrps:.1} M rows/s");
    }
}
