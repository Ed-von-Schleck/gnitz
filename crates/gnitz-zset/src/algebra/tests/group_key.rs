use super::*;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{ColumnTable, SchemaColumn, TypeCode};
use crate::test_support::{
    arb_fold_case, cell, fold_batch, fold_schemas, le_cell, opk_pk, pk_only_schema, pk_payload_schema,
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
            true => b.begin_row_natives(&[0, le_cell(le)], 1),
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

/// Under every key form — the empty set, every leading run of the PK list, each
/// single column, each pair of columns and all of them — over `raw` and its
/// consolidation: `ordinals` gives two rows one ordinal iff they share an
/// `out_pk`, names each group's first row, and lists the groups in strictly
/// ascending `out_pk` order; two rows share an `out_pk` iff they share their
/// group columns' values, and iff they share an identity. `runs`, where it
/// answers, cuts the rows at exactly the ordinals' boundaries, and it answers
/// for a consolidated batch grouped by leading PK columns whose bytes are its
/// output PK.
fn assert_groups_follow_out_pk(raw: &Batch) -> Result<(), TestCaseError> {
    let schema = *raw.schema();
    let consolidated = Batch::clone(raw).into_consolidated();
    let pk: Vec<u32> = schema.pk_cols().to_vec();
    let all: Vec<u32> = (0..schema.num_columns() as u32).collect();
    let n = schema.num_columns() as u32;
    let mut forms = vec![vec![], all];
    forms.extend((1..=pk.len()).map(|lead| pk[..lead].to_vec()));
    forms.extend((0..n).map(|c| vec![c]));
    forms.extend((0..n).flat_map(|a| (0..n).filter(move |&b| b != a).map(move |b| vec![a, b])));
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
            // A longer proper prefix is a fold, which no PK order groups.
            let bytes: usize = cols.iter().map(|&c| schema.columns[c as usize].size() as usize).sum();
            let in_place = *cols == pk || bytes <= GROUP_PK_BYTES;
            if std::ptr::eq(batch, &consolidated) && !cols.is_empty() && pk.starts_with(cols) && in_place {
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
        bb.begin_row_natives(&[(m % 5) as u128, (m % 7) as i32 as i64 as u128], 1);
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

/// Instructions per row of [`GroupOutKey::ordinals`] over 262 144 unconsolidated
/// rows, per key form: every key column drawn from 1024 values, where the rows
/// hash into groups, and from all of `u64`, where they are sorted.
///
/// `cd crates && cargo test -p gnitz-zset --release group_ordinals_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn group_ordinals_bench() {
    use TypeCode::{U128, U64};
    const N: u64 = 1 << 18;
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let three = pk_payload_schema(&[U64, U64, U64]);
    let nullable = {
        let mut cols = [SchemaColumn::new(U64, false); 4];
        cols[3] = SchemaColumn::new(TypeCode::I64, true);
        SchemaDescriptor::new(&cols, &[0, 1, 2])
    };
    let wide = pk_payload_schema(&[U128, U128]);
    let mixed = pk_payload_schema(&[U128, U64, U64]);
    for (label, schema, group_cols) in [
        ("leading PK column", &three, &[0u32][..]),
        ("two leading PK columns", &three, &[0, 1]),
        ("whole 24-byte PK", &three, &[0, 1, 2]),
        ("PK column past the first", &three, &[1]),
        ("payload column", &three, &[3]),
        ("two PK columns past the first", &three, &[1, 2]),
        ("PK column and payload column", &three, &[1, 3]),
        ("nullable payload column", &nullable, &[3]),
        ("leading U128 PK column", &wide, &[0]),
        ("second U128 PK column", &wide, &[1]),
        ("24-byte PK prefix (fold)", &mixed, &[0, 1]),
    ] {
        let key = GroupOutKey::new(schema, group_cols, []).unwrap().0;
        let last_pk = schema.pk_cols().len() - 1;
        for groups in [1024, u64::MAX] {
            let draw = |i: u64, c: u64| (i ^ c << 40).wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(29) % groups;
            let mut bb = BatchBuilder::new(schema);
            for i in 0..N {
                // The last PK column is the row number where no group reads it.
                let natives: Vec<u128> = (0..=last_pk)
                    .map(|c| match c == last_pk && !group_cols.contains(&(c as u32)) {
                        true => i as u128,
                        false => draw(i, c as u64) as u128,
                    })
                    .collect();
                bb.begin_row_natives(&natives, 1);
                bb.put_int(draw(i, 7) as u128);
                bb.end_row();
            }
            let batch = bb.finish();
            std::hint::black_box(key.ordinals(&batch));
            let (ordinals, instructions) = counter.measure(|| key.ordinals(&batch));
            std::hint::black_box(&ordinals.ord);
            println!(
                "group_ordinals_bench {label:<30} {:>20} values: {:7} groups, {:6.1} instr/row",
                groups,
                ordinals.len(),
                instructions as f64 / N as f64
            );
        }
    }
}

/// Instructions per row of grouping 1M rows in 1000 groups by hashing each row's
/// identity, per key form.
///
/// `cd crates && cargo test -p gnitz-zset --release group_identity_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn group_identity_bench() {
    use TypeCode::{I32, I64, U32, U64};
    const N: u64 = 1_000_000;
    const GROUPS: u64 = 1000;
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let schema = |pk: &[TypeCode], payload: TypeCode| {
        let mut cols: Vec<SchemaColumn> = pk.iter().map(|&t| SchemaColumn::new(t, false)).collect();
        cols.push(SchemaColumn::new(payload, false));
        SchemaDescriptor::new(&cols, &(0..pk.len() as u32).collect::<Vec<_>>())
    };
    // The group value sits in the grouped column; a row counter keeps the PK distinct.
    for (label, pk, payload, group_cols) in [
        ("payload I64 image", &[U64][..], I64, &[1u32][..]),
        ("payload I32 image", &[U64], I32, &[1]),
        ("PK column, 8 bytes at offset 8 of 16", &[U64, U64], I64, &[1]),
        ("PK prefix, 8 of 16", &[U64, U64], I64, &[0]),
        ("whole PK, 16 bytes", &[U64, U64], I64, &[0, 1]),
        ("whole PK, 12 bytes", &[U32, U64], I64, &[0, 1]),
    ] {
        let schema = schema(pk, payload);
        let whole_pk = group_cols.len() == pk.len() && pk.len() > 1;
        let mut bb = BatchBuilder::new(&schema);
        for i in 0..N {
            let g = (i.wrapping_mul(0x9E37_79B9) % GROUPS) as u128;
            let natives: Vec<u128> = match (pk.len(), whole_pk, group_cols[0]) {
                (1, ..) => vec![i as u128],
                // The whole PK is the group: 1000 distinct keys, repeated.
                (_, true, _) => vec![g % 10, g],
                (_, false, 0) => vec![g, i as u128],
                _ => vec![i as u128, g],
            };
            bb.begin_row_natives(&natives, 1);
            bb.put_int(g);
            bb.end_row();
        }
        let batch = bb.finish();
        let key = GroupOutKey::new(&schema, group_cols, []).unwrap().0;
        std::hint::black_box(key.numbered(&batch));
        let (groups, instructions) = counter.measure(|| key.numbered(&batch));
        assert_eq!(groups.len(), GROUPS as usize, "{label}");
        std::hint::black_box(groups);
        println!(
            "group_identity_bench {label:<38} {:6.1} instr/row",
            instructions as f64 / N as f64
        );
    }
}
