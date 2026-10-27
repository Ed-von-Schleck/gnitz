use super::*;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::SchemaColumn;
use crate::test_support::{arb_fold_case, cell, fold_batch, le_cell, mix, opk_pk, pk_only_schema};
use gnitz_wire::TypeCode;
use proptest::prelude::*;

/// The identities of `runs`' rows, in row order.
struct Over<'a>(&'a [(usize, usize)]);

impl IdentityLoop for Over<'_> {
    type Out = Vec<u128>;
    fn run(self, identity: impl Fn(usize) -> u128) -> Vec<u128> {
        self.0.iter().flat_map(|&(s, e)| s..e).map(identity).collect()
    }
}

/// Every row's identity.
fn identities(key: &GroupOutKey, mb: &MemBatch) -> Vec<u128> {
    let all = [(0, mb.count)];
    key.with_identity(mb, &all, Over(&all))
}

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
        (identities(&key, &mb)[0], key.out_pk(&mb, 0).bytes().to_vec())
    };

    for (tc, vals) in [
        (TypeCode::I8, vec![0i128, -1, i8::MIN as i128, i8::MAX as i128]),
        (TypeCode::I32, vec![1, -1, 100, i32::MIN as i128, i32::MAX as i128]),
        (TypeCode::I64, vec![0, -1, i64::MIN as i128, i64::MAX as i128]),
        (TypeCode::U16, vec![0, 1, 0xBEEF, u16::MAX as i128]),
        (TypeCode::U64, vec![0, 1, u64::MAX as i128]),
        (TypeCode::U128, vec![0, 1, u64::MAX as i128 + 1, u128::MAX as i128]),
    ] {
        let width = SchemaColumn::new(tc, false).size();
        for v in vals {
            let le = &(v as u128).to_le_bytes()[..width];
            let want_pk = opk_pk(&pk_only_schema(&[tc]), &[v as u128]);
            let want = (gnitz_wire::widen_pk_be(&want_pk), want_pk);
            assert_eq!(key(tc, le, true), want, "PK-column key for {tc} v={v}");
            assert_eq!(key(tc, le, false), want, "payload-column key for {tc} v={v}");
        }
    }
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
            let runs = key.runs(batch);
            let groups = runs
                .as_deref()
                .map_or_else(|| key.numbered(batch), GroupOrdinals::of_runs);
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

            if let Some(runs) = &runs {
                prop_assert_eq!(runs.len(), groups.len(), "{:?}", cols);
                for (g, run) in runs.iter().map(|&(s, e)| s..e).enumerate() {
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
            let bytes: usize = cols.iter().map(|&c| schema.columns()[c as usize].size()).sum();
            let in_place = *cols == pk || bytes <= GROUP_PK_BYTES;
            if std::ptr::eq(batch, &consolidated) && !cols.is_empty() && pk.starts_with(cols) && in_place {
                prop_assert!(runs.is_some(), "{:?}: a PK prefix of a consolidated batch", cols);
            }

            let group =
                |r: usize| -> Vec<_> { cols.iter().map(|&c| cell(&mb, schema.locate(c as usize), r)).collect() };
            let ids = identities(&key, &mb);
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
                    prop_assert_eq!(ids[a] == ids[b], same_out_pk, "{:?}: rows {} and {}", cols, a, b);
                }
            }
        }
    }
    Ok(())
}

proptest! {
    /// [`assert_groups_follow_out_pk`] over every PK width arm.
    #[test]
    fn groups_follow_out_pk_at_every_pk_width((s, rows) in arb_fold_case()) {
        assert_groups_follow_out_pk(&fold_batch(&s, &rows))?;
    }
}

/// [`assert_groups_follow_out_pk`] over a compound signed PK and signed, wide
/// and nullable payload columns, each holding repeated values — two unsigned
/// nullable ones whose zero packs as a NULL's zeroed slot, so only the bitmap
/// tells a NULL in one from a NULL in the other — over 64 rows, so both the
/// hashed and the sorted numbering are taken, by group count.
#[test]
fn groups_follow_out_pk_over_signed_wide_and_nullable_columns() {
    use TypeCode::{I32, I64, U128, U16, U32, U64};
    let c = SchemaColumn::new;
    // `[U32 pk0, I32 pk1, U64 v NULL, U128 w, U16 n NULL, I64 s]`, PK `(pk0, pk1)`.
    let schema = SchemaDescriptor::new(
        &[
            c(U32, false),
            c(I32, false),
            c(U64, true),
            c(U128, false),
            c(U16, true),
            c(I64, false),
        ],
        &[0, 1],
    );
    let mut bb = BatchBuilder::new(&schema);
    for i in 0..64u64 {
        let m = mix(i) >> 40;
        bb.begin_row_natives(&[(m % 5) as u128, (m % 7) as i32 as i64 as u128], 1);
        bb.put_opt_int((!m.is_multiple_of(4)).then_some(u128::from(m % 3)));
        // Distinct in every row, so a key over it sorts.
        bb.put_int(u128::from(i) << 64 | u128::from(m % 3));
        bb.put_opt_int((!(m / 4).is_multiple_of(3)).then_some(u128::from(m / 12 % 3)));
        bb.put_int(((m % 9) as i64 - 4) as u128);
        bb.end_row();
    }
    assert_groups_follow_out_pk(&bb.finish()).unwrap();
}

/// A packed key read over survivor runs hands each of their rows the identity
/// it has over the whole batch, at every sparsity, packed a column at a time or
/// a row at a time.
#[test]
fn packed_identities_over_runs_are_their_whole_batch_identities() {
    const N: usize = 512;
    let c = SchemaColumn::new;
    let ints = SchemaDescriptor::new(
        &[c(TypeCode::U64, false), c(TypeCode::I64, true), c(TypeCode::I32, false)],
        &[0],
    );
    let strs = SchemaDescriptor::new(&[c(TypeCode::U64, false), c(TypeCode::String, false)], &[0]);
    for (schema, cols) in [(&ints, &[1u32, 2][..]), (&strs, &[1][..])] {
        let mut b = BatchBuilder::new(schema);
        for i in 0..N as u64 {
            b.begin_row(i as u128, 1);
            match cols.len() {
                2 => {
                    b.put_opt_int((i % 7 != 0).then(|| mix(i) as i64 as u128));
                    b.put_int(mix(i ^ 1) as u32 as u128);
                }
                _ => b.put_string(&format!("a group key of some length {}", mix(i) % 97)),
            }
            b.end_row();
        }
        let batch = b.finish();
        let mb = batch.as_mem_batch();
        let (key, _) = GroupOutKey::new(schema, cols, []).unwrap();
        assert!(matches!(key.key, GroupKey::Packed(_)), "{cols:?} packs");
        let whole = identities(&key, &mb);
        for (gap, run) in [
            (0, 1),
            (1, 1),
            (7, 1),
            (15, 1),
            (16, 1),
            (63, 1),
            (56, 8),
            (16, 16),
            (500, 3),
        ] {
            let runs: Vec<(usize, usize)> = (0..N).step_by(gap + run).map(|s| (s, (s + run).min(N))).collect();
            let want: Vec<u128> = runs.iter().flat_map(|&(s, e)| s..e).map(|r| whole[r]).collect();
            assert_eq!(
                key.with_identity(&mb, &runs, Over(&runs)),
                want,
                "{cols:?}: runs of {run}, gaps of {gap}"
            );
        }
        assert!(key.with_identity(&mb, &[], Over(&[])).is_empty(), "{cols:?}: no rows");
    }
}
