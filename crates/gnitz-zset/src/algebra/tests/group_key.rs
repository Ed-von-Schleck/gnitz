use super::*;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::SchemaColumn;
use crate::test_support::{
    arb_fold_case, cell, fold_batch, fold_schemas, keyed_schema, le_cell, make_batch_opk,
    make_schema_pk_u64_payload_string, opk_pk, pk_only_schema, pk_payload_schema, radix_fold_rows, u64_pk_schema,
};
use gnitz_foundation::rng::mix;
use gnitz_wire::{MAX_PK_BYTES, TypeCode};
use proptest::prelude::*;
use std::collections::HashMap;

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
    let all = [(0, mb.len())];
    key.key.with_identity(mb, &all, Over(&all))
}

/// Each key's first row with an equal key: two rows share an entry iff their
/// keys are equal.
fn firsts<K: std::hash::Hash + Eq>(keys: impl Iterator<Item = K>) -> Vec<usize> {
    let mut seen = HashMap::new();
    keys.enumerate().map(|(r, k)| *seen.entry(k).or_insert(r)).collect()
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
        let schema = match col1_is_pk {
            true => pk_only_schema(&[TypeCode::U64, tc]),
            false => u64_pk_schema(&[SchemaColumn::new(tc, false)]),
        };
        let mut b = BatchBuilder::new(&schema);
        match col1_is_pk {
            true => {
                b.begin_row_natives(&[0, le_cell(le)], 1);
            }
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
/// consolidation: the numbering — by `runs` where it answers, else numbered —
/// gives two rows one ordinal iff they share an `out_pk`, names each group by its
/// least row, and lists the groups in strictly ascending `out_pk` order; two rows
/// share an `out_pk` iff they share their group columns' values, and iff they
/// share an identity. `runs` answers for a consolidated batch grouped by leading
/// PK columns whose bytes are its output PK. A row's identity rebuilds its `out_pk` under every key form but
/// a PK range, and the numbering's kept identities are its groups'.
fn assert_groups_follow_out_pk(raw: &Batch) -> Result<(), TestCaseError> {
    let schema = *raw.schema();
    let consolidated = Batch::clone(raw).into_consolidated();
    let pk: Vec<u32> = schema.pk_cols().to_vec();
    let all: Vec<u32> = (0..schema.num_columns() as u32).collect();
    let n = schema.num_columns() as u32;
    let payload: Vec<u32> = (0..schema.num_columns() as u32).filter(|c| !pk.contains(c)).collect();
    let mut forms = vec![vec![], all, payload];
    forms.extend((1..=pk.len()).map(|lead| pk[..lead].to_vec()));
    forms.extend((0..n).map(|c| vec![c]));
    forms.extend((0..n).flat_map(|a| (0..n).filter(move |&b| b != a).map(move |b| vec![a, b])));
    for cols in &forms {
        let (key, _) = GroupOutKey::new(&schema, cols, []).unwrap();
        for batch in [raw, &consolidated] {
            let mb = batch.as_mem_batch();
            let out_pk = |row: usize| key.out_pk(&mb, row).bytes().to_vec();
            let mut runs = Vec::new();
            let mut scratch = GroupScratch::default();
            let in_runs = key.runs_into(batch, &mut runs);
            match in_runs {
                true => scratch.groups.fill_runs(&runs),
                false => key.numbered_into(batch, &mut scratch),
            }
            let groups = &scratch.groups;
            let first = firsts((0..batch.len()).map(out_pk));
            let named: Vec<usize> = groups.ord.iter().map(|&g| groups.first[g as usize] as usize).collect();
            prop_assert_eq!(&named, &first, "{:?}: groups", cols);
            let mut seen = groups.by_pk.clone();
            seen.sort_unstable();
            prop_assert_eq!(seen, (0..groups.len() as u32).collect::<Vec<_>>(), "{:?}", cols);
            let pks: Vec<Vec<u8>> = groups
                .by_pk
                .iter()
                .map(|&g| out_pk(groups.first[g as usize] as usize))
                .collect();
            prop_assert!(pks.is_sorted_by(|a, b| a < b), "{:?}: groups out of order", cols);

            // A longer proper prefix is a fold, which no PK order groups.
            let bytes: usize = cols.iter().map(|&c| schema.columns()[c as usize].size()).sum();
            let in_place = *cols == pk || bytes <= GROUP_PK_BYTES;
            if std::ptr::eq(batch, &consolidated) && !cols.is_empty() && pk.starts_with(cols) && in_place {
                prop_assert!(in_runs, "{:?}: a PK prefix of a consolidated batch", cols);
            }

            let group =
                |r: usize| -> Vec<_> { cols.iter().map(|&c| cell(&mb, schema.locate(c as usize), r)).collect() };
            let ids = identities(&key, &mb);
            // The output PK rebuilt from a row's identity is its own: handed
            // row 0, every key but a PK range reads the identity alone.
            let pk_range = matches!(key.key, GroupKey::PkRange { .. });
            for (row, &id) in ids.iter().enumerate() {
                let pk = key.out_pk_with(&mb, if pk_range { row } else { 0 }, Some(id));
                prop_assert_eq!(pk.bytes(), &out_pk(row)[..], "{:?}: row {}", cols, row);
            }
            // The numbering's identities, where it keeps them, are its groups' first rows'.
            if !groups.ids.is_empty() {
                prop_assert_eq!(groups.ids.len(), groups.len(), "{:?}", cols);
                for (g, &first) in groups.first.iter().enumerate() {
                    prop_assert_eq!(groups.ids[g], ids[first as usize], "{:?}: group {}", cols, g);
                }
            }
            prop_assert_eq!(
                &firsts((0..batch.len()).map(group)),
                &first,
                "{:?}: group columns",
                cols
            );
            prop_assert_eq!(&firsts(ids.iter()), &first, "{:?}: identities", cols);
            assert_prefixes_follow_out_pk(&key, cols, &mb, &ids, &first)?;
        }
    }
    Ok(())
}

/// The group prefix of an index grouped by `key` — by `cols` — ahead
/// of a suffix of one column and of two: every row's, written a batch at a time,
/// is its own [`GroupPrefix::of`], built from its identity `ids[row]` or
/// not, and two rows share one iff they share an output PK: `first` names each
/// row's first row of an equal output PK.
fn assert_prefixes_follow_out_pk(
    key: &GroupOutKey,
    cols: &[u32],
    mb: &MemBatch,
    ids: &[u128],
    first: &[usize],
) -> Result<(), TestCaseError> {
    let (ordinal, image) = (
        SchemaColumn::new(TypeCode::U8, false),
        SchemaColumn::new(TypeCode::U64, false),
    );
    for suffix in [&[ordinal][..], &[ordinal, image]] {
        let (prefix, b) = key.index_prefix(suffix);
        let index = b.finish().unwrap();
        let width = prefix.width();
        prop_assert_eq!(
            index.pk_stride(),
            width + suffix.iter().map(|c| c.size()).sum::<usize>()
        );
        // A key past each prefix, so a write past one shows.
        let stride = width + 1;
        let mut keys = vec![0xA5u8; mb.len() * stride];
        prefix.write(mb, &[(0, mb.len())], &mut keys, stride);
        let prefix_of = |row: usize| prefix.of(mb, row, None, &mut [0; MAX_PK_BYTES]).to_vec();
        for (row, &id) in ids.iter().enumerate() {
            let own = prefix_of(row);
            prop_assert_eq!(
                &keys[row * stride..][..stride],
                &[&own[..], &[0xA5]].concat()[..],
                "{:?}: row {}",
                cols,
                row
            );
            let mut buf = [0; MAX_PK_BYTES];
            let from_id = prefix.of(mb, row, Some(id), &mut buf);
            prop_assert_eq!(from_id, &own[..], "{:?}: row {}", cols, row);
        }
        prop_assert_eq!(&firsts((0..mb.len()).map(prefix_of)), first, "{:?}: prefixes", cols);
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

/// [`assert_groups_follow_out_pk`] over batches too long, over too many groups,
/// to number by hashing, at every PK width arm.
#[test]
fn groups_of_a_long_batch_follow_out_pk() {
    for s in fold_schemas() {
        assert_groups_follow_out_pk(&fold_batch(&s, &radix_fold_rows(s.pk_stride(), 1024))).unwrap();
    }
}

/// [`assert_groups_follow_out_pk`] over a compound signed PK and signed, wide
/// and nullable payload columns, each holding repeated values — two unsigned
/// nullable ones whose zero packs as a NULL's zeroed slot, so only the bitmap
/// tells a NULL in one from a NULL in the other — over 64 rows, so both the
/// hashed and the sorted numbering are taken, by group count.
#[test]
fn groups_follow_out_pk_over_signed_wide_and_nullable_columns() {
    use TypeCode::{I32, I64, U16, U32, U64, U128};
    let c = SchemaColumn::new;
    // `[U32 pk0, I32 pk1, U64 v NULL, U128 w, U16 n NULL, I64 s]`, PK `(pk0, pk1)`.
    let schema = keyed_schema(
        &[U32, I32],
        &[c(U64, true), c(U128, false), c(U16, true), c(I64, false)],
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

/// [`assert_groups_follow_out_pk`] over a compound PK of four columns in 15
/// bytes and three nullable byte columns: a whole-PK group and a packed group
/// of every nullable column each fill an index's column limit ahead of a
/// suffix of two columns.
#[test]
fn groups_follow_out_pk_over_a_four_column_pk_and_nullable_bytes() {
    use TypeCode::{I32, U8, U16, U64};
    let c = SchemaColumn::new;
    let schema = keyed_schema(&[U8, U16, I32, U64], &[c(U8, true); 3]);
    let mut bb = BatchBuilder::new(&schema);
    for i in 0..48u64 {
        let m = mix(i) >> 40;
        let pk = [m % 2, m / 2 % 3, m / 6 % 3, m / 18 % 2];
        bb.begin_row_natives(&pk.map(u128::from), 1);
        for k in 0..3 {
            bb.put_opt_int((!(m >> (8 + k)).is_multiple_of(3)).then_some(u128::from(m >> (12 + 2 * k) & 1)));
        }
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
    let ints = u64_pk_schema(&[
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::I32, false),
    ]);
    let strs = make_schema_pk_u64_payload_string();
    for (schema, cols) in [(&ints, &[1u32, 2][..]), (&strs, &[1][..])] {
        let mut b = BatchBuilder::new(schema);
        for i in 0..N as u64 {
            b.begin_row(i as u128, 1);
            match cols.len() {
                2 => {
                    b.put_opt_int((i % 7 != 0).then(|| mix(i) as i64 as u128));
                    b.put_int(mix(i ^ 1) as u32 as u128);
                }
                _ => {
                    b.put_string(&format!("a group key of some length {}", mix(i) % 97));
                }
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
                key.key.with_identity(&mb, &runs, Over(&runs)),
                want,
                "{cols:?}: runs of {run}, gaps of {gap}"
            );
        }
        assert!(
            key.key.with_identity(&mb, &[], Over(&[])).is_empty(),
            "{cols:?}: no rows"
        );
    }
}

// ---------------------------------------------------------------------------
// pk_firsts
// ---------------------------------------------------------------------------

#[test]
fn pk_firsts_numbers_keys_by_first_row() {
    // A narrow key, and a wide one whose keys differ only past the narrow width.
    let narrow = pk_payload_schema(&[TypeCode::U64]);
    let wide = pk_payload_schema(&[TypeCode::U64, TypeCode::U64, TypeCode::U64]);
    assert!(wide.pk_stride() > NARROW_PK_MAX_BYTES);
    for schema in [&narrow, &wide] {
        let pk = |k: u128| match schema.pk_cols().len() {
            1 => opk_pk(schema, &[k]),
            _ => opk_pk(schema, &[1, 2, k]),
        };
        let firsts = |keys: &[u128]| {
            let rows: Vec<_> = keys.iter().map(|&k| (pk(k), 1, 0)).collect();
            pk_firsts(&make_batch_opk(schema, &rows).as_mem_batch())
        };

        let f = firsts(&[5, 5, 7, 5, 9, 7, 9, 9, 1]);
        assert_eq!(f.groups, [0, 0, 1, 0, 2, 1, 2, 2, 3]);
        assert_eq!(f.keep, [(0, 1), (2, 3), (4, 5), (8, 9)]);
        assert!(f.repeats());

        let f = firsts(&[3, 4, 5]);
        assert_eq!(f.groups, [0, 1, 2]);
        assert_eq!(f.keep, [(0, 3)]);
        assert!(!f.repeats());

        let f = firsts(&[]);
        assert!(f.groups.is_empty() && f.keep.is_empty());
        assert!(!f.repeats());
    }
}
