use super::*;
use crate::algebra::aggregate::ReduceShape;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{STRS, le_cell, pk_payload_schema};
use gnitz_foundation::rng::Rng;
use gnitz_wire::{AggDescriptor, TypeCode};

const N: usize = 97;
const INTS: [TypeCode; 8] = [
    TypeCode::U8,
    TypeCode::I8,
    TypeCode::U16,
    TypeCode::I16,
    TypeCode::U32,
    TypeCode::I32,
    TypeCode::U64,
    TypeCode::I64,
];

/// A random cell of `width` bytes.
fn cell(rng: &mut Rng, width: usize) -> u128 {
    le_cell(&rng.next_u64().to_le_bytes()[..width])
}

/// For every aggregate over column `ci` of `b`, `fold` reaches the value one
/// `apply` per row reaches, in each group mode: row ranges into one group, the
/// same ranges a group each, and their rows interleaved over three groups.
fn assert_kernels_match(b: &Batch, ci: u32) {
    let schema = b.schema();
    let mb = b.as_mem_batch();
    for agg_op in [
        AggFunc::Count,
        AggFunc::CountNonNull,
        AggFunc::Sum,
        AggFunc::Min,
        AggFunc::Max,
    ] {
        let aggs = [AggDescriptor { col_idx: ci, agg_op }];
        let aggs = ReduceShape::new(schema, &[0], &aggs).unwrap().0.aggs;
        let ranges = [(0, 0), (0, 1), (3, 40), (40, 41), (41, N)];
        let step = |vals: &mut AggValues, g: usize, row: usize| vals.step(g, &mb, row, mb.get_weight(row));
        let assert_same = |got: &AggValues, want: &AggValues, groups: usize, mode: &str| {
            // The held register image, or `None` for an extreme with no value.
            let bits = |vals: &AggValues, g: usize| {
                let at = vals.at(0, g);
                (vals.has.get(at) != Some(&false)).then(|| vals.acc[at])
            };
            for g in 0..groups {
                assert_eq!(
                    bits(got, g),
                    bits(want, g),
                    "{agg_op:?} over column {ci}, {mode}, group {g}"
                );
            }
        };
        let sized = |groups: usize| AggValues::new(&aggs, groups);

        // The groups of the interleaved fold grow mid-fold, as an ad-hoc fold's
        // do between chunks.
        const GROUPS: usize = 3;
        let (mut one, mut one_each) = (sized(1), sized(1));
        let (mut ranged, mut ranged_each) = (sized(ranges.len()), sized(ranges.len()));
        let (mut mixed, mut mixed_each) = (sized(0), sized(GROUPS));
        ranged.fold(0, &mb, &ranges, FoldGroups::Each);
        for (i, &(s, e)) in ranges.iter().enumerate() {
            one.fold(0, &mb, &[(s, e)], FoldGroups::One(0));
            let ord: Vec<u32> = (s..e).map(|row| (row % GROUPS) as u32).collect();
            mixed.resize(if i < 2 { 1 } else { GROUPS });
            mixed.fold(0, &mb, &[(s, e)], &ord[..]);
            for row in s..e {
                step(&mut one_each, 0, row);
                step(&mut ranged_each, i, row);
                step(&mut mixed_each, row % GROUPS, row);
            }
        }
        assert_same(&one, &one_each, 1, "one group");
        assert_same(&ranged, &ranged_each, ranges.len(), "a group per range");
        assert_same(&mixed, &mixed_each, GROUPS, "interleaved groups");
    }
}

/// Over every scalar payload width, with NULLs and non-unit weights.
#[test]
fn the_column_kernels_match_a_step_per_row() {
    let tcs: Vec<TypeCode> = INTS.into_iter().chain([TypeCode::F32, TypeCode::F64]).collect();
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend(tcs.iter().map(|&tc| SchemaColumn::new(tc, true)));
    let schema = SchemaDescriptor::new(&cols, &[0]);
    let mut rng = Rng::new(0x5eed);
    let mut b = BatchBuilder::new(&schema);
    for row in 0..N {
        b.begin_row(row as u128, rng.gen_range(5) as i64 + 1);
        let nulls = rng.next_u64() & ((1 << tcs.len()) - 1);
        for (pi, &tc) in tcs.iter().enumerate() {
            let v = rng.next_u64();
            match tc {
                _ if nulls >> pi & 1 == 1 => {
                    b.put_null();
                }
                TypeCode::F32 => {
                    b.put_float(f64::from((v as i32) as f32 / 7.0));
                }
                TypeCode::F64 => {
                    b.put_float((v as i64) as f64 / 7.0);
                }
                _ => {
                    b.put_int(le_cell(&v.to_le_bytes()[..schema.columns()[pi + 1].size()]));
                }
            }
        }
        b.end_row();
    }
    let b = b.finish();
    for ci in 1..=tcs.len() as u32 {
        assert_kernels_match(&b, ci);
    }
}

/// Over a PK column of every integer width, whose cells are OPK images: the
/// whole PK, and a column between two others of a compound one.
#[test]
fn the_column_kernels_read_a_pk_column() {
    let mut rng = Rng::new(0x9e37);
    for tc in INTS {
        for (pk, ci) in [(vec![tc], 0), (vec![TypeCode::U16, tc, TypeCode::U32], 1)] {
            let schema = pk_payload_schema(&pk);
            let mut b = BatchBuilder::new(&schema);
            for _ in 0..N {
                let natives: Vec<u128> = pk
                    .iter()
                    .map(|&t| cell(&mut rng, SchemaColumn::new(t, false).size()))
                    .collect();
                b.begin_row_natives(&natives, rng.gen_range(5) as i64 + 1);
                b.put_int(0);
                b.end_row();
            }
            assert_kernels_match(&b.finish(), ci);
        }
    }
}

/// A wide MIN/MAX seeded from a row's value-index image holds the value a step
/// of that row reaches, and `copy` hands it on: over strings holding `0x00`
/// and 16-byte integers, into a group seeded or copied into again with a
/// longer or a shorter value each time.
#[test]
fn a_wide_extreme_seeds_from_its_image_and_copies() {
    use TypeCode::{I128, String, U64};
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(U64, false),
            SchemaColumn::new(String, false),
            SchemaColumn::new(I128, false),
        ],
        &[0],
    );
    let rows = STRS.len();
    let mut b = BatchBuilder::new(&schema);
    for (r, s) in STRS.iter().enumerate() {
        b.begin_row(r as u128, 1);
        b.put_string(s);
        b.put_int((r as i128 - 4).wrapping_mul(i128::MAX / 7) as u128);
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();
    for agg_op in [AggFunc::Min, AggFunc::Max] {
        for ci in [1, 2] {
            let aggs = [AggDescriptor { col_idx: ci, agg_op }];
            let aggs = ReduceShape::new(&schema, &[0], &aggs).unwrap().0.aggs;
            let image_col = aggs[0].index_image().unwrap();
            // Per row: a stepped group, a seeded one; then one reseeded and one
            // copied into for every row.
            let (reseeded, copied) = (2 * rows, 2 * rows + 1);
            let mut vals = AggValues::new(&aggs, 2 * rows + 2);
            for r in (0..rows).chain((0..rows).rev()) {
                let what = format!("{agg_op:?} over column {ci}, row {r}");
                let (stepped, seeded) = (2 * r, 2 * r + 1);
                let mut image = Vec::new();
                image_col.append(&mb, r, &mut image);
                vals.clear_extreme(0, stepped);
                vals.step(stepped, &mb, r, 1);
                vals.seed_from_index(0, seeded, &image);
                vals.seed_from_index(0, reseeded, &image);
                vals.copy(stepped, copied);
                assert!(vals.same(stepped, seeded), "{what}: seeded");
                assert!(vals.same(stepped, reseeded), "{what}: reseeded");
                assert!(vals.same(stepped, copied), "{what}: copied");
                if ci == 1 {
                    assert_eq!(vals.wide(vals.at(0, seeded)), STRS[r].as_bytes(), "{what}");
                }
            }
        }
    }
}
