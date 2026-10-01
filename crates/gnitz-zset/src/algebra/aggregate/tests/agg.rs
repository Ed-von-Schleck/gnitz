use super::*;
use crate::algebra::aggregate::ReduceShape;
use crate::algebra::GroupOutKey;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::le_cell;
use gnitz_wire::AggDescriptor;

/// Both column kernels reach the value one `apply` per row reaches, for every
/// aggregate over every scalar payload width, with NULLs and non-unit weights:
/// `fold_rows` over row ranges, and `fold_grouped` over the same ranges split
/// into interleaved groups.
#[test]
fn the_column_kernels_match_a_step_per_row() {
    const N: usize = 97;
    let tcs = [
        TypeCode::U8,
        TypeCode::I8,
        TypeCode::U16,
        TypeCode::I16,
        TypeCode::U32,
        TypeCode::I32,
        TypeCode::U64,
        TypeCode::I64,
        TypeCode::F32,
        TypeCode::F64,
    ];
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend(tcs.iter().map(|&tc| SchemaColumn::new(tc, true)));
    let schema = SchemaDescriptor::new(&cols, &[0]);
    let mut rng = crate::test_support::Rng::new(0x5eed);
    let mut b = BatchBuilder::new(&schema);
    for row in 0..N {
        b.begin_row(row as u128, rng.gen_range(5) as i64 + 1);
        let nulls = rng.next_u64() & ((1 << tcs.len()) - 1);
        for (pi, &tc) in tcs.iter().enumerate() {
            let v = rng.next_u64();
            match tc {
                _ if nulls >> pi & 1 == 1 => b.put_null(),
                TypeCode::F32 => b.put_float(f64::from((v as i32) as f32 / 7.0)),
                TypeCode::F64 => b.put_float((v as i64) as f64 / 7.0),
                _ => b.put_int(le_cell(&v.to_le_bytes()[..schema.columns[pi + 1].size() as usize])),
            }
        }
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();
    for ci in 1..=tcs.len() as u32 {
        for agg_op in [
            AggFunc::Count,
            AggFunc::CountNonNull,
            AggFunc::Sum,
            AggFunc::Min,
            AggFunc::Max,
        ] {
            let aggs = [AggDescriptor { col_idx: ci, agg_op }, AggDescriptor::COUNT_STAR];
            let (key, prefix) = GroupOutKey::new(&schema, &[0], [0]).unwrap();
            let template = ReduceShape::new(&schema, key, prefix, &aggs).unwrap().acc_template;
            let ranges = [(0, 0), (0, 1), (3, 40), (40, 41), (41, N)];
            let step = |acc: &mut Accumulator, row: usize| acc.apply(acc.kind, acc.src, &mb, row, mb.get_weight(row));
            let bits = |a: &Accumulator| {
                a.value().map(|v| match v {
                    AggValue::Bits(b) => b,
                    AggValue::Wide(..) => unreachable!("a scalar column"),
                })
            };

            let (mut bulk, mut each) = (template[0].clone(), template[0].clone());
            for (s, e) in ranges {
                Accumulator::fold_rows(std::slice::from_mut(&mut bulk), &mb, s..e, true);
                (s..e).for_each(|row| step(&mut each, row));
            }
            assert_eq!(bits(&bulk), bits(&each), "{agg_op:?} over column {ci}");

            // Three groups, their rows interleaved; the state grows mid-fold, as
            // an ad-hoc fold's does between chunks.
            const GROUPS: usize = 3;
            let mut per_group = vec![template[0].clone(); GROUPS];
            let mut state = template[0].grouped();
            for (i, &(s, e)) in ranges.iter().enumerate() {
                let ord: Vec<u32> = (s..e).map(|row| (row % GROUPS) as u32).collect();
                (s..e).for_each(|row| step(&mut per_group[row % GROUPS], row));
                state.resize(if i < 2 { 1 } else { GROUPS }, &template[0]);
                template[0].fold_grouped(&mb, &[(s, e)], &ord, &mut state);
            }
            for (g, want) in per_group.iter().enumerate() {
                let mut got = template[0].clone();
                state.take(g, &mut got);
                assert_eq!(bits(&got), bits(want), "{agg_op:?} over column {ci}, group {g}");
            }
        }
    }
}
