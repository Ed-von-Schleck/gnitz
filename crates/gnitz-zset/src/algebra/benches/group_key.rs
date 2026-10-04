use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{ColumnTable, SchemaColumn, TypeCode};
use crate::test_support::pk_payload_schema;

/// Instructions per row of [`GroupOutKey::ordinals`] over 262 144 unconsolidated
/// rows, per key form: every key column drawn from 1024 values, where the rows
/// hash into groups, and from all of `u64`, where they are sorted.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn group_ordinals_bench() {
    use TypeCode::{U128, U64};
    const N: u64 = 1 << 18;
    let counter = gnitz_foundation::perf::Counter::instructions();
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
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn group_identity_bench() {
    use TypeCode::{I32, I64, U32, U64};
    const N: u64 = 1_000_000;
    const GROUPS: u64 = 1000;
    let counter = gnitz_foundation::perf::Counter::instructions();
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
