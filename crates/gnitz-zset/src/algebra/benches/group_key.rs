use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, TypeCode};
use crate::test_support::{mix, pk_payload_schema, u64_pk_schema};

/// Instructions per [`GroupOutKey::numbered`] call and per row, per key form,
/// over unconsolidated rows: a two-row delta of two groups — an UPDATE's
/// retraction and insert — then 262 144 rows in 1000 groups, where a key that
/// hashes is hashed into them, and with every row a group of its own, where the
/// rows are sorted.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn group_ordinals_bench() {
    use TypeCode::{I32, U32, U64};
    const N: u64 = 1 << 18;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let three = pk_payload_schema(&[U64; 3]);
    let four = pk_payload_schema(&[U64; 4]);
    let short = pk_payload_schema(&[U32, U64]);
    let narrow = u64_pk_schema(&[SchemaColumn::new(I32, false)]);
    let nullable = {
        let mut cols = [SchemaColumn::new(U64, false); 4];
        cols[3] = SchemaColumn::new(TypeCode::I64, true);
        SchemaDescriptor::new(&cols, &[0, 1, 2])
    };
    for (label, schema, group_cols) in [
        ("8-byte PK column", &three, &[1u32][..]),
        ("whole 12-byte PK", &short, &[0, 1]),
        ("16-byte PK prefix", &three, &[0, 1]),
        ("whole 24-byte PK (never hashed)", &three, &[0, 1, 2]),
        ("I64 payload image", &three, &[3]),
        ("I32 payload image", &narrow, &[1]),
        ("packed PK and payload columns", &three, &[1, 3]),
        ("packed nullable payload column", &nullable, &[3]),
        ("24-byte PK prefix (fold)", &four, &[0, 1, 2]),
    ] {
        let key = GroupOutKey::new(schema, group_cols, []).unwrap().0;
        let pk_cols = schema.pk_cols().len();
        for (rows, groups, iters) in [(2, 2, 10_000), (N, 1000, 1), (N, N, 1)] {
            let mut bb = BatchBuilder::new(schema);
            for i in 0..rows {
                // Odd, so distinct for every row at any column width.
                let g = mix(i);
                let g = if groups == rows { g } else { g % groups };
                // The group in every grouped column, the row number in every other.
                let cell = |c: usize| {
                    let v = if group_cols.contains(&(c as u32)) { g } else { i };
                    v as u128 & gnitz_wire::image_mask(schema.columns()[c].size())
                };
                let natives: Vec<u128> = (0..pk_cols).map(cell).collect();
                bb.begin_row_natives(&natives, 1);
                bb.put_int(cell(pk_cols));
                bb.end_row();
            }
            let batch = bb.finish();
            assert!(key.runs(&batch).is_none(), "{label}: unconsolidated rows are numbered");
            let ordinals = key.numbered(&batch);
            assert_eq!(ordinals.len() as u64, groups, "{label}");
            let ((), instructions) = counter.measure(|| {
                for _ in 0..iters {
                    std::hint::black_box(key.numbered(&batch));
                }
            });
            println!(
                "group_ordinals_bench {label:<32} {rows:>6} rows, {groups:>6} groups: {:>9} instr/call, {:6.1} instr/row",
                instructions / iters,
                instructions as f64 / (iters * rows) as f64
            );
        }
    }
}
