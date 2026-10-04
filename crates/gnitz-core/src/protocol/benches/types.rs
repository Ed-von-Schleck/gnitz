use super::*;

/// Instructions per pushed key row: one key column and three through
/// `push_natives`, whose cost is per column, and one through `from_natives`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_column_push_bench() {
    const ROWS: usize = 1_000_000;
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let key_schema = |types: &[TypeCode]| Schema {
        columns: types.iter().map(|&tc| ColumnDef::new("k", tc, false)).collect(),
        pk_cols: (0..types.len() as u32).collect(),
    };
    for types in [&[TypeCode::I64][..], &[TypeCode::I32, TypeCode::U128, TypeCode::I16]] {
        let schema = std::hint::black_box(key_schema(types));
        let mut col = PkColumn::empty_for_schema(&schema);
        col.reserve(ROWS);
        let ((), instr) = counter.measure(|| {
            for i in 0..ROWS as u128 {
                let natives = [i, i ^ 0x55, i & 0x7fff];
                col.push_natives(&natives[..types.len()]);
            }
        });
        std::hint::black_box(&col);
        println!("push_natives {types:?}: {:.1} instr/row", instr as f64 / ROWS as f64);
    }
    let schema = std::hint::black_box(key_schema(&[TypeCode::I64]));
    let (col, instr) = counter.measure(|| PkColumn::from_natives(&schema, 0..ROWS as u128));
    std::hint::black_box(&col);
    println!("from_natives [I64]: {:.1} instr/row", instr as f64 / ROWS as f64);
}
