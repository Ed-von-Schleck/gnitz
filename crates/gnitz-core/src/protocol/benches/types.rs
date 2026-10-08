use super::*;

/// Instructions per pushed key row: one key column and three through
/// `push_natives`, whose cost is per column, and one through `from_natives`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_column_push_bench() {
    const ROWS: usize = 1_000_000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let key_schema = |types: &[TypeCode]| {
        Schema::from_parts(
            types.iter().map(|&tc| ColumnDef::new("k", tc, false)).collect(),
            &(0..types.len() as u32).collect::<Vec<u32>>(),
        )
        .unwrap()
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

/// Instructions per gathered row: a full permutation and half of one, over 1, 4 and 16 I64
/// payload columns and over one with a STRING column, whose cells a partial gather relocates.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn gather_bench() {
    const ROWS: usize = 262_144;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let mut st = 0x9E3779B97F4A7C15u64;
    let mut rng = move || {
        st ^= st << 13;
        st ^= st >> 7;
        st ^= st << 17;
        st
    };
    let mut perm: Vec<u32> = (0..ROWS as u32).collect();
    for i in (1..ROWS).rev() {
        perm.swap(i, (rng() % (i as u64 + 1)) as usize);
    }
    for (name, ints, string) in [
        ("1 col", 1usize, false),
        ("4 cols", 4, false),
        ("16 cols", 16, false),
        ("1 col + string", 1, true),
    ] {
        let mut columns = vec![ColumnDef::new("id", TypeCode::U64, false)];
        columns.extend((0..ints).map(|_| ColumnDef::new("v", TypeCode::I64, false)));
        columns.extend(string.then(|| ColumnDef::new("s", TypeCode::String, false)));
        let schema = Schema::from_parts(columns, &[0]).unwrap();
        let mut b = ZSetBatch::new(&schema);
        let mut app = BatchAppender::new(&mut b);
        for i in 0..ROWS {
            let r = rng();
            app.add_row(i as u128, 1);
            for _ in 0..ints {
                app.i64_val(r as i64);
            }
            if string {
                app.str_val(if r & 1 == 0 {
                    "short"
                } else {
                    "a string long enough to spill out of line"
                });
            }
        }
        for (label, len) in [("full", ROWS), ("half", ROWS / 2)] {
            std::hint::black_box(b.clone().gather(&perm[..len]));
            // `gather` consumes its batch, so the clone it is handed is measured and subtracted.
            let (copy, clone) = counter.measure(|| b.clone());
            let (out, gather) = counter.measure(|| b.clone().gather(&perm[..len]));
            std::hint::black_box((copy, out));
            println!(
                "gather_bench {name:<16} {label:<5} {:7.1} instr/row",
                (gather - clone) as f64 / len as f64
            );
        }
    }
}
