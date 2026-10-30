use super::*;
use gnitz_core::BatchAppender;

/// Rows a bench table holds.
const ROWS: usize = 1 << 20;

/// Rows a row group holds, as a dump page of the default size does.
const GROUP: usize = gnitz_core::DEFAULT_PAGE_ROWS as usize;

/// A BIGINT key and one column `v` of type `ty`.
fn table(ty: ColType, nullable: bool) -> Arc<Schema> {
    let cols = vec![
        ColumnDef::new("id", TypeCode::I64, false),
        ColumnDef::new("v", ty, nullable),
    ];
    Arc::new(Schema::from_parts(cols, &[0]).unwrap())
}

/// `ROWS` rows of `schema` in row groups of `GROUP`, `v` from `cell`.
fn groups(schema: &Arc<Schema>, cell: impl Fn(&mut BatchAppender<'_>, usize)) -> Vec<ZSetBatch> {
    (0..ROWS)
        .step_by(GROUP)
        .map(|start| {
            let mut batch = ZSetBatch::with_capacity(schema, GROUP);
            let mut app = BatchAppender::new(&mut batch);
            for i in start..start + GROUP {
                let r = app.add_row_natives(&[i as u128], 1);
                cell(r, i);
            }
            batch
        })
        .collect()
}

/// A value of `i` that does not follow the key's order.
fn scramble(i: usize) -> u64 {
    (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

/// Instructions per row to write a 1M-row table and read it back, per type of
/// its one payload column, and the file's bytes.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn columns_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    let dir = tempfile::tempdir().unwrap();
    type Fill = fn(&mut BatchAppender<'_>, usize);
    let cases: [(&str, ColType, bool, Fill); 9] = [
        ("BOOLEAN", ColType::of(TypeCode::Bool), false, |r, i| {
            r.int_val(i128::from(scramble(i) & 1));
        }),
        ("SMALLINT", ColType::of(TypeCode::I16), false, |r, i| {
            r.int_val(i128::from(scramble(i) as i16));
        }),
        ("INT", ColType::of(TypeCode::I32), false, |r, i| {
            r.int_val(i128::from(scramble(i) as i32));
        }),
        ("BIGINT", ColType::of(TypeCode::I64), false, |r, i| {
            r.int_val(i128::from(scramble(i) as i64));
        }),
        ("DOUBLE", ColType::of(TypeCode::F64), false, |r, i| {
            r.f64_val(scramble(i) as f64 / 3.0);
        }),
        ("TEXT", ColType::of(TypeCode::String), false, |r, i| {
            r.str_val(&format!("row-{:016x}", scramble(i)));
        }),
        ("BIGINT NULL", ColType::of(TypeCode::I64), true, |r, i| {
            match i % 10 {
                0 => r.null(),
                _ => r.int_val(i128::from(scramble(i) as i64)),
            };
        }),
        ("DECIMAL", ColType::decimal(2), false, |r, i| {
            r.int_val(i128::from((scramble(i) >> 8) as i64));
        }),
        ("UUID", ColType::of(TypeCode::UUID), false, |r, i| {
            r.u128_val(u128::from(scramble(i)) << 64 | u128::from(scramble(i + 1)));
        }),
    ];
    for (name, ty, nullable, fill) in cases {
        let schema = table(ty, nullable);
        let batches = groups(&schema, fill);
        let path = dir.path().join(format!("{name}.parquet"));
        let (_, write) = counter.measure(|| {
            let mut w = TableWriter::create(&path, &schema).unwrap();
            for b in &batches {
                w.write(std::hint::black_box(b)).unwrap();
            }
            w.finish().unwrap()
        });
        let (read_rows, read) = counter.measure(|| {
            let mut r = TableReader::open(&path, &schema).unwrap();
            let mut n = 0;
            while let Some(b) = r.next_group().unwrap() {
                n += std::hint::black_box(b).len();
            }
            n
        });
        assert_eq!(read_rows, ROWS);
        let bytes = std::fs::metadata(&path).unwrap().len();
        println!(
            "{name:12} write {:7.1} instr/row  read {:7.1} instr/row  file {bytes} bytes",
            write as f64 / ROWS as f64,
            read as f64 / ROWS as f64
        );
    }
}
