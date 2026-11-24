use super::*;
use gnitz_foundation::rng::mix;
use gnitz_wire::{ColType, ColumnDef};
use gnitz_zset::repr::{Batch, BatchBuilder};

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
fn groups(schema: &Arc<Schema>, cell: impl Fn(&mut BatchBuilder<&mut Batch>, usize)) -> Vec<ZSetBatch> {
    (0..ROWS)
        .step_by(GROUP)
        .map(|start| {
            let mut batch = ZSetBatch::with_capacity(schema, GROUP);
            let mut app = batch.builder();
            for i in start..start + GROUP {
                let r = app.begin_row_natives(&[i as u128], 1);
                cell(r, i);
            }
            batch
        })
        .collect()
}

/// Instructions per row to write a 1M-row table and read it back, per type of
/// its one payload column, and the file's bytes.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn columns_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    let dir = tempfile::tempdir().unwrap();
    type Fill = fn(&mut BatchBuilder<&mut Batch>, usize);
    let cases: [(&str, ColType, bool, Fill); 9] = [
        ("BOOLEAN", ColType::of(TypeCode::Bool), false, |r, i| {
            r.put_int(u128::from(mix(i as u64) & 1));
        }),
        ("SMALLINT", ColType::of(TypeCode::I16), false, |r, i| {
            r.put_int(mix(i as u64) as i16 as u128);
        }),
        ("INT", ColType::of(TypeCode::I32), false, |r, i| {
            r.put_int(mix(i as u64) as i32 as u128);
        }),
        ("BIGINT", ColType::of(TypeCode::I64), false, |r, i| {
            r.put_int(mix(i as u64) as i64 as u128);
        }),
        ("DOUBLE", ColType::of(TypeCode::F64), false, |r, i| {
            r.put_float(mix(i as u64) as f64 / 3.0);
        }),
        ("TEXT", ColType::of(TypeCode::String), false, |r, i| {
            r.put_string(&format!("row-{:016x}", mix(i as u64)));
        }),
        ("BIGINT NULL", ColType::of(TypeCode::I64), true, |r, i| {
            match i % 10 {
                0 => r.put_null(),
                _ => r.put_int(mix(i as u64) as i64 as u128),
            };
        }),
        ("DECIMAL", ColType::decimal(2), false, |r, i| {
            r.put_int((mix(i as u64) >> 8) as i64 as u128);
        }),
        ("UUID", ColType::of(TypeCode::UUID), false, |r, i| {
            r.put_int(u128::from(mix(i as u64)) << 64 | u128::from(mix(i as u64 + 1)));
        }),
    ];
    for (name, ty, nullable, fill) in cases {
        let schema = table(ty, nullable);
        let batches = groups(&schema, fill);
        let path = dir.path().join(format!("{name}.parquet"));
        let (_, write) = counter.measure(|| {
            let mut w = create(&path, &schema).unwrap();
            for b in &batches {
                write(&mut w, std::hint::black_box(b)).unwrap();
            }
            finish(w).unwrap()
        });
        let (read_rows, read) = counter.measure(|| {
            open(&path, &schema)
                .unwrap()
                .map(|group| std::hint::black_box(group.unwrap()).len())
                .sum::<usize>()
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
