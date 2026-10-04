use super::*;
use gnitz_wire::TypeCode;
use WireConflictMode::Update;

fn wide_schema(ncols: usize) -> Schema {
    let mut columns = vec![ColumnDef::new("pk", TypeCode::U64, false)];
    columns.extend((1..ncols).map(|i| ColumnDef::new(format!("column_{i}"), TypeCode::I64, false)));
    Schema { columns, pk_cols: vec![0] }
}

fn one_row(schema: &Schema) -> ZSetBatch {
    let mut b = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut b);
    let mut row = app.add_row(1, 1);
    for _ in 1..schema.columns.len() {
        row = row.i64_val(7);
    }
    b
}

/// Instructions to buffer a one-row batch, by column count: as a buffer's
/// first family, and as a later push to the same relation.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn txn_buffer_push_bench() {
    const ROUNDS: u64 = 2000;
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    for ncols in [4usize, 16, 64] {
        let schema = Arc::new(wide_schema(ncols));
        let (mut first, mut later) = (0, 0);
        for _ in 0..ROUNDS {
            let batch = one_row(&schema);
            let ((), instr) = counter.measure(|| {
                let mut buf = TxnBuffer::default();
                buf.push(1, &schema, batch, Update, BLIND).unwrap();
                std::hint::black_box(&buf);
            });
            first += instr;
            let mut buf = TxnBuffer::default();
            buf.push(1, &schema, one_row(&schema), Update, BLIND).unwrap();
            let batch = one_row(&schema);
            let ((), instr) = counter.measure(|| buf.push(1, &schema, batch, Update, BLIND).unwrap());
            later += instr;
        }
        println!(
            "txn push, {ncols} columns: new family {} / later push {} instr",
            first / ROUNDS,
            later / ROUNDS
        );
    }
}
