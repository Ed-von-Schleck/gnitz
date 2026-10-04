use super::*;
use gnitz_wire::TypeCode;
use WireConflictMode::Update;

fn one_row(schema: &Schema) -> ZSetBatch {
    let mut b = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut b);
    let row = app.add_row(1, 1);
    for _ in 1..schema.columns.len() {
        row.i64_val(7);
    }
    b
}

/// Instructions to buffer a one-row batch, by column count: as a buffer's
/// first family, and as one more push to a relation the buffer keeps growing
/// under.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn txn_buffer_push_bench() {
    const ROUNDS: u64 = 2000;
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    for ncols in [4usize, 64] {
        let mut columns = vec![ColumnDef::new("pk", TypeCode::U64, false)];
        columns.extend((1..ncols).map(|i| ColumnDef::new(format!("column_{i}"), TypeCode::I64, false)));
        let schema = Arc::new(Schema { columns, pk_cols: vec![0] });
        let push = |buf: &mut TxnBuffer| {
            let batch = one_row(&schema);
            counter
                .measure(|| buf.push(1, &schema, batch, Update, BLIND).unwrap())
                .1
        };
        let mut buf = TxnBuffer::default();
        let mut first = push(&mut buf);
        for _ in 1..ROUNDS {
            first += push(&mut TxnBuffer::default());
        }
        let later: u64 = (0..ROUNDS).map(|_| push(&mut buf)).sum();
        println!(
            "txn push, {ncols} columns: new family {} / later push {} instr",
            first / ROUNDS,
            later / ROUNDS
        );
    }
}
