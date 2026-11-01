use super::*;
use crate::test_support::{parse_stmt, xorshift};
use gnitz_core::BatchAppender;
use sqlparser::ast::Statement;
use std::hint::black_box;

/// Instructions per row `apply_set` rewrites, over `t (id PK, i1..i8 BIGINT, t1, t2 TEXT)`:
/// a fixed-width column copied from another, and a string literal, which rebuilds the
/// other string column onto the new arena.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn apply_set_bench() {
    const ROWS: usize = 100_000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let mut columns = vec![ColumnDef::new("id", TypeCode::U64, false)];
    columns.extend((1..=8).map(|i| ColumnDef::new(format!("i{i}"), TypeCode::I64, false)));
    columns.extend((1..=2).map(|i| ColumnDef::new(format!("t{i}"), TypeCode::String, false)));
    let schema = Schema::from_parts(columns, &[0]).unwrap();
    let mut rows = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut rows);
    let mut state = 0x9E37_79B9_7F4A_7C15;
    for i in 0..ROWS {
        let r = xorshift(&mut state);
        app.add_row(i as u128, 1);
        for _ in 0..8 {
            app.int_val(r as i64);
        }
        for _ in 0..2 {
            app.str_val(if r & 1 == 0 {
                "short"
            } else {
                "a string long enough to spill out of line"
            });
        }
    }
    for (name, sql) in [
        ("copy a fixed column", "UPDATE t SET i1 = i2"),
        ("string literal", "UPDATE t SET t1 = 'x'"),
    ] {
        let Statement::Update(u) = parse_stmt(sql) else {
            panic!("`{sql}` is an UPDATE")
        };
        let mut set = bind_set_list(&u.assignments, &schema, "t", SetClause::Update).unwrap();
        black_box(apply_set(&mut set, rows.clone(), None, &schema).unwrap());
        // `apply_set` consumes its batch, so the clone it is handed is measured and subtracted.
        let (copy, clone) = counter.measure(|| rows.clone());
        let (out, total) = counter.measure(|| apply_set(&mut set, rows.clone(), None, &schema).unwrap());
        black_box((copy, out));
        println!(
            "apply_set_bench {name:<20} {:7.1} instr/row",
            (total - clone) as f64 / ROWS as f64
        );
    }
}
