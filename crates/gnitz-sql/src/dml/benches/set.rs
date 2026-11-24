use super::*;
use crate::test_support::{col, parse_stmt};
use sqlparser::ast::Statement;
use std::hint::black_box;

/// Instructions per row `apply_set` rewrites, over `t (id PK, i1..i8 BIGINT, t1, t2 TEXT)`,
/// for each kind of right-hand side: a copied column, a literal (a string one rebuilds
/// the other string column onto the new arena), and a computed value.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn apply_set_bench() {
    const ROWS: usize = 100_000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let mut columns = vec![col("id", TypeCode::U64)];
    columns.extend((1..=8).map(|i| col(&format!("i{i}"), TypeCode::I64)));
    columns.extend((1..=2).map(|i| col(&format!("t{i}"), TypeCode::String)));
    let schema = std::sync::Arc::new(Schema::from_parts(columns, &[0]).unwrap());
    let mut rows = ZSetBatch::new(&schema);
    let mut app = rows.builder();
    let mut rng = gnitz_foundation::rng::Rng::new(0x9E37_79B9_7F4A_7C15);
    for i in 0..ROWS {
        let r = rng.next_u64();
        app.begin_row(i as u128, 1);
        for _ in 0..8 {
            app.put_int((r as i64) as u128);
        }
        for _ in 0..2 {
            app.put_string(if r & 1 == 0 {
                "short"
            } else {
                "a string long enough to spill out of line"
            });
        }
    }
    for (name, sql) in [
        ("copy a fixed column", "UPDATE t SET i1 = i2"),
        ("string literal", "UPDATE t SET t1 = 'x'"),
        ("int literal", "UPDATE t SET i1 = 7"),
        ("computed", "UPDATE t SET i1 = i2 + 1"),
        ("copy x3", "UPDATE t SET i1 = i2, i3 = i4, i5 = i6"),
    ] {
        let Statement::Update(u) = parse_stmt(sql) else {
            panic!("`{sql}` is an UPDATE")
        };
        let mut set = bind_set_list(&u.assignments, &schema, "t", SetClause::Update).unwrap();
        black_box(apply_set(&mut set, rows.clone(), None).unwrap());
        // `apply_set` consumes its batch, so the clone it is handed is measured and subtracted.
        let (copy, clone) = counter.measure(|| rows.clone());
        let (out, total) = counter.measure(|| apply_set(&mut set, rows.clone(), None).unwrap());
        black_box((copy, out));
        println!(
            "apply_set_bench {name:<20} {:7.1} instr/row",
            (total - clone) as f64 / ROWS as f64
        );
    }
}
