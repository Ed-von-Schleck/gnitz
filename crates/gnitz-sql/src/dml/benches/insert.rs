use super::*;
use crate::test_support::{catalog, col, ncol, parse_stmt, table};
use gnitz_wire::TypeCode;
use sqlparser::ast::Statement;
use std::hint::black_box;

/// Instructions per VALUES row `plan_insert` encodes, over `t (id PK, a BIGINT, b BIGINT,
/// s TEXT)`, for rows in schema order and for a column list naming them out of order.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn plan_insert_bench() {
    const ROWS: usize = 10_000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let i = TypeCode::I64;
    let cols = vec![col("id", i), ncol("a", i), ncol("b", i), ncol("s", TypeCode::String)];
    let cat = catalog(vec![("t", table(1, cols, vec![0]))]);
    let cases = [
        (
            "schema order",
            "INSERT INTO t VALUES",
            (|r| format!("({r}, {r}, -{r}, 'a string long enough to spill')")) as fn(usize) -> String,
        ),
        ("column list", "INSERT INTO t (s, b, a, id) VALUES", |r| {
            format!("('short', -{r}, {r}, {r})")
        }),
    ];
    for (name, head, row) in cases {
        let rows: Vec<String> = (0..ROWS).map(row).collect();
        let Statement::Insert(insert) = parse_stmt(&format!("{head} {}", rows.join(", "))) else {
            panic!("an INSERT")
        };
        black_box(plan_insert(&insert, &cat).unwrap());
        let (plan, total) = counter.measure(|| plan_insert(&insert, &cat).unwrap());
        black_box(plan);
        println!(
            "plan_insert_bench {name:<14} {:7.1} instr/row",
            total as f64 / ROWS as f64
        );
    }
}
