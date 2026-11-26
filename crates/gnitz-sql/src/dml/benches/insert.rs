use super::*;
use crate::test_support::{LONG, TestCatalog, catalog, col, ncol, parse_stmt, table};
use gnitz_wire::TypeCode;
use sqlparser::ast::Statement;
use std::hint::black_box;

/// `t (id PK, a BIGINT, b BIGINT, s TEXT)`.
fn cat() -> TestCatalog {
    let i = TypeCode::I64;
    let cols = vec![col("id", i), ncol("a", i), ncol("b", i), ncol("s", TypeCode::String)];
    catalog(vec![("t", table(1, cols, vec![0]))])
}

/// `INSERT INTO t VALUES` over `rows`, each its key and a string, then `tail`.
fn insert(rows: impl Iterator<Item = usize>, s: &str, tail: &str) -> Insert {
    let values: Vec<String> = rows.map(|r| format!("({r}, {r}, -{r}, '{s}')")).collect();
    let Statement::Insert(insert) = parse_stmt(&format!("INSERT INTO t VALUES {} {tail}", values.join(", "))) else {
        unreachable!("an INSERT parses as one")
    };
    insert
}

/// Instructions per VALUES row `plan_insert` encodes: a string held inline, one
/// spilled to the arena, and under ON CONFLICT with every key written twice, the
/// pass that keeps each key's first row.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn plan_insert_bench() {
    const ROWS: usize = 10_000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let cat = cat();
    for (name, insert) in [
        ("inline string", insert(0..ROWS, "short", "")),
        ("spilled string", insert(0..ROWS, LONG, "")),
        (
            "keys twice",
            insert((0..ROWS).map(|r| r / 2), "short", "ON CONFLICT DO NOTHING"),
        ),
    ] {
        black_box(plan_insert(&insert, &cat, false).unwrap());
        let (plan, total) = counter.measure(|| plan_insert(&insert, &cat, false).unwrap());
        black_box(plan);
        println!(
            "plan_insert_bench {name:<14} {:7.1} instr/row",
            total as f64 / ROWS as f64
        );
    }
}

/// Instructions per VALUES row `resolve_conflicts` merges under DO UPDATE, every
/// other key held: the unheld rows gathered, the held ones rewritten from the
/// incoming row, a string among them.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn resolve_conflicts_bench() {
    const ROWS: usize = 100_000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let insert = insert(
        0..ROWS,
        LONG,
        "ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a, s = EXCLUDED.s",
    );
    let WritePlan {
        target,
        write: Write::Upsert { rows, mut set },
    } = plan_insert(&insert, &cat(), false).unwrap()
    else {
        unreachable!("an ON CONFLICT plans an upsert")
    };
    let mut held = ZSetBatch::new(&target.schema);
    let mut app = held.builder();
    for r in (0..ROWS).step_by(2) {
        app.begin_row(r as u128, 1).put_int(0).put_int(0).put_string("held");
    }
    black_box(resolve_conflicts(rows.clone(), held.clone(), set.as_deref_mut()).unwrap());
    // `resolve_conflicts` consumes both batches, so the clones it is handed are measured and subtracted.
    let (copies, clone) = counter.measure(|| (rows.clone(), held.clone()));
    let (out, total) = counter.measure(|| resolve_conflicts(rows.clone(), held.clone(), set.as_deref_mut()).unwrap());
    black_box((copies, out));
    println!(
        "resolve_conflicts_bench {:7.1} instr/row",
        (total - clone) as f64 / ROWS as f64
    );
}
