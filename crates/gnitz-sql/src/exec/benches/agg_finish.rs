use super::tests::{plan, t};
use super::*;
use crate::test_support::{col, ncol, table};
use gnitz_core::BatchAppender;
use gnitz_wire::TypeCode;

/// Instructions per partial row of `combine` over four workers' replies, each holding a
/// group at most once.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn agg_combine_bench() {
    const REPLY: u64 = 65_536;
    const N: u64 = 4 * REPLY;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let wide = table(
        2,
        vec![
            col("a", TypeCode::U64),
            col("b", TypeCode::U64),
            col("c", TypeCode::U64),
            ncol("x", TypeCode::I64),
        ],
        vec![0, 1, 2],
    );
    let sums = "SELECT g, COUNT(*), SUM(x) FROM t GROUP BY g";
    let float = "SELECT g, COUNT(*), SUM(f) FROM t GROUP BY g";
    let extremes = "SELECT g, COUNT(*), SUM(x), MIN(x), MAX(x) FROM t GROUP BY g";
    let by_pk = "SELECT a, b, c, COUNT(*), SUM(x) FROM t GROUP BY a, b, c";
    // (label, query, relation, row → key, groups)
    for (label, sql, rel, key, groups) in [
        ("every row its own group", sums, &t(), (|i| i) as fn(u64) -> u64, N),
        ("a group on every worker", sums, &t(), |i| i % REPLY, REPLY),
        // Workers past the first hold a new group at every other row.
        (
            "kept rows in one-row runs",
            sums,
            &t(),
            |i| i % REPLY + REPLY * (i / REPLY).min(1) * (i % 2),
            REPLY * 3 / 2,
        ),
        ("float SUM", float, &t(), |i| i % REPLY, REPLY),
        ("MIN, MAX added", extremes, &t(), |i| i % REPLY, REPLY),
        ("24-byte key", by_pk, &wide, |i| i % REPLY, REPLY),
    ] {
        let f = plan(sql, rel);
        let schema = f.partial_schema.as_ref();
        let mut b = ZSetBatch::new(schema);
        let mut app = BatchAppender::new(&mut b);
        for i in 0..N {
            app.add_row_natives(&[key(i) as u128, 0, 0][..schema.pk_cols.len()], 1);
            // By row, so a group's partials differ and an extreme is replaced.
            for (_, _, c) in schema.payload_columns() {
                match c.ty.tc.is_float() {
                    true => app.f64_val((i % 1000) as f64),
                    false => app.int_val((i % 1000) as i128),
                };
            }
        }
        let (out, instructions) = counter.measure(|| f.combine(b));
        assert_eq!(out.len() as u64, groups, "{label}");
        std::hint::black_box(out);
        println!(
            "agg_combine_bench {label:<26} {:6.1} instr/row",
            instructions as f64 / N as f64
        );
    }
}
