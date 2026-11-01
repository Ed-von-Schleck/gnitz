use super::*;

/// Instructions per statement parsed under `GnitzDialect` and under
/// `GenericDialect`, over statements whose expression prefixes are all, some,
/// or none literals.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn parse_bench() {
    const REPS: usize = 2_000;
    let row = "(1, -2, 3.25, 'name', 'it''s', NULL, 42)";
    let insert = format!("INSERT INTO t VALUES {}", vec![row; 64].join(", "));
    let counter = gnitz_foundation::perf::Counter::instructions();
    for (name, sql) in [
        ("INSERT of 64 literal rows", insert.as_str()),
        (
            "SELECT, literal predicates",
            "SELECT a, b FROM t WHERE a = 1 AND b <> 'x' AND c IN (1, 2, 3)",
        ),
        (
            "SELECT, column predicates",
            "SELECT a, b FROM t WHERE a = b AND c <> d AND e IN (f, g, h)",
        ),
        (
            "CREATE VIEW, aggregate",
            "CREATE VIEW v AS SELECT k, SUM(x) AS s FROM t WHERE y > 0 GROUP BY k",
        ),
    ] {
        let per = [&GnitzDialect as &dyn Dialect, &GenericDialect {}].map(|d| {
            let (_, n) = counter.measure(|| {
                for _ in 0..REPS {
                    std::hint::black_box(Parser::parse_sql(d, std::hint::black_box(sql)).unwrap());
                }
            });
            n as f64 / REPS as f64
        });
        println!(
            "parse_bench {name:<28} gnitz {:9.0}  generic {:9.0} instr/stmt",
            per[0], per[1]
        );
    }
}
