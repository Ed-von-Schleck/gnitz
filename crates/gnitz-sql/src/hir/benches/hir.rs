use super::*;
use crate::test_support::{TestCatalog, catalog, col, ncol, parse_query, table};
use gnitz_wire::TypeCode;

/// The relations every case plans against: a 64-column `wide` table, and a small
/// star of `orders`, `customers`, `regions`, `items` and `events`.
fn bench_catalog() -> TestCatalog {
    let (i, s, f) = (TypeCode::I64, TypeCode::String, TypeCode::F64);
    let mut wide = vec![col("id", i)];
    for c in 1..64 {
        let tc = [i, s, f, TypeCode::I32][c % 4];
        wide.push(if c % 3 == 0 {
            ncol(&format!("c{c}"), tc)
        } else {
            col(&format!("c{c}"), tc)
        });
    }
    catalog(vec![
        ("wide", table(1, wide, vec![0])),
        (
            "orders",
            table(
                2,
                vec![
                    col("id", i),
                    col("cust", i),
                    col("amt", i),
                    col("ts", i),
                    ncol("status", i),
                ],
                vec![0],
            ),
        ),
        (
            "customers",
            table(3, vec![col("id", i), col("region", i), ncol("name", s)], vec![0]),
        ),
        ("regions", table(4, vec![col("id", i), col("name", s)], vec![0])),
        (
            "items",
            table(
                5,
                vec![col("id", i), col("oid", i), col("qty", i), ncol("price", f)],
                vec![0],
            ),
        ),
        (
            "events",
            table(6, vec![col("id", i), col("k", i), col("lo", i), col("hi", i)], vec![0]),
        ),
    ])
}

/// `WITH c1 AS (…), …, c8 AS (…)`: each CTE reads the one before it, and the body
/// joins the last against the first.
fn cte_chain() -> String {
    let mut sql = String::from("WITH c1 AS (SELECT id, cust, amt FROM orders WHERE amt > 0)");
    for k in 2..=8 {
        sql += &format!(
            ", c{k} AS (SELECT id, cust, amt + {k} AS amt FROM c{} WHERE amt < {})",
            k - 1,
            1000 * k
        );
    }
    sql + " SELECT c8.id, c8.amt, c1.cust FROM c8 JOIN c1 ON c8.id = c1.id"
}

/// A named statement to plan.
type Case = (&'static str, String);

/// The cases: the view bodies, then the ad-hoc SELECTs.
fn cases() -> (Vec<Case>, Vec<Case>) {
    let wide_cols = (1..64).map(|c| format!("c{c}")).collect::<Vec<_>>().join(", ");
    let wide_filter = (1..64)
        .filter(|c| (c % 8 == 0 || c % 8 == 2) && *c < 48)
        .map(|c| format!("c{c} > {c}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let views: Vec<Case> = vec![
        ("project 2 of orders", "SELECT id, amt FROM orders".into()),
        (
            "filter + map",
            "SELECT id, amt * 2 + 1 AS a2 FROM orders WHERE status = 1 AND amt > 10".into(),
        ),
        ("wide SELECT *", "SELECT * FROM wide".into()),
        (
            "wide project all + filter",
            format!("SELECT id, {wide_cols} FROM wide WHERE {wide_filter}"),
        ),
        (
            "join 2",
            "SELECT o.id, o.amt, c.name FROM orders o JOIN customers c ON o.cust = c.id".into(),
        ),
        (
            "join 4 + WHERE",
            "SELECT o.id, c.name, r.name AS rname, i.qty FROM orders o \
             JOIN customers c ON o.cust = c.id JOIN regions r ON c.region = r.id \
             JOIN items i ON i.oid = o.id WHERE o.amt > 10 AND r.name = 'eu' AND i.qty > 1"
                .into(),
        ),
        (
            "LEFT JOIN",
            "SELECT o.id, o.amt, c.name FROM orders o LEFT JOIN customers c ON o.cust = c.id".into(),
        ),
        (
            "band join",
            "SELECT o.id, e.id AS eid FROM orders o JOIN events e \
             ON o.cust = e.k AND o.ts >= e.lo AND o.ts < e.hi"
                .into(),
        ),
        (
            "EXISTS",
            "SELECT id FROM orders o WHERE EXISTS (SELECT 1 FROM items i WHERE i.oid = o.id)".into(),
        ),
        (
            "NOT EXISTS",
            "SELECT id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM items i WHERE i.oid = o.id)".into(),
        ),
        (
            "IN",
            "SELECT id FROM orders WHERE cust IN (SELECT id FROM customers WHERE region = 3)".into(),
        ),
        (
            "NOT IN",
            "SELECT id FROM orders WHERE cust NOT IN (SELECT id FROM customers)".into(),
        ),
        (
            "scalar subquery",
            "SELECT id, (SELECT SUM(qty) FROM items i WHERE i.oid = o.id) AS q FROM orders o".into(),
        ),
        (
            "GROUP BY + HAVING",
            "SELECT cust, SUM(amt) AS s, COUNT(*) AS n, MAX(ts) AS m FROM orders \
             GROUP BY cust HAVING SUM(amt) > 100"
                .into(),
        ),
        (
            "GROUP BY computed keys",
            "SELECT cust % 16 AS b, ts / 3600 AS h, AVG(amt) AS a, MIN(amt) AS lo FROM orders \
             GROUP BY cust % 16, ts / 3600"
                .into(),
        ),
        (
            "ROW_NUMBER + QUALIFY",
            "SELECT id, cust, amt, ROW_NUMBER() OVER (PARTITION BY cust ORDER BY amt DESC) AS rn \
             FROM orders QUALIFY rn <= 3"
                .into(),
        ),
        (
            "RANK",
            "SELECT id, cust, RANK() OVER (PARTITION BY cust ORDER BY amt) AS rk FROM orders".into(),
        ),
        (
            "running SUM",
            "SELECT id, cust, SUM(amt) OVER (PARTITION BY cust ORDER BY id) AS run FROM orders".into(),
        ),
        (
            "whole-partition SUM",
            "SELECT id, cust, SUM(amt) OVER (PARTITION BY cust) AS tot FROM orders".into(),
        ),
        (
            "UNION/EXCEPT/INTERSECT",
            "SELECT cust FROM orders UNION SELECT id FROM customers \
             EXCEPT SELECT oid FROM items INTERSECT SELECT k FROM events"
                .into(),
        ),
        ("CTE chain 8", cte_chain()),
        ("DISTINCT", "SELECT DISTINCT cust, status FROM orders".into()),
        (
            "ORDER BY LIMIT",
            "SELECT id, amt FROM orders ORDER BY amt DESC LIMIT 10".into(),
        ),
    ];
    let adhoc: Vec<Case> = vec![
        ("point lookup", "SELECT * FROM orders WHERE id = 7".into()),
        ("project 2 + filter", "SELECT id, amt FROM orders WHERE amt > 10".into()),
        ("wide SELECT *", "SELECT * FROM wide".into()),
        (
            "wide project all + filter",
            format!("SELECT id, {wide_cols} FROM wide WHERE {wide_filter}"),
        ),
        (
            "ORDER BY LIMIT",
            "SELECT id, amt FROM orders ORDER BY amt DESC LIMIT 10".into(),
        ),
        (
            "GROUP BY fold",
            "SELECT cust, SUM(amt), COUNT(*) FROM orders GROUP BY cust HAVING COUNT(*) > 1".into(),
        ),
        ("DISTINCT", "SELECT DISTINCT cust FROM orders".into()),
        (
            "CTE chain 4",
            "WITH a AS (SELECT id, amt FROM orders WHERE amt > 1), b AS (SELECT id, amt + 1 AS amt FROM a), \
             c AS (SELECT id, amt FROM b WHERE amt < 100), d AS (SELECT id, amt * 2 AS amt FROM c) \
             SELECT id, amt FROM d"
                .into(),
        ),
    ];
    (views, adhoc)
}

/// The median of `n` measurements of `f`.
fn median(counter: &gnitz_foundation::perf::Counter, n: usize, mut f: impl FnMut()) -> u64 {
    let mut runs: Vec<u64> = (0..n).map(|_| counter.measure(&mut f).1).collect();
    runs.sort_unstable();
    runs[n / 2]
}

/// Instructions to plan one statement, per case: a view body through
/// `bind_and_lower`, an ad-hoc SELECT through the client's whole read plan, each from its parsed
/// AST, the result dropped inside the measurement. The parse is measured alongside
/// for scale and is not part of the plan. An ad-hoc SELECT is also taken from the
/// thread's select cache, its text and its kept plan, in place of both.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hir_plan_bench() {
    const RUNS: usize = 31;
    let cat = bench_catalog();
    let counter = gnitz_foundation::perf::Counter::instructions();
    let (views, adhoc) = cases();
    let parse_cost = |sql: &str| {
        median(&counter, RUNS, || {
            std::hint::black_box(parse_query(sql));
        })
    };
    for (name, sql) in &views {
        let query = parse_query(sql);
        let plan = || {
            let view = bind::ViewBody {
                stmt: "CREATE VIEW",
                replacing: None,
                bounded: false,
            };
            bind_and_lower(&cat, &query, view, std::iter::empty()).unwrap_or_else(|e| panic!("{name}: {e:?}"))
        };
        let segments = plan().segments.len();
        let instr = median(&counter, RUNS, || {
            std::hint::black_box(plan());
        });
        println!(
            "hir_plan_bench view  {name:<26} {instr:>9} instr  ({segments} segments; parse {:>8})",
            parse_cost(sql)
        );
    }
    for (name, sql) in &adhoc {
        let query = parse_query(sql);
        let plan = || crate::dml::plan_read(&query, &cat).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        plan();
        let instr = median(&counter, RUNS, || {
            std::hint::black_box(plan());
        });
        let cached = || match crate::select_cache::parse(sql).unwrap() {
            crate::select_cache::Script::Select(select) => select.plan(&cat).unwrap(),
            crate::select_cache::Script::Statements(_) => panic!("{name}: not kept"),
        };
        // Its first run is noted, its second kept.
        for _ in 0..2 {
            let _ = crate::select_cache::parse(sql).unwrap();
        }
        cached();
        let cached = median(&counter, RUNS, || {
            std::hint::black_box(cached());
        });
        println!(
            "hir_plan_bench adhoc {name:<26} {instr:>9} instr  (parse {:>8}; cached {cached:>6})",
            parse_cost(sql)
        );
    }
}
