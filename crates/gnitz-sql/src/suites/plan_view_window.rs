//! The window desugar through the whole planner: every accepted shape binds
//! *and* lowers to a segment chain, and every rejection names the clause the
//! user wrote.

use super::*;

/// `t(id BIGINT PK, k BIGINT, a BIGINT, b BIGINT NULL, s TEXT, f DOUBLE)`,
/// `u(uid BIGINT PK, k BIGINT)`, `st`, a stream with `t`'s columns, `x(id
/// BIGINT PK, a BIGINT, big UINT128)`, `wk(big UINT128 PK, k BIGINT, a BIGINT)` and
/// `td(id BIGINT PK, dt DATE, a BIGINT)`.
fn cat() -> TestCatalog {
    let i = TypeCode::I64;
    let t_cols = || {
        vec![
            col("id", i),
            col("k", i),
            col("a", i),
            ncol("b", i),
            col("s", TypeCode::String),
            col("f", TypeCode::F64),
        ]
    };
    catalog(vec![
        ("t", table(1, t_cols(), vec![0])),
        ("u", table(2, vec![col("uid", i), col("k", i)], vec![0])),
        // A stream's PK is a routing and sort key, never unique.
        ("st", rel(3, RelClass::Stream, t_cols(), vec![0], vec![])),
        (
            "x",
            table(4, vec![col("id", i), col("a", i), col("big", TypeCode::U128)], vec![0]),
        ),
        (
            "wk",
            table(5, vec![col("big", TypeCode::U128), col("k", i), col("a", i)], vec![0]),
        ),
        (
            "td",
            table(6, vec![col("id", i), col("dt", TypeCode::Date), col("a", i)], vec![0]),
        ),
    ])
}

/// The final view's visible output columns as `(name, type, nullable)`.
fn visible(chain: &PlannedChain) -> Vec<(String, TypeCode, bool)> {
    let cols = &final_view(chain).schema.columns();
    cols.iter()
        .filter(|c| !c.is_hidden)
        .map(|c| (c.name.clone(), c.ty.tc, c.is_nullable))
        .collect()
}

fn c(name: &str, tc: TypeCode, nullable: bool) -> (String, TypeCode, bool) {
    (name.to_string(), tc, nullable)
}

#[test]
fn a_window_call_types_and_names_like_the_aggregate_it_is() {
    let cat = cat();
    let i = TypeCode::I64;
    for (body, want) in [
        (
            "SELECT id, COUNT(*) OVER (), SUM(b) OVER (), AVG(a) OVER (), MIN(b) OVER (PARTITION BY k), \
             RANK() OVER (ORDER BY a), ROW_NUMBER() OVER (PARTITION BY k ORDER BY a DESC), MAX(b) OVER () FROM t",
            vec![
                c("id", i, false),
                c("_count1", i, false),
                c("_sum2", i, true),
                c("_avg3", TypeCode::F64, false),
                c("_min4", i, true),
                c("_rank5", i, false),
                c("_row_number6", i, false),
                c("_max7", i, true),
            ],
        ),
        // A windowed MIN/MAX selects a row, so a TEXT argument survives the
        // desugar's join and reduce at its source type: the join-key rules bind
        // the partition and order keys, not the aggregate's own argument.
        (
            "SELECT id, MIN(s) OVER () AS m FROM t",
            vec![c("id", i, false), c("m", TypeCode::String, false)],
        ),
        (
            "SELECT id, MAX(s) OVER (PARTITION BY k) AS m FROM t",
            vec![c("id", i, false), c("m", TypeCode::String, false)],
        ),
        // A computed DATE is stored through a range check into its 4-byte slot,
        // which is NULL for a day count past it.
        (
            "SELECT id, MIN(dt + a) OVER () AS m, MIN(dt) OVER () AS n FROM td",
            vec![
                c("id", i, false),
                c("m", TypeCode::Date, true),
                c("n", TypeCode::Date, false),
            ],
        ),
    ] {
        assert_eq!(visible(&view(&cat, body)), want, "{body}");
    }
}

#[test]
fn every_supported_window_shape_lowers() {
    let cat = cat();
    for body in [
        // Cumulative frames, default and explicit, and the two whole-partition ones.
        "SELECT id, SUM(a) OVER (PARTITION BY k ORDER BY id) AS running FROM t",
        "SELECT id, SUM(a) OVER (ORDER BY id) AS running FROM t",
        "SELECT id, RANK() OVER (PARTITION BY k ORDER BY a) AS r, DENSE_RANK() OVER (PARTITION BY k ORDER BY a) AS d FROM t",
        "SELECT id, RANK() OVER (ORDER BY a DESC, id) AS r FROM t",
        "SELECT id, ROW_NUMBER() OVER () AS rn FROM t",
        "SELECT SUM(a) AS s, ROW_NUMBER() OVER () AS rn FROM t",
        "SELECT id, AVG(b) OVER (PARTITION BY k ORDER BY a) AS av, MAX(a) OVER (PARTITION BY k ORDER BY a) AS mx FROM t",
        "SELECT id, COUNT(b) OVER (PARTITION BY k ORDER BY a, s) AS c FROM t",
        "SELECT id, SUM(a) OVER (PARTITION BY k ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM t",
        "SELECT id, SUM(a) OVER (PARTITION BY k ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS c FROM t",
        // A WHERE, a computed key, a subquery, a join, a grouped or DISTINCT body.
        "SELECT id, SUM(a) OVER (PARTITION BY k) AS total FROM t WHERE a > 1",
        "SELECT id, SUM(a) OVER (PARTITION BY k % 10) AS total FROM t",
        "SELECT id, a * 2 AS a2, RANK() OVER (ORDER BY a * 2) AS r FROM t",
        "SELECT id, (SELECT COUNT(*) FROM u WHERE u.k = t.k) AS c, RANK() OVER (ORDER BY a) AS r FROM t",
        "SELECT t.id, u.uid, SUM(t.a) OVER (PARTITION BY u.uid) AS total FROM t JOIN u ON t.k = u.k",
        "SELECT k, SUM(a) AS total, RANK() OVER (ORDER BY SUM(a) DESC) AS r FROM t GROUP BY k",
        "SELECT k, SUM(a) * 100 / SUM(SUM(a)) OVER () AS pct FROM t GROUP BY k",
        "SELECT k, COUNT(*) AS n, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn FROM t GROUP BY k HAVING COUNT(*) > 1",
        "SELECT DISTINCT k, SUM(a) OVER (PARTITION BY k) AS total FROM t",
        "WITH c AS (SELECT id, k, a FROM t WHERE a > 0) SELECT id, RANK() OVER (PARTITION BY k ORDER BY a) AS r FROM c",
        "SELECT * FROM (SELECT id, k, a, RANK() OVER (PARTITION BY k ORDER BY a) AS r FROM t) d WHERE r <= 3",
        // A CTE whose body is windowed compiles rather than aliasing the source.
        "WITH c AS (SELECT id, k FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY a) = 1) SELECT id FROM c",
        // QUALIFY on window values and on SELECT aliases.
        "SELECT id, k, RANK() OVER (PARTITION BY k ORDER BY a DESC) AS r FROM t QUALIFY r <= 3 AND a > 0",
        "SELECT k, SUM(a) AS total FROM t GROUP BY k QUALIFY RANK() OVER (ORDER BY SUM(a) DESC) <= 2",
        "SELECT id, k, a FROM t WINDOW w AS (PARTITION BY k ORDER BY a) QUALIFY RANK() OVER w = 1",
        // Named windows, one defined in terms of another.
        "SELECT id, RANK() OVER w AS r, SUM(a) OVER w AS s FROM t WINDOW w AS (PARTITION BY k ORDER BY a)",
        "SELECT id, RANK() OVER w2 AS r FROM t WINDOW w AS (PARTITION BY k ORDER BY a), w2 AS w",
        // A division by a non-zero literal is never NULL; a string is fine as a
        // hash key and as a residual key, a 128-bit value as the first ORDER BY key.
        "SELECT id, RANK() OVER (ORDER BY a / 2) FROM t",
        "SELECT id, COUNT(*) OVER (PARTITION BY s) FROM t",
        "SELECT id, RANK() OVER (ORDER BY a, s) FROM t",
        "SELECT id, RANK() OVER (ORDER BY big) FROM x",
        // A bare `EXISTS (SELECT …)` ignores its select list by definition, so a
        // window call written there is never bound.
        "SELECT t.id FROM t WHERE EXISTS (SELECT ROW_NUMBER() OVER (ORDER BY u.k) FROM u WHERE u.k = t.k)",
        // RANK needs no row key; ROW_NUMBER keeps the left key through a join
        // equating the right side's key, a decorrelated EXISTS, and a derived
        // table keeping it.
        "SELECT t.id, RANK() OVER (ORDER BY t.a) FROM t JOIN u ON t.k = u.k",
        "SELECT id, RANK() OVER (ORDER BY a) FROM st",
        "SELECT t.id, ROW_NUMBER() OVER (ORDER BY t.a) FROM t JOIN u ON t.k = u.uid",
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM (SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.k)) d",
        "SELECT a, ROW_NUMBER() OVER (ORDER BY a) FROM (SELECT id, a, k FROM t) d",
        // Two calls one physical aggregate column serves, and a ROW_NUMBER beside
        // an aggregate of its specification.
        "SELECT id, COUNT(*) OVER (ORDER BY a), COUNT(a) OVER (ORDER BY a) FROM t",
        "SELECT id, COUNT(*) OVER (ORDER BY a), COUNT(b) OVER (ORDER BY a), SUM(b) OVER (ORDER BY a) FROM t",
        "SELECT id, RANK() OVER w AS r, AVG(a) OVER w AS av FROM t WINDOW w AS (ORDER BY a)",
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id), COUNT(a) OVER (ORDER BY id) FROM t",
        // A ranking function ignores its frame.
        "SELECT id, RANK() OVER (PARTITION BY k ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
        "SELECT id, RANK() OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        // A key the partition or an earlier key fixes orders nothing.
        "SELECT id, RANK() OVER (PARTITION BY k ORDER BY k) AS r, SUM(a) OVER (PARTITION BY k ORDER BY k) AS s FROM t",
        "SELECT id, RANK() OVER (ORDER BY a, a) AS r, SUM(a) OVER (ORDER BY a, a) AS s FROM t",
        "SELECT id, SUM(a) OVER (PARTITION BY k, k) AS s FROM t",
        // A window call in ORDER BY alone, and one that is also an item.
        "SELECT id FROM t ORDER BY RANK() OVER (ORDER BY a) LIMIT 3",
        "SELECT id, RANK() OVER (ORDER BY a) AS r FROM t ORDER BY RANK() OVER (ORDER BY a) LIMIT 3",
        "SELECT k, SUM(a) AS s FROM t GROUP BY k ORDER BY RANK() OVER (ORDER BY SUM(a)) LIMIT 3",
    ] {
        view(&cat, body);
    }
}

/// A QUALIFY-bounded ROW_NUMBER nothing else reads is a top-N, which takes the
/// keys a top-N takes: a nullable, string or float order key, a nullable
/// partition, a 128-bit tiebreak, and an input with no unique key.
#[test]
fn a_bounded_row_number_takes_a_top_ns_keys() {
    let cat = cat();
    for over in [
        "FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY b) = 1",
        "FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY s) = 1",
        "FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY f) = 1",
        "FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY b ORDER BY a NULLS FIRST) = 1",
        "FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY s ORDER BY a DESC) <= 3",
        "FROM wk QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY a DESC) = 1",
        "FROM t JOIN u ON t.k = u.k QUALIFY ROW_NUMBER() OVER (PARTITION BY u.uid ORDER BY t.a) = 1",
        "FROM st QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY a) = 1",
        "FROM (SELECT a FROM t UNION ALL SELECT a FROM t) d QUALIFY ROW_NUMBER() OVER (ORDER BY a) <= 1",
        // Any slot, not only the first.
        "FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY a) = 2",
        // With no ORDER BY the unique key orders the partition; where the partition
        // holds that key no order is left, and every row is numbered 1.
        "FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY k) = 1",
        "FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY id) <= 1",
        // Beside another window, and beside another QUALIFY conjunct.
        "FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY a) <= 2 AND SUM(a) OVER (PARTITION BY k) > 10",
        "FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY a) <= 2 AND a > 0",
    ] {
        view(&cat, &format!("SELECT a {over}"));
    }
    for (body, needle) in [
        (
            "SELECT id FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY f ORDER BY a) = 1",
            "window PARTITION BY: a float-valued expression",
        ),
        // No ORDER BY and no unique key: nothing orders the partition.
        (
            "SELECT id FROM st QUALIFY ROW_NUMBER() OVER (PARTITION BY k) = 1",
            "ROW_NUMBER needs an input with a unique row key",
        ),
        // A selected number is a value, which the join-keyed shape computes.
        (
            "SELECT id, ROW_NUMBER() OVER (ORDER BY b) AS rn FROM t QUALIFY rn = 1",
            "window ORDER BY: the key must be provably NOT NULL",
        ),
        (
            "SELECT big, ROW_NUMBER() OVER (ORDER BY a) AS rn FROM wk QUALIFY rn = 1",
            "ROW_NUMBER's tiebreak (the input's row key): a 128-bit key",
        ),
    ] {
        assert_rejects(body, plan(&cat, &format!("CREATE VIEW v AS {body}")), needle);
    }
}

#[test]
fn every_unsupported_window_names_its_clause() {
    let cat = cat();
    for (body, needle) in [
        (
            "SELECT id, RANK() OVER w AS r FROM t",
            "not defined in the WINDOW clause",
        ),
        (
            "SELECT id, RANK() OVER w AS r FROM t WINDOW w AS w2, w2 AS w",
            "defined in terms of itself",
        ),
        (
            "SELECT id, RANK() OVER (w ORDER BY a) AS r FROM t WINDOW w AS (PARTITION BY k)",
            "extending a named window",
        ),
        // The key rules, stated at bind.
        (
            "SELECT id, SUM(a) OVER (PARTITION BY b) FROM t",
            "window PARTITION BY: the key must be provably NOT NULL",
        ),
        (
            "SELECT id, SUM(a) OVER (PARTITION BY f) FROM t",
            "window PARTITION BY: a float-valued expression",
        ),
        (
            "SELECT id, RANK() OVER (ORDER BY b) FROM t",
            "window ORDER BY: the key must be provably NOT NULL",
        ),
        (
            "SELECT id, RANK() OVER (ORDER BY f) FROM t",
            "window ORDER BY: a float-valued expression",
        ),
        (
            "SELECT id, RANK() OVER (ORDER BY s) FROM t",
            "a string cannot be the first ORDER BY key",
        ),
        (
            "SELECT id, RANK() OVER (ORDER BY a / k) FROM t",
            "window ORDER BY: the key must be provably NOT NULL",
        ),
        (
            "SELECT id, RANK() OVER (ORDER BY a, big) FROM x",
            "window ORDER BY: a 128-bit key can only be the first",
        ),
        // Frames and functions outside the supported set.
        (
            "SELECT id, SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
            "ROWS / GROUPS … CURRENT ROW",
        ),
        (
            "SELECT id, SUM(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
            "only RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        ),
        (
            "SELECT id, RANK() OVER (PARTITION BY k) FROM t",
            "RANK / DENSE_RANK need an ORDER BY",
        ),
        (
            "SELECT id, RANK(a) OVER (ORDER BY a) FROM t",
            "RANK: takes no arguments",
        ),
        (
            "SELECT id, LAG(a) OVER (ORDER BY a) FROM t",
            "LAG: not supported as a window function",
        ),
        // A scalar name with an OVER reads the same at an item's top level as
        // nested.
        (
            "SELECT id, ABS(a) OVER (ORDER BY a) FROM t",
            "ABS: not supported as a window function",
        ),
        (
            "SELECT id, ABS(a) OVER (ORDER BY a) + 1 AS x FROM t",
            "ABS: not supported as a window function",
        ),
        (
            "SELECT id, SUM(DISTINCT a) OVER () FROM t",
            "window functions: DISTINCT is not supported",
        ),
        ("SELECT id, SUM(a) FILTER (WHERE a > 1) OVER () FROM t", "FILTER"),
        ("SELECT id, SUM(s) OVER () FROM t", "SUM: not supported on STRING"),
        // A window call belongs to the SELECT list and QUALIFY only.
        (
            "SELECT id FROM t WHERE SUM(a) OVER () > 1",
            "only supported in the SELECT list, QUALIFY and ORDER BY",
        ),
        (
            "SELECT k FROM t GROUP BY k HAVING RANK() OVER (ORDER BY k) = 1",
            "only supported in the SELECT list, QUALIFY and ORDER BY",
        ),
        (
            "SELECT id, SUM(RANK() OVER (ORDER BY a)) OVER () FROM t",
            "cannot be nested",
        ),
        (
            "SELECT id, SUM(a) OVER (PARTITION BY RANK() OVER (ORDER BY a)) FROM t",
            "cannot be nested",
        ),
        ("SELECT id, k FROM t QUALIFY a > 1", "QUALIFY needs a window function"),
        (
            "SELECT k FROM t GROUP BY k QUALIFY k > 1",
            "QUALIFY needs a window function",
        ),
        // A QUALIFY inside a scalar subquery is that surface's rejection, not a
        // silent drop.
        (
            "SELECT id, (SELECT COUNT(*) FROM u WHERE u.k = t.k QUALIFY RANK() OVER (ORDER BY uid) = 1) AS c FROM t",
            "QUALIFY",
        ),
        // ROW_NUMBER needs a unique row key, which a non-key join, a stream and a
        // derived table dropping the key each lose.
        (
            "SELECT t.id, ROW_NUMBER() OVER (ORDER BY t.a) FROM t JOIN u ON t.k = u.k",
            "ROW_NUMBER needs an input with a unique row key",
        ),
        (
            "SELECT id, ROW_NUMBER() OVER (ORDER BY a) FROM st",
            "ROW_NUMBER needs an input with a unique row key",
        ),
        (
            "SELECT a, ROW_NUMBER() OVER (ORDER BY a) FROM (SELECT a, k FROM t) d",
            "ROW_NUMBER needs an input with a unique row key",
        ),
        // A bag holds a row at weight above 1, which no key numbers.
        (
            "SELECT a, ROW_NUMBER() OVER (ORDER BY a) FROM (SELECT a FROM t UNION ALL SELECT a FROM t) d",
            "ROW_NUMBER needs an input with a unique row key",
        ),
        // QUALIFY names a SELECT alias only where the list has exactly one visible
        // item of that name.
        (
            "SELECT * FROM t JOIN u ON t.k = u.k QUALIFY RANK() OVER (ORDER BY t.a) = 1 AND k > 0",
            "ambiguous",
        ),
        (
            "SELECT id, RANK() OVER (ORDER BY a) AS r FROM t QUALIFY _order0 > 1 ORDER BY a + 1 LIMIT 2",
            "column '_order0' not found",
        ),
        (
            "SELECT id, RANK() OVER w FROM t WINDOW w AS (ORDER BY a), w AS (ORDER BY k)",
            "WINDOW clause defines 'w' twice",
        ),
    ] {
        assert_rejects(body, plan(&cat, &format!("CREATE VIEW v AS {body}")), needle);
    }
}

/// `ROW_NUMBER` reads the unique key off the relation, so a registered view whose
/// own lowering said its PK repeats loses it — over a stream, over a join key,
/// over a partition holding more than one slot, or over a `UNION ALL` — and no
/// key column's *name* is read: a user may name one `_join_pk`, over a table or
/// through a view's alias, and keep its unique key.
#[test]
fn row_number_reads_the_row_key_off_the_relation() {
    let cat = cat();
    cat.insert(
        &in_sn("jt"),
        table(
            10,
            vec![col("_join_pk", TypeCode::I64), col("a", TypeCode::I64)],
            vec![0],
        ),
    );
    for (tid, name, body) in [
        (20, "sv", "SELECT id, a FROM st"),
        (21, "jv", "SELECT t.id AS id, t.a AS a FROM t JOIN u ON t.k = u.k"),
        (22, "tv2", "SELECT id, a FROM t ORDER BY a LIMIT 2"),
        (23, "pv", "SELECT id, a FROM t"),
        (24, "tv1", "SELECT id, a FROM t ORDER BY a LIMIT 1"),
        (25, "av", "SELECT id AS _join_pk, a FROM t"),
        (26, "uv", "SELECT id, a FROM t UNION ALL SELECT uid, k FROM u"),
        (27, "dv", "SELECT id, a FROM t UNION SELECT uid, k FROM u"),
    ] {
        let chain = view(&cat, body);
        register(&cat, name, tid, RelClass::View, final_view(&chain));
    }
    let over = |name: &str| {
        plan(
            &cat,
            &format!("CREATE VIEW v AS SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS rn FROM {name}"),
        )
    };
    for name in ["pv", "jt", "av"] {
        over(name).unwrap_or_else(|e| panic!("{name}: {e:?}"));
    }
    for name in ["sv", "jv", "tv2", "uv"] {
        assert_rejects(name, over(name), "unique row key");
    }
    // `tv1` and `dv` keep their unique key, a hidden 128-bit column no later
    // ORDER BY key can compare.
    assert_rejects(
        "dv",
        over("dv"),
        "ROW_NUMBER's tiebreak (the input's row key): a 128-bit key",
    );
    assert_rejects(
        "tv1",
        over("tv1"),
        "ROW_NUMBER's tiebreak (the input's row key): a 128-bit key",
    );
}
