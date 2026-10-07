//! Every `CREATE VIEW` / `ALTER VIEW … AS` rejection, planned with no server.
//! Each test is one guard family as a table of `(sql, substring)`:
//! the substring names the rule, never the sentence.

use super::*;
use gnitz_wire::{ColType, ColumnDef, RelClass, TypeCode, PK_LIST_MAX_COLS};

/// [`base`] plus the shapes the rejections need: `w` (a second typed table),
/// `m` (integer and float payloads for grouped bodies), `p3` (a three-column
/// PK), `wide_a`/`wide_b` (one join key past the arity cap), `wl`/`wr` (33
/// columns each), `st` (a stream), `x` (a DATE and two DECIMAL columns beside an
/// integer, a float and a string), `tt` (a TIMESTAMP), `xd` (a DECIMAL at `x.qty`'s
/// scale), and two join views over duplicated names —
/// `jv`, where `id` and `v` each appear twice, and `ju`, where only `v` does.
fn cat() -> TestCatalog {
    let i = TypeCode::I64;
    let cat = base();
    let keys = || (0..=PK_LIST_MAX_COLS).map(|n| col(&format!("c{n}"), i)).collect();
    let wide = || {
        let mut cols = vec![col("id", i), col("fk", i)];
        cols.extend((0..31).map(|n| ncol(&format!("c{n}"), i)));
        cols
    };
    for (name, desc) in [
        (
            "w",
            table(
                40,
                vec![
                    col("id", i),
                    col("big", TypeCode::U128),
                    col("s", TypeCode::String),
                    col("x", TypeCode::U64),
                    col("f", TypeCode::F64),
                ],
                vec![0],
            ),
        ),
        (
            "m",
            table(
                41,
                vec![
                    col("id", i),
                    col("k", i),
                    col("a", i),
                    col("b", i),
                    col("f", TypeCode::F64),
                ],
                vec![0],
            ),
        ),
        (
            "p3",
            table(
                42,
                vec![col("a", i), col("b", i), col("c", i), col("x", i)],
                vec![0, 1, 2],
            ),
        ),
        ("wide_a", table(43, [vec![col("id", i)], keys()].concat(), vec![0])),
        ("wide_b", table(44, [vec![col("id", i)], keys()].concat(), vec![0])),
        ("wl", table(45, wide(), vec![0])),
        ("wr", table(46, wide(), vec![0])),
        (
            "st",
            rel(49, RelClass::Stream, vec![col("id", i), col("v", i)], vec![0], vec![]),
        ),
        (
            "x",
            table(
                51,
                vec![
                    col("id", i),
                    col("i", i),
                    col("f", TypeCode::F64),
                    col("s", TypeCode::String),
                    ncol("d", TypeCode::Date),
                    ColumnDef::typed("price", ColType { tc: TypeCode::Decimal, scale: 2 }, false),
                    ColumnDef::typed("qty", ColType { tc: TypeCode::Decimal, scale: 3 }, true),
                ],
                vec![0],
            ),
        ),
        (
            "tt",
            table(52, vec![col("id", i), ncol("ts", TypeCode::Timestamp)], vec![0]),
        ),
        (
            "xd",
            table(
                54,
                vec![
                    col("id", i),
                    ColumnDef::typed("q3", ColType { tc: TypeCode::Decimal, scale: 3 }, false),
                ],
                vec![0],
            ),
        ),
        (
            "w64",
            table(
                53,
                [vec![col("id", i)], (0..63).map(|n| col(&format!("c{n}"), i)).collect()].concat(),
                vec![0],
            ),
        ),
    ] {
        cat.insert(&in_sn(name), desc);
    }
    let jv = view(&cat, "SELECT * FROM t JOIN u ON t.v = u.v");
    register(&cat, "jv", 47, jv.props.into(), final_view(&jv));
    let ju = view(&cat, "SELECT * FROM t JOIN c ON t.v = c.v");
    register(&cat, "ju", 48, ju.props.into(), final_view(&ju));
    cat
}

/// Assert each `(body, substring)` row rejects as `CREATE VIEW v AS body`.
fn rejects(cat: &TestCatalog, rows: &[(&str, &str)]) {
    for (body, needle) in rows {
        assert_rejects(body, plan(cat, &format!("CREATE VIEW v AS {body}")), needle);
    }
}

#[test]
fn join_key_rules() {
    let cat = cat();
    let n = PK_LIST_MAX_COLS + 1;
    let on: Vec<String> = (0..n).map(|k| format!("wide_a.c{k} = wide_b.c{k}")).collect();
    let over_cap = format!("SELECT * FROM wide_a JOIN wide_b ON {}", on.join(" AND "));
    let at_cap = format!("SELECT * FROM wide_a JOIN wide_b ON {}", on[..n - 1].join(" AND "));
    view(&cat, &at_cap);
    // A 60-column output over a 66-column join frame: the WHERE reads six right
    // columns the projection drops.
    let (l, r) = (
        (0..=30).map(|n| format!(", wl.c{n} AS l{n}")).collect::<String>(),
        (0..=24).map(|n| format!(", wr.c{n} AS r{n}")).collect::<String>(),
    );
    let frame_over_cap = format!(
        "SELECT wl.id, wl.fk AS lfk, wr.id AS rid{l}{r} FROM wl JOIN wr ON wl.fk = wr.fk \
         WHERE (wl.c0 + wr.c25 + wr.c26 + wr.c27 + wr.c28 + wr.c29 + wr.c30) > 0"
    );
    rejects(
        &cat,
        &[
            ("SELECT * FROM ty JOIN w ON ty.f = w.f", "float"),
            ("SELECT * FROM ty JOIN w ON ty.f < w.f", "float"),
            ("SELECT a.id FROM a FULL JOIN b USING (k)", "COALESCE"),
            ("SELECT a.id FROM a NATURAL FULL JOIN b", "COALESCE"),
            (
                "SELECT a.id FROM a JOIN b USING (nope)",
                "JOIN USING: column 'nope' not found",
            ),
            (
                "SELECT a.id FROM a JOIN b USING (a.k)",
                "JOIN USING: column must be a simple identifier",
            ),
            (
                "WITH d(x BIGINT) AS (SELECT id FROM a) SELECT x FROM d",
                "CTE 'd': a type on a column alias is not supported",
            ),
            (
                "SELECT x FROM (SELECT id FROM a) AS d(x BIGINT)",
                "derived table 'd': a type on a column alias is not supported",
            ),
            ("SELECT * FROM ty JOIN a ON ty.big = a.k", "signed-256"),
            ("SELECT * FROM ty JOIN w ON ty.s = w.big", "content hash never matches"),
            (&over_cap, "join key list"),
            ("SELECT * FROM a LEFT JOIN b ON a.v <> b.w", "may be keyless"),
            ("SELECT * FROM a RIGHT JOIN b ON 1 = 1", "may be keyless"),
            ("SELECT c.v FROM c NATURAL LEFT JOIN ty", "may be keyless"),
            (
                "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.w <> a.v)",
                "may be keyless",
            ),
            (
                "SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.w <> a.v)",
                "may be keyless",
            ),
            ("SELECT * FROM a FULL JOIN b ON a.v <> b.w", "may be keyless"),
            ("SELECT * FROM ty JOIN w ON ty.s < w.s", "order-preserving"),
            ("SELECT x.id FROM x JOIN xd ON x.price = xd.q3", "same scale"),
            ("SELECT x.id FROM x JOIN xd ON x.i = xd.q3", "same scale"),
            (
                "SELECT a.id FROM a JOIN b USING (v)",
                "not found on the right of the join",
            ),
            ("SELECT a.id FROM a, LATERAL (SELECT k FROM b) d", "LATERAL"),
            ("SELECT * FROM p3 JOIN c ON p3.x < c.v", "range JOIN output PK"),
            ("SELECT * FROM wl JOIN wr ON wl.fk = wr.fk", "MAX_COLUMNS"),
            (&frame_over_cap, "MAX_COLUMNS"),
            // A range term leads with the eq prefix plus the range slot, one
            // column past the join frame's source-PK key.
            (
                "SELECT * FROM w64 WHERE EXISTS (SELECT 1 FROM b WHERE b.k = w64.c0 AND b.w < w64.c1)",
                "column limit",
            ),
            (
                "SELECT a.v AS x, b.w AS x FROM a JOIN b ON a.k = b.k",
                "duplicate column name",
            ),
        ],
    );
}

#[test]
fn outer_and_range_join_rules() {
    let cat = cat();
    // The INNER and LEFT forms the rejected orientations are measured against.
    view(
        &cat,
        "SELECT ty.id AS aid, w.id AS bid FROM ty JOIN w ON ty.big < w.big",
    );
    view(&cat, "SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.v < b.w");
    // A pure-range LEFT threshold carries the range column at the PAIR's common
    // width — 16 bytes both-sided, and for a cross-sign 8-byte pair.
    view(
        &cat,
        "SELECT ty.id AS aid, w.id AS bid FROM ty LEFT JOIN w ON ty.big < w.big",
    );
    view(&cat, "SELECT w.id AS aid, a.id AS bid FROM w LEFT JOIN a ON w.x < a.k");
    // An ON conjunct naming only the null-supplying side filters that side.
    view(&cat, "SELECT a.id AS aid FROM a LEFT JOIN b ON a.k = b.k AND b.w > 3");
    rejects(
        &cat,
        &[
            (
                "SELECT * FROM a LEFT JOIN b ON a.k = b.k AND a.v <> b.w",
                "residual ON predicate",
            ),
            // One naming only the preserved side would have to decide the null-fill.
            (
                "SELECT a.id FROM a LEFT JOIN b ON a.k = b.k AND a.v > 5",
                "residual ON predicate",
            ),
            (
                "SELECT * FROM ty JOIN w ON ty.id = w.id AND ty.s LIKE w.s",
                "LIKE pattern must be a string literal",
            ),
            (
                "SELECT * FROM ty JOIN a ON ty.id = a.id AND ty.s <> a.v",
                "both operands to be strings",
            ),
            (
                "SELECT a.id AS aid, b.id AS bid FROM a RIGHT JOIN b ON a.v < b.w",
                "pure-range RIGHT/FULL",
            ),
            (
                "SELECT a.id AS aid, b.id AS bid FROM a FULL JOIN b ON a.v < b.w",
                "pure-range RIGHT/FULL",
            ),
            (
                "SELECT ty.id AS aid, w.id AS bid FROM ty FULL JOIN w ON ty.big < w.big",
                "pure-range RIGHT/FULL",
            ),
        ],
    );
}

#[test]
fn subquery_rules() {
    let cat = cat();
    for body in [
        "SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k) AND EXISTS (SELECT 1 FROM b WHERE b.w = a.v)",
        "SELECT * FROM a WHERE EXISTS (SELECT 1 FROM a AS x WHERE x.k = a.k)",
        "SELECT * FROM a WHERE v = 1 OR EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
        // A nullable IN as a top-level conjunct is the anti-join's own semantics.
        "SELECT * FROM n WHERE k IN (SELECT k FROM b)",
        // Two negations of a nullable NOT IN are a semi-join.
        "SELECT * FROM a WHERE NOT (k NOT IN (SELECT k FROM n))",
        // Marks compose, each read through its own column.
        "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k) OR EXISTS (SELECT 1 FROM b WHERE b.w = a.v)",
        "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k) OR EXISTS (SELECT 1 FROM b WHERE b.w = a.v) \
         OR EXISTS (SELECT 1 FROM b WHERE b.id = a.id)",
        // A subquery body over a derived table carrying its own subquery.
        "SELECT id FROM (SELECT id, k FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)) d WHERE EXISTS (SELECT 1 FROM b WHERE b.w = d.k)",
        // Both bounds of a BETWEEN read the one scalar subquery, joined once.
        "SELECT id FROM a WHERE (SELECT MAX(w) FROM b) BETWEEN a.k AND a.v",
        "SELECT a.id, (SELECT COUNT(*) FROM b) FROM a",
        "SELECT a.id FROM a WHERE a.v <> (SELECT COUNT(*) FROM b)",
        "SELECT a.v FROM a WHERE a.v < ALL (SELECT w FROM b)",
        // A pure-range correlation runs the same threshold a pure-range LEFT
        // JOIN does, at the range pair's common width.
        "SELECT * FROM ty WHERE EXISTS (SELECT 1 FROM w WHERE w.big < ty.big)",
    ] {
        view(&cat, body);
    }
    rejects(
        &cat,
        &[
            ("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND a.v > 5)", "hoist it into the view's own WHERE"),
            ("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w <> a.v)", "match-existence"),
            ("SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w <> a.v)", "match-existence"),
            ("SELECT * FROM a WHERE v = 1 OR EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w <> a.v)", "match-existence"),
            ("SELECT a.id FROM a WHERE a.v < ANY (SELECT v FROM n)", "must be NOT NULL"),
            ("SELECT * FROM n WHERE k NOT IN (SELECT k FROM b)", "NOT NULL"),
            ("SELECT * FROM a WHERE k NOT IN (SELECT k FROM n)", "NOT NULL"),
            ("SELECT * FROM a WHERE (k, v) IN (SELECT k, w FROM b)", "tuple"),
            ("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.w > 5)", "needs at least one equijoin"),
            ("SELECT k, COUNT(*) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k) GROUP BY k", "GROUP BY/aggregates"),
            ("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b JOIN a AS z ON b.k = z.k WHERE b.k = a.k)", "single FROM table without JOINs"),
            ("SELECT * FROM a WHERE EXISTS (SELECT k FROM b WHERE b.k = a.k GROUP BY k)", "GROUP BY"),
            ("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND EXISTS (SELECT 1 FROM b AS z WHERE z.k = b.w))", "nested subqueries"),
            ("SELECT * FROM a WHERE k IN (SELECT k, w FROM b)", "exactly one plain column"),
            ("SELECT * FROM a AS t WHERE EXISTS (SELECT 1 FROM b AS t WHERE t.k = t.k)", "rename one"),
            ("SELECT id FROM n WHERE n.k IN (SELECT k FROM b) OR n.v = 1", "IN (SELECT …) in a mark position"),
            ("SELECT id, k IN (SELECT k FROM b) AS f FROM n", "IN (SELECT …) in a mark position"),
            ("SELECT id FROM n WHERE NOT (k IN (SELECT k FROM b))", "NOT NULL"),
            ("SELECT id FROM n WHERE (k IN (SELECT k FROM b)) IS NULL", "IN (SELECT …) in a mark position"),
            ("SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k GROUP BY b.w) FROM a", "GROUP BY"),
            ("SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k < a.k) FROM a", "range correlation"),
            ("SELECT a.id, (SELECT COUNT(*) FROM b JOIN a AS z ON b.k = z.k WHERE b.k = a.k) FROM a", "single FROM table without JOINs"),
            ("SELECT a.id FROM a JOIN b ON a.k = b.k WHERE a.v < (SELECT MAX(w) FROM b AS z)", "scalar subqueries are not supported"),
            ("SELECT a.id, (SELECT b.w FROM b WHERE b.k = a.k) FROM a", "single aggregate over its correlation group"),
            ("SELECT a.v FROM a WHERE a.k = ALL (SELECT k FROM b)", "`= ALL (SELECT …)` is not supported"),
            ("SELECT a.v FROM a WHERE a.k <> ANY (SELECT k FROM b)", "`<> ANY (SELECT …)` is not supported"),
        ],
    );
}

#[test]
fn grouped_body_rules() {
    let cat = cat();
    for body in [
        "SELECT g, COUNT(*) AS cnt FROM t GROUP BY g HAVING SUM(v) > 0",
        "SELECT id, COUNT(*) FROM t GROUP BY id HAVING COUNT(*) > 0",
        // A HAVING with no GROUP BY groups the whole relation, so an item that
        // is not a group key or an aggregate is what fails — not the clause.
        "SELECT 1 AS one FROM t HAVING SUM(v) > 1",
        "SELECT a + 1 AS x FROM m GROUP BY a",
        "SELECT s, COUNT(*) AS n FROM ty GROUP BY s",
        "SELECT id, MIN(uid) AS x, MAX(s) AS y, MIN(big) AS z FROM ty GROUP BY id",
        "SELECT id, COUNT(*) AS c FROM ty GROUP BY id HAVING MIN(s) > 'a'",
    ] {
        view(&cat, body);
    }
    rejects(
        &cat,
        &[
            ("SELECT id, SUM(s) AS x FROM ty GROUP BY id", "SUM: not supported on"),
            ("SELECT id, AVG(uid) AS x FROM ty GROUP BY id", "AVG: not supported on"),
            ("SELECT id, SUM(big) AS x FROM ty GROUP BY id", "SUM: not supported on"),
            (
                "SELECT g FROM t HAVING SUM(v) > 1",
                "column 'g' must appear in GROUP BY or an aggregate function",
            ),
            (
                "SELECT a + 1 AS x, COUNT(*) AS c FROM m",
                "column 'a' must appear in GROUP BY",
            ),
            (
                "SELECT id, COUNT(*) AS c FROM ty GROUP BY id HAVING SUM(big) > 0",
                "SUM: not supported on",
            ),
            (
                "SELECT id, SUM(v) FROM t GROUP BY id HAVING SUM(*) > 0",
                "expects plain positional arguments",
            ),
            (
                "SELECT id, AVG(v) FROM t GROUP BY id HAVING AVG(*) > 0",
                "expects plain positional arguments",
            ),
            (
                "SELECT f, COUNT(*) AS n FROM ty GROUP BY f",
                "float column 'f' cannot be a key",
            ),
            (
                "SELECT f + 1 AS x, COUNT(*) AS c FROM m GROUP BY f + 1",
                "float-valued expression cannot be a key",
            ),
            (
                "SELECT b + 1 AS x FROM m GROUP BY a",
                "GROUP BY SELECT: column 'b' must appear in GROUP BY",
            ),
            (
                "SELECT b FROM m GROUP BY a",
                "GROUP BY SELECT: column 'b' must appear in GROUP BY",
            ),
            (
                "SELECT a + 1 AS x FROM m GROUP BY a + b",
                "GROUP BY SELECT: column 'a' must appear in GROUP BY",
            ),
            (
                "SELECT a * b AS x, SUM(a * b) AS s FROM m GROUP BY k",
                "GROUP BY SELECT: column 'a' must appear in GROUP BY",
            ),
            (
                "SELECT a, COUNT(*) AS n FROM m GROUP BY a HAVING b > 0",
                "HAVING: column 'b' must appear in GROUP BY",
            ),
            (
                "SELECT a, COUNT(*) AS n FROM m GROUP BY a HAVING zzz > 0",
                "HAVING: column 'zzz' not found",
            ),
            (
                "SELECT a, COUNT(*) AS n FROM m GROUP BY a HAVING n > 0",
                "HAVING: column 'n' not found",
            ),
            (
                "SELECT a + b AS ab, COUNT(*) AS c FROM m GROUP BY a + b HAVING _pre0 > 5",
                "HAVING: column '_pre0' not found",
            ),
            (
                "SELECT k, SUM(a) AS s FROM m GROUP BY k HAVING _agg > 1",
                "HAVING: column '_agg' not found",
            ),
            ("SELECT g, SUM(v) FROM t GROUP BY t.nope", "not found"),
            (
                "SELECT k, COUNT(*) AS c FROM a GROUP BY 0",
                "GROUP BY position 0 is out of range",
            ),
            (
                "SELECT k, COUNT(*) AS c FROM a GROUP BY 5",
                "GROUP BY position 5 is out of range",
            ),
            ("SELECT k, COUNT(*) AS c FROM a GROUP BY 2", "names an aggregate"),
            ("SELECT *, COUNT(*) AS c FROM a GROUP BY 1", "names a wildcard"),
            ("SELECT v FROM t GROUP BY ALL", "GROUP BY"),
            // Every DISTINCT aggregate of a body rides one distinct set.
            ("SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM m", "same argument"),
            ("SELECT COUNT(DISTINCT a), COUNT(*) FROM m", "plain aggregate"),
            ("SELECT COUNT(DISTINCT a), SUM(a) FROM m", "plain aggregate"),
            ("SELECT COUNT(DISTINCT a), COUNT(a) FROM m", "plain aggregate"),
            // MIN/MAX rides the set only for the argument the set is built from.
            ("SELECT COUNT(DISTINCT a), MAX(b) FROM m", "plain aggregate"),
            ("SELECT COUNT(DISTINCT f) FROM m", "cannot be a key"),
            ("SELECT COUNT(DISTINCT *) FROM m", "needs a column argument"),
        ],
    );
}

/// DATE counts days and TIMESTAMP microseconds: a key copy cannot convert one to
/// the other, so neither a join key nor a set-op column pairs them.
#[test]
fn a_date_never_pairs_with_a_timestamp_key() {
    let cat = cat();
    view(
        &cat,
        "SELECT id, CAST(d AS TIMESTAMP) AS t FROM x UNION ALL SELECT id, ts FROM tt",
    );
    rejects(
        &cat,
        &[
            (
                "SELECT x.id FROM x JOIN tt ON x.d = tt.ts",
                "differ in unit (days vs microseconds)",
            ),
            (
                "SELECT x.id FROM x JOIN tt ON x.d < tt.ts",
                "differ in unit (days vs microseconds)",
            ),
            (
                "SELECT id, d FROM x UNION ALL SELECT id, ts FROM tt",
                "column 1 type mismatch",
            ),
            // Nor does either union with its storage integer.
            (
                "SELECT id, d FROM x UNION ALL SELECT id, i32c FROM ty",
                "column 1 type mismatch",
            ),
        ],
    );
}

#[test]
fn set_op_and_distinct_rules() {
    let cat = cat();
    view(&cat, "SELECT id FROM ty UNION SELECT id FROM w");
    view(&cat, "SELECT DISTINCT g FROM t");
    view(&cat, "(SELECT g FROM t WHERE g > 0) UNION SELECT g FROM t");
    // A grouped or DISTINCT set-op side becomes a hidden segment, so its clauses
    // are consumed there rather than refused.
    view(&cat, "SELECT DISTINCT v FROM t UNION ALL SELECT g FROM u GROUP BY g");
    rejects(
        &cat,
        &[
            // A DISTINCT body consumes none of these, so each is named rather
            // than dropped — DISTINCT ON included, rather than folded to DISTINCT.
            ("SELECT DISTINCT ON (v) v, id FROM t", "DISTINCT ON"),
            ("SELECT DISTINCT ON (v) v FROM t UNION SELECT id FROM t", "DISTINCT ON"),
            ("SELECT DISTINCT ON (v) v FROM t", "DISTINCT ON"),
            ("SELECT DISTINCT v FROM t GROUP BY v", "GROUP BY"),
            ("SELECT DISTINCT v FROM t HAVING v > 0", "HAVING"),
            ("SELECT DISTINCT v FROM t PREWHERE v > 5", "PREWHERE"),
            ("SELECT DISTINCT TOP 5 v FROM t", "TOP"),
            (
                "SELECT DISTINCT v FROM t QUALIFY v > 1",
                "QUALIFY needs a window function",
            ),
            (
                "SELECT f FROM ty UNION SELECT f FROM w",
                "float column 'f' cannot be a key",
            ),
            (
                "SELECT f FROM ty EXCEPT SELECT f FROM w",
                "float column 'f' cannot be a key",
            ),
            (
                "SELECT f FROM ty INTERSECT SELECT f FROM w",
                "float column 'f' cannot be a key",
            ),
            ("SELECT DISTINCT f FROM ty", "float column 'f' cannot be a key"),
            ("SELECT DISTINCT * FROM ty", "float column 'f' cannot be a key"),
            ("SELECT * FROM t UNION ALL SELECT * FROM ty", "column count mismatch"),
            ("SELECT id, g FROM t UNION ALL SELECT id, s FROM ty", "type mismatch"),
            ("SELECT price FROM x UNION SELECT qty FROM x", "type mismatch"),
            // U64 against I64 needs a 128-bit common type, past the 8-byte cap.
            ("SELECT x FROM w UNION ALL SELECT id FROM t", "type mismatch"),
            ("SELECT g FROM t UNION ALL BY NAME SELECT g FROM u", "BY NAME"),
            (
                "SELECT g AS x, v AS x FROM t UNION SELECT g AS x, v AS x FROM t",
                "duplicate column name",
            ),
            ("SELECT DISTINCT g AS x, v AS x FROM t", "duplicate column name"),
            ("(SELECT g FROM t LIMIT 1) UNION SELECT g FROM t", "LIMIT/OFFSET"),
            ("SELECT g FROM t UNION (SELECT g FROM t ORDER BY g)", "ORDER BY"),
            (
                "(WITH c AS (SELECT g FROM t) SELECT g FROM c) UNION SELECT g FROM t",
                "WITH (CTE)",
            ),
        ],
    );
}

#[test]
fn projection_and_envelope_rules() {
    let cat = cat();
    view(&cat, "SELECT id, id AS id2 FROM t");
    rejects(
        &cat,
        &[
            ("SELECT g, g FROM t", "duplicate column name"),
            ("SELECT *, * FROM t", "duplicate column name"),
            ("SELECT t.*, t.* FROM t", "duplicate column name"),
            ("SELECT * FROM t LIMIT 10", "LIMIT"),
            ("SELECT * EXCEPT (nope) FROM t", "nope"),
            ("SELECT * EXCEPT (g) RENAME (g AS v) FROM t", "excluded"),
            ("SELECT * RENAME (g AS x, g AS y) FROM t", "twice"),
            ("SELECT * RENAME (g AS v) FROM t", "duplicate column name"),
            ("SELECT * REPLACE (g + 1 AS g) FROM t", "REPLACE"),
            ("SELECT * ILIKE 'g%' FROM t", "ILIKE"),
            (
                "WITH c AS (SELECT * REPLACE (g + 1 AS g) FROM t) SELECT * FROM c",
                "REPLACE",
            ),
            ("SELECT v FROM t PREWHERE v > 5", "PREWHERE"),
            ("SELECT v, COUNT(*) FROM t PREWHERE v > 5 GROUP BY v", "PREWHERE"),
            ("SELECT v FROM t FETCH FIRST 5 ROWS ONLY", "FETCH"),
            ("SELECT v FROM t SORT BY v", "SORT BY"),
            ("SELECT id FROM t FOR UPDATE", "FOR UPDATE"),
            ("SELECT id FROM t SETTINGS max_threads = 1", "SETTINGS"),
            ("SELECT id FROM t FORMAT JSON", "FORMAT"),
        ],
    );
    // An alias list renames what the body produced; it neither disambiguates it
    // nor names more columns than it has.
    for (sql, needle) in [
        (
            "CREATE VIEW v (x, y) AS SELECT t.id, u.id FROM t JOIN u ON t.g = u.g",
            "duplicate column name",
        ),
        ("CREATE VIEW v (a, b) AS SELECT id FROM t", "column aliases"),
    ] {
        assert_rejects(sql, plan(&cat, sql), needle);
    }
}

/// A scalar expression no program can hold is refused while planning, naming
/// what is wrong with the expression rather than the node that rejected it.
#[test]
fn scalar_expression_rules() {
    let cat = cat();
    // The ROUND scales just inside the representable range.
    view(&cat, "SELECT id, ROUND(i, 15) AS a, ROUND(i, -15) AS b FROM x");
    for (expr, needle) in [
        // A view is recomputed from deltas, so a clock read would make its
        // contents depend on when a tick ran.
        ("NOW()", "non-deterministic"),
        ("CURRENT_DATE", "non-deterministic"),
        ("EXTRACT(YEAR FROM i)", "DATE or TIMESTAMP"),
        ("DATE_TRUNC('fortnight', d)", "fortnight"),
        ("DATE '2024-02-30'", "invalid DATE literal"),
        ("CAST(i AS UUID)", "UUID"),
        // An integer literal past `u64::MAX` has no register slot at all.
        ("CAST(18446744073709551616 AS BIGINT UNSIGNED)", "18446744073709551616"),
        ("CAST('abc' AS DECIMAL(5, 2))", "invalid DECIMAL("),
        // A literal past the scaled integer is refused, not wrapped at run time.
        ("CAST(1e30 AS DECIMAL(10, 2))", "value out of range"),
        // Each product adds its operands' scales, so a chain of them runs past
        // what the scaled integer can hold.
        ("qty * qty * qty * qty * qty * qty * qty", "DECIMAL scale"),
        // A string in a numeric position names the string operand.
        ("GREATEST(s, s)", "column \"s\" is a string"),
        ("-s", "column \"s\" is a string"),
        ("s + 1", "string operand"),
        ("s AND i", "a condition must be BOOLEAN"),
        ("s || 1", "expected a string value"),
        ("STRPOS(s, f)", "expected a string value"),
        ("LEFT(s, f)", "must be an integer"),
    ] {
        rejects(&cat, &[(&format!("SELECT id, {expr} AS y FROM x"), needle)]);
    }
    // Summing day counts yields a number that is not a date.
    rejects(&cat, &[("SELECT SUM(d) AS y FROM x", "DATE column")]);
    // The over-scale product is refused as itself wherever it is written — below a
    // reduce or a window as much as in the SELECT list.
    let q7 = "q3 * q3 * q3 * q3 * q3 * q3 * q3";
    for body in [
        format!("SELECT {q7} AS k, COUNT(*) AS n FROM xd GROUP BY {q7}"),
        format!("SELECT SUM({q7}) AS s FROM xd"),
        format!("SELECT id, SUM({q7}) OVER (PARTITION BY id) AS s FROM xd"),
        format!("SELECT id, COUNT(*) OVER (PARTITION BY {q7}) AS n FROM xd"),
    ] {
        let sql = format!("CREATE VIEW v AS {body}");
        assert_eq!(
            rejected(plan(&cat, &sql)),
            "DECIMAL scale 21 exceeds 18; CAST an operand to a narrower scale",
            "{body}"
        );
    }
}

#[test]
fn cte_and_derived_table_rules() {
    let cat = cat();
    rejects(
        &cat,
        &[
            (
                "WITH cte(x, y, z) AS (SELECT id, g FROM t) SELECT x FROM cte",
                "column aliases",
            ),
            (
                "WITH cte AS (SELECT * FROM t FETCH FIRST 5 ROWS ONLY) SELECT id FROM cte",
                "FETCH",
            ),
            (
                "WITH cte AS (WITH d AS (SELECT * FROM t) SELECT * FROM d) SELECT id FROM cte",
                "WITH (CTE)",
            ),
            (
                "WITH cte AS (SELECT * FROM t PREWHERE g > 5) SELECT id FROM cte",
                "PREWHERE",
            ),
            (
                "WITH _cte AS (SELECT id FROM t WHERE id > 0) SELECT id FROM _cte",
                "cannot start with '_'",
            ),
            ("SELECT * FROM _seg4096", "cannot start with '_'"),
            ("SELECT id FROM (SELECT id, v FROM t WHERE v > 10)", "needs an alias"),
        ],
    );
}

#[test]
fn top_n_rules() {
    let cat = cat();
    rejects(
        &cat,
        &[
            ("SELECT id FROM t ORDER BY v", "ORDER BY without LIMIT"),
            ("SELECT id FROM t LIMIT 3", "LIMIT without ORDER BY"),
            ("SELECT id FROM t ORDER BY v LIMIT 0", "LIMIT 0"),
            ("SELECT id FROM t OFFSET 5", "OFFSET without"),
            ("SELECT id FROM t ORDER BY v WITH FILL LIMIT 1", "WITH FILL"),
            (
                "SELECT id FROM t ORDER BY v, v + 1, v + 2, v + 3, v + 4, v + 5, v + 6, v + 7, v + 8, v + 9, v + 10, \
                 v + 11, v + 12, v + 13, v + 14, v + 15, v + 16 LIMIT 1",
                "ORDER BY has more than 16 keys",
            ),
            ("SELECT DISTINCT g FROM t ORDER BY v LIMIT 1", "selected column"),
            (
                "SELECT id FROM t UNION SELECT id FROM u ORDER BY v LIMIT 1",
                "output column or position",
            ),
        ],
    );
    // An ORDER BY key a SELECT item already computes sorts on that item: the same
    // stored output as ordering by the item's name, DISTINCT included.
    let cols = |body: &str| final_view(&view(&cat, body)).schema.columns.clone();
    for (by_expr, by_name) in [
        (
            "SELECT id, v + 1 AS w FROM t ORDER BY v + 1 LIMIT 5",
            "SELECT id, v + 1 AS w FROM t ORDER BY w LIMIT 5",
        ),
        (
            "SELECT DISTINCT v + 1 AS w FROM t ORDER BY v + 1 LIMIT 3",
            "SELECT DISTINCT v + 1 AS w FROM t ORDER BY w LIMIT 3",
        ),
    ] {
        assert_eq!(cols(by_expr), cols(by_name), "`{by_expr}`");
    }
    // The index sorts on the written keys alone.
    let i = TypeCode::I64;
    let cab = catalog(vec![(
        "cab",
        table(60, vec![col("a", i), col("b", i), col("c", i)], vec![0, 1]),
    )]);
    let chain = view(&cab, "SELECT c FROM cab ORDER BY c LIMIT 2");
    let orders: Vec<Vec<u16>> = final_view(&chain)
        .circuit
        .nodes()
        .iter()
        .filter_map(|n| match &n.op {
            OpNode::TopN { order, .. } => Some(order.iter().map(|k| k.col).collect()),
            _ => None,
        })
        .collect();
    assert_eq!(orders, [vec![2u16]]);
}

#[test]
fn ambiguous_and_hidden_column_rules() {
    let cat = cat();
    // A merged name is one column, so a third step can pair with it.
    view(&cat, "SELECT t.id AS x FROM t JOIN u USING (v) JOIN a USING (v)");
    let sv = view(&cat, "SELECT v FROM t");
    register(&cat, "sv", 50, sv.props.into(), final_view(&sv));
    rejects(
        &cat,
        &[
            ("SELECT id FROM jv", "is ambiguous"),
            ("SELECT * FROM jv WHERE id = 5", "is ambiguous"),
            ("SELECT COUNT(*) FROM jv GROUP BY id", "is ambiguous"),
            (
                "SELECT id, COUNT(*) FROM ju GROUP BY id HAVING SUM(v) > 0",
                "is ambiguous",
            ),
            ("SELECT x.k, y.v FROM a x JOIN ju y ON x.k = y.id", "is ambiguous"),
            ("SELECT _join_pk FROM jv", "not found"),
            // A qualifier naming no relation in scope is not a decoration.
            ("SELECT b.id FROM t", "not found"),
            ("SELECT id FROM t WHERE b.id = 1", "not found"),
            ("SELECT id FROM t AS x WHERE t.id = 1", "not found"),
            ("SELECT t.id FROM t JOIN t ON t.id = t.g", "used by two relations"),
            ("SELECT x.id FROM t x JOIN u X ON x.id = x.g", "used by two relations"),
            ("SELECT t.id FROM t, u AS t", "used by two relations"),
            (
                "SELECT t.id AS x FROM t JOIN u ON t.id = u.id JOIN a USING (v)",
                "'v' is ambiguous",
            ),
            // `sv` drops `t`'s PK, which rides hidden.
            ("SELECT id FROM sv", "not found"),
        ],
    );
}

#[test]
fn capacity_rules() {
    let cat = cat();
    for (clause, needle) in [
        ("WITH (capacity = 5)", "single-quoted"),
        ("WITH (capacity = 'lots')", "not a size"),
    ] {
        let sql = format!("CREATE VIEW v {clause} AS SELECT id, v FROM t");
        assert_rejects(&sql, plan(&cat, &sql), needle);
    }
    let sql = "CREATE VIEW v WITH (capacity = '1 MB', delta = '1 MB') AS SELECT id, v FROM t";
    assert_rejects(sql, plan(&cat, sql), "cannot carry a delta feed");

    // A filtered derived-table input fuses into the join's circuit, cutting nothing.
    let sql = "CREATE VIEW v WITH (capacity = '1 MB') AS \
               SELECT a.id AS aid, d.w FROM a JOIN (SELECT k, w FROM b WHERE w > 3) d ON a.k = d.k";
    plan(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
    // A CTE that only renames its table's columns reads the table in place.
    let sql = "CREATE VIEW v WITH (capacity = '1 MB') AS WITH c AS (SELECT v AS w, id FROM t) SELECT id, w FROM c";
    plan(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));

    // An inner equi-join root whose sides cut a segment of their own: the root
    // shape is eligible, but the cut would hold an unbounded copy of its rows.
    for body in [
        "SELECT x.id, y.v FROM t x JOIN t y ON x.id = y.g",
        "SELECT t.id, u.v, a.k FROM t JOIN u ON t.id = u.g JOIN a ON a.id = t.id",
        "SELECT t.id, d.s FROM t JOIN (SELECT g, SUM(v) AS s FROM u GROUP BY g) d ON t.id = d.g",
    ] {
        assert!(
            !view(&cat, body).bundle.segments.is_empty(),
            "`{body}` cuts a segment unbounded"
        );
        let sql = format!("CREATE VIEW v WITH (capacity = '1 MB') AS {body}");
        assert_rejects(&sql, plan(&cat, &sql), "more than one view");
    }

    // A bounded view is a leaf, and cannot be retargeted.
    rejects(
        &cat,
        &[
            (
                "SELECT id, v FROM bv",
                "'bv' is a capacity-bounded view; CREATE VIEW requires a relation a view can be created over \
                 (a capacity-bounded view is a leaf)",
            ),
            ("SELECT bv.id, u.v FROM bv JOIN u ON bv.id = u.g", "capacity-bounded"),
            ("SELECT id FROM t WHERE id IN (SELECT id FROM bv)", "capacity-bounded"),
            ("WITH x AS (SELECT id, v FROM bv) SELECT id FROM x", "capacity-bounded"),
        ],
    );
    let sql = "ALTER VIEW bv AS SELECT id, v FROM t WHERE v > 0";
    assert_rejects(
        sql,
        plan(&cat, sql),
        "'bv' is a capacity-bounded view; ALTER VIEW requires a view created without WITH options \
         (DROP and CREATE the view instead)",
    );
    // A fed view cannot be retargeted either: its feed would be silently dropped.
    let sql = "ALTER VIEW fv AS SELECT id, v FROM t WHERE v > 0";
    assert_rejects(
        sql,
        plan(&cat, sql),
        "'fv' is a view with a delta feed; ALTER VIEW requires a view created without WITH options",
    );
}

/// Both retarget spellings: never onto a body that reads the view itself, and
/// never over a relation that is not a view.
#[test]
fn a_retarget_never_reads_itself_nor_replaces_a_non_view() {
    let cat = cat();
    let chain = plan(&cat, "ALTER VIEW tv AS SELECT id, v FROM t").unwrap();
    assert_eq!(chain.name, in_sn("tv"));
    assert_eq!(final_view(&chain).schema.columns.len(), 2);
    for verb in ["ALTER VIEW", "CREATE OR REPLACE VIEW"] {
        for (target, body, needle) in [
            ("tv", "SELECT id, v FROM tv", "itself"),
            // A CTE is bound whether or not the body reads it.
            ("tv", "WITH x AS (SELECT id FROM tv) SELECT id, v FROM t", "itself"),
            ("t", "SELECT id, v FROM u", "is a table"),
            ("st", "SELECT id, v FROM u", "is a stream"),
        ] {
            let sql = format!("{verb} {target} AS {body}");
            assert_rejects(&sql, plan(&cat, &sql), needle);
        }
    }
    // The self-reference outranks the leaf rule a bounded view would also break.
    let sql = "CREATE OR REPLACE VIEW bv AS SELECT id, v FROM bv";
    assert_rejects(sql, plan(&cat, sql), "itself");
}

/// The statement clauses the view planners themselves turn away, on both spellings:
/// the entry point rejects them, not a guard a caller must remember to run.
#[test]
fn view_statement_rejected_clause_matrix() {
    let cat = cat();
    for (sql, needle) in [
        ("CREATE TEMPORARY VIEW v AS SELECT id FROM t", "TEMPORARY"),
        ("CREATE OR ALTER VIEW v AS SELECT id FROM t", "OR ALTER"),
        // A declared type on a view alias parses only under ClickHouseDialect;
        // the option list is the half `GenericDialect` reaches.
        (
            "CREATE VIEW v (a NOT NULL) AS SELECT id FROM t",
            "an option list on an output column alias",
        ),
        // `ALTER VIEW` retargets a body and carries no option clause, so a `WITH`
        // it accepted would be silently dropped.
        (
            "ALTER VIEW tv WITH (security_barrier = true) AS SELECT id, v FROM t",
            "WITH options",
        ),
        (
            "CREATE OR REPLACE VIEW IF NOT EXISTS v AS SELECT id FROM t",
            "opposite outcomes",
        ),
        // `CREATE TABLE` honours CLUSTER BY, so a view silently dropping it would
        // be built unclustered.
        ("CREATE VIEW v CLUSTER BY (id) AS SELECT id FROM t", "CLUSTER BY"),
        ("CREATE VIEW v COMMENT = 'x' AS SELECT id FROM t", "COMMENT"),
    ] {
        assert_rejects(sql, plan(&cat, sql), needle);
    }
    // Every gnitz view is incrementally materialized, so the keyword is accepted.
    assert!(plan(&cat, "CREATE MATERIALIZED VIEW v AS SELECT id FROM t").is_ok());
    // The column list is consumed as positional output aliases on both.
    assert!(plan(&cat, "ALTER VIEW tv (x, y) AS SELECT id, v FROM t").is_ok());
}

/// An `ALTER VIEW … AS` body reports the statement the user wrote, though its
/// rejections share `bind_select` with `CREATE VIEW`.
#[test]
fn an_alter_view_body_names_alter_view() {
    let cat = cat();
    let sql = "ALTER VIEW tv AS SELECT 1";
    assert_rejects(sql, plan(&cat, sql), "ALTER VIEW: a view body reads");
    let sql = "ALTER VIEW tv AS VALUES (1)";
    assert_rejects(sql, plan(&cat, sql), "ALTER VIEW only supports SELECT");
}
