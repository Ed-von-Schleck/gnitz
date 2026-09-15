//! Every `CREATE VIEW` / `ALTER VIEW … AS` rejection, planned with no server.
//! Each test is one guard family as a table of `(sql, variant, substring)`:
//! the substring names the rule, never the sentence.

mod pure;

use gnitz_core::{CatalogSnapshot, ColType, ColumnDef, RelClass, TypeCode, PK_LIST_MAX_COLS};
use pure::*;

/// [`base`] plus the shapes the rejections need: `w` (a second typed table),
/// `m` (integer and float payloads for grouped bodies), `p3` (a three-column
/// PK), `wide_a`/`wide_b` (one join key past the arity cap), `wl`/`wr` (33
/// columns each), `st` (a stream), `x` (a DATE and two DECIMAL columns beside an
/// integer, a float and a string), and two join views over duplicated names —
/// `jv`, where `id` and `v` each appear twice, and `ju`, where only `v` does.
fn cat() -> CatalogSnapshot {
    let i = TypeCode::I64;
    let mut cat = base();
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
            rel(
                49,
                RelClass::Stream,
                false,
                vec![col("id", i), col("v", i)],
                vec![0],
                &[],
            ),
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
    ] {
        cat.insert(SN, name, Some(desc));
    }
    let jv = view(&cat, "SELECT * FROM t JOIN u ON t.v = u.v");
    register(&mut cat, "jv", 47, &jv);
    let ju = view(&cat, "SELECT * FROM t JOIN c ON t.v = c.v");
    register(&mut cat, "ju", 48, &ju);
    cat
}

/// Assert each `(body, variant, substring)` row rejects as `CREATE VIEW v AS body`.
fn rejects(cat: &CatalogSnapshot, rows: &[(&str, &str, &str)]) {
    for (body, variant, needle) in rows {
        assert_rejects(body, plan(cat, &format!("CREATE VIEW v AS {body}")), variant, needle);
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
    // A narrow output over a wide intermediate: every column of both sides is read,
    // but by fewer items than columns — so the join's own projection outgrows the
    // cap while the registered view does not.
    let names: Vec<String> = ["id".to_string(), "fk".to_string()]
        .into_iter()
        .chain((0..31).map(|n| format!("c{n}")))
        .collect();
    let mut items: Vec<String> = names
        .iter()
        .enumerate()
        .map(|(i, c)| format!("wl.{c} AS l{i}"))
        .collect();
    items.extend(names[..27].iter().enumerate().map(|(i, c)| format!("wr.{c} AS r{i}")));
    items.extend(
        names[27..]
            .chunks(2)
            .enumerate()
            .map(|(i, p)| format!("wr.{} + wr.{} AS x{i}", p[0], p[1])),
    );
    let wide_middle = format!("SELECT {} FROM wl JOIN wr ON wl.fk = wr.fk", items.join(", "));
    rejects(
        &cat,
        &[
            ("SELECT * FROM ty JOIN w ON ty.f = w.f", "Unsupported", "float"),
            ("SELECT * FROM ty JOIN w ON ty.f < w.f", "Unsupported", "float"),
            ("SELECT a.id FROM a FULL JOIN b USING (k)", "Unsupported", "COALESCE"),
            ("SELECT a.id FROM a NATURAL FULL JOIN b", "Unsupported", "COALESCE"),
            (
                "SELECT a.id FROM a JOIN b USING (nope)",
                "Bind",
                "JOIN USING: column 'nope' not found",
            ),
            ("SELECT * FROM ty JOIN a ON ty.big = a.k", "Unsupported", "signed-256"),
            (
                "SELECT * FROM ty JOIN w ON ty.s = w.big",
                "Unsupported",
                "content hash never matches",
            ),
            (&over_cap, "Unsupported", "equijoin key columns"),
            (
                "SELECT * FROM a LEFT JOIN b ON a.v <> b.w",
                "Unsupported",
                "may be keyless",
            ),
            ("SELECT * FROM a RIGHT JOIN b ON 1 = 1", "Unsupported", "may be keyless"),
            (
                "SELECT c.v FROM c NATURAL LEFT JOIN ty",
                "Unsupported",
                "may be keyless",
            ),
            (
                "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.w <> a.v)",
                "Unsupported",
                "may be keyless",
            ),
            (
                "SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.w <> a.v)",
                "Unsupported",
                "may be keyless",
            ),
            (
                "SELECT * FROM a FULL JOIN b ON a.v <> b.w",
                "Unsupported",
                "may be keyless",
            ),
            (
                "SELECT * FROM ty JOIN w ON ty.s < w.s",
                "Unsupported",
                "order-preserving",
            ),
            (
                "SELECT * FROM p3 JOIN c ON p3.x < c.v",
                "Unsupported",
                "range JOIN output PK",
            ),
            (
                "SELECT * FROM wl JOIN wr ON wl.fk = wr.fk",
                "Unsupported",
                "MAX_COLUMNS",
            ),
            (&wide_middle, "Unsupported", "a view circuit projection has 66 columns"),
            (
                "SELECT a.v AS x, b.w AS x FROM a JOIN b ON a.k = b.k",
                "Plan",
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
    // An ON conjunct naming only the null-supplying side filters that side.
    view(&cat, "SELECT a.id AS aid FROM a LEFT JOIN b ON a.k = b.k AND b.w > 3");
    rejects(
        &cat,
        &[
            (
                "SELECT * FROM a LEFT JOIN b ON a.k = b.k AND a.v <> b.w",
                "Unsupported",
                "residual ON predicate",
            ),
            // One naming only the preserved side would have to decide the null-fill.
            (
                "SELECT a.id FROM a LEFT JOIN b ON a.k = b.k AND a.v > 5",
                "Unsupported",
                "residual ON predicate",
            ),
            (
                "SELECT * FROM ty JOIN w ON ty.id = w.id AND ty.s LIKE w.s",
                "Unsupported",
                "LIKE pattern must be a string literal",
            ),
            (
                "SELECT * FROM ty JOIN a ON ty.id = a.id AND ty.s <> a.v",
                "Unsupported",
                "both operands to be strings",
            ),
            (
                "SELECT a.id AS aid, b.id AS bid FROM a RIGHT JOIN b ON a.v < b.w",
                "Unsupported",
                "pure-range RIGHT/FULL",
            ),
            (
                "SELECT a.id AS aid, b.id AS bid FROM a FULL JOIN b ON a.v < b.w",
                "Unsupported",
                "pure-range RIGHT/FULL",
            ),
            // Both guards apply to a 16-byte pure-range FULL; the orientation one answers.
            (
                "SELECT ty.id AS aid, w.id AS bid FROM ty FULL JOIN w ON ty.big < w.big",
                "Unsupported",
                "pure-range RIGHT/FULL",
            ),
            (
                "SELECT ty.id AS aid, w.id AS bid FROM ty LEFT JOIN w ON ty.big < w.big",
                "Unsupported",
                "needs a ≤8-byte integer range column",
            ),
            (
                "SELECT w.id AS aid, a.id AS bid FROM w LEFT JOIN a ON w.x < a.k",
                "Unsupported",
                "needs a ≤8-byte integer range column",
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
    ] {
        view(&cat, body);
    }
    rejects(
        &cat,
        &[
            ("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND a.v > 5)", "Unsupported", "hoist it into the view's own WHERE"),
            ("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w <> a.v)", "Unsupported", "match-existence"),
            ("SELECT * FROM n WHERE k NOT IN (SELECT k FROM b)", "Unsupported", "NOT NULL"),
            ("SELECT * FROM a WHERE k NOT IN (SELECT k FROM n)", "Unsupported", "NOT NULL"),
            ("SELECT * FROM a WHERE (k, v) IN (SELECT k, w FROM b)", "Unsupported", "tuple"),
            ("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.w > 5)", "Unsupported", "uncorrelated EXISTS"),
            ("SELECT * FROM ty WHERE EXISTS (SELECT 1 FROM w WHERE w.big < ty.big)", "Unsupported", "8-byte integer range column"),
            ("SELECT k, COUNT(*) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k) GROUP BY k", "Unsupported", "GROUP BY/aggregates"),
            ("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b JOIN a AS z ON b.k = z.k WHERE b.k = a.k)", "Unsupported", "single FROM table without JOINs"),
            ("SELECT * FROM a WHERE EXISTS (SELECT k FROM b WHERE b.k = a.k GROUP BY k)", "Unsupported", "GROUP BY"),
            ("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND EXISTS (SELECT 1 FROM b AS z WHERE z.k = b.w))", "Unsupported", "nested subqueries"),
            ("SELECT * FROM a WHERE k IN (SELECT k, w FROM b)", "Unsupported", "exactly one plain column"),
            ("SELECT * FROM a AS t WHERE EXISTS (SELECT 1 FROM b AS t WHERE t.k = t.k)", "Bind", "rename one"),
            ("SELECT id FROM n WHERE n.k IN (SELECT k FROM b) OR n.v = 1", "Unsupported", "IN (SELECT …) in a mark position"),
            ("SELECT id, k IN (SELECT k FROM b) AS f FROM n", "Unsupported", "IN (SELECT …) in a mark position"),
            ("SELECT id FROM n WHERE NOT (k IN (SELECT k FROM b))", "Unsupported", "NOT NULL"),
            ("SELECT id FROM n WHERE (k IN (SELECT k FROM b)) IS NULL", "Unsupported", "IN (SELECT …) in a mark position"),
            ("SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k GROUP BY b.w) FROM a", "Unsupported", "GROUP BY"),
            ("SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k < a.k) FROM a", "Unsupported", "range correlation"),
            ("SELECT a.id, (SELECT COUNT(*) FROM b JOIN a AS z ON b.k = z.k WHERE b.k = a.k) FROM a", "Unsupported", "single FROM table without JOINs"),
            ("SELECT a.id FROM a JOIN b ON a.k = b.k WHERE a.v < (SELECT MAX(w) FROM b AS z)", "Unsupported", "scalar subqueries are not supported"),
            ("SELECT a.id, (SELECT b.w FROM b WHERE b.k = a.k) FROM a", "Unsupported", "single aggregate over its correlation group"),
            ("SELECT a.v FROM a WHERE a.k = ALL (SELECT k FROM b)", "Unsupported", "`= ALL (SELECT …)` is not supported"),
            ("SELECT a.v FROM a WHERE a.k <> ANY (SELECT k FROM b)", "Unsupported", "`<> ANY (SELECT …)` is not supported"),
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
            (
                "SELECT id, SUM(s) AS x FROM ty GROUP BY id",
                "Unsupported",
                "SUM: not supported on",
            ),
            (
                "SELECT id, AVG(uid) AS x FROM ty GROUP BY id",
                "Unsupported",
                "AVG: not supported on",
            ),
            (
                "SELECT id, SUM(big) AS x FROM ty GROUP BY id",
                "Unsupported",
                "SUM: not supported on",
            ),
            (
                "SELECT g FROM t HAVING SUM(v) > 1",
                "Plan",
                "column 'g' must appear in GROUP BY or an aggregate function",
            ),
            (
                "SELECT a + 1 AS x, COUNT(*) AS c FROM m",
                "Plan",
                "column 'a' must appear in GROUP BY",
            ),
            (
                "SELECT id, COUNT(*) AS c FROM ty GROUP BY id HAVING SUM(big) > 0",
                "Unsupported",
                "SUM: not supported on",
            ),
            (
                "SELECT id, SUM(v) FROM t GROUP BY id HAVING SUM(*) > 0",
                "Unsupported",
                "requires exactly one column argument",
            ),
            (
                "SELECT id, AVG(v) FROM t GROUP BY id HAVING AVG(*) > 0",
                "Unsupported",
                "requires exactly one column argument",
            ),
            (
                "SELECT f, COUNT(*) AS n FROM ty GROUP BY f",
                "Unsupported",
                "float column 'f' cannot be a key",
            ),
            (
                "SELECT f + 1 AS x, COUNT(*) AS c FROM m GROUP BY f + 1",
                "Unsupported",
                "float-valued expression cannot be a key",
            ),
            (
                "SELECT b + 1 AS x FROM m GROUP BY a",
                "Plan",
                "GROUP BY SELECT: column 'b' must appear in GROUP BY",
            ),
            (
                "SELECT b FROM m GROUP BY a",
                "Plan",
                "GROUP BY SELECT: column 'b' must appear in GROUP BY",
            ),
            (
                "SELECT a + 1 AS x FROM m GROUP BY a + b",
                "Plan",
                "GROUP BY SELECT: column 'a' must appear in GROUP BY",
            ),
            (
                "SELECT a * b AS x, SUM(a * b) AS s FROM m GROUP BY k",
                "Plan",
                "GROUP BY SELECT: column 'a' must appear in GROUP BY",
            ),
            (
                "SELECT a, COUNT(*) AS n FROM m GROUP BY a HAVING b > 0",
                "Plan",
                "HAVING: column 'b' must appear in GROUP BY",
            ),
            (
                "SELECT a, COUNT(*) AS n FROM m GROUP BY a HAVING zzz > 0",
                "Bind",
                "HAVING: column 'zzz' not found",
            ),
            (
                "SELECT a, COUNT(*) AS n FROM m GROUP BY a HAVING n > 0",
                "Bind",
                "HAVING: column 'n' not found",
            ),
            (
                "SELECT a + b AS ab, COUNT(*) AS c FROM m GROUP BY a + b HAVING _pre0 > 5",
                "Bind",
                "HAVING: column '_pre0' not found",
            ),
            (
                "SELECT k, SUM(a) AS s FROM m GROUP BY k HAVING _agg > 1",
                "Bind",
                "HAVING: column '_agg' not found",
            ),
            ("SELECT g, SUM(v) FROM t GROUP BY t.nope", "Bind", "not found"),
            (
                "SELECT k, COUNT(*) AS c FROM a GROUP BY 0",
                "Unsupported",
                "GROUP BY position 0 is out of range",
            ),
            (
                "SELECT k, COUNT(*) AS c FROM a GROUP BY 5",
                "Unsupported",
                "GROUP BY position 5 is out of range",
            ),
            (
                "SELECT k, COUNT(*) AS c FROM a GROUP BY 2",
                "Unsupported",
                "names an aggregate",
            ),
            (
                "SELECT *, COUNT(*) AS c FROM a GROUP BY 1",
                "Unsupported",
                "names a wildcard",
            ),
            ("SELECT v FROM t GROUP BY ALL", "Unsupported", "GROUP BY"),
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
            ("SELECT DISTINCT ON (v) v, id FROM t", "Unsupported", "DISTINCT ON"),
            (
                "SELECT DISTINCT ON (v) v FROM t UNION SELECT id FROM t",
                "Unsupported",
                "DISTINCT ON",
            ),
            ("SELECT DISTINCT ON (v) v FROM t", "Unsupported", "DISTINCT ON"),
            ("SELECT DISTINCT v FROM t GROUP BY v", "Unsupported", "GROUP BY"),
            ("SELECT DISTINCT v FROM t HAVING v > 0", "Unsupported", "HAVING"),
            ("SELECT DISTINCT v FROM t PREWHERE v > 5", "Unsupported", "PREWHERE"),
            ("SELECT DISTINCT TOP 5 v FROM t", "Unsupported", "TOP"),
            (
                "SELECT DISTINCT v FROM t QUALIFY v > 1",
                "Unsupported",
                "QUALIFY needs a window function",
            ),
            (
                "SELECT f FROM ty UNION SELECT f FROM w",
                "Unsupported",
                "float column 'f' cannot be a key",
            ),
            (
                "SELECT f FROM ty EXCEPT SELECT f FROM w",
                "Unsupported",
                "float column 'f' cannot be a key",
            ),
            (
                "SELECT f FROM ty INTERSECT SELECT f FROM w",
                "Unsupported",
                "float column 'f' cannot be a key",
            ),
            (
                "SELECT DISTINCT f FROM ty",
                "Unsupported",
                "float column 'f' cannot be a key",
            ),
            (
                "SELECT DISTINCT * FROM ty",
                "Unsupported",
                "float column 'f' cannot be a key",
            ),
            (
                "SELECT * FROM t UNION ALL SELECT * FROM ty",
                "Plan",
                "column count mismatch",
            ),
            (
                "SELECT id, g FROM t UNION ALL SELECT id, s FROM ty",
                "Plan",
                "type mismatch",
            ),
            // U64 against I64 needs a 128-bit common type, past the 8-byte cap.
            ("SELECT x FROM w UNION ALL SELECT id FROM t", "Plan", "type mismatch"),
            (
                "SELECT g FROM t UNION ALL BY NAME SELECT g FROM u",
                "Unsupported",
                "BY NAME",
            ),
            (
                "SELECT g AS x, v AS x FROM t UNION SELECT g AS x, v AS x FROM t",
                "Plan",
                "duplicate column name",
            ),
            ("SELECT DISTINCT g AS x, v AS x FROM t", "Plan", "duplicate column name"),
            (
                "(SELECT g FROM t LIMIT 1) UNION SELECT g FROM t",
                "Unsupported",
                "LIMIT/OFFSET",
            ),
            (
                "SELECT g FROM t UNION (SELECT g FROM t ORDER BY g)",
                "Unsupported",
                "ORDER BY",
            ),
            (
                "(WITH c AS (SELECT g FROM t) SELECT g FROM c) UNION SELECT g FROM t",
                "Unsupported",
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
            ("SELECT g, g FROM t", "Plan", "duplicate column name"),
            ("SELECT *, * FROM t", "Plan", "duplicate column name"),
            ("SELECT t.*, t.* FROM t", "Unsupported", "SELECT item"),
            ("SELECT * FROM t LIMIT 10", "Unsupported", "LIMIT"),
            ("SELECT * EXCEPT (nope) FROM t", "Bind", "nope"),
            ("SELECT * EXCEPT (g) RENAME (g AS v) FROM t", "Plan", "excluded"),
            ("SELECT * RENAME (g AS x, g AS y) FROM t", "Plan", "twice"),
            ("SELECT * RENAME (g AS v) FROM t", "Plan", "duplicate column name"),
            ("SELECT * REPLACE (g + 1 AS g) FROM t", "Unsupported", "REPLACE"),
            ("SELECT * ILIKE 'g%' FROM t", "Unsupported", "ILIKE"),
            (
                "WITH c AS (SELECT * REPLACE (g + 1 AS g) FROM t) SELECT * FROM c",
                "Unsupported",
                "REPLACE",
            ),
            ("SELECT v FROM t PREWHERE v > 5", "Unsupported", "PREWHERE"),
            (
                "SELECT v, COUNT(*) FROM t PREWHERE v > 5 GROUP BY v",
                "Unsupported",
                "PREWHERE",
            ),
            ("SELECT v FROM t FETCH FIRST 5 ROWS ONLY", "Unsupported", "FETCH"),
            ("SELECT v FROM t SORT BY v", "Unsupported", "SORT BY"),
            ("SELECT id FROM t FOR UPDATE", "Unsupported", "FOR UPDATE"),
            ("SELECT id FROM t SETTINGS max_threads = 1", "Unsupported", "SETTINGS"),
            ("SELECT id FROM t FORMAT JSON", "Unsupported", "FORMAT"),
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
        assert_rejects(sql, plan(&cat, sql), "Plan", needle);
    }
}

/// A scalar expression no program can hold is refused while planning, naming
/// what is wrong with the expression rather than the node that rejected it.
#[test]
fn scalar_expression_rules() {
    let cat = cat();
    // The ROUND scales just inside the representable range.
    view(&cat, "SELECT id, ROUND(i, 15) AS a, ROUND(i, -15) AS b FROM x");
    for (expr, variant, needle) in [
        // A view is recomputed from deltas, so a clock read would make its
        // contents depend on when a tick ran.
        ("NOW()", "Unsupported", "non-deterministic"),
        ("CURRENT_DATE", "Unsupported", "non-deterministic"),
        ("EXTRACT(YEAR FROM i)", "Unsupported", "DATE or TIMESTAMP"),
        ("DATE_TRUNC('fortnight', d)", "Unsupported", "fortnight"),
        ("DATE '2024-02-30'", "Bind", "invalid DATE literal"),
        ("CAST(i AS UUID)", "Unsupported", "UUID"),
        ("CAST(i AS BOOLEAN)", "Unsupported", "BOOLEAN"),
        // A wide integer literal has no register slot at all.
        (
            "CAST(18446744073709551615 AS BIGINT UNSIGNED)",
            "Unsupported",
            "18446744073709551615",
        ),
        ("CAST('abc' AS DECIMAL(5, 2))", "Bind", "invalid DECIMAL literal"),
        // Each product adds its operands' scales, so a chain of them runs past
        // what the scaled integer can hold.
        (
            "qty * qty * qty * qty * qty * qty * qty",
            "Unsupported",
            "DECIMAL scale",
        ),
        // A string in a numeric position names the string operand.
        ("GREATEST(s, s)", "Unsupported", "column \"s\" is a string"),
        ("-s", "Unsupported", "column \"s\" is a string"),
        ("s + 1", "Unsupported", "string operand"),
        ("s AND i", "Unsupported", "is a string"),
        ("s || 1", "Unsupported", "expected a string value"),
        ("STRPOS(s, f)", "Unsupported", "expected a string value"),
        ("LEFT(s, f)", "Unsupported", "must be an integer"),
    ] {
        rejects(&cat, &[(&format!("SELECT id, {expr} AS y FROM x"), variant, needle)]);
    }
    // Summing day counts yields a number that is not a date.
    rejects(&cat, &[("SELECT SUM(d) AS y FROM x", "Unsupported", "Date column")]);
}

#[test]
fn cte_and_derived_table_rules() {
    let cat = cat();
    rejects(
        &cat,
        &[
            (
                "WITH cte(x, y, z) AS (SELECT id, g FROM t) SELECT x FROM cte",
                "Plan",
                "column aliases",
            ),
            (
                "WITH cte AS (SELECT * FROM t FETCH FIRST 5 ROWS ONLY) SELECT id FROM cte",
                "Unsupported",
                "FETCH",
            ),
            (
                "WITH cte AS (WITH d AS (SELECT * FROM t) SELECT * FROM d) SELECT id FROM cte",
                "Unsupported",
                "WITH (CTE)",
            ),
            (
                "WITH cte AS (SELECT * FROM t PREWHERE g > 5) SELECT id FROM cte",
                "Unsupported",
                "PREWHERE",
            ),
            (
                "WITH _cte AS (SELECT id FROM t WHERE id > 0) SELECT id FROM _cte",
                "Plan",
                "cannot start with '_'",
            ),
            (
                "SELECT x FROM (SELECT id AS x FROM t) AS _seg4096",
                "Plan",
                "cannot start with '_'",
            ),
            ("SELECT * FROM _seg4096", "Plan", "cannot start with '_'"),
            (
                "SELECT id FROM (SELECT id, v FROM t WHERE v > 10)",
                "Unsupported",
                "needs an alias",
            ),
        ],
    );
}

#[test]
fn top_n_rules() {
    let cat = cat();
    rejects(
        &cat,
        &[
            ("SELECT id FROM t ORDER BY v", "Unsupported", "ORDER BY without LIMIT"),
            ("SELECT id FROM t LIMIT 3", "Unsupported", "LIMIT without ORDER BY"),
            ("SELECT id FROM t ORDER BY v LIMIT 0", "Plan", "LIMIT 0"),
            ("SELECT id FROM t OFFSET 5", "Unsupported", "OFFSET without"),
            (
                "SELECT DISTINCT g FROM t ORDER BY v LIMIT 1",
                "Unsupported",
                "selected column",
            ),
            (
                "SELECT id FROM t UNION SELECT id FROM u ORDER BY v LIMIT 1",
                "Unsupported",
                "output column or position",
            ),
        ],
    );
}

#[test]
fn ambiguous_and_hidden_column_rules() {
    let mut cat = cat();
    // A merged name is one column, so a third step can pair with it.
    view(&cat, "SELECT t.id AS x FROM t JOIN u USING (v) JOIN a USING (v)");
    let sv = view(&cat, "SELECT v FROM t");
    register(&mut cat, "sv", 50, &sv);
    rejects(
        &cat,
        &[
            ("SELECT id FROM jv", "Bind", "is ambiguous"),
            ("SELECT * FROM jv WHERE id = 5", "Bind", "is ambiguous"),
            ("SELECT COUNT(*) FROM jv GROUP BY id", "Bind", "is ambiguous"),
            (
                "SELECT id, COUNT(*) FROM ju GROUP BY id HAVING SUM(v) > 0",
                "Bind",
                "is ambiguous",
            ),
            (
                "SELECT x.k, y.v FROM a x JOIN ju y ON x.k = y.id",
                "Bind",
                "is ambiguous",
            ),
            ("SELECT _join_pk FROM jv", "Bind", "not found"),
            // A qualifier naming no relation in scope is not a decoration.
            ("SELECT b.id FROM t", "Bind", "not found"),
            (
                "SELECT t.id AS x FROM t JOIN u ON t.id = u.id JOIN a USING (v)",
                "Bind",
                "'v' is ambiguous",
            ),
            // `sv` drops `t`'s PK, which rides hidden.
            ("SELECT id FROM sv", "Bind", "not found"),
        ],
    );
}

#[test]
fn capacity_rules() {
    let cat = cat();
    for (clause, needle) in [
        ("WITH (foo = '1 MB')", "unknown CREATE VIEW option"),
        // An unknown key names every key that would have been read, so the
        // message is enough to fix the statement without opening the grammar.
        ("WITH (foo = '1 MB')", "capacity"),
        ("WITH (foo = '1 MB')", "delta"),
        ("WITH (capacity = 5)", "single-quoted"),
        ("WITH (capacity = 'lots')", "not a size"),
    ] {
        let sql = format!("CREATE VIEW v {clause} AS SELECT id, v FROM t");
        assert_rejects(&sql, plan(&cat, &sql), "Plan", needle);
    }
    let sql = "CREATE VIEW v WITH (capacity = '1 MB', delta = '1 MB') AS SELECT id, v FROM t";
    assert_rejects(sql, plan(&cat, sql), "Unsupported", "cannot carry a delta feed");

    // A filtered derived-table input fuses into the join's circuit, cutting nothing.
    let sql = "CREATE VIEW v WITH (capacity = '1 MB') AS \
               SELECT a.id AS aid, d.w FROM a JOIN (SELECT k, w FROM b WHERE w > 3) d ON a.k = d.k";
    plan(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));

    // Only a filter/projection over one relation and an inner equi-join may be
    // bounded; every other shape compiles unbounded.
    for body in [
        "SELECT g, SUM(v) AS s FROM u GROUP BY g",
        "SELECT id, v FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.g = t.id)",
        "SELECT id, v FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.g = t.id)",
        "SELECT DISTINCT v FROM t",
        "SELECT id, v FROM t UNION ALL SELECT id, v FROM u",
        "SELECT d.id FROM (SELECT id, v FROM t) d",
        "SELECT d.v FROM (SELECT DISTINCT v FROM t) d",
        "SELECT t.id, t.v + u.v AS z FROM t JOIN u ON t.id = u.g",
        "SELECT t.id, u.v FROM t LEFT JOIN u ON t.id = u.g",
        "SELECT t.id, u.v FROM t RIGHT JOIN u ON t.id = u.g",
        "SELECT t.id, u.v FROM t FULL JOIN u ON t.id = u.g",
        "SELECT t.id, u.v FROM t JOIN u ON t.id = u.g AND t.v < u.v",
        "SELECT t.id, u.v FROM t CROSS JOIN u",
        "SELECT id FROM t ORDER BY v LIMIT 1",
    ] {
        view(&cat, body);
        let sql = format!("CREATE VIEW v WITH (capacity = '1 MB') AS {body}");
        assert_rejects(&sql, plan(&cat, &sql), "Unsupported", "capacity");
    }

    // A bounded view is a leaf, and cannot be retargeted.
    rejects(
        &cat,
        &[
            ("SELECT id, v FROM bv", "Unsupported", "capacity-bounded"),
            (
                "SELECT bv.id, u.v FROM bv JOIN u ON bv.id = u.g",
                "Unsupported",
                "capacity-bounded",
            ),
            (
                "SELECT id FROM t WHERE id IN (SELECT id FROM bv)",
                "Unsupported",
                "capacity-bounded",
            ),
            (
                "WITH x AS (SELECT id, v FROM bv) SELECT id FROM x",
                "Unsupported",
                "capacity-bounded",
            ),
        ],
    );
    let sql = "ALTER VIEW bv AS SELECT id, v FROM t WHERE v > 0";
    assert_rejects(sql, plan(&cat, sql), "Unsupported", "capacity-bounded");
}

/// Both retarget spellings: never onto a body that reads the view itself, and
/// never over a relation that is not a view.
#[test]
fn a_retarget_never_reads_itself_nor_replaces_a_non_view() {
    let cat = cat();
    let chain = plan(&cat, "ALTER VIEW tv AS SELECT id, v FROM t").unwrap();
    assert_eq!(chain.name, "tv");
    assert_eq!(final_view(&chain).output_columns.len(), 2);
    for verb in ["ALTER VIEW", "CREATE OR REPLACE VIEW"] {
        for (target, body, needle) in [
            ("tv", "SELECT id, v FROM tv", "itself"),
            ("t", "SELECT id, v FROM u", "is a table"),
            ("st", "SELECT id, v FROM u", "is a stream"),
        ] {
            let sql = format!("{verb} {target} AS {body}");
            assert_rejects(&sql, plan(&cat, &sql), "Unsupported", needle);
        }
    }
}

/// The statement clauses `plan_view` itself turns away, on both spellings:
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
        // Its keys are never read, so accepting it would build an unbounded
        // view where a bounded one was asked for.
        (
            "CREATE VIEW v OPTIONS(capacity = '4 MB') AS SELECT id FROM t",
            "OPTIONS",
        ),
    ] {
        assert_rejects(sql, plan(&cat, sql), "Unsupported", needle);
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
    assert_rejects(sql, plan(&cat, sql), "Unsupported", "ALTER VIEW: a view body reads");
    let sql = "ALTER VIEW tv AS VALUES (1)";
    assert_rejects(sql, plan(&cat, sql), "Unsupported", "ALTER VIEW only supports SELECT");
}
