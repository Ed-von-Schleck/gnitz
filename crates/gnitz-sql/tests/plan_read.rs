//! The ad-hoc read planner as a pure function of `(statement, catalog)`: what
//! each read shape plans to, what EXPLAIN says about it, and which reads are
//! rejected before any request could be issued.

use gnitz_core::{CatalogSnapshot, RelClass, TypeCode};
use gnitz_sql::sqlparser::ast::Statement;
use gnitz_sql::{explain_lines, GnitzSqlError, ReadKind};

mod pure;
use pure::*;

/// `t(id U64 PK, v U64, w U64)` with an index on each of `v` and `w` — every
/// access rung is reachable from it, and two indexes make their arbitration
/// observable. `wide` carries a 128-bit indexed column, `c` a compound PK, `tv`
/// is a view over `t`'s columns, and `tw` has a 128-bit payload column.
fn cat() -> CatalogSnapshot {
    let u = TypeCode::U64;
    let tvw = || vec![col("id", u), col("v", u), col("w", u)];
    catalog(vec![
        ("t", rel(16, RelClass::Table, false, tvw(), vec![0], &[&[1], &[2]])),
        (
            "wide",
            rel(
                17,
                RelClass::Table,
                false,
                vec![col("id", u), col("flag", u), col("big", TypeCode::U128)],
                vec![0],
                &[&[1], &[2]],
            ),
        ),
        (
            "c",
            table(18, vec![col("a", u), col("b", u), col("x", TypeCode::I64)], vec![0, 1]),
        ),
        ("tv", rel(19, RelClass::View, false, tvw(), vec![0], &[])),
        (
            "tw",
            table(20, vec![col("id", TypeCode::I64), col("w", TypeCode::U128)], vec![0]),
        ),
        ("u", table(21, vec![col("id", u), col("k", TypeCode::I64)], vec![0])),
    ])
}

fn explain(cat: &CatalogSnapshot, sql: &str) -> Vec<String> {
    explain_lines(&read(cat, &format!("EXPLAIN {sql}")).unwrap_or_else(|e| panic!("`{sql}`: {e:?}")))
}

// ── EXPLAIN ──────────────────────────────────────────────────────────────────

/// Every decision the read path makes has one name, and the five lines together
/// are the plan. One row per vocabulary token: the access rungs and their
/// arbitration, whether a predicate ships, the sink's shape, and which side does
/// the ORDER BY / LIMIT work.
#[test]
fn explain_names_every_decision() {
    let cat = cat();
    const TRADED: &str = "access: index range on (v) — may be traded for a full scan on low selectivity";
    for (sql, want) in [
        // The bare `*` scan ships no spec; projecting the same columns does.
        (
            "SELECT * FROM t",
            [
                "read table t",
                "access: full scan (unprojected)",
                "predicate: none",
                "projection: 3 columns",
                "order/limit: none",
            ],
        ),
        (
            "SELECT id, v, w FROM t",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "projection: 3 columns",
                "order/limit: none",
            ],
        ),
        (
            "SELECT v FROM t WHERE id = 5",
            [
                "read table t",
                "access: pk point lookup",
                "predicate: none",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        (
            "SELECT v FROM t WHERE id > 5",
            [
                "read table t",
                "access: pk range walk",
                "predicate: none",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        // The IN list deduplicates, so this gathers 2 keys, not 3.
        (
            "SELECT v FROM t WHERE id IN (1, 1, 2)",
            [
                "read table t",
                "access: pk set gather (2 keys)",
                "predicate: none",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        // A full PK point applies the whole WHERE itself; a residual ships.
        (
            "SELECT v FROM t WHERE id = 5 AND w > 5",
            [
                "read table t",
                "access: pk point lookup",
                "predicate: server-side",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        // An inexact index bound keeps the whole WHERE in the predicate.
        (
            "SELECT w FROM t WHERE v = 5",
            [
                "read table t",
                TRADED,
                "predicate: server-side",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        // A literal past the VM's i64 constant has no compiled form, so the walk
        // must apply it and nothing ships.
        (
            "SELECT w FROM t WHERE v = 18446744073709551615",
            [
                "read table t",
                "access: index range on (v) — exact walk, never traded",
                "predicate: none",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        // An equality pins its column outright; a BETWEEN only narrows one.
        (
            "SELECT id FROM t WHERE v = 10 AND w BETWEEN 1 AND 9",
            [
                "read table t",
                TRADED,
                "predicate: server-side",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        // A 128-bit conjunct has no VM register, so the best-ranked bound (the
        // point on `flag`, whose residual is that conjunct) yields to the walk
        // that consumes it byte-exactly.
        (
            "SELECT id FROM wide WHERE flag = 1 AND big BETWEEN 100 AND 200",
            [
                "read table wide",
                "access: index range on (big) — exact walk, never traded",
                "predicate: server-side",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        // Compound PK: a leading-column equality names a key group, the full
        // key names one key.
        (
            "SELECT x FROM c WHERE a = 1",
            [
                "read table c",
                "access: pk range walk",
                "predicate: none",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        (
            "SELECT x FROM c WHERE a = 1 AND b = 2",
            [
                "read table c",
                "access: pk point lookup",
                "predicate: none",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        // A non-integral literal and a top-level OR both keep the full scan.
        (
            "SELECT v FROM t WHERE id = 3.5",
            [
                "read table t",
                "access: full scan",
                "predicate: server-side",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        (
            "SELECT v FROM t WHERE id = 1 OR v = 5",
            [
                "read table t",
                "access: full scan",
                "predicate: server-side",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
        // ORDER BY on a non-projected column adds a hidden reply column.
        (
            "SELECT id, v FROM t ORDER BY w",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "projection: 2 columns (+1 for ordering)",
                "order/limit: client sort",
            ],
        ),
        // The per-worker cut is OFFSET+LIMIT deep because the client windows.
        (
            "SELECT v FROM t ORDER BY v LIMIT 10 OFFSET 2",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "projection: 1 columns",
                "order/limit: server top-12, client sort, client window",
            ],
        ),
        (
            "SELECT v FROM t LIMIT 10",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "projection: 1 columns",
                "order/limit: server early-stop 10, client window",
            ],
        ),
        // `LIMIT 0` is answered from the reply schema; the plan still describes
        // the access and shape it would have run.
        (
            "SELECT v FROM t WHERE id = 2 LIMIT 0",
            [
                "read table t",
                "access: pk point lookup",
                "predicate: none",
                "projection: 1 columns",
                "order/limit: no request (LIMIT 0)",
            ],
        ),
        // Folds name the physical reduce (AVG is its SUM + COUNT_NON_NULL pair)
        // and print no projection line.
        (
            "SELECT COUNT(*) FROM t WHERE id = 5",
            [
                "read table t",
                "access: pk point lookup",
                "predicate: none",
                "fold: global aggregate: COUNT(*)",
                "order/limit: none",
            ],
        ),
        (
            "SELECT v, SUM(w), AVG(w) FROM t GROUP BY v",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "fold: group by (v): SUM(w), SUM(w), COUNT_NON_NULL(w)",
                "order/limit: none",
            ],
        ),
        (
            "SELECT v FROM t GROUP BY v",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "fold: group by (v)",
                "order/limit: none",
            ],
        ),
        (
            "SELECT DISTINCT v, w FROM t",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "fold: distinct on (v, w)",
                "order/limit: none",
            ],
        ),
        (
            "SELECT v, COUNT(*) FROM t GROUP BY v HAVING COUNT(*) > 1 ORDER BY 1 LIMIT 5",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "fold: group by (v): COUNT(*); HAVING applied client-side",
                "order/limit: client sort, client window",
            ],
        ),
        (
            "SELECT v, COUNT(*) FROM t WHERE v = 10 GROUP BY v LIMIT 0",
            [
                "read table t",
                TRADED,
                "predicate: server-side",
                "fold: group by (v): COUNT(*)",
                "order/limit: no request (LIMIT 0)",
            ],
        ),
        // A view read may drain pending ticks; a CTE expands into its source, so
        // the read names the source.
        (
            "SELECT * FROM tv",
            [
                "read view tv (drains pending ticks when stale)",
                "access: full scan (unprojected)",
                "predicate: none",
                "projection: 3 columns",
                "order/limit: none",
            ],
        ),
        (
            "WITH c AS (SELECT * FROM tv) SELECT * FROM c",
            [
                "read view tv (drains pending ticks when stale)",
                "access: full scan (unprojected)",
                "predicate: none",
                "projection: 3 columns",
                "order/limit: none",
            ],
        ),
        (
            "WITH c AS (SELECT * FROM t) SELECT id FROM c",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "projection: 1 columns",
                "order/limit: none",
            ],
        ),
    ] {
        assert_eq!(explain(&cat, sql), want, "`{sql}`");
    }

    // The introducer is inert phrasing.
    let want = explain(&cat, "SELECT v FROM t WHERE id = 5");
    for sql in [
        "DESC SELECT v FROM t WHERE id = 5",
        "DESCRIBE SELECT v FROM t WHERE id = 5",
    ] {
        assert_eq!(explain_lines(&read(&cat, sql).unwrap()), want, "`{sql}`");
    }
}

/// EXPLAIN plans the identical read — same sink, same encoded spec — so a query
/// it cannot describe returns exactly the rejection executing it returns.
#[test]
fn explain_plans_the_query_it_describes_and_shares_its_rejection() {
    let cat = cat();
    for sql in [
        "SELECT id FROM t WHERE v = 3 ORDER BY v LIMIT 4",
        "SELECT v, COUNT(*) FROM t GROUP BY v",
    ] {
        let direct = read(&cat, sql).unwrap();
        let described = read(&cat, &format!("EXPLAIN {sql}")).unwrap();
        assert_eq!(direct.kind(), described.kind(), "`{sql}`");
        assert_eq!(direct.encoded_spec(), described.encoded_spec(), "`{sql}`");
    }
    for (sql, variant, msg) in [
        (
            "SELECT v, COUNT(*) AS n FROM t GROUP BY v ORDER BY nope",
            "Bind",
            "nope",
        ),
        ("SELECT t.v FROM t JOIN u ON t.id = u.id", "Unsupported", "CREATE VIEW"),
        // Unsupported on two axes at once: which one is named pins that the fold
        // sink plans the WHERE and its shape in the same order the rows sink does.
        (
            "SELECT DISTINCT v + 1 FROM t WHERE CAST(v AS CHAR) LIKE CAST(w AS CHAR)",
            "Unsupported",
            "LIKE pattern must be a string literal",
        ),
    ] {
        let direct = err_of(read(&cat, sql));
        let described = err_of(read(&cat, &format!("EXPLAIN {sql}")));
        assert_eq!(variant_of(&direct), variant_of(&described), "`{sql}`");
        assert_rejects(sql, Err::<(), _>(direct), variant, msg);
    }
    assert_rejects(
        "EXPLAIN INSERT",
        read(&cat, "EXPLAIN INSERT INTO t (id, v, w) VALUES (9, 9, 9)"),
        "Unsupported",
        "EXPLAIN",
    );
}

/// Only the plain `EXPLAIN <statement>` is honored; every option asks for a
/// rendering the output shape does not have, and is named in the rejection.
#[test]
fn explain_rejected_clause_matrix() {
    let cat = cat();
    assert!(read(&cat, "EXPLAIN SELECT v FROM t").is_ok());
    for (sql, clause) in [
        ("EXPLAIN ANALYZE SELECT v FROM t", "ANALYZE"),
        ("EXPLAIN VERBOSE SELECT v FROM t", "VERBOSE"),
        ("EXPLAIN QUERY PLAN SELECT v FROM t", "QUERY PLAN"),
        ("EXPLAIN ESTIMATE SELECT v FROM t", "ESTIMATE"),
        ("EXPLAIN FORMAT JSON SELECT v FROM t", "FORMAT"),
        ("EXPLAIN (FORMAT JSON) SELECT v FROM t", "the parenthesized option list"),
    ] {
        assert_rejects(sql, read(&cat, sql), "Unsupported", clause);
    }
}

// ── Read shapes ──────────────────────────────────────────────────────────────

/// Every ad-hoc read shape plans to the sink it belongs on and says whether a
/// request goes out at all.
#[test]
fn every_read_shape_plans_to_its_sink() {
    let cat = base();
    for (sql, kind, dispatches) in [
        ("SELECT * FROM t", ReadKind::PlainScan, true),
        ("SELECT id FROM t", ReadKind::Rows, true),
        ("SELECT * FROM t WHERE id = 3", ReadKind::Rows, true),
        ("SELECT * FROM t WHERE v = 3", ReadKind::Rows, true),
        ("SELECT id FROM t ORDER BY v LIMIT 5 OFFSET 2", ReadKind::Rows, true),
        ("SELECT id FROM t LIMIT 0", ReadKind::Rows, false),
        ("SELECT COUNT(*) FROM t", ReadKind::Fold, true),
        ("SELECT g, SUM(v) AS s FROM t GROUP BY g", ReadKind::Fold, true),
        ("SELECT DISTINCT g FROM t", ReadKind::Fold, true),
        ("SELECT COUNT(*) FROM t LIMIT 0", ReadKind::Fold, false),
        ("WITH c AS (SELECT * FROM t) SELECT id FROM c", ReadKind::Rows, true),
    ] {
        let plan = read(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        assert_eq!(plan.kind(), kind, "`{sql}`");
        assert_eq!(plan.target_id(), Some(16), "`{sql}`: the relation it reads");
        assert_eq!(plan.dispatches(), dispatches, "`{sql}`");
        let plain = kind == ReadKind::PlainScan;
        assert_eq!(
            plan.reply_schema().is_none(),
            plain,
            "`{sql}`: only a plain scan projects nothing"
        );
        assert_eq!(
            plan.encoded_spec().is_none(),
            plain,
            "`{sql}`: only a plain scan ships no spec"
        );
    }
}

/// A parenthesized column reference plans byte-for-byte like the bare one on
/// every read surface: projection, WHERE, ORDER BY, and the fold's GROUP BY.
#[test]
fn a_parenthesized_column_reference_reads_like_a_bare_one() {
    let cat = base();
    for (parens, bare) in [
        ("SELECT (v) FROM t WHERE id = 1", "SELECT v FROM t WHERE id = 1"),
        ("SELECT (t.v) FROM t WHERE (id) = 1", "SELECT t.v FROM t WHERE id = 1"),
        ("SELECT v FROM t ORDER BY (v)", "SELECT v FROM t ORDER BY v"),
        (
            "SELECT (g), COUNT(*) AS n FROM t GROUP BY (g)",
            "SELECT g, COUNT(*) AS n FROM t GROUP BY g",
        ),
        // A CTE expands by column name, which peels too.
        (
            "WITH c AS (SELECT (id), (g), (v) FROM t) SELECT id FROM c",
            "WITH c AS (SELECT id, g, v FROM t) SELECT id FROM c",
        ),
    ] {
        let p = read(&cat, parens).unwrap_or_else(|e| panic!("`{parens}`: {e:?}"));
        let b = read(&cat, bare).unwrap();
        assert_eq!(p.encoded_spec(), b.encoded_spec(), "`{parens}`");
        assert_eq!(
            p.reply_schema().map(visible),
            b.reply_schema().map(visible),
            "`{parens}`"
        );
    }
    // Peeling the wrapper must not turn a computed item into a column reference.
    let s = read(&cat, "SELECT (v + 1) AS x FROM t").unwrap();
    assert_eq!(visible(s.reply_schema().unwrap()), ["x"]);
}

fn visible(s: &gnitz_core::Schema) -> Vec<String> {
    s.visible_columns().map(|(_, c)| c.name.to_lowercase()).collect()
}

/// The reply schema a projection produces: the source PK rides hidden in front
/// of whatever the SELECT list names, so `* EXCEPT (id)` keeps its key.
#[test]
fn a_reads_reply_schema_hides_the_source_pk_behind_the_select_list() {
    let cat = base();
    for (sql, want) in [
        ("SELECT * EXCEPT (g) FROM t", vec!["id", "v"]),
        ("SELECT * EXCEPT (id) FROM t", vec!["g", "v"]),
        ("SELECT * RENAME (g AS x) FROM t", vec!["id", "x", "v"]),
        ("SELECT t.id, t.g AS x FROM t", vec!["id", "x"]),
        ("SELECT id, g AS g1, g AS g2 FROM t", vec!["id", "g1", "g2"]),
    ] {
        let s = read(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        let schema = s.reply_schema().unwrap();
        assert_eq!(visible(schema), want, "`{sql}`");
        assert!(schema.columns[0].is_hidden && schema.columns[0].name == "id", "`{sql}`");
    }
}

// ── Rejections ───────────────────────────────────────────────────────────────

/// Reads rejected at plan time, each with the guard that owns it.
#[test]
fn a_read_the_planner_rejects_names_its_rule() {
    let mut cat = cat();
    // A join view whose two sides both carry `id` and `val`.
    let l = table(30, vec![col("id", TypeCode::I64), col("val", TypeCode::I64)], vec![0]);
    let r = table(31, vec![col("id", TypeCode::I64), col("val", TypeCode::I64)], vec![0]);
    cat.insert(SN, "l", Some(l));
    cat.insert(SN, "r", Some(r));
    let jv = view(&cat, "SELECT * FROM l JOIN r ON l.val = r.val");
    register(&mut cat, "jv", 32, &jv);

    for (sql, variant, msg) in [
        (
            "SELECT w, COUNT(*) FROM tw GROUP BY w HAVING w > 5",
            "Unsupported",
            "128-bit",
        ),
        (
            "WITH x AS (SELECT DISTINCT v FROM t) SELECT v FROM x",
            "Unsupported",
            "CTE 'x'",
        ),
        ("SELECT v AS x, w AS x FROM t", "Plan", "duplicate column name 'x'"),
        (
            "SELECT DISTINCT v AS x, w AS x FROM t",
            "Plan",
            "duplicate column name 'x'",
        ),
        ("SELECT id, v, v FROM t", "Plan", "duplicate column name 'v'"),
        ("SELECT *, * FROM t", "Plan", "duplicate column name"),
        ("SELECT *, * FROM t WHERE v > 0", "Plan", "duplicate column name"),
        ("SELECT DISTINCT *, * FROM t", "Plan", "duplicate column name"),
        ("SELECT t.*, t.* FROM t", "Unsupported", "SELECT item"),
        // `AS d(x, y)` renames the columns positionally; honoring only the
        // relation alias would answer under `t`'s own column names.
        ("SELECT * FROM t AS d(x, y)", "Unsupported", "positional column aliases"),
        ("SELECT id FROM jv", "Bind", "is ambiguous"),
        ("SELECT * FROM jv WHERE id = 5", "Bind", "is ambiguous"),
        ("SELECT _join_pk FROM jv", "Bind", "not found"),
        // A window call in an ad-hoc grouped SELECT list: windows are a view-body
        // feature, so the leaf's own rejection answers rather than the fold
        // lowering's internal error.
        (
            "SELECT v, ROW_NUMBER() OVER (ORDER BY v) FROM t GROUP BY v",
            "Unsupported",
            "only supported in the SELECT list and QUALIFY of a CREATE VIEW",
        ),
        (
            "SELECT v, SUM(w) OVER (PARTITION BY v) FROM t GROUP BY v",
            "Unsupported",
            "only supported in the SELECT list and QUALIFY of a CREATE VIEW",
        ),
    ] {
        assert_rejects(sql, read(&cat, sql), variant, msg);
    }
    // The CTE rejection names the offending clause too.
    let (_, m) = variant_of(&err_of(read(
        &cat,
        "WITH x AS (SELECT DISTINCT v FROM t) SELECT v FROM x",
    )));
    assert!(m.contains("DISTINCT"), "got {m}");
}

// ── Parity inside the single-relation scope ──────────────────────────────────

/// Whatever the flat query plans, the CTE plans; and a name the CTE does not
/// expose is not found, however the source spells it.
#[test]
fn a_cte_expands_to_the_flat_query() {
    let cat = base();
    for (cte, flat) in [
        ("WITH x AS (SELECT id, v FROM t) SELECT id FROM x", "SELECT id FROM t"),
        ("WITH x AS (SELECT v, id FROM t) SELECT * FROM x", "SELECT v, id FROM t"),
        (
            "WITH x AS (SELECT id, v AS q FROM t) SELECT q FROM x",
            "SELECT v AS q FROM t",
        ),
        (
            "WITH x AS (SELECT id, v + 1 AS q FROM t) SELECT q * 2 AS r FROM x WHERE q > 3",
            "SELECT (v + 1) * 2 AS r FROM t WHERE (v + 1) > 3",
        ),
        (
            "WITH x AS (SELECT id FROM t WHERE v > 1) SELECT id FROM x WHERE id = 5",
            "SELECT id FROM t WHERE (v > 1) AND (id = 5)",
        ),
        (
            "WITH x(i, j) AS (SELECT id, v FROM t) SELECT j FROM x ORDER BY i",
            "SELECT v AS j FROM t ORDER BY id",
        ),
        (
            "WITH x AS (SELECT id, v FROM t), y AS (SELECT v AS w FROM x WHERE id > 2) SELECT w FROM y",
            "SELECT v AS w FROM t WHERE id > 2",
        ),
        (
            "WITH x AS (SELECT g, v + 1 AS q FROM t) SELECT g, SUM(q) AS s FROM x GROUP BY g ORDER BY s",
            "SELECT g, SUM(v + 1) AS s FROM t GROUP BY g ORDER BY s",
        ),
        (
            "WITH x AS (SELECT * FROM t) SELECT y.id FROM x AS y ORDER BY y.v",
            "SELECT id FROM t ORDER BY v",
        ),
        (
            "WITH x AS (SELECT * EXCEPT (g) FROM t) SELECT * FROM x",
            "SELECT id, v FROM t",
        ),
    ] {
        let c = read(&cat, cte).unwrap_or_else(|e| panic!("`{cte}`: {e:?}"));
        let f = read(&cat, flat).unwrap_or_else(|e| panic!("`{flat}`: {e:?}"));
        assert_eq!(c.kind(), f.kind(), "`{cte}`");
        assert_eq!(c.encoded_spec(), f.encoded_spec(), "`{cte}`");
        assert_eq!(c.reply_schema().map(visible), f.reply_schema().map(visible), "`{cte}`");
    }
    for (sql, variant, msg) in [
        (
            "WITH x AS (SELECT id FROM t) SELECT v FROM x",
            "Bind",
            "column 'v' not found",
        ),
        (
            "WITH x AS (SELECT id FROM t) SELECT t.id FROM x",
            "Bind",
            "table alias 't' not found",
        ),
        (
            "WITH x AS (SELECT id, v FROM t) SELECT id FROM x ORDER BY g",
            "Bind",
            "column 'g' not found",
        ),
        (
            "WITH x(i) AS (SELECT id, v FROM t) SELECT i FROM x",
            "Plan",
            "1 column aliases but body returns 2",
        ),
        (
            "WITH x AS (SELECT v, v FROM t) SELECT v FROM x",
            "Plan",
            "duplicate column name 'v'",
        ),
        (
            "WITH x AS (SELECT g, COUNT(*) AS n FROM t GROUP BY g) SELECT g FROM x",
            "Unsupported",
            "derives a new one (grouped CTE)",
        ),
        (
            "WITH x AS (SELECT t.id FROM t JOIN u ON t.g = u.g) SELECT id FROM x",
            "Unsupported",
            "derives a new one (JOIN)",
        ),
    ] {
        assert_rejects(sql, read(&cat, sql), variant, msg);
    }

    // A join view surfaces both sides' `v`: a wildcard carries the duplicate
    // through positionally, and only naming it is ambiguous.
    let i = TypeCode::I64;
    let jv = catalog(vec![(
        "jv",
        rel(
            30,
            RelClass::View,
            false,
            vec![col("id", i), col("v", i), col("v", i)],
            vec![0],
            &[],
        ),
    )]);
    let (cte, flat) = ("WITH x AS (SELECT * FROM jv) SELECT * FROM x", "SELECT * FROM jv");
    let c = read(&jv, cte).unwrap_or_else(|e| panic!("`{cte}`: {e:?}"));
    let f = read(&jv, flat).unwrap_or_else(|e| panic!("`{flat}`: {e:?}"));
    assert_eq!(c.encoded_spec(), f.encoded_spec(), "`{cte}`");
    // Defining one over the duplicate is fine — the CTE names no output column
    // of its own, so nothing is duplicated until something reads it by name.
    let ok = "WITH x AS (SELECT * EXCEPT (id) FROM jv) SELECT 1 AS one FROM x";
    read(&jv, ok).unwrap_or_else(|e| panic!("`{ok}`: {e:?}"));
    for sql in [
        "SELECT v FROM jv",
        "WITH x AS (SELECT * EXCEPT (id) FROM jv) SELECT v FROM x",
        // `*` over such a CTE is the one shape a macro cannot carry: expansion
        // has to write the names down, so it reaches the same ambiguity the flat
        // wildcard passes positionally — a rejection, never a silent first match.
        "WITH x AS (SELECT * EXCEPT (id) FROM jv) SELECT * FROM x",
    ] {
        assert_rejects(sql, read(&jv, sql), "Bind", "'v' is ambiguous");
    }
}

/// A chain naming its predecessor's column twice doubles per level, so the
/// planner refuses it rather than allocating it in the caller's own process.
#[test]
fn a_cte_chain_that_doubles_each_level_is_refused() {
    let cat = base();
    let chain = |levels: usize| {
        let mut sql = "WITH c0 AS (SELECT v + v AS a FROM t)".to_string();
        for i in 1..levels {
            sql += &format!(", c{i} AS (SELECT a + a AS a FROM c{})", i - 1);
        }
        sql + &format!(" SELECT a FROM c{}", levels - 1)
    };
    // A chain is not suspicious for being long: only the expansion's size counts.
    let ok = chain(10);
    read(&cat, &ok).unwrap_or_else(|e| panic!("`{ok}`: {e:?}"));
    let big = chain(30);
    assert_rejects(&big, read(&cat, &big), "Unsupported", "expression nodes");
}

/// An ORDER BY key that is not an output column binds in the SELECT list's own
/// scope on every sink and rides as a hidden column. DISTINCT is the exception:
/// a hidden item would widen the set, so its keys must be output columns.
#[test]
fn an_order_by_key_binds_where_the_select_list_does() {
    let cat = base();
    for (sql, line) in [
        (
            "SELECT id FROM t ORDER BY v + 1",
            "projection: 1 columns (+1 for ordering)",
        ),
        (
            "SELECT id FROM t ORDER BY v, g + v DESC",
            "projection: 1 columns (+2 for ordering)",
        ),
        // One appended column per *distinct* key program — a duplicate would be
        // evaluated by the worker and shipped per row.
        (
            "SELECT id FROM t ORDER BY v + 1, v + 1",
            "projection: 1 columns (+1 for ordering)",
        ),
        ("SELECT id, v + 1 AS w FROM t ORDER BY v + 1", "projection: 2 columns"),
        ("SELECT id, v + 1 AS w FROM t ORDER BY w", "projection: 2 columns"),
        ("SELECT id FROM t ORDER BY t.id", "projection: 1 columns"),
    ] {
        assert!(
            explain(&cat, sql).contains(&line.to_string()),
            "`{sql}`: {:?}",
            explain(&cat, sql)
        );
    }
    for sql in [
        "SELECT g FROM t GROUP BY g ORDER BY COUNT(*) DESC",
        "SELECT COUNT(*) AS n FROM t GROUP BY g ORDER BY g",
        "SELECT g, SUM(v) AS s FROM t GROUP BY g ORDER BY SUM(v) + g, MAX(v)",
        "SELECT g + 1 AS h FROM t GROUP BY g + 1 ORDER BY (g + 1) * 2",
        "SELECT DISTINCT v AS w FROM t ORDER BY w DESC, 1",
    ] {
        let plan = read(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        assert_eq!(plan.kind(), ReadKind::Fold, "`{sql}`");
    }
    // An aggregate named only in ORDER BY is collected into the reduce: the
    // partial reply carries its accumulator beside the group column.
    let plan = read(&cat, "SELECT g FROM t GROUP BY g ORDER BY COUNT(*)").unwrap();
    assert_eq!(visible(plan.reply_schema().unwrap()), ["g", "_agg"]);
    // A hidden ordering column is tied to its key by the key's position in the
    // whole ORDER BY, so a positional key ahead of an expression one does not
    // shift the tie: both spellings of one order plan the same output shape.
    for (sql, named) in [
        (
            "SELECT g, SUM(v) AS s FROM t GROUP BY g ORDER BY 1, MAX(v)",
            "SELECT g, SUM(v) AS s FROM t GROUP BY g ORDER BY g, MAX(v)",
        ),
        (
            "SELECT g, SUM(v) AS s FROM t GROUP BY g ORDER BY MAX(v), 1, MIN(v)",
            "SELECT g, SUM(v) AS s FROM t GROUP BY g ORDER BY MAX(v), g, MIN(v)",
        ),
    ] {
        let plan = read(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        let twin = read(&cat, named).unwrap_or_else(|e| panic!("`{named}`: {e:?}"));
        assert_eq!(
            visible(plan.reply_schema().unwrap()),
            visible(twin.reply_schema().unwrap()),
            "`{sql}`"
        );
        assert_eq!(explain(&cat, sql), explain(&cat, named), "`{sql}`");
    }
    for (sql, variant, msg) in [
        (
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY v",
            "Plan",
            "ORDER BY: column 'v' must appear in GROUP BY or an aggregate function",
        ),
        (
            "SELECT DISTINCT v FROM t ORDER BY v + 1",
            "Unsupported",
            "SELECT DISTINCT: ORDER BY expressions must appear in the select list",
        ),
        ("SELECT id FROM t ORDER BY nope + 1", "Bind", "column 'nope' not found"),
    ] {
        assert_rejects(sql, read(&cat, sql), variant, msg);
    }
}

/// A FROM-less SELECT reads nothing: it plans to a constant row with no request,
/// its items are constant expressions under the computed-column naming, and the
/// clauses that need a relation are refused.
#[test]
fn a_from_less_select_plans_a_constant_row() {
    let cat = base();
    for (sql, cols) in [
        ("SELECT 1", vec!["_expr0"]),
        ("SELECT 1 AS one, 'x' AS s, 2 + 3", vec!["one", "s", "_expr2"]),
        ("SELECT 1 AS a ORDER BY a DESC LIMIT 1", vec!["a"]),
        ("SELECT 1 AS a ORDER BY 1 + 1", vec!["a"]),
    ] {
        let plan = read(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        assert_eq!(plan.kind(), ReadKind::Constant, "`{sql}`");
        assert_eq!(plan.target_id(), None, "`{sql}`");
        assert!(!plan.dispatches(), "`{sql}`");
        assert!(plan.encoded_spec().is_none(), "`{sql}`");
        assert_eq!(visible(plan.reply_schema().unwrap()), cols, "`{sql}`");
    }
    assert_eq!(
        explain(&cat, "SELECT 1 AS a ORDER BY 1 + 1 LIMIT 1"),
        [
            "read nothing (constant row)",
            "access: none",
            "predicate: none",
            "projection: 1 columns (+1 for ordering)",
            "order/limit: client sort, client window",
        ]
    );
    for (sql, variant, msg) in [
        ("SELECT x", "Bind", "column 'x' not found"),
        ("SELECT 1 + t.x", "Bind", "column 't.x' not found"),
        ("SELECT *", "Unsupported", "SELECT * is not a supported SELECT item"),
        (
            "SELECT 1 WHERE 1 = 1",
            "Unsupported",
            "SELECT without FROM: WHERE is not supported",
        ),
        ("SELECT COUNT(*)", "Unsupported", "aggregate"),
        ("SELECT 1 AS a, 2 AS a", "Plan", "duplicate column name 'a'"),
        ("SELECT (SELECT 1)", "Unsupported", "scalar subquery"),
    ] {
        assert_rejects(sql, read(&cat, sql), variant, msg);
    }
}

// ── The resolve loop ─────────────────────────────────────────────────────────

/// The loop resolves one name per distinct relation the planner asks for, none
/// for a name only the AST mentions, and re-running a pass over the completed
/// snapshot plans the identical circuit. A recorded absence is not-found, not a
/// miss; a rejection raised before the first resolve costs nothing.
#[test]
fn the_loop_resolves_only_what_the_planner_asks_for() {
    let known = base();
    for (sql, want) in [
        ("SELECT id FROM t", vec!["t"]),
        ("SELECT * FROM t", vec!["t"]),
        ("WITH t2 AS (SELECT * FROM t) SELECT id FROM t2", vec!["t"]),
        ("WITH t AS (SELECT * FROM t) SELECT id FROM t", vec!["t"]),
        (
            "CREATE VIEW vw AS SELECT t.id, u.v FROM t JOIN u ON t.g = u.g",
            vec!["t", "u"],
        ),
        (
            "CREATE VIEW vw AS WITH c AS (SELECT id, v FROM t WHERE v > 1) SELECT c.id FROM c JOIN u ON c.id = u.id",
            vec!["t", "u"],
        ),
        (
            "CREATE VIEW vw AS SELECT g FROM t EXCEPT SELECT g FROM u",
            vec!["t", "u"],
        ),
    ] {
        let stmt = parse(sql);
        match &stmt {
            Statement::CreateView(_) => {
                let (looped, asked) = resolving(&known, |c| gnitz_sql::plan_view(&stmt, c, SN));
                let chain = |p| match p {
                    gnitz_sql::ViewPlan::Create { chain, .. } => chain,
                    gnitz_sql::ViewPlan::Skip { .. } => panic!("`{sql}` planned a skip"),
                };
                let looped = chain(looped.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")));
                assert_eq!(asked, want, "`{sql}`");
                let single = chain(gnitz_sql::plan_view(&stmt, &known, SN).unwrap());
                assert_eq!(looped.views.len(), single.views.len(), "`{sql}`");
                for (a, b) in looped.views.iter().zip(&single.views) {
                    assert_eq!((a.seg, &a.circuit), (b.seg, &b.circuit), "`{sql}`");
                }
            }
            _ => {
                let (plan, asked) = resolving(&known, |c| read(c, sql).map(|_| ()));
                plan.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
                assert_eq!(asked, want, "`{sql}`");
            }
        }
    }

    let mut absent = CatalogSnapshot::default();
    absent.insert(SN, "t", None);
    let e = err_of(read(&absent, "SELECT id FROM t"));
    assert!(
        !matches!(e, GnitzSqlError::CatalogMiss(_)) && format!("{e}").contains("does not exist"),
        "got {e:?}"
    );

    for (sql, variant) in [
        ("SELECT t.id FROM t JOIN u ON t.g = u.g", "Unsupported"),
        ("SELECT id FROM _seg4096", "Plan"),
        ("CREATE VIEW v AS SELECT * FROM _seg4096", "Plan"),
    ] {
        let stmt = parse(sql);
        let (plan, asked) = resolving(&known, |c| match &stmt {
            Statement::CreateView(_) => gnitz_sql::plan_view(&stmt, c, SN).map(|_| ()),
            _ => read(c, sql).map(|_| ()),
        });
        assert_eq!(variant_of(&err_of(plan)).0, variant, "`{sql}`");
        assert!(asked.is_empty(), "`{sql}` costs no resolve, asked: {asked:?}");
    }
}

/// No statement shape lets a `CatalogMiss` or an `Internal` out of the loop —
/// the shapes that reach a relation name only from a deep position, where a pass
/// that forgot to report would surface as an invariant break instead.
#[test]
fn no_shape_leaks_a_control_signal_out_of_the_loop() {
    let cat = base();
    let bodies = [
        "WITH c AS (SELECT id, v FROM t) SELECT c.id FROM c JOIN u ON c.id = u.id",
        "WITH u AS (SELECT id, g, v FROM t) SELECT id FROM u",
        "SELECT d.id FROM (SELECT id, v FROM t WHERE v > 2) d",
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.g = t.g)",
        "SELECT id FROM t WHERE g IN (SELECT g FROM u)",
        "SELECT id, (SELECT MAX(v) FROM u WHERE u.g = t.g) AS m FROM t",
        "SELECT g FROM t UNION ALL SELECT g FROM u",
        "SELECT t.id FROM t JOIN u ON t.g = u.g JOIN t AS t2 ON t2.id = t.id",
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM nope WHERE nope.g = t.g)",
        "SELECT d.id FROM (SELECT id FROM nope) d",
        "WITH c AS (SELECT id FROM nope) SELECT id FROM c",
    ];
    for body in bodies {
        for sql in [
            format!("CREATE VIEW vw AS {body}"),
            format!("SELECT * FROM ({body}) x"),
            format!("EXPLAIN {body}"),
            body.to_string(),
        ] {
            let stmt = parse(&sql);
            let (out, _) = resolving(&cat, |c| match &stmt {
                Statement::CreateView(_) => gnitz_sql::plan_view(&stmt, c, SN).map(|_| ()),
                _ => gnitz_sql::plan_read(&stmt, c, SN).map(|_| ()),
            });
            if let Err(e @ (GnitzSqlError::CatalogMiss(_) | GnitzSqlError::Internal(_))) = out {
                panic!("`{sql}` leaked a control signal: {e:?}");
            }
        }
    }
}
