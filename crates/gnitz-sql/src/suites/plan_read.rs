//! The ad-hoc read planner as a pure function of `(statement, catalog)`: what
//! each read shape plans to, what EXPLAIN says about it, and which reads are
//! rejected before any request could be issued.

use crate::dml::explain_lines;
use gnitz_core::ClientError;
use gnitz_wire::{RelClass, TypeCode, WireFault, WireStatus};
use sqlparser::ast::Statement;

use super::*;

/// `t(id U64 PK, v U64, w U64)` with an index on each of `v` and `w` — every
/// access rung is reachable from it, and two indexes make their arbitration
/// observable. `wide` carries a 128-bit indexed column, `c` a compound PK, `tv`
/// is a view over `t`'s columns, and `tw` has a 128-bit payload column.
fn cat() -> Catalog<'static> {
    let u = TypeCode::U64;
    let tvw = || vec![col("id", u), col("v", u), col("w", u)];
    catalog(vec![
        ("t", rel(16, RelClass::Table, tvw(), vec![0], vec![ix(&[1]), ix(&[2])])),
        (
            "wide",
            rel(
                17,
                RelClass::Table,
                vec![col("id", u), col("flag", u), col("big", TypeCode::U128)],
                vec![0],
                vec![ix(&[1]), ix(&[2])],
            ),
        ),
        (
            "c",
            table(18, vec![col("a", u), col("b", u), col("x", TypeCode::I64)], vec![0, 1]),
        ),
        ("tv", rel(19, RelClass::View, tvw(), vec![0], vec![])),
        (
            "tw",
            table(20, vec![col("id", TypeCode::I64), col("w", TypeCode::U128)], vec![0]),
        ),
        ("u", table(21, vec![col("id", u), col("k", TypeCode::I64)], vec![0])),
    ])
}

fn explain(cat: &Catalog<'static>, sql: &str) -> Vec<String> {
    explain_lines(
        &read(cat, &format!("EXPLAIN {sql}")).unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
        false,
    )
}

// ── EXPLAIN ──────────────────────────────────────────────────────────────────

/// Every decision the read path makes has one name, and the five lines together
/// are the plan. One row per vocabulary token: the access rungs and their
/// arbitration, whether a predicate ships, the sink's shape, and which side does
/// the ORDER BY / LIMIT work.
#[test]
fn explain_names_every_decision() {
    let cat = cat();
    const INDEX_V: &str = "access: index range on (v)";
    for (sql, want) in [
        // A projection reproducing the relation's columns ships no map.
        (
            "SELECT * FROM t",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "projection: 3 columns (unprojected)",
                "order/limit: none",
            ],
        ),
        (
            "SELECT id, v, w FROM t",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "projection: 3 columns (unprojected)",
                "order/limit: none",
            ],
        ),
        (
            "SELECT * FROM t WHERE v = 3 ORDER BY w",
            [
                "read table t",
                INDEX_V,
                "predicate: none",
                "projection: 3 columns (unprojected)",
                "order/limit: client sort",
            ],
        ),
        // A new name is a label over the relation's own regions.
        (
            "SELECT id AS x, v, w FROM t",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "projection: 3 columns (unprojected)",
                "order/limit: none",
            ],
        ),
        (
            "SELECT 1 AS a LIMIT 0",
            [
                "read nothing (constant row)",
                "access: none",
                "predicate: none",
                "projection: 1 column",
                "order/limit: no request (LIMIT 0)",
            ],
        ),
        (
            "SELECT v FROM t WHERE id = 5",
            [
                "read table t",
                "access: pk point lookup",
                "predicate: none",
                "projection: 1 column",
                "order/limit: none",
            ],
        ),
        (
            "SELECT v FROM t WHERE id > 5",
            [
                "read table t",
                "access: pk range walk",
                "predicate: none",
                "projection: 1 column",
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
                "projection: 1 column",
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
                "projection: 1 column",
                "order/limit: none",
            ],
        ),
        // An index bound is exact, so the conjunct it consumes does not ship.
        (
            "SELECT w FROM t WHERE v = 5",
            [
                "read table t",
                INDEX_V,
                "predicate: none",
                "projection: 1 column",
                "order/limit: none",
            ],
        ),
        // A literal past `i64::MAX` is a key like any other.
        (
            "SELECT w FROM t WHERE v = 18446744073709551615",
            [
                "read table t",
                INDEX_V,
                "predicate: none",
                "projection: 1 column",
                "order/limit: none",
            ],
        ),
        // An equality pins its column outright; a BETWEEN only narrows one.
        (
            "SELECT id FROM t WHERE v = 10 AND w BETWEEN 1 AND 9",
            [
                "read table t",
                INDEX_V,
                "predicate: server-side",
                "projection: 1 column",
                "order/limit: none",
            ],
        ),
        // A 128-bit conjunct has no VM register, so the best-ranked bound (the
        // point on `flag`, whose residual is that conjunct) yields to the walk
        // that consumes it.
        (
            "SELECT id FROM wide WHERE flag = 1 AND big BETWEEN 100 AND 200",
            [
                "read table wide",
                "access: index range on (big)",
                "predicate: server-side",
                "projection: 1 column",
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
                "projection: 1 column (unprojected)",
                "order/limit: none",
            ],
        ),
        (
            "SELECT x FROM c WHERE a = 1 AND b = 2",
            [
                "read table c",
                "access: pk point lookup",
                "predicate: none",
                "projection: 1 column (unprojected)",
                "order/limit: none",
            ],
        ),
        // A non-integral literal names no key: the empty range walk.
        (
            "SELECT v FROM t WHERE id = 3.5",
            [
                "read table t",
                "access: pk range walk",
                "predicate: none",
                "projection: 1 column",
                "order/limit: none",
            ],
        ),
        // A top-level OR keeps the full scan.
        (
            "SELECT v FROM t WHERE id = 1 OR v = 5",
            [
                "read table t",
                "access: full scan",
                "predicate: server-side",
                "projection: 1 column",
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
                "projection: 2 columns (+1 for ordering) (unprojected)",
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
                "projection: 1 column",
                "order/limit: server top-12, client sort, client window",
            ],
        ),
        (
            "SELECT v FROM t LIMIT 10",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "projection: 1 column",
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
                "projection: 1 column",
                "order/limit: no request (LIMIT 0)",
            ],
        ),
        // Folds name the physical reduce (AVG is its SUM and a count)
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
                "fold: group by (v): SUM(w), COUNT(*)",
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
                INDEX_V,
                "predicate: none",
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
                "access: full scan",
                "predicate: none",
                "projection: 3 columns (unprojected)",
                "order/limit: none",
            ],
        ),
        (
            "WITH c AS (SELECT * FROM tv) SELECT * FROM c",
            [
                "read view tv (drains pending ticks when stale)",
                "access: full scan",
                "predicate: none",
                "projection: 3 columns (unprojected)",
                "order/limit: none",
            ],
        ),
        (
            "WITH c AS (SELECT * FROM t) SELECT id FROM c",
            [
                "read table t",
                "access: full scan",
                "predicate: none",
                "projection: 1 column",
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
        assert_eq!(explain_lines(&read(&cat, sql).unwrap(), false), want, "`{sql}`");
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
        assert_eq!(
            explain_lines(&direct, false),
            explain_lines(&described, false),
            "`{sql}`"
        );
        assert_eq!(direct.spec(), described.spec(), "`{sql}`");
    }
    for (sql, msg) in [
        ("SELECT v, COUNT(*) AS n FROM t GROUP BY v ORDER BY nope", "nope"),
        ("SELECT t.v FROM t JOIN u ON t.id = u.id", "CREATE VIEW"),
        // Unsupported on two axes at once: which one is named pins that the fold
        // sink plans the WHERE and its shape in the same order the rows sink does.
        (
            "SELECT DISTINCT v + 1 FROM t WHERE CAST(v AS CHAR) LIKE CAST(w AS CHAR)",
            "LIKE pattern must be a string literal",
        ),
    ] {
        let direct = rejected(read(&cat, sql));
        assert_eq!(direct, rejected(read(&cat, &format!("EXPLAIN {sql}"))), "`{sql}`");
        assert!(direct.contains(msg), "`{sql}`: {direct}");
    }
    assert_rejects(
        "EXPLAIN INSERT",
        read(&cat, "EXPLAIN INSERT INTO t (id, v, w) VALUES (9, 9, 9)"),
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
        assert_rejects(sql, read(&cat, sql), clause);
    }
}

/// A cut ordered ascending by a leading run of the key, over a walk in key
/// order, ships no ORDER BY: each worker stops at its first rows, and the client
/// still sorts their union. Any other order ranks every surviving row.
#[test]
fn a_cut_ordered_by_the_key_stops_each_worker_early() {
    let cat = cat();
    for (sql, cut) in [
        ("SELECT * FROM t ORDER BY id LIMIT 5", "early-stop 5"),
        ("SELECT v FROM t ORDER BY id LIMIT 5 OFFSET 2", "early-stop 7"),
        ("SELECT * FROM t WHERE w + 1 > 3 ORDER BY id LIMIT 5", "early-stop 5"),
        ("SELECT * FROM t WHERE id > 3 ORDER BY id LIMIT 5", "early-stop 5"),
        (
            "SELECT * FROM t WHERE id IN (1, 2, 3) ORDER BY id LIMIT 2",
            "early-stop 2",
        ),
        ("SELECT * FROM c ORDER BY a LIMIT 5", "early-stop 5"),
        ("SELECT * FROM c ORDER BY a, b LIMIT 5", "early-stop 5"),
        ("SELECT * FROM c WHERE a = 1 ORDER BY a, b LIMIT 5", "early-stop 5"),
        ("SELECT * FROM tv ORDER BY id LIMIT 5", "early-stop 5"),
        // Not a leading run of the key, not ascending, or not the column itself.
        ("SELECT * FROM c ORDER BY b LIMIT 5", "top-5"),
        ("SELECT * FROM c ORDER BY b, a LIMIT 5", "top-5"),
        ("SELECT * FROM c ORDER BY a, b DESC LIMIT 5", "top-5"),
        ("SELECT * FROM t ORDER BY id DESC LIMIT 5", "top-5"),
        ("SELECT * FROM t ORDER BY id + 0 LIMIT 5", "top-5"),
        ("SELECT * FROM t ORDER BY id, v LIMIT 5", "top-5"),
        // An index walk yields its rows in the index's order.
        ("SELECT * FROM t WHERE v = 3 ORDER BY id LIMIT 5", "top-5"),
    ] {
        let lines = explain(&cat, sql);
        assert_eq!(
            lines[4],
            format!("order/limit: server {cut}, client sort, client window"),
            "`{sql}`"
        );
    }
}

// ── Read shapes ──────────────────────────────────────────────────────────────

/// Every ad-hoc read shape plans to the sink it belongs on, reads the relation it
/// names, ships a spec, and says whether a request goes out at all.
#[test]
fn every_read_shape_plans_to_its_sink() {
    let cat = base();
    for (sql, fold, dispatches) in [
        ("SELECT * FROM t", false, true),
        ("SELECT id FROM t", false, true),
        ("SELECT * FROM t WHERE id = 3", false, true),
        ("SELECT * FROM t WHERE v = 3", false, true),
        ("SELECT id FROM t ORDER BY v LIMIT 5 OFFSET 2", false, true),
        ("SELECT id FROM t LIMIT 0", false, false),
        ("SELECT COUNT(*) FROM t", true, true),
        ("SELECT g, SUM(v) AS s FROM t GROUP BY g", true, true),
        ("SELECT DISTINCT g FROM t", true, true),
        ("SELECT DISTINCT g, v FROM t", true, true),
        ("SELECT DISTINCT id + 1 FROM t", true, true),
        // Rows already a set: the DISTINCT drops, and they read as rows.
        ("SELECT DISTINCT id, v FROM t", false, true),
        ("SELECT DISTINCT v, id FROM t ORDER BY v LIMIT 3", false, true),
        ("SELECT DISTINCT * FROM t", false, true),
        ("SELECT ALL v FROM t", false, true),
        ("SELECT ALL g, COUNT(*) FROM t GROUP BY g", true, true),
        ("SELECT COUNT(*) FROM t LIMIT 0", true, false),
        ("WITH c AS (SELECT * FROM t) SELECT id FROM c", false, true),
    ] {
        let plan = read(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        let lines = explain_lines(&plan, false);
        assert_eq!(lines[0], "read table t", "`{sql}`: the relation it reads");
        let shape = if fold { "fold:" } else { "projection:" };
        assert!(lines[3].starts_with(shape), "`{sql}`: {}", lines[3]);
        assert_eq!(
            lines[4] == "order/limit: no request (LIMIT 0)",
            !dispatches,
            "`{sql}`: {}",
            lines[4]
        );
        assert!(plan.spec().is_some(), "`{sql}`: every relation read ships a spec");
    }
}

/// A DISTINCT drops only over a set read whole on its key: a compound key needs
/// every column, and a view is a set only where its PK does not repeat.
#[test]
fn a_distinct_reads_as_rows_only_over_a_sets_whole_key() {
    let cat = cat();
    let tv = cat.probe_relation(&in_sn("tv")).unwrap();
    let bag = gnitz_core::RelDescriptor {
        tid: 22,
        class: RelClass::View,
        pk_repeats: true,
        serial: false,
        schema: std::sync::Arc::clone(&tv.schema),
        indexes: Vec::new(),
        token: 0,
    };
    cat.insert(&in_sn("bv"), Some(std::sync::Arc::new(bag)));
    for (sql, shape) in [
        ("SELECT DISTINCT a, b FROM c", "projection:"),
        ("SELECT DISTINCT a, x FROM c", "fold: distinct on (a, x)"),
        ("SELECT DISTINCT * FROM tv", "projection:"),
        ("SELECT DISTINCT v, w FROM tv", "fold: distinct on (v, w)"),
        ("SELECT DISTINCT * FROM bv", "fold: distinct on (id, v, w)"),
    ] {
        let line = &explain(&cat, sql)[3];
        assert!(line.starts_with(shape), "`{sql}`: {line}");
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
        assert_eq!(p.spec(), b.spec(), "`{parens}`");
        assert_eq!(visible(p.reply_schema()), visible(b.reply_schema()), "`{parens}`");
    }
    // Peeling the wrapper must not turn a computed item into a column reference.
    let s = read(&cat, "SELECT (v + 1) AS x FROM t").unwrap();
    assert_eq!(visible(s.reply_schema()), ["x"]);
}

fn visible(s: &gnitz_core::Schema) -> Vec<String> {
    s.visible_columns().map(|(_, c)| c.name.to_lowercase()).collect()
}

/// The reply schema a projection produces: the SELECT list in order over the
/// source PK, which the first item copying it names and which rides hidden in
/// front when none does, so `* EXCEPT (id)` keeps its key.
#[test]
fn a_reads_reply_schema_is_the_select_list_over_the_source_pk() {
    let cat = base();
    // `(statement, visible columns, the key's column, whether it is hidden)`.
    for (sql, want, key, hidden) in [
        ("SELECT * EXCEPT (g) FROM t", vec!["id", "v"], 0, false),
        ("SELECT * EXCEPT (id) FROM t", vec!["g", "v"], 0, true),
        ("SELECT * RENAME (g AS x) FROM t", vec!["id", "x", "v"], 0, false),
        ("SELECT t.g AS x, t.id FROM t", vec!["x", "id"], 1, false),
        ("SELECT g, id AS k, id FROM t", vec!["g", "k", "id"], 1, false),
        ("SELECT id + 0 AS k, g FROM t", vec!["k", "g"], 0, true),
    ] {
        let s = read(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        let schema = s.reply_schema();
        assert_eq!(visible(schema), want, "`{sql}`");
        assert_eq!(schema.pk_cols, [key], "`{sql}`");
        assert_eq!(schema.columns[key as usize].is_hidden, hidden, "`{sql}`");
        assert_eq!(schema.columns.len(), want.len() + usize::from(hidden), "`{sql}`");
    }
}

/// `SELECT *` over a table at the column cap reads the table's own layout.
#[test]
fn a_reproducing_read_at_the_column_cap_ships_no_program() {
    let i = TypeCode::I64;
    let mut cols = vec![col("id", i)];
    cols.extend((1..gnitz_wire::MAX_COLUMNS).map(|n| col(&format!("c{n}"), i)));
    let wide = table(70, cols, vec![0]);
    let cat = catalog(vec![("wide", std::sync::Arc::clone(&wide))]);
    let plan = read(&cat, "SELECT * FROM wide").unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(plan.reply_schema(), wide.schema.as_ref());
    assert!(plan.spec().expect("a relation read").sink.map.is_none());
}

// ── Rejections ───────────────────────────────────────────────────────────────

/// Reads rejected at plan time, each with the guard that owns it.
#[test]
fn a_read_the_planner_rejects_names_its_rule() {
    let cat = cat();
    // A join view whose two sides both carry `id` and `val`.
    let l = table(30, vec![col("id", TypeCode::I64), col("val", TypeCode::I64)], vec![0]);
    let r = table(31, vec![col("id", TypeCode::I64), col("val", TypeCode::I64)], vec![0]);
    cat.insert(&in_sn("l"), Some(l));
    cat.insert(&in_sn("r"), Some(r));
    let jv = view(&cat, "SELECT * FROM l JOIN r ON l.val = r.val");
    register(&cat, "jv", 32, jv.props.into(), final_view(&jv));
    let st = rel(33, RelClass::Stream, vec![col("id", TypeCode::I64)], vec![0], vec![]);
    cat.insert(&in_sn("st"), Some(st));

    for (sql, msg) in [
        // A query deriving a new relation is refused from the AST alone, naming
        // the construct and pointing at CREATE VIEW.
        ("SELECT t.id FROM t JOIN u ON t.v = u.k", "(JOIN)"),
        ("SELECT v FROM t UNION SELECT k FROM u", "(set operation)"),
        (
            "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.v)",
            "(EXISTS/IN subquery)",
        ),
        ("SELECT id FROM t WHERE v IN (SELECT k FROM u)", "(EXISTS/IN subquery)"),
        ("SELECT id, (SELECT MAX(k) FROM u) FROM t", "(scalar subquery)"),
        ("SELECT x FROM (SELECT v AS x FROM t) d", "(derived table in FROM)"),
        ("SELECT t.id FROM t, u WHERE t.v = u.k", "CREATE VIEW"),
        // A clause the parser accepts and the read path would otherwise drop.
        ("SELECT DISTINCT ON (id) id FROM t", "DISTINCT ON"),
        ("SELECT * FROM t LIMIT 2 BY v", "LIMIT: BY is not supported"),
        ("SELECT * FROM t LIMIT 1+1", "LIMIT must be an integer literal"),
        (
            "SELECT * FROM t ORDER BY v WITH FILL",
            "ORDER BY: WITH FILL is not supported",
        ),
        (
            "SELECT * FROM t ORDER BY v INTERPOLATE (v AS v + 1)",
            "ORDER BY: INTERPOLATE is not supported",
        ),
        (
            "SELECT * FROM t LIMIT 1 OFFSET 1+1",
            "OFFSET must be an integer literal",
        ),
        ("SELECT * FROM t FETCH FIRST 2 ROWS ONLY", "FETCH"),
        ("SELECT id FROM t PREWHERE v > 5", "PREWHERE"),
        ("SELECT TOP 2 id FROM t", "TOP"),
        (
            "SELECT id FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1",
            "QUALIFY",
        ),
        ("SELECT id FROM t FOR UPDATE", "FOR UPDATE"),
        ("SELECT id FROM t SETTINGS max_threads = 1", "SETTINGS"),
        ("SELECT id FROM t FORMAT JSON", "FORMAT"),
        ("SELECT * REPLACE (v + 1 AS v) FROM t", "REPLACE"),
        ("SELECT * ILIKE 'v%' FROM t", "ILIKE"),
        // Zero rows would make a stream indistinguishable from an empty table.
        ("SELECT * FROM st", "is a stream"),
        ("WITH c AS (SELECT id FROM st) SELECT id FROM c", "is a stream"),
        ("SELECT w, COUNT(*) FROM tw GROUP BY w HAVING w > 5", "128-bit"),
        ("WITH x AS (SELECT DISTINCT v FROM t) SELECT v FROM x", "(DISTINCT CTE)"),
        ("SELECT v AS x, w AS x FROM t", "duplicate column name 'x'"),
        ("SELECT DISTINCT v AS x, w AS x FROM t", "duplicate column name 'x'"),
        ("SELECT id, v, v FROM t", "duplicate column name 'v'"),
        ("SELECT *, * FROM t", "duplicate column name"),
        ("SELECT *, * FROM t WHERE v > 0", "duplicate column name"),
        ("SELECT DISTINCT *, * FROM t", "duplicate column name"),
        ("SELECT t.*, t.* FROM t", "duplicate column name"),
        // `AS d(x, y)` renames the columns positionally; honoring only the
        // relation alias would answer under `t`'s own column names.
        ("SELECT * FROM t AS d(x, y)", "positional column aliases"),
        // The FROM name is checked before the clause gate, on either sink.
        (
            "SELECT DISTINCT ON (v) * FROM t AS d(x, y)",
            "positional column aliases",
        ),
        // ORDER BY parses before the SELECT list binds, on every surface.
        ("SELECT nope FROM t ORDER BY 1.5", "ORDER BY position"),
        ("SELECT id FROM jv", "is ambiguous"),
        ("SELECT * FROM jv WHERE id = 5", "is ambiguous"),
        ("SELECT _join_pk FROM jv", "not found"),
        // A window call in an ad-hoc grouped SELECT list: windows are a view-body
        // feature, so the leaf's own rejection answers rather than the fold
        // lowering's internal error.
        (
            "SELECT v, ROW_NUMBER() OVER (ORDER BY v) FROM t GROUP BY v",
            "only supported in the SELECT list, QUALIFY and ORDER BY of a CREATE VIEW",
        ),
        (
            "SELECT v, SUM(w) OVER (PARTITION BY v) FROM t GROUP BY v",
            "only supported in the SELECT list, QUALIFY and ORDER BY of a CREATE VIEW",
        ),
        (
            "SELECT id, RANK() OVER (ORDER BY v) FROM t",
            "only supported in the SELECT list, QUALIFY and ORDER BY of a CREATE VIEW",
        ),
        // The fold is one stateless pass over one scan: no DISTINCT segment.
        ("SELECT v, COUNT(DISTINCT w) FROM t GROUP BY v", "CREATE VIEW body only"),
    ] {
        assert_rejects(sql, read(&cat, sql), msg);
    }
    // The CTE rejection names the offending clause too.
    let m = rejected(read(&cat, "WITH x AS (SELECT DISTINCT v FROM t) SELECT v FROM x"));
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
        ("WITH x AS (SELECT * FROM t) SELECT x.* FROM x", "SELECT t.* FROM t"),
        // An ORDER BY key names the CTE's column, whatever the source calls it.
        (
            "WITH x AS (SELECT id, g AS v, v AS g FROM t) SELECT id FROM x ORDER BY v",
            "SELECT id FROM t ORDER BY g",
        ),
        (
            "WITH x AS (SELECT id, g AS v, v AS g FROM t) SELECT v FROM x ORDER BY x.g",
            "SELECT g AS v FROM t ORDER BY t.v",
        ),
    ] {
        let c = read(&cat, cte).unwrap_or_else(|e| panic!("`{cte}`: {e:?}"));
        let f = read(&cat, flat).unwrap_or_else(|e| panic!("`{flat}`: {e:?}"));
        assert_eq!(explain_lines(&c, false), explain_lines(&f, false), "`{cte}`");
        assert_eq!(c.spec(), f.spec(), "`{cte}`");
        assert_eq!(visible(c.reply_schema()), visible(f.reply_schema()), "`{cte}`");
    }
    for (sql, msg) in [
        ("WITH x AS (SELECT id FROM t) SELECT v FROM x", "column 'v' not found"),
        (
            "WITH x AS (SELECT id FROM t) SELECT t.id FROM x",
            "table alias 't' not found",
        ),
        (
            "WITH x AS (SELECT id, v FROM t) SELECT id FROM x ORDER BY g",
            "column 'g' not found",
        ),
        // A qualified ORDER BY key names a relation's column, never an output
        // column, so its qualifier is checked.
        ("SELECT v FROM t ORDER BY zzz.v", "table alias 'zzz' not found"),
        ("SELECT v AS w FROM t ORDER BY t.w", "'w' not found"),
        (
            "WITH x(i) AS (SELECT id, v FROM t) SELECT i FROM x",
            "1 column aliases but body returns 2",
        ),
        (
            "WITH x AS (SELECT v, v FROM t) SELECT v FROM x",
            "duplicate column name 'v'",
        ),
        (
            "WITH x AS (SELECT g, COUNT(*) AS n FROM t GROUP BY g) SELECT n, COUNT(*) FROM x GROUP BY n",
            "derives a new one (an aggregate over a grouped CTE)",
        ),
        (
            "WITH x AS (SELECT t.id FROM t JOIN u ON t.g = u.g) SELECT id FROM x",
            "derives a new one (JOIN)",
        ),
    ] {
        assert_rejects(sql, read(&cat, sql), msg);
    }

    // A join view surfaces both sides' `v`: a wildcard carries the duplicate
    // through positionally, and only naming it is ambiguous.
    let i = TypeCode::I64;
    let jv = catalog(vec![(
        "jv",
        rel(
            30,
            RelClass::View,
            vec![col("id", i), col("v", i), col("v", i)],
            vec![0],
            vec![],
        ),
    )]);
    let (cte, flat) = ("WITH x AS (SELECT * FROM jv) SELECT * FROM x", "SELECT * FROM jv");
    let c = read(&jv, cte).unwrap_or_else(|e| panic!("`{cte}`: {e:?}"));
    let f = read(&jv, flat).unwrap_or_else(|e| panic!("`{flat}`: {e:?}"));
    assert_eq!(c.spec(), f.spec(), "`{cte}`");
    // Defining one over the duplicate is fine — the CTE names no output column
    // of its own, so nothing is duplicated until something reads it by name.
    let ok = "WITH x AS (SELECT * EXCEPT (id) FROM jv) SELECT 1 AS one FROM x";
    read(&jv, ok).unwrap_or_else(|e| panic!("`{ok}`: {e:?}"));
    for sql in [
        "SELECT v FROM jv",
        "WITH x AS (SELECT * EXCEPT (id) FROM jv) SELECT v FROM x",
    ] {
        assert_rejects(sql, read(&jv, sql), "'v' is ambiguous");
    }
    // `*` over such a CTE carries the duplicate through positionally, as the flat
    // wildcard does.
    let (cte, flat) = (
        "WITH x AS (SELECT * EXCEPT (id) FROM jv) SELECT * FROM x",
        "SELECT * EXCEPT (id) FROM jv",
    );
    assert_eq!(read(&jv, cte).unwrap().spec(), read(&jv, flat).unwrap().spec());
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
    assert_rejects(&big, read(&cat, &big), "column references");
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
            "projection: 1 column (+1 for ordering)",
        ),
        (
            "SELECT id FROM t ORDER BY v, g + v DESC",
            "projection: 1 column (+2 for ordering)",
        ),
        // One appended column per *distinct* key program — a duplicate would be
        // evaluated by the worker and shipped per row.
        (
            "SELECT id FROM t ORDER BY v + 1, v + 1",
            "projection: 1 column (+1 for ordering)",
        ),
        ("SELECT id, v + 1 AS w FROM t ORDER BY v + 1", "projection: 2 columns"),
        ("SELECT id, v + 1 AS w FROM t ORDER BY w", "projection: 2 columns"),
        ("SELECT id FROM t ORDER BY t.id", "projection: 1 column"),
        ("SELECT v FROM t ORDER BY id", "projection: 1 column"),
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
        "SELECT DISTINCT v + 1 AS w FROM t ORDER BY v + 1",
    ] {
        assert!(explain(&cat, sql)[3].starts_with("fold:"), "`{sql}`");
    }
    // An aggregate named only in ORDER BY is collected into the reduce: the
    // partial reply carries its accumulator behind the group column, its key.
    let plan = read(&cat, "SELECT g FROM t GROUP BY g ORDER BY COUNT(*)").unwrap();
    let names: Vec<&str> = plan.reply_schema().columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["g", "_agg"]);
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
        assert_eq!(visible(plan.reply_schema()), visible(twin.reply_schema()), "`{sql}`");
        assert_eq!(explain(&cat, sql), explain(&cat, named), "`{sql}`");
    }
    for (sql, msg) in [
        (
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY v",
            "ORDER BY: column 'v' must appear in GROUP BY or an aggregate function",
        ),
        (
            "SELECT DISTINCT v FROM t ORDER BY v + 1",
            "SELECT projection: an ORDER BY key under SELECT DISTINCT must be a selected column",
        ),
        ("SELECT id FROM t ORDER BY nope + 1", "column 'nope' not found"),
    ] {
        assert_rejects(sql, read(&cat, sql), msg);
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
        ("WITH x AS (SELECT 40 + 2 AS a) SELECT a + 1 AS b FROM x", vec!["b"]),
    ] {
        let plan = read(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        assert_eq!(explain(&cat, sql)[0], "read nothing (constant row)", "`{sql}`");
        assert!(plan.spec().is_none(), "`{sql}`");
        assert_eq!(visible(plan.reply_schema()), cols, "`{sql}`");
    }
    assert_eq!(
        explain(&cat, "SELECT 1 AS a ORDER BY 1 + 1 LIMIT 1"),
        [
            "read nothing (constant row)",
            "access: none",
            "predicate: none",
            "projection: 1 column",
            "order/limit: client window",
        ]
    );
    for (sql, msg) in [
        ("SELECT x", "column 'x' not found"),
        ("SELECT 1 + t.x", "table alias 't' not found"),
        ("SELECT *", "SELECT * is not a supported SELECT item"),
        ("SELECT 1 WHERE 1 = 1", "SELECT without FROM: WHERE is not supported"),
        ("SELECT COUNT(*)", "aggregate"),
        ("SELECT 1 AS a, 2 AS a", "duplicate column name 'a'"),
        ("SELECT 1 AS a ORDER BY 2", "ORDER BY position 2 is out of range"),
        ("SELECT 1 AS a ORDER BY x", "column 'x' not found"),
        ("SELECT (SELECT 1)", "scalar subquery"),
    ] {
        assert_rejects(sql, read(&cat, sql), msg);
    }
}

// ── Resolution ───────────────────────────────────────────────────────────────

/// A plan resolves one name per distinct relation it reads, and none for a name
/// only the AST mentions. An absent name is not-found; a rejection raised before
/// the first resolve costs nothing.
#[test]
fn a_plan_resolves_only_the_relations_it_reads() {
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
        let stmt = parse_stmt(sql);
        match &stmt {
            Statement::CreateView(_) => {
                let (plan, asked) = resolving(&known, |c| plan(c, sql).map(|_| ()));
                plan.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
                assert_eq!(asked, want, "`{sql}`");
            }
            _ => {
                let (plan, asked) = resolving(&known, |c| read(c, sql).map(|_| ()));
                plan.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
                assert_eq!(asked, want, "`{sql}`");
            }
        }
    }

    let e = read(&catalog(vec![]), "SELECT id FROM t").err();
    assert!(
        matches!(
            e,
            Some(GnitzSqlError::Client(ClientError::Refused(WireFault {
                status: WireStatus::NotFound,
                ..
            })))
        ),
        "got {e:?}"
    );

    for sql in [
        "SELECT t.id FROM t JOIN u ON t.g = u.g",
        "SELECT id FROM _seg4096",
        "CREATE VIEW v AS SELECT * FROM _seg4096",
    ] {
        let stmt = parse_stmt(sql);
        let (plan, asked) = resolving(&known, |c| match &stmt {
            Statement::CreateView(_) => plan(c, sql).map(|_| ()),
            _ => read(c, sql).map(|_| ()),
        });
        rejected(plan);
        assert!(asked.is_empty(), "`{sql}` costs no resolve, asked: {asked:?}");
    }
}

/// A grouped CTE is the fold its body is: what the query filters and projects
/// over it is that fold's HAVING and finalize.
#[test]
fn a_grouped_cte_reads_as_its_fold() {
    let cat = base();
    for (cte, flat) in [
        (
            "WITH x AS (SELECT g, COUNT(*) AS n FROM t GROUP BY g) SELECT g, n FROM x",
            "SELECT g, COUNT(*) AS n FROM t GROUP BY g",
        ),
        (
            "WITH x AS (SELECT g, SUM(v) AS s FROM t WHERE id > 3 GROUP BY g) SELECT g, s * 2 AS d FROM x WHERE s > 10 ORDER BY g",
            "SELECT g, SUM(v) * 2 AS d FROM t WHERE id > 3 GROUP BY g HAVING SUM(v) > 10 ORDER BY g",
        ),
    ] {
        let c = read(&cat, cte).unwrap_or_else(|e| panic!("`{cte}`: {e:?}"));
        let f = read(&cat, flat).unwrap_or_else(|e| panic!("`{flat}`: {e:?}"));
        assert_eq!(explain_lines(&c, false), explain_lines(&f, false), "`{cte}`");
        assert_eq!(c.spec(), f.spec(), "`{cte}`");
        assert_eq!(visible(c.reply_schema()), visible(f.reply_schema()), "`{cte}`");
    }
}
