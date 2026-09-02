//! The output schema a `CREATE VIEW` body compiles to — column names, hidden
//! key slots, nullability, type codes and the PK column set — planned with no
//! server against a hand-built catalog.

use gnitz_core::{RelClass, TypeCode, PK_LIST_MAX_COLS};

mod pure;
use pure::*;

type Shape = Vec<(String, bool, bool)>;

/// `(name, hidden, nullable)` rows from string literals.
fn sh(cols: &[(&str, bool, bool)]) -> Shape {
    cols.iter().map(|&(n, h, nl)| (n.to_string(), h, nl)).collect()
}

/// The final view's PK column set.
fn pk(chain: &gnitz_sql::PlannedChain) -> Vec<u32> {
    final_view(chain).pk_cols.clone()
}

/// The final view's output type codes.
fn types(chain: &gnitz_sql::PlannedChain) -> Vec<TypeCode> {
    final_view(chain).output_columns.iter().map(|c| c.type_code).collect()
}

// ── linear projection ────────────────────────────────────────────────────────

/// The full source PK is pinned to the leading slots in PK order; a projected
/// PK column moves there (shifting, not swapping, what it passes), an omitted
/// one rides hidden, a duplicate reference is a payload copy, and a computed
/// item is nullable and named by its raw projection index.
#[test]
fn a_projection_places_the_source_pk_first() {
    let i = TypeCode::I64;
    let s = TypeCode::String;
    let cat = catalog(vec![
        (
            "late",
            table(30, vec![ncol("name", s), ncol("age", i), col("id", i)], vec![2]),
        ),
        (
            "mid",
            table(31, vec![ncol("name", s), col("id", i), ncol("age", i)], vec![1]),
        ),
        ("tn", table(32, vec![col("id", i), ncol("name", s)], vec![0])),
        (
            "cab",
            table(33, vec![col("a", i), col("b", i), ncol("c", s)], vec![0, 1]),
        ),
        (
            "cabn",
            table(34, vec![col("a", i), col("b", i), col("c", i)], vec![0, 1]),
        ),
        ("tab", table(35, vec![col("id", i), col("a", i), col("b", i)], vec![0])),
    ]);
    let rows: &[(&str, Shape, &[u32])] = &[
        (
            "SELECT name, age, id FROM late",
            sh(&[("id", false, false), ("name", false, true), ("age", false, true)]),
            &[0],
        ),
        (
            "SELECT * FROM late",
            sh(&[("id", false, false), ("name", false, true), ("age", false, true)]),
            &[0],
        ),
        (
            "SELECT name, age, id FROM mid",
            sh(&[("id", false, false), ("name", false, true), ("age", false, true)]),
            &[0],
        ),
        (
            "SELECT id AS employee_id, name FROM tn",
            sh(&[("employee_id", false, false), ("name", false, true)]),
            &[0],
        ),
        (
            "SELECT t.id, t.name FROM tn AS t",
            sh(&[("id", false, false), ("name", false, true)]),
            &[0],
        ),
        (
            "SELECT (id), name FROM tn",
            sh(&[("id", false, false), ("name", false, true)]),
            &[0],
        ),
        (
            "SELECT id + 1, name FROM tn",
            sh(&[("id", true, false), ("_expr0", false, true), ("name", false, true)]),
            &[0],
        ),
        (
            "SELECT id, id AS id2, name FROM tn",
            sh(&[("id", false, false), ("id2", false, false), ("name", false, true)]),
            &[0],
        ),
        (
            "SELECT id, id AS id2 FROM tn",
            sh(&[("id", false, false), ("id2", false, false)]),
            &[0],
        ),
        (
            "SELECT b, c, a FROM cab",
            sh(&[("a", false, false), ("b", false, false), ("c", false, true)]),
            &[0, 1],
        ),
        (
            "SELECT a, b, c + 1 FROM cabn",
            sh(&[("a", false, false), ("b", false, false), ("_expr2", false, true)]),
            &[0, 1],
        ),
        (
            "SELECT c FROM cab",
            sh(&[("a", true, false), ("b", true, false), ("c", false, true)]),
            &[0, 1],
        ),
        (
            "SELECT (a), c FROM cab",
            sh(&[("a", false, false), ("b", true, false), ("c", false, true)]),
            &[0, 1],
        ),
        (
            "SELECT a + 1 AS x FROM tab",
            sh(&[("id", true, false), ("x", false, true)]),
            &[0],
        ),
        (
            "SELECT *, a + b FROM tab",
            sh(&[
                ("id", false, false),
                ("a", false, false),
                ("b", false, false),
                ("_expr1", false, true),
            ]),
            &[0],
        ),
    ];
    for (body, shape, pk_cols) in rows {
        let chain = view(&cat, body);
        assert_eq!(&output_shape(&chain), shape, "{body}");
        assert_eq!(&pk(&chain), pk_cols, "{body}");
    }
}

/// A view over a compound-PK view inherits the compound PK.
#[test]
fn a_view_over_a_compound_pk_view_keeps_its_pk() {
    let i = TypeCode::I64;
    let mut cat = catalog(vec![(
        "base",
        table(
            30,
            vec![col("a", i), col("b", i), ncol("c", TypeCode::String)],
            vec![0, 1],
        ),
    )]);
    let v1 = view(&cat, "SELECT b, c, a FROM base");
    assert_eq!(pk(&v1), [0, 1]);
    register(&mut cat, "v1", 40, &v1);
    let v2 = view(&cat, "SELECT a, b FROM v1");
    assert_eq!(output_shape(&v2), sh(&[("a", false, false), ("b", false, false)]));
    assert_eq!(pk(&v2), [0, 1]);
}

// ── wildcard modifiers ───────────────────────────────────────────────────────

/// `EXCEPT`/`EXCLUDE` drop every visible column of that name, `RENAME`
/// relabels, and a dropped source PK still rides hidden. A modifier-bearing
/// `*` in a CTE body is not an identity pass-through.
#[test]
fn wildcard_modifiers_rewrite_the_expansion() {
    let i = TypeCode::I64;
    let cat = catalog(vec![
        (
            "w",
            table(
                30,
                vec![col("id", i), ncol("name", TypeCode::String), ncol("age", i)],
                vec![0],
            ),
        ),
        ("ja", table(31, vec![col("id", i), col("k", i), col("av", i)], vec![0])),
        ("jb", table(32, vec![col("id", i), col("k", i), col("bv", i)], vec![0])),
        (
            "l",
            table(33, vec![col("id", i), ncol("val", i), ncol("x", i)], vec![0]),
        ),
        ("r", table(34, vec![col("id", i), ncol("val", i)], vec![0])),
        ("sa", table(35, vec![col("id", i), col("x", i), col("y", i)], vec![0])),
        (
            "da",
            table(36, vec![col("id", i), col("d", TypeCode::F64), col("g", i)], vec![0]),
        ),
        (
            "db",
            table(37, vec![col("id", i), col("d", TypeCode::F64), col("g", i)], vec![0]),
        ),
    ]);
    let rows: &[(&str, Shape, &[u32])] = &[
        (
            "SELECT * EXCEPT (age) FROM w",
            sh(&[("id", false, false), ("name", false, true)]),
            &[0],
        ),
        (
            "SELECT * EXCLUDE (age) FROM w",
            sh(&[("id", false, false), ("name", false, true)]),
            &[0],
        ),
        (
            "SELECT * EXCEPT (id) FROM w",
            sh(&[("id", true, false), ("name", false, true), ("age", false, true)]),
            &[0],
        ),
        (
            "WITH c AS (SELECT * EXCEPT (age) FROM w) SELECT * FROM c",
            sh(&[("id", false, false), ("name", false, true)]),
            &[0],
        ),
        (
            "SELECT * EXCEPT (k) FROM ja JOIN jb ON ja.k = jb.k",
            sh(&[
                ("_join_pk", true, false),
                ("id", false, false),
                ("av", false, false),
                ("id", false, false),
                ("bv", false, false),
            ]),
            &[0],
        ),
        (
            "SELECT * EXCEPT (k, id) RENAME (av AS x) FROM ja JOIN jb ON ja.k = jb.k",
            sh(&[("_join_pk", true, false), ("x", false, false), ("bv", false, false)]),
            &[0],
        ),
        (
            "SELECT * EXCEPT (x) FROM l JOIN r ON l.id = r.id",
            sh(&[
                ("_join_pk", true, false),
                ("id", false, false),
                ("val", false, true),
                ("id", false, false),
                ("val", false, true),
            ]),
            &[0],
        ),
        (
            "SELECT * EXCEPT (d) FROM da UNION SELECT * EXCEPT (d) FROM db",
            sh(&[("_set_pk", true, false), ("id", false, false), ("g", false, false)]),
            &[0],
        ),
        (
            "SELECT DISTINCT * EXCEPT (x) FROM sa",
            sh(&[("_distinct_pk", true, false), ("id", false, false), ("y", false, false)]),
            &[0],
        ),
    ];
    for (body, shape, pk_cols) in rows {
        let chain = view(&cat, body);
        assert_eq!(&output_shape(&chain), shape, "{body}");
        assert_eq!(&pk(&chain), pk_cols, "{body}");
    }
}

// ── CTEs and derived tables ──────────────────────────────────────────────────

/// A CTE's column aliases name its output; an identity body is a pass-through
/// (one segment) while any other body is cut into a hidden segment.
#[test]
fn a_cte_body_is_a_pass_through_only_when_it_is_the_identity() {
    let i = TypeCode::I64;
    let cat = catalog(vec![("ab", table(30, vec![col("a", i), col("b", i)], vec![0]))]);
    let rows: &[(&str, usize, Shape)] = &[
        (
            "WITH cte(alias_a, alias_b) AS (SELECT a, b FROM ab) SELECT alias_a FROM cte",
            1,
            sh(&[("alias_a", false, false)]),
        ),
        (
            "WITH cte AS (SELECT * FROM ab) SELECT a FROM cte",
            1,
            sh(&[("a", false, false)]),
        ),
        (
            "WITH cte AS (SELECT ab.a, ab.b FROM ab) SELECT * FROM cte",
            1,
            sh(&[("a", false, false), ("b", false, false)]),
        ),
        (
            "WITH cte AS (SELECT a FROM ab) SELECT a FROM cte",
            2,
            sh(&[("a", false, false)]),
        ),
        // The same-named derived alias shadows the CTE in the final body.
        (
            "WITH x AS (SELECT a AS ca FROM ab) SELECT * FROM (SELECT b AS cb FROM ab) x",
            3,
            sh(&[("a", true, false), ("cb", false, false)]),
        ),
    ];
    for (body, segments, shape) in rows {
        let chain = view(&cat, body);
        assert_eq!(chain.views.len(), *segments, "{body}");
        assert_eq!(&output_shape(&chain), shape, "{body}");
    }
}

/// Which names the planner asks the catalog for says what a reference bound
/// to: a CTE shadows a same-named base table case-insensitively, a sibling
/// derived table is out of scope (the reference falls through to the
/// catalog), and a CTE outranks a sibling alias.
#[test]
fn a_reference_binds_to_the_cte_before_the_catalog_and_never_to_a_sibling() {
    let i = TypeCode::I64;
    let known = catalog(vec![
        ("t", table(30, vec![col("id", i), col("v", i)], vec![0])),
        ("cc", table(31, vec![col("other", i)], vec![0])),
        ("a", table(32, vec![col("id", i), col("v", i)], vec![0])),
    ]);
    let rows: &[(&str, bool, &[&str])] = &[
        (
            "WITH Cc AS (SELECT id FROM t WHERE id > 0) SELECT id FROM cc",
            true,
            &["t"],
        ),
        (
            "SELECT b.v FROM (SELECT id FROM t) a JOIN (SELECT id, v FROM a) b ON a.id = b.id",
            true,
            &["t", "a"],
        ),
        (
            "WITH a AS (SELECT id, v FROM t WHERE v > 0) \
             SELECT b.v FROM (SELECT id FROM t) a JOIN (SELECT id, v FROM a) b ON a.id = b.id",
            true,
            &["t"],
        ),
        (
            "SELECT b.id FROM (SELECT id FROM t) a JOIN (SELECT id FROM nope) b ON a.id = b.id",
            false,
            &["t", "nope"],
        ),
    ];
    for (body, ok, asked_names) in rows {
        let sql = format!("CREATE VIEW v AS {body}");
        let (r, asked) = resolving(&known, |cat| plan(cat, &sql));
        assert_eq!(r.is_ok(), *ok, "{body}: {:?}", r.err());
        assert_eq!(asked, *asked_names, "{body}");
    }
}

// ── set operations ───────────────────────────────────────────────────────────

/// A set-op view keys on the hashed row under a hidden `_set_pk`; its column
/// is nullable when the operator's tuple algebra admits a NULL from either
/// side (UNION), only from the left (EXCEPT), or from both (INTERSECT).
#[test]
fn a_set_op_column_is_nullable_by_the_operators_algebra() {
    let i = TypeCode::I64;
    let cat = catalog(vec![
        ("nn", table(30, vec![col("id", i), col("v", i)], vec![0])),
        ("nl", table(31, vec![col("id", i), ncol("v", i)], vec![0])),
    ]);
    let rows: &[(&str, bool)] = &[
        ("SELECT v FROM nn INTERSECT SELECT v FROM nl", false),
        ("SELECT v FROM nn EXCEPT SELECT v FROM nl", false),
        ("SELECT v FROM nl EXCEPT SELECT v FROM nn", true),
        ("SELECT v FROM nn UNION SELECT v FROM nl", true),
        ("SELECT v FROM nn UNION ALL SELECT v FROM nl", true),
    ];
    for (body, nullable) in rows {
        let chain = view(&cat, body);
        assert_eq!(
            output_shape(&chain),
            sh(&[("_set_pk", true, false), ("v", false, *nullable)]),
            "{body}"
        );
        assert_eq!(pk(&chain), [0], "{body}");
    }
}

// ── aggregates ───────────────────────────────────────────────────────────────

/// A grouped SUM/MIN/MAX is nullable exactly when its argument is; a global one
/// is always nullable (the empty source seeds a NULL row); AVG always is (its
/// divide null-marks a zero count); COUNT never is. A
/// GROUP BY over the source PK, or a single NOT NULL natural-key column, keys
/// the output on that column; any other group set folds into a hidden
/// `_group_pk` and carries the group columns at their source nullability.
#[test]
fn an_aggregate_output_column_is_typed_by_its_argument_and_grouping() {
    let i = TypeCode::I64;
    let u = TypeCode::U64;
    let cat = catalog(vec![
        ("nx", table(30, vec![col("id", i), col("g", u), ncol("x", i)], vec![0])),
        ("nn", table(31, vec![col("id", i), col("g", u), col("x", i)], vec![0])),
        (
            "m",
            table(32, vec![col("id", i), col("g", u), col("x", TypeCode::F64)], vec![0]),
        ),
        (
            "ck",
            table(33, vec![col("k1", i), col("k2", i), col("v", i)], vec![0, 1]),
        ),
        (
            "evt",
            table(34, vec![col("id", i), ncol("grp", u), col("val", i)], vec![0]),
        ),
        (
            "evtu",
            table(
                35,
                vec![col("id", i), col("grp", TypeCode::UUID), col("val", i)],
                vec![0],
            ),
        ),
        (
            "evts",
            table(36, vec![col("id", i), col("grp", i), col("val", i)], vec![0]),
        ),
    ]);
    let rows: &[(&str, Shape, &[u32], &[TypeCode])] = &[
        (
            "SELECT g, SUM(x) AS sx, MIN(x) AS mnx, MAX(x) AS mxx, COUNT(x) AS cx, COUNT(*) AS ca \
             FROM nx GROUP BY g",
            sh(&[
                ("g", false, false),
                ("sx", false, true),
                ("mnx", false, true),
                ("mxx", false, true),
                ("cx", false, false),
                ("ca", false, false),
            ]),
            &[0],
            &[u, i, i, i, i, i],
        ),
        (
            "SELECT g, SUM(x) AS sx, MIN(x) AS mnx, MAX(x) AS mxx, COUNT(x) AS cx FROM nn GROUP BY g",
            sh(&[
                ("g", false, false),
                ("sx", false, false),
                ("mnx", false, false),
                ("mxx", false, false),
                ("cx", false, false),
            ]),
            &[0],
            &[u, i, i, i, i],
        ),
        (
            "SELECT SUM(x) AS sx, MIN(x) AS mnx, MAX(x) AS mxx FROM nn",
            sh(&[
                ("_group_pk", true, false),
                ("sx", false, true),
                ("mnx", false, true),
                ("mxx", false, true),
            ]),
            &[0],
            &[TypeCode::U128, i, i, i],
        ),
        (
            "SELECT g, SUM(x) AS sx, AVG(x) AS ax FROM m GROUP BY g",
            sh(&[("g", false, false), ("sx", false, false), ("ax", false, true)]),
            &[0],
            &[u, TypeCode::F64, TypeCode::F64],
        ),
        (
            "SELECT g, AVG(x) AS ax FROM nn GROUP BY g",
            sh(&[("g", false, false), ("ax", false, true)]),
            &[0],
            &[u, TypeCode::F64],
        ),
        (
            "SELECT k1, k2, SUM(v) AS total FROM ck GROUP BY k1, k2",
            sh(&[("k1", false, false), ("k2", false, false), ("total", false, false)]),
            &[0, 1],
            &[i, i, i],
        ),
        (
            "SELECT k2 AS kb, k1 AS ka, COUNT(*) AS n FROM ck GROUP BY k2, k1",
            sh(&[("ka", false, false), ("kb", false, false), ("n", false, false)]),
            &[0, 1],
            &[i, i, i],
        ),
        (
            "SELECT id, SUM(x) AS total FROM nn GROUP BY id",
            sh(&[("id", false, false), ("total", false, false)]),
            &[0],
            &[i, i],
        ),
        (
            "SELECT grp, COUNT(*) AS n FROM evt GROUP BY grp",
            sh(&[("_group_pk", true, false), ("grp", false, true), ("n", false, false)]),
            &[0],
            &[TypeCode::U128, u, i],
        ),
        (
            "SELECT grp, COUNT(*) AS n FROM evtu GROUP BY grp",
            sh(&[("grp", false, false), ("n", false, false)]),
            &[0],
            &[TypeCode::UUID, i],
        ),
        (
            "SELECT grp, COUNT(*) AS n FROM evts GROUP BY grp",
            sh(&[("_group_pk", true, false), ("grp", false, false), ("n", false, false)]),
            &[0],
            &[TypeCode::U128, i, i],
        ),
    ];
    for (body, shape, pk_cols, tcs) in rows {
        let chain = view(&cat, body);
        assert_eq!(&output_shape(&chain), shape, "{body}");
        assert_eq!(&pk(&chain), pk_cols, "{body}");
        assert_eq!(&types(&chain), tcs, "{body}");
    }
}

// ── joins ────────────────────────────────────────────────────────────────────

/// A join view's PK is its synthetic `_join_pk` — one hidden column per equi
/// pair, promoted to the narrowest signed type holding both sides; a range
/// join keys on the hidden source-PK pair instead. The payload is the left
/// columns then the right columns, source duplicates included.
#[test]
fn a_join_view_is_keyed_on_its_join_key() {
    let i = TypeCode::I64;
    let key_cols = |n: usize| (0..n).map(|k| col(&format!("c{k}"), i)).collect::<Vec<_>>();
    let mut cat = catalog(vec![
        ("a32", table(30, vec![col("id", i), col("fk", TypeCode::U32)], vec![0])),
        ("a8", table(31, vec![col("id", i), col("fk", TypeCode::U8)], vec![0])),
        ("a64", table(32, vec![col("id", i), col("fk", TypeCode::U64)], vec![0])),
        ("b64", table(33, vec![col("id", i), col("fk", i)], vec![0])),
        ("b16", table(34, vec![col("id", i), col("fk", TypeCode::I16)], vec![0])),
        (
            "ka",
            table(35, [vec![col("id", i)], key_cols(PK_LIST_MAX_COLS)].concat(), vec![0]),
        ),
        (
            "kb",
            table(36, [vec![col("id", i)], key_cols(PK_LIST_MAX_COLS)].concat(), vec![0]),
        ),
        (
            "xa",
            table(37, vec![col("id", i), col("x", i), col("y", i), col("av", i)], vec![0]),
        ),
        (
            "xb",
            table(38, vec![col("id", i), col("x", i), col("y", i), col("bv", i)], vec![0]),
        ),
        ("l", table(39, vec![col("id", i), ncol("val", i)], vec![0])),
        ("r", table(40, vec![col("id", i), ncol("val", i)], vec![0])),
        ("ra", table(41, vec![col("id", i), col("x", i)], vec![0])),
        ("rb", table(42, vec![col("id", i), col("y", i)], vec![0])),
        ("ba", table(43, vec![col("id", i), col("k", i), col("lo", i)], vec![0])),
        ("bb", table(44, vec![col("id", i), col("k", i), col("t", i)], vec![0])),
        ("t1", table(45, vec![col("id", i), col("k", i), col("lo", i)], vec![0])),
        ("t2", table(46, vec![col("id", i), col("k", i), col("lo", i)], vec![0])),
    ]);
    let uv = view(&cat, "SELECT k, lo FROM t1 UNION ALL SELECT k, lo FROM t2");
    register(&mut cat, "uv", 60, &uv);
    let on = (0..PK_LIST_MAX_COLS)
        .map(|k| format!("ka.c{k} = kb.c{k}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let wide = format!("SELECT ka.id AS aid, kb.id AS bid FROM ka JOIN kb ON {on}");
    let join_pks: Vec<(String, bool, bool)> = (0..PK_LIST_MAX_COLS)
        .map(|k| (format!("_join_pk_{k}"), true, false))
        .collect();
    let pair_pks = || sh(&[("_pair_pk_0", true, false), ("_pair_pk_1", true, false)]);

    let rows: &[(&str, Shape, &[u32], &[TypeCode])] = &[
        (
            "SELECT * FROM a32 JOIN b64 ON a32.fk = b64.fk",
            sh(&[
                ("_join_pk", true, false),
                ("id", false, false),
                ("fk", false, false),
                ("id", false, false),
                ("fk", false, false),
            ]),
            &[0],
            &[i, i, TypeCode::U32, i, i],
        ),
        (
            "SELECT a8.id AS l, b16.id AS r FROM a8 JOIN b16 ON a8.fk = b16.fk",
            sh(&[("_join_pk", true, false), ("l", false, false), ("r", false, false)]),
            &[0],
            &[TypeCode::I16, i, i],
        ),
        (
            "SELECT a64.id AS l, b64.id AS r FROM a64 JOIN b64 ON a64.fk = b64.fk",
            sh(&[("_join_pk", true, false), ("l", false, false), ("r", false, false)]),
            &[0],
            &[TypeCode::I128, i, i],
        ),
        (
            &wide,
            [join_pks.clone(), sh(&[("aid", false, false), ("bid", false, false)])].concat(),
            &[0, 1, 2, 3],
            &[i, i, i, i, i, i],
        ),
        (
            "SELECT xa.x, xa.y, xa.av, xb.bv FROM xa JOIN xb ON xa.x = xb.x AND xa.y = xb.y",
            sh(&[
                ("_join_pk_0", true, false),
                ("_join_pk_1", true, false),
                ("x", false, false),
                ("y", false, false),
                ("av", false, false),
                ("bv", false, false),
            ]),
            &[0, 1],
            &[i, i, i, i, i, i],
        ),
        (
            "SELECT * FROM l JOIN r ON l.val = r.val",
            sh(&[
                ("_join_pk", true, false),
                ("id", false, false),
                ("val", false, true),
                ("id", false, false),
                ("val", false, true),
            ]),
            &[0],
            &[i, i, i, i, i],
        ),
        (
            "SELECT ra.id AS aid, rb.id AS bid, rb.y AS ry FROM ra JOIN rb ON ra.x < rb.y",
            [
                pair_pks(),
                sh(&[("aid", false, false), ("bid", false, false), ("ry", false, false)]),
            ]
            .concat(),
            &[0, 1],
            &[i, i, i, i, i],
        ),
        (
            "SELECT ba.id AS aid, bb.id AS bid FROM ba JOIN bb ON ba.k = bb.k AND ba.lo <= bb.t",
            [pair_pks(), sh(&[("aid", false, false), ("bid", false, false)])].concat(),
            &[0, 1],
            &[i, i, i, i],
        ),
        (
            "SELECT ba.id AS aid, bb.id AS bid FROM ba LEFT JOIN bb ON ba.k = bb.k AND ba.lo <= bb.t",
            [pair_pks(), sh(&[("aid", false, false), ("bid", false, true)])].concat(),
            &[0, 1],
            &[i, i, i, i],
        ),
        (
            "SELECT ra.id AS aid, rb.id AS bid FROM ra LEFT JOIN rb ON ra.x < rb.y",
            [pair_pks(), sh(&[("aid", false, false), ("bid", false, true)])].concat(),
            &[0, 1],
            &[i, i, i, i],
        ),
        // A bag-valued preserved side: the UNION ALL view's `_set_pk` is its PK.
        (
            "SELECT uv.k AS a, bb.id AS b FROM uv LEFT JOIN bb ON uv.k = bb.k AND uv.lo <= bb.t",
            [pair_pks(), sh(&[("a", false, false), ("b", false, true)])].concat(),
            &[0, 1],
            &[TypeCode::U128, i, i, i],
        ),
    ];
    for (body, shape, pk_cols, tcs) in rows {
        let chain = view(&cat, body);
        assert_eq!(&output_shape(&chain), shape, "{body}");
        assert_eq!(&pk(&chain), pk_cols, "{body}");
        assert_eq!(&types(&chain), tcs, "{body}");
    }
}

/// A multi-way join is cut into segments, and a segment keeps only the
/// columns something downstream reads: its own key, what the final projection
/// names, and what a later ON clause compares. A column consumed by the
/// segment's own ON, an unreferenced column, and the other side's columns are
/// pruned.
#[test]
fn a_chain_segment_keeps_only_its_live_columns() {
    let i = TypeCode::I64;
    let cat = catalog(vec![
        (
            "ja",
            table(
                30,
                vec![col("id", i), col("k", i), col("v", i), col("dead", i)],
                vec![0],
            ),
        ),
        ("jb", table(31, vec![col("id", i), col("x", i)], vec![0])),
        ("jc", table(32, vec![col("id", i), col("cv", i)], vec![0])),
    ]);
    let chain = view(
        &cat,
        "SELECT ja.id AS aid, jc.cv AS ccv FROM ja JOIN jb ON ja.k = jb.id JOIN jc ON ja.v = jc.id",
    );
    assert_eq!(chain.views.len(), 2);
    let seg: Vec<&str> = chain.views[0].output_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(seg, ["_join_pk", "id", "v"]);
    assert_eq!(
        output_shape(&chain),
        sh(&[("_join_pk", true, false), ("aid", false, false), ("ccv", false, false)])
    );
}

/// `register` mirrors what the server records: a bounded view registers as
/// one, so the leaf rule can be planned against it.
#[test]
fn a_registered_bounded_view_carries_its_class() {
    let mut cat = base();
    let bounded = plan(&cat, "CREATE VIEW v WITH (capacity = '1 MB') AS SELECT id, v FROM t").unwrap();
    register(&mut cat, "bnd", 60, &bounded);
    assert_eq!(cat.get(SN, "bnd").flatten().unwrap().class, RelClass::BoundedView);
}
