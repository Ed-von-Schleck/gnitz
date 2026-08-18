#![cfg(feature = "integration")]

//! `(a)` is `a`. SQL lets any column reference carry redundant parentheses, and
//! the parser keeps them as an `Expr::Nested` wrapper. Every surface that
//! recognises a bare column reference must see through that wrapper — otherwise
//! the same clause is accepted on one surface and rejected on another, which is
//! what these pin.

use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// `(id BIGINT UNSIGNED pk, g BIGINT, v BIGINT)` plus three rows.
fn seed(client: &mut gnitz_core::GnitzClient, sn: &str) {
    exec(
        client,
        sn,
        "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, g BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        client,
        sn,
        "INSERT INTO t (id, g, v) VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300)",
    );
}

/// The read surfaces: a projection item, an ORDER BY key, and an
/// INSERT … RETURNING item.
#[test]
fn a_parenthesized_column_reference_reads_like_a_bare_one() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    for (parens, bare) in [
        ("SELECT (v) FROM t WHERE id = 1", "SELECT v FROM t WHERE id = 1"),
        ("SELECT v FROM t ORDER BY (v)", "SELECT v FROM t ORDER BY v"),
        ("SELECT (t.v) FROM t WHERE id = 1", "SELECT t.v FROM t WHERE id = 1"),
    ] {
        let (ps, pb) = read_sql(&mut client, &sn, parens);
        let (bs, bb) = read_sql(&mut client, &sn, bare);
        assert_eq!(visible_names(&ps), visible_names(&bs), "{parens}");
        assert_eq!(pb.len(), bb.len(), "{parens}");
    }

    // RETURNING must carry a PK column (`resolve_projection`'s own rule), so the
    // pair is what a parenthesised and a bare list are compared on.
    let (schema, batch) = read_sql(
        &mut client,
        &sn,
        "INSERT INTO t (id, g, v) VALUES (9, 90, 900) RETURNING (id), (v)",
    );
    assert_eq!(visible_names(&schema), vec!["id".to_string(), "v".to_string()]);
    assert_eq!(batch.len(), 1);
}

/// `GROUP BY (g)` on the three surfaces that each own a grouped front end: the
/// ad-hoc fold, a linear view, and a JOIN view. All three accept it, and the
/// ad-hoc fold agrees with its view twin on the rows.
#[test]
fn group_by_a_parenthesized_column_is_accepted_on_every_surface() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE u (id BIGINT UNSIGNED PRIMARY KEY, g BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "INSERT INTO u (id, g) VALUES (1, 10), (3, 20)");

    let (_, adhoc) = read_sql(&mut client, &sn, "SELECT (g), COUNT(*) AS n FROM t GROUP BY (g)");

    exec(
        &mut client,
        &sn,
        "CREATE VIEW v_linear AS SELECT (g) AS g, COUNT(*) AS n FROM t GROUP BY (g)",
    );
    let (_, linear) = read_view(&mut client, &sn, "v_linear");
    assert_eq!(
        adhoc.len(),
        linear.len(),
        "ad-hoc and linear-view GROUP BY (g) disagree"
    );

    exec(
        &mut client,
        &sn,
        "CREATE VIEW v_join AS SELECT (t.g) AS g, COUNT(*) AS n FROM t JOIN u ON t.id = u.id GROUP BY (t.g)",
    );
    let (_, joined) = read_view(&mut client, &sn, "v_join");
    assert_eq!(
        joined.len(),
        2,
        "JOIN-view GROUP BY (t.g) must group the two matched rows"
    );
}

/// Peeling the wrapper must not turn a *computed* expression into a column
/// reference: `(v + 1)` still computes, and an unknown name inside a
/// parenthesised item is a `Bind` error naming the column, not a shape rejection.
#[test]
fn parentheses_do_not_make_a_computed_item_a_column_reference() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    seed(&mut client, &sn);

    let (schema, batch) = read_sql(&mut client, &sn, "SELECT (v + 1) AS x FROM t WHERE id = 1");
    assert_eq!(visible_names(&schema), vec!["x".to_string()]);
    assert_eq!(i64_at(&batch, col_idx(&schema, "x"), 0), 101);

    assert_rejects_variant(
        &mut client,
        &sn,
        "INSERT INTO t (id, g, v) VALUES (8, 80, 800) RETURNING (nope)",
        "Bind",
        "nope",
    );
}
