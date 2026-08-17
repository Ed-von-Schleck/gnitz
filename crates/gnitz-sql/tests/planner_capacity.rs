#![cfg(feature = "integration")]

//! `CREATE VIEW … WITH (capacity = '…')` — the option's decoding, the shapes it
//! is accepted on, and the two rules that keep a bounded view a leaf: nothing may
//! be created over one, and `ALTER VIEW … AS` may not retarget one.
//!
//! Acceptance here is not just a planner verdict: `CREATE VIEW` compiles the
//! circuit on the master before the bundle is durable, and a bounded view's
//! compile additionally derives its per-key hydration plan from the emitted
//! graph. So every accepted case below is also the drift test for that
//! derivation over a real planner-emitted circuit.

use gnitz_core::GnitzClient;
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// A base pair every test builds on: `t(id, v, s)` and `u(id, tid, w)`.
fn make_tables(client: &mut GnitzClient, sn: &str) {
    exec(
        client,
        sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, s TEXT)",
    );
    exec(
        client,
        sn,
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)",
    );
}

// ── Option decoding ─────────────────────────────────────────────────────────

#[test]
fn capacity_accepts_every_unit_with_and_without_a_space() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    make_tables(&mut client, &sn);
    for (i, lit) in ["16KB", "16 KB", "4mb", "4 MB", "1GB", "1 gb"].iter().enumerate() {
        exec(
            &mut client,
            &sn,
            &format!("CREATE VIEW cap{i} WITH (capacity = '{lit}') AS SELECT id, v FROM t"),
        );
    }
}

#[test]
fn a_malformed_or_unknown_capacity_option_is_rejected() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    make_tables(&mut client, &sn);

    let cases = [
        // Unknown key.
        ("WITH (foo = '1 MB')", "foo"),
        // Not a quoted string.
        ("WITH (capacity = 5)", "single-quoted"),
        // Malformed value.
        ("WITH (capacity = 'lots')", "not a size"),
        ("WITH (capacity = '5')", "not a size"),
        ("WITH (capacity = '5 TB')", "not a size"),
        // Zero and overflow: a store cannot be bounded below its own skeleton.
        ("WITH (capacity = '0 MB')", "out of range"),
        ("WITH (capacity = '18446744073709551615 GB')", "out of range"),
    ];
    for (i, (clause, needle)) in cases.iter().enumerate() {
        let sql = format!("CREATE VIEW bad{i} {clause} AS SELECT id, v FROM t");
        assert_rejects_variant(&mut client, &sn, &sql, "Plan", needle);
        assert!(
            client.resolve_table_or_view_id(&sn, &format!("bad{i}")).is_err(),
            "`{clause}` must not register a view",
        );
    }
}

/// `capacity` is only special inside a `CREATE VIEW` option clause — it stays an
/// ordinary identifier everywhere else.
#[test]
fn capacity_is_not_a_reserved_word() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE capacity (id BIGINT NOT NULL PRIMARY KEY, capacity BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW cv AS SELECT id, capacity FROM capacity WHERE capacity > 3",
    );
}

/// `hir::execute_create_view` stores `format!("{cv}")` as the view's
/// `sql_definition`, and `CreateView`'s `Display` re-emits the `WITH (...)`
/// clause — so the definition round-trips the capacity, and re-creating from it
/// yields a bounded view again.
#[test]
fn the_stored_definition_round_trips_the_with_clause() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    make_tables(&mut client, &sn);
    let sql = "CREATE VIEW b WITH (capacity = '2 MB') AS SELECT id, v FROM t WHERE v > 1";
    exec(&mut client, &sn, sql);

    // The exact text the planner stores.
    let stmt = &sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, sql).unwrap()[0];
    let stored = format!("{stmt}");
    assert!(
        stored.to_lowercase().contains("capacity"),
        "the rendered definition must carry the WITH clause: {stored}",
    );

    // Re-creating from it yields a bounded view: the leaf rule refuses anything
    // over the result.
    exec(&mut client, &sn, &stored.replacen(" b ", " b2 ", 1));
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE VIEW over_b2 AS SELECT id FROM b2",
        "Unsupported",
        "capacity-bounded",
    );
}

// ── Eligible / ineligible shapes ────────────────────────────────────────────

#[test]
fn the_two_eligible_shapes_are_accepted() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    make_tables(&mut client, &sn);
    exec(&mut client, &sn, "CREATE VIEW plain AS SELECT id, v FROM t");
    exec(
        &mut client,
        &sn,
        "CREATE TABLE c (a BIGINT NOT NULL, b BIGINT NOT NULL, x BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );

    // Linear: over a base table, over a compound-PK table, over an unbounded view.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW l1 WITH (capacity = '1 MB') AS SELECT id, v, s FROM t WHERE v > 2",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW l2 WITH (capacity = '1 MB') AS SELECT a, b, x FROM c WHERE x < 9",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW l3 WITH (capacity = '1 MB') AS SELECT id, v FROM plain",
    );
    // Inner equi-join with a residual ON conjunct and a bare-column projection.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW j1 WITH (capacity = '1 MB') AS \
         SELECT t.id, t.s, u.w FROM t JOIN u ON t.id = u.tid AND u.w <> t.v",
    );
}

#[test]
fn every_other_body_shape_rejects_the_clause() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    make_tables(&mut client, &sn);

    let bodies = [
        // Reduce.
        "SELECT tid, SUM(w) AS s FROM u GROUP BY tid",
        // Semi / anti (EXISTS, NOT EXISTS).
        "SELECT id, v FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)",
        "SELECT id, v FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)",
        // Root DISTINCT and root set-op.
        "SELECT DISTINCT v FROM t",
        "SELECT id, v FROM t UNION ALL SELECT id, w FROM u",
        // Derived table, and a DISTINCT subquery.
        "SELECT d.id FROM (SELECT id, v FROM t) d",
        "SELECT d.v FROM (SELECT DISTINCT v FROM t) d",
        // A join carrying a computed projection.
        "SELECT t.id, t.v + u.w AS z FROM t JOIN u ON t.id = u.tid",
        // Outer joins and a range/band join.
        "SELECT t.id, u.w FROM t LEFT JOIN u ON t.id = u.tid",
        "SELECT t.id, u.w FROM t RIGHT JOIN u ON t.id = u.tid",
        "SELECT t.id, u.w FROM t FULL JOIN u ON t.id = u.tid",
        "SELECT t.id, u.w FROM t JOIN u ON t.id = u.tid AND t.v < u.w",
    ];
    for (i, body) in bodies.iter().enumerate() {
        let sql = format!("CREATE VIEW bad{i} WITH (capacity = '1 MB') AS {body}");
        assert_rejects_variant(&mut client, &sn, &sql, "Unsupported", "capacity");
        assert!(
            client.resolve_table_or_view_id(&sn, &format!("bad{i}")).is_err(),
            "`{body}` must not register a view",
        );
        // The same body without the clause is fine, so the rejection is the
        // capacity's and not the shape's.
        exec(&mut client, &sn, &format!("CREATE VIEW ok{i} AS {body}"));
    }
}

// ── Leaf rule and ALTER ─────────────────────────────────────────────────────

#[test]
fn nothing_can_be_created_over_a_bounded_view() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    make_tables(&mut client, &sn);
    exec(
        &mut client,
        &sn,
        "CREATE VIEW b WITH (capacity = '1 MB') AS SELECT id, v FROM t",
    );

    let bodies = [
        // Direct FROM, join side, subquery inner, CTE body.
        "SELECT id, v FROM b",
        "SELECT b.id, u.w FROM b JOIN u ON b.id = u.tid",
        "SELECT id FROM t WHERE id IN (SELECT id FROM b)",
        "WITH x AS (SELECT id, v FROM b) SELECT id FROM x",
    ];
    for (i, body) in bodies.iter().enumerate() {
        let sql = format!("CREATE VIEW over{i} AS {body}");
        assert_rejects_variant(&mut client, &sn, &sql, "Unsupported", "capacity-bounded");
    }
    // Reading it is normal, and dropping it works (it is never a dependency).
    let _ = read_sql(&mut client, &sn, "SELECT id, v FROM b");
    exec(&mut client, &sn, "DROP VIEW b");
}

/// `ALTER VIEW … AS` re-renders its body as a bare `CREATE VIEW`, dropping any
/// option clause — so retargeting a bounded view would silently unbound it.
/// `ALTER … RENAME` is a different operation and preserves the capacity.
#[test]
fn alter_view_cannot_retarget_a_bounded_view_but_rename_preserves_it() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    make_tables(&mut client, &sn);
    exec(
        &mut client,
        &sn,
        "CREATE VIEW b WITH (capacity = '1 MB') AS SELECT id, v FROM t",
    );

    assert_rejects_variant(
        &mut client,
        &sn,
        "ALTER VIEW b AS SELECT id, v FROM t WHERE v > 0",
        "Unsupported",
        "capacity-bounded",
    );

    // The rename must carry the capacity across.
    exec(&mut client, &sn, "ALTER TABLE b RENAME TO b_renamed");
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE VIEW over AS SELECT id FROM b_renamed",
        "Unsupported",
        "capacity-bounded",
    );
}
