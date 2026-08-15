#![cfg(feature = "integration")]

//! The RESOLVE verb and the statement-scoped descriptor built from it.
//!
//! Two claims are pinned here. The first is *correctness*: one reply must
//! reproduce, field for field, what three whole-system-table scans produced —
//! the physical column list including hidden slots, the SERIAL and FK markers,
//! the relation kind, the placement, and the index list. The second is *cost*:
//! a statement resolves each relation exactly once, and that count does not move
//! as the catalog grows.

use gnitz_core::GnitzClient;
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// Request frames the client wrote while running `f`.
fn requests_for(client: &mut GnitzClient, f: impl FnOnce(&mut GnitzClient)) -> u64 {
    let before = client.requests_sent();
    f(client);
    client.requests_sent() - before
}

/// Request frames one `execute` of `sql` costs.
fn requests_for_sql(client: &mut GnitzClient, sn: &str, sql: &str) -> u64 {
    requests_for(client, |c| exec(c, sn, sql))
}

// ── Resolution outcomes ──────────────────────────────────────────────────

#[test]
fn resolve_reports_found_absent_and_missing_schema() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );

    let (tid, schema) = client.resolve_table_or_view_id(&sn, "t").unwrap();
    assert!(tid >= gnitz_core::FIRST_USER_TABLE_ID);
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.pk_cols, vec![0]);

    // Relation absent under a live schema: `Ok(None)` from the kind probe, and
    // the "Table or view" wording from the schema-bearing one.
    assert!(client.resolve_relation_kind(&sn, "nope").unwrap().is_none());
    let err = client.resolve_relation(&sn, "nope").unwrap_err().to_string();
    assert!(err.contains("Table or view") && err.contains("not found"), "got: {err}");

    // Schema absent is an error on every entry point, with the schema's wording.
    let err = client
        .resolve_relation_kind("no_such_schema", "t")
        .unwrap_err()
        .to_string();
    assert!(err.contains("Schema 'no_such_schema' not found"), "got: {err}");
}

/// A view resolves through the same path as a table and reports its kind, and
/// `resolve_table_id` must still *fail* on it — that failure is what lets
/// `resolve_base_table`'s second probe raise the "is a view" error.
#[test]
fn a_view_resolves_as_a_view_and_fails_the_base_table_probe() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT id, v FROM t");

    let (vid, _, is_view) = client.resolve_relation(&sn, "v").unwrap();
    assert!(is_view, "a view must report kind = view");
    assert_eq!(client.resolve_relation_kind(&sn, "v").unwrap(), Some((vid, true)));

    let err = client.resolve_table_id(&sn, "v").unwrap_err().to_string();
    assert!(err.contains("Table") && err.contains("not found"), "got: {err}");

    // …and the SQL layer's ladder turns that miss into the precise error.
    assert_rejects_variant(
        &mut client,
        &sn,
        "INSERT INTO v (id, v) VALUES (1, 2)",
        "Unsupported",
        "is a view",
    );
}

/// A name longer than the 16-byte `PkTuple` head must survive the request. This
/// is the `send_message_with_extra` guard: the `send_message` path derives the
/// blob from `PkTuple::split_wire`, which would truncate it.
#[test]
fn a_long_relation_name_round_trips() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    let long = "a_relation_name_far_longer_than_sixteen_bytes";
    assert!(long.len() > 16);
    exec(
        &mut client,
        &sn,
        &format!("CREATE TABLE {long} (id BIGINT NOT NULL PRIMARY KEY)"),
    );
    let (tid, schema) = client.resolve_table_id(&sn, long).unwrap();
    assert!(tid >= gnitz_core::FIRST_USER_TABLE_ID);
    assert_eq!(schema.columns[0].name, "id");

    // A near-miss differing only past byte 16 must NOT resolve to it.
    let sibling = "a_relation_name_far_longer_than_sixteen_bytes_two";
    assert!(client.resolve_relation_kind(&sn, sibling).unwrap().is_none());
}

// ── The descriptor's contents ────────────────────────────────────────────

/// `is_serial` rides the schema block (`META_FLAG_SERIAL`), which is what keeps
/// INSERT working: `plan_insert` reads it off the resolved schema to decide the
/// PK source.
#[test]
fn serial_marker_survives_the_resolve() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id SERIAL PRIMARY KEY, v BIGINT NOT NULL)",
    );

    let (_, schema) = client.resolve_table_id(&sn, "t").unwrap();
    assert!(schema.columns[0].is_serial, "the SERIAL PK must resolve as serial");
    assert!(!schema.columns[1].is_serial);

    // And the marker is what lets INSERT omit the PK.
    exec(&mut client, &sn, "INSERT INTO t (v) VALUES (7)");
    let (_, batch) = read_sql(&mut client, &sn, "SELECT id, v FROM t");
    assert_eq!(batch.len(), 1, "the SERIAL id was auto-assigned");
}

/// The FK target is not a column-layout fact, so it rides the descriptor blob
/// and is merged back into the schema. Covers a plain FK, two FK columns, and a
/// self-FK (stored as the owner's own id).
#[test]
fn fk_targets_are_merged_into_the_resolved_schema() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, other BIGINT NOT NULL UNIQUE)",
    );
    let (p_tid, _) = client.resolve_table_id(&sn, "p").unwrap();
    exec(
        &mut client,
        &sn,
        "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, \
         a BIGINT NOT NULL REFERENCES p(id), \
         b BIGINT NOT NULL REFERENCES p(other))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE selfref (id BIGINT NOT NULL PRIMARY KEY, parent BIGINT NOT NULL REFERENCES selfref(id))",
    );

    let (_, c) = client.resolve_table_id(&sn, "c").unwrap();
    assert_eq!(c.columns[0].fk_table_id, 0, "the PK carries no FK");
    assert_eq!(c.columns[1].fk_table_id, p_tid);
    assert_eq!(c.columns[1].fk_col_idx, 0);
    assert_eq!(c.columns[2].fk_table_id, p_tid);
    assert_eq!(c.columns[2].fk_col_idx, 1);

    let (self_tid, s) = client.resolve_table_id(&sn, "selfref").unwrap();
    assert_eq!(
        s.columns[1].fk_table_id, self_tid,
        "a self-FK is stored as the owner's own id"
    );
}

/// A hidden (logically dropped) column stays in the resolved schema at its
/// physical position — the descriptor carries the *physical* column list, which
/// is what a COL_TAB `-1` needs.
#[test]
fn a_dropped_column_stays_hidden_in_place() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, gone BIGINT NOT NULL, keep BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "ALTER TABLE t DROP COLUMN gone");

    let (_, schema) = client.resolve_table_id(&sn, "t").unwrap();
    assert_eq!(
        schema.columns.len(),
        3,
        "the dropped column is still physically present"
    );
    assert!(schema.columns[1].is_hidden);
    assert_eq!(schema.columns[2].name, "keep");
    assert!(!schema.columns[2].is_hidden);
}

/// The index list is exact — an empty list means "no index", never "unchanged".
/// A dropped index must therefore disappear from the very next resolve.
#[test]
fn the_index_list_is_exact_across_create_and_drop() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
    );
    let (tid, _) = client.resolve_table_id(&sn, "t").unwrap();
    assert!(client.table_indexes(tid).unwrap().is_empty(), "no index yet");

    exec(&mut client, &sn, "CREATE INDEX ix_ab ON t(a, b)");
    let list = client.table_indexes(tid).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].cols.as_slice(), &[1, 2], "the full declared column list");
    assert!(!list[0].is_unique);

    exec(&mut client, &sn, "CREATE UNIQUE INDEX ix_a ON t(a)");
    assert_eq!(
        client.index_for_column(tid, 1).unwrap().map(|m| m.is_unique),
        Some(true)
    );

    exec(&mut client, &sn, "DROP INDEX ix_ab");
    let list = client.table_indexes(tid).unwrap();
    assert_eq!(list.len(), 1, "the dropped index is gone from the next resolve");
    assert_eq!(list[0].cols.as_slice(), &[1]);
}

/// `replicated` is a planner hint for a reduce built directly over a source, so
/// only a base table reports it. A view over a replicated source and a system
/// family are both *stamped* `Replicated`; answering with that stamp would
/// re-plan an aggregate over a view on a second authority.
#[test]
fn only_a_base_table_reports_its_replication() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL) WITH (replicated = true)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE plain (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "CREATE VIEW rv AS SELECT id, v FROM r");

    let (r_tid, _) = client.resolve_table_id(&sn, "r").unwrap();
    let (plain_tid, _) = client.resolve_table_id(&sn, "plain").unwrap();
    let (rv_tid, _, _) = client.resolve_relation(&sn, "rv").unwrap();

    assert!(client.table_replicated(r_tid).unwrap(), "a REPLICATED base table");
    assert!(!client.table_replicated(plain_tid).unwrap(), "a plain base table");
    assert!(
        !client.table_replicated(rv_tid).unwrap(),
        "a view's locality is the compiler's call, not this hint's"
    );
    // A system family is stamped Replicated too, and is likewise not a base
    // table.
    assert!(
        !client.table_replicated(gnitz_core::TABLE_TAB).unwrap(),
        "a system family is not a base table"
    );
}

// ── The by-id addressing form ────────────────────────────────────────────

/// Every id-addressed probe ends at the same registration gate, so no
/// client-chosen tid reaches the column reader. None of these may abort or hang
/// the master; each is a clean miss.
#[test]
fn an_unregistered_id_is_a_clean_miss() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, _sn) = make_planner(&srv);

    for tid in [
        1_000_000_u64, // never allocated
        1 << 31,       // at RELATION_ID_CEILING
        1 << 55,       // above the COL_TAB packing limit
        u64::MAX,      // negative once cast to i64
        u64::MAX - 1,
    ] {
        let err = client
            .table_indexes(tid)
            .expect_err("an unregistered id must not resolve")
            .to_string();
        assert!(err.contains("not found"), "tid {tid}: got {err}");
    }

    // The connection is still usable — the master neither aborted nor desynced.
    let (mut c2, sn2) = make_planner(&srv);
    exec(&mut c2, &sn2, "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)");
    assert!(c2.resolve_table_id(&sn2, "t").is_ok());
}

/// A tid-addressed resolve returns the same descriptor a by-name one does, and
/// it is what an index / replication probe falls back to outside a statement
/// bracket — not an empty list or `false`.
#[test]
fn a_tid_probe_outside_a_statement_still_answers_the_truth() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL) WITH (replicated = true)",
    );
    exec(&mut client, &sn, "CREATE UNIQUE INDEX ix ON t(a)");

    // No statement bracket is open here, so both probes go to the wire by id.
    let (tid, _) = client.resolve_table_id(&sn, "t").unwrap();
    assert!(client.table_replicated(tid).unwrap());
    let list = client.table_indexes(tid).unwrap();
    assert_eq!(list.len(), 1);
    assert!(list[0].is_unique);
    assert_eq!(
        client.index_for_column(tid, 1).unwrap().map(|m| m.is_unique),
        Some(true)
    );
}

// ── Statement cost ───────────────────────────────────────────────────────

/// A statement resolves each relation exactly once, so a repeated read costs
/// two round trips — the resolve, then the read itself.
#[test]
fn a_repeated_select_costs_two_requests() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "INSERT INTO t (id, x) VALUES (1, 10), (2, 10)");

    // Warm the connection's schema cache once, then measure the steady state.
    for sql in [
        "SELECT * FROM t",
        "SELECT * FROM t WHERE id = 2",
        "SELECT * FROM t WHERE x = 10",
    ] {
        exec(&mut client, &sn, sql);
        for _ in 0..3 {
            assert_eq!(requests_for_sql(&mut client, &sn, sql), 2, "for `{sql}`");
        }
    }
}

/// …and that count does not move as the catalog grows: a statement addresses the
/// relations it names, never the whole catalog.
#[test]
fn statement_cost_is_flat_as_the_catalog_grows() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "SELECT * FROM t WHERE x = 1"); // warm

    let before = requests_for_sql(&mut client, &sn, "SELECT * FROM t WHERE x = 1");
    for i in 0..40 {
        exec(
            &mut client,
            &sn,
            &format!(
                "CREATE TABLE filler{i} (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, \
                 b BIGINT NOT NULL, c BIGINT NOT NULL, d BIGINT NOT NULL)"
            ),
        );
    }
    let after = requests_for_sql(&mut client, &sn, "SELECT * FROM t WHERE x = 1");
    assert_eq!(before, after, "40 more relations must not cost the read anything");
}

/// A statement that resolves the same relation from two places pays once. `ALTER
/// TABLE … DROP COLUMN` is the case: it goes through the kind probe and then the
/// schema-bearing probe.
#[test]
fn one_statement_resolves_a_relation_once() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "SELECT * FROM t"); // warm the schema cache

    // resolve + push. Both the kind probe and the schema probe hit the memo.
    assert_eq!(
        requests_for_sql(&mut client, &sn, "ALTER TABLE t DROP COLUMN a"),
        2,
        "DROP COLUMN resolves once and pushes once"
    );

    // An absent INSERT target runs the two-probe error ladder — still one resolve.
    let n = requests_for(&mut client, |c| {
        assert!(try_exec(c, &sn, "INSERT INTO nope (id) VALUES (1)").is_err());
    });
    assert_eq!(n, 1, "the two-probe error ladder costs one resolve");
}

/// A multi-segment statement probes the index list once per relation, not once
/// per segment — the collapse the per-statement memo's tid half provides.
#[test]
fn a_multi_segment_statement_resolves_once_per_relation() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "CREATE INDEX ix ON t(x)");
    exec(&mut client, &sn, "SELECT * FROM t"); // warm

    let one = requests_for_sql(&mut client, &sn, "SELECT g, COUNT(*) FROM t WHERE x = 1 GROUP BY g");
    let two = requests_for_sql(
        &mut client,
        &sn,
        "SELECT g, COUNT(*) FROM t WHERE x = 1 GROUP BY g HAVING COUNT(*) > 0",
    );
    assert!(one <= 3, "a grouped read resolves once: {one} requests");
    assert!(two <= 4, "an extra segment adds no resolve: {two} requests");
}

/// `DROP SCHEMA` cascades over every member, each re-deriving the schema id and
/// its own family row. The statement-scoped scan cache is what keeps that at its
/// old request count; without it every member would re-scan SCHEMA_TAB.
#[test]
fn drop_schema_cascade_does_not_rescan_per_member() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    for i in 0..4 {
        exec(
            &mut client,
            &sn,
            &format!("CREATE TABLE m{i} (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"),
        );
        exec(
            &mut client,
            &sn,
            &format!("CREATE VIEW vw{i} AS SELECT id, v FROM m{i}"),
        );
    }
    // 4 tables + 4 views. With a per-member SCHEMA_TAB rescan each member would
    // cost an extra request; the retained snapshot holds it to a small constant
    // per member plus the schema row itself.
    let n = requests_for(&mut client, |c| c.drop_schema(&sn).unwrap());
    assert!(
        n <= 8 * 3 + 4,
        "DROP SCHEMA over 8 members cost {n} requests — a per-member rescan crept back"
    );
}

// ── DDL that a client-side descriptor cache would get wrong ──────────────

/// `ALTER … RENAME TO` then recreating the old name: an idle second client's
/// next statement must see the *new* relation. This design gets it right by
/// construction — nothing is retained across statements to go stale.
#[test]
fn a_second_client_sees_a_rename_then_recreate() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut a, sn) = make_planner(&srv);
    let mut b = GnitzClient::connect(srv.sock_path()).unwrap();

    exec(
        &mut a,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(&mut a, &sn, "INSERT INTO t (id, v) VALUES (1, 100)");
    // B reads the original t, warming everything it can warm.
    let (_, batch) = read_sql(&mut b, &sn, "SELECT id, v FROM t");
    assert_eq!(batch.len(), 1);
    let (old_tid, _) = b.resolve_table_id(&sn, "t").unwrap();

    exec(&mut a, &sn, "ALTER TABLE t RENAME TO u");
    exec(
        &mut a,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(&mut a, &sn, "INSERT INTO t (id, v) VALUES (2, 200), (3, 300)");

    // B's next statements must address the NEW t.
    let (new_tid, _) = b.resolve_table_id(&sn, "t").unwrap();
    assert_ne!(new_tid, old_tid, "the name now binds a different relation");
    let (_, batch) = read_sql(&mut b, &sn, "SELECT id, v FROM t");
    assert_eq!(batch.len(), 2, "B read the new t's rows");

    exec(&mut b, &sn, "INSERT INTO t (id, v) VALUES (4, 400)");
    let (_, batch) = read_sql(&mut a, &sn, "SELECT id, v FROM t");
    assert_eq!(batch.len(), 3, "B's INSERT landed in the new t");
    // The renamed relation kept its rows under the new name.
    let (_, batch) = read_sql(&mut a, &sn, "SELECT id, v FROM u");
    assert_eq!(batch.len(), 1);
}

/// Drop and recreate under the same name: B's next statement succeeds against
/// the new relation with no error and no reconnect.
#[test]
fn a_second_client_sees_a_drop_then_recreate() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut a, sn) = make_planner(&srv);
    let mut b = GnitzClient::connect(srv.sock_path()).unwrap();

    exec(
        &mut a,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(&mut a, &sn, "INSERT INTO t (id, v) VALUES (1, 100)");
    assert_eq!(read_sql(&mut b, &sn, "SELECT id, v FROM t").1.len(), 1);

    exec(&mut a, &sn, "DROP TABLE t");
    exec(
        &mut a,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(&mut a, &sn, "INSERT INTO t (id, v) VALUES (9, 900)");

    let (_, batch) = read_sql(&mut b, &sn, "SELECT id, v FROM t");
    assert_eq!(batch.len(), 1, "B reads the new relation");
    exec(&mut b, &sn, "INSERT INTO t (id, v) VALUES (10, 1000)");
    assert_eq!(read_sql(&mut a, &sn, "SELECT id, v FROM t").1.len(), 2);
}

/// A `CREATE INDEX` by A is visible to B's very next statement, and a
/// `DROP INDEX` likewise — an index bound planned against a dropped index is a
/// hard engine error, not a silently-empty read.
#[test]
fn a_second_client_sees_index_ddl_immediately() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut a, sn) = make_planner(&srv);
    let mut b = GnitzClient::connect(srv.sock_path()).unwrap();

    exec(
        &mut a,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
    );
    exec(&mut a, &sn, "INSERT INTO t (id, x) VALUES (1, 5), (2, 5), (3, 6)");
    let (tid, _) = b.resolve_table_id(&sn, "t").unwrap();
    assert!(b.table_indexes(tid).unwrap().is_empty());
    assert_eq!(read_sql(&mut b, &sn, "SELECT id FROM t WHERE x = 5").1.len(), 2);

    exec(&mut a, &sn, "CREATE INDEX ix ON t(x)");
    assert_eq!(b.table_indexes(tid).unwrap().len(), 1, "B sees the new index at once");
    assert_eq!(read_sql(&mut b, &sn, "SELECT id FROM t WHERE x = 5").1.len(), 2);

    exec(&mut a, &sn, "DROP INDEX ix");
    assert!(b.table_indexes(tid).unwrap().is_empty(), "B sees the drop at once");
    assert_eq!(
        read_sql(&mut b, &sn, "SELECT id FROM t WHERE x = 5").1.len(),
        2,
        "B replanned without the index rather than seeking a dropped one"
    );
}

/// `ALTER COLUMN … DROP NOT NULL` on A, then a raw binary `push` on B through
/// `resolve_table_id` + `push` — the surface with no SQL layer above it.
#[test]
fn a_second_client_pushes_after_a_column_alter() {
    use gnitz_core::{BatchAppender, ZSetBatch};
    let Some(srv) = ServerHandle::start() else { return };
    let (mut a, sn) = make_planner(&srv);
    let mut b = GnitzClient::connect(srv.sock_path()).unwrap();

    exec(
        &mut a,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    // B warms its schema cache through the raw surface.
    let (tid, schema) = b.resolve_table_id(&sn, "t").unwrap();
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch, &schema).add_row(1, 1).i64_val(10);
    b.push(tid, &schema, &batch).unwrap();

    exec(&mut a, &sn, "ALTER TABLE t ALTER COLUMN v DROP NOT NULL");

    // B re-resolves and pushes a NULL into the now-nullable column.
    let (tid2, schema2) = b.resolve_table_id(&sn, "t").unwrap();
    assert_eq!(tid2, tid);
    assert!(schema2.columns[1].is_nullable, "B sees the relaxed column");
    let mut batch = ZSetBatch::new(&schema2);
    BatchAppender::new(&mut batch, &schema2).add_row(2, 1).u64_null();
    b.push(tid2, &schema2, &batch).unwrap();

    assert_eq!(read_sql(&mut a, &sn, "SELECT id, v FROM t").1.len(), 2);
}

/// Own-DDL: a `CREATE INDEX` / `CREATE TABLE` is visible to the *same*
/// connection's next statement, since nothing is carried across the bracket.
#[test]
fn own_ddl_is_visible_to_the_next_statement() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "INSERT INTO t (id, x) VALUES (1, 5)");
    let (tid, _) = client.resolve_table_id(&sn, "t").unwrap();

    exec(&mut client, &sn, "CREATE INDEX ix ON t(x)");
    assert_eq!(client.table_indexes(tid).unwrap().len(), 1);
    assert_eq!(read_sql(&mut client, &sn, "SELECT id FROM t WHERE x = 5").1.len(), 1);

    exec(&mut client, &sn, "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY)");
    exec(&mut client, &sn, "INSERT INTO t2 (id) VALUES (1)");
    assert_eq!(read_sql(&mut client, &sn, "SELECT id FROM t2").1.len(), 1);
}

/// A schema dropped and recreated under the same name, and a relation created in
/// the same DDL bundle as its schema, both resolve — the qname denormalizes the
/// schema *name* at insert time, so a recreated schema must rebuild its members'
/// qnames rather than resurrect the old ones.
#[test]
fn a_recreated_schema_resolves_its_new_members() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)");
    let (old_tid, _) = client.resolve_table_id(&sn, "t").unwrap();

    client.drop_schema(&sn).unwrap();
    client.create_schema(&sn).unwrap();
    assert!(
        client.resolve_relation_kind(&sn, "t").unwrap().is_none(),
        "the old member must not resurrect under the recreated schema"
    );

    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    let (new_tid, schema) = client.resolve_table_id(&sn, "t").unwrap();
    assert_ne!(new_tid, old_tid);
    assert_eq!(schema.columns.len(), 2);
}

/// The gateway backstop for a non-SQL front end: `alter_rename_column` against a
/// view name must raise the "requires a base table" error rather than write an
/// `OWNER_KIND_TABLE` row against a stored view row and fail as a CAS conflict.
#[test]
fn alter_rename_column_rejects_a_view_at_the_gateway() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "CREATE VIEW vw AS SELECT id, v FROM t");

    let err = client
        .alter_rename_column(&sn, "vw", "v", "w")
        .expect_err("a view is not a base table")
        .to_string();
    assert!(err.contains("requires a base table"), "got: {err}");
}
