#![cfg(feature = "integration")]

//! The RESOLVE verb and the statement-scoped descriptor built from it.
//!
//! Two claims are pinned here. *Correctness*: one reply reproduces, field for
//! field, what three whole-system-table scans produced — the physical column
//! list including hidden slots, the SERIAL and FK markers, the relation kind,
//! the placement, and the index list. *Cost*: a statement resolves each
//! relation exactly once, and nothing is retained across statements to go
//! stale under another client's DDL.

use gnitz_core::GnitzClient;
use std::sync::Arc;

mod common;
use common::*;

const T_ID_V: &str = "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)";

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
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, T_ID_V);

    let (tid, schema) = client.resolve_table_or_view_id(&sn, "t").unwrap();
    assert!(tid >= gnitz_core::FIRST_USER_TABLE_ID);
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.pk_cols, vec![0]);

    // Relation absent under a live schema: `Ok(None)` from the optional probe,
    // and the classified absence from the erroring one — the noun and the
    // qualified name are carried, not formatted into a message.
    assert!(client.resolve(&sn, "nope").unwrap().is_none());
    let err = client.resolve_relation(&sn, "nope").unwrap_err();
    assert!(
        matches!(&err, gnitz_core::ClientError::NotFound { noun, name }
            if *noun == "table or view" && name == &format!("{sn}.nope")),
        "got: {err:?}"
    );

    // A missing schema is an error, not an absent relation.
    let err = client.resolve("no_such_schema", "t").unwrap_err().to_string();
    assert!(err.contains("not found"), "got: {err}");
}

/// A view resolves through the same path as a table and reports its class, while
/// `resolve_table_id` — which answers only for a base table — must still miss on it.
#[test]
fn a_view_resolves_as_a_view_and_fails_the_base_table_probe() {
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, T_ID_V);
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT id, v FROM t");

    let rel = client.resolve_relation(&sn, "v").unwrap();
    assert_eq!(rel.class, gnitz_core::RelClass::View);
    assert_eq!(client.resolve(&sn, "v").unwrap().map(|d| d.tid), Some(rel.tid));

    let err = client.resolve_table_id(&sn, "v").unwrap_err();
    assert!(
        matches!(&err, gnitz_core::ClientError::NotFound { noun, .. } if *noun == "table"),
        "got: {err:?}"
    );
}

/// A name longer than the PK-region head rides the request's explicit blob, so
/// any length round-trips and a near-miss differing only past the head does not
/// resolve to it.
#[test]
fn a_long_relation_name_round_trips() {
    let (_srv, mut client, sn) = boot(1);
    let long = format!("a_relation_name_{}", "x".repeat(80));
    assert!(long.len() > gnitz_core::MAX_PK_BYTES);
    exec(
        &mut client,
        &sn,
        &format!("CREATE TABLE {long} (id BIGINT NOT NULL PRIMARY KEY)"),
    );
    let (tid, schema) = client.resolve_table_id(&sn, &long).unwrap();
    assert!(tid >= gnitz_core::FIRST_USER_TABLE_ID);
    assert_eq!(schema.columns[0].name, "id");

    let sibling = format!("{long}_two");
    assert!(client.resolve(&sn, &sibling).unwrap().is_none());
}

// ── The descriptor's contents ────────────────────────────────────────────

/// The markers that are not column-layout facts — SERIAL, the FK target, a
/// dropped column's hidden slot at its physical position — all reach the
/// resolved schema.
#[test]
fn descriptor_fields_round_trip() {
    let (_srv, mut client, sn) = boot(1);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, other BIGINT NOT NULL UNIQUE, gone BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "ALTER TABLE p DROP COLUMN gone");
    let (p_tid, p) = client.resolve_table_id(&sn, "p").unwrap();
    assert_eq!(p.columns.len(), 3, "the dropped column is still physically present");
    assert!(p.columns[2].is_hidden);
    assert!(!p.columns[1].is_hidden);

    exec(
        &mut client,
        &sn,
        "CREATE TABLE c (id SERIAL PRIMARY KEY, \
         a BIGINT NOT NULL REFERENCES p(id), \
         b BIGINT NOT NULL REFERENCES p(other))",
    );
    let (_, c) = client.resolve_table_id(&sn, "c").unwrap();
    assert!(c.columns[0].is_serial);
    assert!(!c.columns[1].is_serial);
    assert_eq!(c.columns[0].fk_table_id, 0, "the PK carries no FK");
    assert_eq!((c.columns[1].fk_table_id, c.columns[1].fk_col_idx), (p_tid, 0));
    assert_eq!((c.columns[2].fk_table_id, c.columns[2].fk_col_idx), (p_tid, 1));
}

/// The index list is exact — an empty list means "no index", never "unchanged".
/// A dropped index must therefore disappear from the very next resolve.
#[test]
fn the_index_list_is_exact_across_create_and_drop() {
    let (_srv, mut client, sn) = boot(1);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
    );
    let (tid, _) = client.resolve_table_id(&sn, "t").unwrap();
    assert!(client.describe_by_id(tid).unwrap().indexes.is_empty(), "no index yet");

    exec(&mut client, &sn, "CREATE INDEX ix_ab ON t(a, b)");
    let list = Arc::clone(&client.describe_by_id(tid).unwrap().indexes);
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].cols.as_slice(), &[1, 2], "the full declared column list");
    assert!(!list[0].is_unique);

    exec(&mut client, &sn, "CREATE UNIQUE INDEX ix_a ON t(a)");
    assert_eq!(
        client.index_for_column(tid, 1).unwrap().map(|m| m.is_unique),
        Some(true)
    );

    exec(&mut client, &sn, "DROP INDEX ix_ab");
    let list = Arc::clone(&client.describe_by_id(tid).unwrap().indexes);
    assert_eq!(list.len(), 1, "the dropped index is gone from the next resolve");
    assert_eq!(list[0].cols.as_slice(), &[1]);
}

/// `replicated` is a planner hint for a reduce built directly over a source, so
/// only a base table reports it. A view over a replicated source and a system
/// family are both *stamped* `Replicated`; answering with that stamp would
/// re-plan an aggregate over a view on a second authority.
#[test]
fn only_a_base_table_reports_its_replication() {
    let (_srv, mut client, sn) = boot(1);
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
    let rv_tid = client.resolve_relation(&sn, "rv").unwrap().tid;
    let replicated = |c: &mut GnitzClient, tid: u64| c.describe_by_id(tid).unwrap().replicated;

    assert!(replicated(&mut client, r_tid), "a REPLICATED base table");
    assert!(!replicated(&mut client, plain_tid), "a plain base table");
    assert!(
        !replicated(&mut client, rv_tid),
        "a view's locality is the compiler's call, not this hint's"
    );
    assert!(
        !replicated(&mut client, gnitz_core::TABLE_TAB),
        "a system family is not a base table"
    );
}

// ── The by-id addressing form ────────────────────────────────────────────

/// Every id-addressed probe ends at the same registration gate, so no
/// client-chosen tid reaches the column reader. None of these may abort or hang
/// the master; each is a clean miss.
#[test]
fn an_unregistered_id_is_a_clean_miss() {
    let (srv, mut client, _sn) = boot(1);

    for tid in [
        1_000_000_u64,                   // never allocated
        gnitz_wire::RELATION_ID_CEILING, // at the durable relation-id ceiling
        1 << 55,                         // above the COL_TAB packing limit
        u64::MAX,                        // negative once cast to i64
        u64::MAX - 1,
    ] {
        let err = client
            .describe_by_id(tid)
            .expect_err("an unregistered id must not resolve")
            .to_string();
        assert!(err.contains("not found"), "tid {tid}: got {err}");
    }

    // The master neither aborted nor desynced.
    let mut c2 = GnitzClient::connect(srv.sock_path()).unwrap();
    c2.create_schema("after").unwrap();
    exec(&mut c2, "after", "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)");
    c2.resolve_table_id("after", "t").unwrap();
}

// ── Statement cost ───────────────────────────────────────────────────────

/// A statement resolves each relation exactly once, so a read costs two round
/// trips — the resolve, then the read itself — whatever its shape, and however
/// many segments its plan has.
#[test]
fn a_read_costs_two_requests() {
    let (_srv, mut client, sn) = boot(1);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "CREATE INDEX ix ON t(x)");
    exec(
        &mut client,
        &sn,
        "INSERT INTO t (id, g, x) VALUES (1, 1, 10), (2, 1, 10)",
    );

    for sql in [
        "SELECT * FROM t",
        "SELECT * FROM t WHERE id = 2",
        "SELECT * FROM t WHERE x = 10",
        "SELECT g, COUNT(*) FROM t WHERE x = 10 GROUP BY g",
        "SELECT g, COUNT(*) FROM t WHERE x = 10 GROUP BY g HAVING COUNT(*) > 0",
    ] {
        for _ in 0..2 {
            assert_eq!(requests_for_sql(&mut client, &sn, sql), 2, "for `{sql}`");
        }
    }
}

/// A statement that resolves the same relation from two places pays once. `ALTER
/// TABLE … DROP COLUMN` is the case: it goes through the kind probe and then the
/// schema-bearing probe.
#[test]
fn one_statement_resolves_a_relation_once() {
    let (_srv, mut client, sn) = boot(1);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
    );

    assert_eq!(
        requests_for_sql(&mut client, &sn, "ALTER TABLE t DROP COLUMN a"),
        2,
        "DROP COLUMN resolves once and pushes once"
    );

    // An absent INSERT target runs the two-probe error ladder — still one resolve.
    let n = requests_for(&mut client, |c| {
        assert_rejects_variant(c, &sn, "INSERT INTO nope (id) VALUES (1)", "Exec", "nope");
    });
    assert_eq!(n, 1, "the two-probe error ladder costs one resolve");
}

/// `DROP SCHEMA` is one bundle whatever the member count: the SCHEMA_TAB probe,
/// one VIEW_TAB scan, one TABLE_TAB scan and one push. An equality, not a bound,
/// so a regression to a per-member cascade fails here instead of fitting under a
/// slack ceiling.
#[test]
fn drop_schema_is_four_requests_whatever_the_member_count() {
    let (_srv, mut client, sn) = boot(1);
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
    let n = requests_for(&mut client, |c| c.drop_schema(&sn).unwrap());
    assert_eq!(
        n, 4,
        "DROP SCHEMA over 8 members must cost SCHEMA_TAB + VIEW_TAB + TABLE_TAB + one push"
    );
}

// ── DDL that a client-side descriptor cache would get wrong ──────────────

/// `ALTER … RENAME TO` then recreating the old name: an idle second client's
/// next statement must see the *new* relation — nothing is retained across
/// statements to go stale.
#[test]
fn a_second_client_sees_a_rename_then_recreate() {
    let (srv, mut a, sn) = boot(4);
    let mut b = GnitzClient::connect(srv.sock_path()).unwrap();

    exec(&mut a, &sn, T_ID_V);
    exec(&mut a, &sn, "INSERT INTO t (id, v) VALUES (1, 100)");
    assert_eq!(
        rows(&mut b, &sn, "SELECT id, v FROM t", &["id", "v"]),
        vec![vec![1, 100, 1]]
    );
    let (old_tid, _) = b.resolve_table_id(&sn, "t").unwrap();

    exec(&mut a, &sn, "ALTER TABLE t RENAME TO u");
    exec(&mut a, &sn, T_ID_V);
    exec(&mut a, &sn, "INSERT INTO t (id, v) VALUES (2, 200), (3, 300)");

    let (new_tid, _) = b.resolve_table_id(&sn, "t").unwrap();
    assert_ne!(new_tid, old_tid, "the name now binds a different relation");
    assert_eq!(
        rows(&mut b, &sn, "SELECT id, v FROM t", &["id", "v"]),
        vec![vec![2, 200, 1], vec![3, 300, 1]]
    );

    exec(&mut b, &sn, "INSERT INTO t (id, v) VALUES (4, 400)");
    assert_eq!(
        rows(&mut a, &sn, "SELECT id, v FROM t", &["id", "v"]),
        vec![vec![2, 200, 1], vec![3, 300, 1], vec![4, 400, 1]]
    );
    assert_eq!(
        rows(&mut a, &sn, "SELECT id, v FROM u", &["id", "v"]),
        vec![vec![1, 100, 1]]
    );
}

/// `ALTER COLUMN … DROP NOT NULL` on A, then a raw binary `push` on B through
/// `resolve_table_id` + `push` — the surface with no SQL layer above it.
#[test]
fn a_second_client_pushes_after_a_column_alter() {
    use gnitz_core::{BatchAppender, ZSetBatch};
    let (srv, mut a, sn) = boot(4);
    let mut b = GnitzClient::connect(srv.sock_path()).unwrap();

    exec(&mut a, &sn, T_ID_V);
    let (tid, schema) = b.resolve_table_id(&sn, "t").unwrap();
    let mut batch = ZSetBatch::new(&schema);
    BatchAppender::new(&mut batch, &schema).add_row(1, 1).i64_val(10);
    b.push(tid, &schema, &batch).unwrap();

    exec(&mut a, &sn, "ALTER TABLE t ALTER COLUMN v DROP NOT NULL");

    let (tid2, schema2) = b.resolve_table_id(&sn, "t").unwrap();
    assert_eq!(tid2, tid);
    assert!(schema2.columns[1].is_nullable, "B sees the relaxed column");
    let mut batch = ZSetBatch::new(&schema2);
    BatchAppender::new(&mut batch, &schema2).add_row(2, 1).null();
    b.push(tid2, &schema2, &batch).unwrap();

    let (schema, batch) = read_sql(&mut a, &sn, "SELECT id, v FROM t");
    let mut got: Vec<(i64, Option<i64>)> = (0..batch.len())
        .map(|r| {
            let id = cell_i64(&schema, &batch, col_idx(&schema, "id"), r);
            let vi = col_idx(&schema, "v");
            let v = (!is_null_at(&schema, &batch, vi, r)).then(|| cell_i64(&schema, &batch, vi, r));
            assert_eq!(batch.weights[r], 1);
            (id, v)
        })
        .collect();
    got.sort();
    assert_eq!(got, vec![(1, Some(10)), (2, None)]);
}

/// A schema dropped and recreated under the same name, and a relation created in
/// the same DDL bundle as its schema, both resolve — the qname denormalizes the
/// schema *name* at insert time, so a recreated schema must rebuild its members'
/// qnames rather than resurrect the old ones.
#[test]
fn a_recreated_schema_resolves_its_new_members() {
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)");
    let (old_tid, _) = client.resolve_table_id(&sn, "t").unwrap();

    client.drop_schema(&sn).unwrap();
    client.create_schema(&sn).unwrap();
    assert!(
        client.resolve(&sn, "t").unwrap().is_none(),
        "the old member must not resurrect under the recreated schema"
    );

    exec(&mut client, &sn, T_ID_V);
    let (new_tid, schema) = client.resolve_table_id(&sn, "t").unwrap();
    assert_ne!(new_tid, old_tid);
    assert_eq!(schema.columns.len(), 2);
}

/// `alter_rename_column` against a view name must raise the "requires a base
/// table" error rather than write an `OWNER_KIND_TABLE` row against a stored
/// view row and fail as a CAS conflict — which is what the engine answers if
/// the row reaches it, since the retraction CAS differs on `owner_kind` before
/// the readable kind check runs.
#[test]
fn alter_rename_column_rejects_a_view_at_the_gateway() {
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, T_ID_V);
    exec(&mut client, &sn, "CREATE VIEW vw AS SELECT id, v FROM t");

    let vid = client.resolve_table_or_view_id(&sn, "vw").unwrap().0;
    let err = client
        .alter_rename_column(vid, 1, "w")
        .expect_err("a view is not a base table")
        .to_string();
    assert!(err.contains("requires a base table"), "got: {err}");
}
