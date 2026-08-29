#![cfg(feature = "integration")]

//! What the master refuses to write into its own catalog.
//!
//! `GnitzClient` canonicalizes and validates every name it stores, but
//! `Session::push_ddl_txn` is public — so the rules the engine's caches, its
//! qualified-name keys and `hook_schema_dir`'s filesystem path depend on are
//! enforced at the master's trust boundary, and these tests drive that boundary
//! directly with hand-built bundles.

use gnitz_core::connection::{COL_TAB, IDX_TAB, SCHEMA_TAB, TABLE_TAB};
use gnitz_core::protocol::{BatchAppender, ColumnDef, TypeCode, ZSetBatch};
use gnitz_core::types::sys_schema;
use gnitz_core::{GnitzClient, Session, TableProps};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::sys_rows::{
    write_col_tab_row, write_idx_tab_row, write_schema_tab_row, write_table_tab_row, IdxTabRow, SchemaTabRow,
    TableTabRow,
};

/// One SCHEMA_TAB batch registering `(schema_id, name)`.
fn schema_row(schema_id: u64, name: &str) -> ZSetBatch {
    let s = sys_schema(SCHEMA_TAB);
    let mut b = ZSetBatch::new(s);
    write_schema_tab_row(&mut BatchAppender::new(&mut b, s), &SchemaTabRow { schema_id, name }, 1);
    b
}

/// One TABLE_TAB batch registering `(table_id, schema_id, name)` with a single
/// U64 PK column and no flags.
fn table_row(table_id: u64, schema_id: u64, name: &str) -> ZSetBatch {
    let s = sys_schema(TABLE_TAB);
    let mut b = ZSetBatch::new(s);
    write_table_tab_row(
        &mut BatchAppender::new(&mut b, s),
        &TableTabRow {
            table_id,
            schema_id,
            name,
            pk_col_idx: gnitz_wire::pack_pk_cols(&[0]),
            flags: TableProps::default().pack(),
        },
        1,
    );
    b
}

/// The COL_TAB batch for a `(id U64 PK, v I64)` table owned by `owner_id`.
fn two_columns(owner_id: u64) -> ZSetBatch {
    let s = sys_schema(COL_TAB);
    let mut b = ZSetBatch::new(s);
    let mut a = BatchAppender::new(&mut b, s);
    for (i, cd) in [
        ColumnDef::new("id", TypeCode::U64, false),
        ColumnDef::new("v", TypeCode::I64, false),
    ]
    .iter()
    .enumerate()
    {
        write_col_tab_row(&mut a, &cd.col_tab_row(owner_id, gnitz_wire::OWNER_KIND_TABLE, i), 1).unwrap();
    }
    b
}

fn session(srv: &ServerHandle) -> Session {
    Session::connect(srv.sock_path()).expect("connect").0
}

/// A schema name is the one catalog name the engine interpolates into a
/// filesystem path, so it takes the full identifier rule at the trust boundary —
/// `..` and `/` never reach `create_dir_all`.
#[test]
fn a_schema_name_that_is_not_an_identifier_is_refused() {
    let Some(srv) = ServerHandle::start() else { return };
    let mut s = session(&srv);
    for name in ["..", "../escape", "a/b", "with space", ""] {
        let sid = s.alloc_schema_id().unwrap();
        let err = s
            .push_ddl_txn(&[(SCHEMA_TAB, schema_row(sid, name))])
            .expect_err("a non-identifier schema name must be refused");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("Identifier") || msg.contains("cannot be empty"),
            "name {name:?} gave: {msg}"
        );
    }
}

/// Every cache key and qualified name in the catalog is compared byte-wise
/// against the canonical form the client stores, so a mixed-case row would
/// register a relation no lookup finds.
#[test]
fn a_non_canonical_name_is_refused_for_every_family() {
    let Some(srv) = ServerHandle::start() else { return };
    let mut s = session(&srv);

    let sid = s.alloc_schema_id().unwrap();
    let err = format!(
        "{:?}",
        s.push_ddl_txn(&[(SCHEMA_TAB, schema_row(sid, "MixedCase"))])
            .unwrap_err()
    );
    assert!(err.contains("not canonical"), "{err}");

    // The relation families take the same rule (minus the client's leading-`_`
    // policy, which the engine must not apply — it writes `__h…` segments).
    s.push_ddl_txn(&[(SCHEMA_TAB, schema_row(sid, "guards"))]).unwrap();
    let tid = s.alloc_table_id().unwrap();
    let err = format!(
        "{:?}",
        s.push_ddl_txn(&[
            (COL_TAB, two_columns(tid)),
            (TABLE_TAB, table_row(tid, sid, "MixedCase"))
        ])
        .unwrap_err()
    );
    assert!(err.contains("not canonical"), "{err}");
}

/// A bundle carrying two blocks for one family is structurally ambiguous: every
/// list the handler derives (new view ids, dropped ids, unique-index pre-flight)
/// would read the first block alone while both were applied.
#[test]
fn a_bundle_with_two_blocks_for_one_family_is_refused() {
    let Some(srv) = ServerHandle::start() else { return };
    let mut s = session(&srv);
    let sid = s.alloc_schema_id().unwrap();
    s.push_ddl_txn(&[(SCHEMA_TAB, schema_row(sid, "dupfam"))]).unwrap();

    let (a, b) = (s.alloc_table_id().unwrap(), s.alloc_table_id().unwrap());
    let err = format!(
        "{:?}",
        s.push_ddl_txn(&[
            (COL_TAB, two_columns(a)),
            (TABLE_TAB, table_row(a, sid, "one")),
            (TABLE_TAB, table_row(b, sid, "two")),
        ])
        .unwrap_err()
    );
    assert!(err.contains("two blocks"), "{err}");
}

/// Two IDX_TAB rows under one name in a single bundle: both pass the persisted
/// `index_by_name` check, and applying both would leave one index live and
/// unreachable by name.
#[test]
fn two_indexes_under_one_name_in_one_bundle_are_refused() {
    let Some(srv) = ServerHandle::start() else { return };
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    client.create_schema("dupidx").unwrap();
    let cols = [
        ColumnDef::new("id", TypeCode::U64, false),
        ColumnDef::new("b", TypeCode::I64, false),
        ColumnDef::new("c", TypeCode::I64, false),
    ];
    // The two constraint names fold to the same canonical name.
    let err = format!(
        "{:?}",
        client
            .create_table(
                "dupidx",
                "t",
                &cols,
                &[0],
                TableProps::default(),
                &[
                    gnitz_core::InlineUniqueIndex {
                        col_indices: &[1],
                        name: "u",
                    },
                    gnitz_core::InlineUniqueIndex {
                        col_indices: &[2],
                        name: "U",
                    },
                ],
            )
            .unwrap_err()
    );
    assert!(err.contains("Index already exists"), "{err}");
    // Nothing was written: the bundle is one DDL zone.
    assert!(client.resolve("dupidx", "t").unwrap().is_none());
}

/// The positive case the guards above must not have broken: a `CREATE TABLE`
/// with an inline `UNIQUE` is a three-family `[COL_TAB, TABLE_TAB, IDX_TAB]`
/// bundle and still commits whole.
#[test]
fn a_three_family_create_table_bundle_still_commits() {
    let Some(srv) = ServerHandle::start() else { return };
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    client.create_schema("ok").unwrap();
    let cols = [
        ColumnDef::new("id", TypeCode::U64, false),
        ColumnDef::new("b", TypeCode::I64, false),
    ];
    let tid = client
        .create_table(
            "ok",
            "t",
            &cols,
            &[0],
            TableProps::default(),
            &[gnitz_core::InlineUniqueIndex {
                col_indices: &[1],
                name: "u_b",
            }],
        )
        .unwrap();
    let desc = client.resolve("ok", "t").unwrap().expect("the table exists");
    assert_eq!(desc.tid, tid);
    assert_eq!(desc.indexes.len(), 1);
}

/// An index name carrying the reserved FK infix would be undroppable, so the
/// master refuses it whatever wrote it.
#[test]
fn an_index_name_carrying_the_reserved_fk_infix_is_refused() {
    let Some(srv) = ServerHandle::start() else { return };
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    client.create_schema("infix").unwrap();
    let cols = [
        ColumnDef::new("id", TypeCode::U64, false),
        ColumnDef::new("b", TypeCode::I64, false),
    ];
    let tid = client
        .create_table("infix", "t", &cols, &[0], TableProps::default(), &[])
        .unwrap();

    let mut s = session(&srv);
    let iid = s.alloc_index_id().unwrap();
    let idx_s = sys_schema(IDX_TAB);
    let mut b = ZSetBatch::new(idx_s);
    write_idx_tab_row(
        &mut BatchAppender::new(&mut b, idx_s),
        &IdxTabRow {
            index_id: iid,
            owner_id: tid,
            source_col_idx: gnitz_wire::pack_pk_cols(&[1]),
            name: "t__fk_b",
            is_unique: 0,
        },
        1,
    );
    let err = format!("{:?}", s.push_ddl_txn(&[(IDX_TAB, b)]).unwrap_err());
    assert!(err.contains("reserved"), "{err}");
}

/// An all-negative multi-family bundle is applied View → Table → Schema: a view
/// must be retired before the table it reads, and the schema row last, or its
/// member-count guard rejects it. `DROP SCHEMA` is exactly that bundle.
#[test]
fn drop_schema_retires_a_view_its_table_and_the_schema_in_one_bundle() {
    let Some(srv) = ServerHandle::start() else { return };
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    client.create_schema("teardown").unwrap();
    let cols = [
        ColumnDef::new("id", TypeCode::U64, false),
        ColumnDef::new("v", TypeCode::I64, false),
    ];
    let tid = client
        .create_table("teardown", "t", &cols, &[0], TableProps::default(), &[])
        .unwrap();
    client.create_view("teardown", "v", tid, &cols).unwrap();

    client.drop_schema("teardown").unwrap();

    // The schema row went last and its member-count guard accepted it, so the
    // name is free again — which only holds if the view and the table were
    // retired first, and the view before the table it reads.
    client.create_schema("teardown").unwrap();
    assert!(client.resolve("teardown", "v").unwrap().is_none());
    assert!(client.resolve("teardown", "t").unwrap().is_none());
}

/// A rename must leave the *new* name in the catalog, not merely be accepted:
/// the `-1`/`+1` pair is built from the live row positionally, so a wrong value
/// in the right slot is the failure a "the push succeeded" assertion misses.
#[test]
fn a_rename_stores_the_new_name_for_a_table_and_for_a_view() {
    let Some(srv) = ServerHandle::start() else { return };
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    client.create_schema("ren").unwrap();
    let cols = [
        ColumnDef::new("id", TypeCode::U64, false),
        ColumnDef::new("v", TypeCode::I64, false),
    ];
    let tid = client
        .create_table("ren", "t", &cols, &[0], TableProps::default(), &[])
        .unwrap();
    let vid = client.create_view("ren", "v", tid, &cols).unwrap();

    client.alter_rename_relation("ren", "t", "t2").unwrap();
    client.alter_rename_relation("ren", "v", "v2").unwrap();

    assert!(
        client.resolve("ren", "t").unwrap().is_none(),
        "the old table name is gone"
    );
    assert!(
        client.resolve("ren", "v").unwrap().is_none(),
        "the old view name is gone"
    );
    assert_eq!(client.resolve("ren", "t2").unwrap().expect("t2 resolves").tid, tid);
    assert_eq!(client.resolve("ren", "v2").unwrap().expect("v2 resolves").tid, vid);
    // The rest of each row survived the rewrite pair — a `-1` that changed
    // anything but the name would have been refused, and the `+1` carries the
    // same payload.
    let t2 = client.resolve("ren", "t2").unwrap().unwrap();
    assert_eq!(t2.schema.num_columns(), cols.len());
}

/// A mixed-sign bundle keeps the ascending creation order: `ALTER VIEW … AS`
/// retracts the incumbent and registers its replacement in one VIEW_TAB batch,
/// which only applies if COL_TAB and the circuit families land first.
#[test]
fn an_alter_view_bundle_still_applies_in_creation_order() {
    let Some(srv) = ServerHandle::start() else { return };
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    client.create_schema("mixed").unwrap();
    let cols = [
        ColumnDef::new("id", TypeCode::U64, false),
        ColumnDef::new("v", TypeCode::I64, false),
    ];
    let tid = client
        .create_table("mixed", "t", &cols, &[0], TableProps::default(), &[])
        .unwrap();
    let first = client.create_view("mixed", "v", tid, &cols).unwrap();

    let mut cb = gnitz_core::CircuitBuilder::new(0, tid);
    let scan = cb.input_delta();
    cb.sink(scan);
    let vids = client
        .create_view_chain(
            "mixed",
            vec![gnitz_core::PlannedView {
                name: gnitz_core::ViewName::Named("v".to_string()),
                sql_text: String::new(),
                circuit: cb.build(),
                output_columns: cols.to_vec(),
                pk_cols: vec![0],
                capacity_bytes: None,
                delta_bytes: None,
            }],
            Some("v"),
        )
        .expect("the replacement bundle applies");
    assert_ne!(vids[0], first, "the replacement takes a fresh id");
    assert_eq!(client.resolve("mixed", "v").unwrap().unwrap().tid, vids[0]);
}
