#![cfg(feature = "integration")]

//! What the master refuses to write into its own catalog.
//!
//! `GnitzClient` canonicalizes and validates every name it stores, but
//! `Session::push_ddl_txn` is public — so the rules the engine's caches, its
//! qualified-name keys and `hook_schema_dir`'s filesystem path depend on are
//! enforced at the master's trust boundary, and these tests drive that boundary
//! directly with hand-built bundles.

use gnitz_core::connection::{COL_TAB, IDX_TAB, SCHEMA_TAB, SEQ_TAB, TABLE_TAB};
use gnitz_core::protocol::{BatchAppender, ColumnDef, TypeCode, ZSetBatch};
use gnitz_core::types::sys_schema;
use gnitz_core::ViewReplace;
use gnitz_core::{CircuitBuilder, GnitzClient, PlannedView, Session, TableProps};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::sys_rows::{
    write_circuit_node_row, write_col_tab_row, write_idx_tab_row, write_schema_tab_row, write_table_tab_row,
    CircuitNodeRow, IdxTabRow, SchemaTabRow, TableTabRow,
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
    two_columns_as(owner_id, gnitz_wire::OWNER_KIND_TABLE)
}

/// The COL_TAB batch for a `(id U64 PK, v I64)` relation owned by `owner_id`,
/// with a chosen `owner_kind` — the field that decides whether a row declares an
/// FK, so it may not disagree with what the owner actually is.
fn two_columns_as(owner_id: u64, owner_kind: u64) -> ZSetBatch {
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
        write_col_tab_row(&mut a, &cd.col_tab_row(owner_id, owner_kind, i), 1).unwrap();
    }
    b
}

/// One IDX_TAB batch of `(index_id, weight)` rows, all naming `owner_id`'s
/// column 1.
fn index_rows(owner_id: u64, rows: &[(u64, i64)]) -> ZSetBatch {
    let s = sys_schema(IDX_TAB);
    let mut b = ZSetBatch::new(s);
    let mut a = BatchAppender::new(&mut b, s);
    for &(index_id, weight) in rows {
        write_idx_tab_row(
            &mut a,
            &IdxTabRow {
                index_id,
                owner_id,
                source_col_idx: gnitz_wire::pack_pk_cols(&[1]),
                name: "ix",
                flags: 0,
            },
            weight,
        );
    }
    b
}

/// A `(schema, table)` under a fresh schema, through the ordinary client path.
fn a_table(client: &mut GnitzClient, schema: &str) -> u64 {
    client.create_schema(schema).unwrap();
    client
        .create_table(
            schema,
            "t",
            &[
                ColumnDef::new("id", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            &[0],
            TableProps::default(),
            &[],
        )
        .unwrap()
}

fn session(srv: &ServerHandle) -> Session {
    Session::connect(srv.sock_path()).expect("connect").0
}

/// A schema name is the one catalog name the engine interpolates into a
/// filesystem path, so it takes the full identifier rule at the trust boundary —
/// `..` and `/` never reach `create_dir_all`.
#[test]
fn a_schema_name_that_is_not_an_identifier_is_refused() {
    let srv = ServerHandle::start();
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
    let srv = ServerHandle::start();
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
    let srv = ServerHandle::start();
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
    let srv = ServerHandle::start();
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
    let srv = ServerHandle::start();
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

/// An index flagged engine-internal is one `DROP INDEX` refuses, so a client
/// that could set the flag could deny a name in the index namespace permanently
/// from one frame. Only `submit_local` — the FK auto-index, which bypasses the
/// precheck — may set it.
#[test]
fn a_wire_supplied_internal_index_flag_is_refused() {
    let srv = ServerHandle::start();
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
            name: "forged",
            flags: gnitz_wire::IndexProps {
                is_unique: false,
                is_internal: true,
            }
            .pack(),
        },
        1,
    );
    let err = format!("{:?}", s.push_ddl_txn(&[(IDX_TAB, b)]).unwrap_err());
    assert!(err.contains("engine-internal"), "{err}");
}

/// A VIEW_TAB `+1` naming an `owner_view_id` no relation holds is refused: the
/// drop cascade keys on that column, so a forged owner would point a cascade at
/// nothing.
#[test]
fn a_wire_supplied_owner_view_id_must_name_a_real_view() {
    let srv = ServerHandle::start();
    let mut s = session(&srv);
    let sid = s.alloc_schema_id().unwrap();
    s.push_ddl_txn(&[(SCHEMA_TAB, schema_row(sid, "owned"))]).unwrap();

    let vid = s.alloc_table_id().unwrap();
    let view_s = sys_schema(gnitz_wire::VIEW_TAB);
    let mut b = ZSetBatch::new(view_s);
    gnitz_wire::sys_rows::write_view_tab_row(
        &mut BatchAppender::new(&mut b, view_s),
        &gnitz_wire::sys_rows::ViewTabRow {
            view_id: vid,
            schema_id: sid,
            name: "seg",
            pk_col_idx: 0,
            capacity_bytes: 0,
            delta_bytes: 0,
            owner_view_id: 999_999,
        },
        1,
    );
    let err = format!("{:?}", s.push_ddl_txn(&[(gnitz_wire::VIEW_TAB, b)]).unwrap_err());
    assert!(err.contains("owner_view_id"), "{err}");
}

/// An all-negative multi-family bundle is applied View → Table → Schema: a view
/// must be retired before the table it reads, and the schema row last, or its
/// member-count guard rejects it. `DROP SCHEMA` is exactly that bundle.
#[test]
fn drop_schema_retires_a_view_its_table_and_the_schema_in_one_bundle() {
    let srv = ServerHandle::start();
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
    let srv = ServerHandle::start();
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
    let srv = ServerHandle::start();
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

    let mut cb = gnitz_core::CircuitBuilder::new(tid);
    let scan = cb.input_delta();
    cb.sink(scan);
    let vids = client
        .create_view_chain(
            "mixed",
            "v",
            vec![gnitz_core::PlannedView {
                seg: 0,
                circuit: cb.build(),
                output_columns: cols.to_vec(),
                pk_cols: vec![0],
                capacity_bytes: None,
                delta_bytes: None,
            }],
            ViewReplace::BodyOnly,
        )
        .expect("the replacement bundle applies");
    assert_ne!(vids[0], first, "the replacement takes a fresh id");
    assert_eq!(client.resolve("mixed", "v").unwrap().unwrap().tid, vids[0]);
}

/// `_sequences` carries the durable object-id high-waters, the checkpoint
/// generation and the recorded topology. Boot feeds them straight into the id
/// counters, where a forged value trips the allocator's ceiling assertion on
/// every subsequent start — unrecoverable. So the family is not writable from
/// the wire at all.
#[test]
fn a_sequence_block_is_refused_from_the_wire() {
    let srv = ServerHandle::start();
    let mut s = session(&srv);
    let seq = sys_schema(SEQ_TAB);
    let mut b = ZSetBatch::new(seq);
    BatchAppender::new(&mut b, seq).add_row(2, 1).u64_val(1 << 40);
    let err = format!("{:?}", s.push_ddl_txn(&[(SEQ_TAB, b)]).unwrap_err());
    assert!(err.contains("not writable from the wire"), "{err}");
}

/// A COL_TAB row on an owner nothing registers is unretractable — the only
/// COL_TAB retractor is the owner's own drop cascade, which returns early on an
/// unregistered id — and `apply_fk_edges_and_locks` would build a permanent FK
/// edge from its payload.
#[test]
fn a_column_block_whose_owner_is_never_registered_is_refused() {
    let srv = ServerHandle::start();
    let mut s = session(&srv);
    let tid = s.alloc_table_id().unwrap();
    let err = format!("{:?}", s.push_ddl_txn(&[(COL_TAB, two_columns(tid))]).unwrap_err());
    assert!(err.contains("does not create and the catalog does not hold"), "{err}");
}

/// `owner_kind` alone decides whether a COL_TAB row declares a foreign key, so a
/// row claiming a kind its owner does not have plants an edge no arm validates.
#[test]
fn a_column_row_whose_owner_kind_contradicts_its_owner_is_refused() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let tid = a_table(&mut client, "kindclash");

    let mut s = session(&srv);
    let err = format!(
        "{:?}",
        s.push_ddl_txn(&[(COL_TAB, two_columns_as(tid, gnitz_wire::OWNER_KIND_VIEW))])
            .unwrap_err()
    );
    assert!(err.contains("declares owner_kind"), "{err}");
}

/// A circuit `+1` under a foreign `view_id` either makes `has_dependents` of its
/// source permanently true, blocking `DROP TABLE` forever, or injects nodes into
/// a running view's circuit that the next `load_circuit` picks up.
#[test]
fn a_circuit_row_naming_a_view_the_bundle_does_not_create_is_refused() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let tid = a_table(&mut client, "phantomview");

    let mut s = session(&srv);
    let phantom = s.alloc_table_id().unwrap();
    let nodes = sys_schema(gnitz_wire::CIRCUIT_NODES_TAB);
    let mut b = ZSetBatch::new(nodes);
    write_circuit_node_row(
        &mut BatchAppender::new(&mut b, nodes),
        &CircuitNodeRow {
            view_id: phantom,
            node_id: 0,
            opcode: gnitz_wire::OPCODE_SCAN_DELTA,
            source_table: Some(tid),
            expr_program: None,
        },
        1,
    )
    .unwrap();
    let err = format!(
        "{:?}",
        s.push_ddl_txn(&[(gnitz_wire::CIRCUIT_NODES_TAB, b)]).unwrap_err()
    );
    assert!(err.contains("does not create"), "{err}");

    // The table it named is still droppable, which is the whole point.
    client.drop_table("phantomview", "t", false).unwrap();
}

/// Two `+1` rows under one schema name both pass the cache check and both apply:
/// `schema_by_name` keeps the second, leaving the first id live and unreachable,
/// and dropping the reachable one deletes the orphan's directory.
#[test]
fn two_schema_rows_sharing_a_name_in_one_bundle_are_refused() {
    let srv = ServerHandle::start();
    let mut s = session(&srv);
    let (a, b) = (s.alloc_schema_id().unwrap(), s.alloc_schema_id().unwrap());
    let sc = sys_schema(SCHEMA_TAB);
    let mut batch = ZSetBatch::new(sc);
    let mut app = BatchAppender::new(&mut batch, sc);
    for id in [a, b] {
        write_schema_tab_row(
            &mut app,
            &SchemaTabRow {
                schema_id: id,
                name: "twice",
            },
            1,
        );
    }
    let err = format!("{:?}", s.push_ddl_txn(&[(SCHEMA_TAB, batch)]).unwrap_err());
    assert!(err.contains("Schema already exists"), "{err}");
    assert!(s.push_ddl_txn(&[(SCHEMA_TAB, schema_row(a, "twice"))]).is_ok());
}

/// The per-PK shape rules, each on a family whose declared facts make it the one
/// that fires. None of these is expressible by any legitimate emitter.
#[test]
fn the_per_pk_shape_rules_reject_what_no_emitter_writes() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let tid = a_table(&mut client, "shapes");
    let mut s = session(&srv);

    // A row above ±1: `retract_pk_list` emits a hard `-1`, so a row left at 2 is
    // under-retracted and becomes a permanent live ghost.
    let sid = s.alloc_schema_id().unwrap();
    let sc = sys_schema(SCHEMA_TAB);
    let mut heavy = ZSetBatch::new(sc);
    write_schema_tab_row(
        &mut BatchAppender::new(&mut heavy, sc),
        &SchemaTabRow {
            schema_id: sid,
            name: "heavy",
        },
        2,
    );
    let err = format!("{:?}", s.push_ddl_txn(&[(SCHEMA_TAB, heavy)]).unwrap_err());
    assert!(err.contains("at weight 2"), "{err}");

    // Neither SCHEMA_TAB nor IDX_TAB has a rename surface, so neither admits a
    // rewrite pair on one PK.
    let mut pair = ZSetBatch::new(sc);
    let mut app = BatchAppender::new(&mut pair, sc);
    for w in [-1, 1] {
        write_schema_tab_row(
            &mut app,
            &SchemaTabRow {
                schema_id: sid,
                name: "p",
            },
            w,
        );
    }
    let err = format!("{:?}", s.push_ddl_txn(&[(SCHEMA_TAB, pair)]).unwrap_err());
    assert!(err.contains("more than one row for schema"), "{err}");

    let iid = s.alloc_index_id().unwrap();
    let err = format!(
        "{:?}",
        s.push_ddl_txn(&[(IDX_TAB, index_rows(tid, &[(iid, -1), (iid, 1)]))])
            .unwrap_err()
    );
    assert!(err.contains("more than one row for index"), "{err}");

    // TABLE_TAB does admit a pair, so its rule is one row per *sign*.
    let dup = s.alloc_table_id().unwrap();
    let tt = sys_schema(TABLE_TAB);
    let mut twice = ZSetBatch::new(tt);
    let mut app = BatchAppender::new(&mut twice, tt);
    for name in ["one", "two"] {
        write_table_tab_row(
            &mut app,
            &TableTabRow {
                table_id: dup,
                schema_id: 2,
                name,
                pk_col_idx: gnitz_wire::pack_pk_cols(&[0]),
                flags: TableProps::default().pack(),
            },
            1,
        );
    }
    let err = format!(
        "{:?}",
        s.push_ddl_txn(&[(COL_TAB, two_columns(dup)), (TABLE_TAB, twice)])
            .unwrap_err()
    );
    assert!(err.contains("more than one row for table"), "{err}");

    // `hook_index_register` raises the index-id counter off the ingested row, on
    // the live path and again on boot replay, and the index allocator carries no
    // assertion — so a crafted id durably poisons the counter.
    let err = format!(
        "{:?}",
        s.push_ddl_txn(&[(IDX_TAB, index_rows(tid, &[(gnitz_wire::RELATION_ID_CEILING, 1)]))])
            .unwrap_err()
    );
    assert!(err.contains("id ceiling"), "{err}");
}

/// `create_view_chain` with a `replaces` refuses to rebuild a capacity-bounded
/// or delta-fed view: the replacement carries no `WITH` clause, so the rebuild
/// would silently drop it.
#[test]
fn a_bounded_or_fed_view_cannot_be_retargeted() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let tid = a_table(&mut client, "retarget");
    let view = |capacity_bytes, delta_bytes| {
        let mut cb = CircuitBuilder::new(tid);
        let scan = cb.input_delta();
        cb.sink(scan);
        PlannedView {
            seg: 0,
            circuit: cb.build(),
            output_columns: vec![
                ColumnDef::new("id", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
            capacity_bytes,
            delta_bytes,
        }
    };
    let budgets = [("b", Some(1 << 20), None), ("f", None, Some(1 << 20))];
    for (name, capacity, delta) in budgets {
        client
            .create_view_chain("retarget", name, vec![view(capacity, delta)], ViewReplace::Nothing)
            .unwrap();
    }
    // `BodyOnly` is `ALTER VIEW … AS`, whose grammar carries no `WITH (…)`: the
    // budget would vanish with nothing in the statement saying so.
    for (name, needle) in [("b", "capacity-bounded"), ("f", "delta feed")] {
        let err = client
            .create_view_chain("retarget", name, vec![view(None, None)], ViewReplace::BodyOnly)
            .expect_err("retargeting must be refused")
            .to_string();
        assert!(err.contains(needle), "got: {err}");
    }
    // `WithBudgets` is `CREATE OR REPLACE VIEW`, which writes the budgets out — so
    // the same bundle that `BodyOnly` refuses is exactly what it asks for.
    for (name, capacity, delta) in budgets {
        client
            .create_view_chain("retarget", name, vec![view(capacity, delta)], ViewReplace::WithBudgets)
            .unwrap_or_else(|e| panic!("replacing '{name}' with its budgets restated: {e}"));
    }
}
