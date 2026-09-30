//! What the master's `DDL_TXN` handler decides itself, around the catalog's own
//! rules: `GnitzClient::push_ddl_txn` ships a hand-built bundle without the
//! client's checks.

use super::*;
use gnitz_core::sys_schema;
use gnitz_wire::sys_rows::{write_idx_tab_row, write_schema_tab_row, IdxTabRow, SchemaTabRow};
use gnitz_wire::{IndexProps, IDX_TAB, SCHEMA_TAB, SEQ_TAB};

/// A SCHEMA_TAB batch registering `(schema_id, name)`.
fn schema_row(schema_id: u64, name: &str) -> ZSetBatch {
    let s = sys_schema(SCHEMA_TAB);
    let mut b = ZSetBatch::new(s);
    write_schema_tab_row(&mut BatchAppender::new(&mut b, s), &SchemaTabRow { schema_id, name }, 1);
    b
}

/// Refused whole, before the catalog reads it: a bundle with two blocks for one
/// family, which every list the handler derives would read as its first block
/// alone while both were applied; and `_sequences`, whose durable high-waters
/// boot feeds into the id counters, where a forged one trips the allocator's
/// ceiling on every later start.
#[test]
fn a_bundle_the_handler_cannot_read_as_one_is_refused_whole() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (a, b) = (client.alloc_id().unwrap(), client.alloc_id().unwrap());
    let err = client
        .push_ddl_txn(&[(SCHEMA_TAB, schema_row(a, "one")), (SCHEMA_TAB, schema_row(b, "two"))])
        .unwrap_err()
        .to_string();
    assert!(err.contains("two blocks"), "{err}");

    let seq = sys_schema(SEQ_TAB);
    let mut forged = ZSetBatch::new(seq);
    BatchAppender::new(&mut forged, seq).add_row(2, 1).u64_val(1 << 40);
    let err = client.push_ddl_txn(&[(SEQ_TAB, forged)]).unwrap_err().to_string();
    assert!(err.contains("not writable from the wire"), "{err}");

    // Neither wrote anything: both names are still free.
    client.create_schema("one").unwrap();
    client.create_schema("two").unwrap();
}

/// A unique index the catalog's owner or dropped-column rule refuses is refused
/// as such, before the pre-flight scans its owner for the duplicate below.
#[test]
fn a_unique_index_the_catalog_refuses_is_not_scanned_for_duplicates() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (sn, tid, schema) = create_table(&mut client, schema_of(&[("id", TypeCode::U64), ("v", TypeCode::I64)]));
    let mut dup = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut dup, &schema);
    app.add_row(1, 1).i64_val(5);
    app.add_row(2, 1).i64_val(5);
    client.push(tid, &schema, &dup, WireConflictMode::Update).unwrap();

    let unique_on_v = |client: &mut GnitzClient, owner_id: u64| {
        let s = sys_schema(IDX_TAB);
        let mut b = ZSetBatch::new(s);
        let row = IdxTabRow {
            index_id: client.alloc_id().unwrap(),
            owner_id,
            source_col_idx: gnitz_wire::pack_pk_cols(&[1]),
            name: "ix",
            flags: IndexProps { is_unique: true }.pack(),
        };
        write_idx_tab_row(&mut BatchAppender::new(&mut b, s), &row, 1);
        client.push_ddl_txn(&[(IDX_TAB, b)]).unwrap_err().to_string()
    };

    // The backfill copies both rows into the view.
    let vid = client.create_view(&sn, "v", tid).unwrap();
    let err = unique_on_v(&mut client, vid);
    assert!(err.contains("only a base table can be indexed"), "{err}");

    client.drop_view(&sn, &["v"], false).unwrap();
    client.alter_drop_column(tid, 1).unwrap();
    let err = unique_on_v(&mut client, tid);
    assert!(err.contains("is dropped"), "{err}");
}
