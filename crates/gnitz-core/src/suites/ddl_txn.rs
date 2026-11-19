//! What the master's `DDL_TXN` handler decides itself, around the catalog's own
//! rules. A hand-built bundle goes through the session, without the client's
//! checks.

use super::*;
use crate::{ColumnChange, sys_schema};
use gnitz_wire::{PkColList, SchemaTabRow, SysFamily, SysRow};

/// A SCHEMA_TAB batch registering `(schema_id, name)`.
fn schema_row(schema_id: u64, name: &str) -> ZSetBatch {
    let s = sys_schema(SysFamily::Schema);
    let mut b = ZSetBatch::new(s);
    SchemaTabRow { schema_id, name: name.into() }.write(&mut b.builder(), 1);
    b
}

/// The refusal of `families` sent as one DDL bundle.
fn refusal(s: &mut Session, families: &[(SysFamily, ZSetBatch)]) -> String {
    let sent = s.submit(Request::DdlTxn(families));
    drive(s, sent).0.unwrap_err().to_string()
}

/// Refused whole, before the catalog reads it: a bundle with two blocks for one
/// family, which every list the handler derives would read as its first block
/// alone while both were applied; and `sequences`, whose durable high-waters
/// boot feeds into the id counters, where a forged one trips the allocator's
/// ceiling on every later start.
#[test]
fn a_bundle_the_handler_cannot_read_as_one_is_refused_whole() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let mut s = Session::connect(srv.sock_path()).unwrap();
    let (a, b) = (
        block_on(client.alloc_ids(1)).unwrap(),
        block_on(client.alloc_ids(1)).unwrap(),
    );
    let err = refusal(
        &mut s,
        &[
            (SysFamily::Schema, schema_row(a, "one")),
            (SysFamily::Schema, schema_row(b, "two")),
        ],
    );
    assert!(err.contains("two blocks"), "{err}");

    let seq = sys_schema(SysFamily::Sequence);
    let mut forged = ZSetBatch::new(seq);
    forged.builder().begin_row(2, 1).put_int((1u64 << 40) as u128);
    let err = refusal(&mut s, &[(SysFamily::Sequence, forged)]);
    assert!(err.contains("not writable from the wire"), "{err}");

    // Neither wrote anything: both names are still free.
    block_on(client.create_schema("one", false)).unwrap();
    block_on(client.create_schema("two", false)).unwrap();
}

/// A unique index the catalog's owner or dropped-column rule refuses is refused
/// as such, before the pre-flight scans its owner for the duplicate below.
#[test]
fn a_unique_index_the_catalog_refuses_is_not_scanned_for_duplicates() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (sn, tid, schema) = create_table(&mut client, schema_of(&[("id", TypeCode::U64), ("v", TypeCode::I64)]));
    let mut dup = ZSetBatch::new(&schema);
    let mut app = dup.builder();
    app.begin_row(1, 1).put_int(5);
    app.begin_row(2, 1).put_int(5);
    block_on(client.push(tid, &schema, &dup, WireConflictMode::Update)).unwrap();

    let unique_on_v = |client: &mut GnitzClient, owner: &RelName| {
        let desc = block_on(client.resolve_relation(owner)).unwrap();
        block_on(client.create_index(owner, &desc, PkColList::from_slice(&[1]), Some("ix"), true))
            .unwrap_err()
            .to_string()
    };

    // The backfill copies both rows into the view.
    block_on(client.create_view_chain(bundle(rel(&sn, "v"), Vec::new(), segment(tid, &schema)))).unwrap();
    let err = unique_on_v(&mut client, &rel(&sn, "v"));
    assert!(err.contains("cannot carry a UNIQUE index"), "{err}");

    block_on(client.drop_view(&[rel(&sn, "v")], false)).unwrap();
    block_on(client.alter_column(tid, 1, ColumnChange::Drop)).unwrap();
    let err = unique_on_v(&mut client, &rel(&sn, "t"));
    assert!(err.contains("is dropped"), "{err}");
}
