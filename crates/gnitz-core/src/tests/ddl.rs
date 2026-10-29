use super::*;
use crate::block_on;
use crate::test_support::{client_pair, kv_schema, rel};
use gnitz_wire::TypeCode;

/// DDL inside a transaction is refused even when it has nothing to drop, and
/// the transaction stays open.
#[test]
fn ddl_with_nothing_to_drop_is_refused_inside_a_transaction() {
    let (mut c, _peer) = client_pair();
    c.txn_begin().unwrap();
    let err = block_on(c.drop_table(&[], true)).unwrap_err().to_string();
    assert!(err.contains("not allowed inside a transaction"), "{err}");
    let err = block_on(c.drop_view(&[], true)).unwrap_err().to_string();
    assert!(err.contains("not allowed inside a transaction"), "{err}");
    assert!(c.txn_active());
}

/// Foreign-key slots that are neither absent nor one per column are refused
/// before anything is sent — the peer is gone, so a request would fail as a
/// transport error instead.
#[test]
fn create_table_refuses_an_fk_it_cannot_write() {
    let (mut c, peer) = client_pair();
    drop(peer);
    let schema = kv_schema(TypeCode::I64);
    let fk = Some(FkTarget::Table(FkRef {
        table_id: 16,
        col: 0,
        on_delete: gnitz_wire::FkAction::Restrict,
    }));
    for fks in [vec![fk], vec![None, fk, None]] {
        let want = format!("{} foreign-key slots for 2 columns", fks.len());
        let def = TableDef {
            fks,
            ..TableDef::new(rel("public", "t"), schema.clone())
        };
        let err = block_on(c.create_table(&def)).unwrap_err().to_string();
        assert!(err.contains(&want), "{err}");
    }
}
