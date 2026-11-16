use super::*;
use crate::block_on;
use crate::test_support::{client_pair, kv_schema, rel};
use gnitz_wire::{ColumnDef, TypeCode};

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

/// A foreign key on a column the schema does not have is refused before
/// anything is sent — the peer is gone, so a request would fail as a
/// transport error instead.
#[test]
fn create_table_refuses_an_fk_it_cannot_write() {
    let (mut c, peer) = client_pair();
    drop(peer);
    let schema = kv_schema(TypeCode::I64);
    let fk = FkTarget::Table(FkRef {
        table_id: 16,
        col: 0,
        on_delete: gnitz_wire::FkAction::Restrict,
    });
    for col in [2, 5] {
        let want = format!("a foreign key on column {col}, past the table's 2 columns");
        let def = TableDef {
            fks: [(col, fk)].into(),
            ..TableDef::new(rel("public", "t"), schema.clone())
        };
        let err = block_on(c.create_table(&def)).unwrap_err().to_string();
        assert!(err.contains(&want), "{err}");
    }
}

/// An auto-name is an identifier built from the table and column names, every
/// byte outside the identifier charset `_`, and suffixed past every taken name.
#[test]
fn an_auto_name_is_an_identifier_suffixed_past_taken_names() {
    let columns = [
        ColumnDef::new("a", TypeCode::I64, false),
        ColumnDef::new("B c", TypeCode::I64, false),
        ColumnDef::new("d-e", TypeCode::I64, false),
    ];
    let t = rel("s", "t");
    assert_eq!(auto_index_name(&t, &columns, &[2], &HashSet::new()), "s__t__idx_d_e");
    let mut taken = HashSet::new();
    for want in ["s__t__idx_a_b_c", "s__t__idx_a_b_c_2", "s__t__idx_a_b_c_3"] {
        let name = auto_index_name(&t, &columns, &[0, 1], &taken);
        assert_eq!(name, want);
        taken.insert(name);
    }
}

/// An unnamed inline index routes around a live index of any owner and around
/// every name in the bundle — distinct column sets can render one base when a
/// column name holds `_` — and a written name is left as written, even one
/// equal to an auto-name.
#[test]
fn inline_auto_names_route_around_live_and_bundled_names() {
    let columns: Vec<ColumnDef> = ["id", "a", "b", "d", "e_f", "d_e", "f"]
        .iter()
        .map(|&n| ColumnDef::new(n, TypeCode::I64, false))
        .collect();
    let spec = |cols: &[u32], name: Option<&str>| InlineUniqueIndex {
        cols: PkColList::from_slice(cols),
        name: name.map(str::to_string),
    };
    let specs = [
        spec(&[1], None),
        spec(&[2], Some("keep")),
        spec(&[3, 4], None),
        spec(&[5, 6], None),
        spec(&[2], Some("s__t__idx_b")),
        spec(&[2], None),
    ];
    let live = ["s__t__idx_a", "s__t__idx_a_2", "keep"].map(str::to_string).into();
    let names = inline_index_names(&rel("s", "t"), &columns, &specs, live).unwrap();
    assert_eq!(
        names,
        [
            "s__t__idx_a_3",
            "keep",
            "s__t__idx_d_e_f",
            "s__t__idx_d_e_f_2",
            "s__t__idx_b",
            "s__t__idx_b_2"
        ]
    );
}
