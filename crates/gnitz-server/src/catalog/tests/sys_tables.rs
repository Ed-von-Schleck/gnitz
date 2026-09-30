use super::*;
use crate::test_support::{col_def, push_table_tab_row};
use gnitz_store::storage::BatchBuilder;

/// A pair-keyed family's at-rest PK is `owner_BE ‖ member_BE`, so one owner's
/// rows form one contiguous prefix band.
#[test]
fn a_pair_keyed_familys_at_rest_pk_leads_with_its_owner() {
    let mut circuit = BatchBuilder::new(*SysFamily::CircuitNodes.schema());
    gnitz_wire::sys_rows::write_circuit_node_row(
        &mut circuit,
        &gnitz_wire::sys_rows::CircuitNodeRow {
            view_id: 0x1122,
            node_id: 0xAABB,
            opcode: 0,
            source_table: None,
            inputs: [None; 2],
            params: None,
        },
        1,
    );
    let mut column = BatchBuilder::new(*SysFamily::Column.schema());
    col_def("c", gnitz_wire::TypeCode::U64).write_col_tab_row(&mut column, 0x1122, 0xAABB, 1);

    for (family, bb) in [(SysFamily::CircuitNodes, circuit), (SysFamily::Column, column)] {
        let batch = bb.finish();
        assert_eq!(
            batch.get_pk_bytes(0),
            [0, 0, 0, 0, 0, 0, 0x11, 0x22, 0, 0, 0, 0, 0, 0, 0xAA, 0xBB],
            "{family:?}"
        );
        assert_eq!(family.leading_id(batch.get_pk(0)), 0x1122, "{family:?}");
    }
}

#[test]
fn family_pk_partition_separates_a_drop_from_a_rename() {
    // A plain `-1` TABLE_TAB row is a DROP.
    let mut bb = BatchBuilder::new(*SysFamily::Table.schema());
    push_table_tab_row(&mut bb, 42, PUBLIC_SCHEMA_ID, "t", 0, 0, -1);
    let p = family_pk_partition(SysFamily::Table, &bb.finish());
    assert_eq!((p.creates, p.drops), (vec![], vec![42]));

    // A rename is a `(-1, +1)` rewrite pair on one PK: neither a create nor a
    // drop, so DDL paths keyed off these lists leave a renamed table alone.
    let mut bb = BatchBuilder::new(*SysFamily::Table.schema());
    push_table_tab_row(&mut bb, 42, PUBLIC_SCHEMA_ID, "t", 0, 0, -1);
    push_table_tab_row(&mut bb, 42, PUBLIC_SCHEMA_ID, "t2", 0, 0, 1);
    let p = family_pk_partition(SysFamily::Table, &bb.finish());
    assert_eq!((p.creates, p.drops), (vec![], vec![]));
}

/// Every COL_TAB word must fit the width it is stored at and decode to a value
/// `write_col_tab_row` could emit.
#[test]
fn read_col_tab_row_refuses_forged_words() {
    use gnitz_wire::sys_rows::SysRowSink;
    // `[type_code, is_nullable, fk_table_id, fk_col_idx, is_hidden, scale]`.
    const SOUND: [u64; 6] = [gnitz_wire::TypeCode::I64.as_wire() as u64, 0, 0, 0, 0, 0];
    let row = |words: [u64; 6]| {
        let mut bb = BatchBuilder::new(*SysFamily::Column.schema());
        SysRowSink::begin_row(&mut bb, &[16, 0], 1);
        SysRowSink::put_string(&mut bb, "c");
        for w in words {
            SysRowSink::put_u64(&mut bb, w);
        }
        SysRowSink::end_row(&mut bb);
        bb.finish()
    };
    assert!(read_col_tab_row(&row(SOUND), 0).is_ok());
    let forge = |slot: usize, w: u64| {
        let mut words = SOUND;
        words[slot] = w;
        words
    };
    for (what, words) in [
        ("type_code past a byte", forge(0, 0x104)),
        ("unknown type_code", forge(0, 99)),
        ("is_nullable past a flag", forge(1, 2)),
        ("fk_col_idx past a u32", forge(3, 1 << 32)),
        ("scale past a byte", forge(5, 256)),
        ("scale on an I64 column", forge(5, 3)),
    ] {
        assert!(read_col_tab_row(&row(words), 0).is_err(), "{what}");
    }
}
