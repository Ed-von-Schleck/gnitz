use super::*;
use crate::test_support::push_table_tab_row;
use gnitz_store::storage::BatchBuilder;

#[test]
fn the_circuit_table_has_a_compound_view_id_node_id_pk() {
    // from_wire_cols(&[0, 1]) must produce a 2-column PK whose stride is the
    // sum of the first two columns (U64 + U64 = 16 bytes).
    let schema = SysFamily::CircuitNodes.schema();
    assert_eq!(schema.pk_indices(), &[0, 1], "circuit PK must be (col0, col1)");
    assert_eq!(schema.pk_stride(), 16, "two U64 PK columns pack to 16 bytes");
}

/// A circuit row's at-rest PK region is `view_id_BE ‖ node_id_BE`, so the per-view
/// prefix seek `load_circuit` runs lands on the leading bytes. Written through
/// the production row codec and read back as literal bytes — the one place the
/// engine's half of the key layout is spelled twice on purpose.
#[test]
fn a_circuit_rows_at_rest_pk_leads_with_the_view_id() {
    let mut bb = BatchBuilder::new(*SysFamily::CircuitNodes.schema());
    gnitz_wire::sys_rows::write_circuit_node_row(
        &mut bb,
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
    let batch = bb.finish();
    assert_eq!(
        batch.get_pk_bytes(0),
        [0, 0, 0, 0, 0, 0, 0x11, 0x22, 0, 0, 0, 0, 0, 0, 0xAA, 0xBB],
    );
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

/// A COL_TAB row's at-rest PK region is `owner_id_BE ‖ col_idx_BE` — what makes
/// one owner's records the contiguous band the column scan and the drop cascade
/// walk. Read back as literal bytes.
#[test]
fn a_column_records_at_rest_pk_leads_with_the_owner() {
    let mut bb = BatchBuilder::new(*SysFamily::Column.schema());
    gnitz_wire::sys_rows::write_col_tab_row(
        &mut bb,
        &gnitz_wire::sys_rows::ColTabRow {
            owner_id: 0x1122,
            col_idx: 0xAABB,
            name: "c",
            ty: gnitz_wire::ColType::of(gnitz_wire::TypeCode::U64),
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
            is_hidden: false,
        },
        1,
    );
    let batch = bb.finish();
    assert_eq!(
        batch.get_pk_bytes(0),
        [0, 0, 0, 0, 0, 0, 0x11, 0x22, 0, 0, 0, 0, 0, 0, 0xAA, 0xBB],
    );
    assert_eq!(gnitz_wire::unpack_pair_pk(batch.get_pk(0)), (0x1122, 0xAABB));
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
