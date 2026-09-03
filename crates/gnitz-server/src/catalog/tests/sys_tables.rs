use super::*;

#[test]
fn circuit_tables_have_compound_view_id_sub_pk() {
    // from_wire_cols(&[0, 1]) must produce a 2-column PK whose stride is the
    // sum of the first two columns (U64 + U64 = 16 bytes).
    for schema in [
        SysFamily::CircuitNodes.schema(),
        SysFamily::CircuitEdges.schema(),
        SysFamily::CircuitNodeColumns.schema(),
    ] {
        assert_eq!(schema.pk_indices(), &[0, 1], "circuit PK must be (col0, col1)");
        assert_eq!(schema.pk_stride(), 16, "two U64 PK columns pack to 16 bytes");
    }
}

/// A circuit row's at-rest PK region is `view_id_BE ‖ sub_BE`, so the per-view
/// prefix seek `load_circuit` runs lands on the leading bytes. Written through
/// the production row codec and read back as literal bytes — the one place the
/// engine's half of the key layout is spelled twice on purpose.
#[test]
fn a_circuit_rows_at_rest_pk_leads_with_the_view_id() {
    let mut bb = BatchBuilder::new(SysFamily::CircuitNodes.schema());
    gnitz_wire::sys_rows::write_circuit_node_row(
        &mut bb,
        &gnitz_wire::sys_rows::CircuitNodeRow {
            view_id: 0x1122,
            node_id: 0xAABB,
            opcode: 0,
            source_table: None,
            expr_program: None,
        },
        1,
    )
    .unwrap();
    let batch = bb.finish();
    assert_eq!(
        batch.get_pk_bytes(0),
        [0, 0, 0, 0, 0, 0, 0x11, 0x22, 0, 0, 0, 0, 0, 0, 0xAA, 0xBB],
    );
}

#[test]
fn family_pks_by_sign_separates_a_drop_from_a_rename() {
    // A plain `-1` TABLE_TAB row is a DROP.
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    push_table_tab_row(&mut bb, 42, PUBLIC_SCHEMA_ID, "t", 0, 0, -1);
    let dropped = bb.finish();
    assert_eq!(family_pks_by_sign(&dropped, false), vec![42]);
    assert!(family_pks_by_sign(&dropped, true).is_empty());

    // A rename is a `(-1, +1)` rewrite pair on one PK: neither a create nor a
    // drop, so DDL paths keyed off these lists leave a renamed table alone.
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    push_table_tab_row(&mut bb, 42, PUBLIC_SCHEMA_ID, "t", 0, 0, -1);
    push_table_tab_row(&mut bb, 42, PUBLIC_SCHEMA_ID, "t2", 0, 0, 1);
    let renamed = bb.finish();
    assert!(family_pks_by_sign(&renamed, false).is_empty());
    assert!(family_pks_by_sign(&renamed, true).is_empty());
}
