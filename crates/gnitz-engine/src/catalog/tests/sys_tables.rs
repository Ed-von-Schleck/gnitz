use super::*;

#[test]
fn test_pk_col_packing() {
    for case in [vec![0u32], vec![7], vec![0, 1], vec![3, 9, 40, 64]] {
        let list = unpack_pk_cols(pack_pk_cols(&case));
        assert_eq!(list.decoded_count(), case.len());
        assert_eq!(list.as_slice(), case.as_slice());
    }

    // Reserved bits [32..63) are zero, bit 63 is set on packed values.
    let packed = pack_pk_cols(&[3, 9, 40, 64]);
    assert_eq!(packed >> 63, 1);
    assert_eq!((packed >> 32) & 0x7FFF_FFFF, 0);

    // Bare-index fallback (flag clear → single index).
    assert_eq!(unpack_pk_cols(0).as_slice(), &[0]);
    assert_eq!(unpack_pk_cols(0).decoded_count(), 1);
    assert_eq!(unpack_pk_cols(7).as_slice(), &[7]);
    assert_eq!(unpack_pk_cols(7).decoded_count(), 1);

    // Malformed flag-set value with an out-of-range count: as_slice and
    // decoded_count must be panic-free, slice clamped to PK_LIST_MAX_COLS.
    // `15` is the max the 4-bit count field can hold (independent of the cap).
    let malformed = unpack_pk_cols(PK_LIST_PACKED_FLAG | 15);
    assert_eq!(malformed.decoded_count(), 15);
    assert_eq!(malformed.as_slice(), vec![0u32; PK_LIST_MAX_COLS].as_slice());
}

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

#[test]
fn pack_view_pk_at_rest_is_view_id_leading_opk() {
    // The at-rest OPK image (extend_pk → big-endian) is view_id_BE then
    // sub_BE, so a view_id prefix seek lands on the leading bytes.
    let pk = pack_view_pk(0x1122, 0xAABB);
    let at_rest = pk.to_be_bytes();
    assert_eq!(
        u64::from_be_bytes(at_rest[0..8].try_into().unwrap()),
        0x1122,
        "view_id (PK col 0) must lead the at-rest OPK region",
    );
    assert_eq!(
        u64::from_be_bytes(at_rest[8..16].try_into().unwrap()),
        0xAABB,
        "sub (PK col 1) follows view_id",
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
