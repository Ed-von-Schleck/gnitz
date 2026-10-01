use super::*;
use crate::test_support::{idx_tab_batch, push_sys_row, table_tab_batch};
use gnitz_wire::IndexProps;
use gnitz_zset::repr::BatchBuilder;

/// A pair-keyed family's at-rest PK is `owner_BE ‖ member_BE`, so one owner's
/// rows form one contiguous prefix band.
#[test]
fn a_pair_keyed_familys_at_rest_pk_leads_with_its_owner() {
    for family in [SysFamily::CircuitNodes, SysFamily::Column] {
        let mut bb = BatchBuilder::new(*family.schema());
        push_sys_row(&mut bb, family, [0x1122, 0xAABB], 1, |_| 0);
        let batch = bb.finish();
        assert_eq!(
            batch.get_pk_bytes(0),
            [0, 0, 0, 0, 0, 0, 0x11, 0x22, 0, 0, 0, 0, 0, 0, 0xAA, 0xBB],
            "{family:?}"
        );
        assert_eq!(family.leading_id(batch.get_pk(0)), 0x1122, "{family:?}");
    }
}

/// A rename is a `(-1, +1)` rewrite pair on one PK, in either row order: neither
/// a create nor a drop, so DDL paths keyed off these lists leave a renamed table
/// alone.
#[test]
fn family_pk_partition_reports_creates_and_drops_and_neither_for_a_pair() {
    let batch = table_tab_batch(&[
        (41, "c", 1),
        (42, "d", -1),
        (43, "old", -1),
        (43, "new", 1),
        (44, "new", 1),
        (44, "old", -1),
        (45, "c2", 1),
    ]);
    let p = family_pk_partition(SysFamily::Table, &batch);
    assert_eq!((p.creates, p.drops), (vec![41, 45], vec![42]));
}

#[test]
fn idx_tab_partition_carries_each_rows_column_list() {
    let unique = IndexProps { is_unique: true };
    let mut batch = idx_tab_batch(50, 20, &[1, 2], "a", unique, 1);
    for (index_id, owner_id, cols, weight) in [(51, 21, &[3][..], -1), (52, 22, &[1], -1), (52, 22, &[1], 1)] {
        batch.append_batch(&idx_tab_batch(
            index_id,
            owner_id,
            cols,
            "b",
            IndexProps::default(),
            weight,
        ));
    }
    // A row whose column list does not decode is in neither list.
    let mut undecodable = BatchBuilder::new(*SysFamily::Index.schema());
    push_sys_row(&mut undecodable, SysFamily::Index, [53, 0], 1, |_| 0);
    batch.append_batch(&undecodable.finish());

    let p = idx_tab_partition(&batch);
    let creates: Vec<_> = p
        .creates
        .iter()
        .map(|(o, c, props)| (*o, c.as_slice(), *props))
        .collect();
    let drops: Vec<_> = p.drops.iter().map(|(o, c)| (*o, c.as_slice())).collect();
    assert_eq!(creates, [(20, &[1, 2][..], unique)]);
    assert_eq!(drops, [(21, &[3][..])]);
}

/// Every COL_TAB word must fit the width it is stored at and decode to a value
/// `write_col_tab_row` could emit.
#[test]
fn read_col_tab_row_refuses_forged_words() {
    let read = |forged: Option<(usize, u64)>| {
        let mut bb = BatchBuilder::new(*SysFamily::Column.schema());
        push_sys_row(&mut bb, SysFamily::Column, [16, 0], 1, |pi| match forged {
            Some((slot, word)) if slot == pi => word,
            _ if pi == COLTAB_PAY_TYPE_CODE => gnitz_wire::TypeCode::I64.as_wire() as u64,
            _ => 0,
        });
        read_col_tab_row(&bb.finish(), 0)
    };
    read(None).unwrap();
    for (slot, word, names) in [
        (COLTAB_PAY_TYPE_CODE, 0x104, "type_code"),
        (COLTAB_PAY_TYPE_CODE, 99, "invalid column type 99"),
        (COLTAB_PAY_IS_NULLABLE, 2, "is_nullable"),
        (COLTAB_PAY_IS_HIDDEN, 2, "is_hidden"),
        (COLTAB_PAY_FK_COL_IDX, 1 << 32, "fk_col_idx"),
        (COLTAB_PAY_FK_COL_IDX, 3, "FK column 3 with no FK table"),
        (COLTAB_PAY_SCALE, 256, "scale"),
        // A scale on a type that carries none.
        (COLTAB_PAY_SCALE, 3, "invalid column type"),
    ] {
        let err = read(Some((slot, word))).unwrap_err();
        assert!(err.contains(names), "{names} = {word}: {err}");
    }
}

/// An IDX_TAB word that is not a packed list is refused as a column list.
#[test]
fn read_idx_tab_row_refuses_an_unpacked_column_list() {
    let mut bb = BatchBuilder::new(*SysFamily::Index.schema());
    push_sys_row(&mut bb, SysFamily::Index, [53, 0], 1, |pi| {
        if pi == IDXTAB_PAY_SOURCE_COLS {
            gnitz_wire::pack_pk_cols(&[1]) & !gnitz_wire::PK_LIST_PACKED_FLAG
        } else {
            0
        }
    });
    assert_eq!(
        read_idx_tab_row(&bb.finish(), 0).unwrap_err(),
        "column list word carries no packed-list flag"
    );
}
