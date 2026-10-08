use super::*;
use crate::test_support::{idx_tab_batch, push_sys_row, push_view_tab_row, table_tab_batch};
use gnitz_wire::sys_rows::IdxTabSlot;
use gnitz_zset::repr::BatchBuilder;

/// The pair-keyed family's at-rest PK is `owner_BE ‖ member_BE`, so one owner's
/// rows form one contiguous prefix band.
#[test]
fn the_pair_keyed_familys_at_rest_pk_leads_with_its_owner() {
    let family = SysFamily::Column;
    let mut bb = BatchBuilder::new(family.schema());
    push_sys_row(&mut bb, family, [0x1122, 0xAABB], 1, |_| 0);
    let batch = bb.finish();
    assert_eq!(
        batch.get_pk_bytes(0),
        [0, 0, 0, 0, 0, 0, 0x11, 0x22, 0, 0, 0, 0, 0, 0, 0xAA, 0xBB],
    );
    assert_eq!(family.leading_id(batch.get_pk(0)), 0x1122);
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
    let mut batch = idx_tab_batch(50, 20, &[1, 2], "a", true, 1);
    for (index_id, owner_id, cols, weight) in [(51, 21, &[3][..], -1), (52, 22, &[1], -1), (52, 22, &[1], 1)] {
        batch.append_batch(&idx_tab_batch(index_id, owner_id, cols, "b", false, weight));
    }
    // A row whose column list does not decode is in neither list.
    let mut undecodable = BatchBuilder::new(SysFamily::Index.schema());
    push_sys_row(&mut undecodable, SysFamily::Index, [53, 0], 1, |_| 0);
    batch.append_batch(&undecodable.finish());

    let p = idx_tab_partition(&batch);
    let creates: Vec<_> = p
        .creates
        .iter()
        .map(|(o, c, props)| (*o, c.as_slice(), *props))
        .collect();
    let drops: Vec<_> = p.drops.iter().map(|(o, c)| (*o, c.as_slice())).collect();
    assert_eq!(creates, [(20, &[1, 2][..], true)]);
    assert_eq!(drops, [(21, &[3][..])]);
}

/// An IDX_TAB word that is not a packed list is refused as a column list.
#[test]
fn an_index_row_refuses_an_unpacked_column_list() {
    let mut bb = BatchBuilder::new(SysFamily::Index.schema());
    push_sys_row(&mut bb, SysFamily::Index, [53, 0], 1, |pi| {
        if pi == IdxTabSlot::source_col_idx as usize {
            PkColList::from_slice(&[1]).pack() & !gnitz_wire::PK_LIST_PACKED_FLAG
        } else {
            0
        }
    });
    assert_eq!(
        IdxTabRow::read(&bb.finish(), 0).and_then(|r| r.parts()).unwrap_err(),
        "column list word carries no packed-list flag"
    );
}

/// A chain segment is planner-minted, so its VIEW_TAB row carries no WITH option;
/// a user view's may.
#[test]
fn read_rel_row_refuses_a_segment_with_an_option() {
    let row = |name: &str, owner_view_id: u64| {
        let mut bb = BatchBuilder::new(SysFamily::View.schema());
        push_view_tab_row(&mut bb, 1, 30, name, 4 << 20, 0, owner_view_id);
        bb.finish()
    };
    let err = read_rel_row(SysFamily::View, &row("_seg", 29), 0)
        .map(drop)
        .unwrap_err();
    assert_eq!(
        err,
        "catalog invariant violated: internal segment '_seg' (id=30) carries a WITH option"
    );
    let user = row("v", 0);
    let rel = read_rel_row(SysFamily::View, &user, 0).map_err(drop).unwrap();
    assert!(rel.kind.is_bounded());
    assert_eq!(rel.to_string(), "view 'v' (id=30)");
}

/// A stream's PK repeats; a base table's does not.
#[test]
fn read_rel_row_reports_a_streams_pk_as_repeating() {
    let row = |stream: bool| {
        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        let row = TableTabRow {
            table_id: 40,
            schema_id: PUBLIC_SCHEMA_ID,
            name: "events",
            pk_col_idx: PkColList::from_slice(&[0]).pack(),
            flags: gnitz_wire::TableProps { stream, ..Default::default() }.pack(),
        };
        row.write(&mut bb, 1);
        bb.finish()
    };
    for stream in [true, false] {
        let batch = row(stream);
        let rel = read_rel_row(SysFamily::Table, &batch, 0).map_err(drop).unwrap();
        assert_eq!(rel.kind == RelationKind::Stream, stream);
        assert_eq!(rel.pk_repeats(), stream);
        assert!(!rel.serial());
    }
}

// ── The system-row readers invert the writers ────────────────────────────

fn sys_batch(family: SysFamily, write: impl FnOnce(&mut BatchBuilder)) -> Batch {
    let mut bb = BatchBuilder::new(family.schema());
    write(&mut bb);
    bb.finish()
}

#[test]
fn every_system_family_reads_back_the_row_it_wrote() {
    use gnitz_wire::sys_rows::*;

    let r = SchemaTabRow {
        schema_id: 7,
        name: "a_long_schema_name_past_twelve",
    };
    assert_eq!(
        SchemaTabRow::read(&sys_batch(SysFamily::Schema, |bb| r.write(bb, 1)), 0),
        Ok(r)
    );

    let r = TableTabRow {
        table_id: 9,
        schema_id: 2,
        name: "t",
        pk_col_idx: 0x31,
        flags: 5,
    };
    assert_eq!(
        TableTabRow::read(&sys_batch(SysFamily::Table, |bb| r.write(bb, 1)), 0),
        Ok(r)
    );

    let r = ViewTabRow {
        view_id: 11,
        schema_id: 2,
        name: "v",
        pk_col_idx: 1,
        capacity_bytes: 4 << 20,
        delta_bytes: 32 << 20,
        owner_view_id: 10,
        pk_repeats: 1,
    };
    assert_eq!(
        ViewTabRow::read(&sys_batch(SysFamily::View, |bb| r.write(bb, 1)), 0),
        Ok(r)
    );

    // Both key words distinct.
    let r = ColTabRow {
        owner_id: 16,
        col_idx: 3,
        name: "c",
        type_code: 8,
        is_nullable: 1,
        fk_table_id: 12,
        fk_col_idx: 2,
        fk_on_delete: 1,
        is_hidden: 1,
        scale: 4,
    };
    assert_eq!(
        ColTabRow::read(&sys_batch(SysFamily::Column, |bb| r.write(bb, 1)), 0),
        Ok(r)
    );

    let r = IdxTabRow {
        index_id: 70,
        owner_id: 16,
        source_col_idx: 0x21,
        name: "i",
        is_unique: 1,
    };
    assert_eq!(
        IdxTabRow::read(&sys_batch(SysFamily::Index, |bb| r.write(bb, 1)), 0),
        Ok(r)
    );

    let r = SeqTabRow { seq_id: 3, next_val: u64::MAX };
    assert_eq!(
        SeqTabRow::read(&sys_batch(SysFamily::Sequence, |bb| r.write(bb, 1)), 0),
        Ok(r)
    );

    // A circuit past the inline 12 bytes, and an empty one.
    for circuit in [&b"a circuit cell past twelve bytes"[..], b""] {
        let r = CircuitRow { view_id: 0x1122, circuit };
        assert_eq!(
            CircuitRow::read(&sys_batch(SysFamily::Circuit, |bb| r.write(bb, 1)), 0),
            Ok(r)
        );
    }
}

#[test]
fn a_system_name_that_is_not_utf8_does_not_read() {
    let b = sys_batch(SysFamily::Schema, |bb| {
        bb.begin_row(1, 1);
        bb.put_blob(b"\xff");
        bb.end_row();
    });
    assert_eq!(
        gnitz_wire::sys_rows::SchemaTabRow::read(&b, 0),
        Err("name is not UTF-8".to_string())
    );
}
