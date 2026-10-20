use super::*;

/// A column and its foreign key read back out of the row they were laid into,
/// with both booleans flipped across the cases so a transposed pair fails one.
#[test]
fn a_column_reads_back_out_of_its_row() {
    let score = crate::ColumnDef::typed("score", crate::ColType::decimal(5), true);
    let flipped = crate::ColumnDef {
        is_nullable: false,
        ..score.clone().hidden()
    };
    for (col, fk) in [
        (
            &score,
            Some(FkRef {
                table_id: 17,
                col: 3,
                on_delete: FkAction::Restrict,
            }),
        ),
        (&score, None),
        (
            &flipped,
            Some(FkRef {
                table_id: 17,
                col: 0,
                on_delete: FkAction::Cascade,
            }),
        ),
    ] {
        assert_eq!(
            ColTabRow::of(16, 2, col, fk).column(),
            Ok(CatalogColumn { def: col.clone(), fk })
        );
    }
}

/// Every COL_TAB word must fit the width it is stored at and decode to a value
/// `ColTabRow::of` could emit.
#[test]
fn a_column_row_refuses_forged_words() {
    let col = crate::ColumnDef::new("c", crate::TypeCode::I64, false);
    let sound = ColTabRow::of(16, 0, &col, None);
    assert!(sound.column().is_ok());
    for (r, expect) in [
        (ColTabRow { type_code: 0x104, ..sound.clone() }, "type_code"),
        (ColTabRow { type_code: 99, ..sound.clone() }, "invalid column type 99"),
        (ColTabRow { is_nullable: 2, ..sound.clone() }, "is_nullable"),
        (ColTabRow { is_hidden: 2, ..sound.clone() }, "is_hidden"),
        (ColTabRow { fk_col_idx: 1 << 32, ..sound.clone() }, "fk_col_idx"),
        (
            ColTabRow { fk_col_idx: 3, ..sound.clone() },
            "FK column 3 with no FK table",
        ),
        (ColTabRow { fk_on_delete: 2, ..sound.clone() }, "fk_on_delete"),
        (
            ColTabRow { fk_on_delete: 1, ..sound.clone() },
            "an FK action with no FK table",
        ),
        (ColTabRow { scale: 256, ..sound.clone() }, "scale"),
        (ColTabRow { scale: 3, ..sound.clone() }, "invalid column type"),
    ] {
        let err = r.column().unwrap_err();
        assert!(err.contains(expect), "{expect}: {err}");
    }
}

/// A relation row's `parts` read back the key and properties its writer packed,
/// and refuse a word no writer emits.
#[test]
fn a_relation_rows_parts_invert_its_packed_words() {
    let pk = crate::PkColList::from_slice(&[1, 0]);
    let props = crate::TableProps {
        serial: false,
        distribution: crate::TableDistribution::Keyed { prefix_len: 1 },
        ..Default::default()
    };
    let table = TableTabRow {
        table_id: 17,
        schema_id: 2,
        name: "t".into(),
        pk_col_idx: pk.pack(),
        flags: props.pack(),
    };
    assert_eq!(table.parts(), Ok((pk, props)));
    let stream_serial = crate::TableProps {
        stream: true,
        serial: true,
        ..Default::default()
    };
    let err = TableTabRow {
        flags: stream_serial.pack(),
        ..table.clone()
    }
    .parts()
    .unwrap_err();
    assert!(err.contains("SERIAL"), "{err}");

    let props = crate::ViewProps {
        budget: crate::ViewBudget::Fed {
            delta_bytes: std::num::NonZeroU64::new(64).unwrap(),
        },
        compress: true,
    };
    let (capacity_bytes, delta_bytes) = props.budget.row_words();
    let view = ViewTabRow {
        view_id: 18,
        schema_id: 2,
        name: "v".into(),
        pk_col_idx: pk.pack(),
        capacity_bytes,
        delta_bytes,
        owner_view_id: 0,
        pk_repeats: 1,
        compress: 1,
        definition: Default::default(),
    };
    let parts = ViewParts { props, owner: None, pk_repeats: true };
    assert_eq!(view.parts(), Ok((pk, parts)));
    let segment = ViewTabRow {
        name: "_seg19".into(),
        capacity_bytes: 0,
        delta_bytes: 0,
        owner_view_id: 19,
        compress: 0,
        definition: Default::default(),
        ..view.clone()
    };
    assert_eq!(segment.parts().map(|(_, p)| p.owner), Ok(Some(19)));
    for (r, expect) in [
        (ViewTabRow { capacity_bytes: 1, ..view.clone() }, "delta feed"),
        (ViewTabRow { pk_repeats: 2, ..view.clone() }, "pk_repeats"),
        (ViewTabRow { compress: 2, ..view.clone() }, "compress"),
        (ViewTabRow { pk_col_idx: 0, ..view.clone() }, "packed-list flag"),
        (ViewTabRow { owner_view_id: 19, ..view.clone() }, "WITH option"),
        (
            ViewTabRow { definition: b"x"[..].into(), ..segment },
            "carrying a definition",
        ),
    ] {
        let err = r.parts().unwrap_err();
        assert!(err.contains(expect), "{expect}: {err}");
    }

    let index = IdxTabRow {
        index_id: 100,
        owner_id: 16,
        source_col_idx: pk.pack(),
        name: "idx".into(),
        is_unique: 1,
    };
    assert_eq!(index.parts(), Ok((16, pk, true)));
    for (r, expect) in [
        (IdxTabRow { is_unique: 2, ..index.clone() }, "is_unique"),
        (
            IdxTabRow {
                source_col_idx: pk.pack() & !crate::PK_LIST_PACKED_FLAG,
                ..index
            },
            "column list word carries no packed-list flag",
        ),
    ] {
        let err = r.parts().unwrap_err();
        assert!(err.contains(expect), "{expect}: {err}");
    }
}
