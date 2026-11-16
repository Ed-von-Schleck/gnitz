use super::*;

/// Every system table's key must be admissible for its own column list. The
/// pair is the thing both crates build from, so it is validated here rather
/// than trusted at each derivation site. Swept over `SysFamily::ALL`, so a newly
/// added family is covered without editing this test.
#[test]
fn system_table_keys_are_valid_for_their_columns() {
    for f in SysFamily::ALL.map(SysFamily::def) {
        let (name, cols, pk) = (f.name, f.cols, f.pk_cols);
        assert!(cols.len() <= crate::MAX_COLUMNS, "{name}: too many columns");
        crate::validate_pk_tuple(pk, cols.len(), crate::PK_LIST_MAX_COLS, |c| {
            let col = &cols[c as usize];
            (col.type_code, false)
        })
        .unwrap_or_else(|rule| panic!("{name}: {}", rule.for_role(crate::PkListRole::PrimaryKey)));
    }
}

/// A boolean column holds `0` or `1`.
#[test]
fn a_bool_word_is_zero_or_one() {
    assert_eq!(bool_word(0), Ok(false));
    assert_eq!(bool_word(1), Ok(true));
    assert!(bool_word(2).is_err());
}

/// The system-shape digest moves with every axis of a family's stored identity.
#[test]
fn fold_family_separates_every_stored_axis() {
    const ID: WireSysCol = WireSysCol { name: "id", type_code: TypeCode::U64 };
    const A: &[WireSysCol] = &[ID, WireSysCol { name: "v", type_code: TypeCode::U64 }];
    const RENAMED: &[WireSysCol] = &[ID, WireSysCol { name: "w", type_code: TypeCode::U64 }];
    const RETYPED: &[WireSysCol] = &[ID, WireSysCol { name: "v", type_code: TypeCode::I64 }];
    let fam = |id, name, cols, pk_cols| WireSysFamily { id, name, cols, pk_cols };
    let base = fold_family(0, &fam(1, "_t", A, &[0]));
    let others = [
        fam(2, "_t", A, &[0]),
        fam(1, "_u", A, &[0]),
        fam(1, "_t", RENAMED, &[0]),
        fam(1, "_t", RETYPED, &[0]),
        fam(1, "_t", A, &[0, 1]),
    ];
    for (i, other) in others.iter().enumerate() {
        assert_ne!(fold_family(0, other), base, "variant {i}");
    }
}

/// A column and its foreign key read back out of the row they were laid into,
/// with both booleans flipped across the cases so a transposed pair fails one.
#[test]
fn a_column_reads_back_out_of_its_row() {
    let score = crate::ColumnDef::new("score", crate::ColType::decimal(5), true);
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

/// Every row `column` accepts is the row `of` writes for what it read.
#[test]
fn a_column_row_column_accepts_is_the_row_of_writes() {
    let col = crate::ColumnDef::new("c", crate::TypeCode::I64, false);
    let base = ColTabRow {
        is_nullable: true,
        is_hidden: true,
        ..ColTabRow::of(16, 2, &col, None)
    };
    let fks = [
        (0, 0, 0),
        (0, 0, 1),
        (0, 3, 0),
        (17, 0, 0),
        (17, 3, 1),
        (17, 3, 2),
        (17, 1 << 32, 0),
    ];
    let mut accepted = 0;
    for type_code in 0..=256 {
        for scale in [0, 1, 2, 18, 38, 39, 255, 256] {
            for (fk_table_id, fk_col_idx, fk_on_delete) in fks {
                let row = ColTabRow {
                    type_code,
                    scale,
                    fk_table_id,
                    fk_col_idx,
                    fk_on_delete,
                    ..base.clone()
                };
                let Ok(c) = row.column() else { continue };
                assert_eq!(ColTabRow::of(row.owner_id, row.col_idx, &c.def, c.fk), row);
                accepted += 1;
            }
        }
    }
    assert!(accepted > 0);
}

/// A relation row's properties read back what its writer wrote, and refuse a
/// row no writer emits.
#[test]
fn a_relation_rows_parts_invert_its_written_words() {
    let pk = crate::PkColList::from_slice(&[1, 0]);
    let dists = (0..=2).map(|prefix_len| crate::TableDistribution::Keyed { prefix_len });
    for distribution in dists.chain([crate::TableDistribution::Replicated]) {
        for (stream, serial, compress) in [(false, false, false), (true, false, false), (false, false, true)] {
            let props = crate::TableProps { stream, serial, distribution, compress };
            assert_eq!(TableTabRow::of(17, 2, "t", pk, props).props(), Ok(props));
        }
    }
    let table = TableTabRow::of(17, 2, "t", pk, crate::TableProps::default());
    for (r, expect) in [
        (
            TableTabRow {
                stream: true,
                serial: true,
                ..table.clone()
            },
            "SERIAL",
        ),
        (
            TableTabRow {
                stream: true,
                compress: true,
                ..table.clone()
            },
            "compressed",
        ),
        (
            TableTabRow { dist_prefix: 3, ..table.clone() },
            "exceeds PK column count",
        ),
        (
            TableTabRow {
                dist_prefix: u8::MAX as u64 + 1,
                ..table.clone()
            },
            "dist_prefix",
        ),
        (
            TableTabRow {
                replicated: true,
                dist_prefix: 1,
                ..table.clone()
            },
            "mutually exclusive",
        ),
    ] {
        let err = r.props().unwrap_err();
        assert!(err.contains(expect), "{expect}: {err}");
    }

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
        pk_col_idx: pk,
        capacity_bytes,
        delta_bytes,
        owner_view_id: 0,
        pk_repeats: true,
        compress: true,
        definition: Default::default(),
    };
    let parts = ViewParts { props, owner: None, pk_repeats: true };
    assert_eq!(view.parts(), Ok(parts));
    let segment = ViewTabRow {
        name: "_seg19".into(),
        capacity_bytes: 0,
        delta_bytes: 0,
        owner_view_id: 19,
        compress: false,
        definition: Default::default(),
        ..view.clone()
    };
    assert_eq!(segment.parts().map(|p| p.owner), Ok(Some(19)));
    for (r, expect) in [
        (ViewTabRow { capacity_bytes: 1, ..view.clone() }, "delta feed"),
        (ViewTabRow { owner_view_id: 19, ..view.clone() }, "WITH option"),
        (
            ViewTabRow { definition: b"x"[..].into(), ..segment },
            "carrying a definition",
        ),
    ] {
        let err = r.parts().unwrap_err();
        assert!(err.contains(expect), "{expect}: {err}");
    }
}
