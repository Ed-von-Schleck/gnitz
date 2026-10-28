use super::*;

/// Every system table's key must be admissible for its own column list. The
/// pair is the thing both crates build from, so it is validated here rather
/// than trusted at each derivation site. Swept over `SYS_FAMILIES`, so a newly
/// added family is covered without editing this test.
#[test]
fn system_table_keys_are_valid_for_their_columns() {
    for f in SYS_FAMILIES {
        let (name, cols, pk) = (f.name, f.cols, f.pk_cols);
        assert!(cols.len() <= crate::MAX_COLUMNS, "{name}: too many columns");
        crate::validate_pk_tuple(pk, cols.len(), crate::PK_LIST_MAX_COLS, |c| {
            let col = &cols[c as usize];
            (col.type_code, false)
        })
        .unwrap_or_else(|rule| panic!("{name}: {}", rule.for_role(crate::PkListRole::PrimaryKey)));
    }
}

/// The `TABLE_TAB` flags word round-trips, pins its persisted bits as literals —
/// comparing against the constants the packer is written from would hold for any
/// value it gave them — and accepts exactly the single bits its layout defines. A
/// boolean column holds `0` or `1`.
#[test]
fn catalog_flag_words_roundtrip_pin_and_refuse_undefined_bits() {
    let dists = (0..=crate::PK_LIST_MAX_COLS as u8)
        .map(|prefix_len| TableDistribution::Keyed { prefix_len })
        .chain([TableDistribution::Replicated]);
    for distribution in dists {
        for stream in [false, true] {
            for serial in [false, true] {
                for compress in [false, true] {
                    let p = TableProps { stream, serial, distribution, compress };
                    assert_eq!(TableProps::from_flags(p.pack()), Ok(p));
                }
            }
        }
    }

    let t = |stream, serial, distribution| {
        TableProps {
            stream,
            serial,
            distribution,
            compress: false,
        }
        .pack()
    };
    assert_eq!(TableProps::default().pack(), 0);
    assert_eq!(t(false, false, TableDistribution::Replicated), 0b001);
    assert_eq!(t(true, false, TableDistribution::default()), 0b010);
    assert_eq!(t(false, true, TableDistribution::default()), 0b100);
    let compressed = TableProps { compress: true, ..TableProps::default() };
    assert_eq!(compressed.pack(), 0b1000);
    assert_eq!(t(false, false, TableDistribution::Keyed { prefix_len: 2 }), 2 << 8);

    for bit in 0..64u32 {
        let w = 1u64 << bit;
        assert_eq!(
            TableProps::from_flags(w).is_ok(),
            bit <= 3 || (8..16).contains(&bit),
            "table bit {bit}"
        );
    }
    // The one state the packing can hold and `TableProps` cannot.
    assert!(TableProps::from_flags(0b01 | 2 << 8)
        .unwrap_err()
        .contains("mutually exclusive"));

    assert_eq!(bool_word(0), Ok(false));
    assert_eq!(bool_word(1), Ok(true));
    assert!(bool_word(2).is_err());
}

/// `validate` refuses what the flags word can hold but the table cannot: a
/// distribution prefix longer than the PK, a compressed stream, and a SERIAL
/// table that is a stream or whose PK is not its one generated column.
#[test]
fn table_props_validate_refuses_what_the_pk_cannot_carry() {
    let keyed = |prefix_len| TableProps {
        distribution: TableDistribution::Keyed { prefix_len },
        ..TableProps::default()
    };
    assert_eq!(keyed(3).validate(3), Ok(()));
    assert!(keyed(4).validate(3).unwrap_err().contains("exceeds PK column count"));
    let serial = TableProps { serial: true, ..TableProps::default() };
    assert_eq!(serial.validate(1), Ok(()));
    assert!(serial.validate(2).unwrap_err().contains("one SERIAL column"));
    let stream = TableProps { stream: true, ..serial };
    assert!(stream.validate(1).unwrap_err().contains("a stream cannot be SERIAL"));
    let compressed = TableProps { compress: true, ..stream };
    assert!(compressed
        .validate(1)
        .unwrap_err()
        .contains("a stream cannot be compressed"));
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

#[test]
fn a_user_identifier_is_nonempty_charset_and_not_underscore_led() {
    for name in ["orders", "Orders123", "my_table", "a", "A1_b2", "1a", "99_problems"] {
        assert!(validate_user_identifier(name).is_ok(), "rejected valid: {name}");
    }
    for name in [
        "_private",
        "_",
        "__init__",
        "",
        "has space",
        "has-dash",
        "has.dot",
        "has@",
        "table$",
    ] {
        assert!(validate_user_identifier(name).is_err(), "accepted invalid: {name}");
    }
    assert_eq!(canonical_identifier("MyTab_1"), Ok("mytab_1".into()));
    assert!(canonical_identifier("_x").is_err());
}

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
