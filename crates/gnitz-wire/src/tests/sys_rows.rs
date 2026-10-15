use super::*;

/// A sink that records what a writer emitted, so the tests below read the row
/// back as a sequence rather than through either side's batch type.
#[derive(Default, PartialEq, Eq, Debug)]
struct Recorder {
    pk: Vec<u128>,
    weight: i64,
    vals: Vec<Val>,
    closed: bool,
}

#[derive(PartialEq, Eq, Debug)]
enum Val {
    U64(u64),
    Str(String),
    Bytes(Vec<u8>),
}

impl SysRowSink for Recorder {
    fn begin_row(&mut self, pk: &[u128], weight: i64) {
        self.pk = pk.to_vec();
        self.weight = weight;
    }
    fn put_u64(&mut self, v: u64) {
        self.vals.push(Val::U64(v));
    }
    fn put_string(&mut self, s: &str) {
        self.vals.push(Val::Str(s.to_string()));
    }
    fn put_bytes(&mut self, b: &[u8]) {
        self.vals.push(Val::Bytes(b.to_vec()));
    }
    fn end_row(&mut self) {
        self.closed = true;
    }
}

fn written<R: SysRow>(row: &R, weight: i64) -> Recorder {
    let mut rec = Recorder::default();
    row.write(&mut rec, weight);
    rec
}

/// A writer emits its family's key, then one value per payload column in the
/// column's own type, and closes the row.
#[test]
fn a_row_writes_one_value_of_its_columns_type_per_payload_column() {
    fn check<R: SysRow>(row: &R) {
        let rec = written(row, -1);
        let f = &crate::SYS_FAMILIES[crate::sys_family_index(R::FAMILY).unwrap()];
        assert_eq!(rec.pk.len(), f.pk_cols.len(), "{}: one key value per PK column", f.name);
        assert_eq!(rec.weight, -1);
        assert!(rec.closed, "{}: the writer closes the row", f.name);
        let payload = &f.cols[f.pk_cols.len()..];
        assert_eq!(
            rec.vals.len(),
            payload.len(),
            "{}: one value per payload column",
            f.name
        );
        for (val, col) in rec.vals.iter().zip(payload) {
            let fits = match val {
                Val::U64(_) => col.type_code == crate::TypeCode::U64,
                Val::Str(_) => col.type_code == crate::TypeCode::String,
                Val::Bytes(_) => col.type_code == crate::TypeCode::Blob,
            };
            assert!(fits, "{}.{}: {val:?}", f.name, col.name);
        }
    }
    check(&SchemaTabRow { schema_id: 3, name: "public".into() });
    check(&TableTabRow {
        table_id: 16,
        schema_id: 3,
        name: "t".into(),
        pk_col_idx: 1,
        flags: 0,
    });
    check(&ViewTabRow {
        view_id: 20,
        schema_id: 4,
        name: "v".into(),
        pk_col_idx: 1,
        capacity_bytes: 0,
        delta_bytes: 1 << 20,
        owner_view_id: 21,
        pk_repeats: 1,
        compress: 1,
    });
    let col = crate::ColumnDef::new("c", crate::TypeCode::I64, false);
    check(&ColTabRow::of(16, 2, &col, None));
    check(&IdxTabRow {
        index_id: 100,
        owner_id: 16,
        source_col_idx: 1,
        name: "idx_t_b".into(),
        is_unique: 1,
    });
    check(&SeqTabRow { seq_id: 1, next_val: 17 });
    check(&CircuitRow { view_id: 7, circuit: b"\x01"[..].into() });
}

/// The first key column is the family's leading id, in key order.
#[test]
fn a_column_row_is_keyed_by_its_owner_then_its_index() {
    let col = crate::ColumnDef::new("c", crate::TypeCode::I64, false);
    assert_eq!(written(&ColTabRow::of(16, 2, &col, None), 1).pk, [16, 2]);
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
        let row = ColTabRow::of(16, 2, col, fk);
        assert_eq!((row.owner_id, row.col_idx), (16, 2));
        assert_eq!(row.column(), Ok(CatalogColumn { def: col.clone(), fk }));
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
    };
    let parts = ViewParts { props, owner: None, pk_repeats: true };
    assert_eq!(view.parts(), Ok((pk, parts)));
    let segment = ViewTabRow {
        name: "_seg19".into(),
        capacity_bytes: 0,
        delta_bytes: 0,
        owner_view_id: 19,
        compress: 0,
        ..view.clone()
    };
    assert_eq!(segment.parts().map(|(_, p)| p.owner), Ok(Some(19)));
    for (r, expect) in [
        (ViewTabRow { capacity_bytes: 1, ..view.clone() }, "delta feed"),
        (ViewTabRow { pk_repeats: 2, ..view.clone() }, "pk_repeats"),
        (ViewTabRow { compress: 2, ..view.clone() }, "compress"),
        (ViewTabRow { pk_col_idx: 0, ..view.clone() }, "packed-list flag"),
        (ViewTabRow { owner_view_id: 19, ..view.clone() }, "WITH option"),
    ] {
        let err = r.parts().unwrap_err();
        assert!(err.contains(expect), "{expect}: {err}");
    }
}
