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
    check(&SchemaTabRow { schema_id: 3, name: "public" });
    check(&TableTabRow {
        table_id: 16,
        schema_id: 3,
        name: "t",
        pk_col_idx: 1,
        flags: 0,
    });
    check(&ViewTabRow {
        view_id: 20,
        schema_id: 4,
        name: "v",
        pk_col_idx: 1,
        capacity_bytes: 0,
        delta_bytes: 1 << 20,
        owner_view_id: 21,
        pk_repeats: 1,
    });
    let col = crate::ColumnDef::new("c", crate::TypeCode::I64, false);
    check(&ColTabRow::of(16, 2, &col, None));
    check(&IdxTabRow {
        index_id: 100,
        owner_id: 16,
        source_col_idx: 1,
        name: "idx_t_b",
        is_unique: 1,
    });
    check(&SeqTabRow { seq_id: 1, next_val: 17 });
    check(&CircuitRow { view_id: 7, circuit: b"\x01" });
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
        (&score, Some(FkRef { table_id: 17, col: 3 })),
        (&score, None),
        (&flipped, Some(FkRef { table_id: 17, col: 0 })),
    ] {
        let row = ColTabRow::of(16, 2, col, fk);
        assert_eq!((row.owner_id, row.col_idx), (16, 2));
        assert_eq!(row.column(), Ok((col.clone(), fk)));
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
        (ColTabRow { type_code: 0x104, ..sound }, "type_code"),
        (ColTabRow { type_code: 99, ..sound }, "invalid column type 99"),
        (ColTabRow { is_nullable: 2, ..sound }, "is_nullable"),
        (ColTabRow { is_hidden: 2, ..sound }, "is_hidden"),
        (ColTabRow { fk_col_idx: 1 << 32, ..sound }, "fk_col_idx"),
        (ColTabRow { fk_col_idx: 3, ..sound }, "FK column 3 with no FK table"),
        (ColTabRow { scale: 256, ..sound }, "scale"),
        (ColTabRow { scale: 3, ..sound }, "invalid column type"),
    ] {
        let err = r.column().unwrap_err();
        assert!(err.contains(expect), "{expect}: {err}");
    }
}
