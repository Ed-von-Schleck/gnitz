use super::*;
use crate::{
    CIRCTAB_PAY_CIRCUIT, CIRCUIT_TAB, COLTAB_PAY_FK_COL_IDX, COLTAB_PAY_FK_TABLE_ID, COLTAB_PAY_IS_HIDDEN,
    COLTAB_PAY_IS_NULLABLE, COLTAB_PAY_NAME, COLTAB_PAY_SCALE, COLTAB_PAY_TYPE_CODE, COL_TAB, IDXTAB_PAY_IS_UNIQUE,
    IDXTAB_PAY_NAME, IDXTAB_PAY_OWNER_ID, IDXTAB_PAY_SOURCE_COLS, IDX_TAB, RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID,
    SCHEMATAB_PAY_NAME, SCHEMA_TAB, TABLE_TAB, TABTAB_PAY_FLAGS, TABTAB_PAY_PK_COL_IDX, VIEWTAB_PAY_CAPACITY,
    VIEWTAB_PAY_DELTA, VIEWTAB_PAY_OWNER_VIEW_ID, VIEWTAB_PAY_PK_COL_IDX, VIEWTAB_PAY_PK_REPEATS, VIEW_TAB,
};

/// A sink that records what a writer emitted, so the tests below read the
/// row back as a sequence rather than through either side's batch type.
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

impl Recorder {
    /// The recorded payload slots, after what every writer owes its consumer:
    /// one key value per PK column and one value per payload column of
    /// `family`, and a closed row. An under-filled row desyncs a columnar
    /// builder and only surfaces later as an un-attributed length error, so
    /// every family below reads its slots through here.
    fn row(&self, family: u64) -> &[Val] {
        let f = &crate::SYS_FAMILIES[crate::sys_family_index(family).unwrap()];
        assert_eq!(self.pk.len(), f.pk_cols.len(), "one key value per PK column");
        assert_eq!(
            self.vals.len(),
            f.cols.len() - f.pk_cols.len(),
            "one value per payload column"
        );
        assert!(self.closed, "the writer must close the row");
        &self.vals
    }
}

/// Every `ColTabRow` field landed in its named payload slot. The expectation
/// is read off the struct, so the check cannot itself transpose a pair.
fn assert_col_tab_slots(r: &ColTabRow, weight: i64) {
    let mut rec = Recorder::default();
    r.write(&mut rec, weight);
    assert_eq!(rec.pk, [r.owner_id as u128, r.col_idx as u128]);
    assert_eq!(rec.weight, weight);
    let v = rec.row(COL_TAB);
    assert_eq!(v[COLTAB_PAY_NAME], Val::Str(r.col.name.clone()));
    assert_eq!(v[COLTAB_PAY_TYPE_CODE], Val::U64(r.col.ty.tc.as_wire() as u64));
    assert_eq!(v[COLTAB_PAY_IS_NULLABLE], Val::U64(r.col.is_nullable as u64));
    let (fk_table_id, fk_col_idx) = r.fk.map_or((0, 0), |fk| (fk.table_id, fk.col as u64));
    assert_eq!(v[COLTAB_PAY_FK_TABLE_ID], Val::U64(fk_table_id));
    assert_eq!(v[COLTAB_PAY_FK_COL_IDX], Val::U64(fk_col_idx));
    assert_eq!(v[COLTAB_PAY_IS_HIDDEN], Val::U64(r.col.is_hidden as u64));
    assert_eq!(v[COLTAB_PAY_SCALE], Val::U64(r.col.ty.scale as u64));
}

/// Each value must land in the payload slot the readers look for it in.
#[test]
fn values_land_in_their_named_payload_slots() {
    // Distinct values per u64 field.
    let score = crate::ColumnDef::typed("score", crate::ColType::decimal(5), true);
    let witness = ColTabRow {
        owner_id: 16,
        col_idx: 2,
        col: &score,
        fk: Some(FkRef { table_id: 17, col: 3 }),
    };
    assert_col_tab_slots(&witness, -1);
    assert_col_tab_slots(&ColTabRow { fk: None, ..witness }, 1);
    // Both booleans flipped, so a transposed pair fails one of the two rows.
    let flipped = crate::ColumnDef {
        is_nullable: false,
        ..score.clone().hidden()
    };
    assert_col_tab_slots(&ColTabRow { col: &flipped, ..witness }, 1);

    let idx_cols = crate::PkColList::from_slice(&[2, 1]);
    let mut r = Recorder::default();
    IdxTabRow {
        index_id: 100,
        owner_id: 16,
        cols: idx_cols,
        name: "idx_t_b",
        is_unique: true,
    }
    .write(&mut r, 1);
    assert_eq!(r.pk, [100]);
    let v = r.row(IDX_TAB);
    assert_eq!(v[IDXTAB_PAY_OWNER_ID], Val::U64(16));
    assert_eq!(v[IDXTAB_PAY_SOURCE_COLS], Val::U64(idx_cols.pack()));
    assert_eq!(v[IDXTAB_PAY_NAME], Val::Str("idx_t_b".into()));
    assert_eq!(v[IDXTAB_PAY_IS_UNIQUE], Val::U64(1));

    // Distinct values per field, so a transposed pair in the writer body
    // fails here rather than round-tripping unnoticed.
    let pk = crate::PkColList::from_slice(&[1, 0]);
    let props = crate::TableProps { stream: true, ..Default::default() };
    let mut r = Recorder::default();
    TableTabRow {
        table_id: 16,
        schema_id: 3,
        name: "t",
        pk,
        props,
    }
    .write(&mut r, 1);
    assert_eq!(r.pk, [16]);
    let v = r.row(TABLE_TAB);
    assert_eq!(v[RELTAB_PAY_SCHEMA_ID], Val::U64(3));
    assert_eq!(v[RELTAB_PAY_NAME], Val::Str("t".into()));
    assert_eq!(v[TABTAB_PAY_PK_COL_IDX], Val::U64(pk.pack()));
    assert_eq!(v[TABTAB_PAY_FLAGS], Val::U64(props.pack()));

    let mut r = Recorder::default();
    ViewTabRow {
        view_id: 20,
        schema_id: 4,
        name: "v",
        pk,
        props: crate::ViewProps::Fed {
            delta_bytes: std::num::NonZeroU64::new(1 << 20).unwrap(),
        },
        owner_view_id: 21,
        pk_repeats: true,
    }
    .write(&mut r, 1);
    assert_eq!(r.pk, [20]);
    let v = r.row(VIEW_TAB);
    assert_eq!(v[RELTAB_PAY_SCHEMA_ID], Val::U64(4));
    assert_eq!(v[RELTAB_PAY_NAME], Val::Str("v".into()));
    assert_eq!(v[VIEWTAB_PAY_PK_COL_IDX], Val::U64(pk.pack()));
    assert_eq!(v[VIEWTAB_PAY_CAPACITY], Val::U64(0));
    assert_eq!(v[VIEWTAB_PAY_DELTA], Val::U64(1 << 20));
    assert_eq!(v[VIEWTAB_PAY_OWNER_VIEW_ID], Val::U64(21));
    assert_eq!(v[VIEWTAB_PAY_PK_REPEATS], Val::U64(1));

    let mut r = Recorder::default();
    SchemaTabRow { schema_id: 3, name: "public" }.write(&mut r, 1);
    assert_eq!(r.pk, [3]);
    assert_eq!(r.row(SCHEMA_TAB)[SCHEMATAB_PAY_NAME], Val::Str("public".into()));

    // The circuit family: one row per view, its whole circuit in one cell.
    let mut circuit = crate::Circuit::default();
    let scan = circuit.input_delta(31, crate::ReadBound::None);
    circuit.sink(scan);
    let mut r = Recorder::default();
    CircuitRow { view_id: 7, circuit: &circuit }.write(&mut r, 1);
    assert_eq!((r.pk.as_slice(), r.weight), (&[7][..], 1));
    assert_eq!(r.row(CIRCUIT_TAB)[CIRCTAB_PAY_CIRCUIT], Val::Bytes(circuit.encode()));
}
