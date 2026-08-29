use super::*;
use crate::{
    CIRCEDGES_PAY_DST_NODE, CIRCEDGES_PAY_DST_PORT, CIRCEDGES_PAY_SRC_NODE, CIRCNCOL_PAY_KIND, CIRCNCOL_PAY_NODE_ID,
    CIRCNCOL_PAY_POSITION, CIRCNCOL_PAY_VALUE1, CIRCNCOL_PAY_VALUE2, CIRCNODES_PAY_EXPR_PROGRAM, CIRCNODES_PAY_NODE_ID,
    CIRCNODES_PAY_OPCODE, CIRCNODES_PAY_SOURCE_TABLE, CIRCUIT_EDGES_COLS, CIRCUIT_NODES_COLS,
    CIRCUIT_NODE_COLUMNS_COLS, COLTAB_PAY_COL_IDX, COLTAB_PAY_FK_COL_IDX, COLTAB_PAY_FK_TABLE_ID, COLTAB_PAY_IS_HIDDEN,
    COLTAB_PAY_IS_NULLABLE, COLTAB_PAY_IS_SERIAL, COLTAB_PAY_NAME, COLTAB_PAY_OWNER_ID, COLTAB_PAY_OWNER_KIND,
    COLTAB_PAY_TYPE_CODE, COL_TAB_COLS, IDXTAB_PAY_IS_UNIQUE, IDXTAB_PAY_NAME, IDXTAB_PAY_OWNER_ID,
    IDXTAB_PAY_SOURCE_COLS, IDX_TAB_COLS, SCHEMA_TAB_COLS, TABLE_TAB_COLS, TABTAB_PAY_FLAGS, TABTAB_PAY_NAME,
    TABTAB_PAY_PK_COL_IDX, TABTAB_PAY_SCHEMA_ID, VIEWTAB_PAY_CAPACITY, VIEWTAB_PAY_NAME, VIEWTAB_PAY_PK_COL_IDX,
    VIEWTAB_PAY_SCHEMA_ID, VIEWTAB_PAY_SQL, VIEW_TAB_COLS,
};

/// A sink that records what a writer emitted, so the tests below read the
/// row back as a sequence rather than through either side's batch type.
#[derive(Default, PartialEq, Eq, Debug)]
struct Recorder {
    pk: u128,
    weight: i64,
    vals: Vec<Val>,
    closed: bool,
}

#[derive(PartialEq, Eq, Debug)]
enum Val {
    U64(u64),
    Str(String),
    Bytes(Vec<u8>),
    Null,
}

impl SysRowSink for Recorder {
    fn begin_row(&mut self, pk: u128, weight: i64) {
        self.pk = pk;
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
    fn put_null(&mut self) {
        self.vals.push(Val::Null);
    }
    fn end_row(&mut self) {
        self.closed = true;
    }
}

/// Every writer must emit exactly one value per payload column and close the
/// row — an under-filled row desyncs a columnar builder and only surfaces
/// later as an un-attributed length error.
#[test]
fn every_writer_fills_every_payload_column() {
    let mut r = Recorder::default();
    write_col_tab_row(
        &mut r,
        &ColTabRow {
            owner_id: 16,
            owner_kind: 0,
            col_idx: 2,
            name: "c",
            type_code: 4,
            is_nullable: true,
            fk_table_id: 17,
            fk_col_idx: 1,
            is_serial: false,
            is_hidden: true,
        },
        1,
    )
    .unwrap();
    assert_eq!(r.vals.len(), COL_TAB_COLS.len() - 1);
    assert!(r.closed);

    let mut r = Recorder::default();
    write_table_tab_row(
        &mut r,
        &TableTabRow {
            table_id: 16,
            schema_id: 3,
            name: "t",
            pk_col_idx: 0,
            flags: 0,
        },
        1,
    );
    assert_eq!(r.vals.len(), TABLE_TAB_COLS.len() - 1);
    assert!(r.closed);

    let mut r = Recorder::default();
    write_view_tab_row(
        &mut r,
        &ViewTabRow {
            view_id: 20,
            schema_id: 3,
            name: "v",
            sql_definition: "SELECT 1",
            pk_col_idx: 0,
            capacity_bytes: 0,
            delta_bytes: 0,
        },
        1,
    );
    assert_eq!(r.vals.len(), VIEW_TAB_COLS.len() - 1);
    assert!(r.closed);

    let mut r = Recorder::default();
    write_idx_tab_row(
        &mut r,
        &IdxTabRow {
            index_id: 100,
            owner_id: 16,
            source_col_idx: 1,
            name: "idx",
            is_unique: 1,
        },
        1,
    );
    assert_eq!(r.vals.len(), IDX_TAB_COLS.len() - 1);
    assert!(r.closed);
}

/// Every `ColTabRow` field landed in its named payload slot. The expectation
/// is read off the struct, so the check cannot itself transpose a pair.
fn assert_col_tab_slots(r: &ColTabRow, weight: i64) {
    let mut rec = Recorder::default();
    write_col_tab_row(&mut rec, r, weight).unwrap();
    assert_eq!(rec.pk, pack_col_id(r.owner_id, r.col_idx).unwrap() as u128);
    assert_eq!(rec.weight, weight);
    assert_eq!(rec.vals[COLTAB_PAY_OWNER_ID], Val::U64(r.owner_id));
    assert_eq!(rec.vals[COLTAB_PAY_OWNER_KIND], Val::U64(r.owner_kind));
    assert_eq!(rec.vals[COLTAB_PAY_COL_IDX], Val::U64(r.col_idx));
    assert_eq!(rec.vals[COLTAB_PAY_NAME], Val::Str(r.name.into()));
    assert_eq!(rec.vals[COLTAB_PAY_TYPE_CODE], Val::U64(r.type_code));
    assert_eq!(rec.vals[COLTAB_PAY_IS_NULLABLE], Val::U64(r.is_nullable as u64));
    assert_eq!(rec.vals[COLTAB_PAY_FK_TABLE_ID], Val::U64(r.fk_table_id));
    assert_eq!(rec.vals[COLTAB_PAY_FK_COL_IDX], Val::U64(r.fk_col_idx));
    assert_eq!(rec.vals[COLTAB_PAY_IS_SERIAL], Val::U64(r.is_serial as u64));
    assert_eq!(rec.vals[COLTAB_PAY_IS_HIDDEN], Val::U64(r.is_hidden as u64));
}

/// Each value must land in the payload slot the readers look for it in.
#[test]
fn values_land_in_their_named_payload_slots() {
    // Distinct values per u64 field, and `owner_kind` a sentinel no boolean
    // can take — it reaches the writer as a plain u64.
    let witness = ColTabRow {
        owner_id: 16,
        owner_kind: 7,
        col_idx: 2,
        name: "score",
        type_code: 10,
        is_nullable: true,
        fk_table_id: 17,
        fk_col_idx: 3,
        is_serial: true,
        is_hidden: false,
    };
    assert_col_tab_slots(&witness, -1);
    // A second row for the booleans alone: one row cannot separate three
    // fields drawn from {0,1}. These codes are pairwise distinct —
    // is_nullable (1,0), is_serial (1,1), is_hidden (0,1) — where the
    // tempting complementary row would leave the first and last sharing (1,0).
    assert_col_tab_slots(
        &ColTabRow {
            is_nullable: false,
            is_hidden: true,
            ..witness
        },
        1,
    );

    let mut r = Recorder::default();
    write_idx_tab_row(
        &mut r,
        &IdxTabRow {
            index_id: 100,
            owner_id: 16,
            source_col_idx: 9,
            name: "idx_t_b",
            is_unique: 1,
        },
        1,
    );
    assert_eq!(r.pk, 100);
    assert_eq!(r.vals[IDXTAB_PAY_OWNER_ID], Val::U64(16));
    assert_eq!(r.vals[IDXTAB_PAY_SOURCE_COLS], Val::U64(9));
    assert_eq!(r.vals[IDXTAB_PAY_NAME], Val::Str("idx_t_b".into()));
    assert_eq!(r.vals[IDXTAB_PAY_IS_UNIQUE], Val::U64(1));

    // Distinct values per field, so a transposed pair in the writer body
    // fails here rather than round-tripping unnoticed.
    let mut r = Recorder::default();
    write_table_tab_row(
        &mut r,
        &TableTabRow {
            table_id: 16,
            schema_id: 3,
            name: "t",
            pk_col_idx: 5,
            flags: 9,
        },
        1,
    );
    assert_eq!(r.pk, 16);
    assert_eq!(r.vals[TABTAB_PAY_SCHEMA_ID], Val::U64(3));
    assert_eq!(r.vals[TABTAB_PAY_NAME], Val::Str("t".into()));
    assert_eq!(r.vals[TABTAB_PAY_PK_COL_IDX], Val::U64(5));
    assert_eq!(r.vals[TABTAB_PAY_FLAGS], Val::U64(9));

    let mut r = Recorder::default();
    write_view_tab_row(
        &mut r,
        &ViewTabRow {
            view_id: 20,
            schema_id: 4,
            name: "v",
            sql_definition: "SELECT 1",
            pk_col_idx: 6,
            capacity_bytes: 4096,
            delta_bytes: 1 << 20,
        },
        1,
    );
    assert_eq!(r.pk, 20);
    assert_eq!(r.vals[VIEWTAB_PAY_SCHEMA_ID], Val::U64(4));
    assert_eq!(r.vals[VIEWTAB_PAY_NAME], Val::Str("v".into()));
    assert_eq!(r.vals[VIEWTAB_PAY_SQL], Val::Str("SELECT 1".into()));
    assert_eq!(r.vals[VIEWTAB_PAY_PK_COL_IDX], Val::U64(6));
    assert_eq!(r.vals[VIEWTAB_PAY_CAPACITY], Val::U64(4096));

    let mut r = Recorder::default();
    write_schema_tab_row(
        &mut r,
        &SchemaTabRow {
            schema_id: 3,
            name: "public",
        },
        1,
    );
    assert_eq!(r.pk, 3);
    assert_eq!(r.vals.len(), SCHEMA_TAB_COLS.len() - 1);
    assert_eq!(r.vals[0], Val::Str("public".into()));
    assert!(r.closed);

    // The circuit families. `view_id` must occupy the LOW u128 half of the
    // compound key: the PK region OPK-encodes each column independently, low
    // bytes first, so that is what puts view_id in the leading at-rest bytes
    // the engine's per-view prefix seek reads.
    let mut r = Recorder::default();
    write_circuit_node_row(
        &mut r,
        &CircuitNodeRow {
            view_id: 7,
            node_id: 5,
            opcode: 9,
            source_table: Some(31),
            expr_program: Some(&[0xAB, 0xCD]),
        },
        1,
    )
    .unwrap();
    assert_eq!(r.pk, 7 | (5u128 << 64));
    assert_eq!(
        r.vals.len(),
        CIRCUIT_NODES_COLS.len() - 2,
        "compound PK: two key columns"
    );
    assert_eq!(r.vals[CIRCNODES_PAY_NODE_ID], Val::U64(5));
    assert_eq!(r.vals[CIRCNODES_PAY_OPCODE], Val::U64(9));
    assert_eq!(r.vals[CIRCNODES_PAY_SOURCE_TABLE], Val::U64(31));
    assert_eq!(r.vals[CIRCNODES_PAY_EXPR_PROGRAM], Val::Bytes(vec![0xAB, 0xCD]));

    // The two nullable columns still take their slots when absent, so an
    // omitted `put_null` would shift every later value.
    let mut r = Recorder::default();
    write_circuit_node_row(
        &mut r,
        &CircuitNodeRow {
            view_id: 7,
            node_id: 5,
            opcode: 9,
            source_table: None,
            expr_program: None,
        },
        1,
    )
    .unwrap();
    assert_eq!(r.vals[CIRCNODES_PAY_SOURCE_TABLE], Val::Null);
    assert_eq!(r.vals[CIRCNODES_PAY_EXPR_PROGRAM], Val::Null);

    let mut r = Recorder::default();
    write_circuit_edge_row(
        &mut r,
        &CircuitEdgeRow {
            view_id: 7,
            dst_node: 5,
            dst_port: 1,
            src_node: 4,
        },
        1,
    )
    .unwrap();
    assert_eq!(r.pk, 7 | (((5u128 << 8) | 1) << 64));
    assert_eq!(r.vals.len(), CIRCUIT_EDGES_COLS.len() - 2);
    assert_eq!(r.vals[CIRCEDGES_PAY_DST_NODE], Val::U64(5));
    assert_eq!(r.vals[CIRCEDGES_PAY_DST_PORT], Val::U64(1));
    assert_eq!(r.vals[CIRCEDGES_PAY_SRC_NODE], Val::U64(4));

    let mut r = Recorder::default();
    write_circuit_node_column_row(
        &mut r,
        &CircuitNodeColumnRow {
            view_id: 7,
            node_id: 5,
            kind: 3,
            position: 2,
            value1: 11,
            value2: 12,
        },
        1,
    )
    .unwrap();
    assert_eq!(r.pk, 7 | (((5u128 << 24) | (3u128 << 16) | 2u128) << 64));
    assert_eq!(r.vals.len(), CIRCUIT_NODE_COLUMNS_COLS.len() - 2);
    assert_eq!(r.vals[CIRCNCOL_PAY_NODE_ID], Val::U64(5));
    assert_eq!(r.vals[CIRCNCOL_PAY_KIND], Val::U64(3));
    assert_eq!(r.vals[CIRCNCOL_PAY_POSITION], Val::U64(2));
    assert_eq!(r.vals[CIRCNCOL_PAY_VALUE1], Val::U64(11));
    assert_eq!(r.vals[CIRCNCOL_PAY_VALUE2], Val::U64(12));
}

/// A `sub` field that overflows its bit range would alias another row's
/// record, so `circuit_pk` refuses and the writer emits nothing.
#[test]
fn a_circuit_sub_field_that_would_alias_another_record_is_rejected() {
    let mut r = Recorder::default();
    let err = write_circuit_edge_row(
        &mut r,
        &CircuitEdgeRow {
            view_id: 1,
            dst_node: 1 << 40,
            dst_port: 0,
            src_node: 0,
        },
        1,
    )
    .unwrap_err();
    assert!(err.contains("dst_node"), "{err}");
    assert!(r.vals.is_empty(), "a rejected row must emit nothing");
}

/// A column index past the packed key's 9-bit field would alias another
/// column's record, so the writer refuses it rather than emitting the row.
#[test]
fn a_col_idx_that_would_alias_another_record_is_rejected() {
    let mut r = Recorder::default();
    let err = write_col_tab_row(
        &mut r,
        &ColTabRow {
            owner_id: 16,
            owner_kind: 0,
            col_idx: 1 << 20,
            name: "c",
            type_code: 4,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
            is_serial: false,
            is_hidden: false,
        },
        1,
    )
    .unwrap_err();
    assert!(err.contains("exceeds maximum"), "{err}");
    assert!(r.vals.is_empty(), "a rejected row must emit nothing");
}
