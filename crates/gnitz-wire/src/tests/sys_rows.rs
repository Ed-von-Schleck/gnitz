use super::*;
use crate::{
    CIRCNODES_PAY_INPUT_0, CIRCNODES_PAY_INPUT_1, CIRCNODES_PAY_OPCODE, CIRCNODES_PAY_PARAMS,
    CIRCNODES_PAY_SOURCE_TABLE, CIRCUIT_NODES_TAB, COLTAB_PAY_FK_COL_IDX, COLTAB_PAY_FK_TABLE_ID, COLTAB_PAY_IS_HIDDEN,
    COLTAB_PAY_IS_NULLABLE, COLTAB_PAY_NAME, COLTAB_PAY_SCALE, COLTAB_PAY_TYPE_CODE, COL_TAB, IDXTAB_PAY_FLAGS,
    IDXTAB_PAY_NAME, IDXTAB_PAY_OWNER_ID, IDXTAB_PAY_SOURCE_COLS, IDX_TAB, RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID,
    SCHEMATAB_PAY_NAME, SCHEMA_TAB, TABLE_TAB, TABTAB_PAY_FLAGS, TABTAB_PAY_PK_COL_IDX, VIEWTAB_PAY_CAPACITY,
    VIEWTAB_PAY_DELTA, VIEWTAB_PAY_FLAGS, VIEWTAB_PAY_OWNER_VIEW_ID, VIEWTAB_PAY_PK_COL_IDX, VIEW_TAB,
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
    Null,
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
    fn put_null(&mut self) {
        self.vals.push(Val::Null);
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
    write_col_tab_row(&mut rec, r, weight);
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

    let mut r = Recorder::default();
    write_idx_tab_row(
        &mut r,
        &IdxTabRow {
            index_id: 100,
            owner_id: 16,
            source_col_idx: 9,
            name: "idx_t_b",
            flags: 3,
        },
        1,
    );
    assert_eq!(r.pk, [100]);
    let v = r.row(IDX_TAB);
    assert_eq!(v[IDXTAB_PAY_OWNER_ID], Val::U64(16));
    assert_eq!(v[IDXTAB_PAY_SOURCE_COLS], Val::U64(9));
    assert_eq!(v[IDXTAB_PAY_NAME], Val::Str("idx_t_b".into()));
    assert_eq!(v[IDXTAB_PAY_FLAGS], Val::U64(3));

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
    assert_eq!(r.pk, [16]);
    let v = r.row(TABLE_TAB);
    assert_eq!(v[RELTAB_PAY_SCHEMA_ID], Val::U64(3));
    assert_eq!(v[RELTAB_PAY_NAME], Val::Str("t".into()));
    assert_eq!(v[TABTAB_PAY_PK_COL_IDX], Val::U64(5));
    assert_eq!(v[TABTAB_PAY_FLAGS], Val::U64(9));

    let mut r = Recorder::default();
    write_view_tab_row(
        &mut r,
        &ViewTabRow {
            view_id: 20,
            schema_id: 4,
            name: "v",
            pk_col_idx: 6,
            props: crate::ViewProps::Fed { delta_bytes: 1 << 20 },
            owner_view_id: 21,
            pk_repeats: true,
        },
        1,
    );
    assert_eq!(r.pk, [20]);
    let v = r.row(VIEW_TAB);
    assert_eq!(v[RELTAB_PAY_SCHEMA_ID], Val::U64(4));
    assert_eq!(v[RELTAB_PAY_NAME], Val::Str("v".into()));
    assert_eq!(v[VIEWTAB_PAY_PK_COL_IDX], Val::U64(6));
    assert_eq!(v[VIEWTAB_PAY_CAPACITY], Val::U64(0));
    assert_eq!(v[VIEWTAB_PAY_DELTA], Val::U64(1 << 20));
    assert_eq!(v[VIEWTAB_PAY_OWNER_VIEW_ID], Val::U64(21));
    assert_eq!(
        v[VIEWTAB_PAY_FLAGS],
        Val::U64(crate::ViewFlags { pk_repeats: true }.pack())
    );

    let mut r = Recorder::default();
    write_schema_tab_row(&mut r, &SchemaTabRow { schema_id: 3, name: "public" }, 1);
    assert_eq!(r.pk, [3]);
    assert_eq!(r.row(SCHEMA_TAB)[SCHEMATAB_PAY_NAME], Val::Str("public".into()));

    // The circuit family: `view_id` leads the compound key, so the engine's
    // per-view prefix seek reads it.
    let mut r = Recorder::default();
    write_circuit_node_row(
        &mut r,
        &CircuitNodeRow {
            view_id: 7,
            node_id: 5,
            opcode: 9,
            source_table: Some(31),
            inputs: [Some(4), Some(3)],
            params: Some(&[0xAB, 0xCD]),
        },
        1,
    );
    assert_eq!(r.pk, [7, 5]);
    let v = r.row(CIRCUIT_NODES_TAB);
    assert_eq!(v[CIRCNODES_PAY_OPCODE], Val::U64(9));
    assert_eq!(v[CIRCNODES_PAY_SOURCE_TABLE], Val::U64(31));
    assert_eq!(v[CIRCNODES_PAY_INPUT_0], Val::U64(4));
    assert_eq!(v[CIRCNODES_PAY_INPUT_1], Val::U64(3));
    assert_eq!(v[CIRCNODES_PAY_PARAMS], Val::Bytes(vec![0xAB, 0xCD]));

    // Every nullable column still takes its slot when absent, so an omitted
    // `put_null` would shift every later value.
    let mut r = Recorder::default();
    write_circuit_node_row(
        &mut r,
        &CircuitNodeRow {
            view_id: 7,
            node_id: 5,
            opcode: 9,
            source_table: None,
            inputs: [Some(4), None],
            params: None,
        },
        1,
    );
    let v = r.row(CIRCUIT_NODES_TAB);
    assert_eq!(v[CIRCNODES_PAY_SOURCE_TABLE], Val::Null);
    assert_eq!(v[CIRCNODES_PAY_INPUT_0], Val::U64(4));
    assert_eq!(v[CIRCNODES_PAY_INPUT_1], Val::Null);
    assert_eq!(v[CIRCNODES_PAY_PARAMS], Val::Null);
}

/// Pushing each row `write_circuit_rows` would lay down rebuilds the circuit, and a
/// row whose id skips the next index is refused.
#[test]
fn push_row_rebuilds_the_rows_and_refuses_a_gap() {
    use crate::{encode_op_node, Circuit, NodeInputs, OpNode, Opcode};
    let mut original = Circuit::default();
    let scan = original
        .push(
            OpNode::ScanDelta { source: 7, bound: crate::ReadBound::None },
            NodeInputs::Source,
        )
        .unwrap();
    let filter = original
        .push(OpNode::Filter(vec![1, 2, 3]), NodeInputs::Unary(scan))
        .unwrap();
    original.push(OpNode::IntegrateSink, NodeInputs::Unary(filter)).unwrap();

    let mut rebuilt = Circuit::default();
    for (node_id, node) in original.nodes().iter().enumerate() {
        let (opcode, source_table, params) = encode_op_node(&node.op);
        let row = CircuitNodeRow {
            view_id: 1,
            node_id: node_id as u64,
            opcode: opcode.as_wire(),
            source_table,
            inputs: node.inputs.to_slots(),
            params: params.as_deref(),
        };
        rebuilt.push_row(&row).unwrap();
    }
    assert_eq!(rebuilt, original);

    let gap = CircuitNodeRow {
        view_id: 1,
        node_id: 5,
        opcode: Opcode::Negate.as_wire(),
        source_table: None,
        inputs: [Some(0), None],
        params: None,
    };
    assert_eq!(
        rebuilt.push_row(&gap).unwrap_err(),
        "circuit node ids are not dense from 0"
    );
}

/// A row's scan source is its source-table cell, on a scan alone.
#[test]
fn scan_source_is_a_scans_source_table_cell() {
    use crate::Opcode;

    let row = |opcode: Opcode, source_table| CircuitNodeRow {
        view_id: 1,
        node_id: 0,
        opcode: opcode.as_wire(),
        source_table,
        inputs: [None, None],
        params: None,
    };
    assert_eq!(row(Opcode::ScanDelta, Some(7)).scan_source(), Some(7));
    assert_eq!(row(Opcode::IntegrateSink, Some(7)).scan_source(), None);
    assert_eq!(row(Opcode::ScanDelta, None).scan_source(), None);
}
