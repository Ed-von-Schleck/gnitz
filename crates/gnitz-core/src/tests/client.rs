use super::*;
fn kv_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("val", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    }
}

fn ins(schema: &Schema, pk: u64, val: i64) -> ZSetBatch {
    let mut b = ZSetBatch::new(schema);
    BatchAppender::new(&mut b, schema).add_row(pk as u128, 1).i64_val(val);
    b
}

#[test]
fn empty_push_opens_no_family() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    assert!(buf.families.is_empty());
    buf.push(16, &s, ZSetBatch::new(&s), WireConflictMode::Update);
    assert!(buf.families.is_empty(), "an empty push opens no family");
}

#[test]
fn run_splitting_merges_same_mode_and_splits_on_mode_change() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    let tid = 16u64;
    buf.push(tid, &s, ins(&s, 1, 10), WireConflictMode::Update);
    buf.push(tid, &s, ins(&s, 2, 20), WireConflictMode::Update);
    assert_eq!(buf.families.len(), 1, "same-mode pushes coalesce");
    assert_eq!(buf.families[0].batch.len(), 2);
    assert_eq!(buf.families[0].mode, WireConflictMode::Update);
    buf.push(tid, &s, ins(&s, 3, 30), WireConflictMode::Error);
    assert_eq!(buf.families.len(), 2, "mode change opens a new family");
    assert_eq!(buf.families[1].mode, WireConflictMode::Error);
    buf.push(tid, &s, ins(&s, 4, 40), WireConflictMode::Update);
    assert_eq!(
        buf.families.len(),
        3,
        "back to Update opens a third family in call order"
    );
    assert_eq!(buf.families[2].mode, WireConflictMode::Update);
}

#[test]
fn delete_buffers_into_update_family_before_error_reinsert() {
    // "delete k; insert_error k" → Update family [D(k)] then Error family [I(k)].
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    let tid = 16u64;
    buf.push(
        tid,
        &s,
        retraction_batch(&s, PkColumn::from_u128s(8, [7])),
        WireConflictMode::Update,
    );
    buf.push(tid, &s, ins(&s, 7, 70), WireConflictMode::Error);
    assert_eq!(buf.families.len(), 2);
    assert_eq!(buf.families[0].mode, WireConflictMode::Update);
    assert_eq!(buf.families[0].batch.weights, vec![-1]);
    assert_eq!(buf.families[1].mode, WireConflictMode::Error);
    assert_eq!(buf.families[1].batch.weights, vec![1]);
}

#[test]
fn cross_tid_ops_coalesce_per_tid() {
    // push(A), push(B), push(A) → A one family (2 rows), B one family.
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    buf.push(16, &s, ins(&s, 1, 1), WireConflictMode::Update);
    buf.push(17, &s, ins(&s, 1, 1), WireConflictMode::Update);
    buf.push(16, &s, ins(&s, 2, 2), WireConflictMode::Update);
    assert_eq!(buf.families.len(), 2);
    assert_eq!(buf.families[0].tid, 16);
    assert_eq!(buf.families[0].batch.len(), 2);
    assert_eq!(buf.families[1].tid, 17);
}

#[test]
fn last_op_indexes_rows_across_family_extension_and_split() {
    // The PK index must survive both append shapes: extending an existing
    // family (row index = base + i) and opening a new one (base = 0).
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    let tid = 16u64;
    buf.push(tid, &s, ins(&s, 1, 10), WireConflictMode::Update); // new family 0, row 0
    buf.push(tid, &s, ins(&s, 2, 20), WireConflictMode::Update); // extends family 0, row 1
    buf.push(tid, &s, ins(&s, 3, 30), WireConflictMode::Error); // family 1, row 0
    buf.push(
        tid,
        &s,
        retraction_batch(&s, PkColumn::from_u128s(8, [1])),
        WireConflictMode::Update,
    ); // family 2, row 0 — supersedes pk=1

    let val = |pk: u64| {
        let (b, row) = buf.last_op(tid, &PkTuple::from_u128(8, pk as u128)).unwrap();
        (b.weights[row], row)
    };
    assert_eq!(val(1), (-1, 0), "pk=1's last op is the delete");
    assert_eq!(val(2), (1, 1), "pk=2 is row 1 of the extended family");
    assert_eq!(val(3), (1, 0), "pk=3 is row 0 of the mode-split family");
    assert!(buf.last_op(tid, &PkTuple::from_u128(8, 9)).is_none(), "untouched PK");
    assert!(buf.last_op(17, &PkTuple::from_u128(8, 1)).is_none(), "other tid");
    assert_eq!(buf.last_ops(tid).count(), 3);
}

/// A corrupt catalog batch must surface as `Err`, never as an absent-row
/// miss — which a borrowed decode still gives and a raw byte copy of the row
/// would not.
#[test]
fn find_table_record_surfaces_decode_error_not_miss() {
    let schema = sys_schema(TABLE_TAB);
    let mut batch = ZSetBatch::new(schema);
    gnitz_wire::sys_rows::write_table_tab_row(
        &mut BatchAppender::new(&mut batch, schema),
        &TableTabRow {
            table_id: 7,
            schema_id: 1,
            name: "t",
            pk_col_idx: 0,
            flags: 0,
        },
        1,
    );

    // Truncate the schema_id column (Fixed) so `col_u64` on the live row is
    // out of bounds — a real decode error, which must surface as Err rather
    // than be masked as an absent-name miss.
    let ColData::Fixed(bytes) = &mut batch.columns[TABTAB_COL_SCHEMA_ID] else {
        panic!("expected Fixed column");
    };
    bytes.clear();
    match find_table_tab_row(&batch, 7) {
        Err(ClientError::ServerError(s)) => assert!(s.contains("no 8-byte cell"), "got: {s}"),
        _ => panic!("expected decode ServerError, got a non-error result"),
    }
}

#[test]
fn find_schema_id_miss_vs_hit() {
    let schema = sys_schema(SCHEMA_TAB);
    let mut batch = ZSetBatch::new(schema);
    gnitz_wire::sys_rows::write_schema_tab_row(
        &mut BatchAppender::new(&mut batch, schema),
        &gnitz_wire::sys_rows::SchemaTabRow {
            schema_id: 3,
            name: "foo",
        },
        1,
    );
    assert_eq!(find_schema_id(&batch, "foo").unwrap(), Some(3));
    assert!(find_schema_id(&batch, "bar").unwrap().is_none());
}
