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

fn ins(schema: &Schema, pk: u64) -> ZSetBatch {
    let mut b = ZSetBatch::new(schema);
    BatchAppender::new(&mut b, schema)
        .add_row(pk as u128, 1)
        .i64_val(pk as i64 * 10);
    b
}

fn del(schema: &Schema, pk: u64) -> ZSetBatch {
    retraction_batch(schema, PkColumn::from_u128s(8, [pk as u128]))
}

/// One mixed sequence pins the whole buffer: same-mode pushes extend the tid's
/// current run, a mode change opens the next family in call order, an empty
/// batch opens none, tids are independent, and every PK indexes to its latest
/// row. "delete k; insert_error k" is the Update-then-Error pair.
#[test]
fn pushes_coalesce_per_tid_into_maximal_same_mode_runs() {
    use WireConflictMode::{Error, Update};
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    for (tid, batch, mode) in [
        (16, ins(&s, 1), Update),         // family 0, row 0
        (16, ZSetBatch::new(&s), Update), // empty: opens no family, indexes nothing
        (16, ins(&s, 2), Update),         // extends family 0, row 1
        (17, ins(&s, 1), Update),         // family 1 — a different tid
        (16, ins(&s, 3), Error),          // family 2: the mode changed
        (16, del(&s, 1), Update),         // family 3: back to Update, in call order
        (16, ins(&s, 1), Error),          // family 4: the Error re-insert
    ] {
        buf.push(tid, &s, batch, mode);
    }

    let shape: Vec<_> = buf
        .families
        .iter()
        .map(|f| (f.tid, f.mode, f.batch.weights.clone()))
        .collect();
    assert_eq!(
        shape,
        vec![
            (16, Update, vec![1, 1]),
            (17, Update, vec![1]),
            (16, Error, vec![1]),
            (16, Update, vec![-1]),
            (16, Error, vec![1]),
        ]
    );

    let op = |tid, pk: u64| {
        buf.last_op(tid, &PkTuple::from_u128(8, pk as u128))
            .map(|(b, row)| (b.weights[row], row))
    };
    assert_eq!(op(16, 1), Some((1, 0)), "the last of three ops on pk=1");
    assert_eq!(op(16, 2), Some((1, 1)), "row 1 of the extended family");
    assert_eq!(op(16, 3), Some((1, 0)), "row 0 of the mode-split family");
    assert_eq!(op(16, 9), None, "an untouched PK");
    assert_eq!(op(17, 2), None, "another tid's PK");
    assert_eq!(buf.last_ops(16).count(), 3);
}

/// A corrupt catalog batch must surface as `Err`, never as the absent-row miss
/// an honest lookup returns.
#[test]
fn catalog_lookups_separate_a_miss_from_a_decode_error() {
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
    assert_eq!(find_table_tab_row(&batch, 7).unwrap().map(|r| r.name), Some("t"));
    assert!(find_table_tab_row(&batch, 8).unwrap().is_none(), "an absent tid");

    // Truncate the schema_id column (Fixed) so `col_u64` on the live row is out
    // of bounds — a real decode error, which must not read as an absent name.
    let ColData::Fixed(bytes) = &mut batch.columns[TABTAB_COL_SCHEMA_ID] else {
        panic!("expected Fixed column");
    };
    bytes.clear();
    assert!(
        matches!(find_table_tab_row(&batch, 7), Err(ClientError::ServerError(_))),
        "a decode error must not be masked as a miss"
    );
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
