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

    let mut op = |tid, pk: u64| {
        buf.reads(tid)
            .last_op(&PkTuple::from_u128(8, pk as u128))
            .map(|(b, row)| (b.weights[row], row))
    };
    assert_eq!(op(16, 1), Some((1, 0)), "the last of three ops on pk=1");
    assert_eq!(op(16, 2), Some((1, 1)), "row 1 of the extended family");
    assert_eq!(op(16, 3), Some((1, 0)), "row 0 of the mode-split family");
    assert_eq!(op(16, 9), None, "an untouched PK");
    assert_eq!(op(17, 2), None, "another tid's PK");
    assert_eq!(buf.reads(16).last_ops().count(), 3);
}

/// The read-your-own-writes index is built on read, from a per-family
/// watermark: a blind transaction builds none of it, and an interleaved
/// read/write one folds each row exactly once however often it reads.
#[test]
fn the_overlay_index_is_built_on_read_and_only_once() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    for pk in 1..=4u64 {
        buf.push(16, &s, ins(&s, pk), WireConflictMode::Error);
    }
    assert_eq!(buf.indexed_rows(), 0, "a blind transaction indexes nothing");

    assert_eq!(buf.reads(16).last_ops().count(), 4);
    assert_eq!(buf.indexed_rows(), 4);
    // A second read folds nothing further — the watermark already covers them.
    assert_eq!(buf.reads(16).last_ops().count(), 4);
    assert_eq!(buf.indexed_rows(), 4);

    // A write after a read is picked up by the next read, and only it.
    buf.push(16, &s, ins(&s, 9), WireConflictMode::Error);
    assert_eq!(buf.indexed_rows(), 4, "the write itself indexes nothing");
    assert_eq!(buf.reads(16).last_ops().count(), 5);
    assert_eq!(buf.indexed_rows(), 5);

    // An untouched relation is not indexed by another's read.
    assert_eq!(buf.reads(17).last_ops().count(), 0);
    assert_eq!(buf.indexed_rows(), 5);
}

/// The rename pair every catalog retraction is built from: a verbatim copy at
/// `-1` reproduces the stored row in every column, and the `+1` differs only
/// where `set_string_cell` patched it.
#[test]
fn a_copied_catalog_row_differs_only_where_it_is_patched() {
    let schema = sys_schema(TABLE_TAB);
    let mut scanned = ZSetBatch::new(schema);
    gnitz_wire::sys_rows::write_table_tab_row(
        &mut BatchAppender::new(&mut scanned, schema),
        &TableTabRow {
            table_id: 7,
            schema_id: 1,
            name: "t",
            pk_col_idx: 0,
            flags: 9,
        },
        1,
    );
    let i = scanned.live_row_with_pk(7).expect("the row just written");
    assert!(scanned.live_row_with_pk(8).is_none(), "an absent tid");

    let mut pair = ZSetBatch::new(schema);
    pair.copy_row_at(&scanned, i, -1, schema);
    pair.copy_row_at(&scanned, i, 1, schema);
    pair.set_string_cell(1, TABTAB_COL_NAME, "t2");

    assert_eq!(pair.weights, vec![-1, 1]);
    assert_eq!(pair.pks.to_vec_u128(), vec![7, 7], "the rename keeps the id");
    assert_eq!(pair.nulls, vec![scanned.nulls[i]; 2]);
    // Every non-name column is the stored row's, in both halves.
    let ColData::Fixed(flags) = &pair.columns[gnitz_wire::TABTAB_COL_FLAGS] else {
        panic!("expected Fixed column");
    };
    assert_eq!(flags[..], [9u64.to_le_bytes(), 9u64.to_le_bytes()].concat()[..]);
    let ColData::Strings(names) = &pair.columns[TABTAB_COL_NAME] else {
        panic!("expected Strings column");
    };
    assert_eq!(names, &[Some("t".to_string()), Some("t2".to_string())]);
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
