use super::*;
use crate::SchemaFacts;

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
    retraction_batch(schema, PkColumn::from_natives(schema, [pk as u128]))
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
    for (tid, batch, mode, basis) in [
        (16, ins(&s, 1), Update, BLIND),     // family 0, row 0
        (16, ZSetBatch::new(&s), Update, 5), // empty: opens no family, indexes nothing
        (16, ins(&s, 2), Update, 7),         // extends family 0, row 1, and lowers its basis
        (17, ins(&s, 1), Update, BLIND),     // family 1 — a different tid
        (17, ZSetBatch::new(&s), Update, 5), // empty: lowers nothing
        (16, ins(&s, 3), Error, BLIND),      // family 2: the mode changed
        (16, del(&s, 1), Update, 9),         // family 3: back to Update, in call order
        (16, ins(&s, 1), Error, BLIND),      // family 4: the Error re-insert
    ] {
        buf.push(tid, &s, batch, mode, basis);
    }

    let shape: Vec<_> = buf
        .families
        .iter()
        .map(|f| (f.tid, f.mode, f.basis, f.batch.weights.clone()))
        .collect();
    assert_eq!(
        shape,
        vec![
            (16, Update, 7, vec![1, 1]),
            (17, Update, BLIND, vec![1]),
            (16, Error, BLIND, vec![1]),
            (16, Update, 9, vec![-1]),
            (16, Error, BLIND, vec![1]),
        ]
    );

    let mut op = |tid, pk: u64| {
        buf.reads(tid)?
            .last_op(kv_schema().opk_key_cols(&[pk as u128]).pk_bytes())
            .map(|(b, row)| (b.weights[row], row))
    };
    assert_eq!(op(16, 1), Some((1, 0)), "the last of three ops on pk=1");
    assert_eq!(op(16, 2), Some((1, 1)), "row 1 of the extended family");
    assert_eq!(op(16, 3), Some((1, 0)), "row 0 of the mode-split family");
    assert_eq!(op(16, 9), None, "an untouched PK");
    assert_eq!(op(17, 2), None, "another tid's PK");
    assert_eq!(buf.reads(16).unwrap().last_ops().count(), 3);
}

/// A family extended by batches read at different watermarks keeps the oldest:
/// every row in it is guarded from the earliest read it was built on, and a blind
/// batch weakens nothing.
#[test]
fn an_extended_family_keeps_its_oldest_basis() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    buf.push(16, &s, ins(&s, 1), WireConflictMode::Update, 20);
    buf.push(16, &s, ins(&s, 2), WireConflictMode::Update, 10);
    buf.push(16, &s, ins(&s, 3), WireConflictMode::Update, BLIND);
    let bases: Vec<_> = buf.families.iter().map(|f| (f.tid, f.basis)).collect();
    assert_eq!(bases, [(16, 10)]);
}

/// `reads` answers `None` when there is nothing to overlay: a relation the
/// transaction never wrote, and one whose only ops are inert weight-0 rows.
#[test]
fn reads_is_none_without_a_live_op() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    assert!(buf.reads(16).is_none(), "an untouched relation");

    let mut zero = ins(&s, 1);
    zero.weights[0] = 0;
    buf.push(16, &s, zero, WireConflictMode::Update, BLIND);
    assert!(buf.reads(16).is_none(), "only a weight-0 op");

    buf.push(16, &s, ins(&s, 2), WireConflictMode::Update, BLIND);
    assert!(buf.reads(16).is_some());
}

/// The read-your-own-writes index is built on read, from a per-family
/// watermark: a blind transaction builds none of it, and an interleaved
/// read/write one folds each row exactly once however often it reads.
#[test]
fn the_read_index_is_built_on_read_and_only_once() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    for pk in 1..=4u64 {
        buf.push(16, &s, ins(&s, pk), WireConflictMode::Error, BLIND);
    }
    assert_eq!(buf.indexed_rows(), 0, "a blind transaction indexes nothing");

    assert_eq!(buf.reads(16).unwrap().last_ops().count(), 4);
    assert_eq!(buf.indexed_rows(), 4);
    // A second read folds nothing further — the watermark already covers them.
    assert_eq!(buf.reads(16).unwrap().last_ops().count(), 4);
    assert_eq!(buf.indexed_rows(), 4);

    // A write after a read is picked up by the next read, and only it.
    buf.push(16, &s, ins(&s, 9), WireConflictMode::Error, BLIND);
    assert_eq!(buf.indexed_rows(), 4, "the write itself indexes nothing");
    assert_eq!(buf.reads(16).unwrap().last_ops().count(), 5);
    assert_eq!(buf.indexed_rows(), 5);

    // An untouched relation is not indexed by another's read.
    assert!(buf.reads(17).is_none());
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
    let i = scanned.live_row_with_pk(schema, 7).expect("the row just written");
    assert!(scanned.live_row_with_pk(schema, 8).is_none(), "an absent tid");

    let mut pair = ZSetBatch::new(schema);
    pair.copy_row_at(&scanned, i, -1);
    pair.copy_row_at(&scanned, i, 1);
    pair.set_string_cell(1, RELTAB_PAY_NAME, "t2");

    assert_eq!(pair.weights, vec![-1, 1]);
    assert_eq!(pair.pks.to_vec_u128(schema), vec![7, 7], "the rename keeps the id");
    assert_eq!(pair.nulls, vec![scanned.nulls[i]; 2]);
    // Every non-name column is the stored row's, in both halves.
    let flags = &pair.payload[gnitz_wire::TABTAB_PAY_FLAGS].bytes;
    assert_eq!(flags[..], [9u64.to_le_bytes(), 9u64.to_le_bytes()].concat()[..]);
    let names: Vec<&[u8]> = pair.payload[RELTAB_PAY_NAME]
        .bytes
        .as_chunks::<16>()
        .0
        .iter()
        .map(|cell| gnitz_wire::german_string_content(cell, &pair.blob))
        .collect();
    assert_eq!(names, [b"t".as_slice(), b"t2".as_slice()]);
}

#[test]
fn find_schema_id_miss_vs_hit() {
    let schema = sys_schema(SCHEMA_TAB);
    let mut batch = ZSetBatch::new(schema);
    gnitz_wire::sys_rows::write_schema_tab_row(
        &mut BatchAppender::new(&mut batch, schema),
        &gnitz_wire::sys_rows::SchemaTabRow { schema_id: 3, name: "foo" },
        1,
    );
    assert_eq!(find_schema_id(&batch, "foo").unwrap(), Some(3));
    assert!(find_schema_id(&batch, "bar").unwrap().is_none());
}
