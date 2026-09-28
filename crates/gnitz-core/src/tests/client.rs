use super::*;
use crate::protocol::TypeCode;
use crate::SchemaFacts;
use gnitz_wire::PkKeys;

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

/// `(pk, val, weight)` rows.
fn rows(schema: &Schema, rows: &[(u64, i64, i64)]) -> ZSetBatch {
    let mut b = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut b, schema);
    for &(pk, v, w) in rows {
        app.add_row(pk as u128, w).i64_val(v);
    }
    b
}

/// `(pk, val, weight)` of a `kv_schema` batch, sorted.
fn contents(schema: &Schema, b: &ZSetBatch) -> Vec<(u64, i64, i64)> {
    let mut out: Vec<_> = (0..b.len())
        .map(|i| {
            let v = i64::from_le_bytes(b.payload[0].bytes[i * 8..i * 8 + 8].try_into().unwrap());
            (b.pks.get(schema, i) as u64, v, b.weights[i])
        })
        .collect();
    out.sort();
    out
}

/// `buf`'s overlay of an empty committed read of `tid` over every row, as
/// [`contents`].
fn overlaid(buf: &mut TxnBuffer, tid: u64) -> Vec<(u64, i64, i64)> {
    let s = kv_schema();
    let spec = ReadSpec::all_rows(ReadBound::None);
    let out = buf.overlay(tid, &s, &spec, false, ZSetBatch::new(&s)).unwrap();
    contents(&s, &out)
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
        (16, rows(&s, &[(1, 11, 1)]), Update, BLIND), // family 0, row 0
        (16, ZSetBatch::new(&s), Update, 5),          // empty: opens no family, indexes nothing
        (16, rows(&s, &[(2, 21, 1)]), Update, 7),     // extends family 0, row 1, and lowers its basis
        (17, rows(&s, &[(1, 12, 1)]), Update, BLIND), // family 1 — a different tid
        (17, ZSetBatch::new(&s), Update, 5),          // empty: lowers nothing
        (16, rows(&s, &[(3, 31, 1)]), Error, BLIND),  // family 2: the mode changed
        (16, del(&s, 1), Update, 9),                  // family 3: back to Update, in call order
        (16, rows(&s, &[(1, 13, 1)]), Error, BLIND),  // family 4: the Error re-insert
    ] {
        buf.push(tid, &s, batch, mode, basis).unwrap();
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

    assert_eq!(
        overlaid(&mut buf, 16),
        [(1, 13, 1), (2, 21, 1), (3, 31, 1)],
        "the last of pk 1's three ops wins"
    );
    assert_eq!(overlaid(&mut buf, 17), [(1, 12, 1)], "tids are independent");
}

/// A family extended by batches read at different watermarks keeps the oldest:
/// every row in it is guarded from the earliest read it was built on, and a blind
/// batch weakens nothing.
#[test]
fn an_extended_family_keeps_its_oldest_basis() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    buf.push(16, &s, ins(&s, 1), WireConflictMode::Update, 20).unwrap();
    buf.push(16, &s, ins(&s, 2), WireConflictMode::Update, 10).unwrap();
    buf.push(16, &s, ins(&s, 3), WireConflictMode::Update, BLIND).unwrap();
    let bases: Vec<_> = buf.families.iter().map(|f| (f.tid, f.basis)).collect();
    assert_eq!(bases, [(16, 10)]);
}

/// The overlay is the committed read itself when there is nothing to overlay: a
/// relation the transaction never wrote, and one whose only ops are inert weight-0
/// rows.
#[test]
fn overlay_is_the_committed_read_without_a_live_op() {
    let s = kv_schema();
    let spec = ReadSpec::all_rows(ReadBound::None);
    let committed = rows(&s, &[(1, 10, 1), (2, 20, 1)]);
    let mut buf = TxnBuffer::default();
    let out = buf.overlay(16, &s, &spec, false, committed.clone()).unwrap();
    assert_eq!(out, committed, "an untouched relation");

    let mut zero = ins(&s, 1);
    zero.weights[0] = 0;
    buf.push(16, &s, zero, WireConflictMode::Update, BLIND).unwrap();
    let out = buf.overlay(16, &s, &spec, false, committed.clone()).unwrap();
    assert_eq!(out, committed, "only a weight-0 op");
}

/// The read-your-own-writes index is built on read, from a per-family
/// watermark: a blind transaction builds none of it, and an interleaved
/// read/write one folds each row exactly once however often it reads.
#[test]
fn the_read_index_is_built_on_read_and_only_once() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    for pk in 1..=4u64 {
        buf.push(16, &s, ins(&s, pk), WireConflictMode::Error, BLIND).unwrap();
    }
    assert_eq!(buf.indexed_rows(), 0, "a blind transaction indexes nothing");

    assert_eq!(overlaid(&mut buf, 16).len(), 4);
    assert_eq!(buf.indexed_rows(), 4);
    // A second read folds nothing further — the watermark already covers them.
    assert_eq!(overlaid(&mut buf, 16).len(), 4);
    assert_eq!(buf.indexed_rows(), 4);

    // A write after a read is picked up by the next read, and only it.
    buf.push(16, &s, ins(&s, 9), WireConflictMode::Error, BLIND).unwrap();
    assert_eq!(buf.indexed_rows(), 4, "the write itself indexes nothing");
    assert_eq!(overlaid(&mut buf, 16).len(), 5);
    assert_eq!(buf.indexed_rows(), 5);

    // An untouched relation is not indexed by another's read.
    assert!(overlaid(&mut buf, 17).is_empty());
    assert_eq!(buf.indexed_rows(), 5);
}

/// Every family of a tid shares one layout: a batch in another is refused and
/// leaves the buffer as it was, and a read under another layout is refused too.
#[test]
fn a_tid_refuses_a_second_layout() {
    let s = kv_schema();
    let mut wide = kv_schema();
    wide.columns.push(ColumnDef::new("w", TypeCode::I64, false));
    let mut buf = TxnBuffer::default();
    buf.push(16, &s, ins(&s, 1), WireConflictMode::Update, BLIND).unwrap();

    let mut b = ZSetBatch::new(&wide);
    BatchAppender::new(&mut b, &wide).add_row(2, 1).i64_val(20).i64_val(30);
    let err = buf.push(16, &wide, b, WireConflictMode::Error, BLIND).unwrap_err();
    assert!(err.to_string().contains("changed layout"), "{err}");
    let shape: Vec<_> = buf.families.iter().map(|f| (f.tid, f.batch.len())).collect();
    assert_eq!(shape, [(16, 1)], "the refused batch left no trace");
    let spec = ReadSpec::all_rows(ReadBound::None);
    assert!(buf.overlay(16, &wide, &spec, false, ZSetBatch::new(&wide)).is_err());

    // Another tid is independent.
    let mut b = ZSetBatch::new(&wide);
    BatchAppender::new(&mut b, &wide).add_row(2, 1).i64_val(20).i64_val(30);
    buf.push(17, &wide, b, WireConflictMode::Error, BLIND).unwrap();
}

const TID: u64 = 7;

/// `buf`'s overlay of `committed`, a read of `TID` under `bound` and `predicate`.
fn overlay_of(buf: &mut TxnBuffer, bound: ReadBound, predicate: Vec<u8>, committed: ZSetBatch) -> Vec<(u64, i64, i64)> {
    let s = kv_schema();
    let spec = ReadSpec {
        bound,
        predicate,
        sink: ReadSink::all_rows(),
    };
    let out = buf.overlay(TID, &s, &spec, false, committed).unwrap();
    contents(&s, &out)
}

#[test]
fn the_last_buffered_op_on_a_key_wins() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    buf.push(TID, &s, rows(&s, &[(1, 11, 1)]), WireConflictMode::Update, 3)
        .unwrap();
    buf.push(TID, &s, rows(&s, &[(1, 12, 1)]), WireConflictMode::Update, 3)
        .unwrap();
    let committed = rows(&s, &[(1, 10, 1), (2, 20, 1)]);
    let out = overlay_of(&mut buf, ReadBound::None, Vec::new(), committed);
    assert_eq!(out, [(1, 12, 1), (2, 20, 1)]);
}

#[test]
fn a_delete_then_reinsert_is_present() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    buf.push(TID, &s, del(&s, 5), WireConflictMode::Update, 3).unwrap();
    buf.push(TID, &s, rows(&s, &[(5, 99, 1)]), WireConflictMode::Error, 3)
        .unwrap();
    let committed = rows(&s, &[(5, 50, 1)]);
    let out = overlay_of(&mut buf, ReadBound::None, Vec::new(), committed);
    assert_eq!(out, [(5, 99, 1)]);
}

#[test]
fn a_tid_scopes_its_ops() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    buf.push(TID, &s, rows(&s, &[(1, 10, 1)]), WireConflictMode::Update, 3)
        .unwrap();
    buf.push(TID + 1, &s, rows(&s, &[(2, 20, 1)]), WireConflictMode::Update, 3)
        .unwrap();
    let out = overlay_of(&mut buf, ReadBound::None, Vec::new(), ZSetBatch::new(&s));
    assert_eq!(out, [(1, 10, 1)], "another tid's row stays out");
}

#[test]
fn a_tombstone_drops_the_committed_row() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    buf.push(TID, &s, del(&s, 2), WireConflictMode::Update, 3).unwrap();
    let committed = rows(&s, &[(1, 10, 1), (2, 20, 1)]);
    let out = overlay_of(&mut buf, ReadBound::None, Vec::new(), committed);
    assert_eq!(out, [(1, 10, 1)]);
}

#[test]
fn a_pk_set_restricts_the_buffered_side_to_its_keys() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    buf.push(
        TID,
        &s,
        rows(&s, &[(1, 11, 1), (2, 22, 1)]),
        WireConflictMode::Update,
        3,
    )
    .unwrap();
    let keys = PkKeys::from_keys(s.pk_stride(), [s.opk_key_cols(&[1]).pk_bytes()]);
    let out = overlay_of(&mut buf, ReadBound::PkSet(keys), Vec::new(), ZSetBatch::new(&s));
    assert_eq!(out, [(1, 11, 1)]);
}

#[test]
fn the_predicate_filters_buffered_rows() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    buf.push(
        TID,
        &s,
        rows(&s, &[(1, 10, 1), (2, 20, 1)]),
        WireConflictMode::Update,
        3,
    )
    .unwrap();
    // `val > 15`, over the source schema's column indices.
    let mut b = gnitz_expr::ExprBuilder::new();
    let c = b.emit(gnitz_expr::LogicalInstr::LoadColInt { col: 1 });
    let k = b.emit(gnitz_expr::LogicalInstr::LoadConst { val: 15, unsigned: false });
    let cond = b.emit(gnitz_expr::LogicalInstr::Cmp { op: gnitz_expr::CmpOp::Gt, a: c, b: k });
    let predicate = b.build(vec![gnitz_expr::Sink::Reg(cond)]).unwrap().to_blob_bytes();
    let out = overlay_of(&mut buf, ReadBound::None, predicate, ZSetBatch::new(&s));
    assert_eq!(out, [(2, 20, 1)]);
}

/// A keys reply carries no payload, so a buffered row joins it as its key alone.
#[test]
fn a_keys_reply_appends_keys_only() {
    let s = kv_schema();
    let mut buf = TxnBuffer::default();
    buf.push(TID, &s, rows(&s, &[(3, 30, 1)]), WireConflictMode::Update, 3)
        .unwrap();
    let (reply, sink) = key_reply(&s);
    let spec = ReadSpec {
        bound: ReadBound::None,
        predicate: Vec::new(),
        sink,
    };
    let mut committed = ZSetBatch::new(&reply);
    committed.pks.push_natives(&reply, &[1]);
    committed.weights.push(1);
    committed.nulls.push(0);
    let out = buf.overlay(TID, &s, &spec, true, committed).unwrap();
    out.validate(&reply).expect("the overlay is in the key reply's layout");
    let keys: Vec<u128> = (0..out.len()).map(|i| out.pks.get(&reply, i)).collect();
    assert_eq!(keys, [1, 3]);
    assert_eq!(
        (out.weights.as_slice(), out.nulls.as_slice()),
        (&[1, 1][..], &[0, 0][..])
    );
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
