//! A `scan_spec` read, in instructions per row.
//!
//! `cd crates && cargo test -p gnitz-store --release scan_spec_bench -- --ignored --nocapture --test-threads=1`
//!
//! A fixture is one run of the RAM tier, which the cursor drains in bulk, so a
//! cell's figure is its filter and its sink.

use std::hint::black_box;

use super::tests::layout_of;
use crate::relation::{Cut as At, RelationKind};
use crate::test_support::{cmp_const, cut, img, map_of, relation_fixture, rows_spec, RelationFixture, TID};
use gnitz_expr::{CmpOp, ExprBuilder, IntArithOp, LogicalInstr, LogicalProgram, Sink};
use gnitz_wire::{
    AggDescriptor, AggFunc, AggReadSpec, ComputeMap, Cut, KeyRange, OrderKey, PkColList, ReadBound, ReadSink, ReadSpec,
    SinkKind, TypeCode,
};
use gnitz_zset::repr::{BatchBuilder, SourceCursor};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

/// Rows a fixture holds: a multiple of every period a column repeats at.
const ROWS: u64 = 3 << 17;

/// `id U64 PK` and the NOT NULL `payload` columns.
fn schema(payload: &[TypeCode]) -> SchemaDescriptor {
    let cols: Vec<_> = std::iter::once(&TypeCode::U64)
        .chain(payload)
        .map(|&tc| SchemaColumn::new(tc, false))
        .collect();
    SchemaDescriptor::new(&cols, &[0])
}

/// A base table over [`schema`]`(payload)`, indexed on `indexed`, holding ids
/// `0..ROWS` at weight 1 as one run of the RAM tier. `put_row` writes a row's
/// payload.
fn fixture(payload: &[TypeCode], indexed: &[u32], mut put_row: impl FnMut(&mut BatchBuilder, u64)) -> RelationFixture {
    let schema = schema(payload);
    let mut bb = BatchBuilder::new(&schema);
    for id in 0..ROWS {
        bb.begin_row(id as u128, 1);
        put_row(&mut bb, id);
        bb.end_row();
    }
    let r = relation_fixture(RelationKind::BaseTable, schema, indexed, [bb.finish()]);
    let table = r.relation(TID).unwrap().table();
    assert_eq!((table.all_shard_arcs().len(), table.runs(At::Now).count()), (0, 1));
    r
}

/// `col < lit` over a fixed-int column, as a wire predicate.
fn lt(col: u32, lit: i64) -> Vec<u8> {
    cmp_const(CmpOp::Lt, col, lit).to_blob_bytes()
}

/// `COUNT(*)` and `SUM(sum_col)` per group of `group_cols`, over `map`'s output.
fn fold(map: Option<ComputeMap>, group_cols: Vec<u32>, sum_col: u32) -> ReadSink {
    let aggs = vec![
        AggDescriptor::COUNT_STAR,
        AggDescriptor { agg_op: AggFunc::Sum, col_idx: sum_col },
    ];
    ReadSink {
        map,
        kind: SinkKind::Fold(AggReadSpec { group_cols, aggs }),
    }
}

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn scan_spec_bench() {
    const I64: TypeCode = TypeCode::I64;
    let counter = gnitz_foundation::perf::Counter::instructions();
    // `per` is the rows the read walks, or the rows it returns where it stops
    // early or walks an index.
    let cell = |label: &str, r: &RelationFixture, spec: ReadSpec, want: u64, per: u64| {
        let layout = layout_of(&r.relation(TID).unwrap().schema(), &spec.sink);
        // Untimed: the batch pool is warm.
        black_box(r.scan_spec(TID, spec.clone(), layout, None).unwrap());
        let (reply, instructions) = counter.measure(|| r.scan_spec(TID, spec, layout, None).unwrap());
        assert_eq!(reply.len() as u64, want, "{label}: reply rows");
        println!(
            "scan_spec_bench {label:<44} {want:>7} rows {:>9.1} instr/row",
            instructions as f64 / per as f64
        );
    };

    // `id | c0 = id % 512 | c1 = id % 2 | c2, a permutation of the ids | c3 = id / 7`,
    // indexed on `c2`.
    let r = fixture(&[I64; 4], &[3], |bb, id| {
        bb.put_u64(id % 512);
        bb.put_u64(id % 2);
        bb.put_u64(id * 179_999 % ROWS);
        bb.put_u64(id / 7);
    });

    // `c2, c3, c0`.
    let copy = map_of(LogicalProgram::copy_cols(&[3, 4, 1]), &schema(&[I64; 3]));
    // `c2 + c3, c0`.
    let computed = {
        let mut eb = ExprBuilder::new();
        let (a, b) = (
            eb.emit(LogicalInstr::LoadCol { col: 3 }),
            eb.emit(LogicalInstr::LoadCol { col: 4 }),
        );
        let sum = eb.emit(LogicalInstr::IntArith { op: IntArithOp::Add, a, b });
        map_of(
            eb.build(vec![Sink::Reg(sum), Sink::Col(1)]).unwrap(),
            &schema(&[I64; 2]),
        )
    };
    let rows = |map| rows_spec(map, None).sink;
    // `c2` behind the key region, and `id, c2` as two payload columns.
    let one = map_of(LogicalProgram::copy_cols(&[3]), &schema(&[I64; 1]));
    let key_copied = map_of(LogicalProgram::copy_cols(&[0, 3]), &schema(&[TypeCode::U64, I64]));
    // Each sink over half the rows, a group per `c0` where it folds.
    let sinks = [
        ("rows", ReadSink::all_rows(), ROWS / 2),
        ("copied columns", rows(copy.clone()), ROWS / 2),
        ("one copied column", rows(one), ROWS / 2),
        ("one copied column and the key again", rows(key_copied), ROWS / 2),
        ("computed columns", rows(computed.clone()), ROWS / 2),
        ("fold", fold(None, vec![1], 3), 256),
        ("fold of computed columns", fold(computed, vec![2], 1), 256),
        ("global fold", fold(None, vec![], 3), 1),
    ];
    // A sink reads its survivors as row ranges: 256 rows long under `c0 < 256`,
    // one row long under `c1 < 1`.
    for (ranges, predicate) in [("256-row ranges", lt(1, 256)), ("1-row ranges", lt(2, 1))] {
        for (sink, sink_spec, want) in &sinks {
            let spec = ReadSpec {
                bound: ReadBound::None,
                predicate: predicate.clone(),
                sink: sink_spec.clone(),
            };
            cell(&format!("{sink}, {ranges}"), &r, spec, *want, ROWS);
        }
    }

    // The 100 smallest `c0` of the reply, which all tie, so the row's identity
    // orders them.
    let order = vec![OrderKey { col: 3, desc: false, nulls_first: false }];
    let top = ReadSpec {
        predicate: lt(1, 256),
        ..rows_spec(copy.clone(), cut(100, order))
    };
    cell("ORDER BY .. LIMIT 100", &r, top, 100, ROWS);
    let first = ReadSpec {
        predicate: lt(1, 256),
        ..rows_spec(copy, cut(100, vec![]))
    };
    cell("LIMIT 100", &r, first, 100, 100);
    // A cut by the key over every row. Descending ranks them all; ascending is the
    // walk's own order, which the planner ships as the cut alone.
    let key_desc = vec![OrderKey { col: 0, desc: true, nulls_first: false }];
    cell(
        "ORDER BY id DESC LIMIT 100",
        &r,
        rows_spec(None, cut(100, key_desc)),
        100,
        ROWS,
    );
    cell("ORDER BY id LIMIT 100", &r, rows_spec(None, cut(100, vec![])), 100, 100);

    // The widest range of `c2` the index is walked for, and one row wider,
    // which scans the table under the range.
    for (label, width, walks) in [
        ("range, index walk", ROWS / 16, true),
        ("range, filtered scan", ROWS / 16 + 1, false),
    ] {
        let range = KeyRange::new(
            PkColList::from_slice(&[3]),
            &[],
            Cut::before(img(0)),
            Cut::before(img(width as i64)),
        );
        let bound = ReadBound::Range(range);
        let (source, _) = r.open_bound(TID, bound.clone(), At::Now).unwrap();
        assert_eq!(matches!(source, SourceCursor::Bounded(_)), walks, "{label}");
        cell(label, &r, ReadSpec::all_rows(bound), width, width);
    }

    // `id | s | c1 = id % 2`: of the rows `c1 < 1` keeps, every other `s` is
    // out of line.
    let strings = [TypeCode::String, I64];
    let r = fixture(&strings, &[], |bb, id| {
        match id % 4 < 2 {
            true => bb.put_string(&format!("a-fairly-long-out-of-line-value-{id}")),
            false => bb.put_string("short"),
        }
        bb.put_u64(id % 2);
    });
    let spec = ReadSpec {
        predicate: lt(2, 1),
        ..rows_spec(map_of(LogicalProgram::copy_cols(&[1, 2]), &schema(&strings)), None)
    };
    cell("copied STRING column, 1-row ranges", &r, spec, ROWS / 2, ROWS);
}
