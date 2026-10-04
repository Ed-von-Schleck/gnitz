//! Release benchmarks: instructions a view's maintenance costs per source row,
//! for a projection, and per one-row tick for a grouped MIN and a per-group
//! top-N whose operator state spans many runs.
//!
//! ```text
//! cd crates && cargo test -p gnitz-server --release view_tick_bench \
//!     -- --ignored --nocapture --test-threads=1
//! ```

use super::*;
use crate::query::Drive;
use gnitz_foundation::perf;
use gnitz_wire::{AggDescriptor, AggFunc, Circuit, ReadBound};

/// Base rows each fixture loads before its view is created.
const ROWS: u64 = 1_000_000;

/// A payload value spread over the whole `u64` range.
fn spread(id: u64) -> u64 {
    id.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Column records for a view whose output is `schema`.
fn cols_of(schema: &SchemaDescriptor) -> Vec<CatalogColumn> {
    (0..schema.num_columns())
        .map(|ci| {
            let c = schema.column(ci).expect("in range");
            CatalogColumn {
                def: gnitz_wire::ColumnDef::new(format!("c{ci}"), c.type_code, c.nullable),
                fk: None,
            }
        })
        .collect()
}

/// The files under `dir`, at any depth: a store's shards and manifests.
fn files_under(dir: &str) -> usize {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => files_under(e.path().to_str().expect("a UTF-8 path")),
            _ => 1,
        })
        .sum()
}

/// One tick of `t` over `rows`, each `(id, weight, two payload cells)`; the
/// instructions the drive took.
fn tick(
    engine: &mut CatalogEngine,
    counter: &perf::Counter,
    t: u64,
    round: u64,
    rows: impl Iterator<Item = (u64, i64, [u64; 2])>,
) -> u64 {
    let schema = engine.registry.relation(t).map(Relation::schema).unwrap();
    let mut bb = BatchBuilder::new(&schema);
    for (id, weight, cells) in rows {
        bb.begin_row(id as u128, weight);
        cells.into_iter().for_each(|c| bb.put_u64(c));
        bb.end_row();
    }
    let effective = engine.registry.ingest_returning(t, bb.finish()).unwrap();
    let what = Drive::Tick { source: t, round };
    let (res, instructions) = counter.measure(|| crate::query::drive(&mut LocalDrive(engine), what, Some(effective)));
    res.unwrap();
    instructions
}

/// The summed weight `tid` holds.
fn net_weight(engine: &mut CatalogEngine, tid: u64) -> i64 {
    let rows = scan_all(engine, tid);
    (0..rows.len()).map(|i| rows.get_weight(i)).sum()
}

/// The backfill and the wide ticks of `SELECT id, b FROM t`: every row the map
/// writes leaves it in the order it arrived in.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn projection_view_tick_bench() {
    const TICK_ROWS: u64 = 10_000;
    const TICKS: u64 = 20;
    let counter = perf::Counter::instructions();
    let cols = vec![
        col_def("id", TypeCode::U64),
        col_def("a", TypeCode::U64),
        col_def("b", TypeCode::U64),
    ];
    let cells = |id: u64| [spread(id), spread(id ^ 0x55)];
    let (mut engine, t) = ingest_fixture("projection_view_tick_bench", &cols, ROWS, |bb, id| {
        cells(id).into_iter().for_each(|c| bb.put_u64(c));
    });
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(t, ReadBound::None);
    let projected = circuit.map(scan, &[2]);
    circuit.sink(projected);
    let view_cols = vec![col_def("id", TypeCode::U64), col_def("b", TypeCode::U64)];
    let v = try_register_view(&mut engine, circuit, "p", &view_cols, 0, 0).unwrap();

    let ((), fill) = counter.measure(|| backfill(&mut engine, v, &[t]));
    let mut ticks = 0;
    for round in 0..TICKS {
        let ids = (0..TICK_ROWS).map(|i| ROWS + round * TICK_ROWS + i);
        ticks += tick(&mut engine, &counter, t, round + 1, ids.map(|id| (id, 1, cells(id))));
    }
    assert_eq!(
        net_weight(&mut engine, v) as u64,
        ROWS + TICKS * TICK_ROWS,
        "every row landed"
    );
    println!(
        "projection view: backfill {:>6.1} instr/row, {TICK_ROWS}-row ticks {:>6.1} instr/row",
        fill as f64 / ROWS as f64,
        ticks as f64 / (TICKS * TICK_ROWS) as f64,
    );
}

/// One-row ticks into a view grouped on `k` over `t (id, k, v)`, which `view`
/// builds over the scan and names the output schema of: inserts of fresh ids,
/// then deletes of stored rows. Run with the operator state in RAM, and spilled
/// into few and into many shards.
fn grouped_view_ticks(
    what: &str,
    view: impl Fn(&SchemaDescriptor, &mut Circuit, gnitz_wire::NodeId) -> SchemaDescriptor,
) {
    const GROUPS: u64 = 100_000;
    const TICKS: u64 = 2_000;
    let counter = perf::Counter::instructions();
    let cells = |id: u64| [spread(id) % GROUPS, id + 10];
    // The RAM tier every store of the fixture spills past, and the backfill's
    // chunk size: small chunks leave small shards, and so many of them.
    for (label, ram_tier, chunk_rows) in [
        ("in RAM", None, None),
        ("few shards", Some(256u64 << 10), None),
        ("many shards", Some(32 << 10), Some(1_000)),
    ] {
        if let Some(bytes) = ram_tier {
            std::env::set_var("GNITZ_RAM_TIER_BYTES", bytes.to_string());
        }
        let cols = vec![
            col_def("id", TypeCode::U64),
            col_def("k", TypeCode::U64),
            col_def("v", TypeCode::I64),
        ];
        let name = format!("view_tick_bench_{what}_{}", label.replace(' ', "_"));
        let dir = temp_dir(&name);
        let (mut engine, t) = ingest_fixture(&name, &cols, ROWS, |bb, id| {
            cells(id).into_iter().for_each(|c| bb.put_u64(c));
        });
        std::env::remove_var("GNITZ_RAM_TIER_BYTES");
        if let Some(rows) = chunk_rows {
            engine.registry.set_scan_chunk_rows(rows);
        }

        let schema = engine.registry.relation(t).map(Relation::schema).unwrap();
        let mut circuit = Circuit::default();
        let scan = circuit.input_delta(t, ReadBound::None);
        let out = view(&schema, &mut circuit, scan);
        let v = try_register_view(&mut engine, circuit, "v", &cols_of(&out), 0, 0).unwrap();
        backfill(&mut engine, v, &[t]);
        let files = files_under(&relation_dir(&dir, v));
        let held = net_weight(&mut engine, v);

        let (mut inserts, mut deletes) = (0, 0);
        for i in 0..TICKS {
            let id = ROWS + i;
            inserts += tick(&mut engine, &counter, t, i + 1, [(id, 1, cells(id))].into_iter());
        }
        for i in 0..TICKS {
            // The rows just inserted, so the view ends where the backfill left it.
            let id = ROWS + i;
            deletes += tick(
                &mut engine,
                &counter,
                t,
                TICKS + i + 1,
                [(id, -1, cells(id))].into_iter(),
            );
        }
        assert_eq!(net_weight(&mut engine, v), held, "the deletes undid the inserts");
        println!(
            "{what} view, state {label:<11} ({files:>3} files): insert {:>7} instr/tick, delete {:>7} instr/tick",
            inserts / TICKS,
            deletes / TICKS,
        );
        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }
}

/// `SELECT k, COUNT(*), MIN(v) FROM t GROUP BY k`: an insert leaves its group's
/// minimum where it was, and a delete reads the minimum back out of the value
/// index.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn minmax_view_tick_bench() {
    grouped_view_ticks("MIN", |schema, circuit, scan| {
        let (group, aggs) = (
            [1u32],
            [
                AggDescriptor::COUNT_STAR,
                AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
            ],
        );
        let reduced = circuit.reduce_multi(scan, &group, &aggs);
        circuit.sink(reduced);
        *gnitz_zset::stream::ReducePlan::from_wire(schema, &group, &aggs, false)
            .unwrap()
            .output_schema()
    });
}

/// The three rows of each `k` least in `v`: every tick walks its group's window
/// in the ordered index.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn topn_view_tick_bench() {
    grouped_view_ticks("top-3", |schema, circuit, scan| {
        let (group, order) = (
            [1u32],
            [gnitz_wire::OrderKey { col: 2, desc: false, nulls_first: false }],
        );
        let top = circuit.top_n(scan, &group, &order, 3, 0);
        circuit.sink(top);
        gnitz_zset::stream::TopNPlan::from_wire(schema, &group, &order, 3, 0)
            .unwrap()
            .output_schema
    });
}
