//! Release benchmarks: instructions a view's maintenance costs per source row of
//! a wide tick, for a projection, for a view `distinct` readers consume and for
//! the preserved side of a left join, per final row of a left join's backfill,
//! and per one-row tick for an identity and for the operators that keep state,
//! with that state in RAM and spilled.
//!
//! ```text
//! cd crates && cargo test -p gnitz --release view_tick_bench \
//!     -- --ignored --nocapture --test-threads=1
//! ```

use super::*;
use gnitz_foundation::perf;
use gnitz_wire::{AggDescriptor, AggFunc, Circuit, ReadBound};

/// Base rows a table-sourced fixture loads before its view is created.
const ROWS: u64 = 1_000_000;

/// One tick of `source` over `rows`, pushed as a worker applies them; the
/// instructions the tick took, its seal among them.
fn tick(engine: &mut CatalogEngine, counter: &perf::Counter, source: u64, rows: Batch) -> u64 {
    engine.ingest_unticked(source, rows).unwrap();
    let (res, instructions) = counter.measure(|| crate::query::tick(&mut LocalDrive(engine), source, 1));
    res.unwrap();
    instructions
}

// ── Wide ticks ──────────────────────────────────────────────────────────────

/// Timed wide ticks per cell, and the fresh ids each carries.
const WIDE_TICKS: u64 = 20;
const WIDE_TICK_ROWS: u64 = 10_000;

/// `WIDE_TICKS` ticks of `source`, each over the next `WIDE_TICK_ROWS` ids from
/// `first`, pushed in descending order where `descending`; the instructions per
/// row.
fn wide_ticks<const N: usize>(
    engine: &mut CatalogEngine,
    counter: &perf::Counter,
    source: u64,
    first: u64,
    descending: bool,
    cells: impl Fn(u64) -> [u64; N],
) -> f64 {
    let mut instructions = 0;
    for round in 0..WIDE_TICKS {
        let ids = first + round * WIDE_TICK_ROWS..first + (round + 1) * WIDE_TICK_ROWS;
        let batch = match descending {
            true => rows(engine, source, 1, ids.rev(), &cells),
            false => rows(engine, source, 1, ids, &cells),
        };
        instructions += tick(engine, counter, source, batch);
    }
    instructions as f64 / (WIDE_TICKS * WIDE_TICK_ROWS) as f64
}

/// The backfill and the wide ticks of `SELECT id, b FROM t`: every row the map
/// writes leaves it in the order it arrived in.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn projection_view_tick_bench() {
    let counter = perf::Counter::instructions();
    let cols = [
        col_def("id", TypeCode::U64),
        col_def("a", TypeCode::U64),
        col_def("b", TypeCode::U64),
    ];
    let cells = |id: u64| [scramble(id), scramble(id ^ 0x55)];
    let (mut engine, t) = ingest_fixture("projection_view_tick_bench", &cols, ROWS, cells);
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(t, ReadBound::None);
    circuit.map(scan, &[2]);
    let view_cols = [col_def("id", TypeCode::U64), col_def("b", TypeCode::U64)];
    let v = try_register_view(&mut engine, circuit, "p", &view_cols, 0, 0).unwrap();

    let ((), fill) = counter.measure(|| backfill(&mut engine, v));
    let ticks = wide_ticks(&mut engine, &counter, t, ROWS, false, cells);
    assert_eq!(
        net_weight(&engine, v) as u64,
        ROWS + WIDE_TICKS * WIDE_TICK_ROWS,
        "every row landed"
    );
    println!(
        "projection view: backfill {:>6.1} instr/row, {WIDE_TICK_ROWS}-row ticks {ticks:>6.1} instr/row",
        fill as f64 / ROWS as f64,
    );
    discard(engine);
}

/// The wide ticks of an identity view over a stream, whose output `readers`
/// `distinct` views consume at net weights: a stream's delta reaches the view as
/// pushed, here in descending id order, so what the view echoes to its readers is
/// unsorted until it is folded.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn echo_fold_view_tick_bench() {
    let counter = perf::Counter::instructions();
    let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
    let cells = |id: u64| [scramble(id)];
    for readers in [0usize, 1, 2, 4] {
        let mut engine = CatalogEngine::open(&temp_dir(&format!("echo_fold_{readers}")), 1).unwrap();
        let s = create_flagged_table(&mut engine, "s", &cols, &[0], stream_flags());
        let a = register_identity_view(&mut engine, s, "a", &cols);
        let last = (0..readers).fold(a, |_, r| {
            try_register_view(&mut engine, distinct_circuit(a), &format!("d{r}"), &cols, 0, 0).unwrap()
        });
        // Compiles every plan outside the measurement.
        let warm = rows(&engine, s, 1, [0], cells);
        tick(&mut engine, &counter, s, warm);

        let ticks = wide_ticks(&mut engine, &counter, s, 1, true, cells);
        assert_eq!(
            net_weight(&engine, last) as u64,
            1 + WIDE_TICKS * WIDE_TICK_ROWS,
            "every row reached the last reader"
        );
        println!("readers {readers}: {ticks:>8.1} instr/row");
        discard(engine);
    }
}

/// The rows a [`left_join_engine`] view holds, and how many of them are
/// null-filled.
fn left_join_rows(engine: &mut CatalogEngine, view: u64) -> (u64, u64) {
    let out = scan_all(engine, view);
    let b_w = out.schema().num_payload_cols() - 1;
    let null_filled = (0..out.len())
        .filter(|&i| gnitz_wire::payload_is_null(&*out, i, b_w))
        .count();
    (out.len() as u64, null_filled as u64)
}

/// The wide ticks into the preserved side of `a LEFT JOIN b ON a.<key> = b.k`, by
/// `a`'s key column and by whether `b` holds one match for every row: the PK
/// reaches the join in order and a payload key scattered, a nullable key is
/// re-keyed once for the join and once for the null-fill, and a matched row is
/// found in `b`'s trace and again in its key set.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn left_join_view_tick_bench() {
    let counter = perf::Counter::instructions();
    let total = WIDE_TICKS * WIDE_TICK_ROWS;
    for (label, key, matched) in [
        ("PK key, no match", 0, false),
        ("NOT NULL payload key, no match", 1, false),
        ("nullable payload key, no match", 2, false),
        ("NOT NULL payload key, one match", 1, true),
    ] {
        let dir = temp_dir(&format!("left_join_view_tick_bench_{key}_{matched}"));
        let (mut engine, [a, b], v) = left_join_engine(&dir, key);
        if matched {
            let held = rows(&engine, b, 1, 0..total, |id| [scramble(id), id]);
            tick(&mut engine, &counter, b, held);
        }
        // Compiles the plan outside the measurement.
        seal_and_tick(&mut engine, a);

        let ticks = wide_ticks(&mut engine, &counter, a, 0, false, |id| [scramble(id); 2]);
        let (_, null_filled) = left_join_rows(&mut engine, v);
        assert_eq!(
            (net_weight(&engine, v) as u64, null_filled),
            (total, if matched { 0 } else { total }),
            "{label}"
        );
        println!("left join view, {label:<31}: {ticks:>6.1} instr/row");
        discard(engine);
    }
}

/// Rows each side of a [`left_join_backfill_bench`] holds.
const BACKFILL_ROWS: u64 = 200_000;

/// The backfill of `a LEFT JOIN b ON a.nn = b.k` over populated tables, by
/// whether `b` holds one match for every row of `a`. A backfill feeds the sources
/// in scan order, so the side fed first is joined against nothing.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn left_join_backfill_bench() {
    let counter = perf::Counter::instructions();
    for (label, matched) in [("every row matched", true), ("no row matched", false)] {
        let dir = temp_dir(&format!("left_join_backfill_bench_{matched}"));
        let (mut engine, [a, b], v) = left_join_engine(&dir, 1);
        let held = rows(&engine, a, 1, 0..BACKFILL_ROWS, |id| [scramble(id); 2]);
        engine.registry.ingest(a, held).unwrap();
        // A scramble is a bijection, so the keys past `a`'s ids match none of them.
        let first = if matched { 0 } else { BACKFILL_ROWS };
        let held = rows(&engine, b, 1, 0..BACKFILL_ROWS, |id| [scramble(first + id), id]);
        engine.registry.ingest(b, held).unwrap();

        let ((), fill) = counter.measure(|| backfill(&mut engine, v));
        let (held, null_filled) = left_join_rows(&mut engine, v);
        assert_eq!(
            (net_weight(&engine, v) as u64, held, null_filled),
            (BACKFILL_ROWS, BACKFILL_ROWS, if matched { 0 } else { BACKFILL_ROWS }),
            "{label}"
        );
        println!(
            "left join backfill, {label:<17}: {:>7.1} instr/final row",
            fill as f64 / BACKFILL_ROWS as f64
        );
        discard(engine);
    }
}

// ── One-row ticks ───────────────────────────────────────────────────────────

/// Distinct values of `k` in a [`one_row_ticks`] base.
const GROUPS: u64 = 100_000;
/// Timed one-row inserts per cell, and as many deletes.
const ONE_ROW_TICKS: u64 = 2_000;

fn group_cols() -> [CatalogColumn; 3] {
    [
        col_def("id", TypeCode::U64),
        col_def("k", TypeCode::U64),
        col_def("v", TypeCode::I64),
    ]
}

/// Row `id`'s `(k, v)`: `v` falls as `id` grows, so a row newer than every other
/// of its group is that group's least.
fn group_cells(id: u64) -> [u64; 2] {
    [scramble(id) % GROUPS, 2 * ROWS - id]
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

/// One-row ticks into the view `register` creates over `t (id, k, v)` of `ROWS`
/// rows: inserts of fresh ids, each the least `v` of its group, then deletes of
/// those rows. Run with every store of the engine in RAM, and spilled into few
/// and into many shards.
fn one_row_ticks(what: &str, register: impl Fn(&mut CatalogEngine, u64) -> u64) {
    let counter = perf::Counter::instructions();
    let d = StoreConfig::default();
    // A small scan chunk leaves the backfill's shards small, and so many.
    let many = StoreConfig {
        ram_tier_bytes: 32 << 10,
        scan_chunk_rows: 1_000,
        ..d
    };
    // The files of the arm before: none hold state in RAM, and each arm after
    // spills into more than the last.
    let mut fewer = None;
    for (label, config) in [
        ("in RAM", StoreConfig { ram_tier_bytes: 1 << 30, ..d }),
        ("few shards", StoreConfig { ram_tier_bytes: 256 << 10, ..d }),
        ("many shards", many),
    ] {
        let name = format!("view_tick_bench_{what}_{}", label.replace(' ', "_"));
        let mut engine = CatalogEngine::open_with(&temp_dir(&name), 1, config).unwrap();
        let t = engine.create_table("public.t", &group_cols(), &[0]).unwrap();
        let base = rows(&engine, t, 1, 0..ROWS, group_cells);
        engine.registry.ingest(t, base).unwrap();

        let v = register(&mut engine, t);
        backfill(&mut engine, v);
        let files = files_under(&relation_dir(engine.registry.base_dir(), v));
        match fewer.replace(files) {
            None => assert_eq!(files, 0, "{what}, {label}: the view's state spilled"),
            Some(fewer) => assert!(files > fewer, "{what}, {label}: {files} files after {fewer}"),
        }
        let held = net_weight(&engine, v);

        let [inserts, deletes] = [1, -1].map(|weight| {
            let instructions: u64 = (ROWS..ROWS + ONE_ROW_TICKS)
                .map(|id| {
                    let row = rows(&engine, t, weight, [id], group_cells);
                    tick(&mut engine, &counter, t, row)
                })
                .sum();
            instructions / ONE_ROW_TICKS
        });
        assert_eq!(net_weight(&engine, v), held, "the deletes undid the inserts");
        println!(
            "{what} view, state {label:<11} ({files:>4} files): insert {inserts:>7} instr/tick, delete {deletes:>7} instr/tick",
        );
        discard(engine);
    }
}

/// `SELECT * FROM t`: the tick and the ingest of its one row with no operator
/// state under them, the floor of every cell beside it.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn identity_view_tick_bench() {
    one_row_ticks("identity", |engine, t| {
        register_identity_view(engine, t, "v", &group_cols())
    });
}

/// `SELECT k, COUNT(*), MIN(v) FROM t GROUP BY k`: an insert lowers its group's
/// minimum, and the delete of it reads the next one back out of the value index.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn minmax_view_tick_bench() {
    one_row_ticks("MIN", |engine, t| {
        let (group, aggs) = (
            [1u32],
            [
                AggDescriptor::COUNT_STAR,
                AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
            ],
        );
        let mut circuit = Circuit::default();
        let scan = circuit.input_delta(t, ReadBound::None);
        circuit.reduce_multi(scan, &group, &aggs);
        let schema = engine.registry.relation(t).map(Relation::schema).unwrap();
        let out = *gnitz_zset::stream::ReducePlan::from_wire(&schema, &group, &aggs, false)
            .unwrap()
            .output_schema();
        try_register_view(engine, circuit, "v", &cols_of(&out), 0, 0).unwrap()
    });
}

/// The three rows of each `k` least in `v`: an insert enters its group's window
/// and pushes a row out, and the delete of it lets that row back in.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn topn_view_tick_bench() {
    one_row_ticks("top-3", |engine, t| {
        let (group, order) = (
            [1u32],
            [gnitz_wire::OrderKey { col: 2, desc: false, nulls_first: false }],
        );
        let mut circuit = Circuit::default();
        let scan = circuit.input_delta(t, ReadBound::None);
        circuit.top_n(scan, &group, &order, 3, 0);
        let schema = engine.registry.relation(t).map(Relation::schema).unwrap();
        let out = gnitz_zset::stream::TopNPlan::from_wire(&schema, &group, &order, 3, 0)
            .unwrap()
            .output_schema;
        try_register_view(engine, circuit, "v", &cols_of(&out), 0, 0).unwrap()
    });
}

/// `SELECT DISTINCT * FROM t`: every tick moves one row across the positive
/// boundary, found by a point lookup into the integral.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn distinct_view_tick_bench() {
    one_row_ticks("distinct", |engine, t| {
        try_register_view(engine, distinct_circuit(t), "v", &group_cols(), 0, 0).unwrap()
    });
}

/// `t JOIN u ON t.k = u.k`, `u` one row per group: every tick probes `u`'s trace
/// for its one match.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn join_view_tick_bench() {
    one_row_ticks("join", |engine, t| {
        let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
        let u = engine.create_table("public.u", &cols, &[0]).unwrap();
        let groups = rows(engine, u, 1, 0..GROUPS, |k| [k]);
        engine.registry.ingest(u, groups).unwrap();
        let out = [
            col_def("k", TypeCode::U64),
            col_def("t_id", TypeCode::U64),
            col_def("u_id", TypeCode::U64),
        ];
        try_register_view(engine, two_term_join_circuit(t, u, TypeCode::U64), "v", &out, 0, 0).unwrap()
    });
}

/// Wide ticks through a chain segment into the next one: `t JOIN u ON t.k =
/// u.k`, two rows of `u` a key, registered as a segment, under `SELECT k,
/// SUM(t_id), COUNT(*) … GROUP BY k`. The segment stores nothing on a tick, its
/// delta leaves the join unfolded, and the reduce reads it at raw weights.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn segment_view_tick_bench() {
    const KEYS: u64 = 1_000;
    let counter = perf::Counter::instructions();
    let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
    let mut engine = CatalogEngine::open(&temp_dir("segment_view_tick_bench"), 1).unwrap();
    let t = engine.create_table("public.t", &cols, &[0]).unwrap();
    let u = engine.create_table("public.u", &cols, &[0]).unwrap();
    let matches = rows(&engine, u, 1, 0..2 * KEYS, |id| [id % KEYS]);
    engine.registry.ingest(u, matches).unwrap();

    // A segment is a view whose row names an owner, a user view created with it.
    let segment = engine.allocate_ids(2).unwrap();
    let view = segment + 1;
    let joined = [
        col_def("k", TypeCode::U64),
        col_def("t_id", TypeCode::U64),
        col_def("u_id", TypeCode::U64),
    ];
    let joined_schema = gnitz_zset::schema::SchemaDescriptor::new(&[u64c(); 3], &[0]);
    let aggs = [
        AggDescriptor { agg_op: AggFunc::Sum, col_idx: 1 },
        AggDescriptor::COUNT_STAR,
    ];
    let reduced = *gnitz_zset::stream::ReducePlan::from_wire(&joined_schema, &[0], &aggs, false)
        .unwrap()
        .output_schema();
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(segment, ReadBound::None);
    circuit.reduce_multi_local(scan, &[0], &aggs);
    let mut bb = BatchBuilder::new(crate::catalog::SysFamily::View.schema());
    for (vid, name, circuit, cols, owner) in [
        (
            segment,
            "segment",
            two_term_join_circuit(t, u, TypeCode::U64),
            &joined[..],
            view,
        ),
        (view, "reduced", circuit, &cols_of(&reduced), 0),
    ] {
        crate::test_support::write_circuit(&mut engine, vid, circuit);
        engine.write_column_records(vid, cols).unwrap();
        crate::test_support::push_view_tab_row(&mut bb, 1, vid, name, 0, 0, owner);
    }
    engine.submit(crate::catalog::SysFamily::View, bb.finish()).unwrap();
    backfill(&mut engine, segment);
    backfill(&mut engine, view);

    let ticks = wide_ticks(&mut engine, &counter, t, 0, false, |id| [scramble(id) % KEYS]);
    assert_eq!(net_weight(&engine, segment), 0, "the segment stored no tick");
    assert_eq!(net_weight(&engine, view) as u64, KEYS, "a group a key");
    println!("segment into a reduce: {WIDE_TICK_ROWS}-row ticks {ticks:>6.1} instr/row");
    discard(engine);
}
