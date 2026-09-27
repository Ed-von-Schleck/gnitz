//! Release benchmark: peak resident bytes and instructions of reads that
//! hydrate a capacity-bounded view's skeleton rows.
//!
//! ```text
//! cd crates && cargo test -p gnitz-server --release hydrate_ \
//!     -- --ignored --nocapture --test-threads=1
//! ```

use std::hint::black_box;

use super::*;
use crate::query::{DagEngine, Drive};
use gnitz_foundation::perf;
use gnitz_store::read::SkeletonHydrator;
use gnitz_wire::{KeyRange, PkColList, ReadBound, ReadSpec};

/// Base rows, and so view rows.
const ROWS: u64 = 1_000_000;
/// Incompressible payload columns, so a skeleton row is a fraction of a full one.
const PAYLOAD_COLS: u64 = 4;
/// The RAM tier every store of the fixture spills past.
const RAM_TIER_BYTES: u64 = 1 << 20;

/// The engine's own hydrator, counting the rows it recomputes.
struct Counting<'a> {
    dag: &'a mut DagEngine,
    hydrated: usize,
}

impl SkeletonHydrator for Counting<'_> {
    fn hydrate_keys(&mut self, registry: &RelationRegistry, view_id: i64, keys: Vec<u8>) -> Result<Batch, StoreError> {
        let out = self.dag.hydrate_keys(registry, view_id, keys)?;
        self.hydrated += out.len();
        Ok(out)
    }
}

/// An identity view over a `ROWS`-row base, bounded at `capacity` bytes and
/// checkpointed, so the sweep has skeletonized it. Returns the engine, the base and
/// the view.
fn bounded_fixture(name: &str, capacity: u64) -> (CatalogEngine, i64, i64) {
    let mut cols = vec![col_def("id", TypeCode::U64)];
    cols.extend((0..PAYLOAD_COLS).map(|c| col_def(&format!("v{c}"), TypeCode::I64)));
    std::env::set_var("GNITZ_RAM_TIER_BYTES", RAM_TIER_BYTES.to_string());
    let (mut engine, base) = ingest_fixture(name, &cols, ROWS, 1, |bb, id| {
        for c in 0..PAYLOAD_COLS {
            bb.put_u64(scramble(id ^ c));
        }
    });
    std::env::remove_var("GNITZ_RAM_TIER_BYTES");

    let circuit = crate::test_support::identity_circuit(base, ReadBound::None);
    let view = try_register_view(&mut engine, circuit, "bounded", &cols, capacity, 0).unwrap();
    backfill(&mut engine, view, &[base]);
    checkpoint(&mut engine);
    (engine, base, view)
}

/// Checkpoint every store, so the sweep has skeletonized every bounded view.
fn checkpoint(engine: &mut CatalogEngine) {
    engine.record_topology(1).unwrap();
    let g = engine.bump_checkpoint_generation().unwrap();
    engine.flush_ephemeral_round(g).unwrap();
}

/// One measured read of `engine`.
fn cell(label: &str, engine: &mut CatalogEngine, read: impl Fn(&RelationRegistry, &mut Counting) -> usize) {
    let counter = perf::Counter::instructions().expect("instructions counter");
    black_box(read(
        &engine.registry,
        &mut Counting { dag: &mut engine.dag, hydrated: 0 },
    ));

    let mut hydrator = Counting { dag: &mut engine.dag, hydrated: 0 };
    perf::reset_peak_rss();
    let before = perf::rss_bytes();
    let (rows, instructions) = counter.measure(|| read(&engine.registry, &mut hydrator));
    let peak = perf::peak_rss_bytes().saturating_sub(before);
    let hydrated = hydrator.hydrated;
    assert!(
        hydrated > 0,
        "{label}: the read hydrated nothing, so it measured no hydration"
    );
    println!(
        "{label:<30} rows {rows:>8}  hydrated {hydrated:>8}  peak +{:>7.1} MiB  {:>12} instr",
        peak as f64 / (1 << 20) as f64,
        instructions,
    );
}

/// The whole-relation scan, at the capacities that skeletonize this fixture
/// nearly entirely and about half.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hydrate_full_scan_bench() {
    for (label, capacity) in [("full scan, L ≈ 0", 64 << 10), ("full scan, L ≈ H", 24 << 20)] {
        let (mut engine, _, view) = bounded_fixture("hydrate_full_scan", capacity);
        let schema = engine.registry.relation(view).map(Relation::schema).unwrap();
        cell(label, &mut engine, |registry, h| {
            let spec = ReadSpec::all_rows(ReadBound::None);
            registry
                .scan_spec(view, spec, schema.layout_digest(), Some(h))
                .unwrap()
                .len()
        });
        engine.close();
    }
}

/// Ids upserted into the base without a tick, centred on the sought key.
const UNTICKED_IDS: u64 = 10_000;

/// One key through `scan_spec`: a single hydration chunk, then the same seek with
/// `UNTICKED_IDS` upserts of the base awaiting the view's next tick.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hydrate_seek_bench() {
    let (mut engine, base, view) = bounded_fixture("hydrate_seek", 64 << 10);
    let schema = engine.registry.relation(view).map(Relation::schema).unwrap();
    let key = ROWS / 2;
    let spec = ReadSpec::all_rows(ReadBound::Range(KeyRange::point(
        PkColList::from_slice(schema.pk_indices()),
        &[],
        key as u128,
    )));
    let seek = |registry: &RelationRegistry, h: &mut Counting| {
        let out = registry
            .scan_spec(view, spec.clone(), schema.layout_digest(), Some(h))
            .unwrap();
        assert_eq!(out.len(), 1, "one row at the sought key");
        let v0 = u64::from_le_bytes(out.get_col_ptr(0, 0, 8).try_into().unwrap());
        assert_eq!(v0, scramble(key), "the payload the view last ticked over");
        out.len()
    };
    cell("single-key seek", &mut engine, seek);

    // Each upsert takes effect as a retraction and an insert.
    let mut bb = BatchBuilder::new(engine.registry.relation(base).map(Relation::schema).unwrap());
    for id in key - UNTICKED_IDS / 2..key + UNTICKED_IDS / 2 {
        bb.begin_row(id as u128, 1);
        for c in 0..PAYLOAD_COLS {
            bb.put_u64(scramble(id ^ c).wrapping_add(1));
        }
        bb.end_row();
    }
    engine.ingest_unticked(base, bb.finish()).unwrap();
    let label = format!("single-key seek, {}k unticked", 2 * UNTICKED_IDS / 1000);
    cell(&label, &mut engine, seek);
    engine.close();
}

/// A filtered `LIMIT 1` whose one match sits at merge position `M`: the rows it
/// hydrates are the rows its drain reads to find it.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hydrate_filtered_limit_bench() {
    const M: u64 = 1000;
    let (mut engine, _, view) = bounded_fixture("hydrate_filtered_limit", 64 << 10);
    let schema = engine.registry.relation(view).map(Relation::schema).unwrap();
    let spec = ReadSpec {
        bound: ReadBound::None,
        predicate: pred_cmp_blob(gnitz_expr::CmpOp::Eq, 1, scramble(M) as i64),
        sink: gnitz_wire::ReadSink {
            map: None,
            kind: gnitz_wire::SinkKind::Rows { order: Vec::new(), limit_k: 1 },
        },
    };
    cell("filtered LIMIT 1", &mut engine, |registry, h| {
        let rows = registry
            .scan_spec(view, spec.clone(), schema.layout_digest(), Some(h))
            .unwrap()
            .len();
        assert_eq!(rows, 1, "the predicate matches one row");
        rows
    });
    engine.close();
}

// ── Trace probes ────────────────────────────────────────────────────────────

/// A `[id, k]` base of `ROWS` rows, `k` a bijective scramble of `id`, so two such
/// bases join one-to-one on `k`.
fn join_base(engine: &mut CatalogEngine, name: &str) -> i64 {
    let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
    let tid = engine.create_table(&format!("public.{name}"), &cols, &[0]).unwrap();
    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for id in 0..ROWS {
        bb.begin_row(id as u128, 1);
        bb.put_u64(scramble(id));
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    tid
}

fn scramble(id: u64) -> u64 {
    id.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// A bounded inner equi-join over two `ROWS`-row bases, and a `distinct` view over a
/// third, every store spilled under the 1 MiB RAM tier and checkpointed.
struct ProbeFixture {
    engine: CatalogEngine,
    dir: String,
    join: i64,
    join_bases: [i64; 2],
    distinct_base: i64,
}

fn probe_fixture() -> ProbeFixture {
    std::env::set_var("GNITZ_RAM_TIER_BYTES", RAM_TIER_BYTES.to_string());
    let dir = temp_dir("hydrate_trace_probe");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    std::env::remove_var("GNITZ_RAM_TIER_BYTES");

    let join_bases = [join_base(&mut engine, "a"), join_base(&mut engine, "b")];
    let join_cols = [
        col_def("k", TypeCode::U64),
        col_def("a_id", TypeCode::U64),
        col_def("b_id", TypeCode::U64),
    ];
    let circuit = crate::test_support::two_term_join_circuit(join_bases[0], join_bases[1]);
    let join = try_register_view(&mut engine, circuit, "bounded_join", &join_cols, 64 << 10, 0).unwrap();
    backfill(&mut engine, join, &join_bases);

    let distinct_base = join_base(&mut engine, "c");
    let mut circuit = gnitz_wire::Circuit::default();
    let scan = circuit.input_delta(distinct_base as u64, ReadBound::None);
    let distinct = circuit.distinct(scan);
    circuit.sink(distinct);
    let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
    let view = try_register_view(&mut engine, circuit, "distinct", &cols, 0, 0).unwrap();
    backfill(&mut engine, view, &[distinct_base]);

    checkpoint(&mut engine);
    ProbeFixture {
        engine,
        dir,
        join,
        join_bases,
        distinct_base,
    }
}

/// Shard files under `dir`, per directory that holds any: the regime check that
/// every trace was measured on disk.
fn print_shards(dir: &str) {
    let mut stack = vec![std::path::PathBuf::from(dir)];
    while let Some(d) = stack.pop() {
        let mut shards = 0;
        for e in fs::read_dir(&d).unwrap().flatten() {
            let path = e.path();
            if path.is_dir() {
                stack.push(path);
            } else if e.file_name().to_string_lossy().starts_with("shard_") {
                shards += 1;
            }
        }
        if shards > 0 {
            println!("  {shards:>4} shard(s) in {}", d.display());
        }
    }
}

/// One tick of `base` over a one-row push of a fresh `id`, keyed onto an existing
/// `k` so every probe finds a match; its instructions, ingest excluded.
fn push_epoch(engine: &mut CatalogEngine, base: i64, id: u64) -> u64 {
    let schema = engine.registry.relation(base).map(Relation::schema).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(id as u128, 1);
    bb.put_u64(scramble(id - ROWS));
    bb.end_row();
    let effective = engine.registry.ingest_returning(base, bb.finish()).unwrap();
    let counter = perf::Counter::instructions().expect("instructions counter");
    let what = Drive::Tick { source: base, round: id };
    let (_, instructions) = counter.measure(|| crate::query::drive(&mut LocalDrive(engine), what, effective).unwrap());
    instructions
}

/// Trace probes: a point seek hydrating the bounded join, and one-row push epochs
/// into a join base and into the distinct view's base.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hydrate_trace_probe_bench() {
    let ProbeFixture {
        mut engine,
        dir,
        join,
        join_bases,
        distinct_base,
    } = probe_fixture();
    print_shards(&dir);

    let schema = engine.registry.relation(join).map(Relation::schema).unwrap();
    let spec = ReadSpec::all_rows(ReadBound::Range(KeyRange::point(
        PkColList::from_slice(schema.pk_indices()),
        &[],
        scramble(ROWS / 2) as u128,
    )));
    cell("join point seek", &mut engine, |registry, h| {
        registry
            .scan_spec(join, spec.clone(), schema.layout_digest(), Some(h))
            .unwrap()
            .len()
    });

    for (label, base) in [
        ("join push epoch", join_bases[0]),
        ("distinct push epoch", distinct_base),
    ] {
        push_epoch(&mut engine, base, ROWS);
        let instructions = push_epoch(&mut engine, base, ROWS + 1);
        println!("{label:<20} {instructions:>12} instr");
    }
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
