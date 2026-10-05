//! Release benchmark: peak resident bytes and instructions of reads that
//! hydrate a capacity-bounded view's skeleton rows.
//!
//! ```text
//! cd crates && cargo test -p gnitz --release hydrate_bench \
//!     -- --ignored --nocapture --test-threads=1
//! ```

use gnitz_expr::{ColumnTable, SchemaFacts};
use std::hint::black_box;
use std::rc::Rc;

use super::*;
use crate::query::DagEngine;
use gnitz_foundation::perf;
use gnitz_store::read::SkeletonHydrator;
use gnitz_wire::{KeyRange, PkColList, ReadBound, ReadSpec};

/// Base rows, and so view rows.
const ROWS: u64 = 1_000_000;

/// Row `id`'s payload: incompressible columns, so a skeleton row is a fraction
/// of a full one.
fn payload(id: u64) -> [u64; 4] {
    std::array::from_fn(|c| scramble(id ^ c as u64))
}

/// An empty engine whose every store spills past a small RAM tier.
fn spilling_engine(name: &str) -> CatalogEngine {
    let config = StoreConfig {
        ram_tier_bytes: 1 << 20,
        ..Default::default()
    };
    CatalogEngine::open_with(&temp_dir(name), 1, config).unwrap()
}

/// An identity view over a `ROWS`-row base, bounded at `capacity` bytes and
/// swept. Returns the engine, the base and the view.
fn bounded_fixture(name: &str, capacity: u64) -> (CatalogEngine, u64, u64) {
    let mut cols = vec![col_def("id", TypeCode::U64)];
    cols.extend((0..payload(0).len()).map(|c| col_def(&format!("v{c}"), TypeCode::I64)));
    let mut engine = spilling_engine(name);
    let base = engine.create_table("public.t", &cols, &[0]).unwrap();
    let loaded = rows(&engine, base, 1, 0..ROWS, payload);
    engine.registry.ingest(base, loaded).unwrap();

    let view = try_register_identity_view(&mut engine, base, "bounded", &cols, capacity, 0).unwrap();
    backfill(&mut engine, view, &[base]);
    sweep(&mut engine);
    (engine, base, view)
}

/// Run the ephemeral checkpoint round, whose sweep skeletonizes every bounded
/// view past its capacity.
fn sweep(engine: &mut CatalogEngine) {
    engine.flush_ephemeral_round(1).unwrap();
}

/// The read of every row of `view` at PK `key`.
fn point(engine: &CatalogEngine, view: u64, key: u64) -> ReadSpec {
    let schema = engine.registry.relation(view).map(Relation::schema).unwrap();
    let pk = PkColList::from_slice(schema.pk_cols());
    ReadSpec::all_rows(ReadBound::Range(KeyRange::point(pk, &[], key as u128)))
}

/// The engine's own hydrator, counting the rows it recomputes.
struct Counting<'a> {
    dag: &'a mut DagEngine,
    hydrated: usize,
}

impl SkeletonHydrator for Counting<'_> {
    fn hydrate_keys(
        &mut self,
        registry: &RelationRegistry,
        view_id: u64,
        keys: gnitz_wire::PkKeys,
    ) -> Result<Batch, String> {
        let out = self.dag.hydrate_keys(registry, view_id, keys)?;
        self.hydrated += out.len();
        Ok(out)
    }
}

/// One measured read of `view` under `spec`, after an unmeasured one of the same
/// that leaves the shards it touches resident. Returns its rows and how many it
/// hydrated.
fn cell(label: &str, engine: &mut CatalogEngine, view: u64, spec: &ReadSpec) -> (Rc<Batch>, usize) {
    let counter = perf::Counter::instructions();
    let registry = &engine.registry;
    let digest = registry.relation(view).map(Relation::schema).unwrap().layout_digest();
    let read = |hydrator: &mut Counting| registry.scan_spec(view, spec.clone(), digest, Some(hydrator)).unwrap();
    black_box(read(&mut Counting { dag: &mut engine.dag, hydrated: 0 }));

    let mut hydrator = Counting { dag: &mut engine.dag, hydrated: 0 };
    let before = perf::rss_bytes();
    perf::reset_peak_rss();
    let (rows, instructions) = counter.measure(|| read(&mut hydrator));
    let peak = perf::peak_rss_bytes().saturating_sub(before);
    let hydrated = hydrator.hydrated;
    assert!(
        hydrated > 0,
        "{label}: the read hydrated nothing, so it measured no hydration"
    );
    println!(
        "{label:<30} rows {:>8}  hydrated {hydrated:>8}  peak +{:>7.1} MiB  {instructions:>12} instr",
        rows.len(),
        peak as f64 / (1 << 20) as f64,
    );
    (rows, hydrated)
}

/// The whole-relation scan, at a capacity that skeletonizes this fixture nearly
/// entirely and at one that leaves part of it resident.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hydrate_full_scan_bench() {
    let all = ReadSpec::all_rows(ReadBound::None);
    let mut hydrated = [64 << 10, 24 << 20].map(|capacity: u64| {
        let (mut engine, _, view) = bounded_fixture("hydrate_full_scan", capacity);
        let label = format!("full scan, capacity {} KiB", capacity >> 10);
        let (rows, hydrated) = cell(&label, &mut engine, view, &all);
        assert_eq!(rows.len() as u64, ROWS, "{label}: every row read");
        discard(engine);
        hydrated as u64
    });
    hydrated.reverse();
    assert!(
        hydrated[0] < hydrated[1] && hydrated[1] > ROWS * 9 / 10,
        "the small capacity hydrates nearly every row and the large one fewer: {hydrated:?}"
    );
}

/// Ids upserted into the base without a tick, centred on the sought key.
const UNTICKED_IDS: u64 = 10_000;

/// One key through `scan_spec`: a single hydration chunk, then the same seek with
/// `UNTICKED_IDS` upserts of the base awaiting the view's next tick.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hydrate_seek_bench() {
    let (mut engine, base, view) = bounded_fixture("hydrate_seek", 64 << 10);
    let key = ROWS / 2;
    let spec = point(&engine, view, key);
    let seek = |label: &str, engine: &mut CatalogEngine| {
        let (rows, _) = cell(label, engine, view, &spec);
        assert_eq!(rows.len(), 1, "one row at the sought key");
        assert_eq!(
            gnitz_wire::payload_u64(&*rows, 0, 0),
            payload(key)[0],
            "the payload the view last ticked over"
        );
    };
    seek("single-key seek", &mut engine);

    // Each upsert takes effect as a retraction and an insert.
    let ids = key - UNTICKED_IDS / 2..key + UNTICKED_IDS / 2;
    let upserts = rows(&engine, base, 1, ids, |id| payload(id).map(|c| c.wrapping_add(1)));
    engine.ingest_unticked(base, upserts).unwrap();
    seek("single-key seek, unticked base", &mut engine);
    discard(engine);
}

/// A filtered `LIMIT 1` whose one match sits at merge position `M`: the rows it
/// hydrates are the rows its drain reads to find it.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hydrate_filtered_limit_bench() {
    const M: u64 = 1000;
    let (mut engine, _, view) = bounded_fixture("hydrate_filtered_limit", 64 << 10);
    let spec = ReadSpec {
        bound: ReadBound::None,
        predicate: cmp_const(gnitz_expr::CmpOp::Eq, 1, payload(M)[0] as i64).to_blob_bytes(),
        sink: gnitz_wire::ReadSink {
            map: None,
            kind: gnitz_wire::SinkKind::Rows {
                cut: Some(gnitz_wire::RowsCut {
                    k: std::num::NonZeroU64::MIN,
                    order: Vec::new(),
                }),
            },
        },
    };
    let (rows, hydrated) = cell("filtered LIMIT 1", &mut engine, view, &spec);
    assert_eq!(rows.len(), 1, "the predicate matches one row");
    assert!(
        (hydrated as u64) < ROWS / 10,
        "the drain stopped at its match: hydrated {hydrated}"
    );
    discard(engine);
}

/// One key of a bounded inner equi-join over two `ROWS`-row bases, which
/// hydrates from the join's own operator traces.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hydrate_join_seek_bench() {
    let mut engine = spilling_engine("hydrate_join_seek");
    // `k` is a bijection of `id`, so the two bases join one-to-one.
    let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
    let bases = ["public.a", "public.b"].map(|name| {
        let t = engine.create_table(name, &cols, &[0]).unwrap();
        let loaded = rows(&engine, t, 1, 0..ROWS, |id| [scramble(id)]);
        engine.registry.ingest(t, loaded).unwrap();
        t
    });
    let join_cols = [
        col_def("k", TypeCode::U64),
        col_def("a_id", TypeCode::U64),
        col_def("b_id", TypeCode::U64),
    ];
    let circuit = two_term_join_circuit(bases[0], bases[1], TypeCode::U64);
    let join = try_register_view(&mut engine, circuit, "bounded_join", &join_cols, 64 << 10, 0).unwrap();
    backfill(&mut engine, join, &bases);
    sweep(&mut engine);
    // Shows where the traces the seek reads sit.
    print!(
        "{}",
        gnitz_store::relation::disk_usage(engine.registry.base_dir()).unwrap()
    );

    let spec = point(&engine, join, scramble(ROWS / 2));
    let (rows, _) = cell("join point seek", &mut engine, join, &spec);
    assert_eq!(rows.len(), 1, "one match at the sought key");
    discard(engine);
}
