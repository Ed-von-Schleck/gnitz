//! Release benchmark: peak resident bytes and instructions of reads that
//! hydrate a capacity-bounded view's skeleton rows.
//!
//! ```text
//! cd crates && cargo test -p gnitz-server --release hydrate_bench \
//!     -- --ignored --nocapture --test-threads=1
//! ```

use gnitz_store::read::ReadRows;
use gnitz_store::relation::PkRule;
use std::hint::black_box;

use super::*;
use gnitz_foundation::perf;
use gnitz_wire::{KeyRange, PkColList, PkKeys, ReadBound, ReadSpec};

/// Base rows, and so view rows.
const ROWS: u64 = 1_000_000;

/// Row `id`'s payload: incompressible columns, so a skeleton row is a fraction
/// of a full one.
fn payload(id: u64) -> [u64; 4] {
    std::array::from_fn(|c| mix(id ^ c as u64))
}

/// An empty engine whose every store spills past a small RAM tier.
fn spilling_engine() -> (tempfile::TempDir, CatalogEngine) {
    let config = StoreConfig {
        ram_tier_bytes: 1 << 20,
        ..Default::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = CatalogEngine::open_with(dir.path().to_str().unwrap(), 1, config).unwrap();
    (dir, engine)
}

/// An identity view over a `ROWS`-row base, bounded at `capacity` bytes and
/// swept. Returns the engine's directory guard, the engine, the base and the view.
fn bounded_fixture(capacity: u64) -> (tempfile::TempDir, CatalogEngine, u64, u64) {
    let mut cols = vec![col_def("id", TypeCode::U64)];
    cols.extend((0..payload(0).len()).map(|c| col_def(&format!("v{c}"), TypeCode::I64)));
    let (dir, mut engine) = spilling_engine();
    let base = engine.create_table("public.t", &cols, &[0]).unwrap();
    let loaded = rows(&engine, base, 1, 0..ROWS, payload);
    engine.registry.ingest(base, loaded).unwrap();

    let view = try_register_identity_view(&mut engine, base, "bounded", &cols, capacity, 0).unwrap();
    backfill(&mut engine, view);
    sweep(&mut engine);
    (dir, engine, base, view)
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
    ReadSpec::all_rows(ReadBound::Range(KeyRange::point(pk, &[key as u128])))
}

/// One measured read of `view` under `spec`, after an unmeasured one of the same
/// that leaves the shards it touches resident. Returns its rows and how many it
/// hydrated.
fn cell(label: &str, engine: &mut CatalogEngine, view: u64, spec: &ReadSpec) -> (ReadRows, usize) {
    let counter = perf::Counter::instructions();
    let (registry, dag) = (&engine.registry, &mut engine.dag);
    let digest = registry.relation(view).map(Relation::schema).unwrap().layout_digest();
    // The engine's own hydration, counting the rows it recomputes.
    let mut hydrated = 0;
    let mut read = |count: &mut usize| {
        let mut hydrate = |k: PkKeys| {
            let out = dag.hydrate_keys(registry, view, k)?;
            *count += out.len();
            Ok(out)
        };
        registry
            .scan_spec(view, spec.clone(), digest, Some(&mut hydrate))
            .unwrap()
    };
    black_box(read(&mut 0));

    let resident = perf::Resident::baseline();
    let (rows, instructions) = counter.measure(|| read(&mut hydrated));
    let peak = match resident {
        Some(r) => format!("+{:>7.1} MiB", r.peak_added() as f64 / (1 << 20) as f64),
        None => format!("n/a without {}", perf::PIN_MMAP_THRESHOLD),
    };
    assert!(
        hydrated > 0,
        "{label}: the read hydrated nothing, so it measured no hydration"
    );
    println!(
        "{label:<30} rows {:>8}  hydrated {hydrated:>8}  peak {peak}  {instructions:>12} instr",
        rows.len(),
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
        let (_dir, mut engine, _, view) = bounded_fixture(capacity);
        let label = format!("full scan, capacity {} KiB", capacity >> 10);
        let (rows, hydrated) = cell(&label, &mut engine, view, &all);
        assert_eq!(rows.len() as u64, ROWS, "{label}: every row read");
        drop(engine);
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
    let (_dir, mut engine, base, view) = bounded_fixture(64 << 10);
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
    engine.ingest_unticked(base, upserts, PkRule::Enforce).unwrap();
    seek("single-key seek, unticked base", &mut engine);
}

/// A filtered `LIMIT 1` whose one match sits at merge position `M`: the rows it
/// hydrates are the rows its drain reads to find it.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hydrate_filtered_limit_bench() {
    const M: u64 = 1000;
    let (_dir, mut engine, _, view) = bounded_fixture(64 << 10);
    let spec = ReadSpec {
        bound: ReadBound::None,
        predicate: cmp_all(&[(gnitz_expr::CmpOp::Eq, 1, payload(M)[0] as i64)]).to_blob_bytes(),
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
}

/// One key of a bounded inner equi-join over two `ROWS`-row bases, which
/// hydrates from the join's own operator traces.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hydrate_join_seek_bench() {
    let (_dir, mut engine) = spilling_engine();
    // `k` is a bijection of `id`, so the two bases join one-to-one.
    let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
    let bases = ["public.a", "public.b"].map(|name| {
        let t = engine.create_table(name, &cols, &[0]).unwrap();
        let loaded = rows(&engine, t, 1, 0..ROWS, |id| [mix(id)]);
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
    backfill(&mut engine, join);
    sweep(&mut engine);
    // Shows where the traces the seek reads sit.
    print!(
        "{}",
        gnitz_store::relation::disk_usage(engine.registry.base_dir()).unwrap()
    );

    let spec = point(&engine, join, mix(ROWS / 2));
    let (rows, _) = cell("join point seek", &mut engine, join, &spec);
    assert_eq!(rows.len(), 1, "one match at the sought key");
}
