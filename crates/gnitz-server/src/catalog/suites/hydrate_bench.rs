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
use gnitz_wire::{Cut, RangeDescriptor, ReadBound, ReadSpec};

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
/// checkpointed, so the sweep has skeletonized it.
fn bounded_fixture(name: &str, capacity: u64) -> (CatalogEngine, i64) {
    let mut cols = vec![col_def("id", type_code::U64)];
    cols.extend((0..PAYLOAD_COLS).map(|c| col_def(&format!("v{c}"), type_code::I64)));
    std::env::set_var("GNITZ_RAM_TIER_BYTES", RAM_TIER_BYTES.to_string());
    let (mut engine, base) = ingest_fixture(name, &cols, ROWS, 1, |bb, id| {
        for c in 0..PAYLOAD_COLS {
            bb.put_u64((id ^ c).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        }
    });
    std::env::remove_var("GNITZ_RAM_TIER_BYTES");

    let circuit = crate::test_support::identity_circuit(base, ReadBound::None);
    let view = try_register_view(&mut engine, circuit, "bounded", &cols, capacity, 0).unwrap();
    engine.dag.open_plan(&engine.registry, view).unwrap();
    let chunk_rows = engine.registry.scan_chunk_rows();
    let mut source = engine.open_source_cursor(view, base).unwrap();
    while let Some(chunk) = source.drain_chunk(chunk_rows) {
        let what = Drive::Backfill { view, source: base };
        crate::query::drive(&mut LocalDrive(&mut engine), what, chunk).unwrap();
    }
    engine.dag.finish_backfill(&mut engine.registry, view).unwrap();
    engine.record_topology(1).unwrap();
    let g = engine.bump_checkpoint_generation().unwrap();
    engine.flush_ephemeral_round(g).unwrap();
    (engine, view)
}

/// One measured read of `engine`.
fn cell(label: &str, engine: &mut CatalogEngine, read: impl Fn(&RelationRegistry, &mut Counting) -> usize) {
    let counter = perf::Instructions::open().expect("instructions counter");
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
        "{label:<20} live {:>8}  hydrated {hydrated:>8}  peak +{:>7.1} MiB  {:>12} instr",
        rows - hydrated,
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
        let (mut engine, view) = bounded_fixture("hydrate_full_scan", capacity);
        cell(label, &mut engine, |registry, h| {
            registry.scan(view, Some(h)).unwrap().len()
        });
        engine.close();
    }
}

/// One key through `scan_spec`: a single hydration chunk.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn hydrate_seek_bench() {
    let (mut engine, view) = bounded_fixture("hydrate_seek", 64 << 10);
    let schema = engine.registry.relation(view).map(Relation::schema).unwrap();
    let key = (ROWS / 2) as u128;
    let spec = ReadSpec::all_rows(ReadBound::PkRange(RangeDescriptor::new(
        &[],
        Cut::Before(key),
        Cut::After(key),
    )));
    cell("single-key seek", &mut engine, |registry, h| {
        registry.scan_spec(view, spec.clone(), &schema, Some(h)).unwrap().len()
    });
    engine.close();
}
