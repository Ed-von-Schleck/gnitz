//! In-crate microbenchmarks for the merge / cursor / cogroup hot path.
//!
//! The crate is a binary (no `lib` target) and pulls in no `criterion`, so these
//! live as `#[ignore]`d tests and time with `Instant`. They exist to *decide*
//! the cursor-rework rungs, not to ship — each pairs the current path against a
//! reference floor so the achievable headroom is visible before anything is
//! built. Run (release is mandatory for meaningful numbers):
//!
//! ```text
//! cargo test -p gnitz-engine --release bench_ -- --ignored --nocapture
//! ```
//!
//! - `bench_merge_scan_by_k` — scan ns/row vs source count `k`. The k=1→k=2 gap
//!   is the loser-tree's fixed cost = the upside of a heap-free `Pair` bypass.
//! - `bench_merge_scan_pk_width` — same, U64 vs U128 PK = width sensitivity.
//! - `bench_advance_to_vs_seek` — forward `advance_to` (in-place gallop) vs
//!   `seek_bytes` (reposition + rebuild) over a monotone sweep, by selectivity.
//!   Validates the landed forward-seek and quantifies the gallop win.
//! - `bench_cogroup_group_walk` — production membership walk
//!   (`cogroup_intersection` + `group_has_positive`) vs a dispatch-free,
//!   cursor-free floor over the same rows, by group fan-out. `current / floor`
//!   is the headroom a monomorphized `fold_pk_group` (Rung 1) can capture.

use std::hint::black_box;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::ops::cogroup::cogroup_intersection;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, CursorHandle};

// ---------------------------------------------------------------------------
// data builders (deterministic — no RNG, so runs are comparable across edits)
// ---------------------------------------------------------------------------

fn schema_u64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(type_code::I64, 0)],
        &[0],
    )
}

fn schema_u128() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[SchemaColumn::new(type_code::U128, 0), SchemaColumn::new(type_code::I64, 0)],
        &[0],
    )
}

/// Sorted + consolidated batch from `(pk, weight, payload)` rows.
fn build_batch(
    schema: SchemaDescriptor,
    rows: impl Iterator<Item = (u64, i64, i64)>,
    cap: usize,
) -> Rc<Batch> {
    let mut b = Batch::with_schema(schema, cap.max(1));
    for (pk, w, v) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &v.to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    Rc::new(b)
}

/// `k` sorted sources holding `n` rows total, PKs round-robined across sources
/// so the merge interleaves maximally (worst case for the loser tree).
fn interleaved_sources(schema: SchemaDescriptor, k: usize, n: usize) -> Vec<Rc<Batch>> {
    (0..k)
        .map(|s| {
            build_batch(
                schema,
                (0..n).filter(|i| i % k == s).map(|i| (i as u64, 1, i as i64)),
                n / k + 1,
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// timing: adaptive iters to a floor duration, median of a few measurements
// ---------------------------------------------------------------------------

/// Min ns per row of `f` (one pass over `rows_per_pass` rows) over `SAMPLES`
/// measurements. Min — not median — is the right estimator on a non-quiet
/// machine: it's the run least perturbed by scheduler/thermal/neighbour noise,
/// which is one-sided (it can only *slow* a pass). Each sample grows its inner
/// iteration count until it runs ≥ `MIN`, so fast passes clear clock noise;
/// `black_box` keeps the work from being elided.
fn ns_per_row(rows_per_pass: usize, mut f: impl FnMut() -> u64) -> f64 {
    const MIN: Duration = Duration::from_millis(100);
    const SAMPLES: usize = 9;
    for _ in 0..2 {
        black_box(f());
    }
    let mut best = f64::INFINITY;
    for _ in 0..SAMPLES {
        let mut iters = 1u64;
        loop {
            let t = Instant::now();
            let mut acc = 0u64;
            for _ in 0..iters {
                acc = acc.wrapping_add(f());
            }
            black_box(acc);
            let el = t.elapsed();
            if el >= MIN || iters >= (1 << 24) {
                best = best.min(el.as_nanos() as f64 / (iters * rows_per_pass as u64) as f64);
                break;
            }
            iters *= 2;
        }
    }
    best
}

// ---------------------------------------------------------------------------
// benchmarks
// ---------------------------------------------------------------------------

/// Scan throughput vs source count. The k=1 (Single bypass) → k=2 (loser tree)
/// jump is the heap's fixed per-row cost — the headroom for a `Pair` bypass.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture"]
fn bench_merge_scan_by_k() {
    const N: usize = 500_000;
    let s = schema_u64();
    println!("\n-- merge scan, U64 PK, N={N}, round-robin interleave --");
    for &k in &[1usize, 2, 4, 8, 16] {
        let srcs = interleaved_sources(s, k, N);
        let nsr = ns_per_row(N, || {
            let mut h = CursorHandle::from_owned(&srcs, s);
            let c = h.cursor_mut();
            let mut acc = 0u64;
            while c.valid {
                acc ^= (c.current_key as u64) ^ (c.current_weight as u64);
                c.advance();
            }
            acc
        });
        println!("  k={k:<2} {nsr:7.2} ns/row");
    }
}

/// PK-width sensitivity of the merge scan (narrow u64 vs 16-byte u128 key).
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture"]
fn bench_merge_scan_pk_width() {
    const N: usize = 500_000;
    println!("\n-- merge scan, k=4, N={N}, by PK width --");
    for (name, s) in [("U64 ", schema_u64()), ("U128", schema_u128())] {
        let srcs = interleaved_sources(s, 4, N);
        let nsr = ns_per_row(N, || {
            let mut h = CursorHandle::from_owned(&srcs, s);
            let c = h.cursor_mut();
            let mut acc = 0u64;
            while c.valid {
                acc ^= c.current_key as u64;
                c.advance();
            }
            acc
        });
        println!("  {name} {nsr:7.2} ns/row");
    }
}

/// Forward sweep: in-place gallop (`advance_to`) vs reposition+rebuild
/// (`seek_bytes`) over a monotone ascending probe set, by selectivity. The
/// ratio is the landed forward-seek's win; it should grow as probes thin out.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture"]
fn bench_advance_to_vs_seek() {
    const N: usize = 500_000;
    let s = schema_u64();
    let srcs = interleaved_sources(s, 4, N);
    println!("\n-- forward seek sweep, U64 PK, N={N}, k=4 --");
    for &stride in &[1usize, 4, 16, 64] {
        let probes: Vec<[u8; 8]> =
            (0..N).step_by(stride).map(|i| (i as u64).to_be_bytes()).collect();
        let np = probes.len();
        let gallop = ns_per_row(np, || {
            let mut h = CursorHandle::from_owned(&srcs, s);
            let c = h.cursor_mut();
            let mut acc = 0u64;
            for p in &probes {
                c.advance_to(p);
                acc ^= c.valid as u64;
            }
            acc
        });
        let rebuild = ns_per_row(np, || {
            let mut h = CursorHandle::from_owned(&srcs, s);
            let c = h.cursor_mut();
            let mut acc = 0u64;
            for p in &probes {
                c.seek_bytes(p);
                acc ^= c.valid as u64;
            }
            acc
        });
        let sel = 100.0 / stride as f64;
        println!(
            "  sel={sel:5.1}%  n={np:<7} gallop {gallop:7.2}  rebuild {rebuild:7.2}  ns/probe  ({:.2}x)",
            rebuild / gallop,
        );
    }
}

/// Production membership walk vs a dispatch-free floor over identical rows, by
/// group fan-out. `current / floor` bounds the win a monomorphized
/// `fold_pk_group` (Rung 1) can capture — large gap ⇒ build it, flat ⇒ skip.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture"]
fn bench_cogroup_group_walk() {
    const M: usize = 100_000;
    let s = schema_u64();
    println!("\n-- cogroup membership walk, U64 PK, single-source trace, M={M} groups --");
    for &g in &[1usize, 2, 8, 32] {
        let trace = build_batch(
            s,
            (0..M).flat_map(|p| (0..g).map(move |q| (p as u64, 1, q as i64))),
            M * g,
        );
        let delta = build_batch(s, (0..M).map(|p| (p as u64, 1, 0)), M);
        let rows = M * g;

        // Current: gallop between groups + `group_has_positive` walks each group
        // one `advance()` (full re-dispatch + `current_*` commit) at a time.
        let cur = ns_per_row(rows, || {
            let mut h = CursorHandle::from_owned(std::slice::from_ref(&trace), s);
            let c = h.cursor_mut();
            let mut acc = 0u64;
            cogroup_intersection(&delta, c, |key, _range, m| {
                acc ^= m.group_has_positive(key) as u64;
            });
            acc
        });

        // Floor: same per-group membership logic via direct batch indexing — no
        // cursor, no comparator dispatch, no per-row state commit.
        let floor = ns_per_row(rows, || {
            let mut acc = 0u64;
            let mut i = 0;
            while i < trace.count {
                let k = trace.get_pk_bytes(i);
                let (mut present, mut j) = (false, i);
                while j < trace.count && trace.get_pk_bytes(j) == k {
                    present |= trace.get_weight(j) > 0;
                    j += 1;
                }
                acc ^= present as u64;
                i = j;
            }
            acc
        });

        println!(
            "  fanout={g:<3} current {cur:7.2}  floor {floor:7.2}  ns/row  (headroom {:.2}x)",
            cur / floor,
        );
    }
}
