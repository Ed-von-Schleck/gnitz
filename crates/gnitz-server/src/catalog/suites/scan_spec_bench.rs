//! Isolated release benchmark for the ad-hoc SELECT read path — the in-process
//! `scan_spec_family` sink, with no server, IPC, or socket in the measurement
//! (`make bench` is end-to-end, so it cannot isolate this loop). Run per the
//! developer guide's release-bench procedure:
//!
//! ```text
//! cd crates && cargo test -p gnitz-server --release scan_spec_ \
//!     -- --ignored --nocapture --test-threads=1
//! ```
//!
//! One cell per distinct code path, not per query shape. The fixtures ingest in
//! `INGEST_ROUNDS` PK-interleaved rounds so the cursor is a genuine N-way merge
//! over that many sorted runs (a single round would bulk-drain one source and
//! skip the merge walk entirely), and chunking is the production
//! `scan_chunk_rows`.
//!
//! Each sink is measured at **both survivor-run lengths**, which is the axis
//! that actually decides this path. `c0 = id % SEL_MOD` is PK-correlated, so
//! `c0 < 50` yields contiguous 50-row survivor runs; `cf = id % 2` gives
//! single-row ranges at the same ~50% selectivity — what a predicate on a column
//! uncorrelated with the PK order produces, and where per-range fixed costs
//! rather than per-row copies dominate.

use std::hint::black_box;
use std::time::Instant;

use super::*;
use gnitz_store::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::Batch;
use gnitz_wire::{AggDescriptor, AggFunc, AggReadSpec, OrderKey, ReadBound, ReadSink, ReadSpec};

/// Sorted runs the cursor must merge (one ingest round each).
const INGEST_ROUNDS: u64 = 8;
/// Numeric fixture rows.
const NUMERIC_ROWS: u64 = 2_000_000;
/// String fixture rows.
const STRING_ROWS: u64 = 1_000_000;
/// Timed repetitions per cell; the median is reported.
const REPS: usize = 5;

/// Run `f` once untimed, then `REPS` timed repetitions; report the median as
/// million scanned rows/s over `scanned` (the rows the cursor walked, not the
/// rows returned — the sink's real work unit).
fn cell(label: &str, scanned: u64, mut f: impl FnMut() -> Batch) {
    black_box(f());
    let mut secs: Vec<f64> = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let t = Instant::now();
        let out = f();
        secs.push(t.elapsed().as_secs_f64());
        black_box(&out);
    }
    secs.sort_by(f64::total_cmp);
    let med = secs[REPS / 2];
    println!(
        "{label:<50} {:>8.2} M scanned-rows/s  ({:>7.1} ms median)",
        scanned as f64 / med / 1e6,
        med * 1e3
    );
}

/// `id U64 PK | c0 I64 | cf I64 | c2 I64 | c3 I64` at weight 1. `c0 = id % 100`
/// is the contiguous-run selectivity dial, `cf = id % 2` the fragmenting one.
fn numeric_fixture(name: &str, n: u64) -> (CatalogEngine, i64) {
    let cols = vec![
        col_def("id", type_code::U64),
        col_def("c0", type_code::I64),
        col_def("cf", type_code::I64),
        col_def("c2", type_code::I64),
        col_def("c3", type_code::I64),
    ];
    ingest_fixture(name, &cols, n, INGEST_ROUNDS, |bb, id| {
        bb.put_u64(id % 100);
        bb.put_u64(id % 2);
        bb.put_u64(id ^ 0xa5a5_a5a5);
        bb.put_u64(id / 7);
    })
}

/// `id U64 PK` + `n_payload` I64 payload columns.
fn i64_reply(n_payload: usize) -> SchemaDescriptor {
    let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
    cols.extend((0..n_payload).map(|_| SchemaColumn::new(type_code::I64, 0)));
    SchemaDescriptor::new(&cols, &[0])
}

/// Every non-string sink path over one shared fixture: the gather, the identity
/// projection, the compute projection (survivors compacted, then the kernel onto
/// the keeper tail), the two bounded shapes, and the aggregate fold. The
/// contiguous/fragmented pair is what the range-driven design lives or dies on.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn scan_spec_sinks_bench() {
    let (mut e, tid) = numeric_fixture("ss_bench", NUMERIC_ROWS);
    let src = e.registry().get_schema_desc(tid).unwrap();
    let n = NUMERIC_ROWS;
    // `c0 < 50` → contiguous 50-row runs; `cf < 1` → single-row ranges.
    let (contiguous, fragmented) = (pred_lt_blob(1, 50), pred_lt_blob(2, 1));

    // Gather 3 of the 4 payload columns, permuted: c2→0, c3→1, c0→2.
    let gather3 = proj_blob(&[(3, 0), (4, 1), (1, 2)]);
    let reply3 = i64_reply(3);
    for (label, pred) in [("contiguous", &contiguous), ("fragmented", &fragmented)] {
        let spec = rows_spec(pred.clone(), gather3.clone(), vec![], 0);
        cell(&format!("rows, sel~50% {label}, 3-col gather"), n, || {
            e.scan_spec_family(tid, &spec, &reply3, 0).unwrap()
        });
    }

    // No projection: whole-region range appends, no per-column gather.
    let spec = rows_spec(fragmented.clone(), vec![], vec![], 0);
    cell("rows, sel~50% fragmented, identity projection", n, || {
        e.scan_spec_family(tid, &spec, &src, 0).unwrap()
    });

    // Compute-bearing projection, fragmented — where compacting the survivors
    // before the morsel kernel is meant to earn its keep.
    let compute_proj = {
        let mut eb = gnitz_expr::ExprBuilder::new();
        let (a, b) = (
            eb.emit(gnitz_expr::LogicalInstr::LoadColInt { col: 3 }),
            eb.emit(gnitz_expr::LogicalInstr::LoadColInt { col: 4 }),
        );
        let sum = eb.emit(gnitz_expr::LogicalInstr::IntArith {
            op: gnitz_expr::IntArithOp::Add,
            a,
            b,
        });
        eb.sink(gnitz_expr::Sink::Reg(sum));
        eb.sink(gnitz_expr::Sink::Col(1));
        eb.build(None).expect("a well-formed program").to_blob_bytes()
    };
    let spec = rows_spec(fragmented.clone(), compute_proj, vec![], 0);
    let reply2 = i64_reply(2);
    cell("rows, sel~50% fragmented, compute projection", n, || {
        e.scan_spec_family(tid, &spec, &reply2, 0).unwrap()
    });

    // ORDER BY .. LIMIT — the bounded top-k arm and its `topk_keep` compactions.
    let order = vec![OrderKey {
        col: 1,
        desc: false,
        nulls_first: false,
    }];
    let spec = rows_spec(contiguous.clone(), gather3.clone(), order, 100);
    cell("rows, ORDER BY .. LIMIT 100 (top-k)", n, || {
        e.scan_spec_family(tid, &spec, &reply3, 0).unwrap()
    });

    // No-ORDER-BY LIMIT — the early-stop arm. With a predicate the sink drains
    // full chunks (a tiny chunk would degrade to row-at-a-time cursor driving),
    // and the range-list cut lands inside the first one, so the rows actually
    // walked are one chunk; rating it over the 100 returned rows would report a
    // meaningless ~0.01 M/s.
    let spec = rows_spec(contiguous.clone(), gather3.clone(), vec![], 100);
    let chunk = e.registry().scan_chunk_rows() as u64;
    cell("rows, LIMIT 100 (early stop, 1 chunk)", chunk, || {
        e.scan_spec_family(tid, &spec, &reply3, 0).unwrap()
    });

    // The fold sink: GROUP BY c0 (100 groups), COUNT(*) + SUM(c2).
    let agg = AggReadSpec::direct(
        vec![1],
        vec![
            AggDescriptor {
                agg_op: AggFunc::Count,
                col_idx: 0,
            },
            AggDescriptor {
                agg_op: AggFunc::Sum,
                col_idx: 3,
            },
        ],
    );
    // SyntheticFold reply: `_agg_pk` U128 PK, the group column, then one partial
    // per aggregate.
    let fold_reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    for (label, pred) in [("contiguous", &contiguous), ("fragmented", &fragmented)] {
        let spec = ReadSpec {
            bound: ReadBound::None,
            predicate: pred.clone(),
            sink: ReadSink::Fold(agg.clone()),
        };
        cell(&format!("fold, sel~50% {label}, GROUP BY COUNT+SUM"), n, || {
            e.scan_spec_family(tid, &spec, &fold_reply, 0).unwrap()
        });
    }
}

/// The German-string path, whose mandatory per-cell relocation into the keeper
/// (and its dedup cache) dominates the sink and shares nothing with the fixed-
/// width gather above. Half the values are long (out-of-line in the blob heap),
/// half inline-short, so both arms run.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn scan_spec_string_gather_bench() {
    let cols = vec![
        col_def("id", type_code::U64),
        col_def("s", type_code::STRING),
        col_def("cf", type_code::I64),
    ];
    let (mut e, tid) = ingest_fixture("ss_bench_str", &cols, STRING_ROWS, INGEST_ROUNDS, |bb, id| {
        match id.is_multiple_of(2) {
            true => bb.put_string(&format!("a-fairly-long-out-of-line-value-{id}")),
            false => bb.put_string("short"),
        }
        bb.put_u64(id % 2);
    });

    let reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let spec = rows_spec(pred_lt_blob(2, 1), proj_blob(&[(1, 0), (2, 1)]), vec![], 0);
    cell("rows, sel~50% fragmented, gather incl. STRING", STRING_ROWS, || {
        e.scan_spec_family(tid, &spec, &reply, 0).unwrap()
    });
}
