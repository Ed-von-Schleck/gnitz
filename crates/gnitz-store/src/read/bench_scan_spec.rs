//! Isolated release benchmark for the ad-hoc SELECT read path — the in-process
//! `scan_spec` sink, with no server, IPC, or socket in the measurement
//! (`make bench` is end-to-end, so it cannot isolate this loop). Run per the
//! developer guide's release-bench procedure:
//!
//! ```text
//! cd crates && cargo test -p gnitz-store --release scan_spec_ \
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
use std::rc::Rc;
use std::time::Instant;

use crate::relation::{IndexClaim, RelationKind};
use crate::test_support::{map_of, relation_fixture, rows_spec, RelationFixture, TID};
use gnitz_expr::{CmpOp, ExprBuilder, IntArithOp, LogicalInstr, LogicalProgram, SchemaFacts, Sink};
use gnitz_wire::{AggDescriptor, AggFunc, AggReadSpec, OrderKey, ReadBound, ReadSink, ReadSpec, SinkKind, TypeCode};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

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
fn cell(label: &str, scanned: u64, mut f: impl FnMut() -> Rc<Batch>) {
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

/// A base table over `schema`, ingested in [`INGEST_ROUNDS`] PK-interleaved
/// passes over ids `0..n`, every row at weight 1. `put_row` writes one row's
/// payload columns.
fn ingest_fixture(
    schema: SchemaDescriptor,
    n: u64,
    mut put_row: impl FnMut(&mut BatchBuilder, u64),
) -> RelationFixture {
    let mut r = relation_fixture(RelationKind::BaseTable, schema, &[], Batch::empty_with_schema(&schema));
    for round in 0..INGEST_ROUNDS {
        let mut bb = BatchBuilder::new(&schema);
        for id in (round..n).step_by(INGEST_ROUNDS as usize) {
            bb.begin_row(id as u128, 1);
            put_row(&mut bb, id);
            bb.end_row();
        }
        r.ingest(TID, bb.finish()).unwrap();
    }
    r
}

/// `id U64 PK | c0 I64 | cf I64 | c2 I64 | c3 I64` at weight 1. `c0 = id % 100`
/// is the contiguous-run selectivity dial, `cf = id % 2` the fragmenting one.
fn numeric_fixture(n: u64) -> RelationFixture {
    ingest_fixture(i64_reply(4), n, |bb, id| {
        bb.put_u64(id % 100);
        bb.put_u64(id % 2);
        bb.put_u64(id ^ 0xa5a5_a5a5);
        bb.put_u64(id / 7);
    })
}

/// `id U64 PK` + `n_payload` I64 payload columns.
fn i64_reply(n_payload: usize) -> SchemaDescriptor {
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend((0..n_payload).map(|_| SchemaColumn::new(TypeCode::I64, false)));
    SchemaDescriptor::new(&cols, &[0])
}

/// `col < lit` over a fixed-int column, as a wire predicate.
fn lt(col: u32, lit: i64) -> Vec<u8> {
    crate::test_support::cmp_const(CmpOp::Lt, col, lit).to_blob_bytes()
}

/// A rows spec with no bound, under `predicate`.
fn filtered(predicate: &[u8], map: Option<gnitz_wire::ComputeMap>, order: Vec<OrderKey>, limit_k: u64) -> ReadSpec {
    ReadSpec {
        predicate: predicate.to_vec(),
        ..rows_spec(map, order, limit_k)
    }
}

/// Every non-string sink path over one shared fixture: the gather, the identity
/// projection, the compute projection (survivors compacted, then the kernel onto
/// the keeper tail), the two bounded shapes, and the aggregate fold. The
/// contiguous/fragmented pair is what the range-driven design lives or dies on.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn scan_spec_sinks_bench() {
    let e = numeric_fixture(NUMERIC_ROWS);
    let src = e.relation(TID).unwrap().schema();
    let n = NUMERIC_ROWS;
    // `c0 < 50` → contiguous 50-row runs; `cf < 1` → single-row ranges.
    let (contiguous, fragmented) = (lt(1, 50), lt(2, 1));

    // Gather 3 of the 4 payload columns, permuted: c2→0, c3→1, c0→2.
    let reply3 = i64_reply(3);
    let gather3 = map_of(LogicalProgram::copy_cols(&[3, 4, 1]), &reply3);
    for (label, pred) in [("contiguous", &contiguous), ("fragmented", &fragmented)] {
        let spec = filtered(pred, gather3.clone(), vec![], 0);
        cell(&format!("rows, sel~50% {label}, 3-col gather"), n, || {
            e.scan_spec(TID, spec.clone(), reply3.layout_digest(), None).unwrap()
        });
    }

    // No projection: whole-region range appends, no per-column gather.
    let spec = filtered(&fragmented, None, vec![], 0);
    cell("rows, sel~50% fragmented, identity projection", n, || {
        e.scan_spec(TID, spec.clone(), src.layout_digest(), None).unwrap()
    });

    // Compute-bearing projection, fragmented — where compacting the survivors
    // before the morsel kernel is meant to earn its keep.
    let compute_proj = {
        let mut eb = ExprBuilder::new();
        let (a, b) = (
            eb.emit(LogicalInstr::LoadCol { col: 3 }),
            eb.emit(LogicalInstr::LoadCol { col: 4 }),
        );
        let sum = eb.emit(LogicalInstr::IntArith { op: IntArithOp::Add, a, b });
        eb.build(vec![Sink::Reg(sum), Sink::Col(1)]).unwrap()
    };
    let reply2 = i64_reply(2);
    let spec = filtered(&fragmented, map_of(compute_proj, &reply2), vec![], 0);
    cell("rows, sel~50% fragmented, compute projection", n, || {
        e.scan_spec(TID, spec.clone(), reply2.layout_digest(), None).unwrap()
    });

    // ORDER BY .. LIMIT — the bounded top-k arm and its `topk_keep` compactions.
    let order = vec![OrderKey { col: 1, desc: false, nulls_first: false }];
    let spec = filtered(&contiguous, gather3.clone(), order, 100);
    cell("rows, ORDER BY .. LIMIT 100 (top-k)", n, || {
        e.scan_spec(TID, spec.clone(), reply3.layout_digest(), None).unwrap()
    });

    // The same arm ordered by reply column 3, `c0`, which repeats across the
    // survivors, so nearly every compare falls through to the identity tiebreak.
    let order = vec![OrderKey { col: 3, desc: false, nulls_first: false }];
    let spec = filtered(&contiguous, gather3.clone(), order, 100);
    cell("rows, ORDER BY c0 .. LIMIT 100 (top-k, tie-heavy)", n, || {
        e.scan_spec(TID, spec.clone(), reply3.layout_digest(), None).unwrap()
    });

    // No-ORDER-BY LIMIT — the early-stop arm. With a predicate the sink drains
    // full chunks (a tiny chunk would degrade to row-at-a-time cursor driving),
    // and the range-list cut lands inside the first one, so the rows actually
    // walked are one chunk; rating it over the 100 returned rows would report a
    // meaningless ~0.01 M/s.
    let spec = filtered(&contiguous, gather3.clone(), vec![], 100);
    let chunk = e.scan_chunk_rows() as u64;
    cell("rows, LIMIT 100 (early stop, 1 chunk)", chunk, || {
        e.scan_spec(TID, spec.clone(), reply3.layout_digest(), None).unwrap()
    });

    // The fold sink: GROUP BY c0 (100 groups), COUNT(*) + SUM(c2).
    let agg = AggReadSpec {
        group_cols: vec![1],
        aggs: vec![
            AggDescriptor::COUNT_STAR,
            AggDescriptor { agg_op: AggFunc::Sum, col_idx: 3 },
        ],
    };
    // The group column is the output PK, then one partial per aggregate.
    let fold_reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    for (label, pred) in [("contiguous", &contiguous), ("fragmented", &fragmented)] {
        let spec = ReadSpec {
            bound: ReadBound::None,
            predicate: pred.clone(),
            sink: ReadSink {
                map: None,
                kind: SinkKind::Fold(agg.clone()),
            },
        };
        cell(&format!("fold, sel~50% {label}, GROUP BY COUNT+SUM"), n, || {
            e.scan_spec(TID, spec.clone(), fold_reply.layout_digest(), None)
                .unwrap()
        });
    }

    // The fold at many groups, where the group lookup rather than the
    // accumulation dominates: `c3 = id / 7` below 60_000 names 60_000 groups.
    let spec = ReadSpec {
        bound: ReadBound::None,
        predicate: lt(4, 60_000),
        sink: ReadSink {
            map: None,
            kind: SinkKind::Fold(AggReadSpec {
                group_cols: vec![4],
                aggs: vec![AggDescriptor::COUNT_STAR],
            }),
        },
    };
    let many_reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    cell("fold, c3 < 60000, GROUP BY 60k groups COUNT", n, || {
        e.scan_spec(TID, spec.clone(), many_reply.layout_digest(), None)
            .unwrap()
    });
}

/// The German-string path, whose mandatory per-cell relocation into the keeper
/// (and its dedup cache) dominates the sink and shares nothing with the fixed-
/// width gather above. Half the values are long (out-of-line in the blob heap),
/// half inline-short, so both arms run.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn scan_spec_string_gather_bench() {
    let reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let e = ingest_fixture(reply, STRING_ROWS, |bb, id| {
        match id.is_multiple_of(2) {
            true => bb.put_string(&format!("a-fairly-long-out-of-line-value-{id}")),
            false => bb.put_string("short"),
        }
        bb.put_u64(id % 2);
    });
    let spec = filtered(&lt(2, 1), map_of(LogicalProgram::copy_cols(&[1, 2]), &reply), vec![], 0);
    cell("rows, sel~50% fragmented, gather incl. STRING", STRING_ROWS, || {
        e.scan_spec(TID, spec.clone(), reply.layout_digest(), None).unwrap()
    });
}

/// A global `SUM` over a NOT NULL column, with the COUNT(*) it is gated on.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn scan_spec_global_sum_bench() {
    let e = numeric_fixture(NUMERIC_ROWS);
    let spec = ReadSpec {
        bound: ReadBound::None,
        predicate: Vec::new(),
        sink: ReadSink {
            map: None,
            kind: SinkKind::Fold(AggReadSpec {
                group_cols: vec![],
                aggs: vec![
                    AggDescriptor { agg_op: AggFunc::Sum, col_idx: 3 },
                    AggDescriptor::COUNT_STAR,
                ],
            }),
        },
    };
    let reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    cell("fold, global SUM(c2) + its COUNT(*)", NUMERIC_ROWS, || {
        e.scan_spec(TID, spec.clone(), reply.layout_digest(), None).unwrap()
    });
}

/// An index-range read through the bounded index cursor at a chunk size that makes
/// it refill four times; `c2` is uncorrelated with the PK, so every gathered key seeks.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn scan_spec_index_range_bench() {
    use gnitz_wire::{key_image, Cut, KeyRange, PkColList};
    let mut e = numeric_fixture(NUMERIC_ROWS);
    e.add_index(TID, IndexClaim::Index { id: TID + 1, unique: false }, &[3])
        .unwrap();
    e.set_scan_chunk_rows(16_384);
    let src = e.relation(TID).unwrap().schema();
    // `c2 = id ^ 0xa5a5_a5a5` over `id < 2^21`: a 2^16-wide value band holds at most
    // 2^16 rows, inside the index gate.
    let lo = 0xa5a5_a5a5u64 & !((1 << 21) - 1);
    let img = |v: u64| key_image(TypeCode::I64, v as u128);
    let band = KeyRange::new(
        PkColList::from_slice(&[3]),
        &[],
        Cut::before(img(lo)),
        Cut::before(img(lo + (1 << 16))),
    );
    let spec = ReadSpec::all_rows(ReadBound::Range(band));
    let rows = e.scan_spec(TID, spec.clone(), src.layout_digest(), None).unwrap().len();
    assert!(rows >= 4 * 16_384, "the band spans several refills: {rows} rows");
    cell("index range, 16K-row chunks", rows as u64, || {
        e.scan_spec(TID, spec.clone(), src.layout_digest(), None).unwrap()
    });
}
