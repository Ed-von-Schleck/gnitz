//! Release benchmarks: the heap a view's maintenance takes from the kernel. A
//! view is fed `TICKS` ticks of fresh rows, as one of four workers sees a bulk
//! load, under the allocator settings a worker runs with. Per view shape it
//! prints the instructions per row of the pushes and ticks in user and in
//! kernel mode, and over the whole feed, the input batches built included, the
//! bytes the allocator holds handed out at the end, how far its heap grew, and
//! the minor faults taken: glibc trims nothing under those settings, so every
//! byte the heap grew by was faulted in once, and the kernel's share is mostly
//! those faults. Each runs alone, since the heap is the process's:
//!
//! ```text
//! cd crates && for t in left_join_heap_bench group_by_heap_bench; do \
//!     cargo test -p gnitz-server --release $t -- --ignored --nocapture --test-threads=1; done
//! ```

use super::*;
use gnitz_foundation::perf::{Counters, Event};
use gnitz_foundation::rng::mix;
use gnitz_wire::{AggDescriptor, AggFunc, Circuit, GroupAggs, ReadBound, ReduceKind};

const TICKS: u64 = 50;
const TICK_ROWS: u64 = 5_000;
/// The rows of the joined table, and the groups of the reduce.
const CUSTOMERS: u64 = TICKS * TICK_ROWS / 20;
const MB: f64 = (1 << 20) as f64;

/// The bytes handed out and not freed, and the bytes the allocator holds from
/// the kernel: its `brk` heap and its mapped chunks.
fn heap() -> (usize, usize) {
    // SAFETY: reads the allocator's counters.
    let m = unsafe { libc::mallinfo2() };
    (m.uordblks + m.hblkhd, m.arena + m.hblkhd)
}

fn minor_faults() -> i64 {
    // SAFETY: `getrusage` fills the struct it is handed.
    let mut r: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut r) };
    r.ru_minflt
}

/// Feeds `TICKS` ticks of `TICK_ROWS` fresh ids into `source`, and reports.
fn feed(label: &str, engine: &mut CatalogEngine, source: u64, cells: impl Fn(u64) -> [u64; 2] + Copy) {
    let counters = Counters::open([Event::INSTRUCTIONS, Event::INSTRUCTIONS.with_kernel()]);
    let ((live0, held0), faults0) = (heap(), minor_faults());
    let [mut user, mut all] = [0u64; 2];
    for t in 0..TICKS {
        let batch = rows(engine, source, 1, t * TICK_ROWS..(t + 1) * TICK_ROWS, cells);
        let ((), [u, a]) = counters.measure(|| push_and_tick(engine, source, batch));
        user += u;
        all += a;
    }
    let ((live, held), faults) = (heap(), minor_faults());
    let n = (TICKS * TICK_ROWS) as f64;
    println!(
        "{label:<14}: {:>7.1} instr/row user, {:>6.1} kernel; live {:>5.1} MB, heap grew {:>5.1} MB, \
         faults {:>5.1} MB",
        user as f64 / n,
        (all - user) as f64 / n,
        live.saturating_sub(live0) as f64 / MB,
        held.saturating_sub(held0) as f64 / MB,
        ((faults - faults0) * 4096) as f64 / MB,
    );
}

/// `a LEFT JOIN b ON a.nn = b.k`, `b` a twentieth of `a` and a sixth of `a`
/// matching nothing.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn left_join_heap_bench() {
    gnitz_foundation::host::keep_freed_heap();
    let tmp = tempfile::tempdir().unwrap();
    let (mut engine, [a, b], _) = left_join_engine(tmp.path().to_str().unwrap(), 1);
    let held = rows(&engine, b, 1, 0..CUSTOMERS, |id| [id, mix(id)]);
    push_and_tick(&mut engine, b, held);
    feed("left join", &mut engine, a, |id| [mix(id) % (CUSTOMERS * 6 / 5), id]);
}

/// `SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn group_by_heap_bench() {
    gnitz_foundation::host::keep_freed_heap();
    let cols = [
        col_def("id", TypeCode::U64),
        col_def("k", TypeCode::U64),
        col_def("v", TypeCode::I64),
    ];
    let (_tmp, mut engine, t) = table_engine(1, &cols);
    let (group, aggs) = (
        [1u32],
        [
            AggDescriptor::COUNT_STAR,
            AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum },
        ],
    );
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(t, ReadBound::None);
    let reduce = GroupAggs {
        group_cols: group.to_vec(),
        aggs: aggs.to_vec(),
    };
    circuit.reduce(scan, reduce, ReduceKind::Aggregate);
    let schema = engine.registry.relation(t).map(Relation::schema).unwrap();
    let out = *gnitz_trace::stream::ReducePlan::from_wire(&schema, &group, &aggs, false)
        .unwrap()
        .output_schema();
    try_register_view(&mut engine, circuit, "v", &cols_of(&out), 0, 0).unwrap();
    feed("group by, sum", &mut engine, t, |id| [mix(id) % CUSTOMERS, id]);
}
