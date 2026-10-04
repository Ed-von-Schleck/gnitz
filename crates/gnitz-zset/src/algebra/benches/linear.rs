use super::*;
use crate::test_support::{make_batch, make_schema_u64_i64};

/// One side's `(pk, weight, payload)` row generator, indexed by row number.
type RowGen<'a> = &'a dyn Fn(usize) -> (u64, i64, i64);

/// Release-only microbench for `op_union`'s two-way merge, over the shapes its
/// three arms see. `shared_pk_interleave` and `shared_pk_fold` put every row in
/// an equal-PK group, which is the arm that folds; `alt1` alternates single rows
/// (what a set operation's uniform hashed `_set_pk` produces) and `runs4096` is
/// the `store_io` shape — long PK-disjoint runs. The last two stay entirely in
/// the galloping arms, which the fold does not touch, so they are the controls.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn union_merge_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    const N: usize = 500_000;
    const ITERS: usize = 20;
    const RUN: usize = 4096;
    let schema = make_schema_u64_i64();

    // Each side is built strictly (PK, payload)-ascending, so `make_batch`'s
    // consolidated certification is honest and `op_union` takes its merge.
    let side = |f: RowGen| -> Batch { make_batch(&schema, &(0..N).map(f).collect::<Vec<_>>()) };

    let cases: [(&str, RowGen, RowGen); 4] = [
        // Every PK shared, 8 payloads a side, payloads interleaving one for one:
        // the fold arm's per-row loop with a side switch at every step.
        (
            "shared_pk_interleave",
            &|i| ((i / 8) as u64, 1, (2 * (i % 8)) as i64),
            &|i| ((i / 8) as u64, 1, (2 * (i % 8) + 1) as i64),
        ),
        // Every (PK, payload) shared: every step folds and appends one row.
        ("shared_pk_fold", &|i| ((i / 8) as u64, 1, (i % 8) as i64), &|i| {
            ((i / 8) as u64, 1, (i % 8) as i64)
        }),
        // Disjoint PKs alternating one for one: the galloping arms at run 1.
        ("alt1", &|i| (2 * i as u64, 1, i as i64), &|i| {
            (2 * i as u64 + 1, 1, i as i64)
        }),
        // Disjoint PKs in guard-sized blocks: the galloping arms at run 4096.
        (
            "runs4096",
            &|i| ((2 * (i / RUN) * RUN + i % RUN) as u64, 1, i as i64),
            &|i| ((2 * (i / RUN) * RUN + RUN + i % RUN) as u64, 1, i as i64),
        ),
    ];

    for (label, fa, fb) in cases {
        let (a, b) = (side(fa), side(fb));
        let t = Instant::now();
        let mut acc = 0usize;
        for _ in 0..ITERS {
            acc += black_box(op_union(Cow::Borrowed(&a), Cow::Borrowed(&b), &schema).count);
        }
        let secs = t.elapsed().as_secs_f64();
        println!(
            "union_merge/{label}: {:.1} Mrows/s ({} in-rows × {ITERS} iters in {secs:.3}s, out {})",
            (2 * N * ITERS) as f64 / secs / 1e6,
            2 * N,
            acc / ITERS,
        );
    }
}
