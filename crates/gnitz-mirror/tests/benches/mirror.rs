use super::*;

/// The round trip, measured as instructions retired: a narrowly-bounded read
/// against the mirror versus the same read against the server.
///
/// `#[ignore]`d and run with `--nocapture`, like the engine's other timing
/// tests: it prints a measurement rather than asserting a threshold, and needs a
/// `perf_event_open` the sandbox may refuse.
///
/// The counter's scope is **the calling thread**, kernel time included where the
/// kernel allows it. That is the right scope for what is being claimed: what a
/// mirror removes from a caller is the caller's own syscall, socket and wakeup
/// work. The W workers' work behind a served read is outside it by construction,
/// and the falsifiable form of "no round trip at all" is the request count that
/// `each_mirror_call_costs_what_it_must` pins, not this.
///
/// **The read must be one the bound narrows.** A full scan measures the wrong
/// thing: the mirror does on one thread what the server splits W ways, so
/// instructions retired would come out level while wall-clock moved *against*
/// the mirror. The full-scan number is recorded too, as the statement of that
/// trade rather than as a target.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn round_trip_cost_bench() {
    let mut fx = Fixture::start();
    let Some(counter) = gnitz_foundation::perf::Counter::instructions() else {
        println!("perf_event_open refused; skipping the instruction count");
        return;
    };
    if !counter.counts_kernel {
        println!("note: perf_event_paranoid forbids kernel-mode counting, so a served read's syscall and");
        println!("      wakeup path are invisible here and its figure is an under-count.");
    }

    churn(&mut fx.direct, 1, 2_000);
    fx.mirror_both();
    fx.quiesce();

    for (label, q) in [
        ("point", "SELECT a, b, v FROM v_keyed WHERE a = 977"),
        ("narrow", "SELECT a, b, v FROM v_keyed WHERE a > 900 AND a < 940"),
        ("full", "SELECT a, b, v FROM v_keyed"),
    ] {
        // One warm pass each: the first read of a relation pays cache fills on
        // both sides, and neither is what this measures.
        let _ = fx.local(q);
        let _ = query(&mut fx.direct, "s", q);

        let (_, local) = counter.measure(|| fx.local(q));
        let (_, remote) = counter.measure(|| query(&mut fx.direct, "s", q));
        println!("{label:>7}: mirror {local:>12} instr, server {remote:>12} instr (client side)");
    }
}

/// The client-side cost of one idle poll over M views — the steady state of a
/// subscription: one request and one park, whatever M is. Instructions retired
/// is the same claim, immune to machine frequency.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn idle_poll_client_cost_bench() {
    const M: usize = 16;
    const K: usize = 200;

    let mut fx = Fixture::start();
    let Some(counter) = gnitz_foundation::perf::Counter::instructions() else {
        println!("perf_event_open refused; skipping the instruction count");
        return;
    };

    churn(&mut fx.direct, 1, 2_000);
    fx.many_views("i", M, "a, b, v, f, body");
    // Poll once more after the drain, so the measured run is wholly idle.
    fx.mirror().poll_mirror().expect("poll");

    let before = gnitz_foundation::perf::voluntary_ctx_switches();
    let (_, insns) = counter.measure(|| {
        for _ in 0..K {
            fx.mirror().poll_mirror().expect("poll");
        }
    });
    let switches = gnitz_foundation::perf::voluntary_ctx_switches() - before;

    println!(
        "idle poll over M={M} K={K}: {:.0} instr/poll ({}), {:.2} voluntary ctx switches/poll",
        insns as f64 / K as f64,
        if counter.counts_kernel {
            "kernel included"
        } else {
            "user-space only"
        },
        switches as f64 / K as f64,
    );
}

/// The copy is a store, not a resident Z-set.
///
/// `#[ignore]`d, run with `--nocapture`, and in a child process, so the host RSS
/// measured is the child's alone. **It is not a gate**: a checkpoint publishes shard files
/// whatever the RAM ceiling is, so their presence proves nothing, and an RSS
/// threshold is not sound enough to fail a build on. The number is the
/// statement of the claim.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn resident_footprint_bench() {
    print!(
        "{}",
        run_child(
            "resident_footprint_child",
            &[],
            "the footprint child must run to the end",
        )
    );
}
