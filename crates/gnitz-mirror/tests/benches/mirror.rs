use super::*;
use gnitz_core::block_on;

use std::hint::black_box;

use gnitz_foundation::perf;
use gnitz_wire::{key_image, Cut, KeyRange, PkColList, PkKeys, ReadBound, ReadSpec, TypeCode};

/// One row of the cost table: `calls` runs of `f` on `client`, as what one of
/// them costs the calling thread — user-space instructions retired, parks and
/// requests sent.
fn cell(label: &str, calls: u64, client: &mut GnitzClient, mut f: impl FnMut(&mut GnitzClient)) {
    let counter = perf::Counter::instructions();
    let (parks, sent) = (perf::voluntary_ctx_switches(), client.requests_sent());
    let ((), instructions) = counter.measure(|| (0..calls).for_each(|_| f(client)));
    println!(
        "{label:<28} {:>10} instr {:>5.2} parks {:>5.2} requests",
        instructions / calls,
        (perf::voluntary_ctx_switches() - parks) as f64 / calls as f64,
        (client.requests_sent() - sent) as f64 / calls as f64,
    );
}

/// What each mirror call costs its caller: a bootstrap, a poll that carries a
/// round, a read off the copy beside the same read served, and an idle poll by
/// how many views it covers.
///
/// The reads are `ReadSpec`s, so no figure carries the SQL front end, and the
/// local ones run against the copy's RAM tier and again against its shards. The
/// W workers' share of a served read is on no counter here: its row is what the
/// read costs the client.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn mirror_call_cost_bench() {
    const CALLS: u64 = 200;

    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 2_000);
    fx.tick("s", &["v_keyed"]);
    cell("bootstrap", 1, fx.mirror(), |m| {
        block_on(m.mirror_view("s", "v_keyed")).expect("mirror");
    });
    churn(&mut fx.direct, 2_001, 4_000);
    fx.tick("s", &["v_keyed"]);
    cell("poll carrying a round", 1, fx.mirror(), |m| {
        block_on(m.poll_mirror()).expect("poll");
    });

    let rel = block_on(fx.mirror().resolve_relation("s", "v_keyed")).expect("resolve");
    let image = |v: i64| key_image(TypeCode::I64, v as u128);
    let point: Vec<u8> = [977, 977 % 7]
        .iter()
        .flat_map(|&v| (image(v) as u64).to_be_bytes())
        .collect();
    let band = KeyRange::new(
        PkColList::from_slice(&[0, 1]),
        &[],
        Cut::after(image(900)),
        Cut::before(image(940)),
    );
    let reads = [
        ("point", ReadBound::PkSet(PkKeys::from_sorted(point.len(), point)), 1),
        ("range", ReadBound::Range(band), 39),
        ("full", ReadBound::None, 3_000),
    ]
    .map(|(label, bound, rows)| (label, ReadSpec::all_rows(bound), rows));

    for (label, spec, rows) in &reads {
        let served = block_on(fx.direct.scan_spec(&*rel, spec, &rel.schema)).expect("served read");
        assert_eq!(
            served.batch.weights.len(),
            *rows,
            "{label}: the bound walks what its label says"
        );
        cell(&format!("served {label}, {rows} rows"), CALLS, &mut fx.direct, |c| {
            black_box(block_on(c.scan_spec(&*rel, spec, &rel.schema)).expect("served read"));
        });
    }
    for tier in ["RAM tier", "shards"] {
        if tier == "shards" {
            block_on(fx.mirror().checkpoint_mirror()).expect("checkpoint");
        }
        for (label, spec, rows) in &reads {
            cell(&format!("local {label}, {tier}"), CALLS, fx.mirror(), |m| {
                let local = block_on(m.scan_spec_local_first(&*rel, spec.clone(), &rel.schema));
                assert_eq!(black_box(local.expect("local read")).batch.weights.len(), *rows);
            });
        }
    }

    // Three steps of one registry, each view bootstrapped and drained before the
    // polls over it are measured.
    for (prefix, more) in [("a", 1), ("b", 15), ("c", 46)] {
        fx.many_views(prefix, more, "a, b, v");
        let views = fx.mirror().mirrored_ids().len();
        cell(&format!("idle poll, {views} views"), CALLS, fx.mirror(), |m| {
            block_on(m.poll_mirror()).expect("poll");
        });
    }
}

/// The copy is a store, not a resident Z-set: what a bootstrap and the copy it
/// leaves keep resident, beside the bytes the copy holds on disk.
///
/// The view is far larger than the copy's RAM tier and arrives in frames far
/// smaller than the view, so neither a copy held in memory nor a bootstrap that
/// buffered its reply could stay near the tier.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn resident_footprint_bench() {
    const ROWS: u128 = 200_000;
    const SLICE: u128 = 10_000;

    let mut fx = Fixture::start_with(WORKERS, &[("GNITZ_REPLY_FRAME_BUDGET", "65536")]);
    sql(
        &mut fx.direct,
        "s",
        "CREATE TABLE big (id BIGINT NOT NULL PRIMARY KEY, body TEXT NOT NULL)",
    );
    let big = block_on(fx.direct.resolve_relation("s", "big")).expect("resolve");
    for lo in (0..ROWS).step_by(SLICE as usize) {
        let mut batch = gnitz_core::ZSetBatch::new(&big.schema);
        let mut rows = gnitz_core::BatchAppender::new(&mut batch);
        for id in lo..lo + SLICE {
            // Distinct per row, or a shard stores the column as one value.
            rows.add_row(id, 1).str_val(&format!("{id:0>200}"));
        }
        block_on(
            fx.direct
                .push(big.tid, &big.schema, &batch, gnitz_wire::WireConflictMode::Update),
        )
        .expect("push");
    }
    fed_view(&mut fx.direct, "s", "v_big", "SELECT id, body FROM big WHERE id >= 0");

    // The fixture's own store is at the default tier; this one is far below the view.
    fx.mirror = None;
    let dir = fx.base_dir();
    let mut config = MirrorConfig::default();
    config.store.ram_tier_bytes = 256 * 1024;
    let mut mirror = GnitzClient::connect(fx.server.sock_path()).unwrap();
    mirror
        .attach_mirror(Mirror::open(&dir, config).expect("a store opens"))
        .expect("a fresh client attaches it");

    let resident = perf::Resident::baseline();
    block_on(mirror.mirror_view("s", "v_big")).expect("mirror");
    let peak = resident.as_ref().map(perf::Resident::peak_added);
    block_on(mirror.checkpoint_mirror()).expect("checkpoint");
    let held = resident.as_ref().map(perf::Resident::added);

    print!(
        "{}",
        gnitz_store::relation::disk_usage(&support::common::root(&dir)).expect("the copy's directory")
    );
    match held.zip(peak) {
        Some((held, peak)) => println!("{ROWS} rows mirrored: host RSS +{held} held, +{peak} at the bootstrap's peak"),
        None => println!(
            "{ROWS} rows mirrored: host RSS n/a without {}",
            perf::PIN_MMAP_THRESHOLD
        ),
    }
}
