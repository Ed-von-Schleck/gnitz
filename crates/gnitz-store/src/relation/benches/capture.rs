//! What capturing a view's delta for its feed costs, as the difference between
//! the same ingests into a fed view and into a plain one.
//!
//! `cd crates && TMPDIR=<a disk-backed dir> GLIBC_TUNABLES=glibc.malloc.mmap_threshold=131072 \
//!     cargo test -p gnitz-store --release delta_capture_bench -- --ignored --nocapture --test-threads=1`
//!
//! `GNITZ_CAPTURE_BUDGETS_MIB` picks the feed budgets (default `4,64,256`), and
//! `GNITZ_CAPTURE_ROUND_ROWS` the round sizes (default `1,100,8192`). A cell
//! ingests twice the RAM tier plus the budget, so it crosses both.

use std::hint::black_box;

use crate::relation::RelationKind;
use crate::test_support::{relation_fixture, RelationFixture, TID};
use gnitz_expr::SchemaFacts;
use gnitz_foundation::perf::{Counter, Resident};
use gnitz_wire::{ReadBound, ReadSpec, TypeCode, ViewProps, WireStatus};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

const STRING: &str = "a-payload-string-of-forty-characters-xxx";

/// `id U64 PK | I64 | I64 | I64`, or `id U64 PK | I64 | STRING`.
fn schema(strings: bool) -> SchemaDescriptor {
    let c = SchemaColumn::new;
    let i = c(TypeCode::I64, false);
    match strings {
        false => SchemaDescriptor::new(&[c(TypeCode::U64, false), i, i, i], &[0]),
        true => SchemaDescriptor::new(&[c(TypeCode::U64, false), i, c(TypeCode::String, false)], &[0]),
    }
}

/// One round's delta: `rows` fresh ids from `first`.
fn round(s: &SchemaDescriptor, strings: bool, first: u64, rows: u64) -> Batch {
    let mut bb = BatchBuilder::new(s);
    for id in first..first + rows {
        bb.begin_row(id as u128, 1);
        bb.put_u64(id % 512);
        match strings {
            false => {
                bb.put_u64(id);
                bb.put_u64(!id);
            }
            true => bb.put_string(STRING),
        }
        bb.end_row();
    }
    bb.finish()
}

/// `(rchar-independent) wchar, write_bytes` of this process so far.
fn io_written() -> (u64, u64) {
    let io = std::fs::read_to_string("/proc/self/io").expect("read /proc/self/io");
    let field = |name: &str| {
        io.lines()
            .find_map(|l| l.strip_prefix(name)?.strip_prefix(": ")?.parse::<u64>().ok())
            .unwrap_or_else(|| panic!("no {name} in /proc/self/io"))
    };
    (field("wchar"), field("write_bytes"))
}

struct Run {
    instr: u64,
    wchar: u64,
    write_bytes: u64,
    peak: Option<u64>,
    held: Option<u64>,
    fixture: RelationFixture,
}

fn run(counter: &Counter, kind: RelationKind, strings: bool, rounds: u64, per_round: u64) -> Run {
    let s = schema(strings);
    let resident = Resident::baseline();
    let (w0, b0) = io_written();
    let mut r = relation_fixture(kind, s, &[], []);
    let mut instr = 0;
    for n in 0..rounds {
        let batch = round(&s, strings, n * per_round, per_round);
        instr += counter
            .measure(|| r.ingest_at(TID, batch, Some(2 + n), false).unwrap())
            .1;
    }
    let (w1, b1) = io_written();
    Run {
        instr,
        wchar: w1 - w0,
        write_bytes: b1 - b0,
        peak: resident.as_ref().map(Resident::peak_added),
        held: resident.as_ref().map(Resident::added),
        fixture: r,
    }
}

/// The lowest cursor the feed still serves, and the bytes of the rows past it.
fn readable(r: &RelationFixture, strings: bool, last_round: u64) -> (u64, usize, usize) {
    let layout = schema(strings).layout_digest();
    let read = |after: u64| r.delta_read(TID, after, ReadSpec::all_rows(ReadBound::None), layout);
    let (mut lo, mut hi) = (1, last_round);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match read(mid) {
            Ok(_) => hi = mid,
            Err(f) => {
                assert!(matches!(f.status, WireStatus::DeltaExpired), "{}", f.text);
                lo = mid + 1;
            }
        }
    }
    let rows = read(lo).unwrap();
    (lo, rows.len(), rows.total_bytes())
}

fn env_list(name: &str, default: &[u64]) -> Vec<u64> {
    match std::env::var(name) {
        Ok(v) => v.split(',').map(|x| x.trim().parse().unwrap()).collect(),
        Err(_) => default.to_vec(),
    }
}

/// One line per (cell, kind), `key=value`, for a driver to difference. A
/// resident-set reading means something only in a process that ran one line:
/// `GNITZ_CAPTURE_KIND=fed|plain` and `GNITZ_CAPTURE_SCHEMA=ints|strings` pick it.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn delta_capture_bench() {
    let counter = Counter::instructions();
    let ram_tier = crate::relation::StoreConfig::default().ram_tier_bytes as u64;
    let only = |name: &str, value: &str| std::env::var(name).map_or(true, |v| v == value);
    for budget_mib in env_list("GNITZ_CAPTURE_BUDGETS_MIB", &[4, 64, 256]) {
        let budget = budget_mib << 20;
        for (strings, schema_name) in [(false, "ints"), (true, "strings")] {
            for per_round in env_list("GNITZ_CAPTURE_ROUND_ROWS", &[1, 100, 8192]) {
                // A 256 MiB cell of one-row rounds is 12M ingests of nothing new.
                if (budget_mib > 64 && per_round == 1) || !only("GNITZ_CAPTURE_SCHEMA", schema_name) {
                    continue;
                }
                let row_bytes = round(&schema(strings), strings, 0, 64).total_bytes() as u64 / 64;
                let rounds = (2 * (ram_tier + budget) / row_bytes).div_ceil(per_round);
                let rows = rounds * per_round;
                let fed = RelationKind::View(ViewProps::Fed {
                    delta_bytes: std::num::NonZeroU64::new(budget).unwrap(),
                });
                for (kind, kind_name) in [(RelationKind::View(ViewProps::Plain), "plain"), (fed, "fed")] {
                    if !only("GNITZ_CAPTURE_KIND", kind_name) {
                        continue;
                    }
                    let ran = run(&counter, kind, strings, rounds, per_round);
                    let (floor, kept_rows, kept_bytes) = match kind_name {
                        "fed" => readable(&ran.fixture, strings, 1 + rounds),
                        _ => (0, 0, 0),
                    };
                    black_box(&ran.fixture);
                    println!(
                        "delta_capture_bench kind={kind_name} schema={schema_name} rows_per_round={per_round} \
                         budget_mib={budget_mib} rounds={rounds} rows={rows} ingested_bytes={} instr={} wchar={} \
                         write_bytes={} peak_rss={} held_rss={} floor={floor} readable_rows={kept_rows} \
                         readable_bytes={kept_bytes}",
                        rows * row_bytes,
                        ran.instr,
                        ran.wchar,
                        ran.write_bytes,
                        ran.peak.map_or(-1, |b| b as i64),
                        ran.held.map_or(-1, |b| b as i64),
                    );
                }
            }
        }
    }
}

/// What a fed view that captured nothing, and then one row, holds resident:
/// one line per budget, each meaningful in a process of its own.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn delta_idle_bench() {
    for budget_mib in env_list("GNITZ_CAPTURE_BUDGETS_MIB", &[4, 64, 256]) {
        let resident = Resident::baseline().expect("run under GLIBC_TUNABLES=glibc.malloc.mmap_threshold=131072");
        let fed = RelationKind::View(ViewProps::Fed {
            delta_bytes: std::num::NonZeroU64::new(budget_mib << 20).unwrap(),
        });
        let mut r = relation_fixture(fed, schema(false), &[], []);
        let idle = resident.added();
        r.ingest_at(TID, round(&schema(false), false, 0, 1), Some(2), false)
            .unwrap();
        let one_row = resident.added();
        black_box(&r);
        println!("delta_idle_bench budget_mib={budget_mib} idle_rss={idle} one_row_rss={one_row}");
    }
}
