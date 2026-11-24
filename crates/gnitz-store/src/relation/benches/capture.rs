//! What capturing a view's delta for its feed costs: the same ingests into a
//! fed view and into a plain one, differenced per cell.
//!
//! `cd crates && TMPDIR=<a disk-backed dir> cargo test -p gnitz-store --release delta_capture_bench -- --ignored --nocapture --test-threads=1`
//!
//! `GNITZ_CAPTURE_BUDGETS_MIB` picks the feed budgets (default `4,64,256`), and
//! `GNITZ_CAPTURE_ROUND_ROWS` the round sizes (default `1,100,8192`). A cell
//! ingests twice the RAM tier plus the budget, so it crosses both.

use crate::relation::{RelationKind, StoreConfig};
use crate::test_support::{RelationFixture, TID, fed_view, id_rows, relation_fixture, u64_pk_schema};
use gnitz_foundation::perf::Counter;
use gnitz_wire::{ReadBound, ReadSpec, TypeCode, ViewBudget};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

const STRING: &str = "a-payload-string-of-forty-characters-xxx";

/// `id U64 PK | I64 | I64 | I64`, or `id U64 PK | I64 | STRING`.
fn schema(strings: bool) -> SchemaDescriptor {
    let c = SchemaColumn::new;
    let i = c(TypeCode::I64, false);
    match strings {
        false => u64_pk_schema(&[i, i, i]),
        true => u64_pk_schema(&[i, c(TypeCode::String, false)]),
    }
}

/// One round's delta: `rows` fresh ids from `first`.
fn round(strings: bool, first: u64, rows: u64) -> Batch {
    id_rows(&schema(strings), (first..first + rows).map(|id| (id, 1)), |bb, id| {
        bb.put_u64(id % 512);
        match strings {
            false => {
                bb.put_u64(id);
                bb.put_u64(!id);
            }
            true => {
                bb.put_string(STRING);
            }
        }
    })
}

/// Bytes this process has sent to storage so far.
fn written_bytes() -> u64 {
    let io = std::fs::read_to_string("/proc/self/io").expect("read /proc/self/io");
    io.lines()
        .find_map(|l| l.strip_prefix("write_bytes: ")?.parse().ok())
        .expect("no write_bytes in /proc/self/io")
}

/// `rounds` rounds of `per_round` rows landed into a fresh `kind` view, with
/// the instructions the landings took and the bytes they sent to storage.
fn run(
    counter: &Counter,
    kind: RelationKind,
    strings: bool,
    rounds: u64,
    per_round: u64,
) -> (RelationFixture, u64, u64) {
    let mut r = relation_fixture(kind, schema(strings), &[], []);
    let (mut instr, written) = (0, written_bytes());
    for n in 0..rounds {
        let batch = round(strings, n * per_round, per_round);
        instr += counter.measure(|| r.land_round(batch, 2 + n)).1;
    }
    let written = written_bytes() - written;
    (r, instr, written)
}

fn env_list(name: &str, default: &[u64]) -> Vec<u64> {
    match std::env::var(name) {
        Ok(v) => v.split(',').map(|x| x.trim().parse().unwrap()).collect(),
        Err(_) => default.to_vec(),
    }
}

/// One line per cell: the plain view's instructions per row, what the feed adds
/// to them and to the bytes written, and what the feed still serves after.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn delta_capture_bench() {
    let counter = Counter::instructions();
    let ram_tier = StoreConfig::default().ram_tier_bytes as u64;
    for budget_mib in env_list("GNITZ_CAPTURE_BUDGETS_MIB", &[4, 64, 256]) {
        let budget = budget_mib << 20;
        for (strings, schema_name) in [(false, "ints"), (true, "strings")] {
            for per_round in env_list("GNITZ_CAPTURE_ROUND_ROWS", &[1, 100, 8192]) {
                // A 256 MiB cell of one-row rounds is 12M ingests of nothing new.
                if budget_mib > 64 && per_round == 1 {
                    continue;
                }
                let row_bytes = round(strings, 0, 64).total_bytes() as u64 / 64;
                let rounds = (2 * (ram_tier + budget) / row_bytes).div_ceil(per_round);
                let rows = rounds * per_round;
                let (_, plain, plain_written) = run(
                    &counter,
                    RelationKind::View(ViewBudget::Plain),
                    strings,
                    rounds,
                    per_round,
                );
                let (fed, fed_instr, fed_written) = run(&counter, fed_view(budget), strings, rounds, per_round);

                let floor = fed.relation(TID).unwrap().feed().unwrap().dropped_through().max(1);
                let spec = ReadSpec::all_rows(ReadBound::None);
                let kept = fed
                    .delta_read(TID, floor, spec, schema(strings).layout_digest())
                    .unwrap();
                println!(
                    "delta_capture_bench schema={schema_name} rows_per_round={per_round} budget_mib={budget_mib} \
                     rows={rows} plain_instr_per_row={:.1} capture_instr_per_row={:.1} capture_write_bytes={} \
                     floor={floor} readable_rows={} readable_bytes={}",
                    plain as f64 / rows as f64,
                    (fed_instr as f64 - plain as f64) / rows as f64,
                    fed_written as i64 - plain_written as i64,
                    kept.len(),
                    kept.total_bytes(),
                );
            }
        }
    }
}
