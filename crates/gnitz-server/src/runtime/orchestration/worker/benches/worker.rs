use super::tests::addressed;
use super::tests::frames;
use super::tests::pk_keys;
use super::tests::probe;
use super::tests::table_cols;
use super::tests::worker_over;
use super::*;
use crate::runtime::sal::fixtures::TestLog;
use crate::runtime::sal::{DirectGroup, GroupData, GroupTargets};
use crate::runtime::w2m::W2mReceiver;
use crate::test_support::make_batch_raw;
use gnitz_expr::SchemaFacts;
use gnitz_wire::PkColList;
use gnitz_zset::schema::SchemaDescriptor;

/// [`test_worker`] over a SAL and a ring that hold a whole bench arm.
fn bench_worker(catalog: &mut CatalogEngine) -> (WorkerProcess<'_>, TestLog, W2mReceiver) {
    worker_over(catalog, 1 << 26, 32 << 20)
}

/// Writes a bench arm's groups to the SAL: its table's schema record, schema and id.
type WriteGroups<'a> = &'a dyn Fn(&TestLog, &[u8], &SchemaDescriptor, u64);

/// Groups each arm of [`worker_request_bench`] writes.
const BENCH_GROUPS: u64 = 10_000;

/// Rows a chunk of the bench's span train holds.
const BENCH_CHUNK_ROWS: u64 = 1024;

/// The worker's own request path, in instructions per request: a group read off
/// the SAL, decoded, applied or answered, and its reply on the ring.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn worker_request_bench() {
    use gnitz_store::relation::IndexClaim;
    use gnitz_wire::{PkKeys, ReadBound, ReadSpec};
    use std::hint::black_box;

    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let cols = table_cols();

    // One arm: a fresh table of `rows` rows, the groups `write` puts on the SAL,
    // and `drain_sal` until nothing is owed. `per` is what the count is divided by.
    let arm = |label: &str, rows: u64, index: bool, write: WriteGroups, per: &dyn Fn(usize) -> (u64, &'static str)| {
        if std::env::var("GNITZ_BENCH_ARM").is_ok_and(|only| only != label) {
            return;
        }
        let dir = crate::test_support::scratch_dir("worker", &format!("request_bench_{label}"));
        let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
        let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
        let schema = engine.registry.relation(tid).unwrap().schema();
        engine.registry.set_scan_chunk_rows(BENCH_CHUNK_ROWS as usize);
        if index {
            let claim = IndexClaim::Index { id: tid + 1, unique: false };
            engine
                .registry
                .add_index(tid, claim, PkColList::from_slice(&[1]))
                .unwrap();
        }
        if rows > 0 {
            let held: Vec<_> = (0..rows).map(|i| (i, 1, i as i64)).collect();
            engine.registry.ingest(tid, make_batch_raw(&schema, &held)).unwrap();
        }
        let record = engine.schema_record(tid).expect("a registered table");
        let (mut wp, sal, rx) = bench_worker(&mut engine);
        write(&sal, &record, &schema, tid);
        let ((), instructions) = counter.measure(|| loop {
            wp.drain_sal();
            if wp.replies.is_empty() {
                break;
            }
        });
        let sent = frames(&rx).len();
        black_box(&wp);
        let (n, unit) = per(sent);
        println!(
            "worker_request_bench {label:<14} {:>9.1} instr/{unit}  ({sent} frames)",
            instructions as f64 / n as f64
        );
        drop(wp);
        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
    };
    let per_group = |_: usize| (BENCH_GROUPS, "request");

    arm(
        "push",
        0,
        false,
        &|sal, record, schema, tid| {
            let excl = sal.excl();
            for i in 0..BENCH_GROUPS {
                let row = make_batch_raw(schema, &[(i, 1, i as i64)]);
                let targets = GroupTargets {
                    request_id: i as u32 + 1,
                    ..GroupTargets::UNADDRESSED
                };
                excl.write(&DirectGroup::push(
                    tid,
                    record,
                    GroupData::Same(row.wire_whole()),
                    targets,
                ))
                .expect("group fits");
            }
        },
        &per_group,
    );
    for (label, cut) in [("has_pk", 1), ("has_pk_cut2", 2)] {
        arm(
            label,
            1024,
            false,
            &|sal, _, _, tid| {
                let excl = sal.excl();
                for i in 0..BENCH_GROUPS {
                    excl.write(&probe(tid, &pk_keys(&[i % 1024]), i as u32 + 1, i % cut != 0))
                        .expect("group fits");
                }
            },
            &per_group,
        );
    }
    arm(
        "scan_spec",
        1024,
        false,
        &|sal, _, schema, tid| {
            let excl = sal.excl();
            for i in 0..BENCH_GROUPS {
                let key = (i % 1024).to_be_bytes();
                let spec = ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_keys(8, [&key[..]]))).encode();
                let read = Read::ScanSpec {
                    tid,
                    reply_layout: schema.layout_digest(),
                    spec: spec.into(),
                };
                excl.write(&addressed(read, i as u32 + 1, false)).expect("group fits");
            }
        },
        &per_group,
    );
    arm(
        "tick",
        0,
        false,
        &|sal, _, _, tid| {
            let excl = sal.excl();
            for i in 0..BENCH_GROUPS {
                let tick = Apply::Tick {
                    first_round: i + 2,
                    tids: tid.to_le_bytes().to_vec().into(),
                };
                excl.write(&addressed(tick, i as u32 + 1, false)).expect("group fits");
            }
        },
        &per_group,
    );
    // One train over an indexed table of 64 chunks, per frame.
    arm(
        "key_spans",
        64 * BENCH_CHUNK_ROWS,
        true,
        &|sal, _, _, tid| {
            let cols = PkColList::from_slice(&[1]);
            sal.excl()
                .write(&addressed(Read::KeySpans { tid, cols }, 1, false))
                .expect("group fits");
        },
        &|frames| (frames as u64, "frame"),
    );
}
