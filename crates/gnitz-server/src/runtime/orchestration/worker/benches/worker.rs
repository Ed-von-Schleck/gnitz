use super::tests::{addressed, engine_with_table, frames, pk_keys, probe, push, table_cols, worker_over};
use super::*;
use crate::runtime::sal::SalExcl;
use crate::test_support::{make_batch_raw, make_schema_u64_i64, net_weight, register_identity_view};
use gnitz_foundation::perf::Counter;
use gnitz_store::relation::IndexClaim;
use gnitz_wire::{PkColList, PkKeys, ReadBound, ReadSpec, WireStatus};

/// Requests an arm writes, unless it says otherwise.
const REQUESTS: u32 = 10_000;

/// Rows the table of a read arm holds.
const HELD: u64 = 1024;

/// One arm: a table `prepare` dresses and `held` rows are then ingested into,
/// `requests` groups on the SAL, each written by `request` from the table's id, its schema
/// record and the group's request id, and `drain_sal` until nothing is owed.
/// The worker must answer in `frames` frames, none a fault, and leave the table
/// at net weight `holds`.
fn arm(
    label: &str,
    held: u64,
    prepare: impl FnOnce(&mut CatalogEngine, u64),
    requests: u32,
    request: impl Fn(&SalExcl, u64, &[u8], u32) -> Result<(), WireFault>,
    frames_owed: usize,
    holds: u64,
) {
    let (mut engine, tid) = engine_with_table(&format!("request_bench_{label}"));
    prepare(&mut engine, tid);
    let rows: Vec<_> = (0..held).map(|i| (i, 1, i as i64)).collect();
    let batch = make_batch_raw(&make_schema_u64_i64(), &rows);
    engine.registry.ingest(tid, batch).unwrap();
    let record = engine.schema_record(tid).expect("a registered table");
    let (mut wp, sal, rx) = worker_over(&mut engine, 1 << 26, 32 << 20);
    let excl = sal.excl();
    for id in 1..=requests {
        request(&excl, tid, &record, id).expect("group fits");
    }
    let ((), instructions) = Counter::instructions().measure(|| loop {
        wp.drain_sal();
        if wp.replies.is_empty() {
            break;
        }
    });
    let sent = frames(&rx);
    assert_eq!(sent.len(), frames_owed, "{label}");
    assert!(sent.iter().all(|f| f.ctrl.hdr.status == WireStatus::Ok), "{label}");
    assert_eq!(net_weight(wp.catalog, tid), holds as i64, "{label}");
    println!(
        "worker_request_bench {label:<11} {:>9.1} instr/frame",
        instructions as f64 / frames_owed as f64
    );
    drop(wp);
    let dir = engine.registry.base_dir().to_owned();
    drop(engine);
    let _ = std::fs::remove_dir_all(dir);
}

/// The worker's own request path, in instructions per reply frame: a group read
/// off the SAL, decoded, applied or answered, and its reply on the ring. Every
/// arm but `key_spans` answers a request in one frame.
///
/// `ack` is a push that carries no rows, the floor under the others. `tick` runs
/// one identity view over an empty delta. The `spans` arms are one train over
/// the table's second column, a chunk of spans per frame: sorted out of the
/// table's rows, and read off an index of that column.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn worker_request_bench() {
    const CHUNK_ROWS: usize = 1024;
    const CHUNKS: usize = 64;

    let schema = make_schema_u64_i64();
    let n = REQUESTS as usize;
    let bare = |_: &mut CatalogEngine, _: u64| ();

    let ack = |excl: &SalExcl, tid, _: &[u8], id| excl.write(&addressed(Apply::Push { tid }, id, false));
    arm("ack", 0, bare, REQUESTS, ack, n, 0);

    let one_row = |excl: &SalExcl, tid, record: &[u8], id: u32| {
        let row = make_batch_raw(&schema, &[(id as u64, 1, id as i64)]);
        excl.write(&push(tid, record, &row, id))
    };
    arm("push", 0, bare, REQUESTS, one_row, n, REQUESTS as u64);

    let has_pk =
        |excl: &SalExcl, tid, _: &[u8], id: u32| excl.write(&probe(tid, &pk_keys(&[id as u64 % HELD]), id, false));
    arm("has_pk", HELD, bare, REQUESTS, has_pk, n, HELD);

    let scan = |excl: &SalExcl, tid, _: &[u8], id: u32| {
        let key = (id as u64 % HELD).to_be_bytes();
        let read = Read::ScanSpec {
            tid,
            reply_layout: schema.layout_digest(),
            spec: ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_keys(8, [&key[..]])))
                .encode()
                .into(),
        };
        excl.write(&addressed(read, id, false))
    };
    arm("scan_spec", HELD, bare, REQUESTS, scan, n, HELD);

    let viewed = |engine: &mut CatalogEngine, tid| {
        register_identity_view(engine, tid, "v", &table_cols());
    };
    let tick = |excl: &SalExcl, tid, _: &[u8], id: u32| {
        let tick = Apply::Tick {
            first_round: id as u64 + 1,
            tids: vec![tid].into(),
        };
        excl.write(&addressed(tick, id, false))
    };
    arm("tick", 0, viewed, REQUESTS, tick, n, 0);

    let cols = PkColList::from_slice(&[1]);
    let chunked = |engine: &mut CatalogEngine, _| engine.registry.set_scan_chunk_rows(CHUNK_ROWS);
    let indexed = |engine: &mut CatalogEngine, tid| {
        chunked(engine, tid);
        let claim = IndexClaim::Index { id: tid + 1, unique: false };
        engine.registry.add_index(tid, claim, cols).unwrap();
    };
    let spans = |excl: &SalExcl, tid, _: &[u8], id| excl.write(&addressed(Read::KeySpans { tid, cols }, id, false));
    let held = (CHUNKS * CHUNK_ROWS) as u64;
    // A frame per chunk, then the train's end.
    arm("spans_sort", held, chunked, 1, spans, CHUNKS + 1, held);
    arm("spans_index", held, indexed, 1, spans, CHUNKS + 1, held);
}
