use super::tests::{ask, frames, pk_keys, probe, worker_over};
use super::*;
use crate::runtime::sal::{SalExcl, SalFull};
use crate::runtime::w2m::fixtures::id;
use crate::test_support::{
    make_batch_raw, make_schema_u64_i64, net_weight, register_identity_view, table_engine, u64_i64_cols,
};
use gnitz_foundation::perf::Counter;
use gnitz_store::relation::IndexClaim;
use gnitz_store::relation::PkRule;
use gnitz_wire::{PkColList, PkKeys, ReadBound, ReadSpec, WireStatus};

/// Requests an arm writes, unless it says otherwise: no more ACKs than an ACK
/// ring holds unread, since nothing drains it while the arm runs.
const REQUESTS: u32 = crate::runtime::w2m::ACK_RING_ENTRIES as u32;

/// Rows the table of a read arm holds.
const HELD: u64 = 1024;

/// One arm: a table `prepare` dresses and `held` rows are then ingested into,
/// `requests` groups on the SAL, each written by `request` from the table's id, its schema
/// record and the group's request id, and `drain_sal` until nothing is owed.
/// The worker must answer in `frames` frames or ACKs, no frame a fault, and
/// leave the table at net weight `holds`.
fn arm(
    label: &str,
    held: u64,
    prepare: impl FnOnce(&mut CatalogEngine, u64),
    requests: u32,
    request: impl Fn(&mut SalExcl, u64, &[u8], u32) -> Result<(), SalFull>,
    frames_owed: usize,
    holds: u64,
) {
    let (mut engine, tid) = table_engine("worker", &format!("request_bench_{label}"), 1, &u64_i64_cols());
    prepare(&mut engine, tid);
    let rows: Vec<_> = (0..held).map(|i| (i, 1, i as i64)).collect();
    let batch = make_batch_raw(&make_schema_u64_i64(), &rows);
    engine.registry.ingest(tid, batch).unwrap();
    let record = engine.wire_target(tid).unwrap().1.clone();
    let (mut wp, sal, rx) = worker_over(&mut engine, 1 << 26, 32 << 20);
    // Independent client requests: each under a hold of its own.
    for id in 1..=requests {
        request(&mut sal.excl(), tid, &record, id).expect("group fits");
    }
    let ((), instructions) = Counter::instructions().measure(|| loop {
        wp.drain_sal();
        if wp.replies.is_empty() {
            break;
        }
    });
    let sent = frames(&rx);
    assert_eq!(sent.len() + rx.acks(0).len(), frames_owed, "{label}");
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
/// arm but `spans` answers a request in one frame.
///
/// `ack` is a push that carries no rows, the floor under the others. `tick` runs
/// one identity view over an empty delta. `spans` is one train over the table's
/// second column, a chunk of spans per frame, read off an index of that column.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn worker_request_bench() {
    const CHUNK_ROWS: usize = 1024;
    const CHUNKS: usize = 64;

    let schema = make_schema_u64_i64();
    let n = REQUESTS as usize;
    let bare = |_: &mut CatalogEngine, _: u64| ();

    let ack = |excl: &mut SalExcl, tid, _: &[u8], n| excl.apply(Apply::Push { tid, rule: PkRule::Enforce }, id(n));
    arm("ack", 0, bare, REQUESTS, ack, n, 0);

    let one_row = |excl: &mut SalExcl, tid, record: &[u8], n: u32| {
        let row = make_batch_raw(&schema, &[(n as u64, 1, n as i64)]);
        excl.acked_push(tid, record, &row, id(n))
    };
    arm("push", 0, bare, REQUESTS, one_row, n, REQUESTS as u64);

    let has_pk = |excl: &mut SalExcl, tid, _: &[u8], n: u32| ask(excl, probe(tid, &pk_keys(&[n as u64 % HELD])), n);
    arm("has_pk", HELD, bare, REQUESTS, has_pk, n, HELD);

    let scan = |excl: &mut SalExcl, tid, _: &[u8], id: u32| {
        let key = (id as u64 % HELD).to_be_bytes();
        let read = Read::ScanSpec {
            tid,
            reply_layout: schema.layout_digest(),
            spec: ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_keys(8, [&key[..]])))
                .encode()
                .into(),
        };
        ask(excl, read, id)
    };
    arm("scan_spec", HELD, bare, REQUESTS, scan, n, HELD);

    let viewed = |engine: &mut CatalogEngine, tid| {
        register_identity_view(engine, tid, "v", &u64_i64_cols());
    };
    let tick = |excl: &mut SalExcl, tid, _: &[u8], n: u32| {
        let tick = Apply::Tick {
            first_round: n as u64 + 1,
            tids: vec![tid].into(),
        };
        excl.apply(tick, id(n))
    };
    arm("tick", 0, viewed, REQUESTS, tick, n, 0);

    let cols = PkColList::from_slice(&[1]);
    let indexed = |engine: &mut CatalogEngine, tid| {
        engine.registry.set_scan_chunk_rows(CHUNK_ROWS);
        let claim = IndexClaim::Index { id: tid + 1, unique: false };
        engine.registry.add_indexes(tid, &[(claim, cols)]).unwrap();
    };
    let spans = |excl: &mut SalExcl, tid, _: &[u8], n| ask(excl, Read::KeySpans { tid, cols, claim: false }, n);
    let held = (CHUNKS * CHUNK_ROWS) as u64;
    // A frame per chunk, then the train's end.
    arm("spans", held, indexed, 1, spans, CHUNKS + 1, held);
}
