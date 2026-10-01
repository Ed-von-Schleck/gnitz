#![cfg(feature = "integration")]

//! The tokio client against a real server: what needs real replies — verbs
//! pipelined over both transports, the syscalls a burst costs, and the blocking
//! client every clone shares.

mod support;

use std::ops::Range;
use std::sync::Arc;

use gnitz_core::{BatchAppender, GnitzClient, PkColumn, PollResult, Schema, ZSetBatch};
use gnitz_mirror::Mirror;
use gnitz_test_harness::{strace_test, unique_schema, ServerHandle};
use gnitz_wire::{ColumnDef, ReadBound, ReadSpec, TableProps, TypeCode, ViewProps, WireConflictMode};
use support::submitted;
use tokio::runtime::Runtime;

/// `(pk BIGINT, a BIGINT)`.
fn schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::I64, false),
            ColumnDef::new("a", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    }
}

/// A [`schema`] table `t`: the blocking client that made it, its id, its
/// schema, and the schema name it lives under.
fn table(target: &str) -> (GnitzClient, u64, Arc<Schema>, String) {
    let mut client = GnitzClient::connect(target).expect("connect");
    let sn = unique_schema("tokio");
    client.create_schema(&sn).unwrap();
    let schema = schema();
    let tid = client
        .create_table(&sn, "t", &schema, &[], TableProps::default(), &[])
        .unwrap();
    (client, tid, Arc::new(schema), sn)
}

/// `pks` at weight 1.
fn rows(schema: &Schema, pks: Range<i64>) -> ZSetBatch {
    let mut batch = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut batch, schema);
    for pk in pks {
        app.add_row(pk as u128, 1).i64_val(pk * 3);
    }
    batch
}

/// How many rows a read returned, every one of them at weight 1. Every relation
/// here is keyed uniquely, so a row counted twice is one row at weight 2.
fn unit_rows(batch: &ZSetBatch) -> usize {
    assert!(batch.weights.iter().all(|w| *w == 1), "{:?}", batch.weights);
    batch.len()
}

fn all_rows() -> ReadSpec {
    ReadSpec::all_rows(ReadBound::None)
}

// ── Pipelining ────────────────────────────────────────────────────────────

/// Many verbs in flight on one connection: each resolves to its own reply, and
/// the server takes them in the order they were submitted.
fn pipelined_verbs(target: &str) {
    let (_blocking, tid, schema, sn) = table(target);
    Runtime::new().unwrap().block_on(async {
        let (client, conn) = gnitz_tokio::connect(target).await.expect("connect");
        let driver = tokio::spawn(conn);

        // Megabytes each way, so neither direction fits a socket buffer: writes
        // resume after the socket refused them and replies span many reads.
        let (n, per) = (40, 5_000);
        let mut rounds = Vec::new();
        for i in 0..n {
            rounds.push((
                submitted(client.push(tid, Arc::clone(&schema), rows(&schema, i * per..(i + 1) * per))).await,
                submitted(client.scan_spec(tid, all_rows(), Arc::clone(&schema))).await,
            ));
        }
        for (i, (push, scan)) in rounds.into_iter().enumerate() {
            assert!(push.await.unwrap() > 0, "a push resolves to its ingest LSN");
            assert_eq!(
                unit_rows(&scan.await.unwrap().batch),
                (i + 1) * per as usize,
                "a scan sees exactly the pushes submitted ahead of it"
            );
        }

        // A verb dropped after its submit is not cancelled: the server commits it.
        let last = n * per;
        drop(submitted(client.push(tid, Arc::clone(&schema), rows(&schema, last..last + 1))).await);
        let total = last as usize + 1;

        // Every kind of verb in flight together.
        let key = PkColumn::from_natives(&schema, [7]);
        let (one, many, found, missing) = tokio::join!(
            client.scan_spec(
                tid,
                ReadSpec::all_rows(ReadBound::PkSet(key.keys())),
                Arc::clone(&schema)
            ),
            client.scan_many(vec![(tid, Arc::clone(&schema)), (tid, Arc::clone(&schema))]),
            client.resolve(&sn, "t"),
            client.resolve(&sn, "nope"),
        );
        let one = one.unwrap().batch;
        assert_eq!((unit_rows(&one), one.pks.get(&schema, 0)), (1, 7));
        let many: Vec<usize> = many.unwrap().iter().map(|r| unit_rows(&r.batch)).collect();
        assert_eq!(many, [total, total]);
        let found = found.unwrap().expect("the table exists");
        assert_eq!((found.tid, &found.schema), (tid, &schema));
        assert!(missing.unwrap().is_none());

        drop(client);
        driver.await.unwrap();
    });
}

#[test]
fn pipelined_verbs_unix() {
    let srv = ServerHandle::start_n(4);
    pipelined_verbs(srv.sock_path());
}

#[test]
fn pipelined_verbs_tls() {
    let srv = ServerHandle::start_tls(4);
    pipelined_verbs(&srv.tls_target());
}

// ── Syscalls per burst ────────────────────────────────────────────────────

/// The child half of [`one_writev_per_burst`]: `GNITZ_TOKIO_N` pushes issued as
/// one burst from one task on a `current_thread` runtime. Run under
/// `strace -f -c` by the parent; a no-op otherwise.
#[test]
fn syscall_count_child() {
    let (Ok(target), Ok(tid), Ok(n)) = (
        std::env::var("GNITZ_TOKIO_TARGET"),
        std::env::var("GNITZ_TOKIO_TID"),
        std::env::var("GNITZ_TOKIO_N"),
    ) else {
        return;
    };
    let tid: u64 = tid.parse().unwrap();
    let n: i64 = n.parse().unwrap();
    let schema = Arc::new(schema());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (client, conn) = gnitz_tokio::connect(&target).await.unwrap();
        // One task beside the driver, so all N are submitted before the driver
        // is polled again.
        let burst = async move {
            let mut pushes = Vec::new();
            for pk in 0..n {
                pushes.push(submitted(client.push(tid, Arc::clone(&schema), rows(&schema, pk..pk + 1))).await);
            }
            for push in pushes {
                push.await.unwrap();
            }
            // The last handle goes, which is what lets the driver finish.
        };
        tokio::join!(conn, burst);
    });
}

/// A burst of N pushes issued together leaves in one `writev`, not N: the
/// driver drains the whole channel before it steps.
#[test]
fn one_writev_per_burst() {
    let srv = ServerHandle::start();
    let (mut blocking, tid, schema, _sn) = table(srv.sock_path());
    // Within tokio's per-poll budget: past it a submit suspends, and the burst
    // legitimately leaves in more than one piece.
    let n = 100;
    let Some(counts) = strace_test(
        "syscall_count_child",
        &[
            ("GNITZ_TOKIO_TARGET", srv.sock_path()),
            ("GNITZ_TOKIO_TID", &tid.to_string()),
            ("GNITZ_TOKIO_N", &n.to_string()),
        ],
    ) else {
        eprintln!("strace not installed; skipping the syscall count");
        return;
    };
    let pushed = blocking.scan_spec(tid, &all_rows(), &schema).unwrap().batch;
    assert_eq!(unit_rows(&pushed), n, "the traced child pushed its burst");
    // The transport writes the socket through `write_vectored`; plain `write`
    // is the harness's own stdout. One for the HELLO, one for the burst.
    assert_eq!(
        counts.sum(&["writev", "sendto", "sendmsg"]),
        2,
        "the {n}-push burst did not leave in one writev:\n{}",
        counts.report
    );
}

// ── The shared blocking client ────────────────────────────────────────────

/// Every clone drives one blocking client, one call at a time, and a
/// local-first read routes on what that client mirrors.
#[test]
fn clones_share_one_blocking_client() {
    let srv = ServerHandle::start_n(4);
    let (mut blocking, tid, schema, sn) = table(srv.sock_path());
    let vid = blocking
        .create_view(&sn, "v", tid, ViewProps::Fed { delta_bytes: 8 << 20 })
        .unwrap();
    // A push, then a read against the server: the read drains the pending
    // ticks, so the rounds a mirror reads already exist.
    let mut push_and_tick = |pks| {
        blocking
            .push(tid, &schema, &rows(&schema, pks), WireConflictMode::Update)
            .unwrap();
        blocking.scan_spec(vid, &all_rows(), &schema).unwrap();
    };
    push_and_tick(0..50);

    Runtime::new().unwrap().block_on(async {
        let (client, conn) = gnitz_tokio::connect(srv.sock_path()).await.expect("connect");
        let driver = tokio::spawn(conn);
        {
            let read = |tid| client.scan_spec_local_first(tid, all_rows(), Arc::clone(&schema));

            // No blocking client yet, so nothing is mirrored: the server answers.
            let served = read(vid).await.unwrap();
            assert!(served.lsn.is_some(), "a served read carries the server's LSN");
            assert_eq!(unit_rows(&served.batch), 50);

            // One clone mirrors the view; every other reads off that copy.
            let dir = tempfile::tempdir().unwrap();
            let store = Mirror::open(dir.path().to_str().unwrap()).unwrap();
            let name = sn.clone();
            let mirrored = client
                .clone()
                .with_blocking_client(move |c| {
                    c.attach_mirror(store)?;
                    c.mirror_view(&name, "v")
                })
                .await
                .expect("mirror the view");
            assert_eq!(mirrored.view_id, vid);

            // Two clones polling at once. One whole advance runs under one lock,
            // so they serialize instead of both fetching the same interval and
            // both applying it, which would leave every new row at weight 2.
            push_and_tick(50..100);
            let (a, b) = (client.clone(), client.clone());
            let (polled_a, polled_b) = tokio::join!(
                a.with_blocking_client(GnitzClient::poll_mirror),
                b.with_blocking_client(GnitzClient::poll_mirror),
            );
            for outcome in polled_a.unwrap().into_iter().chain(polled_b.unwrap()) {
                assert!(matches!(outcome.result, PollResult::Advanced), "{outcome:?}");
            }
            let local = read(vid).await.unwrap();
            assert!(local.lsn.is_none(), "a local answer carries no served LSN");
            assert_eq!(unit_rows(&local.batch), 100);

            // A relation the copy does not hold still reads, over the wire.
            let unheld = read(tid).await.unwrap();
            assert!(unheld.lsn.is_some(), "a delegated read carries the server's LSN");
            assert_eq!(unit_rows(&unheld.batch), 100);
        }
        drop(client);
        driver.await.unwrap();
    });
}
