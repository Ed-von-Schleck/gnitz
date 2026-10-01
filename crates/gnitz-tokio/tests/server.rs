#![cfg(feature = "integration")]

//! The tokio client against a real server: what needs real replies — verbs
//! pipelined over both transports, the syscalls a burst costs, and the blocking
//! client every clone shares.

mod support;

use std::ops::Range;
use std::sync::Arc;

use gnitz_core::{BatchAppender, ClientError, GnitzClient, PkColumn, PollResult, Schema, ZSetBatch};
use gnitz_mirror::Mirror;
use gnitz_test_harness::{strace_test, unique_schema, ServerHandle};
use gnitz_wire::{read_i64_le, ColumnDef, ReadBound, ReadSpec, TableProps, TypeCode, ViewProps, WireConflictMode};
use support::settled;
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

/// Many verbs in flight on one connection, over each transport: each resolves
/// to its own reply, and the server takes them in the order they were called.
#[test]
fn pipelined_verbs() {
    let srv = ServerHandle::start_tls(4);
    for target in [srv.sock_path().to_string(), srv.tls_target()] {
        // Shown only if the test fails.
        eprintln!("pipelining over {target}");
        let (_blocking, tid, schema, sn) = table(&target);
        let (_, empty, ..) = table(&target);
        settled(&Runtime::new().unwrap(), async {
            let (client, conn) = gnitz_tokio::connect(&target).await.expect("connect");
            let driver = tokio::spawn(conn);

            // Megabytes each way, so neither direction fits a socket buffer: writes
            // resume after the socket refused them and replies span many reads.
            let (n, per) = (40, 5_000);
            let rounds: Vec<_> = (0..n)
                .map(|i| {
                    (
                        client.push(tid, &schema, &rows(&schema, i * per..(i + 1) * per)),
                        client.scan_spec(tid, &all_rows(), &schema),
                    )
                })
                .collect();
            for (i, (push, scan)) in rounds.into_iter().enumerate() {
                assert!(push.await.unwrap() > 0, "a push resolves to its ingest LSN");
                assert_eq!(
                    unit_rows(&scan.await.unwrap().batch),
                    (i + 1) * per as usize,
                    "a scan sees exactly the pushes called ahead of it"
                );
            }

            // Every kind of verb in flight together, behind a push that replaces
            // a row rather than adding to it.
            let mut replaced = ZSetBatch::new(&schema);
            BatchAppender::new(&mut replaced, &schema).add_row(7, 1).i64_val(-1);
            let key = PkColumn::from_natives(&schema, [7]);
            let (_, one, many, found, missing) = tokio::join!(
                client.push(tid, &schema, &replaced),
                client.scan_spec(tid, &ReadSpec::all_rows(ReadBound::PkSet(key.keys())), &schema),
                client.scan_many(&[(tid, &schema), (empty, &schema)]),
                client.resolve(&sn, "t"),
                client.resolve(&sn, "nope"),
            );
            let one = one.unwrap().batch;
            assert_eq!(unit_rows(&one), 1);
            assert_eq!(
                (one.pks.get(&schema, 0), read_i64_le(&one.payload[0].bytes, 0)),
                (7, -1)
            );
            let many: Vec<usize> = many.unwrap().iter().map(|r| unit_rows(&r.batch)).collect();
            assert_eq!(
                many,
                [(n * per) as usize, 0],
                "one reply per relation, in request order"
            );
            let found = found.unwrap().expect("the table exists");
            assert_eq!((found.tid, &found.schema), (tid, &schema));
            assert!(missing.unwrap().is_none());

            drop(client);
            driver.await.unwrap();
        });
    }
}

// ── Syscalls per burst ────────────────────────────────────────────────────

/// Pushes in the burst [`one_writev_per_burst`] counts.
const BURST: i64 = 100;

/// The child half of [`one_writev_per_burst`]: the burst, called before the
/// driver is first polled. Run under `strace -f -c` by the parent; a no-op
/// otherwise.
#[test]
fn syscall_count_child() {
    let (Ok(target), Ok(tid)) = (std::env::var("GNITZ_TOKIO_TARGET"), std::env::var("GNITZ_TOKIO_TID")) else {
        return;
    };
    let tid: u64 = tid.parse().unwrap();
    let schema = schema();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    settled(&rt, async {
        let (client, conn) = gnitz_tokio::connect(&target).await.unwrap();
        let pushes: Vec<_> = (0..BURST)
            .map(|pk| client.push(tid, &schema, &rows(&schema, pk..pk + 1)))
            .collect();
        // The last handle goes, which is what lets the driver finish.
        drop(client);
        conn.await;
        for push in pushes {
            push.await.unwrap();
        }
    });
}

/// A burst of pushes called together leaves in one `writev`, not one each: the
/// driver drains the whole channel before it steps.
#[test]
fn one_writev_per_burst() {
    let srv = ServerHandle::start();
    let (mut blocking, tid, schema, _sn) = table(srv.sock_path());
    let Some(counts) = strace_test(
        "syscall_count_child",
        &[
            ("GNITZ_TOKIO_TARGET", srv.sock_path()),
            ("GNITZ_TOKIO_TID", &tid.to_string()),
        ],
    ) else {
        eprintln!("strace not installed; skipping the syscall count");
        return;
    };
    let pushed = blocking.scan_spec(tid, &all_rows(), &schema).unwrap().batch;
    assert_eq!(unit_rows(&pushed), BURST as usize, "the traced child pushed its burst");
    // The transport writes the socket through `write_vectored`; plain `write`
    // is the harness's own stdout. One for the HELLO, one for the burst.
    assert_eq!(
        counts.sum(&["writev", "sendto", "sendmsg"]),
        2,
        "the {BURST}-push burst did not leave in one writev:\n{}",
        counts.report
    );
}

// ── The shared blocking client ────────────────────────────────────────────

/// Every clone drives one blocking client, one call at a time, which outlives
/// a call that panics; and a local-first read routes on what that client
/// mirrors.
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

    settled(&Runtime::new().unwrap(), async {
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

            // Two clones polling at once: the second waits for the first's lock.
            push_and_tick(50..100);
            let (a, b) = (client.clone(), client.clone());
            let (polled_a, polled_b) = tokio::join!(
                a.with_blocking_client(GnitzClient::poll_mirror),
                b.with_blocking_client(GnitzClient::poll_mirror),
            );
            for outcome in polled_a.unwrap().into_iter().chain(polled_b.unwrap()) {
                assert!(matches!(outcome.result, PollResult::Advanced), "{outcome:?}");
            }

            // A panic in a call resumes on its caller and takes neither the lock
            // nor the client with it: the reads below still find the copy.
            let panicking = client.clone();
            let panicked = tokio::spawn(async move {
                let boom = |_: &mut GnitzClient| -> Result<(), ClientError> { panic!("inside the blocking call") };
                panicking.with_blocking_client(boom).await
            });
            assert!(panicked.await.unwrap_err().is_panic());

            let sent = || client.with_blocking_client(|c| Ok(c.requests_sent()));
            let before = sent().await.unwrap();
            let local = read(vid).await.unwrap();
            assert!(local.lsn.is_none(), "a local answer carries no served LSN");
            assert_eq!(unit_rows(&local.batch), 100);

            // A relation the copy does not hold still reads, over the driver's
            // connection rather than the blocking client's.
            let unheld = read(tid).await.unwrap();
            assert!(unheld.lsn.is_some(), "a delegated read carries the server's LSN");
            assert_eq!(unit_rows(&unheld.batch), 100);
            assert_eq!(
                sent().await.unwrap(),
                before,
                "neither read went through the blocking client"
            );
        }
        drop(client);
        driver.await.unwrap();
    });
}
