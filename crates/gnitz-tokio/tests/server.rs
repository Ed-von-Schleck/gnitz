#![cfg(feature = "integration")]

//! The tokio client against a real server: what needs real replies — verbs
//! pipelined over both transports, the syscalls a burst costs, and the mirror
//! a shared client keeps.

mod support;

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use std::future::Future;

use gnitz_core::{
    block_on, BatchAppender, ClientError, GnitzClient, PkColumn, PollResult, RelName, ScanReply, Schema, ZSetBatch,
};
use gnitz_mirror::{Mirror, MirrorConfig};
use gnitz_test_harness::{strace_test, unique_schema, ServerHandle};
use gnitz_tokio::AsyncClient;
use gnitz_wire::{read_i64_le, ColumnDef, ReadBound, ReadSpec, TableProps, TypeCode, ViewProps, WireConflictMode};
use support::settled;
use tokio::runtime::Runtime;

/// `schema.name`.
fn rel(schema: &str, name: &str) -> RelName {
    RelName::new(schema, name).unwrap()
}

/// `(pk BIGINT, a BIGINT)`.
fn schema() -> Schema {
    Schema::from_parts(
        vec![
            ColumnDef::new("pk", TypeCode::I64, false),
            ColumnDef::new("a", TypeCode::I64, false),
        ],
        &[0],
    )
    .unwrap()
}

/// A [`schema`] table `t`: the blocking client that made it, its id, its
/// schema, and the schema name it lives under.
fn table(target: &str) -> (GnitzClient, u64, Arc<Schema>, String) {
    let mut client = GnitzClient::connect(target).expect("connect");
    let sn = unique_schema("tokio");
    block_on(client.create_schema(&sn)).unwrap();
    let schema = schema();
    let tid = block_on(client.create_table(&rel(&sn, "t"), &schema, &[], TableProps::default(), &[])).unwrap();
    (client, tid, Arc::new(schema), sn)
}

/// `pks` at weight 1.
fn rows(schema: &Schema, pks: Range<i64>) -> ZSetBatch {
    let mut batch = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut batch);
    for pk in pks {
        app.add_row(pk as u128, 1).i64_val(pk * 3);
    }
    batch
}

/// [`table`] with a fed view `v` of all of `t`, whose id follows the table's.
fn fed_table(target: &str) -> (GnitzClient, u64, u64, Arc<Schema>, String) {
    let (mut blocking, tid, schema, sn) = table(target);
    let source = block_on(blocking.resolve_relation(&rel(&sn, "t"))).unwrap();
    let fed = ViewProps::Fed {
        delta_bytes: std::num::NonZeroU64::new(8 << 20).unwrap(),
    };
    let vid = block_on(blocking.create_view(&rel(&sn, "v"), &source, fed)).unwrap();
    (blocking, tid, vid, schema, sn)
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

/// Push `batch`, submitted when called.
fn push(
    c: &AsyncClient,
    tid: u64,
    schema: &Arc<Schema>,
    batch: ZSetBatch,
) -> impl Future<Output = Result<u64, ClientError>> {
    let schema = Arc::clone(schema);
    c.send(move |c| c.push(tid, &schema, batch, WireConflictMode::Update))
}

/// Read `tid` under `spec`, submitted when called.
fn scan(
    c: &AsyncClient,
    tid: u64,
    spec: ReadSpec,
    schema: &Arc<Schema>,
) -> impl Future<Output = Result<ScanReply, ClientError>> {
    let schema = Arc::clone(schema);
    c.send(move |c| c.scan_spec(tid, &spec, &schema))
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
            let (client, conn) = gnitz_tokio::share(gnitz_tokio::connect(&target).await.expect("connect"));
            let driver = tokio::spawn(conn);

            // Megabytes each way, so neither direction fits a socket buffer: writes
            // resume after the socket refused them and replies span many reads.
            let (n, per) = (40, 5_000);
            let rounds: Vec<_> = (0..n)
                .map(|i| {
                    (
                        push(&client, tid, &schema, rows(&schema, i * per..(i + 1) * per)),
                        scan(&client, tid, all_rows(), &schema),
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
            BatchAppender::new(&mut replaced).add_row(7, 1).i64_val(-1);
            let key = PkColumn::from_natives(&schema, [7]);
            let (all, reply) = (ReadSpec::all_rows(ReadBound::None), Arc::clone(&schema));
            let resolve = |name: &'static str| {
                let sn = sn.clone();
                client.run(move |c| Box::pin(async move { c.resolve(&rel(&sn, name)).await }))
            };
            let (_, one, many, found, missing) = tokio::join!(
                push(&client, tid, &schema, replaced),
                scan(&client, tid, ReadSpec::all_rows(ReadBound::PkSet(key.keys())), &schema),
                client.send(move |c| c.scan_many(&[(tid.into(), &all, &reply), (empty.into(), &all, &reply)])),
                resolve("t"),
                resolve("nope"),
            );
            let (found, missing) = (found.unwrap(), missing.unwrap());
            let one = one.unwrap().batch;
            assert_eq!(unit_rows(&one), 1);
            assert_eq!((one.pks.get(0), read_i64_le(&one.payload[0].bytes, 0)), (7, -1));
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
    let schema = Arc::new(schema());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    settled(&rt, async {
        let (client, conn) = gnitz_tokio::share(gnitz_tokio::connect(&target).await.unwrap());
        let pushes: Vec<_> = (0..BURST)
            .map(|pk| push(&client, tid, &schema, rows(&schema, pk..pk + 1)))
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
/// driver runs every call queued before it steps.
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
    let pushed = block_on(blocking.scan_spec(tid, &all_rows(), &schema)).unwrap().batch;
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

// ── The shared client's mirror ────────────────────────────────────────────

/// A shared client mirrors a view: calls from its clones run one at a time on
/// the one client, and a local-first read routes on what the client mirrors.
#[test]
fn a_shared_client_mirrors() {
    let srv = ServerHandle::start_n(4);
    let (mut blocking, tid, vid, schema, sn) = fed_table(srv.sock_path());
    let mut push = |pks| {
        block_on(blocking.push(tid, &schema, rows(&schema, pks), WireConflictMode::Update)).unwrap();
    };
    push(0..50);

    settled(&Runtime::new().unwrap(), async {
        let (client, conn) = gnitz_tokio::share(gnitz_tokio::connect(srv.sock_path()).await.expect("connect"));
        let driver = tokio::spawn(conn);
        {
            let read = |tid| {
                let schema = Arc::clone(&schema);
                client.send(move |c| c.scan_spec_local_first(tid, all_rows(), &schema))
            };
            let sent = || client.run(|c| Box::pin(async move { c.requests_sent() }));

            // Nothing is mirrored yet: the server answers.
            let served = read(vid).await.unwrap();
            assert!(served.lsn.is_some(), "a served read carries the server's LSN");
            assert_eq!(unit_rows(&served.batch), 50);

            let dir = tempfile::tempdir().unwrap();
            let store = Mirror::open(dir.path().to_str().unwrap(), MirrorConfig::default()).unwrap();
            let name = sn.clone();
            let mirrored = client
                .run(move |c| {
                    Box::pin(async move {
                        c.attach_mirror(store)?;
                        c.mirror_view(&rel(&name, "v")).await
                    })
                })
                .await
                .unwrap()
                .expect("mirror the view");
            assert_eq!(mirrored.view_id, vid);

            // Two clones polling at once: the second runs after the first.
            push(50..100);
            let (a, b) = (client.clone(), client.clone());
            let (polled_a, polled_b) = tokio::join!(
                a.run(|c| Box::pin(c.sync(Duration::ZERO))),
                b.run(|c| Box::pin(c.sync(Duration::ZERO)))
            );
            for outcome in polled_a
                .unwrap()
                .unwrap()
                .mirrored
                .into_iter()
                .chain(polled_b.unwrap().unwrap().mirrored)
            {
                assert!(matches!(outcome.result, PollResult::Advanced), "{outcome:?}");
            }

            let before = sent().await.unwrap();
            let local = read(vid).await.unwrap();
            assert!(local.lsn.is_none(), "a local answer carries no served LSN");
            assert_eq!(unit_rows(&local.batch), 100);
            assert_eq!(sent().await.unwrap(), before, "the copy answered, not the server");

            // A relation the copy does not hold still reads, upstream.
            let unheld = read(tid).await.unwrap();
            assert!(unheld.lsn.is_some(), "a delegated read carries the server's LSN");
            assert_eq!(unit_rows(&unheld.batch), 100);
            assert_eq!(sent().await.unwrap(), before + 1);

            client
                .run(|c| Box::pin(c.close_mirror()))
                .await
                .unwrap()
                .expect("the exit checkpoint");
        }
        drop(client);
        driver.await.unwrap();
    });
}

// ── A held sync on the shared handle ──────────────────────────────────────

/// Longer than anything here may take: a sync held this long fails its test.
const HELD: Duration = Duration::from_secs(30);

/// Subscribe `client` to the view `vid` whole; the subscription's id and how
/// many rows the read brought.
async fn subscribe(client: &AsyncClient, vid: u64, schema: &Arc<Schema>) -> (u64, usize) {
    let schema = Arc::clone(schema);
    let subscribed = client.run(move |c| {
        Box::pin(async move {
            let (sub, rows, _) = c.subscribe(vid, None, &schema, &all_rows().encode()).await?;
            Ok::<_, ClientError>((sub, unit_rows(&rows.batch)))
        })
    });
    subscribed.await.unwrap().expect("subscribe")
}

/// Poll `f` once, which makes the calls it starts with; whether it is pending.
async fn started<F: Future + Unpin>(f: &mut F) -> bool {
    std::future::poll_fn(|cx| std::task::Poll::Ready(std::pin::Pin::new(&mut *f).poll(cx).is_pending())).await
}

/// The rows a sync brought `sub`, the connection's one subscription.
fn pushed_rows(synced: gnitz_core::Synced, sub: u64) -> usize {
    let [pushed] = &synced.pushed[..] else {
        panic!("one entry per subscription")
    };
    assert_eq!(pushed.sub, sub);
    unit_rows(&pushed.result.as_ref().expect("the subscription is held").0.batch)
}

/// A sync held with nothing to report leaves the handle to its other calls: a
/// scan made beside it is answered and leaves it held, and the push that then
/// reaches the view ends it, with that push's rows.
#[test]
fn a_held_sync_leaves_the_shared_handle_free() {
    let srv = ServerHandle::start_n(4);
    let (mut blocking, tid, vid, schema, _) = fed_table(srv.sock_path());
    let mut push = |pks| {
        block_on(blocking.push(tid, &schema, rows(&schema, pks), WireConflictMode::Update)).unwrap();
    };
    push(0..50);

    settled(&Runtime::new().unwrap(), async {
        let (client, conn) = gnitz_tokio::share(gnitz_tokio::connect(srv.sock_path()).await.expect("connect"));
        let driver = tokio::spawn(conn);
        let (sub, seed) = subscribe(&client, vid, &schema).await;
        assert_eq!(seed, 50);
        assert_eq!(pushed_rows(client.sync(Duration::ZERO).await.unwrap(), sub), 0);

        let mut held = Box::pin(client.sync(HELD));
        assert!(started(&mut held).await);
        let beside = scan(&client.clone(), tid, all_rows(), &schema).await.unwrap();
        assert_eq!(unit_rows(&beside.batch), 50);
        assert!(started(&mut held).await, "the scan left the sync held");

        push(50..60);
        assert_eq!(pushed_rows(held.await.unwrap(), sub), 10);
        drop(client);
        driver.await.unwrap();
    });
}

/// A held sync that is dropped loses nothing: the next one is not held behind
/// it, and brings what the dropped one's answer carried.
#[test]
fn a_dropped_held_sync_loses_nothing() {
    let srv = ServerHandle::start_n(4);
    let (mut blocking, tid, vid, schema, _) = fed_table(srv.sock_path());
    let mut push = |pks| {
        block_on(blocking.push(tid, &schema, rows(&schema, pks), WireConflictMode::Update)).unwrap();
    };
    push(0..50);

    settled(&Runtime::new().unwrap(), async {
        let (client, conn) = gnitz_tokio::share(gnitz_tokio::connect(srv.sock_path()).await.expect("connect"));
        let driver = tokio::spawn(conn);
        let (sub, _) = subscribe(&client, vid, &schema).await;
        assert_eq!(pushed_rows(client.sync(Duration::ZERO).await.unwrap(), sub), 0);

        let mut held = Box::pin(client.sync(HELD));
        assert!(started(&mut held).await);
        // Answered behind the sync's request, so the server holds that one.
        scan(&client, tid, all_rows(), &schema).await.unwrap();
        assert!(started(&mut held).await);
        drop(held);

        push(50..60);
        let t0 = std::time::Instant::now();
        assert_eq!(pushed_rows(client.sync(HELD).await.unwrap(), sub), 10);
        assert!(t0.elapsed() < HELD / 2, "the dropped sync's answer held up no other");
        drop(client);
        driver.await.unwrap();
    });
}
