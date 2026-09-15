#![cfg(feature = "integration")]

//! The tokio client against a real server: the guards that only a live
//! connection can show — an idle driver that spins, and a burst that leaves one
//! frame at a time.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use gnitz_core::{ColumnDef, GnitzClient, PkColumn, Schema, TableProps, TypeCode, ZSetBatch};
use gnitz_mirror::Mirror;
use gnitz_test_harness::{strace_test, unique_schema, ServerHandle};
use tokio::runtime::Runtime;

fn cols() -> Vec<ColumnDef> {
    vec![
        ColumnDef::new("pk", TypeCode::I64, false),
        ColumnDef::new("a", TypeCode::I64, false),
    ]
}

/// A `(pk BIGINT, a BIGINT)` table, the blocking client that made it, and the
/// schema name it lives under.
fn table(target: &str) -> (GnitzClient, u64, Arc<Schema>, String) {
    let mut client = GnitzClient::connect(target).expect("connect");
    let sn = unique_schema("tokio");
    client.create_schema(&sn).unwrap();
    client
        .create_table(&sn, "t", &cols(), &[0], TableProps::default(), &[])
        .unwrap();
    let (tid, schema) = client.resolve_table_id(&sn, "t").unwrap();
    (client, tid, schema, sn)
}

/// The fixture table's schema, built locally — the batch builders below need it
/// to encode a key, and `table()`'s copy comes back from the server.
fn local_schema() -> Schema {
    Schema { columns: cols(), pk_cols: vec![0] }
}

fn rows(start: i64, count: usize) -> ZSetBatch {
    let mut a = Vec::with_capacity(count * 8);
    for i in 0..count as i64 {
        a.extend_from_slice(&((start + i) * 3).to_le_bytes());
    }
    let schema = local_schema();
    let mut b = ZSetBatch::new(&schema);
    b.pks = PkColumn::from_natives(&schema, (0..count as i64).map(|i| (start + i) as u128));
    b.weights = vec![1; count];
    b.nulls = vec![0; count];
    b.payload[0].bytes = a;
    b
}

/// Drive every future in **one** task: the first poll takes each through its
/// submit, so all N reach the channel before the driver is polled at all. That
/// is what makes "one burst, one writev" the driver's property rather than the
/// scheduler's.
async fn all<F: Future>(futs: Vec<F>) -> Vec<F::Output> {
    let mut pinned: Vec<Pin<Box<F>>> = futs.into_iter().map(Box::pin).collect();
    let mut out: Vec<Option<F::Output>> = pinned.iter().map(|_| None).collect();
    std::future::poll_fn(move |cx| {
        for (slot, f) in out.iter_mut().zip(pinned.iter_mut()) {
            if slot.is_none() {
                if let Poll::Ready(v) = f.as_mut().poll(cx) {
                    *slot = Some(v);
                }
            }
        }
        if out.iter().all(Option::is_some) {
            Poll::Ready(out.iter_mut().map(|s| s.take().expect("just checked")).collect())
        } else {
            Poll::Pending
        }
    })
    .await
}

/// Process CPU time so far — user plus system, every thread of this process.
fn cpu_time() -> Duration {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: one valid timespec, a clock id the kernel always knows.
    unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

/// The busy-loop guard: an idle connection must consume no CPU. A driver that
/// returns `Ready` where it should park spins here.
#[test]
fn an_idle_connection_consumes_no_cpu() {
    let srv = ServerHandle::start_with_env(1, &[]);
    let (_blocking, tid, schema, _sn) = table(srv.sock_path());
    let rt = Runtime::new().unwrap();
    let (client, conn) = rt.block_on(gnitz_tokio::connect(srv.sock_path())).expect("connect");
    let driver = rt.spawn(conn);

    rt.block_on(client.push(tid, Arc::clone(&schema), rows(0, 4))).unwrap();
    let before = cpu_time();
    std::thread::sleep(Duration::from_secs(1));
    let spent = cpu_time() - before;
    // A spin burns a whole core, so 250ms of the second still catches one four
    // times over. Loose because this counts every thread, and tokio's idle
    // workers spin before parking — more so when the rest of the suite is busy.
    assert!(
        spent < Duration::from_millis(250),
        "an idle connection spent {spent:?} of CPU over a quiet second",
    );

    drop(client);
    rt.block_on(driver).unwrap().unwrap();
}

/// N operations on cloned handles across tasks, each to its own result; the
/// driver serves one connection for all of them.
#[test]
fn cloned_handles_across_tasks_each_get_their_own_result() {
    let srv = ServerHandle::start_with_env(4, &[]);
    let (mut blocking, tid, schema, sn) = table(srv.sock_path());
    let rt = Runtime::new().unwrap();
    let (client, conn) = rt.block_on(gnitz_tokio::connect(srv.sock_path())).expect("connect");
    let driver = rt.spawn(conn);

    let n = 32usize;
    let pushes: Vec<_> = (0..n)
        .map(|i| {
            let (c, s) = (client.clone(), Arc::clone(&schema));
            rt.spawn(async move { c.push(tid, s, rows((i * 10) as i64, 10)).await })
        })
        .collect();
    for p in pushes {
        assert!(rt.block_on(p).unwrap().unwrap() > 0, "each push gets its own LSN");
    }

    // Every read verb, and a seek whose row is its own.
    assert_eq!(rt.block_on(client.scan(tid)).unwrap().batch.len(), n * 10);
    assert_eq!(rt.block_on(client.seek(tid, 7, &[])).unwrap().batch.len(), 1);
    let many = rt.block_on(client.scan_many(&[tid, tid])).unwrap_err();
    assert!(
        matches!(many, gnitz_core::ClientError::ServerError(_)),
        "a duplicate tid is refused locally and fails that one future"
    );
    let many = rt.block_on(client.scan_many(&[tid])).unwrap();
    assert_eq!(many.len(), 1);
    assert_eq!(many[0].batch.len(), n * 10);

    // `resolve` must agree with the blocking client's.
    let theirs = blocking.resolve(&sn, "t").unwrap().expect("the table exists");
    let ours = rt
        .block_on(client.resolve(&sn, "t"))
        .unwrap()
        .expect("the table exists");
    assert_eq!(ours.tid, theirs.tid);
    assert_eq!(ours.class, theirs.class);
    assert_eq!(ours.schema.columns.len(), theirs.schema.columns.len());
    assert!(rt.block_on(client.resolve(&sn, "nope")).unwrap().is_none());

    drop(client);
    rt.block_on(driver).unwrap().unwrap();
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
    let n: usize = n.parse().unwrap();
    let schema = Arc::new(Schema { columns: cols(), pk_cols: vec![0] });
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (client, conn) = gnitz_tokio::connect(&target).await.unwrap();
        let work = async move {
            let futs: Vec<_> = (0..n)
                .map(|i| client.push(tid, Arc::clone(&schema), rows(i as i64, 1)))
                .collect();
            for r in all(futs).await {
                r.unwrap();
            }
            // The last handle goes, which is what lets the driver finish.
        };
        let (driven, ()) = tokio::join!(conn, work);
        driven.unwrap();
    });
}

/// A burst of N pushes issued together leaves in one `writev`, not N: the
/// driver drains the whole channel before it steps.
#[test]
fn one_writev_per_burst() {
    let srv = ServerHandle::start_with_env(1, &[]);
    let (_c, tid, _s, _sn) = table(srv.sock_path());
    // Every push here is cold — the child connects fresh and never resolves — so
    // the burst stays within one writev quantum; a larger `n`, or a warm or
    // control-only burst, can legitimately move the count.
    let n = 200usize;
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
    // `writev` only: the transport writes the socket through `write_vectored`,
    // where plain `write` is the harness's own stdout. One for the HELLO, one
    // for the burst; the slack is for a partial write.
    let writes = counts.sum(&["writev", "sendto", "sendmsg"]);
    assert!(
        writes <= 4,
        "expected the {n}-push burst to leave in one writev, strace counted {writes}:\n{}",
        counts.report
    );
}

// ── Mirroring ─────────────────────────────────────────────────────────────

/// A fed pass-through view over `tid`, built by hand: this crate links no
/// planner, so the circuit is a `ScanDelta` into the sink the compiler takes as
/// the plan's output register.
fn fed_view(client: &mut GnitzClient, sn: &str, tid: u64) -> u64 {
    let mut b = gnitz_core::CircuitBuilder::new();
    let src = b.input_delta(tid, None);
    b.sink(src);
    let vids = client
        .create_view_chain(
            sn,
            "v",
            vec![gnitz_core::PlannedView {
                seg: 0,
                circuit: b.build(),
                output_columns: cols(),
                pk_cols: vec![0],
                capacity_bytes: None,
                delta_bytes: Some(8 << 20),
            }],
            false,
        )
        .expect("create the fed view");
    vids[0]
}

/// `pk → summed weight`, so a comparison is weight-exact: a poll applied twice
/// leaves the row set identical and doubles every weight in the interval.
fn weights(b: &ZSetBatch) -> std::collections::BTreeMap<u64, i64> {
    let mut out = std::collections::BTreeMap::new();
    for row in 0..b.weights.len() {
        let pk = b.pks.get(&local_schema(), row) as u64;
        *out.entry(pk).or_insert(0) += b.weights[row];
    }
    out.retain(|_, w| *w != 0);
    out
}

/// The async handle's whole mirroring surface: what it refuses, what it answers
/// off the copy, and what two clones doing the same thing at once leave behind.
#[test]
fn an_async_handle_mirrors_through_a_blocking_client() {
    let srv = ServerHandle::start_with_env(4, &[]);
    let (mut blocking, tid, schema, sn) = table(srv.sock_path());
    let vid = fed_view(&mut blocking, &sn, tid);
    blocking.push(tid, &schema, &rows(0, 50)).unwrap();
    // A read against the server drains the pending ticks, so the rounds a
    // bootstrap reads already exist.
    let _ = blocking.scan(vid).unwrap();

    let rt = Runtime::new().unwrap();
    let (client, conn) = rt.block_on(gnitz_tokio::connect(srv.sock_path())).expect("connect");
    let driver = rt.spawn(conn);

    // A handle that never attached answers exactly what `scan` answers, served
    // LSN included — the observable half of the unattached fast path. That it
    // took no `spawn_blocking` is not assertable and is not asserted.
    let bare = rt.block_on(client.scan_local_first(vid)).unwrap();
    let plain = rt.block_on(client.scan(vid)).unwrap();
    assert_eq!(weights(&bare.batch), weights(&plain.batch));
    assert!(bare.lsn.is_some(), "an unmirrored read carries the server's LSN");
    assert_eq!(bare.lsn, plain.lsn);
    assert!(
        !weights(&bare.batch).is_empty(),
        "the view must hold rows, or this proves nothing"
    );

    // Two clones attaching at once install once and refuse once. Two stores on
    // two directories, because two on *one* never get this far: the engine's
    // lock refuses the second open.
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (p1, p2) = (
        d1.path().to_str().unwrap().to_string(),
        d2.path().to_str().unwrap().to_string(),
    );
    let (a, b) = (client.clone(), client.clone());
    let (s1, s2) = (Mirror::open(&p1).unwrap(), Mirror::open(&p2).unwrap());
    let (ra, rb) = rt.block_on(async {
        tokio::join!(
            a.with_blocking_client(move |c| c.attach_mirror(s1)),
            b.with_blocking_client(move |c| c.attach_mirror(s2)),
        )
    });
    drop((a, b));
    assert_eq!(
        [ra.is_ok(), rb.is_ok()].iter().filter(|ok| **ok).count(),
        1,
        "exactly one of two concurrent attaches installs",
    );
    // The refused one dropped the store it was passed, so its directory is free.
    let loser = if ra.is_ok() { &p2 } else { &p1 };
    Mirror::open(loser).expect("a refused attach releases the store it was passed");
    rt.block_on(client.with_blocking_client(GnitzClient::close_mirror))
        .expect("close");

    // The real one: attach, mirror, poll, read off the copy.
    let store = Mirror::open(&p1).unwrap();
    rt.block_on(client.with_blocking_client(move |c| c.attach_mirror(store)))
        .expect("attach");
    let view_schema = sn.clone();
    let outcome = rt
        .block_on(client.with_blocking_client(move |c| c.mirror_view(&view_schema, "v")))
        .expect("mirror the view");
    assert_eq!(outcome.view_id, vid);
    assert!(rt
        .block_on(client.with_blocking_client(move |c| Ok(c.mirrors(vid))))
        .unwrap());
    assert_eq!(
        rt.block_on(client.with_blocking_client(|c| Ok(c.mirrored_ids())))
            .unwrap(),
        vec![vid]
    );
    assert!(rt
        .block_on(client.with_blocking_client(move |c| Ok(c.cursor_of(vid))))
        .unwrap()
        .is_some());
    assert!(rt
        .block_on(client.with_blocking_client(|c| Ok(c.mirror_poisoned().map(str::to_string))))
        .unwrap()
        .is_none());

    // Two clones polling one view concurrently. One whole advance runs under one
    // lock, so they serialize instead of both fetching `(c, …]` and both
    // applying it.
    blocking.push(tid, &schema, &rows(50, 50)).unwrap();
    let expected = weights(&blocking.scan(vid).unwrap().batch);
    let (a, b) = (client.clone(), client.clone());
    let (ra, rb) = rt.block_on(async {
        tokio::join!(
            a.with_blocking_client(GnitzClient::poll_mirror),
            b.with_blocking_client(GnitzClient::poll_mirror),
        )
    });
    ra.expect("the first poll");
    rb.expect("the second poll");
    drop((a, b));
    rt.block_on(client.with_blocking_client(GnitzClient::poll_mirror))
        .expect("a third, for the tail round");

    let local = rt.block_on(client.scan_local_first(vid)).unwrap();
    assert!(local.lsn.is_none(), "a local answer carries no served LSN");
    assert_eq!(
        weights(&local.batch),
        expected,
        "two concurrent polls must leave the weights one poll produces",
    );

    // A relation the copy does not hold still reads, over the wire.
    let unheld = rt.block_on(client.scan_local_first(tid)).unwrap();
    assert!(unheld.lsn.is_some(), "a delegated read carries the server's LSN");
    assert_eq!(weights(&unheld.batch).len(), 100);

    // A dead or replaced connection is recovered in place, and the copies ride
    // along: the next poll re-resolves and the view is mirrored again.
    let target = srv.sock_path().to_string();
    rt.block_on(client.with_blocking_client(move |c| c.reconnect(&target)))
        .expect("reconnect");
    rt.block_on(client.with_blocking_client(GnitzClient::poll_mirror))
        .expect("a poll after reconnect");
    assert!(rt
        .block_on(client.with_blocking_client(move |c| Ok(c.mirrors(vid))))
        .unwrap());

    rt.block_on(client.with_blocking_client(GnitzClient::checkpoint_mirror))
        .expect("checkpoint");
    rt.block_on(client.with_blocking_client(move |c| c.forget_view(vid)))
        .expect("forget");
    assert!(!rt
        .block_on(client.with_blocking_client(move |c| Ok(c.mirrors(vid))))
        .unwrap());

    rt.block_on(client.with_blocking_client(GnitzClient::close_mirror))
        .expect("close the store");
    let store = Mirror::open(&p1).unwrap();
    rt.block_on(client.with_blocking_client(move |c| c.attach_mirror(store)))
        .expect("the same directory can be attached again");
    rt.block_on(client.with_blocking_client(GnitzClient::close_mirror))
        .expect("close it again");

    drop(client);
    rt.block_on(driver).unwrap().unwrap();
}
