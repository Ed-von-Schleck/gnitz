#![cfg(feature = "integration")]

//! The tokio client against a real server: the guards that only a live
//! connection can show — an idle driver that spins, a burst that leaves one
//! frame at a time, and a `resolve` that must agree with the blocking client's.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use gnitz_core::{ColData, ColumnDef, GnitzClient, PkColumn, PkTuple, Schema, TableProps, TypeCode, ZSetBatch};
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

fn rows(start: i64, count: usize) -> ZSetBatch {
    let mut a = Vec::with_capacity(count * 8);
    for i in 0..count as i64 {
        a.extend_from_slice(&((start + i) * 3).to_le_bytes());
    }
    ZSetBatch {
        pks: PkColumn::from_u128s(8, (0..count as i64).map(|i| (start + i) as u128)),
        weights: vec![1; count],
        nulls: vec![0; count],
        columns: vec![ColData::Fixed(vec![]), ColData::Fixed(a)],
    }
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
    let Some(srv) = ServerHandle::start_with_env(1, &[]) else {
        return;
    };
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
    let Some(srv) = ServerHandle::start_with_env(4, &[]) else {
        return;
    };
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
    let (_, batch, _) = rt.block_on(client.scan(tid)).unwrap();
    assert_eq!(batch.map_or(0, |b| b.len()), n * 10);
    let (_, row, _) = rt.block_on(client.seek(tid, PkTuple::from_u128_narrow(7))).unwrap();
    assert_eq!(row.map_or(0, |b| b.len()), 1);
    let many = rt.block_on(client.scan_many(&[tid, tid])).unwrap_err();
    assert!(
        matches!(many, gnitz_core::ClientError::ServerError(_)),
        "a duplicate tid is refused locally and fails that one future"
    );
    let many = rt.block_on(client.scan_many(&[tid])).unwrap();
    assert_eq!(many.len(), 1);
    assert_eq!(many[0].1.as_ref().map_or(0, |b| b.len()), n * 10);

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
    let schema = Arc::new(Schema {
        columns: cols(),
        pk_cols: vec![0],
    });
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
    let Some(srv) = ServerHandle::start_with_env(1, &[]) else {
        return;
    };
    let (_c, tid, _s, _sn) = table(srv.sock_path());
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
