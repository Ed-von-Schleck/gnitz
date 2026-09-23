use std::rc::Rc;
use std::time::Duration;

use super::super::fixtures::{test_dispatcher, test_dispatcher_with_writers};
use crate::catalog::{CatalogEngine, SysFamily};
use crate::runtime::reactor::oneshot;
use crate::runtime::sal::{GroupTargets, WorkerSet};
use crate::runtime::test_support::{fork_child, try_poll_once, within};
use gnitz_foundation::posix_io::retry_eintr;
use gnitz_wire::WireStatus;

/// Fork a child that exits immediately and block until it is a zombie *without*
/// reaping it, so the probe's own `waitpid(WNOHANG)` is guaranteed to find it on
/// the first call. `waitpid` rejects `WNOWAIT`, hence `waitid`.
fn spawn_zombie() -> i32 {
    let pid = unsafe { fork_child(|| {}) };
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    retry_eintr(|| unsafe { libc::waitid(libc::P_PID, pid as libc::id_t, &mut info, libc::WEXITED | libc::WNOWAIT) })
        .expect("waitid pre-sync");
    pid
}

/// Fork a child that exits immediately, then block-reap it. The pid is now a
/// confirmed non-child, so a later `waitpid` yields ECHILD — the other "dead"
/// verdict, with no race against the probe.
fn spawn_and_reap_dead() -> i32 {
    let pid = spawn_zombie();
    let mut status = 0;
    unsafe { libc::waitpid(pid, &mut status, 0) };
    pid
}

/// `check_workers` reports the first dead worker on every probe, and leaves the
/// pid set as it was. Both dead arms are covered: worker 1's zombie is reaped by
/// the first probe's own `waitpid`, and answers ECHILD on every probe after.
#[test]
fn check_workers_keeps_reporting_a_dead_worker() {
    // `PR_SET_PDEATHSIG` on the forking thread: a panicking test unwinds off
    // this thread and takes the paused child with it, rather than orphaning it.
    let child = || unsafe {
        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0);
        libc::pause();
    };
    let live = unsafe { fork_child(child) };
    let (zombie, reaped) = (spawn_zombie(), spawn_and_reap_dead());

    let disp = test_dispatcher(vec![live, zombie, reaped], std::ptr::null_mut());

    assert_eq!(disp.check_workers(), Some(1), "the unreaped zombie is detected dead");
    assert_eq!(disp.check_workers(), Some(1), "and still reported once reaped");
    assert_eq!(disp.check_workers(), Some(1), "on every probe");
    assert_eq!(
        disp.worker_pids,
        vec![live, zombie, reaped],
        "a probe leaves the pid set unchanged"
    );

    unsafe {
        libc::kill(live, libc::SIGKILL);
        let mut status = 0;
        libc::waitpid(live, &mut status, 0);
    }
}

/// A round no worker answers must surface a dead worker as a clean error naming
/// the worker and the caller's own phase — not wait on the ACK.
#[test]
fn ack_collection_errors_when_a_worker_is_dead() {
    for ctx in ["checkpoint base round", "backfill relay"] {
        let disp = Rc::new(test_dispatcher(vec![spawn_and_reap_dead()], std::ptr::null_mut()));
        let d = Rc::clone(&disp);
        let err = disp
            .reactor()
            .block_on(async move { d.broadcast_round(ctx, false, |_, _| Ok(())).await })
            .expect_err("a dead worker must fail ack collection");
        assert!(err.text.contains("worker 0") && err.text.contains(ctx), "{err}");
    }
}

/// A worker that fails before it joins a round leaves the others in their
/// exchange wait, never ACKing: the error ends the round without their ACKs.
#[test]
fn a_round_fails_on_a_worker_error_without_the_other_acks() {
    let (disp, writers) = test_dispatcher_with_writers(vec![0, 0]);
    let disp = Rc::new(disp);
    let d = Rc::clone(&disp);
    let err = disp
        .reactor()
        .block_on(async move {
            d.broadcast_round("backfill relay", false, |_, targets| {
                let GroupTargets::Leased { request_id, .. } = targets else {
                    unreachable!("a round broadcasts")
                };
                writers[0].send_status(0, request_id, WireStatus::Error, b"boom");
                Ok(())
            })
            .await
        })
        .expect_err("a worker's error ACK must fail the round");
    assert!(err.text.contains("worker 0") && err.text.contains("boom"), "{err}");
}

/// A refused write fails the round before any worker is signalled, and keeps its
/// status: a full SAL reaches the caller as the retryable `SalFull`, not as a
/// flattened error.
#[test]
fn a_round_keeps_its_write_refusals_status() {
    let (disp, _writers) = test_dispatcher_with_writers(vec![0]);
    let disp = Rc::new(disp);
    let d = Rc::clone(&disp);
    let err = disp
        .reactor()
        .block_on(async move {
            d.broadcast_round("view tick drain", false, |_, _| {
                Err(gnitz_wire::WireFault {
                    status: WireStatus::SalFull,
                    text: "SAL full".into(),
                })
            })
            .await
        })
        .expect_err("a refused write must fail the round");
    assert_eq!(err.status, WireStatus::SalFull, "{err}");
}

/// Other tasks run while a round is pending: the round's one ACK comes from a
/// separately spawned task, which learns the request id only once the round has
/// written its group.
#[test]
fn a_round_lets_other_tasks_run() {
    within(Duration::from_secs(30), || {
        let (disp, mut writers) = test_dispatcher_with_writers(vec![0]);
        let disp = Rc::new(disp);
        let writer = writers.pop().expect("one ring");
        let (id_tx, id_rx) = oneshot::channel::<u32>();
        disp.reactor().spawn(async move {
            let id = id_rx.await;
            writer.send_status(0, id, WireStatus::Ok, &[]);
        });
        let d = Rc::clone(&disp);
        disp.reactor()
            .block_on(async move {
                d.broadcast_round("view tick drain", false, |_, targets| {
                    let GroupTargets::Leased { request_id, .. } = targets else {
                        unreachable!("a round broadcasts")
                    };
                    id_tx.send(request_id);
                    Ok(())
                })
                .await
            })
            .expect("the other task's ACK completes the round");
    });
}

/// The checkpoint finalizer flushes system tables before resetting the SAL. A
/// sequence advance can reach the `sys_sequences` MemTable after the base
/// round's reset, so the finalizer is its only durability event before the
/// ephemeral reset makes the SAL useless for recovery.
#[test]
fn checkpoint_post_ack_flushes_a_memtable_only_sequence_advance() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let user_seq;
    {
        let mut engine = CatalogEngine::open(dir, 1).unwrap();
        user_seq = engine
            .create_table(
                "public.t",
                &[crate::test_support::col_def("id", gnitz_wire::TypeCode::U64)],
                &[0],
            )
            .unwrap();
        // Reserve + ingest straight into the catalog — no SAL involved, so the
        // advance lands ONLY in the sys_sequences MemTable.
        let (_base, delta) = engine.reserve_user_sequence(user_seq, 64).unwrap();
        engine.submit(SysFamily::Sequence, delta).unwrap();
        // As the serial path's emit does: the broadcast leaves, the rows stay.
        engine.drain_pending_broadcasts();

        let disp = test_dispatcher(Vec::new(), &mut engine);
        disp.checkpoint_post_ack(&mut try_poll_once(disp.sal().lock()).expect("uncontended"))
            .unwrap();
        drop(disp);

        // Crash semantics: no `engine.close()`, and no Drop impl — only what the
        // finalizer flushed survives.
        drop(engine);
    }

    let engine = CatalogEngine::open(dir, 1).unwrap();
    assert_eq!(
        engine.sequence_value(user_seq),
        Some(64),
        "sys_sequences high-water must survive a crash right after the checkpoint finalize"
    );
    engine.close();
}

/// A whole checkpoint is `reclaim_base` + `restamp_derived`: the base round owns
/// the one generation bump that invalidates every checkpointed view, and the
/// ephemeral round re-stamps the derived state at that same generation, so
/// nothing is left rebuild-on-boot.
#[test]
fn a_checkpoint_bumps_the_generation_once_and_restamps_at_it() {
    let tmp = tempfile::tempdir().unwrap();
    let mut engine = CatalogEngine::open(tmp.path().to_str().unwrap(), 1).unwrap();

    let disp = Rc::new(test_dispatcher(Vec::new(), &mut engine));
    // Epoch 0 is the empty-slot sentinel, so the region needs a boot reset
    // before any group is written — what `server_main` does after worker ACKs.
    try_poll_once(disp.sal().lock()).expect("uncontended").boot_rewind(1);

    let gen = disp.cat().durable_generation();
    let d = Rc::clone(&disp);
    disp.reactor()
        .block_on(async move { d.reclaim_base(&mut d.sal().lock().await).await })
        .unwrap();
    assert_eq!(
        disp.cat().durable_generation(),
        gen + 1,
        "the base round bumps exactly once"
    );
    assert!(
        disp.derived_needs_restamp(),
        "the base round alone leaves every derived manifest behind the new generation"
    );

    // Empty drain set: zero workers hold no pending deltas.
    let d = Rc::clone(&disp);
    disp.reactor()
        .block_on(async move { d.restamp_derived(&[]).await })
        .unwrap();
    assert!(
        !disp.derived_needs_restamp(),
        "the ephemeral round re-validates the derived state at the durable generation"
    );

    let d = Rc::clone(&disp);
    disp.reactor()
        .block_on(async move { d.boot_checkpoint().await })
        .unwrap();
    assert_eq!(
        disp.cat().durable_generation(),
        gen + 1,
        "boot_checkpoint restamps at the reserved generation without a bump",
    );
    assert!(!disp.derived_needs_restamp());

    drop(disp);
    engine.close();
}

#[test]
fn read_route_reaches_the_owners_a_bound_names() {
    use super::super::route_read;
    use gnitz_store::schema::Placement;
    use gnitz_wire::{Cut, KeyRange, PkColList, PkKeys, ReadBound, ReadSpec};

    const NW: usize = 4;
    let keyed = crate::test_support::pk_only_schema(&[gnitz_wire::TypeCode::U64]);
    let block = crate::catalog::encode_schema_block(&keyed);
    let key = |k: u64| k.to_be_bytes();
    let set_blob = |keys: &[u64]| {
        let keys: Vec<[u8; 8]> = keys.iter().map(|&k| key(k)).collect();
        ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_keys(8, keys.iter().map(|k| &k[..])))).encode(&block)
    };
    let owner = |k: u64| keyed.worker_for_pk(&key(k), NW);

    let replicated = keyed.with_placement(Placement::Replicated);
    for blob in [None, Some(set_blob(&[1, 2, 3]))] {
        let r = route_read(&replicated, blob.as_deref(), NW);
        assert_eq!((r.set, r.per_worker.is_none()), (WorkerSet::one(0), true), "replicated");
    }

    let range = |start, end| {
        ReadSpec::all_rows(ReadBound::Range(KeyRange::new(
            PkColList::from_slice(&[0]),
            &[],
            start,
            end,
        )))
        .encode(&block)
    };
    assert_eq!(
        route_read(&keyed, Some(&range(Cut::before(42), Cut::after(42))), NW).set,
        WorkerSet::one(owner(42)),
        "confined range"
    );
    assert_eq!(
        route_read(&keyed, Some(&range(Cut::after(9), Cut::before(3))), NW).set,
        WorkerSet::one(0),
        "a provably empty range is answered by one worker"
    );

    assert_eq!(
        route_read(&keyed, Some(&set_blob(&[42])), NW).set,
        WorkerSet::one(owner(42)),
        "one owner"
    );
    assert_eq!(
        route_read(&keyed, Some(&set_blob(&[])), NW).set,
        WorkerSet::one(0),
        "empty set"
    );

    // Two keys on two distinct owners: each is sent its own key and no other.
    let (a, b) = (1..)
        .map(|k| (0, k))
        .find(|&(_, k)| owner(k) != owner(0))
        .expect("keys spread");
    let spread = route_read(&keyed, Some(&set_blob(&[a, b])), NW);
    assert_eq!(spread.set, WorkerSet::one(owner(a)).with(owner(b)));
    let per_worker = spread.per_worker.expect("a spread set splits");
    assert_eq!(per_worker.len(), NW);
    for (w, got) in per_worker.iter().enumerate() {
        let want = if w == owner(a) {
            set_blob(&[a])
        } else if w == owner(b) {
            set_blob(&[b])
        } else {
            Vec::new()
        };
        assert_eq!(*got, want, "worker {w} is sent exactly its own keys");
    }

    let local = keyed.with_placement(Placement::Local);
    assert_eq!(
        route_read(&local, Some(&set_blob(&[a, b])), NW).set,
        WorkerSet::ALL,
        "Local"
    );
    let wide = crate::test_support::pk_only_schema(&[gnitz_wire::TypeCode::U128]);
    assert_eq!(
        route_read(&wide, Some(&set_blob(&[a, b])), NW).set,
        WorkerSet::ALL,
        "a foreign stride"
    );
}
