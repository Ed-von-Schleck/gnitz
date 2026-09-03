use super::super::fixtures::test_dispatcher;
use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID, SEQ_TAB_ID};
use gnitz_store::foundation::posix_io::retry_eintr;

/// Fork a child that exits immediately and block until it is a zombie *without*
/// reaping it, so the probe's own `waitpid(WNOHANG)` is guaranteed to find it on
/// the first call. `waitpid` rejects `WNOWAIT`, hence `waitid`.
fn spawn_zombie() -> i32 {
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");
    if pid == 0 {
        unsafe { libc::_exit(0) };
    }
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

/// `check_workers` reports the first dead worker and zeroes its slot, so each
/// death is reported exactly once. Both dead arms are covered: worker 1's
/// zombie is reaped by the probe's own `waitpid`, worker 2 was reaped already
/// and answers ECHILD.
#[test]
fn check_workers_reports_each_dead_worker_exactly_once() {
    // `PR_SET_PDEATHSIG` on the forking thread: a panicking test unwinds off
    // this thread and takes the paused child with it, rather than orphaning it.
    let live = unsafe { libc::fork() };
    assert!(live >= 0, "fork failed");
    if live == 0 {
        unsafe {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0);
            libc::pause();
            libc::_exit(0);
        }
    }
    let (zombie, reaped) = (spawn_zombie(), spawn_and_reap_dead());

    let disp = test_dispatcher(vec![live, zombie, reaped], std::ptr::null_mut());

    assert_eq!(disp.check_workers(), Some(1), "the unreaped zombie is detected dead");
    assert_eq!(disp.check_workers(), Some(2), "then the already-reaped worker");
    assert_eq!(disp.check_workers(), None, "a zeroed worker is never re-reported");
    assert_eq!(
        *disp.worker_pids.borrow(),
        vec![live, 0, 0],
        "a dead worker's pid is zeroed"
    );

    unsafe {
        libc::kill(live, libc::SIGKILL);
        let mut status = 0;
        libc::waitpid(live, &mut status, 0);
    }
}

/// ACK collection makes no progress on an empty ring and reaches its park arm,
/// whose liveness probe must surface the dead worker as a clean error naming the
/// worker and the caller's own phase — not loop on the park.
#[test]
fn ack_collection_errors_when_a_worker_is_dead() {
    for ctx in ["checkpoint base round", "backfill relay"] {
        let disp = test_dispatcher(vec![spawn_and_reap_dead()], std::ptr::null_mut());
        let err = disp
            .collect_acks_and_relay(ctx, false)
            .expect_err("a dead worker must fail ack collection");
        assert!(err.contains("worker 0") && err.contains(ctx), "{err}");
    }
}

/// The checkpoint finalizer flushes system tables before resetting the SAL. A
/// sequence advance can reach the `sys_sequences` MemTable after the base
/// round's reset, so the finalizer is its only durability event before the
/// ephemeral reset makes the SAL useless for recovery.
#[test]
fn checkpoint_post_ack_flushes_a_memtable_only_sequence_advance() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let user_seq = FIRST_USER_TABLE_ID + 3;
    {
        let mut engine = CatalogEngine::open(dir, 1).unwrap();
        // Reserve + ingest straight into the catalog — no SAL involved, so the
        // advance lands ONLY in the sys_sequences MemTable.
        let (_base, delta, _lsn) = engine.reserve_user_sequence(user_seq, 64);
        engine.ingest_to_family(SEQ_TAB_ID, &delta).unwrap();

        let disp = test_dispatcher(Vec::new(), &mut engine);
        disp.checkpoint_post_ack().unwrap();
        drop(disp);

        // Crash semantics: no `engine.close()`, and no Drop impl — only what the
        // finalizer flushed survives.
        drop(engine);
    }

    let engine = CatalogEngine::open(dir, 1).unwrap();
    assert_eq!(
        engine.user_sequence(user_seq),
        Some(64),
        "sys_sequences high-water must survive a crash right after the checkpoint finalize"
    );
    engine.close();
}

/// A whole checkpoint is `reclaim_base` + `restamp_derived`: the base round owns
/// the one generation bump that invalidates every checkpointed view, and the
/// ephemeral round re-stamps the derived state at that same generation, so
/// nothing is left rebuild-on-boot. `boot_checkpoint` is one such pair.
#[test]
fn a_checkpoint_bumps_the_generation_once_and_restamps_at_it() {
    let tmp = tempfile::tempdir().unwrap();
    let mut engine = CatalogEngine::open(tmp.path().to_str().unwrap(), 1).unwrap();

    let disp = test_dispatcher(Vec::new(), &mut engine);
    // Epoch 0 is the empty-slot sentinel, so the region needs a boot reset
    // before any group is written — what `server_main` does after worker ACKs.
    disp.boot_rewind_sal(0);

    let gen = disp.cat().durable_generation();
    disp.reclaim_base().unwrap();
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
    disp.restamp_derived(&[]).unwrap();
    assert!(
        !disp.derived_needs_restamp(),
        "the ephemeral round re-validates the derived state at the durable generation"
    );

    disp.boot_checkpoint(1).unwrap();
    assert_eq!(
        disp.cat().durable_generation(),
        gen + 2,
        "boot_checkpoint is one whole checkpoint, not two bumps",
    );

    drop(disp);
    engine.close();
}
