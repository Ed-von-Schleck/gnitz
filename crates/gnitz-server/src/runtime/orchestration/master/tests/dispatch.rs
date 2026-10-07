use std::rc::Rc;

use super::super::fixtures::test_dispatcher;
use crate::catalog::{CatalogEngine, SysFamily};
use crate::runtime::sal::{Apply, DirectGroup};
use crate::runtime::test_support::{assert_child_exited_ok, fork_child, try_poll_once, within};
use gnitz_foundation::posix_io::retry_eintr;
use gnitz_wire::{WireFault, WireStatus};

/// Fork a child that exits immediately, and wait until it is a zombie *without*
/// reaping it, so the probe's own `waitpid(WNOHANG)` finds it on the first call.
/// `waitpid` rejects `WNOWAIT`, hence `waitid`.
fn spawn_zombie() -> i32 {
    let pid = unsafe { fork_child(|| {}) };
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    retry_eintr(|| unsafe { libc::waitid(libc::P_PID, pid as libc::id_t, &mut info, libc::WEXITED | libc::WNOWAIT) })
        .expect("waitid pre-sync");
    pid
}

/// The pid of a child already reaped, which every `waitpid` answers ECHILD.
fn spawn_and_reap_dead() -> i32 {
    let pid = unsafe { fork_child(|| {}) };
    unsafe { assert_child_exited_ok(pid) };
    pid
}

/// `check_workers` reports the first dead worker on every probe. Both dead arms
/// are covered: worker 1's zombie is reaped by the first probe's own `waitpid`,
/// and answers ECHILD on every probe after.
#[test]
fn check_workers_keeps_reporting_a_dead_worker() {
    // `PR_SET_PDEATHSIG` on the forking thread: a panicking test unwinds off
    // this thread and takes the paused child with it, rather than orphaning it.
    let live = unsafe {
        fork_child(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0);
            libc::pause();
        })
    };
    let (disp, _) = test_dispatcher(vec![live, spawn_zombie()], std::ptr::null_mut());

    assert_eq!(disp.check_workers(), Some(1), "the unreaped zombie is detected dead");
    assert_eq!(disp.check_workers(), Some(1), "and still reported once reaped");

    unsafe {
        libc::kill(live, libc::SIGKILL);
        libc::waitpid(live, &mut 0, 0);
    }
}

/// `worker_death` resolves with the dead worker's rank, here a pid every probe
/// answers ECHILD for.
#[test]
fn worker_death_names_the_dead_worker() {
    within(|| {
        let (disp, _) = test_dispatcher(vec![0, spawn_and_reap_dead()], std::ptr::null_mut());
        let disp = Rc::new(disp);
        let d = Rc::clone(&disp);
        assert_eq!(disp.reactor().block_on(async move { d.worker_death().await }), 1);
    });
}

/// A group written on an ACK lease ends on every worker's ACK; one the SAL has
/// no room for is refused with the SAL's own status.
#[test]
fn an_acked_group_ends_on_its_acks_or_is_refused() {
    within(|| {
        for refused in [false, true] {
            let (disp, mut writers) = test_dispatcher(vec![0, 0], std::ptr::null_mut());
            let disp = Rc::new(disp);
            let d = Rc::clone(&disp);
            let got = disp.reactor().block_on(async move {
                // Wider than the fixture's whole SAL.
                let tick = Apply::Tick {
                    first_round: 2,
                    tids: vec![0; if refused { 2 << 17 } else { 1 }].into(),
                };
                let lease = d.write_acked(&d.sal().lock().await, DirectGroup::new(tick))?;
                writers.iter_mut().for_each(|w| w.send_ack(lease.id()));
                lease.acks().await;
                Ok::<(), WireFault>(())
            });
            match (refused, got) {
                (false, Ok(())) => {}
                (true, Err(e)) => assert_eq!(e.status, WireStatus::SalFull, "{e}"),
                (refused, got) => panic!("refused={refused}: {got:?}"),
            }
        }
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
        user_seq = engine.create_serial_table("public.t").unwrap();
        // Reserve + ingest straight into the catalog — no SAL involved, so the
        // advance lands ONLY in the sys_sequences MemTable.
        let (_base, delta) = engine.reserve_user_sequence(user_seq, 64).unwrap();
        engine.submit(SysFamily::Sequence, delta).unwrap();
        // As the serial path's emit does: the broadcast leaves, the rows stay.
        engine.drain_pending_broadcasts();

        let (disp, _) = test_dispatcher(Vec::new(), &mut engine);
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

/// A whole checkpoint is `checkpoint_base` + an ephemeral round. The base round
/// owns the one generation bump that invalidates every checkpointed view, and
/// makes it only when a push group was written since the last: without one the
/// round moves no base store, and the views checkpointed at the generation in
/// force stay valid. Neither the ephemeral round nor the boot checkpoint's
/// restamp bumps.
#[test]
fn a_checkpoint_bumps_the_generation_once_iff_a_push_was_written() {
    let tmp = tempfile::tempdir().unwrap();
    let mut engine = CatalogEngine::open(tmp.path().to_str().unwrap(), 1).unwrap();
    let (disp, _) = test_dispatcher(Vec::new(), &mut engine);
    let disp = Rc::new(disp);
    let gen = disp.cat().durable_generation();
    let checkpoint = |disp: &Rc<super::super::MasterDispatcher>| {
        let d = Rc::clone(disp);
        disp.reactor()
            .block_on(async move {
                d.checkpoint_base().await?;
                d.checkpoint_ephemeral().await?;
                d.boot_checkpoint().await
            })
            .unwrap();
        disp.cat().durable_generation()
    };

    assert_eq!(checkpoint(&disp), gen, "no push since the last base round");
    disp.unflushed_pushes.set(true);
    assert_eq!(checkpoint(&disp), gen + 1);
    assert_eq!(checkpoint(&disp), gen + 1, "the base round took the push with it");

    drop(disp);
    engine.close();
}
