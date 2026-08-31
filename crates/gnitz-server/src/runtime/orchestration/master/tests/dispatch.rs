mod worker_liveness_tests {
    use super::super::*;
    use crate::runtime::test_support::SharedRegion;

    const RING_CAP: usize = 64 * 1024;

    // Build an inert dispatcher for the pre-reactor liveness-probe paths: real
    // but empty W2M rings so the bootstrap wait loops can `try_read` (always
    // None here, so they reach the park / no-progress arm that probes), a null
    // SAL and catalog (untouched on the no-frame path), and the given worker
    // pids. Returns the ring regions so they outlive the dispatcher
    // (W2mReceiver holds the raw ptrs but does not own them).
    fn probe_dispatcher(worker_pids: Vec<i32>) -> (MasterDispatcher, Vec<SharedRegion>) {
        let nw = worker_pids.len();
        let mut rings = Vec::with_capacity(nw);
        for _ in 0..nw {
            rings.push(unsafe { crate::runtime::w2m::test_ring(RING_CAP) });
        }
        // `-1` eventfds: nothing parks on them here, and `eventfd_signal`
        // discards a failed write (the counter is only a wake hint).
        let disp = MasterDispatcher::new(
            nw,
            worker_pids,
            std::ptr::null_mut(),
            0,
            SalWriter::new(std::ptr::null_mut(), -1, 0, nw),
            Rc::new(W2mReceiver::new(rings.iter().map(|r| r.ptr()).collect())),
            vec![-1; nw],
        );
        (disp, rings)
    }

    // Fork a child that exits immediately, then block-reap it. The pid is now a
    // confirmed non-child, so a later `waitpid` on it yields ECHILD — a
    // deterministic "dead" verdict with no race against the probe.
    fn spawn_and_reap_dead() -> i32 {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            unsafe { libc::_exit(0) };
        }
        let mut status = 0;
        unsafe { libc::waitpid(pid, &mut status, 0) };
        pid
    }

    #[test]
    fn check_workers_reports_neg1_for_live_worker() {
        // Child blocks in pause() so it is unambiguously alive across the probe.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            unsafe {
                libc::pause();
                libc::_exit(0);
            }
        }
        let (disp, _rings) = probe_dispatcher(vec![pid]);
        assert_eq!(disp.check_workers(), -1, "a live worker must not be reported dead");
        assert_eq!(
            disp.worker_pids.borrow()[0],
            pid,
            "a live worker's pid must be retained"
        );
        unsafe {
            libc::kill(pid, libc::SIGKILL);
            let mut status = 0;
            libc::waitpid(pid, &mut status, 0);
        }
        drop(disp);
    }

    #[test]
    fn check_workers_reaps_and_zeroes_then_does_not_re_report() {
        // An exited child becomes a zombie; the detecting `waitpid(WNOHANG)`
        // reaps it (rpid > 0) and must zero the slot so a second probe does not
        // re-`waitpid` a non-child and re-report the same worker as dead.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            unsafe { libc::_exit(0) };
        }
        let (disp, _rings) = probe_dispatcher(vec![pid]);
        // Bounded poll until the zombie is reaped by the probe (the child exits
        // near-instantly). The bound keeps a regression from hanging the suite.
        let mut detected = -1;
        for _ in 0..2000 {
            detected = disp.check_workers();
            if detected >= 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(detected, 0, "the exited worker must be detected dead");
        assert_eq!(disp.worker_pids.borrow()[0], 0, "a reaped pid must be zeroed");
        assert_eq!(disp.check_workers(), -1, "a zeroed worker must not be re-reported");
        drop(disp);
    }

    #[test]
    fn collect_acks_errors_when_worker_dies_before_acking() {
        // Boot-recovery path: wait_all_workers finds an empty ring and reaches
        // the park arm, whose liveness probe must surface the dead worker as a
        // clean error instead of looping on the park forever.
        let dead = spawn_and_reap_dead();
        let (disp, _rings) = probe_dispatcher(vec![dead]);
        let err = disp
            .collect_acks("recovery sync")
            .expect_err("a dead worker must fail ack collection");
        assert!(err.contains("worker 0"), "error names the dead worker: {err}");
        assert!(
            err.contains("recovery sync"),
            "error identifies the recovery path: {err}"
        );
        drop(disp);
    }

    #[test]
    fn collect_acks_and_relay_errors_when_worker_dies_mid_backfill() {
        // Backfill path: collect_acks_and_relay makes no progress on an empty
        // ring and reaches the !progressed arm, whose probe must surface the
        // dead worker.
        let dead = spawn_and_reap_dead();
        let (disp, _rings) = probe_dispatcher(vec![dead]);
        let err = disp
            .collect_acks_and_relay(true, "backfill relay")
            .expect_err("a dead worker must fail the backfill relay");
        assert!(err.contains("worker 0"), "error names the dead worker: {err}");
        assert!(
            err.contains("backfill relay"),
            "error identifies the backfill path: {err}"
        );
        drop(disp);
    }
}

mod checkpoint_finalize_tests {
    use super::super::*;
    use crate::runtime::sal::SalWriter;
    use crate::runtime::test_support::SharedRegion;
    use crate::runtime::w2m::W2mReceiver;
    use gnitz_engine::catalog::{CatalogEngine, FIRST_USER_TABLE_ID, SEQ_TAB_ID};

    const SAL_SIZE: usize = 4096;

    fn finalize_temp_dir(name: &str) -> String {
        gnitz_engine_testkit::scratch_dir("checkpoint_finalize", name)
    }

    /// Zero workers over a real catalog and a real (if tiny, and necessarily
    /// writable) SAL region: every ACK collection is an empty loop, so the
    /// checkpoint rounds and the finalizer run for real without a forked
    /// cluster.
    fn finalize_dispatcher(catalog: *mut CatalogEngine, sal_region: &SharedRegion) -> MasterDispatcher {
        MasterDispatcher::new(
            0,
            Vec::new(),
            catalog,
            0,
            SalWriter::new(sal_region.ptr(), -1, SAL_SIZE as u64, 0),
            Rc::new(W2mReceiver::new(Vec::new())),
            Vec::new(),
        )
    }

    /// The checkpoint finalizer must flush system tables before resetting the SAL:
    /// a `commit_serial_range_durable` advance can land in the `sys_sequences`
    /// MemTable during the drain window (bypassing the committer barrier and the SAL
    /// reset that already ran in the base round), so the finalizer is its only
    /// durability event before the ephemeral reset makes the SAL log useless for
    /// recovery. Strip the flush and a crash right after (no `close()`) loses it.
    #[test]
    fn checkpoint_post_ack_flushes_memtable_only_sequence_advance() {
        let dir = finalize_temp_dir("seq_survives_reset");
        let user_seq = FIRST_USER_TABLE_ID + 3;
        {
            let mut engine = CatalogEngine::open(&dir, 1).unwrap();

            // Reserve + ingest straight into the catalog — no SAL involved, so the
            // advance lands ONLY in the sys_sequences MemTable (mirrors
            // test_user_sequence_durable_roundtrip's setup).
            let (base, delta, _lsn) = engine.reserve_user_sequence(user_seq, 64);
            assert_eq!(base, 1);
            engine.ingest_to_family(SEQ_TAB_ID, &delta).unwrap();
            assert_eq!(engine.user_sequence(user_seq), Some(64));

            // A fake but real, writable SAL region: checkpoint_reset() stores 0 at
            // the base pointer, so it must not be null.
            let sal_region = SharedRegion::new(SAL_SIZE);
            let disp = finalize_dispatcher(&mut engine as *mut CatalogEngine, &sal_region);

            // The finalizer under guard: must durably flush sys_sequences before the
            // reset.
            disp.checkpoint_post_ack().unwrap();
            drop(disp);

            // Crash semantics: no engine.close(). No Drop impl, so only flushed data survives.
            drop(engine);
        }

        let engine = CatalogEngine::open(&dir, 1).unwrap();
        assert_eq!(
            engine.user_sequence(user_seq),
            Some(64),
            "sys_sequences high-water must survive a crash right after the checkpoint finalize"
        );
        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `reclaim_base` owns the generation bump that invalidates every
    /// checkpointed view and index before it publishes a newer base cut: exactly
    /// one per call, and `boot_checkpoint` consumes that one rather than adding
    /// its own.
    ///
    /// Zero workers: every round's ACK collection is an empty loop, so the two
    /// rounds and the finalizer run for real without a forked cluster.
    #[test]
    fn reclaim_base_bumps_once_and_boot_checkpoint_consumes_it() {
        let dir = finalize_temp_dir("reclaim_base_bumps");
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        assert_eq!(engine.durable_generation(), 0, "fresh DB starts at generation 0");

        let sal_region = SharedRegion::new(SAL_SIZE);
        let disp = finalize_dispatcher(&mut engine as *mut CatalogEngine, &sal_region);

        // Epoch 0 is the empty-slot sentinel, so the region needs a boot reset
        // before any group is written — what `server_main` does after worker ACKs.
        disp.rewind_sal(1);

        assert_eq!(disp.reclaim_base().unwrap(), 1, "the base round bumps G → G+1");
        assert_eq!(disp.cat().durable_generation(), 1);
        assert_eq!(disp.reclaim_base().unwrap(), 2, "and exactly once per call");

        // The boot checkpoint's base round bumps 2 → 3; its ephemeral round stamps
        // 3 (`note_flush_round`'s `debug_assert_eq!` is what pins that here).
        disp.boot_checkpoint(1).unwrap();
        assert_eq!(
            disp.cat().durable_generation(),
            3,
            "boot_checkpoint advances by exactly one",
        );

        drop(disp);
        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The pair `reclaim_base` + `restamp_derived` is a whole checkpoint: the
    /// ephemeral round re-stamps at exactly the generation the base round
    /// invalidated everything at, so nothing is left rebuild-on-boot.
    #[test]
    fn restamp_derived_stamps_the_durable_generation() {
        let dir = finalize_temp_dir("restamp_stamps_durable_gen");
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();

        let sal_region = SharedRegion::new(SAL_SIZE);
        let disp = finalize_dispatcher(&mut engine as *mut CatalogEngine, &sal_region);
        disp.rewind_sal(1);

        let gen = disp.reclaim_base().unwrap();
        assert_ne!(
            disp.last_ephemeral_gen.get(),
            gen,
            "the base round alone leaves every derived manifest behind the new generation"
        );

        // Empty drain set: zero workers hold no pending deltas.
        disp.restamp_derived(&[]).unwrap();
        assert_eq!(
            disp.last_ephemeral_gen.get(),
            disp.cat().durable_generation(),
            "the ephemeral round re-validates the derived state at the durable generation"
        );

        drop(disp);
        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
