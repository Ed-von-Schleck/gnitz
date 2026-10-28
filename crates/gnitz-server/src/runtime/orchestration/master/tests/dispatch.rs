use super::super::fixtures::test_dispatcher;
use crate::catalog::CatalogEngine;
use crate::runtime::sal::WorkerSet;
use crate::runtime::sal::{Apply, DirectGroup, GroupData, GroupTargets};
use crate::runtime::test_support::{assert_child_exited_ok, fork_child, try_poll_once, within};
use gnitz_foundation::posix_io::retry_eintr;
use gnitz_wire::{ColTabRow, SysRow, WireFault, WireStatus};

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
        assert_eq!(disp.reactor().block_on(disp.worker_death()), 1);
    });
}

/// A group written on an ACK lease ends on every worker's ACK; one the SAL has
/// no room for is refused with the SAL's own status.
#[test]
fn an_acked_group_ends_on_its_acks_or_is_refused() {
    within(|| {
        for refused in [false, true] {
            let (disp, mut writers) = test_dispatcher(vec![0, 0], std::ptr::null_mut());
            let got = disp.reactor().block_on(async {
                // Wider than the fixture's whole SAL.
                let tick = Apply::Tick {
                    first_round: 2,
                    tids: vec![0; if refused { 2 << 17 } else { 1 }].into(),
                };
                let lease = disp.write_acked(&disp.sal().lock().await, DirectGroup::new(tick))?;
                writers.iter_mut().for_each(|w| w.send_ack(lease.targets().request_id));
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

/// Fill the log until it refuses a group far smaller than any DDL zone.
fn fill_log(excl: &mut crate::runtime::sal::SalExcl<'_>) {
    for tids in [1 << 12, 1 << 4, 1] {
        let tick = || Apply::Tick {
            first_round: 2,
            tids: vec![0; tids].into(),
        };
        while excl.write(&DirectGroup::new(tick())).is_ok() {}
    }
}

/// A zone the log has no room for is refused with the SAL's own status and
/// reaches no store: every system family holds the rows and weights it held
/// before, and the same bundle applies once a checkpoint has reset the log.
#[test]
fn a_zone_the_log_refuses_reaches_no_store_and_applies_after_a_checkpoint() {
    use crate::catalog::SysFamily;
    use crate::test_support::{bundle, col_def, col_tab_batch, net_weight, scan_all, table_tab_batch};

    let tmp = tempfile::tempdir().unwrap();
    let mut engine = CatalogEngine::open(tmp.path().to_str().unwrap(), 1).unwrap();
    let tid = engine.allocate_ids(1).unwrap();
    let cols: Vec<_> = (0..32)
        .map(|i| col_def(&format!("a_column_with_a_long_name_{i}"), gnitz_wire::TypeCode::U64))
        .collect();
    let create = || {
        bundle([
            (SysFamily::Column, col_tab_batch(tid, &cols, 1)),
            (SysFamily::Table, table_tab_batch(&[(tid, "t", 1)])),
        ])
    };
    let held = |engine: &mut CatalogEngine| -> Vec<(usize, i64)> {
        SysFamily::ALL
            .map(|f| (scan_all(engine, f.id()).len(), net_weight(engine, f.id())))
            .to_vec()
    };
    let before = held(&mut engine);

    let (disp, _) = test_dispatcher(Vec::new(), &mut engine);
    let mut excl = try_poll_once(disp.sal().lock()).expect("uncontended");
    fill_log(&mut excl);
    let watermark = disp.sal().watermark();

    let refused = disp
        .emit_zone(&mut excl, "test", |cat, sink| {
            cat.apply_bundle(create(), sink).map(drop)
        })
        .expect_err("no room for the zone");
    assert_eq!(refused.status, WireStatus::SalFull, "{refused}");
    assert!(!engine.registry.has_id(tid), "the refused table is not registered");
    assert_eq!(held(&mut engine), before);
    assert_eq!(
        disp.sal().watermark(),
        watermark,
        "the refused zone's scope is discarded"
    );

    disp.checkpoint_post_ack(&mut excl).unwrap();
    disp.emit_zone(&mut excl, "test", |cat, sink| cat.apply_bundle(create(), sink))
        .expect("the reset log takes the zone");
    assert!(engine.registry.has_id(tid));

    drop(excl);
    drop(disp);
    engine.close();
}

/// An ADD COLUMN the log refuses leaves the table's schema and column records
/// as they were — its descriptor swap cannot be undone, so it must never run —
/// and applies once a checkpoint has reset the log.
#[test]
fn a_log_refusal_of_add_column_leaves_the_schema() {
    use crate::catalog::SysFamily;
    use crate::test_support::{bundle, col_def, nullable_def, scan_all};

    let tmp = tempfile::tempdir().unwrap();
    let mut engine = CatalogEngine::open(tmp.path().to_str().unwrap(), 1).unwrap();
    let cols = [
        col_def("id", gnitz_wire::TypeCode::U64),
        col_def("v", gnitz_wire::TypeCode::U64),
    ];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let add = || {
        let mut bb = gnitz_zset::repr::BatchBuilder::new(SysFamily::Column.schema());
        ColTabRow::of(tid, 2, &nullable_def("w", gnitz_wire::TypeCode::U64).def, None).write(&mut bb, 1);
        bundle([(SysFamily::Column, bb.finish())])
    };
    let width = |engine: &CatalogEngine| engine.registry.relation(tid).unwrap().schema().num_columns();
    let record = engine.schema_record(tid).unwrap();
    let col_rows = scan_all(&mut engine, SysFamily::Column.id()).len();

    let (disp, _) = test_dispatcher(Vec::new(), &mut engine);
    let mut excl = try_poll_once(disp.sal().lock()).expect("uncontended");
    fill_log(&mut excl);

    let refused = disp
        .emit_zone(&mut excl, "test", |cat, sink| cat.apply_bundle(add(), sink).map(drop))
        .expect_err("no room for the zone");
    assert_eq!(refused.status, WireStatus::SalFull, "{refused}");
    assert_eq!(width(&engine), 2, "the refused ADD COLUMN leaves the schema");
    assert_eq!(engine.schema_record(tid).unwrap(), record);
    assert_eq!(scan_all(&mut engine, SysFamily::Column.id()).len(), col_rows);

    disp.checkpoint_post_ack(&mut excl).unwrap();
    disp.emit_zone(&mut excl, "test", |cat, sink| cat.apply_bundle(add(), sink))
        .expect("the reset log takes the zone");
    assert_eq!(width(&engine), 3, "the retry widens the schema");
    assert_eq!(scan_all(&mut engine, SysFamily::Column.id()).len(), col_rows + 1);

    drop(excl);
    drop(disp);
    engine.close();
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
        // Reserve straight into the catalog — no SAL involved, so the advance
        // lands ONLY in the sys_sequences MemTable.
        engine.reserve_user_sequence(user_seq, 64, &mut |_, _| Ok(())).unwrap();

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
    let gen = disp.cat().durable_generation();
    let checkpoint = |disp: &super::super::MasterDispatcher| {
        disp.reactor()
            .block_on(async {
                disp.checkpoint_base().await?;
                disp.checkpoint_ephemeral().await?;
                disp.boot_checkpoint().await
            })
            .unwrap();
        disp.cat().durable_generation()
    };

    assert_eq!(checkpoint(&disp), gen, "no push since the last base round");
    {
        let mut excl = try_poll_once(disp.sal().lock()).expect("uncontended");
        let scope = excl.begin("test");
        // No rows, so no schema block is framed, and addressed to no worker, so
        // no ACK is owed.
        let push = DirectGroup::push(
            1,
            &[],
            GroupData::Same(None),
            GroupTargets {
                set: WorkerSet::EMPTY,
                ..GroupTargets::UNADDRESSED
            },
        );
        scope.write(&push, true).unwrap();
        assert!(scope.commit(), "the push is a zone member");
    }
    assert_eq!(checkpoint(&disp), gen + 1);
    assert_eq!(checkpoint(&disp), gen + 1, "the base round took the push with it");

    drop(disp);
    engine.close();
}
