use gnitz_store::relation::PkRule;

use super::super::fixtures::test_dispatcher;
use super::super::MasterDispatcher;
use crate::catalog::{CatalogEngine, DdlBundle};
use crate::runtime::reactor::test_support::reactor_with_rings;
use crate::runtime::sal::fixtures::test_log;
use crate::runtime::sal::{Apply, Inbound, Routed};
use crate::runtime::test_support::{empty_catalog, within};
use crate::runtime::w2m::fixtures::id;
use gnitz_wire::{ColTabRow, WireFault, WireStatus};

/// A group written on an ACK lease ends on every worker's ACK; one the SAL has
/// no room for is refused with the SAL's own status.
#[test]
fn an_acked_group_ends_on_its_acks_or_is_refused() {
    within(|| {
        for refused in [false, true] {
            let (_tmp, engine) = empty_catalog();
            let (disp, mut writers) = test_dispatcher(2, engine);
            let got = disp.reactor().block_on(async {
                // Wider than the fixture's whole SAL.
                let tick = Apply::Tick {
                    first_round: 2,
                    tids: vec![0; if refused { 2 << 17 } else { 1 }].into(),
                };
                let lease = disp.write_acked(&mut disp.sal().lock().await, tick)?;
                writers.iter_mut().for_each(|w| w.ack(lease.id()));
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

/// A tick the SAL refuses takes no round: the next tick written starts at the
/// round the refused one would have.
#[test]
fn a_refused_tick_takes_no_round() {
    let (_tmp, engine) = empty_catalog();
    let (disp, _) = test_dispatcher(1, engine);
    let mut excl = disp.sal().excl();
    let before = disp.round.get();

    // Wider than the fixture's whole SAL.
    let e = disp
        .emit_tick(&mut excl, &vec![0; 2 << 17])
        .map(drop)
        .expect_err("no room for the tick");
    assert_eq!(e.status, WireStatus::SalFull, "{e}");
    assert_eq!(disp.round.get(), before, "the refused tick took no round");

    disp.emit_tick(&mut excl, &[7, 8]).expect("the tick fits");
    assert_eq!(disp.round.get(), before + 2, "one round per relation ticked");
    drop(excl);
    let mut reader = disp.sal().reader(0);
    let written = reader.next_request(|_, _| None);
    assert!(
        matches!(
            &written,
            Some(Inbound::Apply { apply: Apply::Tick { first_round, tids }, .. })
                if *first_round == before + 1 && tids[..] == [7, 8]
        ),
        "the tick written starts at the round behind the last one taken"
    );
    drop(written);
    disp.into_catalog().close();
}

/// A view's last round is the last one that ticked a relation it reads,
/// directly or through a view under it — taken at the write, with no ACK in —
/// and a refused tick or a tick of other relations leaves it.
#[test]
fn a_views_last_round_is_the_last_tick_of_a_source() {
    use crate::test_support::{col_def, register_identity_view};

    let (_tmp, mut engine) = empty_catalog();
    let cols = [col_def("id", gnitz_wire::TypeCode::U64)];
    let [t, other] = ["public.t", "public.other"].map(|name| engine.create_table(name, &cols, &[0]).unwrap());
    let under = register_identity_view(&mut engine, t, "under", &cols);
    let view = register_identity_view(&mut engine, under, "v", &cols);
    let (disp, _) = test_dispatcher(1, engine);
    let mut excl = disp.sal().excl();
    assert_eq!(disp.last_delta_round(view), 1, "no source ticked");

    let first = disp.round.get() + 1;
    let _unacked = disp.emit_tick(&mut excl, &[other, t, other]).expect("the tick fits");
    assert_eq!(
        disp.last_delta_round(view),
        first + 1,
        "the round of `t`, through the view under it"
    );
    assert_eq!(disp.last_delta_round(under), first + 1);

    disp.emit_tick(&mut excl, &[other]).expect("the tick fits");
    // Wider than the fixture's whole SAL.
    let mut wide = vec![other; 2 << 17];
    wide.push(t);
    disp.emit_tick(&mut excl, &wide)
        .map(drop)
        .expect_err("no room for the tick");
    assert_eq!(disp.last_delta_round(view), first + 1);

    disp.forget_relation(t);
    assert_eq!(disp.last_delta_round(view), 1, "a forgotten source holds no round");
    drop((excl, _unacked));
    disp.into_catalog().close();
}

/// Fill the log until it refuses a group far smaller than any DDL zone, and
/// require `make()`'s zone to be refused with the SAL's own status and its scope
/// discarded; run `refused`, then require the same zone to apply once a
/// checkpoint's base round has rewound the log, and run `applied`.
fn refused_then_applied(
    engine: CatalogEngine,
    make: impl Fn() -> DdlBundle,
    refused: impl FnOnce(&MasterDispatcher),
    applied: impl FnOnce(&MasterDispatcher),
) {
    let (disp, _) = test_dispatcher(0, engine);
    let mut excl = disp.sal().excl();
    for tids in [1 << 12, 1 << 4, 1] {
        let tick = || Apply::Tick {
            first_round: 2,
            tids: vec![0; tids].into(),
        };
        while excl.apply(tick(), id(1)).is_ok() {}
    }
    let watermark = disp.sal().watermark();

    let e = disp
        .emit_zone(&mut excl, |cat, sink| cat.apply_bundle(make(), sink).map(drop))
        .expect_err("no room for the zone");
    assert_eq!(e.status, WireStatus::SalFull, "{e}");
    assert_eq!(
        disp.sal().watermark(),
        watermark,
        "the refused zone's scope is discarded"
    );
    refused(&disp);

    // At worker count 0 the round owes no ACK.
    drop(excl);
    disp.reactor().block_on(disp.checkpoint_base()).unwrap();
    disp.emit_zone(&mut disp.sal().excl(), |cat, sink| cat.apply_bundle(make(), sink))
        .expect("the rewound log takes the zone");
    applied(&disp);

    disp.into_catalog().close();
}

/// A zone the log has no room for is refused with the SAL's own status and
/// reaches no store: every system family holds the rows and weights it held
/// before, and the same bundle applies once a checkpoint has rewound the log.
#[test]
fn a_zone_the_log_refuses_reaches_no_store_and_applies_after_a_checkpoint() {
    use crate::catalog::SysFamily;
    use crate::test_support::{bundle, col_def, col_tab_batch, net_weight, scan_all, table_tab_batch};

    let (_tmp, mut engine) = empty_catalog();
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

    refused_then_applied(
        engine,
        create,
        |disp| {
            assert!(
                disp.cat().registry.relation(tid).is_none(),
                "the refused table is not registered"
            );
            assert_eq!(held(&mut disp.cat_mut()), before);
        },
        |disp| assert!(disp.cat().registry.relation(tid).is_some()),
    );
}

/// An ADD COLUMN the log refuses leaves the table's schema and column records
/// as they were — its descriptor swap cannot be undone, so it must never run —
/// and applies once a checkpoint has rewound the log.
#[test]
fn a_log_refusal_of_add_column_leaves_the_schema() {
    use crate::catalog::SysFamily;
    use crate::test_support::{bundle, col_def, nullable_def, scan_all};

    let (_tmp, mut engine) = empty_catalog();
    let cols = [
        col_def("id", gnitz_wire::TypeCode::U64),
        col_def("v", gnitz_wire::TypeCode::U64),
    ];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let add = || {
        let w = nullable_def("w", gnitz_wire::TypeCode::U64);
        let row = ColTabRow::of(tid, 2, &w.def, None);
        bundle([(SysFamily::Column, crate::test_support::sys_batch([(row, 1)]))])
    };
    let width = |engine: &CatalogEngine| engine.registry.relation(tid).unwrap().schema().num_columns();
    let record = engine.wire_target(tid).unwrap().1.clone();
    let col_rows = scan_all(&mut engine, SysFamily::Column.id()).len();

    refused_then_applied(
        engine,
        add,
        |disp| {
            assert_eq!(width(&disp.cat()), 2, "the refused ADD COLUMN leaves the schema");
            assert_eq!(disp.cat().wire_target(tid).unwrap().1, &record);
            assert_eq!(scan_all(&mut disp.cat_mut(), SysFamily::Column.id()).len(), col_rows);
        },
        |disp| {
            assert_eq!(width(&disp.cat()), 3, "the retry widens the schema");
            assert_eq!(
                scan_all(&mut disp.cat_mut(), SysFamily::Column.id()).len(),
                col_rows + 1
            );
        },
    );
}

/// An ephemeral round flushes the system tables behind its ACKs: a sequence
/// advance that reached the `sys_sequences` MemTable after the base round's
/// flush is durable once the checkpoint has finished.
#[test]
fn an_ephemeral_round_flushes_a_memtable_only_sequence_advance() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let user_seq;
    {
        let mut engine = CatalogEngine::open(dir, 1).unwrap();
        user_seq = engine.create_serial_table("public.t").unwrap();
        // Reserve straight into the catalog — no SAL involved, so the advance
        // lands ONLY in the sys_sequences MemTable.
        engine.reserve_user_sequence(user_seq, 64, &mut |_, _| Ok(())).unwrap();

        let (disp, _) = test_dispatcher(0, engine);
        disp.reactor().block_on(disp.checkpoint_ephemeral()).unwrap();

        // Crash semantics: no `engine.close()`, and no Drop impl — only what the
        // round flushed survives.
        drop(disp);
    }

    let engine = CatalogEngine::open(dir, 1).unwrap();
    assert_eq!(
        engine.sequence_value(user_seq),
        Some(64),
        "sys_sequences high-water must survive a crash right after the ephemeral round"
    );
    engine.close();
}

/// The boot checkpoint's `FlushEph` is the last group of a checkpoint: at a
/// one-byte threshold, the log it leaves calls for no further one.
#[test]
fn the_boot_checkpoint_calls_for_no_other() {
    let (_tmp, engine) = empty_catalog();
    let (reactor, _) = reactor_with_rings(0);
    let sal = test_log(1 << 20, 0, 1).checkpoint_at(1);
    let disp = MasterDispatcher::new(engine, sal, reactor);
    disp.reactor().block_on(disp.boot_checkpoint()).unwrap();
    assert!(disp.sal().cursor() > 0, "the boot checkpoint's group is in the log");
    assert!(!disp.sal().needs_checkpoint());
}

/// A whole checkpoint is `checkpoint_base` + an ephemeral round. The base round
/// owns the one generation bump that invalidates every checkpointed view, and
/// makes it only when a push group was written since the last: without one the
/// round moves no base store, and the views checkpointed at the generation in
/// force stay valid. Neither the ephemeral round nor the boot checkpoint's
/// restamp bumps.
///
/// The base round alone moves the SAL into its next epoch: the ephemeral round
/// and the boot checkpoint leave their groups in the epoch they were written in.
#[test]
fn a_checkpoint_bumps_the_generation_once_iff_a_push_was_written() {
    let (_tmp, engine) = empty_catalog();
    let (disp, _) = test_dispatcher(0, engine);
    let gen = disp.cat().durable_generation();
    let checkpoint = |disp: &MasterDispatcher| {
        let epoch = disp.sal().epoch();
        disp.reactor()
            .block_on(async {
                disp.checkpoint_base().await?;
                assert_eq!(disp.sal().epoch(), epoch + 1, "the base round's rewind");
                assert_eq!(disp.sal().cursor(), 0);
                disp.checkpoint_ephemeral().await?;
                let behind = disp.sal().cursor();
                assert!(behind > 0, "the ephemeral round's group stays in the log");
                disp.boot_checkpoint().await?;
                assert!(disp.sal().cursor() > behind);
                Ok::<(), WireFault>(())
            })
            .unwrap();
        assert_eq!(disp.sal().epoch(), epoch + 1, "one epoch per checkpoint");
        disp.cat().durable_generation()
    };

    assert_eq!(checkpoint(&disp), gen, "no push since the last base round");
    {
        let mut excl = disp.sal().excl();
        let mut scope = excl.begin();
        // No rows, so no schema block is framed; no worker is launched to read it.
        scope.push(1, PkRule::Enforce, &[], Routed::Same(None), true).unwrap();
        assert!(scope.commit().is_some(), "the push is a zone member");
    }
    assert_eq!(checkpoint(&disp), gen + 1);
    assert_eq!(checkpoint(&disp), gen + 1, "the base round took the push with it");

    disp.into_catalog().close();
}
