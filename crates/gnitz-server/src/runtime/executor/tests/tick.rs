use super::*;
use crate::runtime::master::fixtures::test_dispatcher;
use crate::runtime::test_support::{empty_catalog, try_poll_once, within};
use crate::test_support::{register_identity_view, table_engine, u64_i64_cols};

/// A pending commit leaves unticked exactly the targets its relation's tick
/// reaches, through a view in between, until its entry goes.
#[test]
fn all_ticked_reads_each_targets_roots() {
    let cols = u64_i64_cols();
    let (mut engine, t) = table_engine("ticks", "all_ticked", 1, &cols);
    let u = engine.create_table("public.u", &cols, &[0]).unwrap();
    let v1 = register_identity_view(&mut engine, t, "v1", &cols);
    let v2 = register_identity_view(&mut engine, v1, "v2", &cols);
    let w = register_identity_view(&mut engine, u, "w", &cols);
    let (ticks, _requests) = Ticks::new(0);
    let dag = &engine.dag;

    assert!(ticks.all_ticked(dag, [v1, v2, w]));
    ticks.pending.borrow_mut().insert(t, 1);
    assert!(!ticks.all_ticked(dag, [v2]));
    assert!(!ticks.all_ticked(dag, [w, v1]));
    assert!(ticks.all_ticked(dag, [w]));
    // A target that is no view has no roots.
    assert!(ticks.all_ticked(dag, [t, u, u64::MAX]));
    assert!(ticks.all_ticked(dag, []));
    ticks.forget(t);
    assert!(ticks.all_ticked(dag, [v1, v2, w]));
}

/// A watch is woken by a relation it names and by no other, once; one that
/// stops waiting for a reason of its own leaves no entry behind; and a
/// `wake_watches` wakes them all.
#[test]
fn a_watch_is_woken_by_what_it_names_and_leaves_with_its_guard() {
    let (ticks, _requests) = Ticks::new(0);
    let mut on_7 = ticks.watch([7, 8].into_iter().collect()).expect("nothing is pending");
    let mut on_9 = ticks.watch([9].into_iter().collect()).expect("nothing is pending");

    ticks.wake(5);
    assert!(try_poll_once(&mut on_7.woken).is_none(), "5 is named by neither");
    assert!(try_poll_once(&mut on_9.woken).is_none(), "5 is named by neither");
    ticks.wake(8);
    assert!(try_poll_once(&mut on_7.woken).is_some(), "8 is one of its relations");
    assert!(try_poll_once(&mut on_9.woken).is_none(), "and none of the other's");
    assert_eq!(ticks.watches.borrow().len(), 1, "a woken watch is no longer registered");

    drop(on_7);
    assert_eq!(ticks.watches.borrow().len(), 1, "its guard removes only its own entry");
    drop(on_9);
    assert!(ticks.watches.borrow().is_empty(), "a watch given up leaves no entry");

    let mut on_7 = ticks.watch([7].into_iter().collect()).expect("nothing is pending");
    let mut on_9 = ticks.watch([9].into_iter().collect()).expect("nothing is pending");
    ticks.wake_watches();
    assert!(try_poll_once(&mut on_7.woken).is_some());
    assert!(try_poll_once(&mut on_9.woken).is_some());
    assert!(ticks.watches.borrow().is_empty());
}

/// A relation's first commit since a tick took it wakes the watches naming
/// it, and no other, and refuses a watch until a tick takes it; its next
/// commit after that wakes again.
#[test]
fn a_relations_first_unticked_commit_wakes_its_watches_and_refuses_new_ones() {
    within(|| {
        let (_tmp, mut engine) = empty_catalog();
        let cols = u64_i64_cols();
        let t = engine.create_table("public.t", &cols, &[0]).unwrap();
        let u = engine.create_table("public.u", &cols, &[0]).unwrap();
        register_identity_view(&mut engine, t, "v", &cols);
        register_identity_view(&mut engine, u, "w", &cols);
        let (dispatcher, _) = test_dispatcher(0, engine);
        let (shared, _commits, _ticks, _asks) = Shared::new(dispatcher, Duration::ZERO, None);
        let reactor = shared.dispatcher.reactor();
        let commit = |tid| {
            reactor.block_on(async {
                let laid_out = shared.dispatcher.sal().lock().await;
                shared.note_commit_rows(&laid_out, [(tid, 1)].into_iter());
            });
        };
        let ticks = &shared.ticks;

        let mut on_t = ticks.watch([t].into_iter().collect()).expect("nothing is pending");
        let mut on_u = ticks.watch([u].into_iter().collect()).expect("nothing is pending");
        commit(t);
        assert!(try_poll_once(&mut on_t.woken).is_some(), "`t` gained a pending tick");
        assert!(try_poll_once(&mut on_u.woken).is_none(), "`u` did not");
        assert!(
            ticks.watch([t].into_iter().collect()).is_none(),
            "a pending relation refuses a watch"
        );

        reactor
            .block_on(shared.emit_pending_tick(&shared.dispatcher.alone()))
            .expect("the tick of `t` is emitted");
        let mut on_t = ticks.watch([t].into_iter().collect()).expect("the tick took `t`");
        commit(t);
        assert!(try_poll_once(&mut on_t.woken).is_some(), "`t` is pending again");
        assert!(try_poll_once(&mut on_u.woken).is_none());
    });
}
