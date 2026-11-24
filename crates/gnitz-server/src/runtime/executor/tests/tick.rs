use super::*;
use crate::runtime::master::fixtures::test_dispatcher;
use crate::runtime::test_support::{try_poll_once, within};
use crate::test_support::empty_catalog;
use crate::test_support::{register_identity_view, u64_i64_cols};

/// A relation's first commit since a tick took it wakes the watches naming
/// it, and no other, and refuses a watch until a tick takes it; its next
/// commit after that wakes again.
#[test]
fn a_relations_first_unticked_commit_wakes_its_watches_and_refuses_new_ones() {
    within(|| {
        let (_tmp, mut engine) = empty_catalog(1);
        let cols = u64_i64_cols();
        let t = engine.create_table("public.t", &cols, &[0]).unwrap();
        let u = engine.create_table("public.u", &cols, &[0]).unwrap();
        let v = register_identity_view(&mut engine, t, "v", &cols);
        let w = register_identity_view(&mut engine, u, "w", &cols);
        let (dispatcher, _) = test_dispatcher(0, engine);
        let (shared, _commits, _asks) = Shared::new(dispatcher, Duration::ZERO, None);
        let reactor = shared.dispatcher.reactor();
        let commit = |tid| {
            reactor.block_on(async {
                let laid_out = shared.dispatcher.sal().lock().await;
                shared.dispatcher.credit(&laid_out, tid, 1);
            });
        };
        let mut on_t = shared.dispatcher.watch([v]).expect("nothing is pending");
        let mut on_u = shared.dispatcher.watch([w]).expect("nothing is pending");
        commit(t);
        assert!(try_poll_once(&mut on_t).is_some(), "`t` gained a pending tick");
        assert!(try_poll_once(&mut on_u).is_none(), "`u` did not");
        assert!(
            shared.dispatcher.watch([v]).is_none(),
            "a pending relation refuses a watch"
        );

        reactor
            .block_on(shared.dispatcher.emit_tick(&shared.dispatcher.alone()))
            .expect("the tick of `t` is emitted");
        let mut on_t = shared.dispatcher.watch([v]).expect("the tick took `t`");
        commit(t);
        assert!(try_poll_once(&mut on_t).is_some(), "`t` is pending again");
        assert!(try_poll_once(&mut on_u).is_none());
    });
}

/// A read of a view whose sources are all ticked takes the guard at once and
/// ticks nothing, while a relation it does not read stays pending.
#[test]
fn a_read_of_a_ticked_view_neither_waits_on_nor_ticks_an_unrelated_commit() {
    within(|| {
        let (_tmp, mut engine) = empty_catalog(1);
        let cols = u64_i64_cols();
        let t = engine.create_table("public.t", &cols, &[0]).unwrap();
        let u = engine.create_table("public.u", &cols, &[0]).unwrap();
        let v = register_identity_view(&mut engine, t, "v", &cols);
        let w = register_identity_view(&mut engine, u, "w", &cols);
        let (dispatcher, _) = test_dispatcher(0, engine);
        let (shared, _commits, _asks) = Shared::new(dispatcher, Duration::ZERO, None);
        let reactor = shared.dispatcher.reactor();
        reactor.block_on(async {
            let laid_out = shared.dispatcher.sal().lock().await;
            shared.dispatcher.credit(&laid_out, u, 1);
        });

        let read = try_poll_once(fresh_read_lock(shared, [v], reactor.now()));
        assert!(read.is_some_and(|locked| locked.is_ok()), "`v` is fresh");
        assert!(!shared.dispatcher.all_ticked([w]), "`u` is still pending");
        assert!(
            try_poll_once(fresh_read_lock(shared, [w], reactor.now())).is_none(),
            "a read of `w` drains"
        );
    });
}
