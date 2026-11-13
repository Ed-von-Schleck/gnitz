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
        let mut on_t = shared
            .dispatcher
            .watch([t].into_iter().collect())
            .expect("nothing is pending");
        let mut on_u = shared
            .dispatcher
            .watch([u].into_iter().collect())
            .expect("nothing is pending");
        commit(t);
        assert!(try_poll_once(&mut on_t.woken).is_some(), "`t` gained a pending tick");
        assert!(try_poll_once(&mut on_u.woken).is_none(), "`u` did not");
        assert!(
            shared.dispatcher.watch([t].into_iter().collect()).is_none(),
            "a pending relation refuses a watch"
        );

        reactor
            .block_on(shared.dispatcher.emit_tick(&shared.dispatcher.alone()))
            .expect("the tick of `t` is emitted");
        let mut on_t = shared
            .dispatcher
            .watch([t].into_iter().collect())
            .expect("the tick took `t`");
        commit(t);
        assert!(try_poll_once(&mut on_t.woken).is_some(), "`t` is pending again");
        assert!(try_poll_once(&mut on_u.woken).is_none());
    });
}
