use std::io::Read;

use super::*;
use crate::runtime::reactor::test_support::make_reactor;
use crate::runtime::test_support::try_poll_once;
use crate::test_support::{apply_ddl, col_def, scratch_dir, table_blocks};
use gnitz_wire::TypeCode;

/// Trains queued for a connection reach the wire whole and in queue order
/// behind the reply already corked, and are counted until the connection's
/// next sync is answered.
#[test]
fn queued_trains_are_shipped_whole_behind_the_reply() {
    const LARGE: usize = 1 << 20;
    let r = make_reactor();
    let (peer, receiver) = Peer::over_socketpair(&r);
    let subs = Subscriptions::default();
    let id = |id| SubId::new(id).unwrap();
    subs.out.send(7, id(1), Rc::new(vec![0x11; 100]));
    subs.out.send(7, id(2), Rc::new(vec![0x21; LARGE]));
    subs.out.send(9, id(3), Rc::new(vec![0x31; 100]));
    let queued = 3 * MARKER_BYTES + 200 + LARGE;
    assert_eq!(subs.out.unsynced(), queued);

    peer.cork(b"reply");
    let wire = std::thread::spawn(move || {
        let mut wire = vec![0u8; 5 + queued];
        (&receiver).read_exact(&mut wire).expect("the reply and every train");
        wire
    });
    r.block_on(async {
        subs.ship(&peer).await.expect("an open peer");
        peer.flush_egress().await.expect("an open peer");
    });
    assert!(subs.out.is_empty());
    assert_eq!(subs.out.unsynced(), queued, "a ship is no sync");
    subs.out.synced();
    assert_eq!(subs.out.unsynced(), 0);

    let want = [
        &b"reply"[..],
        &framed(pushed_marker(7, id(1))),
        &[0x11; 100],
        &framed(pushed_marker(7, id(2))),
        &vec![0x21; LARGE],
        &framed(pushed_marker(9, id(3))),
        &[0x31; 100],
    ]
    .concat();
    assert!(wire.join().unwrap() == want, "every train whole, in queue order");
}

/// A parked sync is woken by a relation it watches and by no other, once; and
/// one that stops waiting for a reason of its own leaves no entry behind.
#[test]
fn a_parked_sync_is_woken_by_what_it_watches_and_leaves_with_its_guard() {
    let waiters = SyncWaiters::default();
    let mut on_7 = waiters.park([7, 8].into_iter().collect());
    let mut on_9 = waiters.park([9].into_iter().collect());

    waiters.wake(5);
    assert!(try_poll_once(&mut on_7.woken).is_none(), "5 is watched by neither");
    waiters.wake(8);
    assert!(try_poll_once(&mut on_7.woken).is_some(), "8 is one of its relations");
    assert!(try_poll_once(&mut on_9.woken).is_none(), "and none of the other's");
    assert_eq!(waiters.parked.borrow().len(), 1, "a woken sync is no longer parked");

    drop(on_7);
    assert_eq!(waiters.parked.borrow().len(), 1, "its guard removes only its own entry");
    drop(on_9);
    assert!(
        waiters.parked.borrow().is_empty(),
        "a sync that timed out is not left parked"
    );
}

/// A view's cursor tag is of one registration: a relation dropped and created
/// again under its id answers a cursor of the first as foreign.
#[test]
fn a_cursor_tag_names_one_registration() {
    let (feeds, _asks) = Feeds::new();
    let cols = [col_def("id", TypeCode::U64)];
    let mut engine = CatalogEngine::open(&scratch_dir("executor", "cursor_tag_registration"), 1).unwrap();
    let sid = engine.schema_id("public").unwrap();
    let tid = engine.allocate_ids(1).unwrap();
    let create = || table_blocks(tid, sid, "t", &cols, &[0], Default::default());
    apply_ddl(&mut engine, create()).unwrap();
    let tag = feeds.cursor_tag(&engine, tid, b"spec");
    assert_eq!(feeds.cursor_tag(&engine, tid, b"spec"), tag);

    let [_, (family, row)] = create();
    apply_ddl(&mut engine, [(family, row.negated())]).unwrap();
    apply_ddl(&mut engine, create()).unwrap();
    assert_ne!(feeds.cursor_tag(&engine, tid, b"spec"), tag);
    engine.close();
}
