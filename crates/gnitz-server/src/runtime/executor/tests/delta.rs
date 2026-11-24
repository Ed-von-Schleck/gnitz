use crate::runtime::test_support::spawn_read;

use super::*;
use crate::runtime::reactor::test_support::make_reactor;

/// `msg`'s framed bytes, by the wire crate's framing.
fn framed(msg: WireMsg<'_>) -> Vec<u8> {
    gnitz_wire::framed(&msg.encode_to_vec())
}

/// Trains queued for a connection reach the wire whole and in queue order
/// behind the reply already corked, and are counted until the connection's
/// next sync is answered.
#[test]
fn queued_trains_are_shipped_whole_behind_the_reply() {
    const LARGE: usize = 1 << 20;
    let r = make_reactor();
    let (mut peer, receiver) = Peer::over_socketpair(r);
    let subs = Subscriptions::default();
    let id = |id| SubId::new(id).unwrap();
    subs.out.send(7, id(1), Rc::new(vec![0x11; 100]));
    subs.out.send(7, id(2), Rc::new(vec![0x21; LARGE]));
    subs.out.send(9, id(3), Rc::new(vec![0x31; 100]));
    let queued = 3 * HEAD_FRAME_BYTES + 200 + LARGE;
    assert_eq!(subs.out.unsynced(), queued);

    peer.cork(b"reply");
    let wire = spawn_read(receiver, 5 + queued);
    r.block_on(async {
        subs.ship(&mut peer).await.expect("an open peer");
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

/// A ship sends only what was queued since the last one, so the trains a ship
/// drained are never sent again.
#[test]
fn a_second_ship_sends_only_the_trains_queued_since() {
    let r = make_reactor();
    let (mut peer, receiver) = Peer::over_socketpair(r);
    let subs = Subscriptions::default();
    let id = |id| SubId::new(id).unwrap();
    subs.out.send(7, id(1), Rc::new(vec![0x11; 10]));
    subs.out.send(7, id(2), Rc::new(vec![0x21; 20]));
    r.block_on(async { subs.ship(&mut peer).await.expect("an open peer") });
    assert!(subs.out.is_empty());
    subs.out.send(9, id(3), Rc::new(vec![0x31; 30]));
    let len = 3 * HEAD_FRAME_BYTES + 60;
    let wire = spawn_read(receiver, len);
    r.block_on(async {
        subs.ship(&mut peer).await.expect("an open peer");
        peer.flush_egress().await.expect("an open peer");
    });
    assert!(subs.out.is_empty());
    let want = [
        &framed(pushed_marker(7, id(1)))[..],
        &[0x11; 10],
        &framed(pushed_marker(7, id(2))),
        &[0x21; 20],
        &framed(pushed_marker(9, id(3))),
        &[0x31; 30],
    ]
    .concat();
    assert!(wire.join().unwrap() == want, "each train once, in queue order");
}

/// A view's cursor tag is of one (boot, view, spec): the same view under the
/// same spec answers one tag, another view or spec another.
#[test]
fn a_cursor_tag_names_one_view_and_spec() {
    let (feeds, _asks) = Feeds::new();
    let tag = feeds.cursor_tag(7, b"spec");
    assert_eq!(feeds.cursor_tag(7, b"spec"), tag);
    assert_ne!(feeds.cursor_tag(8, b"spec"), tag);
    assert_ne!(feeds.cursor_tag(7, b"other"), tag);
}
