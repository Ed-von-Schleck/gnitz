use std::io::Read;

use super::*;
use crate::runtime::reactor::{egress_pair, Limits};

/// Trains queued for a connection reach the wire whole and in queue order
/// behind the reply already corked, and are counted until the connection's
/// next sync is answered.
#[test]
fn queued_trains_are_shipped_whole_behind_the_reply() {
    const LARGE: usize = 1 << 20;
    let (r, conn, receiver) = egress_pair(Limits::TEST, None);
    let peer = Rc::new(Peer::new(&r, conn, None));
    let subs = Rc::new(Subscriptions::default());
    subs.out.send(vec![0x10; 8], Rc::new(vec![0x11; 100]));
    subs.out.send(vec![0x20; 8], Rc::new(vec![0x21; LARGE]));
    subs.out.send(vec![0x30; 8], Rc::new(vec![0x31; 100]));
    let queued = 3 * 8 + 200 + LARGE;
    assert_eq!(subs.out.unsynced(), queued);

    peer.cork(b"reply");
    let wire = std::thread::spawn(move || {
        let mut wire = vec![0u8; 5 + queued];
        (&receiver).read_exact(&mut wire).expect("the reply and every train");
        wire
    });
    let (shipping, sender) = (Rc::clone(&subs), Rc::clone(&peer));
    r.block_on(async move {
        shipping.ship(&sender).await.expect("an open peer");
        sender.flush_egress().await.expect("an open peer");
    });
    assert!(subs.out.is_empty());
    assert_eq!(subs.out.unsynced(), queued, "a ship is no sync");
    subs.out.synced();
    assert_eq!(subs.out.unsynced(), 0);

    let want = [
        &b"reply"[..],
        &[0x10; 8],
        &[0x11; 100],
        &[0x20; 8],
        &vec![0x21; LARGE],
        &[0x30; 8],
        &[0x31; 100],
    ]
    .concat();
    assert!(wire.join().unwrap() == want, "every train whole, in queue order");
}
