//! Task scheduling and the tick body: `spawn`, the run queue's dedup, and the
//! waker vtable the wakes land in.

use super::super::test_support::*;
use super::*;

/// A panic during poll must propagate rather than be swallowed: it unwinds
/// through `tick` and, in real use, up to the caller.
#[test]
#[should_panic(expected = "boom")]
fn panic_in_spawned_task_propagates_not_swallowed() {
    let r = make_reactor();
    r.spawn(async {
        panic!("boom");
    });
    r.tick(false);
}

/// A task may spawn during its own poll, and what it spawns runs.
#[test]
fn a_task_spawns_during_its_own_poll() {
    let r = Rc::new(make_reactor());
    let ran = Rc::new(Cell::new(0));
    let (r2, ran2) = (Rc::clone(&r), Rc::clone(&ran));
    r.spawn(async move {
        for _ in 0..64 {
            let ran = Rc::clone(&ran2);
            r2.spawn(async move { ran.set(ran.get() + 1) });
        }
    });
    r.block_until_idle();
    assert_eq!(ran.get(), 64);
}

/// `RunQueue::push` dedups by task key, so N wakes before a task's next poll cost
/// one poll, not N — here through a clone that outlived the waker it came from.
#[test]
fn repeated_wakes_for_one_key_collapse_to_a_single_poll() {
    let r = make_reactor();
    let polls = Rc::new(Cell::new(0u32));
    let kept: Rc<RefCell<Option<Waker>>> = Rc::default();
    let (p, k) = (Rc::clone(&polls), Rc::clone(&kept));
    r.spawn(std::future::poll_fn(move |cx| {
        p.set(p.get() + 1);
        k.borrow_mut().get_or_insert_with(|| cx.waker().clone());
        Poll::<()>::Pending
    }));
    r.tick(false); // the spawn's own first poll
    assert_eq!(polls.get(), 1);

    let waker = kept.borrow().clone().expect("the first poll kept its waker");
    for _ in 0..16 {
        waker.wake_by_ref();
    }
    r.tick(false);
    assert_eq!(
        polls.get(),
        2,
        "16 wakes for one key must cost exactly one further poll"
    );
}
