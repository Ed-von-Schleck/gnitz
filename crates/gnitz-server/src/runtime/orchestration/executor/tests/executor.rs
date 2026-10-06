use super::*;
use crate::test_support::{make_batch_raw, make_schema_u64_i64};
use gnitz_wire::WireConflictMode::{Error, Update};

/// A stream is append-only and nothing clamps its weights, so every weight `>= 1`
/// is admitted and the first `<= 0` one is named. `Error` mode is refused whatever
/// the rows, an empty delta included.
#[test]
fn a_stream_push_admits_only_positive_weights_outside_error_mode() {
    let weighted = |weights: &[i64]| {
        let rows: Vec<(u64, i64, i64)> = weights.iter().enumerate().map(|(i, &w)| (i as u64, w, 0)).collect();
        make_batch_raw(&make_schema_u64_i64(), &rows)
    };
    assert_eq!(check_stream_push(7, None, Update), Ok(()));
    assert_eq!(check_stream_push(7, Some(&weighted(&[1, 5, 1])), Update), Ok(()));
    for (weights, row) in [(&[0][..], 0), (&[-1], 0), (&[1, 1, -3], 2), (&[2, 0, -1], 1)] {
        let e = check_stream_push(7, Some(&weighted(weights)), Update).expect_err("a non-positive weight");
        let w = weights[row];
        assert!(
            e.contains("table 7") && e.contains(&format!("row {row} of this push carries weight {w}")),
            "{e}"
        );
    }
    for batch in [None, Some(weighted(&[1]))] {
        let e = check_stream_push(7, batch.as_ref(), Error).expect_err("Error mode");
        assert!(e.contains("conflict mode 'error'"), "{e}");
    }
}

/// A parked poll is woken by a relation it watches and by no other, once; and
/// one that stops waiting for a reason of its own leaves no entry behind.
#[test]
fn a_parked_poll_is_woken_by_what_it_watches_and_leaves_with_its_guard() {
    use crate::runtime::test_support::try_poll_once;

    let waiters = PollWaiters::default();
    let mut on_7 = waiters.park([7, 8].into_iter().collect());
    let mut on_9 = waiters.park([9].into_iter().collect());

    waiters.wake(5);
    assert!(try_poll_once(&mut on_7.woken).is_none(), "5 is watched by neither");
    waiters.wake(8);
    assert!(try_poll_once(&mut on_7.woken).is_some(), "8 is one of its relations");
    assert!(try_poll_once(&mut on_9.woken).is_none(), "and none of the other's");
    assert_eq!(waiters.parked.borrow().len(), 1, "a woken poll is no longer parked");

    drop(on_7);
    assert_eq!(waiters.parked.borrow().len(), 1, "its guard removes only its own entry");
    drop(on_9);
    assert!(
        waiters.parked.borrow().is_empty(),
        "a poll that timed out is not left parked"
    );
}
