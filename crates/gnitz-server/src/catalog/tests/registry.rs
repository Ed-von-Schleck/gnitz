//! `validated_run_last` is what stands between an untrusted `count` and the
//! catalog object-id counter in `allocate_ids`.

use super::*;

#[test]
fn validated_run_last_bounds_a_run_by_count_overflow_and_ceiling() {
    let c = 100;
    assert_eq!(validated_run_last(10, 1, c), Some(10));
    assert_eq!(
        validated_run_last(10, 90, c),
        Some(99),
        "a run ending at ceiling - 1 fits"
    );
    assert_eq!(
        validated_run_last(10, 91, c),
        None,
        "a run reaching the ceiling does not"
    );
    assert_eq!(validated_run_last(10, 0, c), None, "an empty run");
    assert_eq!(validated_run_last(2, u64::MAX, u64::MAX), None, "overflow against base");
}
