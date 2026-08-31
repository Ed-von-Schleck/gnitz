use super::*;

/// An unset variable is whatever the reader asked for, either way round.
/// Hermetic: nothing here mutates the process environment, which the
/// thread-per-test runner would let another test observe.
#[test]
fn an_unset_flag_reads_as_its_default() {
    assert!(env_flag("GNITZ_UNSET_FLAG_FOR_TEST", true));
    assert!(!env_flag("GNITZ_UNSET_FLAG_FOR_TEST", false));
}
