use super::*;

#[test]
fn positive_refuses_zero_negative_and_garbage() {
    assert_eq!(positive::<u64>("5"), Some(5));
    assert_eq!(positive::<i64>("-1"), None);
    for v in ["0", "", "x", "5 "] {
        assert_eq!(positive::<u64>(v), None, "{v:?}");
    }
}

#[test]
fn flag_is_off_only_for_zero_and_empty() {
    assert!(!flag("0"));
    assert!(!flag(""));
    assert!(flag("1"));
    assert!(flag("no"));
}
