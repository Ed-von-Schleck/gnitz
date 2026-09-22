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
fn flag_reads_its_spellings_in_any_case_and_nothing_else() {
    for v in ["", "0", "false", "False", "NO"] {
        assert_eq!(flag(v), Some(false), "{v:?}");
    }
    for v in ["1", "true", "TRUE", "yes"] {
        assert_eq!(flag(v), Some(true), "{v:?}");
    }
    // One letter from "false", which read as on while any non-zero value did.
    for v in ["flase", "2", "on", " 1"] {
        assert_eq!(flag(v), None, "{v:?}");
    }
}

#[test]
fn an_unset_variable_reads_as_its_default() {
    assert_eq!(env_num("GNITZ_TEST_ENV_UNSET", 7u64), 7);
    assert!(env_flag("GNITZ_TEST_ENV_UNSET", true));
    assert_eq!(read("GNITZ_X", None, 7u64, positive, ""), 7);
}

#[test]
fn a_set_variable_overrides_its_default() {
    assert_eq!(read("GNITZ_X", Some("4096"), 1u64, positive, ""), 4096);
    assert!(!read("GNITZ_X", Some("no"), true, flag, ""));
}

#[test]
#[should_panic(expected = "GNITZ_X=\"134217728x\" is not a positive integer")]
fn a_value_its_rule_rejects_panics() {
    read("GNITZ_X", Some("134217728x"), 1u64, positive, "a positive integer");
}
