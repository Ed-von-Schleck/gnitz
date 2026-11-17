use super::*;

/// A seam whose setting is pre-read, so no test touches the environment.
fn var<T>(value: Option<T>) -> Var<T> {
    Var {
        name: "GNITZ_X",
        value: OnceLock::from(value),
    }
}

fn stages() -> Stage {
    Stage::new("GNITZ_X", &["index", "store"])
}

#[test]
fn off_or_unset_disarms_every_kind() {
    for raw in [None, Some(""), Some("0"), Some("false"), Some("NO")] {
        assert_eq!(Switch::read("GNITZ_X", raw), None, "{raw:?}");
        assert_eq!(Count::read("GNITZ_X", raw), None, "{raw:?}");
        assert_eq!(stages().read("GNITZ_X", raw), None, "{raw:?}");
    }
}

#[test]
fn each_kind_reads_its_setting() {
    for raw in ["1", "true", "YES"] {
        assert_eq!(Switch::read("GNITZ_X", Some(raw)), Some(true), "{raw:?}");
    }
    assert_eq!(Count::read("GNITZ_X", Some("50")), Some(50));
    assert_eq!(stages().read("GNITZ_X", Some("store")), Some("store"));
}

#[test]
#[should_panic(expected = "GNITZ_X=\"flase\" is not a boolean")]
fn a_switch_refuses_a_misspelling() {
    Switch::read("GNITZ_X", Some("flase"));
}

#[test]
#[should_panic(expected = "GNITZ_X=\"5ms\" is not a positive integer")]
fn a_count_refuses_a_value_that_is_no_number() {
    Count::read("GNITZ_X", Some("5ms"));
}

#[test]
#[should_panic(expected = "GNITZ_X=\"stroe\" is not one of index|store")]
fn a_stage_refuses_a_point_it_does_not_list() {
    stages().read("GNITZ_X", Some("stroe"));
}

#[test]
fn a_count_is_none_while_disarmed() {
    assert_eq!(Count(var(Some(50))).count(), Some(50));
    assert_eq!(Count(var(None)).count(), None);
}

#[test]
fn take_once_fires_once_while_armed() {
    let s = Switch {
        var: var(Some(true)),
        spent: AtomicBool::new(false),
    };
    assert!(s.take_once());
    assert!(!s.take_once());
    assert!(s.armed(), "spending the latch does not disarm the seam");

    let s = Switch {
        var: var(None),
        spent: AtomicBool::new(false),
    };
    assert!(!s.armed() && !s.take_once());
    assert!(
        !s.spent.load(Ordering::Relaxed),
        "a disarmed seam never spends its latch"
    );
}

#[test]
fn at_matches_only_the_stage_named() {
    let s = Stage { var: var(Some("b")), stages: &["a", "b"] };
    assert!(s.at("b") && !s.at("a"));
    let s = Stage { var: var(None), stages: &["a", "b"] };
    assert!(!s.at("a") && !s.at("b"));
}
