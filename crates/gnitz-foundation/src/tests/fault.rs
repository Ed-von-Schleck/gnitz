use super::*;

/// A seam whose setting is pre-seeded, so no test touches the environment.
fn seam(setting: Option<&str>) -> Seam {
    Seam {
        var: "",
        setting: OnceLock::from(setting.map(str::to_owned)),
        spent: AtomicBool::new(false),
    }
}

#[test]
fn take_once_fires_once_while_armed() {
    let s = seam(Some("1"));
    assert!(s.take_once());
    assert!(!s.take_once());
    assert!(s.armed(), "spending the latch does not disarm the seam");

    for setting in [None, Some("0"), Some("")] {
        let s = seam(setting);
        assert!(!s.armed() && !s.take_once(), "{setting:?}");
        assert!(
            !s.spent.load(Ordering::Relaxed),
            "a disarmed seam never spends its latch"
        );
    }
}

#[test]
fn at_matches_only_the_exact_stage() {
    let s = seam(Some("flush"));
    assert!(s.at("flush"));
    assert!(!s.at("flus"));
    assert!(!seam(None).at(""));
}

#[test]
fn count_reads_a_positive_number_or_nothing_while_disarmed() {
    assert_eq!(seam(Some("50")).count(), Some(50));
    for setting in [None, Some("0"), Some("")] {
        assert_eq!(seam(setting).count(), None, "{setting:?}");
    }
}

#[test]
#[should_panic(expected = "\"5ms\" is not a positive integer")]
fn count_refuses_an_armed_setting_that_is_no_number() {
    seam(Some("5ms")).count();
}
