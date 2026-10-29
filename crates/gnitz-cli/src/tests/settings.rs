use super::*;

#[test]
fn a_boolean_setting_reads_postgres_spellings_in_any_case() {
    let mut s = Settings::default();
    Setting::Timing.set(&mut s, Some("ON")).unwrap();
    assert!(s.timing);
    Setting::Pager.set(&mut s, Some("no")).unwrap();
    assert!(!s.pager);
    Setting::Pager.set(&mut s, Some("yes")).unwrap();
    assert!(s.pager);
    Setting::Footer.set(&mut s, None).unwrap();
    assert!(!s.footer);
    assert!(Setting::Footer.set(&mut s, Some("maybe")).is_err());
}

#[test]
fn null_reads_a_sql_string_else_the_whole_value_and_no_value_keeps_it() {
    let mut s = Settings::default();
    Setting::Null.set(&mut s, Some("'a b'")).unwrap();
    assert_eq!(s.null, "a b");
    Setting::Null.set(&mut s, None).unwrap();
    assert_eq!(s.null, "a b");
    Setting::Null.set(&mut s, Some("'it''s'")).unwrap();
    assert_eq!(s.null, "it's");
    Setting::Null.set(&mut s, Some("(none)")).unwrap();
    assert_eq!(s.null, "(none)");
}

#[test]
fn format_reads_its_names_in_any_case_and_no_value_keeps_it() {
    let mut s = Settings::default();
    Setting::Format.set(&mut s, None).unwrap();
    assert_eq!(s.format, Format::Aligned);
    Setting::Format.set(&mut s, Some("CSV")).unwrap();
    assert_eq!(s.format, Format::Csv);
    assert!(Setting::Format.set(&mut s, Some("xml")).is_err());
}

#[test]
fn expanded_toggles_between_off_and_on_and_reads_boolean_spellings() {
    let mut s = Settings::default();
    Setting::Expanded.set(&mut s, None).unwrap();
    assert_eq!(s.expanded, Expanded::On);
    Setting::Expanded.set(&mut s, Some("auto")).unwrap();
    Setting::Expanded.set(&mut s, None).unwrap();
    assert_eq!(s.expanded, Expanded::Off);
    Setting::Expanded.set(&mut s, Some("yes")).unwrap();
    assert_eq!(s.expanded, Expanded::On);
    Setting::Expanded.set(&mut s, Some("off")).unwrap();
    assert_eq!(s.expanded, Expanded::Off);
}
