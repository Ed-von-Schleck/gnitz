use super::*;

#[test]
fn a_command_word_ending_in_plus_asks_for_the_verbose_form() {
    let line = MetaLine::parse("\\d+ t").unwrap();
    assert_eq!(line.command.map(MetaCommand::name), Some("\\d"));
    assert!(line.verbose);
    assert_eq!(line.name, 0..3);
    assert!(!MetaLine::parse("\\d t").unwrap().verbose);
}

#[test]
fn lookup_knows_the_aliases() {
    assert_eq!(lookup("\\quit").map(MetaCommand::name), Some("\\q"));
    assert_eq!(lookup("\\help").map(MetaCommand::name), Some("\\?"));
    assert!(lookup("\\nope").is_none());
}

#[test]
fn every_command_name_is_unique_and_found() {
    for c in COMMANDS {
        for &name in c.names {
            assert!(name.starts_with('\\'));
            assert_eq!(lookup(name).map(MetaCommand::name), Some(c.name()));
        }
    }
}

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
fn null_takes_the_whole_value_and_no_value_keeps_it() {
    let mut s = Settings::default();
    Setting::Null.set(&mut s, Some("'a b'")).unwrap();
    assert_eq!(s.null, "a b");
    Setting::Null.set(&mut s, None).unwrap();
    assert_eq!(s.null, "a b");
    Setting::Format.set(&mut s, None).unwrap();
    assert_eq!(s.format, Format::Aligned);
    Setting::Format.set(&mut s, Some("CSV")).unwrap();
    assert_eq!(s.format, Format::Csv);
    assert!(Setting::Format.set(&mut s, Some("xml")).is_err());
}

#[test]
fn expanded_toggles_between_off_and_on() {
    let mut s = Settings::default();
    Setting::Expanded.set(&mut s, None).unwrap();
    assert_eq!(s.expanded, Expanded::On);
    Setting::Expanded.set(&mut s, Some("auto")).unwrap();
    Setting::Expanded.set(&mut s, None).unwrap();
    assert_eq!(s.expanded, Expanded::Off);
}

#[test]
fn expand_home_reads_only_the_own_home() {
    let home = dirs::home_dir().unwrap();
    assert_eq!(expand_home("~/x"), format!("{}/x", home.display()));
    assert_eq!(expand_home("~"), home.display().to_string());
    assert_eq!(expand_home("~bob/x"), "~bob/x");
    assert_eq!(expand_home("a/~/x"), "a/~/x");
}
