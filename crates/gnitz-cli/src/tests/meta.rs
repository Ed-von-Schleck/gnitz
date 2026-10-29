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
fn expand_home_reads_only_the_own_home() {
    let home = dirs::home_dir().unwrap();
    assert_eq!(expand_home("~/x"), format!("{}/x", home.display()));
    assert_eq!(expand_home("~"), home.display().to_string());
    assert_eq!(expand_home("~bob/x"), "~bob/x");
    assert_eq!(expand_home("a/~/x"), "a/~/x");
    assert_eq!(expand_home(""), "");
}

#[test]
fn a_synopsis_out_of_brackets_is_an_argument_the_command_needs() {
    let needs = |name| lookup(name).is_some_and(MetaCommand::needs_arg);
    for name in ["\\use", "\\watch", "\\i", "\\dump", "\\restore", "\\backup"] {
        assert!(needs(name), "{name}");
    }
    for name in [
        "\\c", "\\d", "\\dt", "\\o", "\\e", "\\echo", "\\!", "\\pset", "\\x", "\\q",
    ] {
        assert!(!needs(name), "{name}");
    }
}
