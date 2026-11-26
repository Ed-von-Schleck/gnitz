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
fn every_command_name_is_unique_and_found() {
    for c in COMMANDS {
        for &name in c.names {
            assert!(name.starts_with('\\'));
            assert_eq!(lookup(name).map(MetaCommand::name), Some(c.name()));
        }
    }
    assert!(lookup("\\nope").is_none());
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

#[test]
fn the_shell_page_names_every_command() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/src/clients/shell.md");
    let page = std::fs::read_to_string(path).unwrap();
    let missing: Vec<&str> = COMMANDS
        .iter()
        .flat_map(|c| c.names.first())
        .copied()
        .filter(|n| !page.contains(&format!("`{n}`")) && !page.contains(&format!("`{n} ")))
        .collect();
    assert!(missing.is_empty(), "{path} does not name {missing:?}");
}
