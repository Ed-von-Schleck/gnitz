use super::*;

#[test]
fn glob_matches_psql_wildcards_case_insensitively() {
    assert!(glob("ord*", "Orders"));
    assert!(glob("?rders", "orders"));
    assert!(glob("*", ""));
    assert!(!glob("ord", "orders"));
    assert!(glob("public.*", "public.t"));
}

#[test]
fn lookup_strips_the_verbose_suffix_and_knows_the_aliases() {
    assert_eq!(lookup("\\d+").map(MetaCommand::name), Some("\\d"));
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
fn watch_sql_runs_past_its_first_word() {
    let watch = lookup("\\watch").unwrap();
    assert!(matches!(watch.arg(5), ArgKind::Sql));
    assert!(matches!(lookup("\\x").unwrap().arg(1), ArgKind::None));
}
