use super::*;

#[test]
fn a_name_is_its_folded_parts() {
    let t = RelName::new("S", "T").unwrap();
    assert_eq!((t.schema(), t.name(), t.key()), ("s", "t", "s.t"));
    assert_eq!((t.to_string().as_str(), t.spelled_name()), ("S.T", "T"));
    assert_eq!(RelName::parse("other", "s.T").unwrap(), t);
    assert_eq!(RelName::parse("S", "t").unwrap(), t);
    assert_eq!(t.sibling("U").unwrap(), RelName::new("s", "u").unwrap());
    assert_ne!(RelName::new("other", "t").unwrap(), t);
}

#[test]
fn a_name_prints_as_it_was_written() {
    let written = RelName::parse("other", "S.T").unwrap();
    assert!(written.is_qualified());
    assert_eq!(written.to_string(), "S.T");
    let alone = RelName::parse("S", "T").unwrap();
    assert!(!alone.is_qualified());
    assert_eq!(alone.to_string(), "T");
    assert_eq!(alone, written);
    assert_eq!(alone.sibling("U").unwrap().to_string(), "U");
    assert_eq!(written.sibling("U").unwrap().to_string(), "S.U");
}

#[test]
fn each_part_is_a_users_identifier() {
    for (schema, name) in [
        ("s", "a.b"),
        ("a.b", "t"),
        ("s", ""),
        ("", "t"),
        ("s", "a b"),
        ("_other", "t"),
        ("_system", "_x"),
        ("public", "_x"),
        ("s", "_seg"),
    ] {
        assert!(RelName::new(schema, name).is_err(), "{schema:?} {name:?}");
    }
    for text in ["a.b.c", ".t", "s.", ""] {
        assert!(RelName::parse("s", text).is_err(), "{text:?}");
    }
}

#[test]
fn the_system_schema_is_spelled_in_any_case() {
    let t = RelName::new("_system", "tables").unwrap();
    assert_eq!((t.schema(), t.name(), t.key()), ("_system", "tables", "_system.tables"));
    let shouted = RelName::parse("public", "_SYSTEM.Tables").unwrap();
    assert_eq!(shouted, t);
    assert_eq!(shouted.to_string(), "_SYSTEM.Tables");
}
