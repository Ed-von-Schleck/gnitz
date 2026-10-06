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
fn each_part_is_a_users_identifier() {
    for (schema, name) in [
        ("s", "a.b"),
        ("a.b", "t"),
        ("s", ""),
        ("", "t"),
        ("s", "a b"),
        ("_system", "t"),
        ("s", "_seg"),
    ] {
        assert!(RelName::new(schema, name).is_err(), "{schema:?} {name:?}");
    }
    for text in ["a.b.c", ".t", "s.", ""] {
        assert!(RelName::parse("s", text).is_err(), "{text:?}");
    }
}
