use super::*;

#[test]
fn a_user_identifier_is_nonempty_charset_and_not_underscore_led() {
    for name in ["orders", "Orders123", "my_table", "a", "A1_b2", "1a", "99_problems"] {
        assert!(validate_user_identifier(name).is_ok(), "rejected valid: {name}");
    }
    for name in [
        "_private",
        "_",
        "__init__",
        "",
        "has space",
        "has-dash",
        "has.dot",
        "has@",
        "table$",
    ] {
        assert!(validate_user_identifier(name).is_err(), "accepted invalid: {name}");
    }
    assert_eq!(canonical_identifier("MyTab_1"), Ok("mytab_1".into()));
    assert!(canonical_identifier("_x").is_err());
}
