use super::*;

const V: u128 = 0x550e8400_e29b_41d4_a716_446655440000_u128;

#[test]
fn parse_accepts_canonical_forms() {
    assert_eq!(parse_uuid("550e8400-e29b-41d4-a716-446655440000"), Some(V));
    assert_eq!(parse_uuid("550E8400-E29B-41D4-A716-446655440000"), Some(V));
    assert_eq!(parse_uuid("550e8400e29b41d4a716446655440000"), Some(V));
    assert_eq!(parse_uuid("  550e8400-e29b-41d4-a716-446655440000  "), Some(V));
    assert_eq!(parse_uuid(&format_uuid(u128::MAX)), Some(u128::MAX));
    assert_eq!(parse_uuid(&format_uuid(0)), Some(0));
}

#[test]
fn parse_rejects_non_canonical_forms() {
    assert_eq!(parse_uuid(""), None);
    assert_eq!(parse_uuid("not-a-uuid"), None);
    assert_eq!(parse_uuid("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz"), None);
    // Short hex is not a UUID.
    assert_eq!(parse_uuid("abc"), None);
    // Arbitrary hyphen placement is rejected.
    assert_eq!(parse_uuid("550e84-00e29b-41d4a716-4466-55440000"), None);
    // A sign inside a hex group is rejected (from_str_radix would accept "+…").
    assert_eq!(parse_uuid("+50e8400e29b41d4a716446655440000"), None);
    // 36 chars without correctly placed hyphens.
    assert_eq!(parse_uuid("550e8400ee29b441d4aa716544665544000-"), None);
}

#[test]
fn format_is_canonical() {
    assert_eq!(format_uuid(V), "550e8400-e29b-41d4-a716-446655440000");
    assert_eq!(format_uuid(0), "00000000-0000-0000-0000-000000000000");
}
