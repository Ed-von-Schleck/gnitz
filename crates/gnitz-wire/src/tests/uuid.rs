use super::*;

#[test]
fn format_is_canonical_and_parse_accepts_its_spellings() {
    for (v, canonical) in [
        (
            0x550e8400_e29b_41d4_a716_446655440000_u128,
            "550e8400-e29b-41d4-a716-446655440000",
        ),
        (0, "00000000-0000-0000-0000-000000000000"),
        (u128::MAX, "ffffffff-ffff-ffff-ffff-ffffffffffff"),
    ] {
        assert_eq!(format_uuid(v), canonical);
        for s in [
            canonical.to_string(),
            canonical.to_uppercase(),
            canonical.replace('-', ""),
            format!("  {canonical}  "),
        ] {
            assert_eq!(parse_uuid(&s), Some(v), "{s:?}");
        }
    }
}

#[test]
fn parse_rejects_non_canonical_forms() {
    for s in [
        "",
        "abc",
        "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz",
        // Arbitrary hyphen placement.
        "550e84-00e29b-41d4a716-4466-55440000",
        // A sign inside a hex group (from_str_radix would accept "+…").
        "+50e8400e29b41d4a716446655440000",
    ] {
        assert_eq!(parse_uuid(s), None, "{s:?}");
    }
}
