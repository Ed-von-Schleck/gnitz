//! The stored-name rule.

use super::*;

/// The user-identifier rule itself is `validate_user_identifier`'s; these rows
/// are the segment carve-out and the canonical form.
#[test]
fn a_stored_name_is_an_identifier_or_a_segment_and_canonical() {
    for (name, segment, want) in [
        ("a_b_9", false, ""),
        ("_seg4096", true, ""),
        ("_", true, ""),
        ("_t", false, "cannot start with '_'"),
        ("seg", true, "a chain segment's name starts with '_'"),
        ("_a.b", true, "invalid characters"),
        ("_é", true, "invalid characters"),
        ("MixedCase", false, "not canonical"),
        ("_Seg", true, "not canonical"),
    ] {
        let got = check_stored_name(name, "table", segment).err().unwrap_or_default();
        let holds = if want.is_empty() {
            got.is_empty()
        } else {
            got.contains(want)
        };
        assert!(holds, "{name:?} (segment: {segment}): want {want:?}, got {got:?}");
    }
}

/// Every name the client's fold admits is stored as it folded it.
#[test]
fn a_canonical_identifier_is_a_stored_name() {
    for name in ["a", "Mixed_Case9", "UPPER", "x_1_Y"] {
        let canonical = gnitz_wire::canonical_identifier(name).unwrap();
        assert_eq!(check_stored_name(&canonical, "table", false), Ok(()), "{name:?}");
    }
}
