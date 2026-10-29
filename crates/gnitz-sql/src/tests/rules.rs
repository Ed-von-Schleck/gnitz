use super::*;
use crate::test_support::{rejected, shadowed_cols};

#[test]
fn a_name_resolves_to_its_one_visible_column_by_physical_index() {
    let cols = shadowed_cols();
    for (name, want) in [("id", Some(1)), ("Id", Some(1)), ("_k", None), ("missing", None)] {
        assert_eq!(find_unique_column(&cols, name).unwrap(), want, "{name}");
    }
    assert_eq!(
        rejected(find_unique_column(&cols, "a")),
        "column reference 'a' is ambiguous"
    );
    assert_eq!(rejected(require_column(&cols, "_k")), "column '_k' not found");
}
