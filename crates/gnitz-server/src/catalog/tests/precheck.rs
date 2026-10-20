//! The stored-name rule and [`check_batch_shape`]. The shape rules read a row's
//! key, weight and payload equality and never decode a cell, so every family is
//! driven through one schema-generic row.

use super::*;
use crate::test_support::push_sys_row;
use gnitz_wire::FIRST_USER_TABLE_ID;
use gnitz_zset::repr::BatchBuilder;

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

/// An id every family's range admits.
const ID: u64 = 20;

/// The id counter the shape rules run under.
const NEXT_ID: u64 = 4096;

/// That the shape rules refuse `family` rows `(leading id, weight, differing
/// column)` with an error containing `want`, or pass them when `want` is `""`.
/// Every row carries the same payload, except that a row's named column holds
/// another value.
fn assert_shape(family: SysFamily, rows: &[(u64, i64, &str)], want: &str) {
    let wire = family.wire();
    let payload = &wire.cols[wire.pk_cols.len()..];
    let mut bb = BatchBuilder::new(family.schema());
    for &(id, weight, differing) in rows {
        push_sys_row(&mut bb, family, [id, 0], weight, |pi| {
            (payload[pi].name == differing) as u64
        });
    }
    let got = check_batch_shape(family, &bb.finish(), NEXT_ID)
        .err()
        .unwrap_or_default();
    let holds = if want.is_empty() {
        got.is_empty()
    } else {
        got.contains(want)
    };
    assert!(holds, "{family:?} {rows:?}: want {want:?}, got {got:?}");
}

#[test]
fn a_system_row_is_written_at_weight_plus_or_minus_one() {
    for family in SysFamily::ALL {
        for w in [0, 2, -2, i64::MIN] {
            assert_shape(family, &[(ID, w, "")], "expected ±1");
        }
    }
}

#[test]
fn a_repeated_sign_on_one_pk_is_rejected() {
    for family in SysFamily::ALL {
        for w in [1, -1] {
            assert_shape(family, &[(ID, w, ""), (ID, w, "")], "more than one row");
        }
    }
}

/// The id space bounds every sign: below the floor is the bootstrap's, at or
/// past the counter was never allocated. A family whose rows go only with their
/// owner refuses any unpaired `-1` first.
#[test]
fn an_id_is_checked_against_its_familys_range_and_owner() {
    let below = FIRST_USER_TABLE_ID - 1;
    for family in SysFamily::ALL {
        let retract = |or: &'static str| {
            if family.retracts_with_owner() {
                "retracted only with its owner"
            } else {
                or
            }
        };
        let unallocated = if family.allocates_ids() { "never allocated" } else { "" };
        for (rows, want) in [
            (&[(FIRST_USER_TABLE_ID, 1, "")][..], ""),
            (&[(NEXT_ID - 1, 1, "")], ""),
            (&[(ID, -1, "")], retract("")),
            (&[(below, 1, "")], "cannot CREATE a system"),
            (&[(below, -1, "")], retract("cannot DROP a system")),
            (&[(below, -1, ""), (below, 1, "name")], "cannot ALTER a system"),
            (&[(NEXT_ID, 1, "")], unallocated),
            (&[(NEXT_ID, -1, "")], retract(unallocated)),
        ] {
            assert_shape(family, rows, want);
        }
    }
    assert_shape(SysFamily::Column, &[(ID, -1, "")], "column 0 of owner 20");
}

/// The fields a client's rewrite pair may change, per column: everything else
/// of a live row is fixed for its lifetime.
#[test]
fn a_rewrite_pair_may_change_only_the_fields_its_family_declares() {
    for family in SysFamily::ALL {
        let may_change: &[&str] = match family {
            SysFamily::Table | SysFamily::View => &["name"],
            SysFamily::Column => &["name", "is_nullable", "is_hidden"],
            SysFamily::Schema | SysFamily::Index | SysFamily::Sequence | SysFamily::Circuit => &[],
        };
        let no_pair = if may_change.is_empty() {
            "admits no rewrite pair"
        } else {
            ""
        };
        assert_shape(family, &[(ID, -1, ""), (ID, 1, "")], no_pair);
        let wire = family.wire();
        for col in &wire.cols[wire.pk_cols.len()..] {
            let want = match may_change {
                [] => "admits no rewrite pair",
                names if names.contains(&col.name) => "",
                _ => "changes a field it may not",
            };
            assert_shape(family, &[(ID, -1, ""), (ID, 1, col.name)], want);
        }
    }
}
