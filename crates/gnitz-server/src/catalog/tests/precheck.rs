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
        assert_verdict(
            check_stored_name(name, "table", segment),
            want,
            format_args!("{name:?} (segment: {segment})"),
        );
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

/// Assert that `got` failed with an error containing `want`, or passed when
/// `want` is `""`; `ctx` labels the failure.
fn assert_verdict(got: Result<impl Sized, String>, want: &str, ctx: impl std::fmt::Display) {
    let got = got.err().unwrap_or_default();
    let holds = if want.is_empty() {
        got.is_empty()
    } else {
        got.contains(want)
    };
    assert!(holds, "{ctx}: want {want:?}, got {got:?}");
}

/// An id every family's range admits.
const ID: u64 = 20;

/// That the shape rules refuse `family` rows `(leading id, weight, differing
/// column)` with an error containing `want`, or pass them when `want` is `""`.
/// Every row carries the same payload, except that a row's named column holds
/// another value.
fn assert_shape(family: SysFamily, rows: &[(u64, i64, &str)], want: &str) {
    let wire = family.def();
    let payload = &wire.cols[wire.pk_cols.len()..];
    let mut bb = BatchBuilder::new(family.schema());
    for &(id, weight, differing) in rows {
        push_sys_row(&mut bb, family, [id, 0], weight, |pi| {
            (payload[pi].name == differing) as u64
        });
    }
    assert_verdict(
        check_batch_shape(family, &bb.finish()),
        want,
        format_args!("{family:?} {rows:?}"),
    );
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

/// The id space bounds every sign: below the floor is the bootstrap's. A family
/// whose rows go only with their owner refuses any unpaired `-1` first.
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
        for (rows, want) in [
            (&[(FIRST_USER_TABLE_ID, 1, "")][..], ""),
            (&[(ID, -1, "")], retract("")),
            (&[(below, 1, "")], "cannot CREATE a system"),
            (&[(below, -1, "")], retract("cannot DROP a system")),
            (&[(below, -1, ""), (below, 1, "name")], "cannot ALTER a system"),
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
        let wire = family.def();
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
