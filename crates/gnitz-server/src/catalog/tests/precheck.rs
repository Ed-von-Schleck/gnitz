//! The name rules and [`check_batch_shape`]. The shape rules read a row's key,
//! weight and payload equality and never decode a cell, so every family is
//! driven through one schema-generic row.

use super::*;
use crate::test_support::push_sys_row;
use gnitz_wire::{CATALOG_ID_CEILING, FIRST_USER_TABLE_ID};
use gnitz_zset::repr::BatchBuilder;

#[test]
fn an_unstorable_name_is_rejected_and_a_leading_underscore_is_not() {
    for (name, fragment) in [
        ("", "cannot be empty"),
        ("a b", "invalid characters"),
        ("a.b", "invalid characters"),
        ("a/b", "invalid characters"),
        ("é", "invalid characters"),
        ("MixedCase", "not canonical"),
    ] {
        let err = reject_unstorable_name(name, "table").unwrap_err();
        assert!(err.contains(fragment), "name {name:?} gave: {err}");
    }
    for name in ["_", "_foo", "_seg4096", "_fk_16_1", "a_b_9"] {
        reject_unstorable_name(name, "table").unwrap();
    }
}

/// An id every family's range admits.
const ID: u64 = 20;

/// The id counter the shape rules run under.
const NEXT_ID: u64 = 4096;

/// What the shape rules say of `family` rows `(leading id, weight, differing
/// column)`, `""` when they pass: every row carries the same payload, except
/// that a row's named column holds another value.
fn shape(family: SysFamily, rows: &[(u64, i64, &str)]) -> String {
    let wire = family.wire();
    let payload = &wire.cols[wire.pk_cols.len()..];
    let mut bb = BatchBuilder::new(family.schema());
    for &(id, weight, differing) in rows {
        push_sys_row(&mut bb, family, [id, 0], weight, |pi| {
            (payload[pi].name == differing) as u64
        });
    }
    check_batch_shape(family, &bb.finish(), NEXT_ID)
        .err()
        .unwrap_or_default()
}

/// Rows a drop cascade retracts with their owner, and so no client delta may.
fn owned(family: SysFamily) -> bool {
    matches!(family, SysFamily::Column | SysFamily::Circuit)
}

#[test]
fn a_system_row_is_written_at_weight_plus_or_minus_one() {
    for family in SysFamily::ALL {
        assert_eq!(shape(family, &[(ID, 1, "")]), "", "{family:?}");
        for w in [0, 2, -2, i64::MIN] {
            let err = shape(family, &[(ID, w, "")]);
            assert!(err.contains("expected ±1"), "{family:?} at {w}: {err}");
        }
    }
}

#[test]
fn a_repeated_sign_on_one_pk_is_rejected() {
    for family in SysFamily::ALL {
        for w in [1, -1] {
            let err = shape(family, &[(ID, w, ""), (ID, w, "")]);
            assert!(err.contains("more than one row"), "{family:?} at {w}: {err}");
        }
    }
}

#[test]
fn only_an_owned_row_refuses_an_unpaired_retraction() {
    for family in SysFamily::ALL {
        let err = shape(family, &[(ID, -1, "")]);
        let want = if owned(family) {
            "retracted only with its owner"
        } else {
            ""
        };
        assert!(
            err.contains(want) && err.is_empty() == want.is_empty(),
            "{family:?}: {err}"
        );
    }
    assert!(shape(SysFamily::Column, &[(ID, -1, "")]).contains("column 0 of owner 20"));
}

#[test]
fn an_id_outside_a_familys_range_is_rejected_whatever_its_sign() {
    for family in SysFamily::ALL {
        let floor = FIRST_USER_TABLE_ID;
        assert_eq!(shape(family, &[(floor, 1, "")]), "", "{family:?}");
        let below = floor - 1;
        let drop = if owned(family) {
            "retracted only with its owner"
        } else {
            "cannot DROP a system"
        };
        for (rows, want) in [
            (&[(below, 1, "")][..], "cannot CREATE a system"),
            (&[(below, -1, "")], drop),
            (&[(below, -1, ""), (below, 1, "name")], "cannot ALTER a system"),
        ] {
            let err = shape(family, rows);
            assert!(err.contains(want), "{family:?} {want}: {err}");
        }

        let capped = matches!(
            family,
            SysFamily::Schema | SysFamily::Table | SysFamily::View | SysFamily::Index
        );
        assert_eq!(shape(family, &[(NEXT_ID - 1, 1, "")]), "", "{family:?}");
        for id in [NEXT_ID, CATALOG_ID_CEILING - 1, CATALOG_ID_CEILING] {
            let err = shape(family, &[(id, 1, "")]);
            assert_eq!(err.contains("never allocated"), capped, "{family:?} {id}: {err}");
            assert_eq!(err.is_empty(), !capped, "{family:?} {id}: {err}");
            if capped {
                let err = shape(family, &[(id, -1, "")]);
                assert!(err.contains("never allocated"), "{family:?} {id}: {err}");
            }
        }
    }
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
        let wire = family.wire();
        for col in &wire.cols[wire.pk_cols.len()..] {
            let err = shape(family, &[(ID, -1, ""), (ID, 1, col.name)]);
            let want = match may_change {
                [] => "admits no rewrite pair",
                names if names.contains(&col.name) => "",
                _ => "changes a field it may not",
            };
            assert!(
                err.contains(want) && err.is_empty() == want.is_empty(),
                "{family:?}.{}: {err}",
                col.name
            );
        }
    }
}
