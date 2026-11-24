//! [`check_batch_shape`] and the bundle it admits blocks into. The shape rules
//! read a row's key, weight and payload equality and never decode a cell, so
//! every family is driven through one schema-generic row.

use super::*;
use crate::test_support::push_sys_row;
use gnitz_wire::FIRST_USER_TABLE_ID;

/// The lowest id a client may write.
const ID: u64 = FIRST_USER_TABLE_ID;

/// The families a client's bundle may carry.
fn writable() -> impl Iterator<Item = SysFamily> {
    SysFamily::ALL.into_iter().filter(|f| !f.master_only())
}

/// `family` rows `(key, weight, differing column)`: every row carries the same
/// payload, except that a row's named column holds another value.
fn sys_rows(family: SysFamily, rows: &[([u64; 2], i64, &str)]) -> Batch {
    let wire = family.def();
    let payload = &wire.cols[wire.pk_cols.len()..];
    let mut bb = BatchBuilder::new(family.schema());
    for &(key, weight, differing) in rows {
        push_sys_row(&mut bb, family, key, weight, |pi| {
            (payload[pi].name == differing) as u64
        });
    }
    bb.finish()
}

/// That the shape rules refuse `family` rows `(leading id, weight, differing
/// column)` with an error containing `want`, or pass them when `want` is `""`.
fn assert_shape(family: SysFamily, rows: &[(u64, i64, &str)], want: &str) {
    let rows: Vec<_> = rows.iter().map(|&(id, w, col)| ([id, 0], w, col)).collect();
    let got = check_batch_shape(family, &sys_rows(family, &rows)).err();
    assert!(
        got.as_deref()
            .map_or(want.is_empty(), |e| !want.is_empty() && e.contains(want)),
        "{family:?} {rows:?}: want {want:?}, got {got:?}"
    );
}

#[test]
fn a_master_only_family_is_refused_whatever_its_rows() {
    for family in SysFamily::ALL.into_iter().filter(|f| f.master_only()) {
        assert_shape(family, &[(ID, 1, "")], "not writable from the wire");
    }
}

#[test]
fn a_system_row_is_written_at_weight_plus_or_minus_one() {
    for family in writable() {
        for w in [0, 2, -2, i64::MIN] {
            assert_shape(family, &[(ID, w, "")], "expected ±1");
        }
    }
}

#[test]
fn a_repeated_sign_on_one_pk_is_rejected() {
    for family in writable() {
        for w in [1, -1] {
            assert_shape(family, &[(ID, w, ""), (ID, w, "")], "more than one row");
        }
    }
}

/// The id space bounds every sign: below the floor is the bootstrap's. A family
/// whose rows go only with their owner refuses any unpaired `-1` first.
#[test]
fn an_id_is_checked_against_its_familys_range_and_owner() {
    let below = ID - 1;
    for family in writable() {
        let owned = matches!(family, SysFamily::Column | SysFamily::Circuit).then_some("retracted only with its owner");
        for (rows, want) in [
            (&[(ID, 1, "")][..], ""),
            (&[(ID, -1, "")], owned.unwrap_or("")),
            (&[(below, 1, "")], "cannot CREATE a system"),
            (&[(below, -1, "")], owned.unwrap_or("cannot DROP a system")),
            (&[(below, -1, ""), (below, 1, "")], "cannot ALTER a system"),
        ] {
            assert_shape(family, rows, want);
        }
    }
}

/// A pair-keyed row is named by both members of its key.
#[test]
fn a_pair_keyed_row_is_named_by_its_owner_and_member() {
    let batch = sys_rows(SysFamily::Column, &[([ID, 3], -1, "")]);
    let err = check_batch_shape(SysFamily::Column, &batch).err().unwrap();
    assert!(err.contains(&format!("column 3 of owner {ID}")), "{err}");
}

/// The fields a client's rewrite pair may change, per column: everything else
/// of a live row is fixed for its lifetime. `""` changes nothing: an identical
/// pair.
#[test]
fn a_rewrite_pair_may_change_only_the_fields_its_family_declares() {
    for family in writable() {
        let may_change: &[&str] = match family {
            SysFamily::Table | SysFamily::View => &["name"],
            SysFamily::Column => &["name", "is_nullable", "is_hidden"],
            SysFamily::Schema | SysFamily::Index | SysFamily::Circuit => &[],
            SysFamily::Sequence => unreachable!("the wire writes no Sequence row"),
        };
        let wire = family.def();
        let payload = wire.cols[wire.pk_cols.len()..].iter().map(|c| c.name);
        for col in std::iter::once("").chain(payload) {
            let want = match may_change {
                [] => "admits no rewrite pair",
                names if col.is_empty() || names.contains(&col) => "",
                _ => "changes a field it may not",
            };
            assert_shape(family, &[(ID, -1, ""), (ID, 1, col)], want);
            // A pair is read by sign, not by position.
            assert_shape(family, &[(ID, 1, col), (ID, -1, "")], want);
        }
    }
}

#[test]
fn a_bundle_carries_one_block_per_family() {
    let mut bundle = DdlBundle::default();
    let block = || sys_rows(SysFamily::Table, &[([ID, 0], 1, "")]);
    bundle.insert(SysFamily::Table, block()).unwrap();
    let err = bundle.insert(SysFamily::Table, block()).unwrap_err();
    assert!(err.contains("two blocks"), "{err}");
}
