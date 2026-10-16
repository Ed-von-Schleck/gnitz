use super::*;
use crate::test_support::{
    make_batch_raw, make_schema_u64_i64, pk_u64_two_i64_schema, relation_fixture, solo, spec, zset_of, TID,
};

fn index(id: u64, unique: bool) -> IndexClaim {
    IndexClaim::Index { id, unique }
}

#[test]
fn a_circuit_lives_while_one_claim_remains() {
    let mut registry = relation_fixture(RelationKind::BaseTable, pk_u64_two_i64_schema(), &[], []);
    registry
        .add_index(TID, index(70, false), PkColList::from_slice(&[2]))
        .unwrap();
    registry
        .add_index(TID, index(71, true), PkColList::from_slice(&[2]))
        .unwrap();
    let circuit = |r: &RelationRegistry| r.relation(TID).unwrap().index_on(&[2]).map(|ix| ix.is_unique());
    assert_eq!(
        registry.relation(TID).unwrap().indexes().len(),
        1,
        "one circuit per column list"
    );
    assert_eq!(circuit(&registry), Some(true));

    registry.release_index(TID, 71);
    assert_eq!(
        circuit(&registry),
        Some(false),
        "the non-unique claim keeps the circuit"
    );

    registry.release_index(TID, 12345);
    registry.release_index(TID + 1, 70);
    assert_eq!(circuit(&registry), Some(false), "an unknown claim or owner is a no-op");

    registry.release_index(TID, 70);
    assert!(
        registry.relation(TID).unwrap().indexes().is_empty(),
        "the last release drops the circuit"
    );
}

/// A store holding each of its keys once, in full, takes a non-unique index, and
/// only a base table a unique one.
#[test]
fn only_a_store_naming_one_row_per_key_admits_an_index() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    let schema = make_schema_u64_i64();
    let plain = RelationKind::View(ViewBudget::Plain);
    let mb = std::num::NonZeroU64::new(1 << 20).unwrap();
    let fed = RelationKind::View(ViewBudget::Fed { delta_bytes: mb });
    let bounded = RelationKind::View(ViewBudget::Bounded { capacity_bytes: mb });
    let owners = [
        (RelationKind::BaseTable, false, Ok(true)),
        (plain, false, Ok(false)),
        (fed, false, Ok(false)),
        (plain, true, Err("repeats its primary key")),
        (bounded, false, Err("without a capacity")),
        (RelationKind::Stream, true, Err("without a capacity")),
    ];
    for (id, (kind, pk_repeats, want)) in (70..).zip(owners) {
        let spec = RelationSpec { pk_repeats, ..spec(id, kind, schema) };
        registry.register(spec).unwrap();
        let verdict = |unique| registry.index_owner(id, unique).map(drop);
        match want {
            Ok(unique_too) => {
                assert!(verdict(false).is_ok(), "{kind:?}");
                assert_eq!(verdict(true).is_ok(), unique_too, "{kind:?}");
            }
            Err(needle) => {
                let err = verdict(false).unwrap_err();
                assert!(err.contains(needle), "{kind:?}: {err}");
                assert!(verdict(true).is_err(), "{kind:?}");
            }
        }
    }
}

/// Not unique, or covering the PK: either one excludes an index.
#[test]
fn a_unique_index_covering_the_pk_has_nothing_left_to_check() {
    let mut registry = relation_fixture(RelationKind::BaseTable, pk_u64_two_i64_schema(), &[], []);
    registry
        .add_index(TID, index(901, true), PkColList::from_slice(&[0]))
        .unwrap(); // exactly the PK
    registry
        .add_index(TID, index(902, true), PkColList::from_slice(&[2, 0]))
        .unwrap(); // the PK plus a payload column
    registry
        .add_index(TID, index(903, true), PkColList::from_slice(&[1]))
        .unwrap(); // the only real check
    registry
        .add_index(TID, index(904, false), PkColList::from_slice(&[2]))
        .unwrap(); // not unique at all

    let cols: Vec<Vec<u32>> = registry
        .relation(TID)
        .unwrap()
        .unique_indexes_to_check()
        .map(|ic| ic.cols().as_slice().to_vec())
        .collect();
    assert_eq!(cols, vec![vec![1u32]]);
}

/// An index reopened beside its owner resumes from the manifest the owner's
/// checkpoint published it with, and is filled from the owner when its own
/// manifest is of another generation or gone. Either way it holds the owner's
/// entries.
#[test]
fn a_reopened_index_resumes_only_at_its_owners_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let spec = spec(9, RelationKind::View(ViewBudget::Plain), schema);
    let cols = PkColList::from_slice(&[1]);
    let rows = make_batch_raw(&schema, &[(1, 1, 30), (2, 1, 10), (3, 1, 20)]);
    {
        let mut origin = solo(tmp.path());
        origin.register(spec).unwrap();
        origin.add_index(9, index(1, false), cols).unwrap();
        origin.ingest(9, rows.clone()).unwrap();
        origin.checkpoint_ephemeral([], 5, |_| true).unwrap();
    }
    let reopen = |index_generation: u64| {
        let mut r = solo(tmp.path());
        r.reopen_view(spec, 5).unwrap();
        r.reopen_index(9, index(1, false), cols, index_generation).unwrap();
        let ix = r.relation(9).unwrap().index_on(&[1]).unwrap();
        let want = gnitz_zset::algebra::index_entries(&rows, &ix.key_spec(), &ix.schema());
        assert_eq!(
            zset_of(&ix.cursor().materialize(), &ix.schema()),
            zset_of(&want, &ix.schema()),
            "generation {index_generation}"
        );
        ix.resumed()
    };
    assert!(reopen(5), "published with its owner");
    assert!(!reopen(4), "of another generation, so filled from the owner");
    assert!(!reopen(5), "the refused manifest is gone");
}
