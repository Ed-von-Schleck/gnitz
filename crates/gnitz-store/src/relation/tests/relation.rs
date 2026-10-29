use super::*;
use crate::test_support::{
    assert_index_holds, id_rows, make_batch_raw, make_schema_u64_i64, pk_u64_two_i64_schema, relation_fixture, solo,
    spec, TID,
};

fn index(id: u64, unique: bool) -> IndexClaim {
    IndexClaim::Index { id, unique }
}

#[test]
fn a_circuit_lives_while_one_claim_remains() {
    let mut registry = relation_fixture(RelationKind::BaseTable, pk_u64_two_i64_schema(), &[], []);
    registry
        .add_indexes(TID, &[(index(70, false), PkColList::from_slice(&[2]))])
        .unwrap();
    registry
        .add_indexes(TID, &[(index(71, true), PkColList::from_slice(&[2]))])
        .unwrap();
    let circuit = |r: &RelationRegistry| r.relation(TID).unwrap().index_on(&[2]).map(|ix| ix.is_unique());
    assert_eq!(
        registry.relation(TID).unwrap().indexes().len(),
        1,
        "one circuit per column list"
    );
    assert_eq!(circuit(&registry), Some(true));

    registry.release_index(TID, index(71, true));
    assert_eq!(
        circuit(&registry),
        Some(false),
        "the non-unique claim keeps the circuit"
    );

    registry.release_index(TID, index(12345, false));
    registry.release_index(TID + 1, index(70, false));
    assert_eq!(circuit(&registry), Some(false), "an unknown claim or owner is a no-op");

    registry.release_index(TID, index(70, false));
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
        registry.register(spec, schema).unwrap();
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

/// The pre-flight's claim enters an index filled from the rows already held,
/// which the catalog's claim then takes over: releasing the pre-flight keeps the
/// index it adopted and drops the one nothing adopted. The pre-flight's claim
/// makes no index unique.
#[test]
fn a_preflight_index_lives_on_only_under_the_claim_that_adopts_it() {
    let schema = pk_u64_two_i64_schema();
    let rows = id_rows(&schema, (1..=3).map(|id| (id, 1)), |bb, id| {
        bb.put_int(id as u128 * 10);
        bb.put_int(7);
    });
    let mut registry = relation_fixture(RelationKind::BaseTable, schema, &[], [rows]);
    let (adopted, abandoned) = (PkColList::from_slice(&[2]), PkColList::from_slice(&[1]));
    for cols in [adopted, abandoned] {
        registry.add_indexes(TID, &[(IndexClaim::Preflight, cols)]).unwrap();
    }
    // Whether the index on `cols` is unique, and how many spans it holds.
    let index = |r: &RelationRegistry, cols: PkColList| {
        let ix = r.relation(TID).unwrap().index_on(cols.as_slice())?;
        let mut spans = r.key_spans(TID, cols.as_slice()).unwrap();
        let mut held = 0;
        while !spans.chunk().is_empty() {
            held += spans.chunk().len();
            spans.advance();
        }
        Some((ix.is_unique(), held))
    };
    assert_eq!(index(&registry, adopted), Some((false, 3)), "filled from the held rows");

    registry
        .add_indexes(TID, &[(IndexClaim::Index { id: 80, unique: true }, adopted)])
        .unwrap();
    registry.release_index(TID, IndexClaim::Preflight);
    assert_eq!(index(&registry, adopted), Some((true, 3)));
    assert_eq!(index(&registry, abandoned), None);
}

/// Not unique, or covering the PK: either one excludes an index.
#[test]
fn a_unique_index_covering_the_pk_has_nothing_left_to_check() {
    let mut registry = relation_fixture(RelationKind::BaseTable, pk_u64_two_i64_schema(), &[], []);
    registry
        .add_indexes(TID, &[(index(901, true), PkColList::from_slice(&[0]))])
        .unwrap(); // exactly the PK
    registry
        .add_indexes(TID, &[(index(902, true), PkColList::from_slice(&[2, 0]))])
        .unwrap(); // the PK plus a payload column
    registry
        .add_indexes(TID, &[(index(903, true), PkColList::from_slice(&[1]))])
        .unwrap(); // the only real check
    registry
        .add_indexes(TID, &[(index(904, false), PkColList::from_slice(&[2]))])
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
/// manifest is of another generation — one a release left behind — or gone.
/// Either way it holds the owner's entries.
#[test]
fn a_reopened_index_resumes_only_at_its_owners_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let spec = spec(9, RelationKind::View(ViewBudget::Plain), schema);
    let indexes = [(index(1, false), PkColList::from_slice(&[1]))];
    let rows = make_batch_raw(&schema, &[(1, 1, 30), (2, 1, 10), (3, 1, 20)]);
    {
        let mut origin = solo(tmp.path());
        origin.register_at(spec, schema, &indexes, None).unwrap();
        origin.ingest(9, rows.clone()).unwrap();
        origin.checkpoint_ephemeral([], 4, |_| true).unwrap();
    }
    // How many indexes a reopen at `generation` filled.
    let reopen = |generation: u64| {
        let mut r = solo(tmp.path());
        let filled = r.register_at(spec, schema, &indexes, Some(generation)).unwrap();
        let ix = r.relation(9).unwrap().index_on(&[1]).unwrap();
        assert_index_holds(ix, &rows, format_args!("generation {generation}"));
        (r, filled)
    };
    assert_eq!(reopen(4).1, 0, "published with its owner");
    {
        // The owner alone publishes at 5: the released index keeps its manifest at 4.
        let (mut r, _) = reopen(4);
        r.release_index(9, index(1, false));
        r.checkpoint_ephemeral([], 5, |_| true).unwrap();
    }
    assert_eq!(reopen(5).1, 1, "of another generation, so filled from the owner");
    assert_eq!(reopen(5).1, 1, "the refused manifest is gone");
}
