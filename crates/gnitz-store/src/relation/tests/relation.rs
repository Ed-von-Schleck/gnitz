use super::*;
use crate::test_support::{
    TID, assert_index_holds, fed_view, id_rows, make_schema_u64_i64, pk_only_schema, pk_u64_two_i64_schema,
    relation_fixture, solo, spec,
};

fn index(id: u64, unique: bool) -> IndexClaim {
    IndexClaim::Index { id, unique }
}

/// Claims on one column list share one circuit, filled from the rows already
/// held, whether they enter in one call or across calls. It is unique while a
/// unique claim holds it, and lives until its last claim is released: releasing
/// the pre-flight's claim keeps the index a catalog claim adopted and drops the
/// one nothing adopted. An unknown claim or owner releases nothing.
#[test]
fn a_circuit_lives_while_one_claim_remains() {
    let schema = pk_u64_two_i64_schema();
    let rows = id_rows(&schema, (1..=3).map(|id| (id, 1)), |bb, id| {
        bb.put_int(id as u128 * 10);
        bb.put_int(7);
    });
    let mut registry = relation_fixture(RelationKind::BaseTable, schema, &[], [rows.clone()]);
    let (adopted, abandoned) = (PkColList::from_slice(&[2]), PkColList::from_slice(&[1]));
    registry
        .add_indexes(
            TID,
            &[
                (IndexClaim::Preflight, adopted),
                (index(70, false), adopted),
                (IndexClaim::Preflight, abandoned),
            ],
        )
        .unwrap();
    registry.add_indexes(TID, &[(index(71, true), adopted)]).unwrap();
    let unique = |r: &RelationRegistry, cols: PkColList| {
        r.relation(TID)
            .unwrap()
            .index_on(cols.as_slice())
            .map(SecondaryIndex::is_unique)
    };
    assert_eq!(
        registry.relation(TID).unwrap().indexes().len(),
        2,
        "one circuit per column list"
    );
    for cols in [adopted, abandoned] {
        let what = format_args!("{cols:?} is filled from the held rows");
        assert_index_holds(registry.relation(TID).unwrap(), cols.as_slice(), &rows, what);
    }
    assert_eq!(unique(&registry, adopted), Some(true));

    registry.release_claim(IndexClaim::Preflight);
    assert_eq!(unique(&registry, abandoned), None, "nothing adopted it");
    registry.release_index(TID, index(71, true));
    assert_eq!(
        unique(&registry, adopted),
        Some(false),
        "the non-unique claim keeps the circuit"
    );

    registry.release_index(TID, index(12345, false));
    registry.release_index(TID + 1, index(70, false));
    assert_eq!(
        unique(&registry, adopted),
        Some(false),
        "an unknown claim or owner is a no-op"
    );

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
    let bounded = RelationKind::View(ViewBudget::Bounded {
        capacity_bytes: std::num::NonZeroU64::new(1 << 20).unwrap(),
    });
    let (view_unique, no_capacity) = (Err("cannot carry a UNIQUE"), Err("without a capacity"));
    // The verdicts on a non-unique index and on a unique one.
    let owners = [
        (RelationKind::BaseTable, false, [Ok(()), Ok(())]),
        (plain, false, [Ok(()), view_unique]),
        (fed_view(1 << 20), false, [Ok(()), view_unique]),
        (plain, true, [Err("repeats its primary key"), view_unique]),
        (bounded, false, [no_capacity; 2]),
        (RelationKind::Stream, true, [no_capacity; 2]),
        (RelationKind::SystemCatalog, false, [no_capacity; 2]),
    ];
    for (id, (kind, pk_repeats, verdicts)) in (70..).zip(owners) {
        let spec = RelationSpec { pk_repeats, ..spec(id, kind, schema) };
        registry.register(spec, schema).unwrap();
        for (unique, want) in [false, true].into_iter().zip(verdicts) {
            let got = registry.relation(id).unwrap().admit_index(unique);
            match want {
                Ok(()) => assert!(got.is_ok(), "{kind:?} unique={unique}: {got:?}"),
                Err(needle) => {
                    let err = got.unwrap_err();
                    assert!(err.contains(needle), "{kind:?} unique={unique}: {err}");
                }
            }
        }
    }
}

/// Not unique, or covering the PK: either one excludes an index.
#[test]
fn a_unique_index_covering_the_pk_has_nothing_left_to_check() {
    let mut registry = relation_fixture(RelationKind::BaseTable, pk_u64_two_i64_schema(), &[], []);
    let on = PkColList::from_slice;
    registry
        .add_indexes(
            TID,
            &[
                (index(901, true), on(&[0])),    // exactly the PK
                (index(902, true), on(&[2, 0])), // the PK plus a payload column
                (index(903, true), on(&[1])),    // the only real check
                (index(904, false), on(&[2])),   // not unique at all
            ],
        )
        .unwrap();
    let cols: Vec<Vec<u32>> = registry
        .relation(TID)
        .unwrap()
        .unique_indexes_to_check()
        .map(|ic| ic.cols().as_slice().to_vec())
        .collect();
    assert_eq!(cols, vec![vec![1u32]]);
}

/// A column ALTER swaps in a trailing append of a base table's schema, and is
/// refused for any other relation or descriptor.
#[test]
fn swap_schema_takes_only_a_trailing_append_of_a_base_table() {
    let (narrow, wide) = (make_schema_u64_i64(), pk_u64_two_i64_schema());
    let mut view = relation_fixture(RelationKind::View(ViewBudget::Plain), narrow, &[], []);
    let err = view.swap_schema(TID, wide).unwrap_err();
    assert!(err.contains("not a base table"), "{err}");

    let mut table = relation_fixture(RelationKind::BaseTable, narrow, &[], []);
    let shrunk = pk_only_schema(&[gnitz_wire::TypeCode::U64]);
    let err = table.swap_schema(TID, shrunk).unwrap_err();
    assert!(err.contains("not a trailing append"), "{err}");
    assert_eq!(
        table.relation(TID).unwrap().schema().num_payload_cols(),
        1,
        "a refusal swaps nothing"
    );
    table.swap_schema(TID, wide).unwrap();
    assert_eq!(table.relation(TID).unwrap().schema().num_payload_cols(), 2);
}
