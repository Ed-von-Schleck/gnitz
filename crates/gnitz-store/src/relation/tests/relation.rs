use super::*;
use crate::test_support::{
    TID, assert_index_holds, id_rows, make_batch, make_schema_u64_i64, pk_u64_two_i64_schema, relation_fixture, solo,
    spec, zset_of,
};
use gnitz_zset::schema::pk_keys;

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
        let verdict = |unique| registry.relation_or_err(id)?.admit_index(unique);
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
    let rows = make_batch(&schema, &[(1, 1, 30), (2, 1, 10), (3, 1, 20)]);
    {
        let mut origin = solo(tmp.path());
        origin
            .register_at(spec, schema, &indexes, StateLayout::default(), None)
            .unwrap();
        origin.ingest(9, rows.clone()).unwrap();
        origin.checkpoint_ephemeral([], 4, |_| true).unwrap();
    }
    // How many indexes a reopen at `generation` filled.
    let reopen = |generation: u64| {
        let mut r = solo(tmp.path());
        let filled = r
            .register_at(spec, schema, &indexes, StateLayout::default(), Some(generation))
            .unwrap();
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

/// An index whose columns lead the owner's PK keeps no store: its probe and its
/// spans read the owner's rows, entry for entry, across retractions and updates,
/// and a directory left on such a column list is reclaimed as an orphan.
#[test]
fn an_index_leading_the_pk_reads_the_owners_rows() {
    use crate::read::Probe;
    use gnitz_wire::TypeCode;
    use gnitz_zset::repr::BatchBuilder;
    use gnitz_zset::schema::SchemaColumn;
    use std::num::NonZeroU64;
    use std::path::Path;

    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0, 1],
    );
    let batch = |rows: &[(i64, u64, u64, i64)]| {
        let mut bb = BatchBuilder::new(&schema);
        for &(a, b, v, w) in rows {
            bb.begin_row_natives(&[a as u64 as u128, b as u128], w);
            bb.put_int(v as u128);
            bb.end_row();
        }
        bb.finish()
    };
    let first: Vec<_> = (-2..3i64)
        .flat_map(|a| (0..4u64).map(move |b| (a, b, b * 7, 1)))
        .collect();
    let second = [(0, 1, 7, -1), (1, 2, 14, -1), (1, 2, 99, 1), (2, 9, 5, 1)];
    let leading: [&[u32]; 2] = [&[0], &[0, 1]];
    let r = relation_fixture(
        RelationKind::BaseTable,
        schema,
        &[leading[0], leading[1], &[2]],
        [batch(&first), batch(&second)],
    );
    let rel = r.relation(TID).unwrap();
    let dir = |cols: &[u32]| r.child_dir(TID, ChildKind::Index(PkColList::from_slice(cols)));
    assert!(!rel.index_on(&[2]).unwrap().reads_owner_pk());
    assert!(Path::new(&dir(&[2])).exists());

    for cols in leading {
        let ix = rel.index_on(cols).unwrap();
        assert!(ix.reads_owner_pk(), "{cols:?}");
        assert!(!Path::new(&dir(cols)).exists(), "{cols:?} keeps no store");
        let ix_schema = ix.key_spec().index_schema(&schema);
        // An owner row's entry is its PK.
        let want = rel.full_scan();
        let k = ix.key_spec().key_size();
        let entries: Vec<&[u8]> = (0..want.len()).map(|i| want.get_pk_bytes(i)).collect();

        let mut spans: Vec<&[u8]> = entries.iter().map(|e| &e[..k]).collect();
        spans.dedup();
        let picked: Vec<&[u8]> = spans.iter().copied().step_by(2).collect();
        let probe = |cap: u64| {
            let probe = Probe::Index(PkColList::from_slice(cols), NonZeroU64::new(cap).unwrap());
            r.probe(TID, probe, pk_keys(k, picked.iter().copied())).unwrap()
        };
        let mut hit = Batch::empty_with_schema(&ix_schema);
        entries
            .iter()
            .filter(|e| picked.contains(&&e[..k]))
            .for_each(|e| hit.push_key_row(e, 1));
        let got = probe(u64::MAX);
        assert_eq!(zset_of(&got, &ix_schema), zset_of(&hit, &ix_schema), "{cols:?}");
        assert_eq!(probe(1).len(), 1, "{cols:?}: the cap bounds the entries");

        let mut drained = Vec::new();
        let mut key_spans = r.key_spans(TID, cols).unwrap();
        while !key_spans.chunk().is_empty() {
            let chunk = key_spans.chunk();
            drained.extend((0..chunk.len()).map(|i| (chunk.get_pk_bytes(i).to_vec(), chunk.get_weight(i))));
            key_spans.advance();
        }
        let want_spans: Vec<_> = entries.iter().map(|e| (e[..k].to_vec(), 1)).collect();
        assert_eq!(drained, want_spans, "{cols:?}");
    }

    std::fs::create_dir_all(dir(leading[0])).unwrap();
    r.reclaim_orphan_relation_dirs().unwrap();
    assert!(
        !Path::new(&dir(leading[0])).exists(),
        "an index that reads the PK owns no directory"
    );
    assert!(Path::new(&dir(&[2])).exists());
}
