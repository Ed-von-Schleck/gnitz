use super::*;
use crate::test_support::{pk_only_schema, pk_payload_schema};
use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec, StoreConfig};
use gnitz_wire::{KeyRange, PkKeys, ReadBound, ReadSpec, TypeCode};
use std::num::NonZeroU64;

const NW: usize = 4;

/// A registry holding one relation of `schema` placed by `placement`, id 16.
fn registry(schema: SchemaDescriptor, placement: Placement) -> RelationRegistry {
    // A stream is storeless, so it registers without a directory.
    let mut registry = RelationRegistry::new("", gnitz_zset::algebra::Slot::SOLO, StoreConfig::default());
    let spec = RelationSpec {
        id: 16,
        kind: RelationKind::Stream,
        schema,
        placement,
        pk_repeats: false,
        compress: false,
    };
    registry.register(spec).unwrap();
    registry
}

/// `keys` of `stride`-byte big-endian ids, as a `PkSet` bound.
fn pk_set(stride: usize, keys: &[u64]) -> ReadBound {
    let keys: Vec<Vec<u8>> = keys.iter().map(|k| k.to_be_bytes()[8 - stride..].to_vec()).collect();
    ReadBound::PkSet(PkKeys::from_keys(stride, keys.iter().map(Vec::as_slice)))
}

/// A read reaches exactly the workers its bound names: a replicated relation's
/// one copy, a confined range's owner, a key set's owners, and every worker
/// where the bound names none. Only a key set spread over several owners splits,
/// each owner sent its own keys and no other.
#[test]
fn a_read_reaches_the_owners_its_bound_names() {
    let schema = pk_only_schema(&[TypeCode::U64]);
    let keyed = Placement::full_pk(&schema);
    let owner = |k: u64| keyed.owner(&k.to_be_bytes(), NW).unwrap();
    let set = |keys: &[u64]| ReadSpec::all_rows(pk_set(8, keys)).encode();
    let range = |first, last| {
        ReadSpec::all_rows(ReadBound::Range(KeyRange::new(
            PkColList::from_slice(&[0]),
            &[],
            first,
            last,
        )))
        .encode()
    };
    let b = (1..).find(|&k| owner(k) != owner(0)).expect("keys spread");
    let replicated = Placement::Replicated;
    let wide = pk_only_schema(&[TypeCode::U128]);

    let cases = [
        ("replicated", schema, replicated, None, WorkerSet::one(0)),
        (
            "replicated key set",
            schema,
            replicated,
            Some(set(&[0, b])),
            WorkerSet::one(0),
        ),
        ("unbounded", schema, keyed, None, WorkerSet::ALL),
        (
            "confined range",
            schema,
            keyed,
            Some(range(42, 42)),
            WorkerSet::one(owner(42)),
        ),
        ("empty range", schema, keyed, Some(range(10, 2)), WorkerSet::one(0)),
        ("one owner", schema, keyed, Some(set(&[42])), WorkerSet::one(owner(42))),
        ("empty set", schema, keyed, Some(set(&[])), WorkerSet::one(0)),
        ("Local", schema, Placement::Local, Some(set(&[0, b])), WorkerSet::ALL),
        (
            "foreign stride",
            wide,
            Placement::full_pk(&wide),
            Some(set(&[0, b])),
            WorkerSet::ALL,
        ),
    ];
    let scan = |spec: Vec<u8>| Read::ScanSpec {
        tid: 16,
        reply_layout: 0,
        spec: spec.into(),
    };
    for (case, schema, placement, blob, want) in cases {
        let registry = registry(schema, placement);
        let r = route_read(&scan(blob.unwrap_or_default()), registry.relation(16).unwrap(), NW);
        assert_eq!(r.set, want, "{case}");
        assert!(r.per_worker.is_none(), "{case}: every worker is sent the whole bound");
    }

    let registry = registry(schema, keyed);
    let spread = route_read(&scan(set(&[0, b])), registry.relation(16).unwrap(), NW);
    assert_eq!(spread.set, WorkerSet::one(owner(0)).with(owner(b)));
    let own_keys = (0..NW)
        .map(|w| match w {
            _ if w == owner(0) => set(&[0]),
            _ if w == owner(b) => set(&[b]),
            _ => Vec::new(),
        })
        .collect();
    assert_eq!(spread.per_worker, Some(own_keys));
}

/// An own-PK probe goes to the owners of its keys — under a replicated
/// relation's full key, every worker holding it whole — where a scan of that
/// relation reads its one copy. An index probe goes to every worker holding the
/// relation, even where its spans are as wide as the PK.
#[test]
fn a_probe_routes_by_the_store_it_reads() {
    let schema = pk_payload_schema(&[TypeCode::U64]);
    let full = Placement::full_pk(&schema);
    let owner = |k: u64| full.owner(&k.to_be_bytes(), NW).unwrap();
    let b = (1..).find(|&k| owner(k) != owner(0)).expect("keys spread");
    let keys = pk_set(8, &[0, b]).encode();
    let probe = |probe| Read::HasPk { tid: 16, probe, keys: (&keys[..]).into() };
    let index = Probe::Index(PkColList::from_slice(&[1]), NonZeroU64::MAX);

    let replicated = registry(schema, Placement::Replicated);
    let rel = replicated.relation(16).unwrap();
    for own in [Probe::Pk, Probe::PkColumn(1)] {
        let r = route_read(&probe(own), rel, NW);
        assert_eq!(r.set, WorkerSet::one(owner(0)).with(owner(b)), "{own:?}");
        assert!(r.per_worker.is_some(), "{own:?}: each worker is sent its own keys");
    }
    let scan = Read::ScanSpec {
        tid: 16,
        reply_layout: 0,
        spec: ReadSpec::all_rows(pk_set(8, &[0, b])).encode().into(),
    };
    let r = route_read(&scan, rel, NW);
    assert_eq!(r.set, WorkerSet::one(Placement::REPLICA_OWNER as usize));

    let keyed = registry(schema, full);
    let r = route_read(&probe(index), keyed.relation(16).unwrap(), NW);
    assert_eq!((r.set, r.per_worker), (WorkerSet::ALL, None));
}
