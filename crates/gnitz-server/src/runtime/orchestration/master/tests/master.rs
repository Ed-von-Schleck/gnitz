use super::*;
use crate::test_support::pk_only_schema;
use gnitz_wire::{Cut, KeyRange, PkKeys, ReadBound, ReadSpec, TypeCode};

const NW: usize = 4;

/// A read reaches exactly the workers its bound names: a replicated relation's
/// one copy, a confined range's owner, a key set's owners, and every worker
/// where the bound names none. Only a key set spread over several owners splits,
/// each owner sent its own keys and no other.
#[test]
fn a_read_reaches_the_owners_its_bound_names() {
    let keyed = pk_only_schema(&[TypeCode::U64]);
    let owner = |k: u64| keyed.worker_for_pk(&k.to_be_bytes(), NW);
    let set = |keys: &[u64]| {
        let keys: Vec<[u8; 8]> = keys.iter().map(|k| k.to_be_bytes()).collect();
        ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_keys(8, keys.iter().map(|k| &k[..])))).encode()
    };
    let range = |start, end| {
        ReadSpec::all_rows(ReadBound::Range(KeyRange::new(
            PkColList::from_slice(&[0]),
            &[],
            start,
            end,
        )))
        .encode()
    };
    let b = (1..).find(|&k| owner(k) != owner(0)).expect("keys spread");
    let replicated = keyed.with_placement(Placement::Replicated);

    let cases = [
        ("replicated", replicated, None, WorkerSet::one(0)),
        ("replicated key set", replicated, Some(set(&[0, b])), WorkerSet::one(0)),
        ("unbounded", keyed, None, WorkerSet::ALL),
        (
            "confined range",
            keyed,
            Some(range(Cut::before(42), Cut::after(42))),
            WorkerSet::one(owner(42)),
        ),
        (
            "empty range",
            keyed,
            Some(range(Cut::after(9), Cut::before(3))),
            WorkerSet::one(0),
        ),
        ("one owner", keyed, Some(set(&[42])), WorkerSet::one(owner(42))),
        ("empty set", keyed, Some(set(&[])), WorkerSet::one(0)),
        (
            "Local",
            keyed.with_placement(Placement::Local),
            Some(set(&[0, b])),
            WorkerSet::ALL,
        ),
        (
            "foreign stride",
            pk_only_schema(&[TypeCode::U128]),
            Some(set(&[0, b])),
            WorkerSet::ALL,
        ),
    ];
    for (case, schema, blob, want) in &cases {
        let r = route_read(schema, blob.as_deref(), NW);
        assert_eq!(&r.set, want, "{case}");
        assert!(r.per_worker.is_none(), "{case}: every worker is sent the whole bound");
    }

    let spread = route_read(&keyed, Some(&set(&[0, b])), NW);
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
