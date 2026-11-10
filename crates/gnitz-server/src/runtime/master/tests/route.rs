use super::*;
use crate::runtime::sal::fixtures::{group_at, scan, test_log};
use crate::runtime::sal::SalWriter;
use crate::test_support::{
    make_batch, make_batch_bytes_raw, make_batch_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64,
    pk_only_schema, pk_payload_schema, weighted_rows, zset_of,
};
use gnitz_store::relation::PkRule;
use gnitz_store::relation::{RelationKind, RelationSpec, StoreConfig};
use gnitz_wire::{KeyRange, PkColList, PkKeys, ReadBound, ReadSpec, TypeCode};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::encode_schema_block;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};
use std::num::NonZeroU64;

/// Write `batch` to `log` as the master's push of it to relation 16, placed by
/// `placement`, handing `inspect` the layout the scatter picked.
fn push(log: &SalWriter, batch: &Batch, placement: Placement, inspect: impl FnOnce(&Routed)) {
    let record = encode_schema_block(batch.schema());
    let mut excl = log.excl();
    let mut scope = excl.begin();
    with_routed(batch.as_mem_batch(), placement, log.num_workers(), |rows| {
        inspect(&rows);
        scope.push(16, PkRule::Enforce, &record, rows, false)
    })
    .expect("group fits");
    scope.commit();
}

/// A keyed push addresses exactly the workers that own its rows, each sent its
/// own; a replicated push sends every worker its live rows as one payload; and
/// only a payload with rows carries a schema block.
#[test]
fn a_push_group_decodes_to_each_workers_rows() {
    let nw = 4;
    let narrow = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I32, false),
        ],
        &[0],
    );
    let string = make_schema_pk_u64_payload_string();
    let long: Vec<String> = (0..8)
        .map(|pk| format!("a string past the inline prefix {pk}"))
        .collect();
    let fixed = make_schema_u64_i64();
    let keyed = |b: Batch| (Placement::full_pk(b.schema()), b);
    let replicated = |b: Batch| (Placement::Replicated, b);
    type LaidOut = fn(&Routed, &Batch) -> bool;
    let cases: [(&str, (Placement, Batch), LaidOut); 6] = [
        (
            "keyed single row: framed from the batch, its owner alone addressed",
            keyed(make_batch(&fixed, &[(1, 1, 10)])),
            |g, _| matches!(g, Routed::Each(_)),
        ),
        (
            "keyed fixed-width: framed from the batch",
            keyed(make_batch_raw(&narrow, &[(0, 1, 0), (1, 1, 1), (2, 1, 2)])),
            |g, _| matches!(g, Routed::Each(_)),
        ),
        (
            "keyed inline strings: framed from the batch",
            keyed(make_batch_bytes_raw(
                &string,
                &[(1, 1, b"a"), (2, 3, b"twelve bytes"), (3, 1, b"")],
            )),
            |g, _| matches!(g, Routed::Each(_)),
        ),
        (
            "keyed heap strings: framed from the batch, one entry per worker",
            keyed(make_batch_bytes_raw(
                &string,
                &long
                    .iter()
                    .enumerate()
                    .map(|(pk, s)| (pk as u64, 1, s.as_bytes()))
                    .collect::<Vec<_>>(),
            )),
            |g, _| matches!(g, Routed::Each(e) if e.len() == 4),
        ),
        (
            "replicated live rows: the batch itself",
            replicated(make_batch(&fixed, &[(1, 1, 10), (2, 2, 20), (3, -1, 30)])),
            |g, batch| matches!(g, Routed::Same(Some(d)) if d.rows() == batch.len()),
        ),
        (
            "replicated with a weight-0 row: one whole batch",
            replicated(make_batch_bytes_raw(
                &string,
                &[(1, 1, long[0].as_bytes()), (2, 0, b"dropped"), (3, 2, b"c")],
            )),
            |g, _| matches!(g, Routed::Same(Some(_))),
        ),
    ];

    for (case, (placement, batch), laid_out) in &cases {
        let schema = *batch.schema();
        let log = test_log(1 << 20, nw, 1);
        push(&log, batch, *placement, |g| assert!(laid_out(g, batch), "{case}"));
        let msg = group_at(log.log(), 0);
        let sent: Vec<(usize, Batch)> = (0..nw)
            .filter_map(|w| {
                let bytes = msg.slot(w as u32)?;
                let rows = msg.rows(bytes, |_, _| None).expect("every payload decodes");
                Some((
                    w,
                    rows.unwrap_or_else(|| panic!("{case}: worker {w} is addressed without rows")),
                ))
            })
            .collect();
        if placement.is_replicated() {
            let live = weighted_rows(batch)
                .into_iter()
                .filter(|&(_, w)| w != 0)
                .collect::<Vec<_>>();
            assert_eq!(sent.len(), nw, "{case}: every worker is sent the rows");
            assert_eq!(msg.payloads().count(), 1, "{case}: as one payload");
            assert!(
                sent.iter().all(|(_, s)| weighted_rows(s) == live),
                "{case}: every worker holds the live rows"
            );
        } else {
            let owners = (0..batch.len())
                .map(|row| placement.owner(batch.get_pk_bytes(row), nw).expect("a keyed placement"))
                .collect::<WorkerSet>();
            for w in 0..nw {
                assert_eq!(
                    msg.slot(w as u32).is_some(),
                    owners.contains(w),
                    "{case}: worker {w} is addressed iff it owns a row"
                );
            }
            assert_eq!(msg.payloads().count(), owners.len(), "{case}: one payload each");
            for (w, s) in &sent {
                for row in 0..s.len() {
                    assert_eq!(
                        placement.owner(s.get_pk_bytes(row), nw),
                        Some(*w),
                        "{case}: a row sent to worker {w}"
                    );
                }
            }
            let all = Batch::concat(&schema, sent.iter().map(|(_, s)| s.as_mem_batch()));
            assert_eq!(
                zset_of(&all, &schema),
                zset_of(batch, &schema),
                "{case}: the payloads sum to the pushed rows"
            );
        }
    }
}

/// A keyed push of rows that name one heap span — a join view's rows, pushed
/// back by a client — sends each worker that span once: no payload's heap
/// outgrows the pushed one.
#[test]
fn a_keyed_push_of_rows_sharing_a_span_keeps_each_heap_within_the_pushed_one() {
    let (nw, rows) = (4, 20u64);
    let schema = make_schema_pk_u64_payload_string();
    let heap = vec![b'v'; 200];
    let mut cell = gnitz_wire::encode_german_string(&heap, &mut Vec::new());
    gnitz_wire::write_u64_le(&mut cell, 8, 0);
    let pks: Vec<u8> = (0..rows).flat_map(u64::to_be_bytes).collect();
    let weights: Vec<u8> = (0..rows).flat_map(|_| 1i64.to_le_bytes()).collect();
    let nulls = vec![0u8; 8 * rows as usize];
    let cells = cell.repeat(rows as usize);
    let mut block = Vec::new();
    gnitz_wire::wal::append_block(&[&pks, &weights, &nulls, &cells, &heap], 0, &mut block);
    let batch = Batch::decode_foreign_wal_block(&block, &schema).expect("a canonical block");
    assert_eq!(batch.blob().len(), heap.len());

    let placement = Placement::full_pk(&schema);
    let log = test_log(1 << 20, nw, 1);
    push(&log, &batch, placement, |g| {
        assert!(matches!(g, Routed::Each(e) if e.len() == nw))
    });
    let msg = group_at(log.log(), 0);
    let sent: Vec<Batch> = (0..nw)
        .filter_map(|w| {
            let bytes = msg.slot(w as u32)?;
            msg.rows(bytes, |_, _| None).expect("every payload decodes")
        })
        .collect();
    assert!(
        sent.iter().any(|s| s.len() > 1),
        "precondition: a worker owns several rows"
    );
    for s in &sent {
        assert_eq!(
            s.blob().len(),
            heap.len(),
            "a worker's {} rows carry the span once",
            s.len()
        );
    }
    let all = Batch::concat(&schema, sent.iter().map(Batch::as_mem_batch));
    assert_eq!(zset_of(&all, &schema), zset_of(&batch, &schema));
}

const NW: usize = 4;

/// A registry holding one relation of `schema` placed by `placement`, id 16.
pub(super) fn registry(schema: SchemaDescriptor, placement: Placement) -> RelationRegistry {
    // A stream is storeless, so it registers without a directory.
    let mut registry = RelationRegistry::new("", gnitz_zset::algebra::Slot::SOLO, StoreConfig::default());
    let spec = RelationSpec {
        id: 16,
        kind: RelationKind::Stream,
        placement,
        pk_repeats: false,
        compress: false,
    };
    registry.register(spec, schema).unwrap();
    registry
}

/// `keys` of `stride`-byte big-endian ids, as a `PkSet` bound.
pub(super) fn pk_set(stride: usize, keys: &[u64]) -> ReadBound {
    ReadBound::PkSet(pk_keys(stride, keys))
}

fn pk_keys(stride: usize, keys: &[u64]) -> PkKeys {
    let keys: Vec<Vec<u8>> = keys.iter().map(|k| k.to_be_bytes()[8 - stride..].to_vec()).collect();
    PkKeys::from_keys(stride, keys.iter().map(Vec::as_slice))
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
            "Local, one key",
            schema,
            Placement::Local,
            Some(set(&[42])),
            WorkerSet::ALL,
        ),
        (
            "foreign stride",
            wide,
            Placement::full_pk(&wide),
            Some(set(&[0, b])),
            WorkerSet::ALL,
        ),
    ];
    for (case, schema, placement, blob, want) in cases {
        let registry = registry(schema, placement);
        let r = route_read(&scan(16, &blob.unwrap_or_default()), &registry, NW);
        assert_eq!(r.set, want, "{case}");
        assert!(r.per_worker.is_none(), "{case}: every worker is sent the whole bound");
    }

    let registry = registry(schema, keyed);
    let spread = route_read(&scan(16, &set(&[0, b])), &registry, NW);
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
    let keys = pk_keys(8, &[0, b]).encode_probe();
    let probe = |probe| Read::HasPk { tid: 16, probe, keys: (&keys[..]).into() };
    let index = Probe::Index(PkColList::from_slice(&[1]), NonZeroU64::MAX);

    let replicated = registry(schema, Placement::Replicated);
    for own in [Probe::Pk, Probe::PkColumn(1)] {
        let r = route_read(&probe(own), &replicated, NW);
        assert_eq!(r.set, WorkerSet::one(owner(0)).with(owner(b)), "{own:?}");
        assert!(r.per_worker.is_some(), "{own:?}: each worker is sent its own keys");
    }
    let one = pk_keys(8, &[b]).encode_probe();
    let r = route_read(
        &Read::HasPk {
            tid: 16,
            probe: Probe::Pk,
            keys: (&one[..]).into(),
        },
        &replicated,
        NW,
    );
    assert_eq!(
        (r.set, r.per_worker),
        (WorkerSet::one(owner(b)), None),
        "one key, its one owner"
    );
    let r = route_read(
        &scan(16, &ReadSpec::all_rows(pk_set(8, &[0, b])).encode()),
        &replicated,
        NW,
    );
    assert_eq!(r.set, WorkerSet::one(Placement::REPLICA_OWNER as usize));

    let keyed = registry(schema, full);
    let r = route_read(&probe(index), &keyed, NW);
    assert_eq!((r.set, r.per_worker), (WorkerSet::ALL, None));
}
