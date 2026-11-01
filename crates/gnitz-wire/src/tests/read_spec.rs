use std::num::NonZeroU64;

use super::*;
use crate::{AggDescriptor, AggFunc, PkColList, TypeCode, MAX_COLUMNS};

fn sample_order() -> Vec<OrderKey> {
    vec![
        OrderKey { col: 3, desc: false, nulls_first: true },
        OrderKey { col: 0, desc: true, nulls_first: false },
    ]
}

fn empty_spec() -> ReadSpec {
    ReadSpec::all_rows(ReadBound::None)
}

/// A rows sink's cut at `k > 0`.
fn cut(k: u64, order: Vec<OrderKey>) -> Option<RowsCut> {
    Some(RowsCut { k: NonZeroU64::new(k).unwrap(), order })
}

/// Three OPK keys of `stride`, deliberately handed over out of order.
fn keys(stride: usize) -> PkKeys {
    let ks: Vec<Vec<u8>> = [9u8, 3, 200].iter().map(|&b| vec![b; stride]).collect();
    PkKeys::from_keys(stride, ks.iter().map(Vec::as_slice))
}

/// Bound, map and sink kind are independent axes of the encoding, so every
/// combination round-trips, the fold kind over every `AggFunc`.
#[test]
fn roundtrips_every_bound_against_every_sink() {
    let bounds = [
        ReadBound::None,
        // KeyRange shapes are `range`'s; one stands for them here.
        ReadBound::Range(KeyRange::new(PkColList::from_slice(&[1, 2]), &[7], 1, u128::MAX - 1)),
        ReadBound::PkSet(keys(8)),
        ReadBound::PkSet(keys(crate::MAX_PK_BYTES)),
    ];
    let maps = [
        None,
        Some(ComputeMap {
            program: vec![4, 1, 5, 9, 2, 6],
            out_cols: vec![(TypeCode::I64, false), (TypeCode::I64, true), (TypeCode::F64, true)],
        }),
        // A keys-only map: a program that declares no slot.
        Some(ComputeMap { program: vec![7], out_cols: vec![] }),
    ];
    let kinds = [
        SinkKind::Rows { cut: None },
        SinkKind::Rows { cut: cut(100, vec![]) },
        SinkKind::Rows { cut: cut(100, sample_order()) },
        // Every aggregate the enum names, at a distinct source column.
        SinkKind::Fold(GroupAggs {
            group_cols: vec![0, 3],
            aggs: AggFunc::ALL
                .iter()
                .enumerate()
                .map(|(i, &agg_op)| AggDescriptor { agg_op, col_idx: i as u32 })
                .collect(),
        }),
        // DISTINCT: group cols, no aggs.
        SinkKind::Fold(GroupAggs { group_cols: vec![1, 2, 5], aggs: vec![] }),
        // A global aggregate: no group cols.
        SinkKind::Fold(GroupAggs {
            group_cols: vec![],
            aggs: vec![AggDescriptor { agg_op: AggFunc::Min, col_idx: 4 }],
        }),
    ];
    for (i, bound) in bounds.iter().enumerate() {
        for map in &maps {
            for kind in &kinds {
                // Rotate the predicate so both the empty and the non-empty section
                // length are exercised against every bound.
                let spec = ReadSpec {
                    bound: bound.clone(),
                    predicate: if i % 2 == 0 { vec![1, 2, 3, 4] } else { vec![] },
                    sink: ReadSink { map: map.clone(), kind: kind.clone() },
                };
                let bytes = spec.encode();
                assert_eq!(ReadSpec::decode(&bytes), Ok(spec.clone()), "{spec:?}");
            }
        }
    }
}

/// The request blob's bytes, one spec per sink kind. A change to them moves
/// `WIRE_EPOCH`.
#[test]
fn the_encoded_bytes_are_pinned() {
    let rows = ReadSpec {
        bound: ReadBound::None,
        predicate: vec![1, 2],
        sink: ReadSink {
            map: None,
            kind: SinkKind::Rows {
                cut: cut(5, vec![OrderKey { col: 3, desc: true, nulls_first: true }]),
            },
        },
    };
    #[rustfmt::skip]
    let want = [
        0,                      // bound: none
        2, 0, 0, 0, 1, 2,       // predicate
        0,                      // no map
        0,                      // sink: rows
        5, 0, 0, 0, 0, 0, 0, 0, // cut k
        1, 0,                   // order keys
        3, 0, 0b11,             // column, desc | nulls first
    ];
    assert_eq!(rows.encode(), want);

    let fold = ReadSpec {
        bound: ReadBound::PkSet(PkKeys::from_sorted(1, vec![4, 9])),
        predicate: vec![],
        sink: ReadSink {
            map: Some(ComputeMap {
                program: vec![7],
                out_cols: vec![(TypeCode::I64, true)],
            }),
            kind: SinkKind::Fold(GroupAggs {
                group_cols: vec![2],
                aggs: vec![AggDescriptor { agg_op: AggFunc::Min, col_idx: 4 }],
            }),
        },
    };
    #[rustfmt::skip]
    let want = [
        2, 1, 2, 0, 0, 0, 4, 9, // bound: PK set, stride, count, keys
        0, 0, 0, 0,             // predicate
        1,                      // a map
        1, 0, 9, 1,             // one output slot: I64, nullable
        1, 0, 0, 0, 7,          // program
        1,                      // sink: fold
        1, 0, 2, 0, 0, 0,       // group columns
        1, 0, 3, 4, 0, 0, 0,    // aggregates: MIN over column 4
    ];
    assert_eq!(fold.encode(), want);
}

/// A probe's keys decode to themselves and route as a `PkSet` bound; trailing
/// bytes, an empty blob and any other bound are refused.
#[test]
fn a_pk_probe_round_trips() {
    let keys = keys(8);
    let blob = keys.encode_probe();
    let mut bound = Writer::new();
    bound.put(&ReadBound::PkSet(keys.clone()));
    assert_eq!(blob, bound.into_vec(), "a probe is a PkSet bound's bytes");
    assert_eq!(PkKeys::decode_probe(&blob), Ok(keys));
    assert!(PkKeys::decode_probe(&[blob, vec![0]].concat()).is_err());
    assert!(PkKeys::decode_probe(&[]).is_err());
    assert!(PkKeys::decode_probe(&[BoundTag::None.as_wire()])
        .unwrap_err()
        .contains("PkSet"));
}

/// A list longer than any 16-bit count round-trips whole: the frame is the only
/// bound on a key list.
#[test]
fn a_long_pk_set_round_trips_whole() {
    let n = (1u32 << 16) + 1;
    let bytes: Vec<u8> = (0..n).flat_map(u32::to_be_bytes).collect();
    let spec = ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_sorted(4, bytes)));
    assert_eq!(ReadSpec::decode(&spec.encode()), Ok(spec));
}

/// A hand-built `PkSet` bound: stride, count, then `keys` verbatim.
fn pk_set_blob(stride: u8, count: u32, keys: &[u8]) -> Vec<u8> {
    let mut bytes = vec![BoundTag::PkSet.as_wire(), stride];
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(keys);
    bytes
}

/// Every decode guard, against the forgery that trips it. The substring is what
/// separates the guards: a merged table that only asserted "an error" would pass
/// on the wrong one.
#[test]
fn each_decode_guard_rejects_its_own_forgery() {
    // Layout of the empty Rows spec: bound tag 0 | predicate len 1..5 | map
    // presence 5 | sink tag 6 | cut k 7..15.
    let poke = |off: usize, val: u8| {
        let mut bytes = empty_spec().encode();
        bytes[off] = val;
        bytes
    };
    let fold = |aggs: Vec<AggDescriptor>| ReadSpec {
        sink: ReadSink {
            map: None,
            kind: SinkKind::Fold(GroupAggs { group_cols: vec![], aggs }),
        },
        ..empty_spec()
    };
    let min = AggDescriptor { agg_op: AggFunc::Min, col_idx: 0 };
    let bad_agg = crate::outside(AggFunc::from_wire);
    let bad_agg_op = {
        // The last aggregate's op byte, then its u32 column.
        let mut bytes = fold(vec![min]).encode();
        let at = bytes.len() - 5;
        bytes[at] = bad_agg;
        bytes
    };
    // No cut, then the order list only a cut carries.
    let mut order_without_cut = empty_spec().encode();
    order_without_cut.extend([1, 0, 3, 0, 0]);
    let mut trailing = empty_spec().encode();
    trailing.push(0);

    let cases: &[(&str, Vec<u8>, &str)] = &[
        (
            "bound tag",
            poke(0, crate::outside(BoundTag::from_wire)),
            "unknown BoundTag",
        ),
        (
            "sink tag",
            poke(6, crate::outside(SinkTag::from_wire)),
            "unknown SinkTag",
        ),
        ("map presence", poke(5, 2), "boolean byte 2"),
        ("zero stride", pk_set_blob(0, 0, &[]), "PkSet stride"),
        (
            "over-wide stride",
            pk_set_blob((crate::MAX_PK_BYTES + 1) as u8, 0, &[]),
            "PkSet stride",
        ),
        ("unsorted keys", pk_set_blob(1, 2, &[5, 3]), "strictly ascending"),
        ("duplicate keys", pk_set_blob(1, 2, &[5, 5]), "strictly ascending"),
        ("count past the cell", pk_set_blob(1, 3, &[1, 2]), "truncated"),
        ("trailing bytes", trailing, "trailing"),
        ("order key without a cut", order_without_cut, "trailing"),
        ("unknown aggregate", bad_agg_op, "unknown AggFunc"),
        (
            "aggregate cap",
            fold(vec![min; MAX_COLUMNS + 1]).encode(),
            "aggregate list",
        ),
    ];
    for (name, bytes, want) in cases {
        let err = ReadSpec::decode(bytes).expect_err(name);
        assert!(err.contains(want), "{name}: {err:?} does not name {want:?}");
    }

    // A spec cut anywhere is refused, and the format labels the error once.
    let rich = ReadSpec {
        bound: ReadBound::PkSet(keys(8)),
        predicate: vec![1, 2, 3],
        sink: ReadSink {
            map: Some(ComputeMap {
                program: vec![4, 1],
                out_cols: vec![(TypeCode::I64, true)],
            }),
            kind: SinkKind::Fold(GroupAggs { group_cols: vec![0], aggs: vec![min] }),
        },
    }
    .encode();
    for cut in 0..rich.len() {
        let err = ReadSpec::decode(&rich[..cut]).expect_err("a cut spec");
        assert_eq!(err.matches("read_spec").count(), 1, "cut at {cut}: {err:?}");
    }
}

/// Routing reads a `Range` and a `PkSet`'s keys in place, and declines every other
/// bound and any truncated prefix.
#[test]
fn peek_bound_reads_only_the_routing_bounds() {
    let with = |bound: ReadBound| {
        ReadSpec {
            bound,
            predicate: vec![1, 2],
            sink: ReadSink::all_rows(),
        }
        .encode()
    };

    let range = KeyRange::new(PkColList::from_slice(&[0, 1]), &[4], 2, 8);
    match peek_bound(&with(ReadBound::Range(range))) {
        Some(BoundPeek::Range(got)) => assert_eq!(got, range),
        _ => panic!("a Range peeks as itself"),
    }

    let set = keys(8);
    let blob = with(ReadBound::PkSet(set.clone()));
    match peek_bound(&blob) {
        Some(BoundPeek::PkSet(got)) => {
            assert_eq!(got.stride, 8);
            assert_eq!(got.keys, set.as_bytes());
        }
        _ => panic!("a PkSet peeks as its keys"),
    }

    assert!(peek_bound(&with(ReadBound::None)).is_none());
    // Cut inside the key list: the count names bytes the blob does not hold.
    let key_end = 1 + 1 + 4 + set.as_bytes().len();
    assert!(peek_bound(&blob[..key_end - 1]).is_none());
    assert!(peek_bound(&[]).is_none());
}

/// Splitting a blob by key owner keeps every other section byte-for-byte: each
/// worker's blob decodes to the same spec over the keys it owns, in ascending
/// order, a worker owning none gets nothing, and together they carry every key.
#[test]
fn a_pk_set_split_by_owner_decodes_to_the_same_spec_per_worker() {
    for stride in [8usize, 24] {
        let set = keys(stride);
        let spec = ReadSpec {
            bound: ReadBound::PkSet(set.clone()),
            predicate: vec![1, 2, 3],
            sink: ReadSink {
                map: Some(ComputeMap {
                    program: vec![4, 1, 5],
                    out_cols: vec![(TypeCode::I64, true)],
                }),
                kind: SinkKind::Rows { cut: cut(7, sample_order()) },
            },
        };
        let blob = spec.encode();
        let Some(BoundPeek::PkSet(peek)) = peek_bound(&blob) else {
            panic!("a PkSet peeks as its keys");
        };
        // The keys ascend as 3, 9, 200; each table names the owner of each in turn.
        for owners in [[0usize, 0, 0], [0, 2, 0], [2, 1, 0], [1, 1, 2]] {
            let owner = |k: &[u8]| match k[0] {
                3 => owners[0],
                9 => owners[1],
                _ => owners[2],
            };
            let column: Vec<u8> = set.iter().map(|k| owner(k) as u8).collect();
            let blobs = peek.split(&column, 3);
            assert_eq!(blobs.len(), 3);
            let mut union: Vec<&[u8]> = Vec::new();
            for (w, got) in blobs.iter().enumerate() {
                let mine: Vec<&[u8]> = set.iter().filter(|k| owner(k) == w).collect();
                if mine.is_empty() {
                    assert!(got.is_empty(), "{owners:?} worker {w}");
                    continue;
                }
                let want = ReadSpec {
                    bound: ReadBound::PkSet(PkKeys::from_keys(stride, mine.iter().copied())),
                    ..spec.clone()
                };
                // Decoding a PkSet refuses keys out of order.
                assert_eq!(ReadSpec::decode(got), Ok(want), "{owners:?} worker {w}");
                union.extend(mine);
            }
            union.sort();
            assert_eq!(union, set.iter().collect::<Vec<_>>(), "{owners:?}");
        }
    }
}
