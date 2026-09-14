use super::*;
use crate::range::Cut::{After, Before};
use crate::TypeCode;
use crate::{AggFunc, MAX_COLUMNS};

/// A stand-in reply block: the codec carries it opaquely.
fn block() -> Vec<u8> {
    vec![9u8; 40]
}

/// The spec part of an encoded request: its bytes past the reply block.
fn spec_len(bytes: &[u8], block: &[u8]) -> usize {
    bytes.len() - 4 - block.len()
}

fn sample_order() -> Vec<OrderKey> {
    vec![
        OrderKey { col: 3, desc: false, nulls_first: true },
        OrderKey { col: 0, desc: true, nulls_first: false },
    ]
}

fn empty_spec() -> ReadSpec {
    ReadSpec::all_rows(ReadBound::None)
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
        ReadBound::PkRange(RangeDescriptor::new(&[], After(10), After(u64::MAX as u128))),
        ReadBound::IndexRange {
            bound: IndexBound {
                idx_cols: crate::PkColList::from_slice(&[1, 2]),
                desc: RangeDescriptor::new(&[7], Before(1), Before(u128::MAX)),
            },
            walk: IndexWalk::Optional,
        },
        // The required form: the client stripped the bounded conjuncts, so the
        // flag must survive the round trip or the walk becomes tradeable.
        ReadBound::IndexRange {
            bound: IndexBound {
                idx_cols: crate::PkColList::from_slice(&[3]),
                desc: RangeDescriptor::point(&[], u128::MAX),
            },
            walk: IndexWalk::Required,
        },
        ReadBound::PkSet(keys(8)),
        // A compound key wider than any scalar.
        ReadBound::PkSet(keys(24)),
    ];
    let maps = [
        None,
        Some(ComputeMap {
            program: vec![4, 1, 5, 9, 2, 6],
            out_cols: vec![
                (TypeCode::I64 as u8, false),
                (TypeCode::I64 as u8, true),
                (TypeCode::F64 as u8, true),
            ],
        }),
        // A keys-only map: a program that declares no slot.
        Some(ComputeMap { program: vec![7], out_cols: vec![] }),
    ];
    let kinds = [
        SinkKind::Rows { order: vec![], limit_k: 0 },
        SinkKind::Rows { order: sample_order(), limit_k: 100 },
        // Every aggregate the enum names, at a distinct source column.
        SinkKind::Fold(AggReadSpec {
            group_cols: vec![0, 3],
            aggs: AggFunc::ALL
                .iter()
                .enumerate()
                .map(|(i, &agg_op)| AggDescriptor { agg_op, col_idx: i as u32 })
                .collect(),
        }),
        // DISTINCT: group cols, no aggs.
        SinkKind::Fold(AggReadSpec { group_cols: vec![1, 2, 5], aggs: vec![] }),
        // A global aggregate: no group cols.
        SinkKind::Fold(AggReadSpec {
            group_cols: vec![],
            aggs: vec![AggDescriptor { agg_op: AggFunc::Min, col_idx: 4 }],
        }),
    ];
    let block = block();
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
                let bytes = spec.encode(&block);
                assert!(spec_len(&bytes, &block) <= MAX_READ_SPEC_BYTES);
                assert_eq!(ReadSpec::decode(&bytes), Ok((spec.clone(), &block[..])), "{spec:?}");
            }
        }
    }
}

/// `from_keys` is the one sort: duplicates collapse and the list comes out
/// ascending whatever order it went in, and `per_request` splits it into
/// consecutive lists that concatenate back to it.
#[test]
fn pk_keys_sort_dedup_and_split() {
    let raw: Vec<[u8; 2]> = vec![[0, 9], [0, 1], [0, 9], [1, 0]];
    let k = PkKeys::from_keys(2, raw.iter().map(|k| &k[..]));
    assert_eq!(k.iter().collect::<Vec<_>>(), vec![&[0, 1][..], &[0, 9], &[1, 0]]);
    assert_eq!(k.len(), 3);

    // Per-request caps: every scalar-wide stride takes the key cap, and a wide
    // one the byte budget of that many 16-byte keys.
    assert_eq!(PkKeys::max_per_request(8), MAX_PK_SET_KEYS);
    assert_eq!(PkKeys::max_per_request(16), MAX_PK_SET_KEYS);
    assert_eq!(PkKeys::max_per_request(32), MAX_PK_SET_KEYS / 2);

    let many: Vec<[u8; 4]> = (0..(MAX_PK_SET_KEYS as u32 + 5)).map(u32::to_be_bytes).collect();
    let big = PkKeys::from_keys(4, many.iter().map(|k| &k[..]));
    let parts: Vec<PkKeys> = big.per_request().collect();
    assert_eq!(
        parts.iter().map(PkKeys::len).collect::<Vec<_>>(),
        vec![MAX_PK_SET_KEYS, 5]
    );
    let rejoined: Vec<u8> = parts.iter().flat_map(|p| p.as_bytes().to_vec()).collect();
    assert_eq!(rejoined, big.as_bytes());
}

/// A widest-stride list at its per-request cap still encodes under the spec's
/// byte ceiling — the reason the cap is a byte budget rather than a key count.
#[test]
fn a_wide_pk_set_at_its_cap_fits_the_spec_ceiling() {
    let stride = crate::MAX_PK_BYTES;
    let n = PkKeys::max_per_request(stride);
    let raw: Vec<Vec<u8>> = (0..n as u32)
        .map(|i| {
            let mut k = vec![0u8; stride];
            k[stride - 4..].copy_from_slice(&i.to_be_bytes());
            k
        })
        .collect();
    let spec = ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_keys(
        stride,
        raw.iter().map(Vec::as_slice),
    )));
    let block = block();
    let bytes = spec.encode(&block);
    assert!(spec_len(&bytes, &block) <= MAX_READ_SPEC_BYTES, "{} bytes", bytes.len());
    assert_eq!(ReadSpec::decode(&bytes), Ok((spec, &block[..])));
}

/// The watermark's two halves survive the round trip independently — the
/// server packs it, the client unpacks it, and nothing else binds them.
#[test]
fn delta_watermark_roundtrips_both_halves() {
    for &(tag, tick) in &[(0u64, 0u64), (1, 2), (u64::MAX, 0), (0, u64::MAX), (u64::MAX, u64::MAX)] {
        assert_eq!(unpack_delta_watermark(pack_delta_watermark(tag, tick)), (tag, tick));
    }
}

/// Empty reply block, no bound, empty predicate, absent map and the fold tag of
/// a hand-built fold-sink blob.
fn fold_header() -> Vec<u8> {
    let mut bytes = 0u32.to_le_bytes().to_vec(); // reply block len
    bytes.push(BOUND_NONE);
    bytes.extend_from_slice(&0u32.to_le_bytes()); // predicate len
    bytes.push(0); // no map
    bytes.push(SINK_FOLD);
    bytes
}

/// A hand-built `PkSet` bound after an empty reply block: stride, count, then
/// `keys` verbatim.
fn pk_set_blob(stride: u8, count: u32, keys: &[u8]) -> Vec<u8> {
    let mut bytes = 0u32.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[BOUND_PK_SET, stride]);
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(keys);
    bytes
}

/// Every decode guard, against the forgery that trips it. The substring is what
/// separates the guards: a merged table that only asserted "an error" would pass
/// on the wrong one.
#[test]
fn each_decode_guard_rejects_its_own_forgery() {
    // Layout of the empty Rows spec behind an empty reply block: block len
    // 0..4 | bound tag 4 | predicate len 5..9 | map presence 9 | sink tag 10 |
    // limit_k 11..19 | order key count 19..21.
    let poke = |off: usize, val: u8| {
        let mut bytes = empty_spec().encode(&[]);
        bytes[off] = val;
        bytes
    };
    // The first byte naming no aggregate — not a literal, which the next
    // aggregate added to the enum would quietly turn into a valid opcode.
    let bad_agg = (0u8..=u8::MAX)
        .find(|&b| AggFunc::from_wire(b).is_none())
        .expect("some byte names no aggregate");

    // The count is read before any per-key bytes, so no keys are needed.
    let pk_set_over_cap = pk_set_blob(8, (MAX_PK_SET_KEYS + 1) as u32, &[]);
    let wide_over_cap = pk_set_blob(32, (MAX_PK_SET_KEYS / 2 + 1) as u32, &[]);
    let zero_stride = pk_set_blob(0, 0, &[]);
    let over_wide_stride = pk_set_blob((crate::MAX_PK_BYTES + 1) as u8, 0, &[]);
    let unsorted = pk_set_blob(1, 2, &[5, 3]);
    let duplicate = pk_set_blob(1, 2, &[5, 5]);
    let trailing = {
        let mut bytes = empty_spec().encode(&[]);
        bytes.push(0);
        bytes
    };
    let truncated = {
        // Chop the last PkSet key's bytes off.
        let bytes = ReadSpec::all_rows(ReadBound::PkSet(keys(8))).encode(&[]);
        bytes[..bytes.len() - 4].to_vec()
    };
    let oversized = {
        let mut bytes = 0u32.to_le_bytes().to_vec();
        bytes.resize(4 + MAX_READ_SPEC_BYTES + 1, 0);
        bytes
    };
    let bad_agg_op = {
        let mut bytes = fold_header();
        bytes.extend_from_slice(&0u16.to_le_bytes()); // n_group_cols = 0
        bytes.extend_from_slice(&1u16.to_le_bytes()); // n_aggs = 1
        bytes.push(bad_agg);
        bytes.extend_from_slice(&0u32.to_le_bytes()); // src_col
        bytes
    };
    let group_cols_over_cap = {
        let mut bytes = fold_header();
        bytes.extend_from_slice(&((MAX_COLUMNS + 1) as u16).to_le_bytes());
        bytes
    };
    let aggs_over_cap = {
        let mut bytes = fold_header();
        bytes.extend_from_slice(&0u16.to_le_bytes()); // n_group_cols = 0
        bytes.extend_from_slice(&((MAX_COLUMNS + 1) as u16).to_le_bytes());
        bytes
    };

    let cases: &[(&str, Vec<u8>, &str)] = &[
        ("bound kind", poke(4, 9), "bound kind"),
        ("sink tag", poke(10, 9), "sink tag"),
        ("map presence", poke(9, 2), "map presence byte"),
        (
            "order key cap",
            poke(19, (MAX_ORDER_KEYS + 1) as u8),
            "order keys: 17 exceeds cap",
        ),
        ("PkSet cap", pk_set_over_cap, "PkSet count"),
        ("wide PkSet cap", wide_over_cap, "PkSet count"),
        ("zero stride", zero_stride, "PkSet stride"),
        ("over-wide stride", over_wide_stride, "PkSet stride"),
        ("unsorted keys", unsorted, "strictly ascending"),
        ("duplicate keys", duplicate, "strictly ascending"),
        ("trailing bytes", trailing, "trailing"),
        ("oversized blob", oversized, "exceeds cap"),
        ("unknown aggregate", bad_agg_op, "unknown agg func id"),
        (
            "group col cap",
            group_cols_over_cap,
            "column list: 66 entries exceeds cap",
        ),
        ("agg item cap", aggs_over_cap, "aggregate list: 66 entries exceeds cap"),
    ];
    for (name, bytes, want) in cases {
        let err = ReadSpec::decode(bytes).expect_err(name);
        assert!(err.contains(want), "{name}: {err:?} does not name {want:?}");
    }
    // Truncation trips the reader itself, which words its own message.
    assert!(ReadSpec::decode(&truncated).is_err(), "truncated PkSet");
}

/// Routing reads a `PkRange`'s descriptor and a `PkSet`'s keys in place, and
/// declines every other bound and any truncated prefix.
#[test]
fn peek_bound_reads_only_the_routing_bounds() {
    let block = block();
    let with = |bound: ReadBound| {
        ReadSpec {
            bound,
            predicate: vec![1, 2],
            sink: ReadSink::all_rows(),
        }
        .encode(&block)
    };

    let desc = RangeDescriptor::new(&[4], After(1), Before(9));
    match peek_bound(&with(ReadBound::PkRange(desc))) {
        Some(BoundPeek::PkRange(got)) => assert_eq!(got, desc),
        _ => panic!("a PkRange peeks as its descriptor"),
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
    let index = ReadBound::IndexRange {
        bound: IndexBound {
            idx_cols: crate::PkColList::from_slice(&[1]),
            desc: RangeDescriptor::point(&[], 3),
        },
        walk: IndexWalk::Required,
    };
    assert!(peek_bound(&with(index)).is_none());
    // Cut inside the key list: the count names bytes the blob does not hold.
    let key_end = 4 + block.len() + 1 + 1 + 4 + set.as_bytes().len();
    assert!(peek_bound(&blob[..key_end - 1]).is_none());
    assert!(peek_bound(&[]).is_none());
}

/// Re-keying a blob keeps every other section byte-for-byte: it decodes to the
/// same spec over the subsequence, the empty one included.
#[test]
fn a_pk_set_with_fewer_keys_decodes_to_the_same_spec() {
    let block = block();
    let maps = [
        None,
        Some(ComputeMap {
            program: vec![4, 1, 5],
            out_cols: vec![(TypeCode::I64 as u8, true)],
        }),
    ];
    let kinds = [
        SinkKind::Rows { order: sample_order(), limit_k: 7 },
        SinkKind::Fold(AggReadSpec {
            group_cols: vec![0],
            aggs: vec![AggDescriptor { agg_op: AggFunc::Sum, col_idx: 1 }],
        }),
    ];
    for stride in [8usize, 24] {
        let set = keys(stride);
        let all: Vec<&[u8]> = set.iter().collect();
        let subsequences: [Vec<&[u8]>; 4] = [vec![], vec![all[0]], vec![all[0], all[2]], all.clone()];
        for map in &maps {
            for kind in &kinds {
                let spec = ReadSpec {
                    bound: ReadBound::PkSet(set.clone()),
                    predicate: vec![1, 2, 3],
                    sink: ReadSink { map: map.clone(), kind: kind.clone() },
                };
                let blob = spec.encode(&block);
                let Some(BoundPeek::PkSet(peek)) = peek_bound(&blob) else {
                    panic!("a PkSet peeks as its keys");
                };
                for sub in &subsequences {
                    let want = ReadSpec {
                        bound: ReadBound::PkSet(PkKeys::from_keys(stride, sub.iter().copied())),
                        ..spec.clone()
                    };
                    let rekeyed = peek.with_keys(&sub.concat());
                    assert_eq!(ReadSpec::decode(&rekeyed), Ok((want, &block[..])), "{sub:?}");
                }
            }
        }
    }
}
