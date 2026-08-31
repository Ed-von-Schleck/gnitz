use super::*;
use crate::range::Cut::{After, Before};
use crate::TypeCode;

/// Encode through the production entry point; the struct is a decode-side
/// shape, so nothing but tests ever holds one to re-encode.
fn enc(spec: &ReadSpec) -> Vec<u8> {
    ReadSpec::encode_parts(&spec.bound, &spec.predicate, &spec.sink)
}

fn sample_order() -> Vec<OrderKey> {
    vec![
        OrderKey {
            col: 3,
            desc: false,
            nulls_first: true,
        },
        OrderKey {
            col: 0,
            desc: true,
            nulls_first: false,
        },
    ]
}

fn empty_spec() -> ReadSpec {
    ReadSpec {
        bound: ReadBound::None,
        predicate: vec![],
        sink: ReadSink::all_rows(),
    }
}

/// Bound and sink are independent axes of the encoding, so every pair of the
/// two tables round-trips. The fold sink is driven by `AggFunc::ALL`: an
/// aggregate added to the enum is round-tripped without a second edit here.
#[test]
fn roundtrips_every_bound_against_every_sink() {
    let bounds = [
        ReadBound::None,
        ReadBound::PkRange(RangeDescriptor::new(&[], After(10), After(u64::MAX as u128))),
        ReadBound::IndexRange {
            idx_cols: 0x0000_0002_0000_0001,
            exact: false,
            desc: RangeDescriptor::new(&[7], Before(1), Before(u128::MAX)),
        },
        // The exact form: the client stripped the bounded conjuncts, so the
        // flag must survive the round trip or the walk becomes tradeable.
        ReadBound::IndexRange {
            idx_cols: 0x0000_0000_0000_0003,
            exact: true,
            desc: RangeDescriptor::point(&[], u128::MAX),
        },
        ReadBound::PkSet(vec![5, 10, 3, 99]),
        // The bootstrap sentinel and a saturated cursor, both of which the
        // router must see as a delta bound rather than as a range.
        ReadBound::Delta { after_tick: 0 },
        ReadBound::Delta { after_tick: u64::MAX },
    ];
    let sinks = [
        ReadSink::all_rows(),
        ReadSink::Rows {
            projection: vec![9, 9, 9],
            order: vec![],
            limit_k: 0,
        },
        ReadSink::Rows {
            projection: vec![6],
            order: sample_order(),
            limit_k: 100,
        },
        // Every aggregate the enum names, at a distinct source column.
        ReadSink::Fold(AggReadSpec {
            group_cols: vec![0, 3],
            aggs: AggFunc::ALL
                .iter()
                .enumerate()
                .map(|(i, &op)| AggReadItem { op, src_col: i as u16 })
                .collect(),
            pre_map: vec![],
            pre_payload: vec![],
        }),
        // DISTINCT: group cols, no aggs.
        ReadSink::Fold(AggReadSpec {
            group_cols: vec![1, 2, 5],
            aggs: vec![],
            pre_map: vec![],
            pre_payload: vec![],
        }),
        // A global aggregate: no group cols.
        ReadSink::Fold(AggReadSpec {
            group_cols: vec![],
            aggs: vec![AggReadItem {
                op: AggFunc::Min,
                src_col: 4,
            }],
            pre_map: vec![],
            pre_payload: vec![],
        }),
        // A pre-map fold (`GROUP BY a + b`, `SUM(a * b)`): the program plus the
        // reduce input's payload declarations, both non-empty.
        ReadSink::Fold(AggReadSpec {
            group_cols: vec![7],
            aggs: vec![AggReadItem {
                op: AggFunc::Sum,
                src_col: 8,
            }],
            pre_map: vec![4, 1, 5, 9, 2, 6],
            pre_payload: vec![
                (TypeCode::I64 as u8, false),
                (TypeCode::I64 as u8, true),
                (TypeCode::F64 as u8, true),
            ],
        }),
    ];
    for (i, bound) in bounds.iter().enumerate() {
        for sink in &sinks {
            // Rotate the predicate so both the empty and the non-empty section
            // length are exercised against every bound.
            let spec = ReadSpec {
                bound: bound.clone(),
                predicate: if i % 2 == 0 { vec![1, 2, 3, 4] } else { vec![] },
                sink: sink.clone(),
            };
            let bytes = enc(&spec);
            assert!(bytes.len() <= MAX_READ_SPEC_BYTES);
            assert_eq!(ReadSpec::decode(&bytes), Ok(spec.clone()), "{spec:?}");
        }
    }
}

/// The decoder does not police `PkSet` contents — neither order nor duplicates.
/// `pk IN (-1, 5)` on a signed PK widens to `[0xFFFF…FF, 5]`, typed-increasing
/// but raw-`u128`-decreasing; and rejecting duplicates needs the PK width, which
/// this decoder does not have (the worker's OPK-sorted gather rejects them).
#[test]
fn the_decoder_does_not_police_pk_set_contents() {
    for keys in [vec![u64::MAX as u128, 5], vec![5, 5]] {
        let spec = ReadSpec {
            bound: ReadBound::PkSet(keys),
            predicate: vec![],
            sink: ReadSink::all_rows(),
        };
        assert_eq!(ReadSpec::decode(&enc(&spec)), Ok(spec));
    }
}

/// The delta bound must peek as itself and NOT as a `PkRange`: the master
/// routes off `peek_pk_range`, and a delta read misread as a range would be
/// hashed against the view's schema and unicast to one worker.
#[test]
fn delta_bound_peeks_as_delta_and_never_as_a_range() {
    for tick in [0u64, 1, 42, u64::MAX] {
        let bytes = enc(&ReadSpec {
            bound: ReadBound::Delta { after_tick: tick },
            predicate: vec![],
            sink: ReadSink::all_rows(),
        });
        assert_eq!(peek_delta_bound(SpecBytes(&bytes)), Some(tick));
        assert_eq!(peek_pk_range(SpecBytes(&bytes)), None);
    }
    // And every other bound peeks as no delta.
    let range = enc(&ReadSpec {
        bound: ReadBound::PkRange(RangeDescriptor::new(&[], After(1), After(9))),
        predicate: vec![],
        sink: ReadSink::all_rows(),
    });
    assert!(peek_pk_range(SpecBytes(&range)).is_some());
    assert_eq!(peek_delta_bound(SpecBytes(&range)), None);
    assert_eq!(peek_delta_bound(SpecBytes(&enc(&empty_spec()))), None);
}

/// The watermark's two halves survive the round trip independently — the
/// server packs it, the client unpacks it, and nothing else binds them.
#[test]
fn delta_watermark_roundtrips_both_halves() {
    for &(tag, tick) in &[(0u64, 0u64), (1, 2), (u64::MAX, 0), (0, u64::MAX), (u64::MAX, u64::MAX)] {
        assert_eq!(unpack_delta_watermark(pack_delta_watermark(tag, tick)), (tag, tick));
    }
}

/// Header + empty predicate of a hand-built fold-sink blob.
fn fold_header() -> Vec<u8> {
    let mut bytes = vec![VERSION, BOUND_NONE, SINK_FOLD, 0];
    bytes.extend_from_slice(&0u32.to_le_bytes()); // predicate len
    bytes
}

/// Every decode guard, against the forgery that trips it. The substring is what
/// separates the guards: a merged table that only asserted "an error" would pass
/// on the wrong one.
#[test]
fn each_decode_guard_rejects_its_own_forgery() {
    // Layout of the empty Rows spec: version | bound kind | sink tag |
    // reserved | u32 predicate len | n_order at offset 8.
    let poke = |off: usize, val: u8| {
        let mut bytes = enc(&empty_spec());
        bytes[off] = val;
        bytes
    };
    // The first byte naming no aggregate — not a literal, which the next
    // aggregate added to the enum would quietly turn into a valid opcode.
    let bad_agg = (0u8..=u8::MAX)
        .find(|&b| AggFunc::from_wire(b as u64).is_none())
        .expect("some byte names no aggregate");

    let pk_set_over_cap = {
        // The count is read before any per-key bytes, so no keys are needed.
        let mut bytes = vec![VERSION, BOUND_PK_SET, SINK_ROWS, 0];
        bytes.extend_from_slice(&((MAX_PK_SET_KEYS + 1) as u32).to_le_bytes());
        bytes
    };
    let trailing = {
        let mut bytes = enc(&empty_spec());
        bytes.push(0);
        bytes
    };
    let truncated = {
        // Chop the last PkSet key's bytes off.
        let bytes = enc(&ReadSpec {
            bound: ReadBound::PkSet(vec![1, 2, 3]),
            predicate: vec![],
            sink: ReadSink::all_rows(),
        });
        bytes[..bytes.len() - 4].to_vec()
    };
    let bad_agg_op = {
        let mut bytes = fold_header();
        bytes.extend_from_slice(&0u16.to_le_bytes()); // n_group_cols = 0
        bytes.push(1); // n_aggs = 1
        bytes.push(bad_agg);
        bytes.extend_from_slice(&0u16.to_le_bytes()); // src_col
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
        bytes.push((MAX_COLUMNS + 1) as u8);
        bytes
    };

    let cases: &[(&str, Vec<u8>, &str)] = &[
        ("version", poke(0, 99), "version"),
        ("bound kind", poke(1, 9), "bound kind"),
        ("sink tag", poke(2, 9), "sink tag"),
        (
            "order key cap",
            poke(8, (MAX_ORDER_KEYS + 1) as u8),
            "order keys exceeds cap",
        ),
        ("PkSet cap", pk_set_over_cap, "PkSet count"),
        ("trailing bytes", trailing, "trailing"),
        ("oversized blob", vec![0u8; MAX_READ_SPEC_BYTES + 1], "exceeds cap"),
        ("unknown aggregate", bad_agg_op, "unknown aggregate op"),
        ("group col cap", group_cols_over_cap, "group cols exceeds cap"),
        ("agg item cap", aggs_over_cap, "agg items exceeds cap"),
    ];
    for (name, bytes, want) in cases {
        let err = ReadSpec::decode(bytes).expect_err(name);
        assert!(err.contains(want), "{name}: {err:?} does not name {want:?}");
    }
    // Truncation trips the reader itself, which words its own message.
    assert!(ReadSpec::decode(&truncated).is_err(), "truncated PkSet");
}

#[test]
fn scan_spec_extra_roundtrips_and_rejects_a_malformed_blob() {
    let block = vec![9u8; 40];
    // Either side may be empty: an identity projection still carries a real
    // block, but the codec must not choke on the degenerate shape.
    for spec in [vec![1u8, 2, 3, 4, 5], vec![]] {
        let packed = pack_scan_spec_extra(&spec, &block);
        let (got_spec, got_block) = unpack_scan_spec_extra(&packed).expect("well-formed blob");
        assert_eq!(got_spec.0, spec.as_slice());
        assert_eq!(got_block, block.as_slice());
    }

    let packed = pack_scan_spec_extra(&[1, 2, 3], &[4, 5]);
    assert!(
        unpack_scan_spec_extra(&packed[..packed.len() - 1]).is_err(),
        "truncated"
    );
    let mut long = packed.clone();
    long.push(0);
    assert!(unpack_scan_spec_extra(&long).is_err(), "trailing byte");
    assert!(unpack_scan_spec_extra(&[]).is_err(), "empty");
}
