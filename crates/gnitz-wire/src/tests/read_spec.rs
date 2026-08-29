use super::*;
use crate::range::Cut::{After, Before};

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

#[test]
fn roundtrips_every_bound() {
    let specs = [
        ReadSpec {
            bound: ReadBound::None,
            predicate: vec![1, 2, 3, 4],
            sink: ReadSink::Rows {
                projection: vec![],
                order: sample_order(),
                limit_k: 42,
            },
        },
        ReadSpec {
            bound: ReadBound::PkRange(RangeDescriptor::new(&[], After(10), After(u64::MAX as u128))),
            predicate: vec![],
            sink: ReadSink::Rows {
                projection: vec![9, 9, 9],
                order: vec![],
                limit_k: 0,
            },
        },
        ReadSpec {
            bound: ReadBound::IndexRange {
                idx_cols: 0x0000_0002_0000_0001,
                exact: false,
                desc: RangeDescriptor::new(&[7], Before(1), Before(u128::MAX)),
            },
            predicate: vec![5, 5],
            sink: ReadSink::Rows {
                projection: vec![6],
                order: sample_order(),
                limit_k: 100,
            },
        },
        // The exact form: the client stripped the bounded conjuncts, so the
        // flag must survive the round trip or the walk becomes tradeable.
        ReadSpec {
            bound: ReadBound::IndexRange {
                idx_cols: 0x0000_0000_0000_0003,
                exact: true,
                desc: RangeDescriptor::point(&[], u128::MAX),
            },
            predicate: vec![],
            sink: ReadSink::all_rows(),
        },
        ReadSpec {
            bound: ReadBound::PkSet(vec![5, 10, 3, 99]),
            predicate: vec![],
            sink: ReadSink::Rows {
                projection: vec![],
                order: vec![OrderKey {
                    col: 7,
                    desc: false,
                    nulls_first: false,
                }],
                limit_k: 8,
            },
        },
        // The bootstrap sentinel and a saturated cursor, both of which the
        // router must see as a delta bound rather than as a range.
        ReadSpec {
            bound: ReadBound::Delta { after_tick: 0 },
            predicate: vec![],
            sink: ReadSink::all_rows(),
        },
        ReadSpec {
            bound: ReadBound::Delta { after_tick: u64::MAX },
            predicate: vec![7, 7],
            sink: ReadSink::all_rows(),
        },
    ];
    for spec in specs {
        let bytes = enc(&spec);
        assert!(bytes.len() <= MAX_READ_SPEC_BYTES);
        assert_eq!(ReadSpec::decode(&bytes), Ok(spec.clone()), "{spec:?}");
    }
}

#[test]
fn accepts_unsorted_pk_set_including_widened_signed() {
    // `pk IN (-1, 5)` on a signed PK widens to `[0xFFFF…FF, 5]` — typed order
    // is increasing but raw-u128 order is DECREASING. The decoder must accept
    // this without a sortedness check.
    let spec = ReadSpec {
        bound: ReadBound::PkSet(vec![u64::MAX as u128, 5]),
        predicate: vec![],
        sink: ReadSink::all_rows(),
    };
    let bytes = enc(&spec);
    assert_eq!(ReadSpec::decode(&bytes), Ok(spec));
}

fn empty_spec() -> ReadSpec {
    ReadSpec {
        bound: ReadBound::None,
        predicate: vec![],
        sink: ReadSink::all_rows(),
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
        assert_eq!(peek_delta_bound(&bytes), Some(tick));
        assert_eq!(peek_pk_range(&bytes), None);
    }
    // And every other bound peeks as no delta.
    let range = enc(&ReadSpec {
        bound: ReadBound::PkRange(RangeDescriptor::new(&[], After(1), After(9))),
        predicate: vec![],
        sink: ReadSink::all_rows(),
    });
    assert_eq!(peek_delta_bound(&range), None);
    assert_eq!(peek_delta_bound(&enc(&empty_spec())), None);
}

/// The watermark's two halves survive the round trip independently — the
/// server packs it, the client unpacks it, and nothing else binds them.
#[test]
fn delta_watermark_roundtrips_both_halves() {
    for &(tag, tick) in &[(0u64, 0u64), (1, 2), (u64::MAX, 0), (0, u64::MAX), (u64::MAX, u64::MAX)] {
        assert_eq!(unpack_delta_watermark(pack_delta_watermark(tag, tick)), (tag, tick));
    }
}

/// Both peeks take the **unpacked** `ReadSpec`, never the packed
/// `seek_pk_extra` blob it travels inside. The two are both `&[u8]`, so
/// handing over the wrong one compiles, routes every read as a broadcast and
/// gates no poll — correct answers, silently unrouted, which no row
/// comparison would show. Pin the distinction the types cannot.
#[test]
fn the_peeks_read_a_spec_never_the_packed_blob() {
    let range = enc(&ReadSpec {
        bound: ReadBound::PkRange(RangeDescriptor::new(&[], After(1), After(9))),
        predicate: vec![],
        sink: ReadSink::all_rows(),
    });
    let delta = enc(&ReadSpec {
        bound: ReadBound::Delta { after_tick: 7 },
        predicate: vec![],
        sink: ReadSink::all_rows(),
    });
    assert!(peek_pk_range(&range).is_some(), "an unpacked spec names its range");
    assert_eq!(peek_delta_bound(&delta), Some(7), "an unpacked spec names its cursor");

    // The reply-schema block is what a real request carries beside the spec.
    assert_eq!(
        peek_pk_range(&pack_scan_spec_extra(&range, &[1, 2, 3])),
        None,
        "the packed blob is not a spec: it must confine no worker"
    );
    assert_eq!(
        peek_delta_bound(&pack_scan_spec_extra(&delta, &[1, 2, 3])),
        None,
        "the packed blob is not a spec: it must gate no poll"
    );
}

#[test]
fn decode_rejects_bad_version() {
    let mut bytes = enc(&empty_spec());
    bytes[0] = 99;
    assert!(ReadSpec::decode(&bytes).unwrap_err().contains("version"));
}

#[test]
fn decode_rejects_unknown_kind() {
    let mut bytes = enc(&empty_spec());
    bytes[1] = 9;
    assert!(ReadSpec::decode(&bytes).unwrap_err().contains("bound kind"));
}

#[test]
fn decode_rejects_unknown_sink_tag() {
    let mut bytes = enc(&empty_spec());
    bytes[2] = 9;
    assert!(ReadSpec::decode(&bytes).unwrap_err().contains("sink tag"));
}

#[test]
fn decode_rejects_order_key_over_cap() {
    // Layout of the empty Rows spec: 4-byte header | u32 predicate len |
    // n_order at offset 8.
    let mut bytes = enc(&empty_spec());
    bytes[8] = (MAX_ORDER_KEYS + 1) as u8;
    assert!(ReadSpec::decode(&bytes).unwrap_err().contains("order keys exceeds cap"));
}

#[test]
fn decode_rejects_pk_set_over_cap() {
    // Hand-build a header + PkSet count exceeding the cap (the count is read
    // before any per-key bytes, so no keys are needed to trip it).
    let mut bytes = vec![VERSION, BOUND_PK_SET, SINK_ROWS, 0];
    bytes.extend_from_slice(&((MAX_PK_SET_KEYS + 1) as u32).to_le_bytes());
    assert!(ReadSpec::decode(&bytes).unwrap_err().contains("PkSet count"));
}

/// Duplicates round-trip: rejecting them needs the PK width, which this
/// decoder does not have — the worker's OPK-sorted gather rejects them.
#[test]
fn decode_keeps_duplicate_pk_set_keys() {
    let spec = ReadSpec {
        bound: ReadBound::PkSet(vec![5, 5]),
        predicate: vec![],
        sink: ReadSink::all_rows(),
    };
    assert_eq!(ReadSpec::decode(&enc(&spec)).unwrap(), spec);
}

#[test]
fn decode_rejects_trailing_bytes() {
    let mut bytes = enc(&empty_spec());
    bytes.push(0);
    assert!(ReadSpec::decode(&bytes).unwrap_err().contains("trailing"));
}

#[test]
fn decode_rejects_truncation() {
    let bytes = enc(&ReadSpec {
        bound: ReadBound::PkSet(vec![1, 2, 3]),
        predicate: vec![],
        sink: ReadSink::all_rows(),
    });
    // Chop the last key's bytes off.
    assert!(ReadSpec::decode(&bytes[..bytes.len() - 4]).is_err());
}

#[test]
fn decode_rejects_oversized_blob() {
    let big = vec![0u8; MAX_READ_SPEC_BYTES + 1];
    assert!(ReadSpec::decode(&big).unwrap_err().contains("exceeds cap"));
}

#[test]
fn scan_spec_extra_roundtrips() {
    let spec = vec![1u8, 2, 3, 4, 5];
    let block = vec![9u8; 40];
    let packed = pack_scan_spec_extra(&spec, &block);
    assert_eq!(unpack_scan_spec_extra(&packed), Ok((spec.as_slice(), block.as_slice())));
    // Empty block is valid (identity projection carries a real block anyway,
    // but the codec must not choke on either side being empty).
    let packed = pack_scan_spec_extra(&[], &block);
    assert_eq!(unpack_scan_spec_extra(&packed), Ok(([].as_slice(), block.as_slice())));
}

#[test]
fn scan_spec_extra_rejects_truncation_and_trailing() {
    let packed = pack_scan_spec_extra(&[1, 2, 3], &[4, 5]);
    assert!(unpack_scan_spec_extra(&packed[..packed.len() - 1]).is_err());
    let mut long = packed.clone();
    long.push(0);
    assert!(unpack_scan_spec_extra(&long).is_err());
    assert!(unpack_scan_spec_extra(&[]).is_err());
}

#[test]
fn roundtrips_fold_sink() {
    let specs = [
        // Grouped aggregate: two group cols, COUNT(*) + SUM(col 2).
        ReadSpec {
            bound: ReadBound::None,
            predicate: vec![1, 2, 3],
            sink: ReadSink::Fold(AggReadSpec {
                group_cols: vec![0, 3],
                aggs: vec![
                    AggReadItem {
                        op: AggFunc::Count,
                        src_col: 0,
                    },
                    AggReadItem {
                        op: AggFunc::Sum,
                        src_col: 2,
                    },
                ],
            }),
        },
        // Global aggregate over a PK range: no group cols, MIN/MAX.
        ReadSpec {
            bound: ReadBound::PkRange(RangeDescriptor::new(&[], After(1), After(9))),
            predicate: vec![],
            sink: ReadSink::Fold(AggReadSpec {
                group_cols: vec![],
                aggs: vec![
                    AggReadItem {
                        op: AggFunc::Min,
                        src_col: 4,
                    },
                    AggReadItem {
                        op: AggFunc::Max,
                        src_col: 4,
                    },
                ],
            }),
        },
        // DISTINCT: group cols, no aggs.
        ReadSpec {
            bound: ReadBound::None,
            predicate: vec![],
            sink: ReadSink::Fold(AggReadSpec {
                group_cols: vec![1, 2, 5],
                aggs: vec![],
            }),
        },
    ];
    for spec in specs {
        let bytes = enc(&spec);
        assert!(bytes.len() <= MAX_READ_SPEC_BYTES);
        assert_eq!(ReadSpec::decode(&bytes), Ok(spec.clone()), "{spec:?}");
    }
}

/// Header + empty predicate of a hand-built fold-sink blob.
fn fold_header() -> Vec<u8> {
    let mut bytes = vec![VERSION, BOUND_NONE, SINK_FOLD, 0];
    bytes.extend_from_slice(&0u32.to_le_bytes()); // predicate len
    bytes
}

#[test]
fn fold_rejects_bad_op_code() {
    let mut bytes = fold_header();
    bytes.extend_from_slice(&0u16.to_le_bytes()); // n_group_cols = 0
    bytes.push(1); // n_aggs = 1
    bytes.push(7); // no such AggFunc
    bytes.extend_from_slice(&0u16.to_le_bytes()); // src_col
    assert!(ReadSpec::decode(&bytes).unwrap_err().contains("unknown aggregate op"));
}

#[test]
fn fold_rejects_over_cap() {
    // Group-col count over the cap (read before per-col bytes, so no col
    // bytes are needed to trip).
    let mut bytes = fold_header();
    bytes.extend_from_slice(&((MAX_COLUMNS + 1) as u16).to_le_bytes());
    assert!(ReadSpec::decode(&bytes).unwrap_err().contains("group cols exceeds cap"));

    // And an over-cap agg-item count (zero group cols first).
    let mut bytes = fold_header();
    bytes.extend_from_slice(&0u16.to_le_bytes()); // n_group_cols = 0
    bytes.push((MAX_COLUMNS + 1) as u8);
    assert!(ReadSpec::decode(&bytes).unwrap_err().contains("agg items exceeds cap"));
}
