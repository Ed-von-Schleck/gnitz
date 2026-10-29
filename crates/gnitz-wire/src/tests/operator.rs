use super::*;

/// COUNT is always I64, and MIN/MAX select an existing row, so they keep the
/// source type. A SUM needs a scalar register, and a calendar value does not add.
#[test]
fn output_type_is_count_i64_min_max_identity_and_sum_widening() {
    use TypeCode::*;
    for &tc in TypeCode::ALL {
        assert_eq!(AggFunc::Count.output_type(tc), Some(I64), "{tc}");
        assert_eq!(AggFunc::Min.output_type(tc), Some(tc), "{tc}");
        assert_eq!(AggFunc::Max.output_type(tc), Some(tc), "{tc}");
    }
    for (src, want) in [
        (F64, Some(F64)),
        (I32, Some(I64)),
        (F32, Some(F64)),
        // The i64 accumulator's bit pattern is the correct unsigned sum, so a
        // downstream unsigned compare re-seeds right.
        (U64, Some(U64)),
        (Decimal, Some(Decimal)),
        (Bool, None),
        (String, None),
        (Blob, None),
        (U128, None),
        (Date, None),
        (Timestamp, None),
    ] {
        assert_eq!(AggFunc::Sum.output_type(src), want, "SUM over {src}");
    }
}

/// A partial's merge aggregate keeps the partial's type.
#[test]
fn agg_merge_preserves_output_type() {
    for &f in AggFunc::ALL {
        let merge = f.merge_op();
        for &tc in TypeCode::ALL {
            let Some(out) = f.output_type(tc) else {
                continue;
            };
            assert_eq!(merge.output_type(out), Some(out), "{f:?} over {tc:?}");
        }
    }
}

/// COUNT and SUM never render NULL; MIN/MAX do over a nullable source or with no group.
#[test]
fn raw_output_nullable_matches_emit_semantics() {
    for f in [AggFunc::Count, AggFunc::CountNonNull, AggFunc::Sum] {
        for src_nullable in [false, true] {
            for ungrouped in [false, true] {
                assert!(!f.raw_output_nullable(src_nullable, ungrouped), "{f:?}");
            }
        }
    }
    for f in [AggFunc::Min, AggFunc::Max] {
        assert!(!f.raw_output_nullable(false, false), "{f:?} grouped, non-nullable");
        assert!(f.raw_output_nullable(true, false), "{f:?} grouped, nullable");
        assert!(f.raw_output_nullable(false, true), "{f:?} global, non-nullable");
        assert!(f.raw_output_nullable(true, true), "{f:?} global, nullable");
    }
}
