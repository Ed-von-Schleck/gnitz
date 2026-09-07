use super::Cut::{After, Before};
use super::*;

/// `write_range_descriptor` into a standalone buffer — the descriptor is only
/// ever spliced into a larger blob, so the tests build that buffer themselves.
fn enc(d: &RangeDescriptor) -> Vec<u8> {
    let mut w = crate::codec::Writer::with_capacity(RangeDescriptor::encoded_len(d.eq_vals().len()));
    write_range_descriptor(&mut w, d);
    w.into_vec()
}

#[test]
fn roundtrips_every_shape() {
    let shapes: [(&[u128], Cut, Cut); 5] = [
        (&[], After(10), After(u64::MAX as u128)), // pure x > 10
        (&[], Before(0), After(20)),               // pure x <= 20
        (&[7], Before(1), Before(u128::MAX)),      // a = 7 AND 1 <= b < MAX
        (&[1, 2, 3], After(10), Before(50)),       // max arity, 82 bytes
        (&[], After(5), After(5)),                 // zero-width (empty)
    ];
    for (eq, start, end) in shapes {
        let d = RangeDescriptor::new(eq, start, end);
        let bytes = enc(&d);
        assert_eq!(
            bytes.len(),
            RangeDescriptor::encoded_len(eq.len()),
            "the writer must emit exactly what encoded_len promises"
        );
        assert_eq!(RangeDescriptor::decode(&bytes), Ok(d), "{eq:?} {start:?} {end:?}");
    }
    // The one literal the module's own doc reasons about, against the 64-byte
    // `PkBuf` cap the descriptor rides a control-block blob to clear.
    assert_eq!(RangeDescriptor::encoded_len(PK_LIST_MAX_COLS - 1), 82);
}

#[test]
fn decode_rejects_malformed() {
    // Too short for the fixed header.
    assert!(RangeDescriptor::decode(&[]).is_err());
    assert!(RangeDescriptor::decode(&[0]).is_err());
    // n_eq with no slot left for the range column.
    let mut d = enc(&RangeDescriptor::new(&[1, 2, 3], Before(0), Before(1)));
    d[0] = PK_LIST_MAX_COLS as u8;
    assert!(RangeDescriptor::decode(&d).is_err());
    // Unknown flag bits.
    let mut stray = enc(&RangeDescriptor::new(&[7], Before(1), After(2)));
    stray[1] |= 1 << 4;
    assert!(RangeDescriptor::decode(&stray).is_err());
    // Length disagreeing with n_eq — both directions.
    let good = enc(&RangeDescriptor::new(&[7], Before(1), After(2)));
    assert!(RangeDescriptor::decode(&good[..good.len() - 1]).is_err());
    let mut long = good.clone();
    long.push(0);
    assert!(RangeDescriptor::decode(&long).is_err());
}

/// `Cut::type_edges` answers "what does an unconstrained side of a range on this
/// column widen to?", and is the module's whole type dispatch. The edges must be
/// the type's own representable ends — a narrower pair would clip real index
/// entries out of an unbounded scan. `U128` carries edges while `UUID` does not,
/// though both are 16-byte unsigned: a UUID has no ordered arithmetic a planner
/// could saturate a literal against.
#[test]
fn type_edges_are_the_representable_ends_of_an_orderable_column() {
    for tc in TypeCode::ALL {
        let edges = Cut::type_edges(tc);
        let Some(fi) = FixedInt::from_type_code(tc) else {
            let want = matches!(tc, TypeCode::U128).then_some((Before(0), After(u128::MAX)));
            assert_eq!(edges, want, "{tc:?}");
            continue;
        };
        let (min, max) = fi.range();
        assert_eq!(
            edges,
            Some((Before(fi.pack(min)), After(fi.pack(max)))),
            "{tc:?} must widen to its own representable ends"
        );
    }
}
