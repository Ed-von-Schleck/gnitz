use super::Cut::{After, Before};
use super::*;

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
        let bytes = d.encode();
        assert_eq!(RangeDescriptor::decode(&bytes), Ok(d), "{eq:?} {start:?} {end:?}");
    }
}

#[test]
fn max_arity_encodes_to_82_bytes() {
    let d = RangeDescriptor::new(&[1, 2, 3], After(10), Before(50));
    assert_eq!(d.encode().len(), 82);
}

#[test]
fn decode_rejects_malformed() {
    // Too short for the fixed header.
    assert!(RangeDescriptor::decode(&[]).is_err());
    assert!(RangeDescriptor::decode(&[0]).is_err());
    // n_eq with no slot left for the range column.
    let mut d = RangeDescriptor::new(&[1, 2, 3], Before(0), Before(1)).encode();
    d[0] = PK_LIST_MAX_COLS as u8;
    assert!(RangeDescriptor::decode(&d).is_err());
    // Unknown flag bits.
    let mut stray = RangeDescriptor::new(&[7], Before(1), After(2)).encode();
    stray[1] |= 1 << 4;
    assert!(RangeDescriptor::decode(&stray).is_err());
    // Length disagreeing with n_eq — both directions.
    let good = RangeDescriptor::new(&[7], Before(1), After(2)).encode();
    assert!(RangeDescriptor::decode(&good[..good.len() - 1]).is_err());
    let mut long = good.clone();
    long.push(0);
    assert!(RangeDescriptor::decode(&long).is_err());
}
