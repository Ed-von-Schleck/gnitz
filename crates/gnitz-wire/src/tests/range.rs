use super::*;
use crate::codec::{decode_all, Writer};

fn enc(r: &KeyRange) -> Vec<u8> {
    let mut w = Writer::new();
    w.put(r);
    w.into_vec()
}

fn dec(bytes: &[u8]) -> Result<KeyRange, String> {
    decode_all(bytes, "range", |r| r.get())
}

fn cols(c: &[u32]) -> PkColList {
    PkColList::from_slice(c)
}

#[test]
fn roundtrips_every_shape() {
    let shapes = [
        KeyRange::new(cols(&[0]), &[], Cut::after(10), Cut::after(u64::MAX as u128)), // x > 10
        KeyRange::new(cols(&[2]), &[], Cut::before(0), Cut::after(20)),               // x <= 20
        KeyRange::new(cols(&[1, 2]), &[7], Cut::before(1), Cut::before(u128::MAX)),
        KeyRange::new(cols(&[3, 0, 1, 2]), &[1, 2, 3], Cut::after(10), Cut::before(50)), // max arity
        KeyRange::new(cols(&[0]), &[], Cut::after(5), Cut::after(5)),                    // empty
        KeyRange::point(cols(&[4, 5]), &[u128::MAX], 0),
    ];
    for r in shapes {
        assert_eq!(dec(&enc(&r)), Ok(r), "{r:?}");
    }
}

#[test]
fn cut_order_is_image_then_kind() {
    assert!(Cut::before(5) < Cut::after(5));
    assert!(Cut::after(5) < Cut::before(6));
    assert!(Cut::after(u128::MAX - 1) < Cut::before(u128::MAX));
}

#[test]
fn a_walk_is_exact_iff_no_unbounded_column_is_nullable() {
    let prefix = KeyRange::point(cols(&[4, 5, 6]), &[1], 2);
    assert!(prefix.is_exact(|_| false));
    assert!(prefix.is_exact(|c| c != 6), "a nullable bounded column loses no row");
    assert!(!prefix.is_exact(|c| c == 6));
    assert!(KeyRange::point(cols(&[4, 5]), &[1], 2).is_exact(|_| true));
}

#[test]
fn walks_pk_iff_the_list_leads_the_pk() {
    let r = KeyRange::point(cols(&[2, 0]), &[1], 2);
    assert!(r.walks_pk(&[2, 0]));
    assert!(r.walks_pk(&[2, 0, 1]));
    assert!(!r.walks_pk(&[0, 2]));
    assert!(!r.walks_pk(&[2]));
}

#[test]
fn decode_rejects_malformed() {
    let good = enc(&KeyRange::new(cols(&[1, 2]), &[7], Cut::before(1), Cut::after(2)));
    assert!(dec(&good).is_ok());
    // Truncation anywhere.
    for n in 0..good.len() {
        assert!(dec(&good[..n]).is_err(), "truncated to {n}");
    }
    // Unknown flag bits.
    let mut stray = good.clone();
    stray[9] |= 1 << 4;
    assert!(dec(&stray).is_err());
    // An equality prefix leaving no range column, up to the byte's maximum.
    for n_eq in [2u8, 3, 255] {
        let mut bad = good.clone();
        bad[8] = n_eq;
        let err = dec(&bad).unwrap_err();
        assert!(err.contains("leave no range column"), "{err}");
    }
    // An over-long column word, and one carrying no packed-list flag.
    let mut long = good.clone();
    long[..8].copy_from_slice(&(crate::PK_LIST_PACKED_FLAG | 7).to_le_bytes());
    assert!(dec(&long).is_err());
    let mut untagged = good.clone();
    untagged[..8].copy_from_slice(&1u64.to_le_bytes());
    assert!(dec(&untagged).is_err());
}

#[test]
#[should_panic(expected = "leave no range column")]
fn new_refuses_an_equality_prefix_over_every_column() {
    KeyRange::new(cols(&[1, 2]), &[7, 8], Cut::before(0), Cut::after(0));
}
