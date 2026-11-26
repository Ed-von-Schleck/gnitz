use super::*;
use crate::codec::decode_all;

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
        KeyRange::new(cols(&[0]), &[], 11, u64::MAX as u128), // x > 10
        KeyRange::new(cols(&[2]), &[], 0, 20),                // x <= 20
        KeyRange::new(cols(&[1, 2]), &[7], 1, u128::MAX),
        KeyRange::new(cols(&[3, 0, 1, 2]), &[1, 2, 3], 11, 49), // max arity
        KeyRange::new(cols(&[0]), &[], 6, 5),                   // empty
        KeyRange::point(cols(&[4, 5]), &[u128::MAX, 0]),
    ];
    for r in shapes {
        assert_eq!(dec(&enc(&r)), Ok(r.clone()), "{r:?}");
    }
}

#[test]
fn a_walk_is_exact_iff_no_unbounded_column_is_nullable() {
    let prefix = KeyRange::point(cols(&[4, 5, 6]), &[1, 2]);
    assert!(prefix.is_exact(|_| false));
    assert!(prefix.is_exact(|c| c != 6), "a nullable bounded column loses no row");
    assert!(!prefix.is_exact(|c| c == 6));
    assert!(KeyRange::point(cols(&[4, 5]), &[1, 2]).is_exact(|_| true));
}

#[test]
fn walks_pk_iff_the_list_leads_the_pk() {
    let r = KeyRange::point(cols(&[2, 0]), &[1, 2]);
    assert!(r.walks_pk(&[2, 0]));
    assert!(r.walks_pk(&[2, 0, 1]));
    assert!(!r.walks_pk(&[0, 2]));
    assert!(!r.walks_pk(&[2]));
}

#[test]
fn decode_rejects_malformed() {
    let good = enc(&KeyRange::new(cols(&[1, 2]), &[7], 1, 2));
    assert!(dec(&good).is_ok());
    // Truncation anywhere.
    for n in 0..good.len() {
        assert!(dec(&good[..n]).is_err(), "truncated to {n}");
    }
    // An equality prefix leaving no range column, up to the byte's maximum.
    for n_eq in [2u8, 3, 255] {
        let mut bad = good.clone();
        bad[8] = n_eq;
        let err = dec(&bad).unwrap_err();
        assert!(err.contains("leave no range column"), "{err}");
    }
    // The column list's own rules are `PkColList`'s; one shows the
    // decode routes the word through it.
    let mut empty = good.clone();
    empty[..8].copy_from_slice(&0u64.to_le_bytes());
    assert!(dec(&empty).unwrap_err().contains("at least one column"));
}

#[test]
#[should_panic(expected = "leave no range column")]
fn new_refuses_an_equality_prefix_over_every_column() {
    KeyRange::new(cols(&[1, 2]), &[7, 8], 0, 0);
}
