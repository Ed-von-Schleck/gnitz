use super::*;
use crate::test_support::Rng;
use gnitz_wire::MAX_PK_BYTES;

/// Every byte primitive over OPK keys, at every PK width and over key pairs
/// sharing a prefix of every length: each packs, compares or sort-keys exactly
/// as `memcmp` orders the bytes.
#[test]
fn opk_byte_primitives_agree_with_memcmp_at_every_width() {
    let mut rng = Rng::new(0xB17E5);
    for w in 0..=MAX_PK_BYTES {
        for _ in 0..16 {
            let a: Vec<u8> = (0..w).map(|_| rng.next_u64() as u8).collect();
            // The leading 16 bytes, zero-padded, read big-endian.
            let mut lead = [0u8; 16];
            lead[..w.min(16)].copy_from_slice(&a[..w.min(16)]);
            assert_eq!(pack_pk_be(&a), u128::from_be_bytes(lead), "w={w}");
            for pos in 0..=w {
                let mut b = a.clone();
                if pos < w {
                    b[pos] = rng.next_u64() as u8;
                }
                let want = a.cmp(&b);
                assert_eq!(compare_pk_ordering(&a, &b), want, "w={w} pos={pos}");
                pk_width_dispatch!(w, |K| assert_eq!(
                    Ord::cmp(&K::from_opk(&a[..]), &K::from_opk(&b[..])),
                    want,
                    "w={w} pos={pos}"
                ));
            }
        }
    }
}
