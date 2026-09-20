use super::*;

/// Both seeks equal the linear lower bound, for every hint and every width arm.
///
/// The distinguishing bytes sit in each key's LAST two, so from stride 24 on the
/// tie-break falls in the low limb or past the register width, where a
/// byte-order mistake miscompares instead of passing by accident.
#[test]
fn seek_matches_the_linear_lower_bound_at_every_width() {
    let vals: [u16; 8] = [10, 10, 20, 30, 30, 30, 40, 50];
    let enc = |v: u16, stride: usize| -> Vec<u8> {
        let mut k = vec![0xABu8; stride];
        k[stride - 2..].copy_from_slice(&v.to_be_bytes());
        k
    };

    for &stride in &[2usize, 8, 12, 16, 24, 32, 40] {
        let region: Vec<u8> = vals.iter().flat_map(|&v| enc(v, stride)).collect();
        let n = vals.len();
        let pk = ColPtr { base: region.as_ptr(), stride };
        let row = |i: usize| &region[i * stride..(i + 1) * stride];

        for p in [0u16, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60] {
            let key = enc(p, stride);
            let expected = (0..n).find(|&i| row(i) >= &key[..]).unwrap_or(n);
            assert_eq!(
                unsafe { seek_lower_bound(n, stride, pk, &key) },
                expected,
                "lower bound stride={stride} p={p}"
            );
            for hint in 0..=n {
                assert_eq!(
                    unsafe { seek_advance_to(n, stride, pk, &key, hint) },
                    expected,
                    "gallop stride={stride} p={p} hint={hint}"
                );
            }
        }
    }
}

/// `count == 0` returns 0 for every hint and never reads the (empty) region.
#[test]
fn seek_over_no_rows() {
    let region: Vec<u8> = Vec::new();
    let pk = ColPtr { base: region.as_ptr(), stride: 8 };
    let key = [0u8; 8];
    assert_eq!(unsafe { seek_lower_bound(0, 8, pk, &key) }, 0);
    assert_eq!(unsafe { seek_advance_to(0, 8, pk, &key, 0) }, 0);
    assert_eq!(unsafe { seek_advance_to(0, 8, pk, &key, 7) }, 0);
}
