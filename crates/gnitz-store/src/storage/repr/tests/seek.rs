use super::*;

/// Both seeks equal the linear lower bound, for every row count, hint and width
/// arm. Keys differ only in their last two bytes, past the register from stride 24.
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
        let pk = ColPtr { base: region.as_ptr(), stride };
        let row = |i: usize| &region[i * stride..(i + 1) * stride];

        for n in 0..=vals.len() {
            for p in [0u16, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60] {
                let key = enc(p, stride);
                let expected = (0..n).find(|&i| row(i) >= &key[..]).unwrap_or(n);
                assert_eq!(
                    unsafe { seek_lower_bound(n, stride, pk, &key) },
                    expected,
                    "lower bound stride={stride} n={n} p={p}"
                );
                // Hints past `n` included: a caller's hint may have run off the end.
                for hint in 0..=vals.len() {
                    assert_eq!(
                        unsafe { seek_advance_to(n, stride, pk, &key, hint) },
                        expected,
                        "gallop stride={stride} n={n} p={p} hint={hint}"
                    );
                }
            }
        }
    }
}
