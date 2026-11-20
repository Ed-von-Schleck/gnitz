use super::*;

/// The seeks equal the linear lower bound, and the key's rows run from it to the
/// linear upper bound, for every row count, hint and width arm. Keys differ only in their last two bytes, past the
/// register from stride 24.
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
                    unsafe { seek_lower_bound(n, stride, pk, &key, None) },
                    expected,
                    "lower bound stride={stride} n={n} p={p}"
                );
                let upper = (0..n).find(|&i| row(i) > &key[..]).unwrap_or(n);
                assert_eq!(
                    unsafe { seek_pk_rows(n, stride, pk, &key, None) },
                    expected..upper,
                    "rows of stride={stride} n={n} p={p}"
                );
                // Hints past `n` included: a caller's hint may have run off the end.
                for hint in 0..=vals.len() {
                    assert_eq!(
                        unsafe { seek_lower_bound(n, stride, pk, &key, Some(hint)) },
                        expected,
                        "gallop stride={stride} n={n} p={p} hint={hint}"
                    );
                    assert_eq!(
                        unsafe { seek_pk_rows(n, stride, pk, &key, Some(hint)) },
                        expected..upper,
                        "hinted rows of stride={stride} n={n} p={p} hint={hint}"
                    );
                }
            }
        }
    }
}

/// A PK region of `stride`-byte rows, read as a source for `pk_group_end`.
struct PkRows<'a> {
    region: &'a [u8],
    stride: usize,
}

impl RowSource for PkRows<'_> {
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        &self.region[row * self.stride..(row + 1) * self.stride]
    }
    fn get_null_word(&self, _row: usize) -> u64 {
        0
    }
    fn get_col_ptr(&self, _row: usize, _payload_col: usize, _col_size: usize) -> &[u8] {
        unreachable!("a PK region has no payload")
    }
    fn heap(&self) -> gnitz_wire::Heap<'_> {
        gnitz_wire::Heap::Plain(&[])
    }
    fn row_count(&self) -> usize {
        self.region.len() / self.stride.max(1)
    }
}

/// `pk_group_end` equals the linear scan for the end of each row's run of equal
/// leading `width` bytes, for every stride up to `MAX_PK_BYTES` and every width
/// up to it. Rows differ at their first, a middle and their last byte, so every
/// prefix width splits the rows into groups of its own.
#[test]
fn pk_group_end_matches_the_linear_scan_at_every_width() {
    use gnitz_wire::MAX_PK_BYTES;
    let digits: [(u8, u8, u8); 9] = [
        (1, 1, 1),
        (1, 1, 1),
        (1, 1, 2),
        (1, 2, 2),
        (1, 2, 2),
        (2, 2, 2),
        (2, 2, 2),
        (2, 2, 3),
        (3, 1, 1),
    ];
    for stride in 1..=MAX_PK_BYTES {
        let mid = stride / 2;
        let region: Vec<u8> = digits
            .iter()
            .flat_map(|&(first, middle, last)| {
                let mut k = vec![0x5Au8; stride];
                k[mid] = middle;
                k[stride - 1] = last;
                k[0] = first;
                k
            })
            .collect();
        let src = PkRows { region: &region, stride };
        let row = |i: usize| &region[i * stride..(i + 1) * stride];
        for width in 0..=stride {
            for start in 0..digits.len() {
                let expected = (start + 1..digits.len())
                    .find(|&j| row(j)[..width] != row(start)[..width])
                    .unwrap_or(digits.len());
                assert_eq!(
                    pk_group_end(&src, start, width),
                    expected,
                    "stride={stride} width={width} start={start}"
                );
            }
        }
    }
}
