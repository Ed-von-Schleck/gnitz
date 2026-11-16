use super::*;

#[test]
fn as_le_bytes_mut_writes_through_to_the_typed_slice() {
    let mut words = [0u64; 2];
    as_le_bytes_mut(&mut words)[8..16].copy_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
    assert_eq!(words, [0, 0x0102_0304_0506_0708]);
    assert_eq!(&as_le_bytes(&words)[8..16], &0x0102_0304_0506_0708u64.to_le_bytes());
}

#[test]
fn extend_from_le_bytes_round_trips_as_le_bytes() {
    let src = [1i64, -2, i64::MAX, i64::MIN];
    let mut dst = vec![7i64];
    extend_from_le_bytes(&mut dst, as_le_bytes(&src));
    assert_eq!(dst, [7, 1, -2, i64::MAX, i64::MIN]);
}

#[test]
#[should_panic(expected = "not a whole number")]
fn extend_from_le_bytes_rejects_a_partial_scalar() {
    let mut dst: Vec<u32> = Vec::new();
    extend_from_le_bytes(&mut dst, &[0u8; 6]);
}

/// `BitIter` yields lowest-first and stops at the empty mask — including for
/// bit 63, where `low_bits_mask`'s widest word ends.
#[test]
fn bit_iter_yields_set_bits_lowest_first() {
    assert_eq!(BitIter(0).next(), None);
    assert_eq!(BitIter(0b1011).collect::<Vec<_>>(), vec![0, 1, 3]);
    assert_eq!(BitIter(1u64 << 63).collect::<Vec<_>>(), vec![63]);
    assert_eq!(BitIter(low_bits_mask(64)).count(), 64);
}

/// `set_bit_runs` against a bit-by-bit run collector:
/// every maximal run of set bits below `n`, over single bits at both ends of a
/// word, whole words set and clear, runs crossing and ending on word
/// boundaries, and set bits of the last word at or past `n`.
#[test]
fn set_bit_runs_reports_every_maximal_run() {
    let naive = |bits: &[u64], n: usize| {
        let mut out: Vec<(usize, usize)> = Vec::new();
        for i in (0..n).filter(|&i| bits[i / 64] >> (i % 64) & 1 != 0) {
            match out.last_mut() {
                Some((_, end)) if *end == i => *end = i + 1,
                _ => out.push((i, i + 1)),
            }
        }
        out
    };
    let words = [
        0,
        u64::MAX,
        1,
        1 << 63,
        0x5555_5555_5555_5555,
        0xF0F0_0000_0000_000F,
        0x8000_0000_0000_0001,
    ];
    for a in words {
        for b in words {
            for c in words {
                for d in words {
                    for n in [0usize, 1, 63, 64, 65, 127, 128, 150, 191, 192, 200, 255, 256] {
                        let bits = [a, b, c, d];
                        let bits = &bits[..n.div_ceil(64)];
                        let want = naive(bits, n);
                        let mut got = Vec::new();
                        set_bit_runs(bits.iter().copied(), n, &mut got);
                        assert_eq!(got, want, "{bits:x?} n={n}");
                    }
                }
            }
        }
    }
}

/// Runs are appended after what `out` already holds.
#[test]
fn set_bit_runs_appends() {
    let mut out = vec![(0, 1)];
    set_bit_runs([0b110u64], 3, &mut out);
    assert_eq!(out, [(0, 1), (1, 3)]);
}
