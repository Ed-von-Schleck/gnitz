use super::*;

#[test]
fn adjacent_seeds_name_distinct_streams() {
    let mut firsts: Vec<u64> = (0..1024).map(|seed| Rng::new(seed).next_u64()).collect();
    firsts.sort_unstable();
    firsts.dedup();
    assert_eq!(firsts.len(), 1024);
}

#[test]
fn mix_permutes_the_low_bits() {
    let n = 1u64 << 10;
    let mut low: Vec<u64> = (0..n).map(|i| mix(i) & (n - 1)).collect();
    low.sort_unstable();
    assert!(low.iter().copied().eq(0..n));
}
