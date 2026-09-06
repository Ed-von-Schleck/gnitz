use super::super::fixtures::span_uint;
use super::*;

/// OPK leading-key span of a single U128 value (U128 OPK == big-endian).
fn span(v: u128) -> PkBuf {
    span_uint(v, 16)
}

fn offer_all(acc: &mut PreflightAccumulator, keys: &[PkBuf]) -> bool {
    for &k in keys {
        if !acc.offer(k) {
            return false;
        }
    }
    true
}

/// Adjacent equal spans — within one worker's run or as two workers' equal
/// heads, indistinguishable at this layer — flip the verdict; the verdict is
/// monotonic thereafter.
#[test]
fn accumulator_adjacent_equal_is_duplicate() {
    let mut acc = PreflightAccumulator::new(1000);
    assert!(offer_all(&mut acc, &[span(1), span(2), span(3)]));
    assert!(!acc.offer(span(3)), "equal to prev ⇒ duplicate");
    assert!(!acc.offer(span(4)), "verdict is monotonic");
    assert!(acc.duplicate);
}

#[test]
fn accumulator_distinct_keys_no_duplicate() {
    let mut acc = PreflightAccumulator::new(1000);
    let keys: Vec<PkBuf> = [1u128, 2, 3, 100, u128::MAX].into_iter().map(span).collect();
    assert!(offer_all(&mut acc, &keys));
    assert!(!acc.duplicate);
    let seed = acc.into_seed();
    assert!(!seed.capped(), "under-cap seed must not report capped");
    for k in &keys {
        assert!(seed.may_contain(k.pk_bytes()), "seed under cap holds every span");
    }
    assert!(!seed.may_contain(span(999).pk_bytes()), "and nothing else");
}

/// A duplicate found after the cap has been crossed is still detected — the
/// verdict never depends on the seed.
#[test]
fn accumulator_duplicate_after_cap_crossing() {
    let mut acc = PreflightAccumulator::new(2);
    assert!(offer_all(&mut acc, &[span(1), span(2), span(3), span(4)]));
    assert!(!acc.offer(span(4)));
    assert!(acc.duplicate);
    assert!(acc.into_seed().capped());
}
