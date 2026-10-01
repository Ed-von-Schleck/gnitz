use super::*;
use crate::test_support::Rng;
use gnitz_wire::{key_image, TypeCode};

/// The largest worker count a server launches.
const NW_MAX: usize = 64;

/// Peak worker load divided by the mean, over `keys` at `nw` workers.
fn load_spread(keys: &[u128], nw: usize) -> f64 {
    let mut counts = vec![0usize; nw];
    for &k in keys {
        counts[worker_for_key(k, nw)] += 1;
    }
    let mean = keys.len() as f64 / nw as f64;
    counts.iter().map(|&c| c as f64 / mean).fold(0.0, f64::max)
}

/// Every structured key shape a real PK takes spreads evenly at every worker
/// count.
#[test]
fn router_spreads_structured_keys_evenly() {
    const N: u128 = 100_000;
    let sequential: Vec<u128> = (0..N).collect();
    // i64 spanning zero, as key images.
    let signed: Vec<u128> = (0..N)
        .map(|i| key_image(TypeCode::I64, (i as i64 - N as i64 / 2) as u64 as u128))
        .collect();
    let strided: Vec<u128> = (0..N).map(|i| i * 4096).collect();
    // Compound OPK: (hi, lo) packed as one u128, one half held constant. Halves
    // varying in lockstep partially cancel in the mix and are not held to this
    // bound.
    let const_hi: Vec<u128> = (0..N).map(|i| (7u128 << 64) | i).collect();
    let const_lo: Vec<u128> = (0..N).map(|i| (i << 64) | 7).collect();

    for (name, keys) in [
        ("sequential u64", &sequential),
        ("signed i64", &signed),
        ("strided", &strided),
        ("compound const-hi", &const_hi),
        ("compound const-lo", &const_lo),
    ] {
        for nw in 1..=NW_MAX {
            let spread = load_spread(keys, nw);
            assert!(spread <= 1.01, "{name}: worker load {spread:.4}x the mean at nw={nw}",);
        }
    }
}

/// Independent 128-bit keys (the UUID shape) spread evenly; their sampling
/// noise needs a looser bound than the structured shapes.
#[test]
fn router_spreads_random_keys_evenly() {
    let mut rng = Rng::new(0x243f_6a88_85a3_08d3);
    let keys: Vec<u128> = (0..500_000).map(|_| rng.gen_u128()).collect();
    for nw in [1, 2, 3, 4, 8, 16, 32, NW_MAX] {
        let spread = load_spread(&keys, nw);
        assert!(spread <= 1.05, "random keys: {spread:.4}x the mean at nw={nw}");
    }
}

/// A narrow OPK region routes as the key image it spells, at every width that
/// holds it — so the route is a function of the key, never of its physical
/// width. A wide region routes on all its bytes.
#[test]
fn worker_for_pk_bytes_routes_a_narrow_region_by_its_value() {
    for v in [0u128, 1, 0x7F, 0x80, 0xFFFF, 1 << 63, u64::MAX as u128, u128::MAX] {
        let be = v.to_be_bytes();
        let min_width = 16 - (v.leading_zeros() as usize / 8);
        for nw in [1, 3, 7, NW_MAX] {
            for w in min_width.max(1)..=NARROW_PK_MAX_BYTES {
                assert_eq!(
                    worker_for_pk_bytes(&be[16 - w..], nw),
                    worker_for_key(v, nw),
                    "v={v:#x} width={w} nw={nw}"
                );
            }
        }
    }

    let routes: std::collections::HashSet<usize> = (0..64u8)
        .map(|tail| {
            let mut wide = [7u8; NARROW_PK_MAX_BYTES + 8];
            wide[NARROW_PK_MAX_BYTES + 7] = tail;
            worker_for_pk_bytes(&wide, 8)
        })
        .collect();
    assert!(routes.len() > 1, "the bytes past 16 must reach the route");
}
