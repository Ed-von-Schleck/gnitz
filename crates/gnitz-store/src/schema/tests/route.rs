use super::*;
use gnitz_wire::{encode_pk_column, TypeCode};

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
    // i64 spanning zero, as OPK images (sign-flipped).
    let signed: Vec<u128> = (0..N)
        .map(|i| (i as i64 - N as i64 / 2) as u64 as u128 ^ (1u128 << 63))
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

/// A PK region wider than `NARROW_PK_MAX_BYTES` routes on all its bytes.
#[test]
fn worker_for_pk_bytes_reads_a_whole_wide_region() {
    let mut head = [0u8; 16];
    encode_pk_column(&7u64.to_le_bytes(), TypeCode::U64, &mut head[..8]);
    encode_pk_column(&9u64.to_le_bytes(), TypeCode::U64, &mut head[8..]);

    let mut seen = std::collections::HashSet::new();
    for tail in 0..64u64 {
        let mut opk = [0u8; 24];
        opk[..16].copy_from_slice(&head);
        encode_pk_column(&tail.to_le_bytes(), TypeCode::U64, &mut opk[16..]);
        assert!(opk.len() > NARROW_PK_MAX_BYTES);
        for nw in [1usize, 2, 3, 7, NW_MAX] {
            assert!(worker_for_pk_bytes(&opk, nw) < nw, "route out of range at nw={nw}");
        }
        seen.insert(worker_for_pk_bytes(&opk, 8));
    }
    assert!(seen.len() > 1, "the region past 16 bytes must reach the route");
}

/// Independent 128-bit keys (the UUID shape) spread evenly; their sampling
/// noise needs a looser bound than the structured shapes.
#[test]
fn router_spreads_random_keys_evenly() {
    const N: usize = 500_000;
    // SplitMix64-style stream over both halves — deterministic, no rand dep.
    let mut s: u64 = 0x243f_6a88_85a3_08d3;
    let mut next = || {
        s = s.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = s;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    };
    let keys: Vec<u128> = (0..N).map(|_| ((next() as u128) << 64) | next() as u128).collect();
    for nw in [1, 2, 3, 4, 8, 16, 32, NW_MAX] {
        let spread = load_spread(&keys, nw);
        assert!(spread <= 1.05, "random keys: {spread:.4}x the mean at nw={nw}");
    }
}

/// A narrow OPK region routes as `worker_for_key` of its widened value.
#[test]
fn worker_for_pk_bytes_matches_widened_key() {
    for &(tc, sz) in &[
        (TypeCode::U8, 1usize),
        (TypeCode::I8, 1),
        (TypeCode::U16, 2),
        (TypeCode::I16, 2),
        (TypeCode::U32, 4),
        (TypeCode::I32, 4),
        (TypeCode::U64, 8),
        (TypeCode::I64, 8),
        (TypeCode::U128, 16),
    ] {
        for v in [0i128, 1, -1, 7, -7, 127, -128, 1000, -1000, i32::MAX as i128] {
            let le = (v as u128).to_le_bytes();
            let mut opk = [0u8; 16];
            encode_pk_column(&le[..sz], tc, &mut opk[..sz]);
            for nw in [1usize, 2, 3, 4, 7, 16, NW_MAX] {
                assert_eq!(
                    worker_for_pk_bytes(&opk[..sz], nw),
                    worker_for_key(widen_pk_be(&opk[..sz]), nw),
                    "tc={tc} v={v} nw={nw}",
                );
            }
        }
    }
}
