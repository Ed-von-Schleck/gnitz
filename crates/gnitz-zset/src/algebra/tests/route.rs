use super::*;
use crate::schema::SchemaColumn;
use crate::test_support::Rng;
use gnitz_wire::{key_image, Cut, PkBuf, PkColList, TypeCode};

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

fn col(tc: TypeCode) -> SchemaColumn {
    SchemaColumn::new(tc, false)
}

/// A keyed placement's stride is the OPK width of the leading PK columns it
/// names, and a prefix outside the PK's arity is a panic.
#[test]
fn keyed_placements_take_the_leading_pk_columns_width() {
    use TypeCode::{I64, U32, U64};
    // PK (U32, U64, U64): distinct widths, so each prefix stride is unambiguous.
    let s = SchemaDescriptor::new(&[col(U32), col(U64), col(U64), col(I64)], &[0, 1, 2]);
    for (prefix, dist_stride) in [(1, 4), (2, 12), (3, 20)] {
        assert_eq!(
            Placement::keyed(&s, prefix),
            Placement::Keyed { dist_stride },
            "{prefix}"
        );
    }
    assert_eq!(Placement::full_pk(&s), Placement::Keyed { dist_stride: 20 });
    for prefix in [0, 4] {
        assert!(
            std::panic::catch_unwind(|| Placement::keyed(&s, prefix)).is_err(),
            "prefix {prefix}"
        );
    }
}

/// A keyed placement's owner is the worker its leading `dist_stride` key bytes
/// hash to; no other placement has one.
#[test]
fn owner_hashes_the_distribution_prefix_alone() {
    let mut rng = Rng::new(0x0091_ACE5);
    for _ in 0..2000 {
        let pk: [u8; 20] = std::array::from_fn(|_| rng.gen_u128() as u8);
        for nw in [1, 3, 4, NW_MAX] {
            for dist_stride in [4u8, 12, 20] {
                assert_eq!(
                    Placement::Keyed { dist_stride }.owner(&pk, nw),
                    Some(worker_for_pk_bytes(&pk[..dist_stride as usize], nw)),
                    "stride {dist_stride} nw={nw}"
                );
            }
            assert_eq!(Placement::Replicated.owner(&pk, nw), None);
            assert_eq!(Placement::Local.owner(&pk, nw), None);
        }
    }
}

/// Worker count the confinement checks route against; any count works.
const NW: usize = 4;

/// The PK span's `range_keys` is the band of exactly the keys a range admits, and
/// `confined_worker` names the band's worker iff every key in it shares the
/// distribution prefix — worker 0 for an empty band.
#[test]
fn pk_range_and_confinement_follow_key_membership() {
    let s = SchemaDescriptor::new(&[col(TypeCode::I8), col(TypeCode::U8)], &[0, 1]);
    let placed = [
        Placement::keyed(&s, 1),
        Placement::full_pk(&s),
        Placement::Replicated,
        Placement::Local,
    ];
    let edges = [0u128, 1, 0x7F, 0x80, 0xFE, 0xFF];
    let probes = [0u8, 1, 2, 0x7E, 0x7F, 0x80, 0x81, 0xFD, 0xFE, 0xFF];
    let cuts: Vec<Cut> = edges.iter().flat_map(|&e| [Cut::before(e), Cut::after(e)]).collect();
    let mut ranges = Vec::new();
    for &start in &cuts {
        for &end in &cuts {
            ranges.push(KeyRange::new(PkColList::from_slice(&[0]), &[], start, end));
            for &e in &edges {
                ranges.push(KeyRange::new(PkColList::from_slice(&[0, 1]), &[e], start, end));
            }
        }
    }

    let key = |k: &PkBuf| u32::from(u16::from_be_bytes(k.pk_bytes().try_into().unwrap()));
    for r in &ranges {
        let (lo, hi) = match crate::schema::KeySpec::for_pk(&s).range_keys(s.pk_stride(), r) {
            Some((start, end)) => (key(&start), end.as_ref().map_or(1 << 16, key)),
            None => (0, 0),
        };
        let n = r.eq_vals().len();
        for k0 in probes {
            for k1 in probes {
                let img = [k0 as u128, k1 as u128];
                let admitted =
                    img[..n] == r.eq_vals()[..] && r.start <= Cut::before(img[n]) && Cut::after(img[n]) <= r.end;
                let k = u32::from(u16::from_be_bytes([k0, k1]));
                assert_eq!((lo..hi).contains(&k), admitted, "{r:?}: key ({k0:#x}, {k1:#x})");
            }
        }
        for p in placed {
            let want = match p {
                _ if lo == hi => Some(0),
                Placement::Keyed { dist_stride }
                    if lo >> (16 - 8 * dist_stride) == (hi - 1) >> (16 - 8 * dist_stride) =>
                {
                    p.owner(&(lo as u16).to_be_bytes(), NW)
                }
                _ => None,
            };
            assert_eq!(p.confined_worker(&s, r, NW), want, "{p:?} {r:?}");
        }
    }

    // A range over a column that does not lead the PK walks an index or a scan.
    let off_pk = KeyRange::point(PkColList::from_slice(&[1]), &[], 7);
    assert_eq!(placed[0].confined_worker(&s, &off_pk, NW), None);
}
