use super::*;
use crate::test_support::simd_levels;

/// The bit patterns a lane predicate has to get right: the integer extremes
/// either side of the sign, and the floats that do not order like their bits.
const EDGES: [i64; 14] = [
    0,
    1,
    -1,
    2,
    i64::MIN,
    i64::MAX,
    i64::MIN + 1,
    f64::NAN.to_bits() as i64,
    (-0.0f64).to_bits() as i64,
    f64::INFINITY.to_bits() as i64,
    f64::NEG_INFINITY.to_bits() as i64,
    1.5f64.to_bits() as i64,
    (-1.5f64).to_bits() as i64,
    f64::MIN_POSITIVE.to_bits() as i64,
];

/// Every ordered pair of [`EDGES`], padded to whole words with a fixed mix of
/// arbitrary patterns and small values that collide.
fn operand_lanes() -> (Vec<i64>, Vec<i64>) {
    let (mut a, mut b) = (Vec::new(), Vec::new());
    for &x in &EDGES {
        for &y in &EDGES {
            a.push(x);
            b.push(y);
        }
    }
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state as i64
    };
    while !a.len().is_multiple_of(64) || a.len() < 512 {
        let (x, y) = (next(), next());
        let small = a.len().is_multiple_of(3);
        a.push(if small { x % 4 } else { x });
        b.push(if small { y % 4 } else { y });
    }
    (a, b)
}

/// One bit per lane, as a per-row loop packs it.
fn packed(n: usize, bit: impl Fn(usize) -> bool) -> Vec<u64> {
    let mut out = vec![0u64; n / 64];
    for i in (0..n).filter(|&i| bit(i)) {
        out[i / 64] |= 1 << (i % 64);
    }
    out
}

fn check_pred<P: LanePred>(name: &str) {
    let (a, b) = operand_lanes();
    let want = packed(a.len(), |i| P::scalar(a[i], b[i]));
    for (level_name, level) in simd_levels() {
        let mut got = vec![0u64; want.len()];
        pred_bits::<P>(level, &a, &b, &mut got);
        assert_eq!(got, want, "{name} at {level_name}");
    }
}

#[test]
fn a_predicate_packs_what_it_says_per_row() {
    check_pred::<Eq>("Eq");
    check_pred::<Not<Eq>>("Not<Eq>");
    check_pred::<GtSigned>("GtSigned");
    check_pred::<GeSigned>("GeSigned");
    check_pred::<GtUnsigned>("GtUnsigned");
    check_pred::<GeUnsigned>("GeUnsigned");
    check_pred::<EqUnsignedSigned>("EqUnsignedSigned");
    check_pred::<Not<EqUnsignedSigned>>("Not<EqUnsignedSigned>");
    check_pred::<LtUnsignedSigned>("LtUnsignedSigned");
    check_pred::<Not<LtUnsignedSigned>>("Not<LtUnsignedSigned>");
    check_pred::<LeUnsignedSigned>("LeUnsignedSigned");
    check_pred::<Not<LeUnsignedSigned>>("Not<LeUnsignedSigned>");
    check_pred::<EqFloat>("EqFloat");
    check_pred::<Not<EqFloat>>("Not<EqFloat>");
    check_pred::<GtFloat>("GtFloat");
    check_pred::<GeFloat>("GeFloat");
}

/// The spellings `eval_batch` relies on for the operators it has no predicate
/// of its own for.
#[test]
fn a_mirrored_or_complemented_predicate_is_the_operator() {
    for &x in &EDGES {
        for &y in &EDGES {
            let (fx, fy) = (f64::from_bits(x as u64), f64::from_bits(y as u64));
            assert_eq!(GtSigned::scalar(y, x), x < y);
            assert_eq!(GeSigned::scalar(y, x), x <= y);
            assert_eq!(GtUnsigned::scalar(y, x), (x as u64) < (y as u64));
            assert_eq!(GeUnsigned::scalar(y, x), (x as u64) <= (y as u64));
            assert_eq!(GtFloat::scalar(y, x), fx < fy, "{fx} < {fy}");
            assert_eq!(GeFloat::scalar(y, x), fx <= fy, "{fx} <= {fy}");
            assert_eq!(<Not<EqFloat>>::scalar(x, y), fx != fy, "{fx} <> {fy}");
            // An unsigned `x` against a signed `y`, widened so both fit.
            let (wx, wy) = (x as u64 as i128, y as i128);
            assert_eq!(EqUnsignedSigned::scalar(x, y), wx == wy);
            assert_eq!(LtUnsignedSigned::scalar(x, y), wx < wy);
            assert_eq!(LeUnsignedSigned::scalar(x, y), wx <= wy);
        }
    }
}

#[test]
fn truthy_bits_marks_the_non_zero_lanes() {
    let (a, _) = operand_lanes();
    let want = packed(a.len(), |i| a[i] != 0);
    for (level_name, level) in simd_levels() {
        let mut got = vec![0u64; want.len()];
        truthy_bits(level, &a, &mut got);
        assert_eq!(got, want, "{level_name}");
    }
}

#[test]
fn in_set_bits_marks_the_members() {
    let (a, b) = operand_lanes();
    for len in [0, 1, 2, 3, 4, 5, 8, 11, 33] {
        let set = &b[..len];
        let want = packed(a.len(), |i| set.contains(&a[i]));
        for (level_name, level) in simd_levels() {
            let mut got = vec![0u64; want.len()];
            in_set_bits(level, &a, set, &mut got);
            assert_eq!(got, want, "{len} values at {level_name}");
        }
    }
}

#[test]
fn null_bits_gathers_the_selected_columns() {
    let (a, _) = operand_lanes();
    // A row's null word is an arbitrary pattern on one row in three and sparse
    // on the rest, so a column mask both hits and misses.
    let word = |i: usize| {
        if i.is_multiple_of(3) {
            a[i] as u64
        } else {
            1u64 << (i % 64)
        }
    };
    for rows in [0, 1, 63, 64, 65, 200, 256] {
        let bytes: Vec<u8> = (0..rows).flat_map(|i| word(i).to_le_bytes()).collect();
        for cols in [1u64, 1 << 63, 0b1010, u64::MAX, 0] {
            let mut want = vec![0u64; rows.div_ceil(64)];
            for i in (0..rows).filter(|&i| word(i) & cols != 0) {
                want[i / 64] |= 1 << (i % 64);
            }
            for (level_name, level) in simd_levels() {
                let mut got = vec![u64::MAX; want.len()];
                null_bits(level, &bytes, cols, &mut got);
                assert_eq!(got, want, "{rows} rows, cols {cols:#x} at {level_name}");
            }
        }
    }
}

#[test]
fn blend_takes_a_where_the_bit_is_set() {
    let (a, b) = operand_lanes();
    let take: Vec<u64> = (0..a.len() / 64)
        .map(|w| [0, u64::MAX, 0x5555_5555_5555_5555, a[w] as u64][w % 4])
        .collect();
    let want: Vec<i64> = (0..a.len())
        .map(|i| {
            if (take[i / 64] >> (i % 64)) & 1 != 0 {
                a[i]
            } else {
                b[i]
            }
        })
        .collect();
    for (level_name, level) in simd_levels() {
        let mut got = vec![0i64; a.len()];
        blend(level, &take, &a, &b, &mut got);
        assert_eq!(got, want, "{level_name}");
    }
}

#[test]
fn bit_lanes_writes_each_bit_as_a_lane() {
    let (a, _) = operand_lanes();
    let bits: Vec<u64> = a
        .iter()
        .take(4)
        .map(|&x| x as u64)
        .chain([0, u64::MAX, 1, 1 << 63])
        .collect();
    let want: Vec<i64> = (0..bits.len() * 64)
        .map(|i| ((bits[i / 64] >> (i % 64)) & 1) as i64)
        .collect();
    for (level_name, level) in simd_levels() {
        let mut got = vec![-1i64; want.len()];
        bit_lanes(level, &bits, &mut got);
        assert_eq!(got, want, "{level_name}");
    }
}
