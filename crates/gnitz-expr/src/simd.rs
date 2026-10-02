//! The lane kernels that go through a mask: a per-row verdict packed to one bit
//! a row, and a packed bit choosing between two lanes.
//!
//! LLVM vectorizes neither shape on its own — it narrows the compare's lanes to
//! a byte each and has no route from there to a bit — so these are written
//! against vector types, whose mask converts to bits in one instruction.
//!
//! Each kernel runs at the widest instruction set the CPU reports, chosen at run
//! time through its [`Level`], so the instruction set the build targets is the
//! floor of these loops and not their width.
//!
//! Every kernel works in whole 64-lane words. A caller hands it the lanes of
//! every word its rows reach, so the lanes past a morsel's last row are read and
//! their bits written like any other.

use fearless_simd::{dispatch, prelude::*};

pub(crate) use fearless_simd::Level;

/// A predicate over two register lanes, spelled once per row and once per
/// vector. The two agree on every pair of bit patterns.
pub(crate) trait LanePred {
    fn scalar(x: i64, y: i64) -> bool;
    fn vector<S: Simd>(simd: S, a: S::i64s, b: S::i64s) -> S::mask64s;
}

macro_rules! lane_pred {
    ($(#[$doc:meta])* $name:ident, |$x:ident, $y:ident| $scalar:expr, |$simd:ident, $a:ident, $b:ident| $vector:expr) => {
        $(#[$doc])*
        pub(crate) struct $name;
        impl LanePred for $name {
            #[inline(always)]
            fn scalar($x: i64, $y: i64) -> bool {
                $scalar
            }
            #[inline(always)]
            fn vector<S: Simd>($simd: S, $a: S::i64s, $b: S::i64s) -> S::mask64s {
                $vector
            }
        }
    };
}

/// The lanes as the `u64` they hold.
#[inline(always)]
fn unsigned<S: Simd>(_: S, v: S::i64s) -> S::u64s {
    v.bitcast()
}

/// The lanes as the `f64` image they hold.
#[inline(always)]
fn float<S: Simd>(_: S, v: S::i64s) -> S::f64s {
    v.bitcast()
}

#[inline(always)]
fn non_negative<S: Simd>(simd: S, v: S::i64s) -> S::mask64s {
    v.simd_ge(S::i64s::splat(simd, 0))
}

lane_pred!(Eq, |x, y| x == y, |_simd, a, b| a.simd_eq(b));
lane_pred!(GtSigned, |x, y| x > y, |_simd, a, b| a.simd_gt(b));
lane_pred!(GeSigned, |x, y| x >= y, |_simd, a, b| a.simd_ge(b));
lane_pred!(GtUnsigned, |x, y| (x as u64) > (y as u64), |simd, a, b| unsigned(
    simd, a
)
.simd_gt(unsigned(simd, b)));
lane_pred!(GeUnsigned, |x, y| (x as u64) >= (y as u64), |simd, a, b| unsigned(
    simd, a
)
.simd_ge(unsigned(simd, b)));
lane_pred!(
    /// An unsigned `x` against a signed `y`, as the three below: a negative `y`
    /// is below every `x`.
    EqUnsignedSigned,
    |x, y| y >= 0 && x == y,
    |simd, a, b| non_negative(simd, b) & a.simd_eq(b)
);
lane_pred!(
    LtUnsignedSigned,
    |x, y| y >= 0 && (x as u64) < (y as u64),
    |simd, a, b| non_negative(simd, b) & unsigned(simd, a).simd_lt(unsigned(simd, b))
);
lane_pred!(
    LeUnsignedSigned,
    |x, y| y >= 0 && (x as u64) <= (y as u64),
    |simd, a, b| non_negative(simd, b) & unsigned(simd, a).simd_le(unsigned(simd, b))
);
lane_pred!(
    /// IEEE comparison, as the two below: a NaN on either side is unordered,
    /// and `-0.0 == 0.0`.
    EqFloat,
    |x, y| f64::from_bits(x as u64) == f64::from_bits(y as u64),
    |simd, a, b| float(simd, a).simd_eq(float(simd, b))
);
lane_pred!(
    GtFloat,
    |x, y| f64::from_bits(x as u64) > f64::from_bits(y as u64),
    |simd, a, b| float(simd, a).simd_gt(float(simd, b))
);
lane_pred!(
    GeFloat,
    |x, y| f64::from_bits(x as u64) >= f64::from_bits(y as u64),
    |simd, a, b| float(simd, a).simd_ge(float(simd, b))
);

/// The complement of `P`. Over floats that is not the mirrored comparison: a
/// NaN fails `P` and so passes `Not<P>`.
pub(crate) struct Not<P>(std::marker::PhantomData<P>);

impl<P: LanePred> LanePred for Not<P> {
    #[inline(always)]
    fn scalar(x: i64, y: i64) -> bool {
        !P::scalar(x, y)
    }
    #[inline(always)]
    fn vector<S: Simd>(simd: S, a: S::i64s, b: S::i64s) -> S::mask64s {
        !P::vector(simd, a, b)
    }
}

/// The vector of lanes `at..` of one word.
#[inline(always)]
fn lanes<S: Simd>(simd: S, word: &[i64; 64], at: usize) -> S::i64s {
    S::i64s::from_slice(simd, &word[at..at + S::i64s::LEN])
}

/// One word of bits from the masks of its vectors, `mask(at)` being the one
/// over lanes `at..`. Each mask enters at the bottom and the word rotates it
/// into place, the rotations adding up to a whole turn: a chain, where OR-ing
/// each mask in at its own shift would be a reduction LLVM rebuilds in vector
/// registers.
#[inline(always)]
fn mask_word<S: Simd>(_: S, mut mask: impl FnMut(usize) -> S::mask64s) -> u64 {
    let n = S::i64s::LEN;
    debug_assert!(64usize.is_multiple_of(n), "a word is a whole number of vectors");
    let mut bits = 0u64;
    for at in (0..64).step_by(n) {
        bits = (bits | mask(at).to_bitmask()).rotate_right(n as u32);
    }
    bits
}

/// The three windows of a kernel over whole words agree on their word count.
fn debug_assert_words(lanes: &[&[i64]], words: usize) {
    debug_assert!(
        lanes.iter().all(|l| l.len() == words * 64),
        "a lane window is 64 lanes a word"
    );
}

/// Bit `i` of `out` set iff `P` holds of lane `i` of `a` and `b`.
pub(crate) fn pred_bits<P: LanePred>(level: Level, a: &[i64], b: &[i64], out: &mut [u64]) {
    debug_assert_words(&[a, b], out.len());
    dispatch!(level, simd => pred_bits_at::<_, P>(simd, a, b, out));
}

#[inline(always)]
fn pred_bits_at<S: Simd, P: LanePred>(simd: S, a: &[i64], b: &[i64], out: &mut [u64]) {
    for ((w, a), b) in out.iter_mut().zip(a.as_chunks::<64>().0).zip(b.as_chunks::<64>().0) {
        *w = mask_word(simd, |at| P::vector(simd, lanes(simd, a, at), lanes(simd, b, at)));
    }
}

/// Bit `i` of `out` set iff lane `i` of `src` is non-zero.
pub(crate) fn truthy_bits(level: Level, src: &[i64], out: &mut [u64]) {
    debug_assert_words(&[src], out.len());
    dispatch!(level, simd => truthy_bits_at(simd, src, out));
}

#[inline(always)]
fn truthy_bits_at<S: Simd>(simd: S, src: &[i64], out: &mut [u64]) {
    let zero = S::i64s::splat(simd, 0);
    for (w, src) in out.iter_mut().zip(src.as_chunks::<64>().0) {
        *w = !mask_word(simd, |at| lanes(simd, src, at).simd_eq(zero));
    }
}

/// Bit `i` of `out` set iff lane `i` of `a` is one of `set`.
///
/// One pass over the lanes per four values of the set, each OR-ing its bits
/// in: the values of a pass are an array, so its length is a constant of the
/// loop and the values stay in registers.
pub(crate) fn in_set_bits(level: Level, a: &[i64], set: &[i64], out: &mut [u64]) {
    debug_assert_words(&[a], out.len());
    out.fill(0);
    let (fours, rest) = set.as_chunks::<4>();
    for four in fours {
        dispatch!(level, simd => or_member_bits(simd, a, four, out));
    }
    match *rest {
        [v0] => dispatch!(level, simd => or_member_bits(simd, a, &[v0], out)),
        [v0, v1] => dispatch!(level, simd => or_member_bits(simd, a, &[v0, v1], out)),
        [v0, v1, v2] => dispatch!(level, simd => or_member_bits(simd, a, &[v0, v1, v2], out)),
        _ => {}
    }
}

/// OR into bit `i` of `out` whether lane `i` of `a` is one of `set`.
#[inline(always)]
fn or_member_bits<S: Simd, const N: usize>(simd: S, a: &[i64], set: &[i64; N], out: &mut [u64]) {
    let set = set.map(|v| S::i64s::splat(simd, v));
    for (w, a) in out.iter_mut().zip(a.as_chunks::<64>().0) {
        *w |= mask_word(simd, |at| {
            let x = lanes(simd, a, at);
            set.iter()
                .fold(S::mask64s::splat(simd, false), |hit, &v| hit | x.simd_eq(v))
        });
    }
}

/// Gather one bit per row of `rows` (a batch's 8-byte null words) into `out`:
/// bit `j` of word `w` is set iff row `w * 64 + j` is null in any column of
/// `cols`. Whole words, so the bits past the last row are 0. A column *mask*,
/// so a two-operand read gathers both columns in one pass.
pub(crate) fn null_bits(level: Level, rows: &[u8], cols: u64, out: &mut [u64]) {
    debug_assert_eq!(out.len(), (rows.len() / 8).div_ceil(64), "one bit per row");
    let (blocks, tail) = rows.as_chunks::<{ 64 * 8 }>();
    dispatch!(level, simd => null_bits_at(simd, blocks, cols, out));
    if let Some(w) = out.get_mut(blocks.len()) {
        let mut word = 0u64;
        for (j, row) in tail.as_chunks::<8>().0.iter().enumerate() {
            word |= ((u64::from_le_bytes(*row) & cols != 0) as u64) << j;
        }
        *w = word;
    }
}

#[inline(always)]
fn null_bits_at<S: Simd>(simd: S, blocks: &[[u8; 64 * 8]], cols: u64, out: &mut [u64]) {
    let (cols, zero) = (S::u64s::splat(simd, cols), S::u64s::splat(simd, 0));
    for (w, block) in out.iter_mut().zip(blocks) {
        *w = !mask_word(simd, |at| {
            // `gnitz-wire` builds for little-endian hosts alone, so a row's
            // eight bytes are its word.
            let rows: S::u64s = S::u8s::from_slice(simd, &block[at * 8..at * 8 + S::u8s::LEN]).bitcast();
            (rows & cols).simd_eq(zero)
        });
    }
}

/// `d[i] = if bit i of take_a { a[i] } else { b[i] }`.
pub(crate) fn blend(level: Level, take_a: &[u64], a: &[i64], b: &[i64], d: &mut [i64]) {
    debug_assert_words(&[a, b, d], take_a.len());
    dispatch!(level, simd => blend_at(simd, take_a, a, b, d));
}

#[inline(always)]
fn blend_at<S: Simd>(simd: S, take_a: &[u64], a: &[i64], b: &[i64], d: &mut [i64]) {
    let n = S::i64s::LEN;
    let words = a.as_chunks::<64>().0.iter().zip(b.as_chunks::<64>().0);
    for ((&take, (a, b)), d) in take_a.iter().zip(words).zip(d.as_chunks_mut::<64>().0) {
        for at in (0..64).step_by(n) {
            S::mask64s::from_bitmask(simd, take >> at)
                .select(lanes(simd, a, at), lanes(simd, b, at))
                .store_slice(&mut d[at..at + n]);
        }
    }
}

/// `d[i] = bit i of bits`, as 0 or 1.
pub(crate) fn bit_lanes(level: Level, bits: &[u64], d: &mut [i64]) {
    debug_assert_words(&[d], bits.len());
    dispatch!(level, simd => bit_lanes_at(simd, bits, d));
}

#[inline(always)]
fn bit_lanes_at<S: Simd>(simd: S, bits: &[u64], d: &mut [i64]) {
    let n = S::u64s::LEN;
    let one = S::u64s::splat(simd, 1);
    let lane = S::u64s::from_fn(simd, |i| i as u64);
    for (&word, d) in bits.iter().zip(d.as_chunks_mut::<64>().0) {
        let word = S::u64s::splat(simd, word);
        for at in (0..64).step_by(n) {
            let v: S::i64s = ((word >> (lane + S::u64s::splat(simd, at as u64))) & one).bitcast();
            v.store_slice(&mut d[at..at + n]);
        }
    }
}

#[cfg(test)]
#[path = "tests/simd.rs"]
mod tests;
