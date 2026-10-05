//! Morsel-oriented batch expression evaluator.
//!
//! `eval_batch` processes one morsel at a time (up to MORSEL rows), applying
//! all expression opcodes as columnar loops over register buffers. Base
//! pointers are hoisted outside the inner loop, which is what lets LLVM
//! auto-vectorize the arithmetic opcodes. The loops that end in packed bits or
//! start from them are [`crate::simd`]'s; the string kernels do not vectorize.

use std::cmp::Ordering;
use std::fmt::{self, Write as _};

use crate::chars::{char_count, char_offset, char_offset_back, reverse_chars};
use crate::program::{EmitWidth, FloatUnaryOp, IntArithOp, IntOrder, IntReg, IntUnaryOp, ScalarEmit, StrEmit};
use crate::search::{fields, find};
use crate::simd::{self, LanePred, Level};
use crate::{calendar, BatchView, CalendarOp, CmpOp, FloatArithOp, Instr, ResolvedProgram};
use gnitz_wire::{
    compare_german_strings, german_string_content, german_string_heap, german_string_inline, low_bits_mask,
    null_word_get, FixedInt,
};

/// Integer-cast bounds for the target type: the signed-source window `[lo, hi]`
/// and the unsigned-source ceiling `hi_u`. Only U64 needs the two to differ —
/// its `hi` is clamped to `i64::MAX` because the signed test runs in i64.
fn int_cast_bounds(fi: FixedInt) -> (i64, i64, u64) {
    let (lo, hi) = fi.range();
    (lo as i64, hi.min(i64::MAX as i128) as i64, hi as u64)
}

/// Float→int bounds as an f64 half-open window `[lo, hi)`. The upper bound is
/// exclusive at every width: after `trunc` the value is integral, so `t < 2^k`
/// is exactly `t <= 2^k - 1` — and it avoids naming `2^63-1`/`2^64-1`, neither
/// of which is representable in f64. Both bounds are then powers of two, so
/// the f64 conversion is exact.
fn float_to_int_bounds(fi: FixedInt) -> (f64, f64) {
    let (lo, hi) = fi.range();
    (lo as f64, (hi + 1) as f64)
}

pub(crate) const MORSEL: usize = 256;
const _: () = assert!(MORSEL.is_multiple_of(64), "a morsel owns whole bitmap words");
const NULL_WORDS_PER_REG: usize = MORSEL / 64;

/// Live bits of the last of `m.div_ceil(64)` words: all-ones when the morsel
/// fills it, so a tail mask is written unconditionally.
fn tail_mask(m: usize) -> u64 {
    low_bits_mask(m - m.div_ceil(64).saturating_sub(1) * 64)
}

// ---------------------------------------------------------------------------
// EvalScratch — the SoA register file for batch evaluation
// ---------------------------------------------------------------------------

/// One register: a lane per row, and a null bit and a truth bit per row.
///
/// On a cache-line boundary: lanes that start off a 32-byte one split the
/// loaders' stores and the kernels' loads across lines, which the filter
/// benches count as `ls_misal_loads`.
#[derive(Clone)]
#[repr(align(64))]
pub(crate) struct Reg {
    /// A lane at a NULL row holds whatever its kernel computed from the row's
    /// stored bytes: kernels run unconditionally to stay branch-free, so a
    /// consumer must read `lanes` against [`Self::nulls`], never alone.
    ///
    /// The lanes between a morsel's last row and the end of its last 64-row
    /// word hold no row: [`crate::simd`]'s kernels read and write them with
    /// the rest of the word. A string register leaves its lanes unused.
    lanes: [i64; MORSEL],
    /// All zero for the life of a `no_nulls` scratch, which writes none.
    nulls: [u64; NULL_WORDS_PER_REG],
    /// Packed truthy bits: what a boolean consumer reads a register through,
    /// on both arms. A producer writes whole words, so the bits past a
    /// morsel's last row are undefined.
    bools: [u64; NULL_WORDS_PER_REG],
}

pub(crate) struct EvalScratch {
    regs: Vec<Reg>,
    /// One view per row, per string register. Empty (capacity 0) unless the
    /// program has string instructions; a zero-length default view reads as
    /// `""`.
    str_views: Vec<[StrView; MORSEL]>,
    /// Computed string bytes — case folds, concatenations, numeric text —
    /// behind the program's string constants, which occupy a prefix the
    /// per-morsel reset does not clear. A view lives as long as the
    /// [`MorselOut`] borrowing it.
    ///
    /// The constant prefix is written once by [`EvalScratch::new`], so a scratch
    /// is tied to the one program it was built for. The evaluator owns both, so
    /// that pairing holds by construction.
    str_arena: Vec<u8>,
    /// The instruction set [`crate::simd`]'s kernels run at.
    level: Level,
}

/// String register `reg`'s views for a morsel's `m` rows, cut from the
/// flattened file: sliced off its own array a lane costs the view loops an
/// instruction a row, which the `str_len` and `str_like` shapes of
/// `expr_kernel_bench` measure.
#[inline(always)]
fn str_lane(views: &[[StrView; MORSEL]], reg: usize, m: usize) -> &[StrView] {
    &views.as_flattened()[reg * MORSEL..][..m]
}

#[inline(always)]
fn str_lane_mut(views: &mut [[StrView; MORSEL]], reg: usize, m: usize) -> &mut [StrView] {
    &mut views.as_flattened_mut()[reg * MORSEL..][..m]
}

/// `N` shared registers plus one mutable register of the same file. The one
/// unsafe split in this file, over the scalar registers and the string lanes
/// alike.
///
/// Sound because `d` is none of `srcs`: a register *is* the index of its writing
/// instruction, and `LogicalProgram::from_instrs` — which every constructor
/// routes through — rejects an operand not written before its reader. Duplicate
/// *sources* alias harmlessly. The `debug_assert`s restate both halves.
#[inline(always)]
fn split_regs<T, const N: usize>(buf: &mut [T], srcs: [u16; N], d: u16) -> ([&T; N], &mut T) {
    debug_assert!(!srcs.contains(&d), "split_regs: dst aliases a src register");
    debug_assert!(
        srcs.iter()
            .chain(std::iter::once(&d))
            .all(|&i| (i as usize) < buf.len()),
        "split_regs: a register past the scratch buffer",
    );
    let ptr = buf.as_mut_ptr();
    unsafe {
        (
            std::array::from_fn(|k| &*ptr.add(srcs[k] as usize)),
            &mut *ptr.add(d as usize),
        )
    }
}

impl EvalScratch {
    /// The whole register file for `prog`, sized and seeded once — every buffer
    /// is a function of the program alone, and a scratch belongs to exactly one
    /// evaluator, hence to one program.
    pub(crate) fn new(prog: &ResolvedProgram) -> Self {
        const ZERO: Reg = Reg {
            lanes: [0; MORSEL],
            nulls: [0; NULL_WORDS_PER_REG],
            bools: [0; NULL_WORDS_PER_REG],
        };
        let mut scratch = EvalScratch {
            regs: vec![ZERO; prog.num_regs()],
            str_views: vec![[StrView::default(); MORSEL]; prog.str_lanes as usize],
            str_arena: prog.const_arena.clone(),
            level: simd::level(),
        };
        scratch.install_consts(prog);
        scratch
    }

    /// Write the constant registers at full [`MORSEL`] width, once: a register
    /// *is* the index of the instruction that writes it, so no morsel can
    /// overwrite the lane.
    fn install_consts(&mut self, prog: &ResolvedProgram) {
        for &(dst, val) in &prog.const_regs {
            let r = &mut self.regs[dst as usize];
            r.lanes.fill(val);
            if prog.needs_bool_pack(dst as usize) {
                simd::truthy_bits(self.level, &r.lanes, &mut r.bools);
            }
        }
        for &(dst, off, len) in &prog.const_str_regs {
            self.str_views[dst as usize].fill(StrView { off: off as u64, len, src: SRC_ARENA });
        }
    }

    fn reg_mut(&mut self, reg: u16, m: usize) -> &mut [i64] {
        &mut self.regs[reg as usize].lanes[..m]
    }

    /// The first `m` lanes of `N` source registers and of one destination.
    /// Backs every unary opcode (`N = 1`), every binary one (`N = 2`).
    #[inline(always)]
    fn regs_split<const N: usize>(&mut self, srcs: [u16; N], d: u16, m: usize) -> ([&[i64]; N], &mut [i64]) {
        let (srcs, d) = split_regs(&mut self.regs, srcs, d);
        (srcs.map(|r| &r.lanes[..m]), &mut d.lanes[..m])
    }

    /// The same split over the null words.
    #[inline(always)]
    fn null_split<const N: usize>(&mut self, srcs: [u16; N], d: u16, words: usize) -> ([&[u64]; N], &mut [u64]) {
        let (srcs, d) = split_regs(&mut self.regs, srcs, d);
        (srcs.map(|r| &r.nulls[..words]), &mut d.nulls[..words])
    }

    /// This morsel's registers.
    #[inline(always)]
    pub(crate) fn morsel_out<'a>(&'a self, prog: &ResolvedProgram, bufs: StrBufs<'a>, m: usize) -> MorselOut<'a> {
        MorselOut {
            regs: &self.regs,
            no_nulls: prog.no_nulls,
            str_views: &self.str_views,
            str_arena: &self.str_arena,
            bufs,
            m,
        }
    }

    /// Zero the null bits for one register's morsel region.
    fn clear_null_reg(&mut self, mo: &Morsel<'_>, reg: u16) {
        if mo.no_nulls() {
            return;
        }
        self.regs[reg as usize].nulls[..mo.m.div_ceil(64)].fill(0);
    }
}

/// One morsel's results, read out of the register file.
pub(crate) struct MorselOut<'a> {
    regs: &'a [Reg],
    no_nulls: bool,
    str_views: &'a [[StrView; MORSEL]],
    str_arena: &'a [u8],
    bufs: StrBufs<'a>,
    m: usize,
}

impl MorselOut<'_> {
    /// How many rows this morsel covers (always ≥ 1).
    #[inline(always)]
    pub(crate) fn rows(&self) -> usize {
        self.m
    }

    /// Register `reg` as a filter verdict, one bit per row into `out`: set where
    /// the value is true and not NULL.
    pub(crate) fn filter_words(&self, reg: usize, out: &mut [u64]) {
        debug_assert_eq!(out.len(), self.m.div_ceil(64), "one bit per row");
        let r = &self.regs[reg];
        if self.no_nulls {
            out.copy_from_slice(&r.bools[..out.len()]);
        } else {
            for (w, word) in out.iter_mut().enumerate() {
                *word = r.bools[w] & !r.nulls[w];
            }
        }
        // Boolean producers write whole words.
        out[out.len() - 1] &= tail_mask(self.m);
    }

    /// Register `reg`'s values for this morsel's rows, in row order.
    #[inline(always)]
    pub(crate) fn reg_values(&self, reg: usize) -> &[i64] {
        &self.regs[reg].lanes[..self.m]
    }

    /// The same values as their little-endian byte image.
    #[inline(always)]
    pub(crate) fn reg_bytes(&self, reg: usize) -> &[u8] {
        gnitz_wire::as_le_bytes(self.reg_values(reg))
    }

    /// Write scalar emit `e` for this morsel's rows into its slot from `row0`:
    /// each value's low bytes, NULL rows zeroed with their bit set in `nb`.
    pub(crate) fn emit_scalar(&self, e: &ScalarEmit, (col, nb, _): (&mut [u8], &mut [u8], &mut Vec<u8>), row0: usize) {
        let w = e.width.bytes();
        let win = &mut col[row0 * w..(row0 + self.m) * w];
        let vals = self.reg_values(e.reg);
        match e.width {
            EmitWidth::W8 => win.copy_from_slice(self.reg_bytes(e.reg)),
            EmitWidth::W4 => Self::narrow_cells(win, vals, |v| (v as u32).to_le_bytes()),
            EmitWidth::W2 => Self::narrow_cells(win, vals, |v| (v as u16).to_le_bytes()),
            EmitWidth::W1 => Self::narrow_cells(win, vals, |v| (v as u8).to_le_bytes()),
        }
        self.write_null_rows(e.reg, win, w, nb, row0, e.slot);
    }

    /// Each value's low `W` bytes, as `to_bytes` truncates them, into its own cell.
    fn narrow_cells<const W: usize>(win: &mut [u8], vals: &[i64], to_bytes: impl Fn(i64) -> [u8; W]) {
        for (dst, v) in win.as_chunks_mut::<W>().0.iter_mut().zip(vals) {
            *dst = to_bytes(*v);
        }
    }

    /// Write string emit `e` for this morsel's rows into its slot from `row0`:
    /// one German-string cell per row, long bodies appended to the slot's heap.
    pub(crate) fn emit_str(&self, e: &StrEmit, (col, nb, blob): (&mut [u8], &mut [u8], &mut Vec<u8>), row0: usize) {
        let win = &mut col[row0 * 16..(row0 + self.m) * 16];
        self.write_str_cells(e.reg, win, blob);
        self.write_null_rows(e.reg, win, 16, nb, row0, e.slot);
    }

    fn write_str_cells(&self, reg: usize, win: &mut [u8], blob: &mut Vec<u8>) {
        for (i, cell) in win.as_chunks_mut::<16>().0.iter_mut().enumerate() {
            *cell = gnitz_wire::encode_german_string(self.str_bytes(reg, i), blob);
        }
    }

    /// String register `reg`'s bytes for row `i` of this morsel.
    #[inline(always)]
    pub(crate) fn str_bytes(&self, reg: usize, i: usize) -> &[u8] {
        debug_assert!(i < self.m, "str_bytes row {i} is outside the morsel's {} rows", self.m);
        view_bytes(self.str_views[reg][i], self.str_arena, self.bufs)
    }

    /// Zero each NULL row's `stride`-byte cell in `win` and set its bit
    /// `out_payload` in the row-major bitmap `nb`, whose rows start at `row0`.
    fn write_null_rows(
        &self,
        reg: usize,
        win: &mut [u8],
        stride: usize,
        nb: &mut [u8],
        row0: usize,
        out_payload: usize,
    ) {
        self.for_each_null_row(reg, |i| {
            win[i * stride..(i + 1) * stride].fill(0);
            let off = (row0 + i) * 8;
            let mut merged = gnitz_wire::read_u64_le(nb, off);
            gnitz_wire::null_word_set(&mut merged, out_payload, true);
            gnitz_wire::write_u64_le(nb, off, merged);
        });
    }

    /// Call `f(i)` for each of the morsel's rows where register `reg` is NULL.
    #[inline(always)]
    pub(crate) fn for_each_null_row(&self, reg: usize, mut f: impl FnMut(usize)) {
        if self.no_nulls {
            return;
        }
        let words = self.m.div_ceil(64);
        for (w, &word) in self.regs[reg].nulls[..words].iter().enumerate() {
            debug_assert!(
                w + 1 < words || self.m.is_multiple_of(64) || (word >> (self.m % 64)) == 0,
                "null_bits tail word has bits set beyond m={}",
                self.m,
            );
            let lo = w * 64;
            for bit in gnitz_wire::BitIter(word) {
                f(lo + bit);
            }
        }
    }
}

/// Collect every maximal run of set bits into `out`: one pass per word, and one
/// step per run inside it rather than per bit.
///
/// `rest` keeps the not-yet-scanned bits **in place** rather than shifting them
/// down, so no shift can reach 64 and every mask below is well-defined — which
/// is what lets an all-ones word and an empty one fall out of the same loop.
pub fn scan_filter_bits(bits: &[u64], n: usize, out: &mut Vec<(usize, usize)>) {
    // The start of a run that reached the top of the previous word. It continues
    // into this one only while the low bit is still set.
    let mut open: Option<usize> = None;
    for (w, &word) in bits.iter().enumerate() {
        let base = w * 64;
        if word & 1 == 0 {
            if let Some(s) = open.take() {
                out.push((s, base));
            }
        }
        let mut rest = word;
        while rest != 0 {
            let start_bit = rest.trailing_zeros() as usize;
            // Filling in below the run's start is what makes `trailing_ones`
            // measure the run rather than the gap under it.
            let end_bit = (rest | ((1u64 << start_bit) - 1)).trailing_ones() as usize;
            let start = open.take().unwrap_or(base + start_bit);
            if end_bit == 64 {
                open = Some(start); // reaches the top; the next word may continue it
                break;
            }
            out.push((start, base + end_bit));
            rest &= !((1u64 << end_bit) - 1);
        }
    }
    // A run still open past the last word ends at the batch, not at the bitmap:
    // `n` is not always a multiple of 64.
    if let Some(s) = open {
        out.push((s, n));
    }
}

// ---------------------------------------------------------------------------
// Morsel context
// ---------------------------------------------------------------------------

/// What every kernel helper reads besides its own operands: the program being
/// evaluated, the batch's null bitmap, and the morsel's row window. Assembled
/// once per [`eval_batch`] call and passed by shared reference, so a helper's
/// signature carries a destination register and its operands and nothing else.
struct Morsel<'a> {
    prog: &'a ResolvedProgram,
    null_bmp: &'a [u8],
    /// Absolute index of the morsel's first row in the batch.
    start: usize,
    /// Rows in the morsel (≤ [`MORSEL`]).
    m: usize,
}

impl Morsel<'_> {
    /// This morsel's null-bitmap rows, 8 bytes each. Cut once so the per-row
    /// bounds check is against the morsel, not the whole batch.
    fn null_words(&self) -> &[u8] {
        &self.null_bmp[self.start * 8..(self.start + self.m) * 8]
    }

    /// The program's nullability arm. The one copy of the verdict — every helper
    /// that branches on it reads it back through here, so a scratch sized for one
    /// arm can never be evaluated on the other.
    fn no_nulls(&self) -> bool {
        self.prog.no_nulls
    }
}

// ---------------------------------------------------------------------------
// Null-bit helpers
// ---------------------------------------------------------------------------

/// Propagate binary null: dst_null = a_null | b_null (word-at-a-time).
///
/// Kept apart from [`null_or_all`]; the `int_div` and `select` shapes of
/// `expr_kernel_bench` measure the split.
#[inline]
fn null_or2(s: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, a: u16, b: u16) {
    if mo.no_nulls() {
        return;
    }
    let words = mo.m.div_ceil(64);
    let ([na, nb], nd) = s.null_split([a, b], dst, words);
    for w in 0..words {
        nd[w] = na[w] | nb[w];
    }
}

/// Propagate null over any sequence of source registers: dst_null = OR of
/// theirs (word-at-a-time). Plain indexing rather than a split: the source count
/// is not a `const N`, so [`split_regs`] does not apply, and at four words
/// per morsel the bounds checks cost nothing that matters.
#[inline]
fn null_or_all(s: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, srcs: impl Iterator<Item = u16> + Clone) {
    if mo.no_nulls() {
        return;
    }
    let words = mo.m.div_ceil(64);
    for w in 0..words {
        let acc = srcs.clone().fold(0u64, |acc, r| acc | s.regs[r as usize].nulls[w]);
        s.regs[dst as usize].nulls[w] = acc;
    }
}

/// Propagate unary null: dst_null = src_null (word-at-a-time). `#[inline]` for
/// [`null_or2`]'s reason.
#[inline]
fn null_copy1(s: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, src: u16) {
    if mo.no_nulls() {
        return;
    }
    let ([ns], nd) = s.null_split([src], dst, mo.m.div_ceil(64));
    nd.copy_from_slice(ns);
}

/// Fill null bits into register `di` for the payload columns selected by
/// `cols` (bit N = payload slot N): a row is null iff any selected column is
/// null. A single-column caller passes `1 << pi`; the two-operand string
/// compare passes both columns' bits in one pass.
///
/// `NOT NULL` columns drop out here, against the program's `nullable_slots`, so
/// no column-reading opcode carries nullability of its own. Once every selected
/// column is `NOT NULL` the gather below could only write zero words, so it
/// collapses into a clear.
fn fill_null_bits_mask(s: &mut EvalScratch, di: u16, mo: &Morsel<'_>, cols: u64) {
    // The one null helper that legitimately runs on the fast arm — where the
    // gather it skips could only have written zero words.
    if mo.no_nulls() {
        debug_assert_eq!(cols & mo.prog.nullable_slots, 0, "no_nulls over a nullable column");
        return;
    }
    let mask = cols & mo.prog.nullable_slots;
    if mask == 0 {
        s.clear_null_reg(mo, di);
        return;
    }
    let rows = mo.null_words();
    simd::null_bits(s.level, rows, mask, &mut s.regs[di as usize].nulls[..mo.m.div_ceil(64)]);
}

/// `IS [NOT] NULL` (`invert` for IS NOT NULL): payload column `pi`'s bit of the
/// batch bitmap, per row, as a boolean in register `dst` that is never NULL.
fn eval_is_null(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, pi: u8, invert: bool) {
    let pi = pi as usize;
    // A destination read as a packed bit builds its word off the gather directly.
    if mo.prog.needs_bool_pack(dst as usize) {
        return is_null_packed(scratch, mo, dst, pi, invert);
    }
    scratch.clear_null_reg(mo, dst);
    fill_is_null_regs(scratch, mo, dst, pi, invert);
}

/// The value half of [`eval_is_null`]: one definite boolean per row in `regs`.
#[inline]
fn fill_is_null_regs(s: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, pi: usize, invert: bool) {
    let rows = mo.null_words();
    let rd = s.reg_mut(dst, mo.m);
    for (r, w) in rd.iter_mut().zip(rows.as_chunks::<8>().0) {
        *r = (null_word_get(u64::from_le_bytes(*w), pi) ^ invert) as i64;
    }
}

/// [`eval_is_null`]'s packed arm, kept out of line so the two arms above stay
/// the two loads that choose between them plus one loop.
#[inline(never)]
fn is_null_packed(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, pi: usize, invert: bool) {
    let words = mo.m.div_ceil(64);
    let flip = if invert { u64::MAX } else { 0 };
    let bools = &mut scratch.regs[dst as usize].bools[..words];
    simd::null_bits(scratch.level, mo.null_words(), 1u64 << pi, bools);
    for w in bools {
        *w ^= flip;
    }
    // Whole words; a verdict's reader masks past the last row.
    scratch.clear_null_reg(mo, dst);
    maybe_unpack_bool_to_regs(scratch, mo, dst);
}

/// `IS [NOT] NULL` over register `a`'s null lane, a definite boolean: the lane
/// *is* the packed boolean. Under `no_nulls` there is no lane and nothing is
/// NULL, so every row is the constant `invert`.
fn eval_is_null_reg(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, a: u16, invert: bool) {
    let flip = if invert { u64::MAX } else { 0 };
    if mo.no_nulls() {
        scratch.regs[dst as usize].bools[..mo.m.div_ceil(64)].fill(flip);
        return maybe_unpack_bool_to_regs(scratch, mo, dst);
    }
    for w in 0..mo.m.div_ceil(64) {
        scratch.regs[dst as usize].bools[w] = scratch.regs[a as usize].nulls[w] ^ flip;
        scratch.regs[dst as usize].nulls[w] = 0;
    }
    maybe_unpack_bool_to_regs(scratch, mo, dst);
}

/// `BoolBinary`'s word-level kernel, both selectors: three-valued over the
/// truth and null words, and plain AND / OR over the truth words under
/// `no_nulls`, which reads and writes no null word.
fn bool_and_or_word_loop(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, a: u16, b: u16, is_or: bool) {
    let words = mo.m.div_ceil(64);
    if mo.no_nulls() {
        let ([ra, rb], rd) = split_regs(&mut scratch.regs, [a, b], dst);
        for ((d, &va), &vb) in rd.bools[..words]
            .iter_mut()
            .zip(&ra.bools[..words])
            .zip(&rb.bools[..words])
        {
            *d = if is_or { va | vb } else { va & vb };
        }
        return;
    }
    for w in 0..words {
        let va = scratch.regs[a as usize].bools[w];
        let vb = scratch.regs[b as usize].bools[w];
        let na = scratch.regs[a as usize].nulls[w];
        let nb = scratch.regs[b as usize].nulls[w];
        let (result_bits, nd) = if is_or {
            // SQL 3VL OR: definite_true wherever a non-null side is true;
            // result-null only when neither side is definitely-true and at
            // least one side is null.
            let definite_true = (!na & va) | (!nb & vb);
            let nd = !definite_true & (na | nb);
            (definite_true, nd)
        } else {
            let definite_false = (!na & !va) | (!nb & !vb);
            let nd = !definite_false & (na | nb);
            let result_bits = !nd & va & vb;
            (result_bits, nd)
        };
        scratch.regs[dst as usize].bools[w] = result_bits;
        scratch.regs[dst as usize].nulls[w] = nd;
    }
}

/// Unpack `dst`'s packed bits into its lanes unless `dst` is bit_only, whose
/// readers take the packed bits directly. The counterpart to
/// [`maybe_pack_bool_bits`].
fn maybe_unpack_bool_to_regs(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16) {
    if mo.prog.is_bit_only(dst as usize) {
        return;
    }
    let words = mo.m.div_ceil(64);
    let r = &mut scratch.regs[dst as usize];
    simd::bit_lanes(scratch.level, &r.bools[..words], &mut r.lanes[..words * 64]);
}

/// Bridge for producers that wrote `dst`'s lanes and may have a downstream BOOL
/// consumer, a filter's verdict included. Non-bool producers reach a BOOL
/// consumer through this path without restructuring their inner loop.
///
/// The tests inline into the caller and the pack does not; the `map` shape of
/// `expr_kernel_bench` measures the split.
#[inline]
fn maybe_pack_bool_bits(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16) {
    if mo.prog.needs_bool_pack(dst as usize) {
        pack_bool_bits(scratch, mo.m, dst);
    }
}

fn pack_bool_bits(scratch: &mut EvalScratch, m: usize, dst: u16) {
    let words = m.div_ceil(64);
    let r = &mut scratch.regs[dst as usize];
    simd::truthy_bits(scratch.level, &r.lanes[..words * 64], &mut r.bools[..words]);
}

/// OR a per-row failure flag into `dst`'s null words. A byte per row, packed
/// here in a second pass: packing inside the value loop would read-modify-write
/// one mask word every iteration, which serialises it.
fn merge_fail_mask(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, bad: &[u8; MORSEL]) {
    // The usual morsel has no failure. A fold rather than `any`: with no early
    // exit it vectorizes.
    if bad.iter().fold(0u8, |acc, &b| acc | b) == 0 {
        return;
    }
    assert!(
        !mo.no_nulls(),
        "a kernel raised a failure flag under no_nulls: \
         its `Operands` entry does not set `makes_null`"
    );
    let mut failed = [0u64; NULL_WORDS_PER_REG];
    pack_bytes(bad, &mut failed);
    for (w, f) in failed.iter().enumerate().take(mo.m.div_ceil(64)) {
        scratch.regs[dst as usize].nulls[w] |= f;
    }
}

/// One bit per byte of `src`, each byte 0 or 1: eight bytes fold to eight bits
/// in one multiply.
fn pack_bytes(src: &[u8; MORSEL], out: &mut [u64]) {
    for (w, blk) in out.iter_mut().zip(src.as_chunks::<64>().0) {
        let mut bits = 0u64;
        for (k, g) in blk.as_chunks::<8>().0.iter().enumerate() {
            bits |= (u64::from_le_bytes(*g).wrapping_mul(0x0102_0408_1020_4080) >> 56) << (k * 8);
        }
        *w = bits;
    }
}

// ---------------------------------------------------------------------------
// String registers — views over the arena and the batch blob
// ---------------------------------------------------------------------------

/// One string register lane: a (buffer, offset, length) view.
///
/// Offsets rather than pointers, because `Vec` growth would dangle a pointer;
/// `u64` because the arena is bounded only by the data flowing through a morsel.
/// A `&[u8]` lane is not expressible at all — [`EvalScratch`] has no lifetime
/// parameter and outlives every batch it is driven over.
#[derive(Clone, Copy, Default)]
struct StrView {
    off: u64,
    len: u32,
    /// Which buffer `off` indexes — see [`StrBufs::region`]. A column load points
    /// a short cell at the column region and a long one at the blob, so this
    /// varies row to row within one lane on a mixed-width column: the dispatch in
    /// [`view_bytes_at`] is data-dependent, not hoistable.
    src: u32,
}

const SRC_ARENA: u32 = 0;
const SRC_BLOB: u32 = 1;
/// Buffer index of payload slot 0's string column region: a `LoadColStr` on
/// payload slot `pi` reads [`StrBufs::cols`]`[pi]` as `SRC_COL_BASE + pi`.
const SRC_COL_BASE: u32 = 2;

/// One buffer slot per payload slot — the cap the row-major null bitmap already
/// imposes. Indexing by the slot makes the table **total**: every string column
/// addresses its own region in place, with no per-row copy to fall back to.
const STR_COL_BUFS: usize = 64;

/// The buffers a [`StrView`]'s `src` indexes, minus the arena: the batch's blob
/// heap, then one region per payload slot.
///
/// A reference to a fixed-width array rather than a slice, with unloaded slots
/// left empty: every `src` is below [`STR_COL_BUFS`], so a slice's length could
/// only ever be re-loaded and re-checked per row. By reference because `StrBufs`
/// is `Copy` and rides inside [`MorselOut`].
#[derive(Clone, Copy)]
pub(crate) struct StrBufs<'a> {
    blob: &'a [u8],
    cols: &'a [&'a [u8]; STR_COL_BUFS],
}

impl<'a> StrBufs<'a> {
    /// The buffer `src` names, or `None` for the arena — the one buffer a kernel
    /// may be growing as it resolves a view, so it never lives here.
    #[inline]
    fn region(&self, src: u32) -> Option<&'a [u8]> {
        match src {
            SRC_ARENA => None,
            SRC_BLOB => Some(self.blob),
            _ => Some(self.cols[(src - SRC_COL_BASE) as usize]),
        }
    }
}

/// Resolve the string column regions `prog` holds views into and run `f` over
/// the buffer table. Once per batch, never per morsel: every region spans the
/// whole batch.
///
/// A scope function because the table is a fixed-width stack array whose borrow
/// has to outlive both [`eval_batch`] and the [`MorselOut`] built after it —
/// [`EvalScratch`] has no lifetime parameter to hold one, and `MorselOut`
/// borrows the scratch.
///
/// The table is filled whether or not the program loads a string column; the
/// string shapes of `expr_kernel_bench` measure that against a shared empty one.
pub(crate) fn with_str_bufs(prog: &ResolvedProgram, mb: &dyn BatchView, f: impl FnOnce(StrBufs<'_>)) {
    // The blob is read whatever `str_cols` says: `StrColConst` / `StrColCol`
    // reach it through their own operand without registering a column, so an
    // empty column mask is not "no blob reader".
    let blob = mb.blob();
    let mut cols: [&[u8]; STR_COL_BUFS] = [&[]; STR_COL_BUFS];
    for pi in gnitz_wire::BitIter(prog.str_cols) {
        cols[pi] = mb.col_data(pi, 16);
    }
    f(StrBufs { blob, cols: &cols })
}

impl StrView {
    /// A view over `[off, off + len)` of `src`'s buffer.
    fn at(src: u32, off: usize, len: usize) -> Self {
        StrView { off: off as u64, len: len as u32, src }
    }

    fn arena(off: usize, len: usize) -> Self {
        Self::at(SRC_ARENA, off, len)
    }

    /// `(offset, length)` in `src`'s buffer. A view is cut from a buffer that
    /// held it, and no buffer shrinks under a live view.
    fn span(self) -> (usize, usize) {
        (self.off as usize, self.len as usize)
    }
}

/// `v`'s bytes together with the offset they sit at; the sub-view producers
/// need both.
fn view_bytes_at<'x>(v: StrView, arena: &'x [u8], bufs: StrBufs<'x>) -> (&'x [u8], usize) {
    let (o, l) = v.span();
    (&bufs.region(v.src).unwrap_or(arena)[o..o + l], o)
}

fn view_bytes<'x>(v: StrView, arena: &'x [u8], bufs: StrBufs<'x>) -> &'x [u8] {
    view_bytes_at(v, arena, bufs).0
}

/// Append `v`'s bytes to the arena and return the appended span.
///
/// The arena→arena case goes through `extend_from_within`, not
/// `extend_from_slice`: the source borrows the very buffer being grown. Growth
/// never invalidates a view either way — views are offsets, not pointers.
#[inline]
fn arena_push_view(arena: &mut Vec<u8>, bufs: StrBufs<'_>, v: StrView) -> (usize, usize) {
    let start = arena.len();
    let (o, l) = v.span();
    arena_push_span(arena, bufs, v.src, o, l);
    (start, arena.len() - start)
}

/// Append an already-resolved span to the arena. Callers that need the extent
/// for their own arithmetic resolve it once and come here.
///
/// `#[inline]` because it is called per row from the concatenation kernels and
/// is smaller than its own call.
#[inline]
fn arena_push_span(arena: &mut Vec<u8>, bufs: StrBufs<'_>, src: u32, o: usize, l: usize) {
    match bufs.region(src) {
        Some(buf) => arena.extend_from_slice(&buf[o..o + l]),
        None => arena.extend_from_within(o..o + l),
    }
}

/// A 16-byte German-string cell at byte `o` of buffer `buf` as a view, no byte
/// copied: a long cell points into the blob, a short one at its own inline bytes.
/// A corrupt long cell reads as the empty string.
fn cell_to_view(cell: &[u8; 16], o: usize, buf: u32, blob_len: usize) -> StrView {
    match german_string_inline(cell) {
        Some(inline) => StrView::at(buf, o + gnitz_wire::GERMAN_INLINE_OFF, inline.len()),
        None => heap_view(cell, blob_len),
    }
}

/// A long cell's blob view, degrading a corrupt header to the empty string —
/// `gnitz_wire::german_string_content`'s convention, never a panic.
fn heap_view(cell: &[u8], blob_len: usize) -> StrView {
    match german_string_heap(cell, blob_len) {
        Some(r) => StrView::at(SRC_BLOB, r.start, r.len()),
        None => StrView::default(),
    }
}

fn in_trim_set(set: &[u64; 4], b: u8) -> bool {
    (set[(b >> 6) as usize] >> (b & 63)) & 1 != 0
}

impl IntReg {
    /// The register's first `m` rows.
    #[inline]
    fn lane(self, regs: &[Reg], m: usize) -> IntLane<'_> {
        IntLane {
            rows: &regs[self.reg as usize].lanes[..m],
            signed: self.signed,
        }
    }
}

/// One [`IntReg`]'s rows in a morsel.
#[derive(Clone, Copy)]
struct IntLane<'r> {
    rows: &'r [i64],
    signed: bool,
}

impl IntLane<'_> {
    /// Row `i`, widened. Each operand's magnitude is ≤ 2^64, so the sum of two
    /// cannot overflow.
    #[inline]
    fn get(self, i: usize) -> i128 {
        let v = self.rows[i];
        if self.signed {
            v as i128
        } else {
            v as u64 as i128
        }
    }
}

/// ASCII decimal with surrounding whitespace and an optional sign, and nothing
/// else — no base prefix, no digit separator, no fractional part. `i128::from_str`
/// is that grammar exactly, and it saturates nothing: an overlong digit string is
/// `None` rather than a wrapped value.
fn parse_decimal_i128(s: &[u8]) -> Option<i128> {
    std::str::from_utf8(s.trim_ascii()).ok()?.parse().ok()
}

/// The arena as a `fmt::Write` sink, so a number's decimal text is formatted
/// straight into its final position — no intermediate buffer and no copy. Its
/// `write_str` cannot fail, which is why the `write!` results below are dropped.
struct ArenaText<'a>(&'a mut Vec<u8>);

impl fmt::Write for ArenaText<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

/// Format one number into the arena and return the view over its text.
fn arena_push_text(arena: &mut Vec<u8>, args: fmt::Arguments<'_>) -> StrView {
    let off = arena.len();
    let _ = ArenaText(arena).write_fmt(args);
    StrView::arena(off, arena.len() - off)
}

/// An integer's decimal text, written straight into the arena. `core::fmt` would
/// build an `Arguments`, call out-of-line through `Display` and
/// `Formatter::pad_integral`'s statically-empty width/precision checks, and reach
/// the arena through an indirect `&mut dyn fmt::Write` — several times this loop
/// for a value the caller has already split into sign and magnitude.
fn arena_push_int(arena: &mut Vec<u8>, magnitude: u64, neg: bool) -> StrView {
    let off = arena.len();
    if neg {
        arena.push(b'-');
    }
    // Digits fall out least-significant first, so they are staged and reversed
    // in. `u64::MAX` is 20 digits.
    let mut digits = [0u8; 20];
    let mut n = 0;
    let mut x = magnitude;
    loop {
        digits[n] = b'0' + (x % 10) as u8;
        n += 1;
        x /= 10;
        if x == 0 {
            break;
        }
    }
    for &d in digits[..n].iter().rev() {
        arena.push(d);
    }
    StrView::arena(off, arena.len() - off)
}

/// A float's decimal text: shortest round-trip, switched to scientific notation
/// outside `[1e-4, 1e15)`. The switch is what bounds the output — Rust's
/// positional `Display` is unbounded, rendering `1e300` as 301 digits and
/// `5e-324` as 326. Non-finite values are spelled as PostgreSQL spells them,
/// rather than Rust's `inf`.
fn arena_push_float(arena: &mut Vec<u8>, v: f64) -> StrView {
    if !v.is_finite() {
        let name = if v.is_nan() {
            "NaN"
        } else if v > 0.0 {
            "Infinity"
        } else {
            "-Infinity"
        };
        return arena_push_text(arena, format_args!("{name}"));
    }
    if v == 0.0 || (1e-4..1e15).contains(&v.abs()) {
        arena_push_text(arena, format_args!("{v}"))
    } else {
        arena_push_text(arena, format_args!("{v:e}"))
    }
}

/// SELECT's branch choice, and the null mask that follows from it — the half
/// that does not depend on which lane array holds the value, so the scalar and
/// string arms share it.
///
/// Returns the per-row "take `a`" bits: `cond` truthy AND non-null. `cond` may be
/// bit_only (its producer skips the unpack to lanes), so its truthiness comes
/// from its packed bits, not its lanes. `dst`'s null word is written here:
/// `dst` is null wherever the chosen branch is null. Under `no_nulls` the mask
/// is `cond`'s bits and no null word is written.
fn select_take_mask(
    s: &mut EvalScratch,
    mo: &Morsel<'_>,
    dst: u16,
    cond: u16,
    a: u16,
    b: u16,
) -> [u64; NULL_WORDS_PER_REG] {
    let words = mo.m.div_ceil(64);
    let mut take_a = [0u64; NULL_WORDS_PER_REG];
    if mo.no_nulls() {
        take_a[..words].copy_from_slice(&s.regs[cond as usize].bools[..words]);
        return take_a;
    }
    for (w, t) in take_a.iter_mut().enumerate().take(words) {
        *t = s.regs[cond as usize].bools[w] & !s.regs[cond as usize].nulls[w];
    }
    let ([na, nb], nd) = s.null_split([a, b], dst, words);
    for w in 0..words {
        nd[w] = (take_a[w] & na[w]) | (!take_a[w] & nb[w]);
    }
    take_a
}

/// Set the null bit of every live row of `dst`, masking the tail word so stale
/// high bits never read as null. The inverse of [`EvalScratch::clear_null_reg`],
/// and what backs both `LoadNull` opcodes.
///
/// No `no_nulls` arm: a result's reader skips the null words there, so this
/// panics rather than losing a NULL.
fn set_null_reg(s: &mut EvalScratch, mo: &Morsel<'_>, dst: u16) {
    assert!(
        !mo.no_nulls(),
        "set_null_reg under no_nulls: its caller must set `makes_null`"
    );
    let m = mo.m;
    let words = m.div_ceil(64);
    s.regs[dst as usize].nulls[..words].fill(u64::MAX);
    s.regs[dst as usize].nulls[words - 1] = tail_mask(m);
}

/// Measure every row of string register `a` into scalar register `dst`. `f` is
/// total — the operand's null bit is the only NULL, so no fail mask is packed —
/// and runs over NULL rows too, on whatever view they carry, keeping the loop
/// branch-free.
fn str_to_scalar(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    bufs: StrBufs<'_>,
    dst: u16,
    a: u16,
    mut f: impl FnMut(&[u8]) -> i64,
) {
    let (d, ai) = (dst as usize, a as usize);
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        // Both windows cut to the morsel: the destination already was, and
        // relating the two is what lets LLVM drop the source's per-row bound.
        let (va, rd) = (str_lane(str_views, ai, mo.m), &mut regs[d].lanes[..mo.m]);
        for (i, r) in rd.iter_mut().enumerate() {
            *r = f(view_bytes(va[i], str_arena, bufs));
        }
    }
    null_copy1(scratch, mo, dst, a);
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// The byte length of every row of string register `a` into scalar register
/// `dst`, read off the views: every producer writes a view that lies inside its
/// buffer, so `len` is the length resolving it would report.
fn str_byte_len(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, a: u16) {
    {
        let EvalScratch { regs, str_views, .. } = &mut *scratch;
        let va = str_lane(str_views, a as usize, mo.m);
        let rd = &mut regs[dst as usize].lanes[..mo.m];
        for (r, v) in rd.iter_mut().zip(va) {
            *r = v.len as i64;
        }
    }
    null_copy1(scratch, mo, dst, a);
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// Parse every row of string register `a` into scalar register `dst`. `f`
/// returns `None` for an unparsable or out-of-range value, which becomes a NULL
/// — the [`unary_null_like`] shape over a view instead of a register.
fn str_parse_to_scalar(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    bufs: StrBufs<'_>,
    dst: u16,
    a: u16,
    f: impl Fn(&[u8]) -> Option<i64>,
) {
    let (d, ai) = (dst as usize, a as usize);
    let mut bad = [0u8; MORSEL];
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        let (va, rd) = (str_lane(str_views, ai, mo.m), &mut regs[d].lanes[..mo.m]);
        for (i, r) in rd.iter_mut().enumerate() {
            match f(view_bytes(va[i], str_arena, bufs)) {
                Some(v) => *r = v,
                None => {
                    *r = 0;
                    bad[i] = 1;
                }
            }
        }
    }
    null_copy1(scratch, mo, dst, a);
    merge_fail_mask(scratch, mo, dst, &bad);
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// Render every row of scalar register `a` into string register `dst` via `f`,
/// then propagate the operand's null bit. The three numeric→text opcodes differ
/// only in `f`.
fn num_to_str(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, a: u16, f: impl Fn(&mut Vec<u8>, i64) -> StrView) {
    let (d, ai, m) = (dst as usize, a as usize, mo.m);
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        let (ra, vd) = (&regs[ai].lanes[..m], str_lane_mut(str_views, d, m));
        for (i, r) in vd.iter_mut().enumerate() {
            *r = f(str_arena, ra[i]);
        }
    }
    null_copy1(scratch, mo, dst, a);
}

/// Combine every row of string registers `a` and `b` into scalar register
/// `dst`. `f` is total — the operands' null bits are the only NULL — and runs
/// over NULL rows too, on whatever views they carry, keeping the loop
/// branch-free: the compares, and STRPOS.
fn str2_to_scalar(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    bufs: StrBufs<'_>,
    dst: u16,
    a: u16,
    b: u16,
    f: impl Fn(&[u8], &[u8]) -> i64,
) {
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        // All three windows cut to the morsel: indexing the lane `Vec` directly
        // costs two bounds checks and two counter bumps per row.
        let (sa, sb) = (
            str_lane(str_views, a as usize, mo.m),
            str_lane(str_views, b as usize, mo.m),
        );
        let rd = &mut regs[dst as usize].lanes[..mo.m];
        for ((r, &va), &vb) in rd.iter_mut().zip(sa).zip(sb) {
            *r = f(view_bytes(va, str_arena, bufs), view_bytes(vb, str_arena, bufs));
        }
    }
    null_or2(scratch, mo, dst, a, b);
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// Produce every row of string register `dst` from `S` string and `I` integer
/// operands. `f` returns the result view — a sub-view of an operand, or a fresh
/// arena copy — or `None` for a NULL of the kernel's own, which is why the
/// opcode must set `makes_null`.
fn str_kernel<const S: usize, const I: usize>(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    bufs: StrBufs<'_>,
    dst: u16,
    strs: [u16; S],
    ints: [IntReg; I],
    f: impl Fn(&mut Vec<u8>, StrBufs<'_>, [StrView; S], [i128; I]) -> Option<StrView>,
) {
    let mut bad = [0u8; MORSEL];
    str_kernel_rows(scratch, mo, bufs, dst, strs, ints, |arena, bufs, views, nums, i| {
        f(arena, bufs, views, nums).unwrap_or_else(|| {
            bad[i] = 1;
            StrView::default()
        })
    });
    merge_fail_mask(scratch, mo, dst, &bad);
}

/// [`str_kernel`] for a closure that cannot fail: no fail mask is written or
/// scanned.
fn str_kernel_total<const S: usize, const I: usize>(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    bufs: StrBufs<'_>,
    dst: u16,
    strs: [u16; S],
    ints: [IntReg; I],
    f: impl Fn(&mut Vec<u8>, StrBufs<'_>, [StrView; S], [i128; I]) -> StrView,
) {
    str_kernel_rows(scratch, mo, bufs, dst, strs, ints, |arena, bufs, views, nums, _| {
        f(arena, bufs, views, nums)
    });
}

/// The row loop and operand null propagation both string-producing kernels
/// share. `f` also gets the row index, which is all the fallible one needs it
/// for.
#[inline]
fn str_kernel_rows<const S: usize, const I: usize>(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    bufs: StrBufs<'_>,
    dst: u16,
    strs: [u16; S],
    ints: [IntReg; I],
    mut f: impl FnMut(&mut Vec<u8>, StrBufs<'_>, [StrView; S], [i128; I], usize) -> StrView,
) {
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        let (srcs, vd) = split_regs(str_views, strs, dst);
        let (srcs, vd) = (srcs.map(|s| &s[..mo.m]), &mut vd[..mo.m]);
        let lanes = ints.map(|r| r.lane(regs, vd.len()));
        for (i, r) in vd.iter_mut().enumerate() {
            let views = srcs.map(|w| w[i]);
            let nums = lanes.map(|l| l.get(i));
            *r = f(str_arena, bufs, views, nums, i);
        }
    }
    null_or_all(scratch, mo, dst, strs.iter().copied().chain(ints.iter().map(|r| r.reg)));
}

/// `Select`: rows where `cond` is true and not NULL take `a`'s value and null
/// bit, every other row `b`'s.
fn eval_select(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, cond: u16, a: u16, b: u16) {
    let take_a = select_take_mask(scratch, mo, dst, cond, a, b);
    let (level, words) = (scratch.level, mo.m.div_ceil(64));
    {
        let ([ra, rb], rd) = scratch.regs_split([a, b], dst, words * 64);
        simd::blend(level, &take_a[..words], ra, rb, rd);
    }
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// [`eval_select`] over string lanes instead of scalar registers.
fn eval_str_select(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, cond: u16, a: u16, b: u16) {
    let take_a = select_take_mask(scratch, mo, dst, cond, a, b);
    let ([sa, sb], sd) = split_regs(&mut scratch.str_views, [a, b], dst);
    let (sa, sb, sd) = (&sa[..mo.m], &sb[..mo.m], &mut sd[..mo.m]);
    for ((sd, (sa, sb)), &word) in sd.chunks_mut(64).zip(sa.chunks(64).zip(sb.chunks(64))).zip(&take_a) {
        for (j, (v, (&x, &y))) in sd.iter_mut().zip(sa.iter().zip(sb)).enumerate() {
            *v = if (word >> j) & 1 != 0 { x } else { y };
        }
    }
}

/// SUBSTRING: a sub-view of the source, the bytes never copied.
///
/// Kept apart from [`str_kernel`], whose `<1, 2>` case this is; the
/// `str_substr` shape of `expr_kernel_bench` measures the split.
fn eval_str_substr(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    bufs: StrBufs<'_>,
    d: u16,
    si: u16,
    start: IntReg,
    len: Option<IntReg>,
) {
    let m = mo.m;
    let mut bad = [0u8; MORSEL];
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        let (start, len) = (start.lane(regs, m), len.map(|l| l.lane(regs, m)));
        let ([sv], dv) = split_regs(str_views, [si], d);
        let (sv, dv) = (&sv[..m], &mut dv[..m]);
        for i in 0..m {
            let v = sv[i];
            let (s, base_off) = view_bytes_at(v, str_arena, bufs);
            // 0-based character indices, clamped to the byte length, which
            // bounds the character count.
            let n = s.len() as i128;
            let lo = start.get(i) - 1;
            let hi = match len {
                Some(l) => {
                    let len = l.get(i);
                    bad[i] = (len < 0) as u8;
                    lo + len
                }
                None => n,
            };
            let (t_lo, t_hi) = (lo.clamp(0, n) as usize, hi.clamp(0, n) as usize);
            dv[i] = if t_lo >= t_hi {
                StrView::default()
            } else {
                let b_lo = char_offset(s, t_lo);
                // No character ends past the byte length.
                let b_hi = if t_hi == s.len() {
                    s.len()
                } else {
                    b_lo + char_offset(&s[b_lo..], t_hi - t_lo)
                };
                StrView::at(v.src, base_off + b_lo, b_hi - b_lo)
            };
        }
    }
    // The fail flag is a negative length, so the no-FOR form has none — matching
    // its `Operands` entry, which leaves `makes_null` unset.
    match len {
        Some(l) => {
            null_or_all(scratch, mo, d, [si, start.reg, l.reg].into_iter());
            merge_fail_mask(scratch, mo, d, &bad);
        }
        None => null_or2(scratch, mo, d, si, start.reg),
    }
}

/// `||` and the CONCAT fold step. `skip_null` is CONCAT's asymmetric rule: a
/// NULL `b` contributes the empty string, a NULL `a` propagates.
fn eval_str_concat(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    bufs: StrBufs<'_>,
    d: u16,
    a: u16,
    b: u16,
    skip_null: bool,
) {
    // `StrConcat` sets `makes_null` (a combined length above `u32::MAX`), so the
    // null words read below are maintained.
    debug_assert!(
        !mo.no_nulls(),
        "eval_str_concat under no_nulls: `StrConcat` must set `makes_null`"
    );
    let m = mo.m;
    let mut bad = [0u8; MORSEL];
    {
        let EvalScratch { str_views, str_arena, regs, .. } = &mut *scratch;
        let ([sa, sb], sd) = split_regs(str_views, [a, b], d);
        let (sa, sb, sd) = (&sa[..m], &sb[..m], &mut sd[..m]);
        for i in 0..m {
            let va = sa[i];
            // Under CONCAT's rule a NULL argument contributes the empty string,
            // whatever view its lane happens to carry.
            let b_is_null = skip_null && (regs[b as usize].nulls[i / 64] >> (i % 64)) & 1 != 0;
            let vb = if b_is_null { StrView::default() } else { sb[i] };
            // Each operand's extent is resolved once and reused for both the
            // length sum and the copy. Widened before summing: two near-u32::MAX
            // operands wrap in u32 space, and an oversized length would trip
            // `encode_german_string`'s release assert — a worker abort, which the
            // totality rule forbids.
            let (oa, la) = va.span();
            let (ob, lb) = vb.span();
            let total = la as u64 + lb as u64;
            if total > u32::MAX as u64 {
                bad[i] = 1;
                sd[i] = StrView::default();
                continue;
            }
            let o = str_arena.len();
            arena_push_span(str_arena, bufs, va.src, oa, la);
            arena_push_span(str_arena, bufs, vb.src, ob, lb);
            sd[i] = StrView {
                off: o as u64,
                len: total as u32,
                src: SRC_ARENA,
            };
        }
    }
    if skip_null {
        null_copy1(scratch, mo, d, a);
    } else {
        null_or2(scratch, mo, d, a, b);
    }
    merge_fail_mask(scratch, mo, d, &bad);
}

// ---------------------------------------------------------------------------
// String comparison — one kernel over two interchangeable operands
// ---------------------------------------------------------------------------

/// Where a compare operand's cell for morsel row `i` comes from.
trait Cells: Copy {
    fn cell(&self, i: usize) -> &[u8; 16];
    /// Every row's cell, in row order.
    fn iter(&self) -> impl Iterator<Item = &[u8; 16]>;
}

/// A payload column's cells over the morsel.
#[derive(Clone, Copy)]
struct ColumnCells<'a>(&'a [[u8; 16]]);

impl Cells for ColumnCells<'_> {
    fn cell(&self, i: usize) -> &[u8; 16] {
        &self.0[i]
    }
    fn iter(&self) -> impl Iterator<Item = &[u8; 16]> {
        self.0.iter()
    }
}

/// A resolved constant: one cell for every row.
#[derive(Clone, Copy)]
struct ConstCell<'a>(&'a [u8; 16]);

impl Cells for ConstCell<'_> {
    fn cell(&self, _: usize) -> &[u8; 16] {
        self.0
    }
    fn iter(&self) -> impl Iterator<Item = &[u8; 16]> {
        std::iter::repeat(self.0)
    }
}

/// One side of a German-string compare: its cells, the heap they point into, and
/// the null bit that makes the *result* null.
#[derive(Clone, Copy)]
struct StrOperand<'a, C> {
    cells: C,
    blob: &'a [u8],
    null_bit: u64,
}

impl<'a> StrOperand<'a, ColumnCells<'a>> {
    fn column(mb: &'a dyn BatchView, blob: &'a [u8], pi_byte: u8, mo: &Morsel<'_>) -> Self {
        let pi = pi_byte as usize;
        let rows = &mb.col_data(pi, 16)[mo.start * 16..][..mo.m * 16];
        StrOperand {
            cells: ColumnCells(rows.as_chunks::<16>().0),
            blob,
            null_bit: 1u64 << pi,
        }
    }
}

impl<'a> StrOperand<'a, ConstCell<'a>> {
    fn constant(prog: &'a ResolvedProgram, const_idx: usize) -> Self {
        StrOperand {
            cells: ConstCell(&prog.const_cells[const_idx]),
            blob: &prog.const_arena,
            null_bit: 0,
        }
    }
}

/// The ordering string-compare kernel. The compare runs unconditionally, including on
/// rows where either operand's column is null, keeping the loop branch-free.
/// `str_const_filter_bench` measures the inlining.
#[inline(always)]
fn eval_str_cmp<A: Cells, B: Cells>(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    dst: u16,
    a: StrOperand<'_, A>,
    b: StrOperand<'_, B>,
    pred: impl Fn(Ordering) -> bool,
) {
    fill_null_bits_mask(scratch, dst, mo, a.null_bit | b.null_bit);
    let rd = &mut scratch.regs[dst as usize].lanes[..mo.m];
    for (i, r) in rd.iter_mut().enumerate() {
        *r = pred(compare_german_strings(a.cells.cell(i), a.blob, b.cells.cell(i), b.blob)) as i64;
    }
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// A cell's two words: its length under its prefix, then its suffix or heap
/// offset.
#[inline(always)]
fn cell_words(cell: &[u8; 16]) -> (u64, u64) {
    (
        u64::from_le_bytes(cell[..8].try_into().unwrap()),
        u64::from_le_bytes(cell[8..].try_into().unwrap()),
    )
}

/// String equality, `<>` when `ne`, read off the cells. Two canonical cells
/// hold one string iff their first words agree and, short, their second words
/// do — or, long, their heap bytes. One branch-free pass settles every short
/// row and marks the long rows whose length and prefix agree; only those reach
/// the heap. Runs over NULL rows too, as [`eval_str_cmp`] does.
#[inline(always)]
fn eval_str_eq<A: Cells, B: Cells>(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    dst: u16,
    a: StrOperand<'_, A>,
    b: StrOperand<'_, B>,
    ne: bool,
) {
    fill_null_bits_mask(scratch, dst, mo, a.null_bit | b.null_bit);
    let (mut eq, mut long_match) = ([0u8; MORSEL], [0u8; MORSEL]);
    let cells = a.cells.iter().zip(b.cells.iter()).take(mo.m);
    for ((eq, long_match), (ca, cb)) in eq.iter_mut().zip(&mut long_match).zip(cells) {
        let ((a0, a1), (b0, b1)) = (cell_words(ca), cell_words(cb));
        let long = a0 as u32 > gnitz_wire::SHORT_STRING_THRESHOLD as u32;
        *eq = ((a0 == b0) & (a1 == b1) & !long) as u8;
        *long_match = ((a0 == b0) & long) as u8;
    }
    let words = mo.m.div_ceil(64);
    let mut eq_bits = [0u64; NULL_WORDS_PER_REG];
    pack_bytes(&eq, &mut eq_bits);
    // A fold rather than `any`: with no early exit it vectorizes.
    if long_match.iter().fold(0u8, |acc, &c| acc | c) != 0 {
        let mut pending = [0u64; NULL_WORDS_PER_REG];
        pack_bytes(&long_match, &mut pending);
        for (w, &word) in pending.iter().enumerate().take(words) {
            for j in gnitz_wire::BitIter(word) {
                let (ca, cb) = (a.cells.cell(w * 64 + j), b.cells.cell(w * 64 + j));
                let same = german_string_content(ca, a.blob) == german_string_content(cb, b.blob);
                eq_bits[w] |= (same as u64) << j;
            }
        }
    }
    let flip = if ne { u64::MAX } else { 0 };
    for (w, &bits) in eq_bits.iter().enumerate().take(words) {
        // Whole words; a verdict's reader masks past the last row.
        scratch.regs[dst as usize].bools[w] = bits ^ flip;
    }
    maybe_unpack_bool_to_regs(scratch, mo, dst);
}

/// Reinterpret a register's raw i64 bits as the `f64` they encode, and back.
/// `pub(crate)` so [`crate::ExprBuilder::const_f64`] and the tests reach the
/// register file's own codec rather than a second spelling of it.
pub(crate) fn decode_f64(bits: i64) -> f64 {
    f64::from_bits(bits as u64)
}
pub(crate) fn encode_f64(f: f64) -> i64 {
    f64::to_bits(f) as i64
}

// ---------------------------------------------------------------------------
// Register-loop shapes — one function per per-row calling convention
// ---------------------------------------------------------------------------
//
// Each takes the per-row body as a closure. `#[inline]` is what splices that
// closure into the loop rather than calling it: `[profile.dev]` leaves
// `codegen-units = 256`, and an instantiation landing in a different CGU from
// its caller cannot be inlined without the attribute.

/// Every binary arithmetic/comparison opcode shares one shape: read two source
/// registers, write one, OR the source null words, repack bool bits. Bodies must
/// stay branch-free; float ops reinterpret register bits via
/// `decode_f64`/`encode_f64`. [`div_like`] stays separate: it additionally
/// merges a zero-divisor mask into the destination null word.
#[inline]
fn bin_op(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, b: u16, f: impl Fn(i64, i64) -> i64) {
    {
        let ([ra, rb], rd) = scratch.regs_split([a, b], d, mo.m);
        for ((r, &x), &y) in rd.iter_mut().zip(ra).zip(rb) {
            *r = f(x, y);
        }
    }
    null_or2(scratch, mo, d, a, b);
    maybe_pack_bool_bits(scratch, mo, d);
}

/// [`bin_op`] for a predicate. A bit_only destination takes its packed bits
/// straight off the operands and no lane is written; any other gets its lanes
/// from the scalar loop, which the `cmp_value` shapes of `expr_kernel_bench`
/// measure against packing and unpacking.
///
/// `#[inline(always)]`: left to LLVM some instantiations are called, a call a
/// compare a morsel; the `nullable` and `literals` shapes of
/// `filter_kernel_bench` measure it.
#[inline(always)]
fn bin_pred<P: LanePred>(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, b: u16) {
    if !mo.prog.is_bit_only(d as usize) {
        return bin_op(scratch, mo, d, a, b, |x, y| P::scalar(x, y) as i64);
    }
    let words = mo.m.div_ceil(64);
    {
        let level = scratch.level;
        let ([ra, rb], rd) = split_regs(&mut scratch.regs, [a, b], d);
        simd::pred_bits::<P>(
            level,
            &ra.lanes[..words * 64],
            &rb.lanes[..words * 64],
            &mut rd.bools[..words],
        );
    }
    null_or2(scratch, mo, d, a, b);
}

/// Unary counterpart of [`bin_op`]: read one source register, write one, copy
/// the source null word, repack bool bits.
#[inline]
fn un_op(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, f: impl Fn(i64) -> i64) {
    {
        let ([ra], rd) = scratch.regs_split([a], d, mo.m);
        for (r, &x) in rd.iter_mut().zip(ra) {
            *r = f(x);
        }
    }
    null_copy1(scratch, mo, d, a);
    maybe_pack_bool_bits(scratch, mo, d);
}

/// The largest IN set scanned a value at a time, and only by a morsel of at
/// least as many rows: a scan compares every lane of the morsel's words with
/// every value, where a search probes once per row. The `in*` shapes of
/// `filter_kernel_bench` and `expr_kernel_bench` place both, the second under
/// `GNITZ_BENCH_ROWS`.
const IN_SET_BITS_MAX: usize = 64;

/// `a IN set` by scanning the set: the packed bits straight off the operand.
fn in_set_bits(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, set: &[i64]) {
    let words = mo.m.div_ceil(64);
    {
        let level = scratch.level;
        let ([ra], rd) = split_regs(&mut scratch.regs, [a], d);
        simd::in_set_bits(level, &ra.lanes[..words * 64], set, &mut rd.bools[..words]);
    }
    null_copy1(scratch, mo, d, a);
    maybe_unpack_bool_to_regs(scratch, mo, d);
}

/// DIV-shaped op: compute per-row, mark divide-by-zero rows null, substitute a
/// safe divisor so the computed slot holds a defined (irrelevant) value. `f`
/// returns `(result, is_zero)`. Flags go a byte per row to [`merge_fail_mask`],
/// which states why.
#[inline]
fn div_like(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, b: u16, f: impl Fn(i64, i64) -> (i64, bool)) {
    let mut bad = [0u8; MORSEL];
    {
        let ([ra, rb], rd) = scratch.regs_split([a, b], d, mo.m);
        for (((r, &x), &y), flag) in rd.iter_mut().zip(ra).zip(rb).zip(&mut bad) {
            let (val, is_zero) = f(x, y);
            *r = val;
            *flag = is_zero as u8;
        }
    }
    null_or2(scratch, mo, d, a, b);
    merge_fail_mask(scratch, mo, d, &bad);
    maybe_pack_bool_bits(scratch, mo, d);
}

/// `IntArith`'s `Div` (`MOD = false`) and `Mod`, over the four
/// (operator × signedness) whole loops — neither branch is inside the row loop.
/// A zero divisor is substituted with 1 and the row marked NULL, so `wrapping_*`
/// never divides by zero and `i64::MIN / -1` wraps rather than trapping.
#[inline]
fn int_divmod<const MOD: bool>(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, b: u16, signed: bool) {
    if signed {
        div_like(scratch, mo, d, a, b, |x, y| {
            let is_zero = y == 0;
            let dd = if is_zero { 1 } else { y };
            (if MOD { x.wrapping_rem(dd) } else { x.wrapping_div(dd) }, is_zero)
        })
    } else {
        div_like(scratch, mo, d, a, b, |x, y| {
            let is_zero = y == 0;
            let dd = if is_zero { 1u64 } else { y as u64 };
            let x = x as u64;
            (
                (if MOD { x.wrapping_rem(dd) } else { x.wrapping_div(dd) }) as i64,
                is_zero,
            )
        })
    }
}

/// Unary counterpart of [`div_like`]: compute every row unconditionally, then
/// mark the failures NULL. `f` returns `(value, failed)`; the value is always
/// defined, so a failed row never leaves uninitialised bits behind.
#[inline]
fn unary_null_like(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, f: impl Fn(i64) -> (i64, bool)) {
    let mut bad = [0u8; MORSEL];
    {
        let ([ra], rd) = scratch.regs_split([a], d, mo.m);
        for ((r, &x), flag) in rd.iter_mut().zip(ra).zip(&mut bad) {
            let (val, failed) = f(x);
            *r = val;
            *flag = failed as u8;
        }
    }
    null_copy1(scratch, mo, d, a);
    merge_fail_mask(scratch, mo, d, &bad);
    maybe_pack_bool_bits(scratch, mo, d);
}

/// Null-skipping 2-ary extremum: NULL only when both operands are. `P` holds
/// where `a` wins on value; a NULL operand loses whatever its lane holds.
#[inline]
fn minmax2<P: LanePred>(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, b: u16) {
    let (level, words) = (scratch.level, mo.m.div_ceil(64));
    {
        let ([ra, rb], rd) = split_regs(&mut scratch.regs, [a, b], d);
        if mo.no_nulls() {
            let m = mo.m;
            for ((r, &x), &y) in rd.lanes[..m].iter_mut().zip(&ra.lanes[..m]).zip(&rb.lanes[..m]) {
                *r = if P::scalar(x, y) { x } else { y };
            }
        } else {
            let (la, lb) = (&ra.lanes[..words * 64], &rb.lanes[..words * 64]);
            let mut take_a = [0u64; NULL_WORDS_PER_REG];
            simd::pred_bits::<P>(level, la, lb, &mut take_a[..words]);
            for (w, take) in take_a[..words].iter_mut().enumerate() {
                *take = !ra.nulls[w] & (rb.nulls[w] | *take);
                rd.nulls[w] = ra.nulls[w] & rb.nulls[w];
            }
            simd::blend(level, &take_a[..words], la, lb, &mut rd.lanes[..words * 64]);
        }
    }
    maybe_pack_bool_bits(scratch, mo, d);
}

/// Dispatch a German-string compare on its operator, hoisting the six-way
/// branch out of the row loop: the two equalities read the cells through
/// [`eval_str_eq`], and each ordering arm instantiates [`eval_str_cmp`] with
/// its own `Ordering` predicate. Out of line, so the row loops are compiled
/// apart from the instruction dispatch that calls them;
/// `str_const_filter_bench` measures it.
#[inline(never)]
fn str_cmp<A: Cells, B: Cells>(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    op: CmpOp,
    d: u16,
    a: StrOperand<'_, A>,
    b: StrOperand<'_, B>,
) {
    match op {
        CmpOp::Eq => eval_str_eq(scratch, mo, d, a, b, false),
        CmpOp::Ne => eval_str_eq(scratch, mo, d, a, b, true),
        CmpOp::Gt => eval_str_cmp(scratch, mo, d, a, b, |o| o == Ordering::Greater),
        CmpOp::Ge => eval_str_cmp(scratch, mo, d, a, b, |o| o != Ordering::Less),
        CmpOp::Lt => eval_str_cmp(scratch, mo, d, a, b, |o| o == Ordering::Less),
        CmpOp::Le => eval_str_cmp(scratch, mo, d, a, b, |o| o != Ordering::Greater),
    }
}

// ---------------------------------------------------------------------------
// eval_batch — single morsel
// ---------------------------------------------------------------------------

/// Decode PK column `fi` at byte `off` of each of the morsel's rows into register
/// `dst`, undoing the OPK encoding.
fn load_pk(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    dst: u16,
    (pk, stride): (&[u8], usize),
    off: usize,
    fi: FixedInt,
) {
    let rows = &pk[mo.start * stride..(mo.start + mo.m) * stride];
    let dst_reg = scratch.reg_mut(dst, mo.m);
    gnitz_wire::for_each_fixed_int!(fi, |FI| {
        const W: usize = FI.width();
        assert!(off + W <= stride, "a PK column lies inside the PK");
        // A single-column PK is a fixed-stride array.
        if stride == W {
            for (r, key) in dst_reg.iter_mut().zip(rows.as_chunks::<W>().0) {
                *r = gnitz_wire::decode_opk_i64(key, FI);
            }
        } else {
            for (r, row) in dst_reg.iter_mut().zip(rows.chunks_exact(stride)) {
                *r = gnitz_wire::decode_opk_i64(&row[off..off + W], FI);
            }
        }
    });
    scratch.clear_null_reg(mo, dst);
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// Evaluate `prog` over one morsel of `mb` (`morsel_start..morsel_start+m`).
/// Results land in `scratch.regs`: a register's values in its lanes, its null
/// bits beside them.
///
/// Callers loop over morsels and call this function once per morsel.
pub(crate) fn eval_batch(
    prog: &ResolvedProgram,
    mb: &dyn BatchView,
    bufs: StrBufs<'_>,
    morsel_start: usize,
    m: usize,
    scratch: &mut EvalScratch,
) {
    // Hoisted, like the blob and column regions already in `bufs`: a `&dyn` call
    // is not `readonly`, so LLVM cannot fold repeated ones the way it folded a
    // monomorphized view's field loads. `col_data` cannot join them — it is
    // addressed by `(pi, width)` on demand.
    let (pk_region, pk_stride) = mb.pk_region();
    let morsel = Morsel {
        prog,
        null_bmp: mb.null_bmp(),
        start: morsel_start,
        m,
    };
    // Back to the constant prefix: the previous morsel's views die here.
    scratch.str_arena.truncate(prog.const_arena.len());

    for &(dst, instr) in &prog.instrs {
        // Opaque per opcode, or LLVM hoists every arm's morsel arithmetic into one
        // prologue each morsel pays; `filter_kernel_bench` measures it.
        let mo = std::hint::black_box(&morsel);
        let (morsel_start, m) = (mo.start, mo.m);
        match instr {
            // ----------------------------------------------------------------
            // Load operations
            // ----------------------------------------------------------------
            Instr::LoadPayloadInt { pi, fi } => {
                let col_data = mb.col_data(pi as usize, fi.width());
                let dst_reg = scratch.reg_mut(dst, m);
                gnitz_wire::for_each_fixed_int!(fi, |FI| {
                    const W: usize = FI.width();
                    let b = &col_data[morsel_start * W..(morsel_start + m) * W];
                    for (r, c) in dst_reg.iter_mut().zip(b.as_chunks::<W>().0) {
                        *r = FI.decode_le_i64(c);
                    }
                });
                fill_null_bits_mask(scratch, dst, mo, 1u64 << pi);
                maybe_pack_bool_bits(scratch, mo, dst);
            }

            // Every float register holds an f64 image, so an F32 column widens
            // on load.
            Instr::LoadPayloadF32 { pi } => {
                let col_data = mb.col_data(pi as usize, 4);
                let dst_reg = scratch.reg_mut(dst, m);
                let b = &col_data[morsel_start * 4..(morsel_start + m) * 4];
                for (r, c) in dst_reg.iter_mut().zip(b.as_chunks::<4>().0) {
                    *r = encode_f64(f32::from_bits(u32::from_le_bytes(*c)) as f64);
                }
                fill_null_bits_mask(scratch, dst, mo, 1u64 << pi);
                maybe_pack_bool_bits(scratch, mo, dst);
            }

            Instr::LoadPk { off, fi } => load_pk(scratch, mo, dst, (pk_region, pk_stride), off as usize, fi),

            // ----------------------------------------------------------------
            // Integer arithmetic
            // ----------------------------------------------------------------
            // The operator match is OUTSIDE the row loop, so each arm is its own
            // branch-free loop.
            Instr::IntArith { op, a, b, signed } => match op {
                IntArithOp::Add => bin_op(scratch, mo, dst, a, b, |x, y| x.wrapping_add(y)),
                IntArithOp::Sub => bin_op(scratch, mo, dst, a, b, |x, y| x.wrapping_sub(y)),
                IntArithOp::Mul => bin_op(scratch, mo, dst, a, b, |x, y| x.wrapping_mul(y)),
                IntArithOp::Div => int_divmod::<false>(scratch, mo, dst, a, b, signed),
                IntArithOp::Mod => int_divmod::<true>(scratch, mo, dst, a, b, signed),
            },
            Instr::IntUnary { op, a, signed } => match op {
                IntUnaryOp::Neg => un_op(scratch, mo, dst, a, |x| x.wrapping_neg()),
                IntUnaryOp::Abs => un_op(scratch, mo, dst, a, |x| x.wrapping_abs()),
                IntUnaryOp::Sign if signed => un_op(scratch, mo, dst, a, |x| x.signum()),
                IntUnaryOp::Sign => un_op(scratch, mo, dst, a, |x| (x != 0) as i64),
            },
            // One loop per (op, unit), so each inlines `calendar::eval` as that
            // op's arithmetic alone. Only the op that can NULL pays for a fail mask.
            Instr::Calendar { op, a, micros } => {
                macro_rules! per_op {
                    ($($v:ident)*) => {
                        match (op, micros) {
                            (CalendarOp::ToMicros, _) => {
                                unary_null_like(scratch, mo, dst, a, calendar::days_to_micros)
                            }
                            $(
                                (CalendarOp::$v, true) => {
                                    un_op(scratch, mo, dst, a, |x| calendar::eval(CalendarOp::$v, x, true))
                                }
                                (CalendarOp::$v, false) => {
                                    un_op(scratch, mo, dst, a, |x| calendar::eval(CalendarOp::$v, x, false))
                                }
                            )*
                        }
                    };
                }
                per_op!(Year Quarter Month Week Day Dow Isodow Doy Hour Minute Second Epoch TruncYear
                    TruncQuarter TruncMonth TruncWeek TruncDay TruncHour TruncMinute TruncSecond ToDays)
            }
            Instr::FloatUnary { op, a } => match op {
                FloatUnaryOp::Neg => un_op(scratch, mo, dst, a, |x| encode_f64(-decode_f64(x))),
                FloatUnaryOp::Abs => un_op(scratch, mo, dst, a, |x| encode_f64(decode_f64(x).abs())),
                FloatUnaryOp::Floor => un_op(scratch, mo, dst, a, |x| encode_f64(decode_f64(x).floor())),
                FloatUnaryOp::Ceil => un_op(scratch, mo, dst, a, |x| encode_f64(decode_f64(x).ceil())),
                FloatUnaryOp::Round => un_op(scratch, mo, dst, a, |x| encode_f64(decode_f64(x).round_ties_even())),
                FloatUnaryOp::Trunc => un_op(scratch, mo, dst, a, |x| encode_f64(decode_f64(x).trunc())),
                FloatUnaryOp::Sqrt => un_op(scratch, mo, dst, a, |x| encode_f64(decode_f64(x).sqrt())),
                FloatUnaryOp::Ln => un_op(scratch, mo, dst, a, |x| encode_f64(decode_f64(x).ln())),
                FloatUnaryOp::Log10 => un_op(scratch, mo, dst, a, |x| encode_f64(decode_f64(x).log10())),
                FloatUnaryOp::Exp => un_op(scratch, mo, dst, a, |x| encode_f64(decode_f64(x).exp())),
                // `partial_cmp` against zero: -0.0 and +0.0 are `Equal`, NaN is
                // `None` and stays NaN, as PostgreSQL spells it.
                FloatUnaryOp::Sign => un_op(scratch, mo, dst, a, |x| {
                    encode_f64(decode_f64(x).partial_cmp(&0.0).map_or(f64::NAN, |o| o as i32 as f64))
                }),
            },
            // A finite source whose rounded result is not finite overflowed f32's
            // range. Testing the ROUNDED value (not `|x| > f32::MAX`) keeps the
            // 2^28-1 doubles just above f32::MAX that round down to it.
            Instr::FloatToF32 { a } => unary_null_like(scratch, mo, dst, a, |x| {
                let f = decode_f64(x);
                let v32 = f as f32;
                (encode_f64(v32 as f64), f.is_finite() && v32.is_infinite())
            }),
            Instr::IntCast { a, fi, src_signed } => {
                let (lo, hi, hi_u) = int_cast_bounds(fi);
                if src_signed {
                    unary_null_like(scratch, mo, dst, a, |x| (x, x < lo || x > hi))
                } else {
                    unary_null_like(scratch, mo, dst, a, |x| (x, (x as u64) > hi_u))
                }
            }
            // Truncate toward zero, then range-check in f64: NaN fails every
            // comparison and so fails the check, as ±inf and out-of-range do.
            Instr::FloatToInt { a, fi } => {
                let (flo, fhi) = float_to_int_bounds(fi);
                if fi == FixedInt::U64 {
                    unary_null_like(scratch, mo, dst, a, |x| {
                        let t = decode_f64(x).trunc();
                        let ok = t >= flo && t < fhi;
                        (if ok { t as u64 as i64 } else { 0 }, !ok)
                    })
                } else {
                    unary_null_like(scratch, mo, dst, a, |x| {
                        let t = decode_f64(x).trunc();
                        let ok = t >= flo && t < fhi;
                        (if ok { t as i64 } else { 0 }, !ok)
                    })
                }
            }
            Instr::IntMinMax2 { a, b, is_max, signed } => match (is_max, signed) {
                (true, true) => minmax2::<simd::GtSigned>(scratch, mo, dst, a, b),
                (false, true) => minmax2::<simd::Not<simd::GtSigned>>(scratch, mo, dst, a, b),
                (true, false) => minmax2::<simd::GtUnsigned>(scratch, mo, dst, a, b),
                (false, false) => minmax2::<simd::Not<simd::GtUnsigned>>(scratch, mo, dst, a, b),
            },
            // `total_cmp` and nothing else: -0.0 and +0.0 are `==`-equal but
            // total_cmp-distinct, so an `==`-based pick would let operand order
            // decide which bit pattern survives.
            Instr::FloatMinMax2 { a, b, is_max } => match is_max {
                true => minmax2::<simd::GtTotal>(scratch, mo, dst, a, b),
                false => minmax2::<simd::Not<simd::GtTotal>>(scratch, mo, dst, a, b),
            },

            // ----------------------------------------------------------------
            // Integer set membership (col IN (…) as one opcode)
            // ----------------------------------------------------------------
            Instr::IntInSet { value_reg, set_idx } => {
                let set = &prog.int_sets[set_idx as usize];
                if set.len() <= IN_SET_BITS_MAX.min(m) {
                    in_set_bits(scratch, mo, dst, value_reg, set);
                } else {
                    un_op(scratch, mo, dst, value_reg, |x| set.binary_search(&x).is_ok() as i64)
                }
            }

            // ----------------------------------------------------------------
            // Float arithmetic
            // ----------------------------------------------------------------
            Instr::FloatArith { op, a, b } => match op {
                FloatArithOp::Add => bin_op(scratch, mo, dst, a, b, |x, y| encode_f64(decode_f64(x) + decode_f64(y))),
                FloatArithOp::Sub => bin_op(scratch, mo, dst, a, b, |x, y| encode_f64(decode_f64(x) - decode_f64(y))),
                FloatArithOp::Mul => bin_op(scratch, mo, dst, a, b, |x, y| encode_f64(decode_f64(x) * decode_f64(y))),
                FloatArithOp::Pow => bin_op(scratch, mo, dst, a, b, |x, y| {
                    encode_f64(decode_f64(x).powf(decode_f64(y)))
                }),
                FloatArithOp::Div => div_like(scratch, mo, dst, a, b, |x, y| {
                    let (fa, fb) = (decode_f64(x), decode_f64(y));
                    let is_zero = fb == 0.0;
                    let fb_safe = if is_zero { 1.0 } else { fb };
                    (encode_f64(fa / fb_safe), is_zero)
                }),
            },

            // ----------------------------------------------------------------
            // Integer comparisons
            // ----------------------------------------------------------------
            // One arm per (op, order), so the per-row loop carries no branch.
            // `UnsignedSigned` decides a negative `y` by its sign alone.
            Instr::Cmp { op, a, b, order } => match (op, order) {
                (CmpOp::Eq, IntOrder::Signed | IntOrder::Unsigned) => bin_pred::<simd::Eq>(scratch, mo, dst, a, b),
                (CmpOp::Ne, IntOrder::Signed | IntOrder::Unsigned) => {
                    bin_pred::<simd::Not<simd::Eq>>(scratch, mo, dst, a, b)
                }
                (CmpOp::Gt, IntOrder::Signed) => bin_pred::<simd::GtSigned>(scratch, mo, dst, a, b),
                (CmpOp::Ge, IntOrder::Signed) => bin_pred::<simd::Not<simd::GtSigned>>(scratch, mo, dst, b, a),
                (CmpOp::Lt, IntOrder::Signed) => bin_pred::<simd::GtSigned>(scratch, mo, dst, b, a),
                (CmpOp::Le, IntOrder::Signed) => bin_pred::<simd::Not<simd::GtSigned>>(scratch, mo, dst, a, b),
                (CmpOp::Gt, IntOrder::Unsigned) => bin_pred::<simd::GtUnsigned>(scratch, mo, dst, a, b),
                (CmpOp::Ge, IntOrder::Unsigned) => bin_pred::<simd::Not<simd::GtUnsigned>>(scratch, mo, dst, b, a),
                (CmpOp::Lt, IntOrder::Unsigned) => bin_pred::<simd::GtUnsigned>(scratch, mo, dst, b, a),
                (CmpOp::Le, IntOrder::Unsigned) => bin_pred::<simd::Not<simd::GtUnsigned>>(scratch, mo, dst, a, b),
                (CmpOp::Eq, IntOrder::UnsignedSigned) => bin_pred::<simd::EqUnsignedSigned>(scratch, mo, dst, a, b),
                (CmpOp::Ne, IntOrder::UnsignedSigned) => {
                    bin_pred::<simd::Not<simd::EqUnsignedSigned>>(scratch, mo, dst, a, b)
                }
                (CmpOp::Lt, IntOrder::UnsignedSigned) => bin_pred::<simd::LtUnsignedSigned>(scratch, mo, dst, a, b),
                (CmpOp::Le, IntOrder::UnsignedSigned) => bin_pred::<simd::LeUnsignedSigned>(scratch, mo, dst, a, b),
                (CmpOp::Gt, IntOrder::UnsignedSigned) => {
                    bin_pred::<simd::Not<simd::LeUnsignedSigned>>(scratch, mo, dst, a, b)
                }
                (CmpOp::Ge, IntOrder::UnsignedSigned) => {
                    bin_pred::<simd::Not<simd::LtUnsignedSigned>>(scratch, mo, dst, a, b)
                }
            },

            // ----------------------------------------------------------------
            // Float comparisons
            // ----------------------------------------------------------------
            // `<` and `<=` are the mirrored comparison, which a NaN fails as it
            // fails them; `<>` is the complement of `=`, which a NaN passes.
            Instr::FCmp { op, a, b } => match op {
                CmpOp::Eq => bin_pred::<simd::EqFloat>(scratch, mo, dst, a, b),
                CmpOp::Ne => bin_pred::<simd::Not<simd::EqFloat>>(scratch, mo, dst, a, b),
                CmpOp::Gt => bin_pred::<simd::GtFloat>(scratch, mo, dst, a, b),
                CmpOp::Ge => bin_pred::<simd::GeFloat>(scratch, mo, dst, a, b),
                CmpOp::Lt => bin_pred::<simd::GtFloat>(scratch, mo, dst, b, a),
                CmpOp::Le => bin_pred::<simd::GeFloat>(scratch, mo, dst, b, a),
            },

            // ----------------------------------------------------------------
            // Boolean 3VL
            // ----------------------------------------------------------------
            // Word operations over the packed bits on both arms. The unpack to
            // `dst`'s lanes is skipped for a bit_only destination, whose only
            // readers are boolean.
            Instr::BoolBinary { a, b, is_or } => {
                bool_and_or_word_loop(scratch, mo, dst, a, b, is_or);
                maybe_unpack_bool_to_regs(scratch, mo, dst);
            }
            Instr::BoolNot { a } => {
                for w in 0..m.div_ceil(64) {
                    let va = scratch.regs[a as usize].bools[w];
                    if mo.no_nulls() {
                        scratch.regs[dst as usize].bools[w] = !va;
                    } else {
                        // 3VL: NOT NULL = NULL, its truth bit cleared.
                        let na = scratch.regs[a as usize].nulls[w];
                        scratch.regs[dst as usize].bools[w] = !va & !na;
                        scratch.regs[dst as usize].nulls[w] = na;
                    }
                }
                maybe_unpack_bool_to_regs(scratch, mo, dst);
            }

            // ----------------------------------------------------------------
            // IS NULL / IS NOT NULL
            // ----------------------------------------------------------------
            Instr::IsNull { pi, invert } => eval_is_null(scratch, mo, dst, pi, invert),
            Instr::IsNullReg { a, invert } => eval_is_null_reg(scratch, mo, dst, a, invert),

            // ----------------------------------------------------------------
            // Type cast. `signed: false` reinterprets the register as u64 first
            // so values >= 2^63 cast to the correct large positive float.
            // ----------------------------------------------------------------
            Instr::IntToFloat { a, signed } => match signed {
                true => un_op(scratch, mo, dst, a, |x| encode_f64(x as f64)),
                false => un_op(scratch, mo, dst, a, |x| encode_f64(x as u64 as f64)),
            },

            // ----------------------------------------------------------------
            // Conditional select (SQL CASE blend) / manufactured NULL
            // ----------------------------------------------------------------
            Instr::Select { cond, a, b } => eval_select(scratch, mo, dst, cond, a, b),
            // Manufacture a NULL: zero the value lane, set the null bit for every
            // live row. Only ever reached on the nullable arm (LoadNull forces
            // `no_nulls` off via its `Operands::makes_null` flag).
            Instr::LoadNull => {
                scratch.regs[dst as usize].lanes[..m].fill(0);
                set_null_reg(scratch, mo, dst);
                maybe_pack_bool_bits(scratch, mo, dst);
            }

            // ----------------------------------------------------------------
            // String comparisons (column vs constant / column vs column)
            // ----------------------------------------------------------------
            // Both arms are the same compare over two operands; only where
            // operand B comes from differs. `str_cmp` keeps the three-way
            // operator branch outside the row loop, so each operator gets its own
            // branch-free kernel.
            Instr::StrColConst { op, pi, const_idx } => str_cmp(
                scratch,
                mo,
                op,
                dst,
                StrOperand::column(mb, bufs.blob, pi, mo),
                StrOperand::constant(prog, const_idx as usize),
            ),
            Instr::StrColCol { op, pi_a, pi_b } => str_cmp(
                scratch,
                mo,
                op,
                dst,
                StrOperand::column(mb, bufs.blob, pi_a, mo),
                StrOperand::column(mb, bufs.blob, pi_b, mo),
            ),

            // ----------------------------------------------------------------
            // String registers
            // ----------------------------------------------------------------
            // Every arm writes all `m` lanes, following the `unary_null_like`
            // discipline: a failed row gets a defined-but-irrelevant view plus a
            // null bit, never an untouched lane. Skipping NULL rows would leave
            // a previous morsel's view behind, and it would resolve against the
            // refilled arena.
            Instr::LoadColStr { pi } => {
                let blob_len = bufs.blob.len();
                // The buffer slot *is* the payload slot: `with_str_bufs` filled
                // it, because `resolve_program` recorded `pi` in the same mask.
                let buf = SRC_COL_BASE + pi as u32;
                let cells = bufs.region(buf).expect("a column slot names a buffer");
                let rows = cells[morsel_start * 16..(morsel_start + m) * 16].as_chunks::<16>().0;
                let lane = str_lane_mut(&mut scratch.str_views, dst as usize, m);
                for (i, (v, cell)) in lane.iter_mut().zip(rows).enumerate() {
                    *v = cell_to_view(cell, (morsel_start + i) * 16, buf, blob_len);
                }
                fill_null_bits_mask(scratch, dst, mo, 1u64 << pi);
            }

            // The `LoadNull` shape: a defined empty lane plus the null bit for
            // every live row, with the tail word masked so stale high bits never
            // read as null.
            Instr::LoadNullStr => {
                str_lane_mut(&mut scratch.str_views, dst as usize, m).fill(StrView::default());
                set_null_reg(scratch, mo, dst);
            }

            Instr::StrSelect { cond, a, b } => eval_str_select(scratch, mo, dst, cond, a, b),

            // The operator branch stays outside the row loop, as `str_cmp` does
            // for the `StrColConst` / `StrColCol` pair.
            Instr::StrCmp { op, a, b } => match op {
                CmpOp::Eq => str2_to_scalar(scratch, mo, bufs, dst, a, b, |x, y| (x == y) as i64),
                CmpOp::Ne => str2_to_scalar(scratch, mo, bufs, dst, a, b, |x, y| (x != y) as i64),
                CmpOp::Gt => str2_to_scalar(scratch, mo, bufs, dst, a, b, |x, y| (x > y) as i64),
                CmpOp::Ge => str2_to_scalar(scratch, mo, bufs, dst, a, b, |x, y| (x >= y) as i64),
                CmpOp::Lt => str2_to_scalar(scratch, mo, bufs, dst, a, b, |x, y| (x < y) as i64),
                CmpOp::Le => str2_to_scalar(scratch, mo, bufs, dst, a, b, |x, y| (x <= y) as i64),
            },

            // Characters are counted off the bytes; a byte length is the view's own.
            Instr::StrLen { a, chars } => {
                if chars {
                    str_to_scalar(scratch, mo, bufs, dst, a, |s| char_count(s) as i64);
                } else {
                    str_byte_len(scratch, mo, dst, a);
                }
            }

            // A fresh copy in the arena, folded in place. Unswitched on `upper`,
            // as `StrLen` and `int_divmod` are, so the direction is a constant
            // inside the byte loop rather than a test per byte.
            Instr::StrCase { a, upper } => {
                macro_rules! fold_case {
                    (|$b:ident| $hit:expr) => {
                        str_kernel_total(scratch, mo, bufs, dst, [a], [], |arena, bufs, [v], []| {
                            let (o, l) = arena_push_view(arena, bufs, v);
                            for $b in &mut arena[o..o + l] {
                                // `b ^ 0x20` on a hit and `b ^ 0` otherwise — the
                                // fold with no branch in the byte loop.
                                *$b ^= (($hit) as u8) << 5;
                            }
                            StrView::arena(o, l)
                        })
                    };
                }
                if upper {
                    fold_case!(|byte| byte.is_ascii_lowercase())
                } else {
                    fold_case!(|byte| byte.is_ascii_uppercase())
                }
            }

            Instr::StrSubstr { src, start, len } => eval_str_substr(scratch, mo, bufs, dst, src, start, len),

            // A sub-view of the source: the bytes are not copied, only the
            // offset and length narrowed.
            Instr::StrTrim { a, mode, set_idx } => {
                let set = &prog.trim_sets[set_idx as usize];
                str_kernel_total(scratch, mo, bufs, dst, [a], [], |arena, bufs, [v], []| {
                    let (s, base_off) = view_bytes_at(v, arena, bufs);
                    let (mut lo, mut hi) = (0usize, s.len());
                    if mode.trims_start() {
                        while lo < hi && in_trim_set(set, s[lo]) {
                            lo += 1;
                        }
                    }
                    if mode.trims_end() {
                        while hi > lo && in_trim_set(set, s[hi - 1]) {
                            hi -= 1;
                        }
                    }
                    StrView::at(v.src, base_off + lo, hi - lo)
                });
            }

            Instr::StrLike { src, matcher_idx } => {
                let matcher = &prog.like_matchers[matcher_idx as usize];
                let mut folded = Vec::new();
                str_to_scalar(scratch, mo, bufs, dst, src, |s| matcher.matches(s, &mut folded) as i64);
            }

            Instr::StrConcat { a, b, skip_null } => eval_str_concat(scratch, mo, bufs, dst, a, b, skip_null),

            // The three numeric→text arms share one loop, monomorphised per
            // closure so the signed/float branch stays outside it.
            // `unsigned_abs` rather than `-v`: `i64::MIN` has no positive i64.
            Instr::IntToStr { a, signed } => {
                if signed {
                    num_to_str(scratch, mo, dst, a, |arena, v| {
                        arena_push_int(arena, v.unsigned_abs(), v < 0)
                    });
                } else {
                    num_to_str(scratch, mo, dst, a, |arena, v| arena_push_int(arena, v as u64, false));
                }
            }

            Instr::FloatToStr { a } => {
                num_to_str(scratch, mo, dst, a, |arena, v| arena_push_float(arena, decode_f64(v)));
            }

            Instr::StrToInt { a, fi } => {
                let (lo, hi) = fi.range();
                // A U64 value above i64::MAX narrows to its own bit pattern,
                // which is what the register holds; the resolve-time U64
                // tracking makes downstream reads agree.
                str_parse_to_scalar(scratch, mo, bufs, dst, a, |s| {
                    parse_decimal_i128(s).filter(|v| *v >= lo && *v <= hi).map(|v| v as i64)
                });
            }

            Instr::StrToFloat { a } => {
                str_parse_to_scalar(scratch, mo, bufs, dst, a, |s| {
                    std::str::from_utf8(s.trim_ascii())
                        .ok()
                        .and_then(|t| t.parse::<f64>().ok())
                        .map(encode_f64)
                });
            }
            Instr::StrPos { hay, needle } => {
                str2_to_scalar(scratch, mo, bufs, dst, hay, needle, |h, n| match find(h, n) {
                    Some(off) => char_count(&h[..off]) as i64 + 1,
                    None => 0,
                })
            }
            Instr::StrSide { src, n, left } => {
                str_kernel_total(scratch, mo, bufs, dst, [src], [n], |arena, bufs, [v], [n]| {
                    let (s, base) = view_bytes_at(v, arena, bufs);
                    // Clamped to the byte length, which no character count exceeds.
                    let n_abs = n.unsigned_abs().min(s.len() as u128) as usize;
                    // A non-negative LEFT and a negative RIGHT count from the start.
                    let counts_from_start = (n >= 0) == left;
                    let cut = if counts_from_start {
                        char_offset(s, n_abs)
                    } else {
                        char_offset_back(s, n_abs)
                    };
                    if left {
                        StrView::at(v.src, base, cut)
                    } else {
                        StrView::at(v.src, base + cut, s.len() - cut)
                    }
                })
            }
            Instr::StrReverse { a } => str_kernel_total(scratch, mo, bufs, dst, [a], [], |arena, bufs, [v], []| {
                let (o, l) = arena_push_view(arena, bufs, v);
                reverse_chars(&mut arena[o..o + l]);
                StrView::arena(o, l)
            }),
            Instr::StrReplace { s, from, to } => {
                str_kernel(
                    scratch,
                    mo,
                    bufs,
                    dst,
                    [s, from, to],
                    [],
                    |arena, bufs, [vs, vf, vt], []| {
                        let (s, from, to) = (
                            view_bytes(vs, arena, bufs),
                            view_bytes(vf, arena, bufs),
                            view_bytes(vt, arena, bufs),
                        );
                        if from.is_empty() {
                            return Some(vs);
                        }
                        let hits = fields(s, from).count() - 1;
                        if hits == 0 {
                            return Some(vs);
                        }
                        let total = (s.len() + hits * to.len()) as u128 - (hits * from.len()) as u128;
                        if total > u32::MAX as u128 {
                            return None;
                        }
                        // The operands may live in the arena being grown, so the
                        // result's span is reserved first and the arena split
                        // around it: the operands are read out of the prefix
                        // while the result is written into the tail.
                        let (out, total) = (arena.len(), total as usize);
                        arena.resize(out + total, 0);
                        let (prefix, res) = arena.split_at_mut(out);
                        let prefix: &[u8] = prefix;
                        let (s, from, to) = (
                            view_bytes(vs, prefix, bufs),
                            view_bytes(vf, prefix, bufs),
                            view_bytes(vt, prefix, bufs),
                        );
                        let mut w = 0;
                        for (k, (lo, hi)) in fields(s, from).enumerate() {
                            if k > 0 {
                                res[w..w + to.len()].copy_from_slice(to);
                                w += to.len();
                            }
                            res[w..w + hi - lo].copy_from_slice(&s[lo..hi]);
                            w += hi - lo;
                        }
                        debug_assert_eq!(w, total);
                        Some(StrView::arena(out, total))
                    },
                )
            }
            Instr::StrPad { s, n, fill, left } => {
                str_kernel(scratch, mo, bufs, dst, [s, fill], [n], |arena, bufs, [vs, vf], [n]| {
                    let (s, base) = view_bytes_at(vs, arena, bufs);
                    if n <= 0 {
                        return Some(StrView::default());
                    }
                    let s_chars = char_count(s);
                    // A width at or below the subject's own length truncates —
                    // the LEFT of `n` characters, a sub-view.
                    if n <= s_chars as i128 {
                        return Some(StrView::at(vs.src, base, char_offset(s, n as usize)));
                    }
                    let (fill, fill_base) = view_bytes_at(vf, arena, bufs);
                    let fill_chars = char_count(fill);
                    if fill_chars == 0 {
                        return Some(vs);
                    }
                    // A pad character is at least one byte, so a count past the
                    // byte ceiling is already NULL — and what remains fits a `usize`.
                    let pad_chars = match usize::try_from(n - s_chars as i128) {
                        Ok(c) if c <= u32::MAX as usize => c,
                        _ => return None,
                    };
                    let (whole, rem) = (pad_chars / fill_chars, pad_chars % fill_chars);
                    let pad_len = whole * fill.len() + char_offset(fill, rem);
                    let total = s.len() as u128 + pad_len as u128;
                    if total > u32::MAX as u128 {
                        return None;
                    }
                    let (s_len, fill_len, fill0) = (s.len(), fill.len(), fill[0]);
                    let out = arena.len();
                    // The pad doubles itself: `n` fills cost `log2 n` copies.
                    let push_pad = |arena: &mut Vec<u8>| {
                        let p = arena.len();
                        if fill_len == 1 {
                            arena.resize(p + pad_len, fill0);
                            return;
                        }
                        arena_push_span(arena, bufs, vf.src, fill_base, fill_len.min(pad_len));
                        while arena.len() - p < pad_len {
                            let have = arena.len() - p;
                            arena.extend_from_within(p..p + have.min(pad_len - have));
                        }
                    };
                    if left {
                        push_pad(arena);
                        arena_push_span(arena, bufs, vs.src, base, s_len);
                    } else {
                        arena_push_span(arena, bufs, vs.src, base, s_len);
                        push_pad(arena);
                    }
                    Some(StrView::arena(out, total as usize))
                })
            }
            Instr::StrSplitPart { s, delim, n } => {
                str_kernel(scratch, mo, bufs, dst, [s, delim], [n], |arena, bufs, [vs, vd], [n]| {
                    if n == 0 {
                        return None;
                    }
                    let (s, base) = view_bytes_at(vs, arena, bufs);
                    let d = view_bytes(vd, arena, bufs);
                    if d.is_empty() {
                        return Some(if n == 1 || n == -1 { vs } else { StrView::default() });
                    }
                    // The 0-based field index: a negative `n` counts back from
                    // the field total. Off either end is the empty string.
                    let idx = if n > 0 {
                        usize::try_from(n - 1).ok()
                    } else {
                        usize::try_from(-n)
                            .ok()
                            .and_then(|k| fields(s, d).count().checked_sub(k))
                    };
                    let field = idx.and_then(|i| fields(s, d).nth(i));
                    Some(field.map_or(StrView::default(), |(lo, hi)| StrView::at(vs.src, base + lo, hi - lo)))
                })
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;
