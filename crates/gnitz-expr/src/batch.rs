//! Morsel-oriented batch expression evaluator.
//!
//! `eval_batch` processes one morsel at a time (up to MORSEL rows), applying
//! all expression opcodes as columnar loops over register buffers. Base
//! pointers are hoisted outside the inner loop, which is what lets LLVM
//! auto-vectorize the arithmetic opcodes — the string and null-gathering
//! kernels do not vectorize.

use std::cmp::Ordering;
use std::fmt::{self, Write as _};

use crate::chars::{char_count, char_offset, char_offset_back, reverse_chars};
use crate::like::{fields, find};
use crate::program::{FloatUnaryOp, IntArithOp, IntReg, IntUnaryOp};
use crate::{calendar, BatchView, CalendarOp, CmpOp, FloatArithOp, Instr, ResolvedProgram};
use gnitz_wire::{
    compare_german_strings, german_string_heap, german_string_inline, low_bits_mask, null_word_get, read_u64_le,
    FixedInt,
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
const NULL_WORDS_PER_REG: usize = MORSEL / 64; // 4

/// Live bits of the last of `m.div_ceil(64)` words: all-ones when the morsel
/// fills it, so a tail mask is written unconditionally.
fn tail_mask(m: usize) -> u64 {
    low_bits_mask(m - m.div_ceil(64).saturating_sub(1) * 64)
}

// ---------------------------------------------------------------------------
// EvalScratch — the SoA register file for batch evaluation
// ---------------------------------------------------------------------------

pub(crate) struct EvalScratch {
    /// Register buffers, register-major layout: regs[reg * MORSEL + row].
    ///
    /// A lane at a NULL row holds whatever its kernel computed from the row's
    /// stored bytes: kernels run unconditionally to stay branch-free, so a
    /// consumer must read `regs` against [`Self::null_bits`], never alone.
    regs: Vec<i64>,
    /// Null bitmask, register-major: null_bits[reg * NULL_WORDS_PER_REG + word].
    /// Empty (capacity 0) when `no_nulls` is true.
    null_bits: Vec<u64>,
    /// Packed truthy bits, register-major; bridges boolean producers and
    /// consumers on the nullable arm without per-row repack from `regs`.
    /// Empty (capacity 0) when `no_nulls` is true.
    bool_bits: Vec<u64>,
    /// Per-row filter bitmask; written only by the filter path, and the one
    /// buffer whose width follows the batch rather than the program.
    filter_bits: Vec<u64>,
    /// String register lanes, register-major like [`Self::regs`]:
    /// `str_views[reg * MORSEL + row]`. Empty (capacity 0) unless the program
    /// has string instructions; a zero-length default view reads as `""`.
    str_views: Vec<StrView>,
    /// Computed string bytes — case folds, concatenations, numeric text —
    /// behind the program's string constants, which occupy a prefix the
    /// per-morsel reset does not clear. Views never outlive their morsel plus
    /// its emit phase, and the borrow checker enforces that ordering:
    /// `MorselOut` borrows the scratch immutably while the next `eval_batch`
    /// needs it mutably.
    ///
    /// The constant prefix is written once by [`EvalScratch::new`], so a scratch
    /// is tied to the one program it was built for. An `Evaluator` owns both, so
    /// that pairing holds by construction.
    str_arena: Vec<u8>,
}

/// `N` shared windows plus one mutable window into the same register-major
/// buffer, each `len` elements wide at `index * stride`. The one unsafe split in
/// this file: `regs`, `null_bits` and `str_views` differ only in element type
/// and stride, and the opcodes only in how many sources they read.
///
/// Sound because `d` is none of `srcs`: a register *is* the index of its writing
/// instruction, and `LogicalProgram::from_instrs` — which every constructor
/// routes through — rejects an operand not written before its reader. Duplicate
/// *sources* alias harmlessly. The `debug_assert`s restate both halves.
fn split_windows<T, const N: usize>(
    buf: &mut [T],
    stride: usize,
    srcs: [usize; N],
    d: usize,
    len: usize,
) -> ([&[T]; N], &mut [T]) {
    debug_assert!(!srcs.contains(&d), "split_windows: dst aliases a src register");
    debug_assert!(
        srcs.iter()
            .chain(std::iter::once(&d))
            .all(|&i| i * stride + len <= buf.len()),
        "split_windows: window runs past the scratch buffer",
    );
    let ptr = buf.as_mut_ptr();
    unsafe {
        (
            srcs.map(|s| std::slice::from_raw_parts(ptr.add(s * stride), len)),
            std::slice::from_raw_parts_mut(ptr.add(d * stride), len),
        )
    }
}

impl EvalScratch {
    /// The whole register file for `prog`, sized and seeded once — every buffer
    /// but [`Self::filter_bits`] is a function of the program alone, and a
    /// scratch belongs to exactly one [`crate::Evaluator`], hence to one program.
    /// A zero capacity is how a buffer stays unallocated for a program that never
    /// touches it.
    pub(crate) fn new(prog: &ResolvedProgram) -> Self {
        let num_regs = prog.num_regs as usize;
        // Full width, unlike `regs`: `set_null_reg` indexes these by the raw
        // register and runs for `LoadNullStr`, a string one.
        let null_cap = if prog.no_nulls {
            0
        } else {
            num_regs * NULL_WORDS_PER_REG
        };
        let mut scratch = EvalScratch {
            regs: vec![0; prog.scalar_lanes as usize * MORSEL],
            null_bits: vec![0; null_cap],
            bool_bits: vec![0; null_cap],
            filter_bits: Vec::new(),
            str_views: vec![StrView::default(); prog.str_lanes as usize * MORSEL],
            str_arena: prog.const_arena.clone(),
        };
        scratch.install_consts(prog);
        scratch
    }

    /// Write the constant registers at full [`MORSEL`] width, once: a register
    /// *is* the index of the instruction that writes it, so no morsel can
    /// overwrite the lane. The buffers are freshly zeroed, so no null clear.
    fn install_consts(&mut self, prog: &ResolvedProgram) {
        for &(dst, val) in &prog.const_regs {
            let base_r = dst as usize * MORSEL;
            self.regs[base_r..base_r + MORSEL].fill(val);
            if !prog.no_nulls && prog.needs_bool_pack(dst as usize) {
                let base_b = dst as usize * NULL_WORDS_PER_REG;
                pack_truthy(
                    &self.regs[base_r..base_r + MORSEL],
                    &mut self.bool_bits[base_b..base_b + NULL_WORDS_PER_REG],
                );
            }
        }
        for &(dst, off, len) in &prog.const_str_regs {
            let base_d = dst as usize * MORSEL;
            self.str_views[base_d..base_d + MORSEL].fill(StrView { off: off as u64, len, src: SRC_ARENA });
        }
    }

    /// Ensure the filter bitmap holds `(n + 63) / 64` words — the one buffer
    /// [`Self::new`] cannot size, since it follows the batch. Does not shrink.
    pub(crate) fn ensure_filter_words(&mut self, n: usize) {
        let words = n.div_ceil(64);
        if self.filter_bits.len() < words {
            self.grow_filter_words(words);
        }
    }

    /// The growth half of [`Self::ensure_filter_words`], taken once per widest
    /// batch an evaluator is driven over.
    #[cold]
    fn grow_filter_words(&mut self, words: usize) {
        self.filter_bits.resize(words, 0);
    }

    fn reg_mut(&mut self, reg: u16, m: usize) -> &mut [i64] {
        let base = reg as usize * MORSEL;
        &mut self.regs[base..base + m]
    }

    /// Split borrows over `regs`: `N` shared source windows + one mutable
    /// destination. Backs every unary opcode (`N = 1`), every binary one
    /// (`N = 2`) and SELECT's no-nulls value blend (`N = 3`).
    fn regs_split<const N: usize>(&mut self, srcs: [u16; N], d: u16, m: usize) -> ([&[i64]; N], &mut [i64]) {
        split_windows(&mut self.regs, MORSEL, srcs.map(usize::from), d as usize, m)
    }

    /// The same split over `null_bits`, whose windows are `NULL_WORDS_PER_REG`
    /// words rather than `MORSEL` values. Fixed at two sources: both callers
    /// propagate a binary operand pair.
    fn null_split(&mut self, srcs: [u16; 2], d: u16, words: usize) -> ([&[u64]; 2], &mut [u64]) {
        split_windows(
            &mut self.null_bits,
            NULL_WORDS_PER_REG,
            srcs.map(usize::from),
            d as usize,
            words,
        )
    }

    /// Pack this morsel's filter verdict into `filter_bits`, at the word run
    /// morsel `morsel_start` owns. `MORSEL` is 64-aligned, so that run is whole
    /// words: every word is written in full, sparing both the read-modify-write
    /// and the up-front zero-fill.
    ///
    /// Not `#[inline(always)]`: non-generic, so its body lives in this crate's
    /// opt-1 rlib rather than in `filter`'s opt-0 monomorphization.
    pub(crate) fn write_filter_words(&mut self, prog: &ResolvedProgram, morsel_start: usize, m: usize) {
        let r = prog.result_reg as usize;
        let base_w = morsel_start / 64;
        let words = m.div_ceil(64);
        if prog.no_nulls {
            // `no_nulls` allocates no `bool_bits`, so the verdict is in `regs`.
            let base_r = r * MORSEL;
            pack_truthy(
                &self.regs[base_r..base_r + m],
                &mut self.filter_bits[base_w..base_w + words],
            );
            return;
        }
        // Word-level merge: filter bit = truthy & !null.
        let base = r * NULL_WORDS_PER_REG;
        for w in 0..words {
            self.filter_bits[base_w + w] = self.bool_bits[base + w] & !self.null_bits[base + w];
        }
        // Every nullable-arm boolean producer writes whole words, so the last one
        // carries 1s past the morsel's last row. Masked once here rather than by
        // each of them: unmasked they append the degenerate range `(n, n)`,
        // breaking the maximal-non-empty-run contract `filter` documents.
        self.filter_bits[base_w + words - 1] &= tail_mask(m);
    }

    /// The filter bitmap covering `n` rows. No bit past `n` is set — both arms
    /// of [`Self::write_filter_words`] see to that.
    pub(crate) fn filter_words(&self, n: usize) -> &[u64] {
        &self.filter_bits[..n.div_ceil(64)]
    }

    /// The result register's value at row `i` of the morsel just evaluated, or
    /// `None` for a NULL row — the **one** statement of how a result is read
    /// back, whatever arity the caller drives at. Reads the `regs` lane, which
    /// a bit_only register has none of — [`crate::Evaluator::eval_all`]'s role
    /// guard is what keeps those out.
    pub(crate) fn result_value(&self, prog: &ResolvedProgram, i: usize) -> Option<i64> {
        let r = prog.result_reg as usize;
        let (word, bit) = (r * NULL_WORDS_PER_REG + i / 64, i % 64);
        if !prog.no_nulls && (self.null_bits[word] >> bit) & 1 != 0 {
            return None;
        }
        Some(self.regs[r * MORSEL + i])
    }

    /// [`Self::result_value`] for every row of the morsel, appended to `out`.
    pub(crate) fn append_result_values(&self, prog: &ResolvedProgram, m: usize, out: &mut Vec<Option<i64>>) {
        out.extend((0..m).map(|i| self.result_value(prog, i)));
    }

    /// This morsel's registers, as the shape [`crate::Evaluator::eval_morsels`]
    /// hands out. `#[inline(always)]` — it returns a couple of hundred bytes by
    /// value into an opt-0 caller, once per morsel.
    #[inline(always)]
    pub(crate) fn morsel_out<'a>(&'a self, bufs: StrBufs<'a>, m: usize) -> MorselOut<'a> {
        MorselOut {
            regs: &self.regs,
            null_bits: &self.null_bits,
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
        let base = reg as usize * NULL_WORDS_PER_REG;
        self.null_bits[base..base + mo.m.div_ceil(64)].fill(0);
    }
}

/// One morsel's results, read out of the register file. Handed to
/// [`crate::eval::Evaluator::eval_morsels`]'s callback for the lifetime of that call.
pub struct MorselOut<'a> {
    regs: &'a [i64],
    /// Empty exactly when the program resolved `no_nulls`, which is the one
    /// record of the arm here: `analyze` starts the verdict at `true` and only
    /// ANDs, so a nullable program has at least one register and a non-empty
    /// window.
    null_bits: &'a [u64],
    str_views: &'a [StrView],
    str_arena: &'a [u8],
    bufs: StrBufs<'a>,
    m: usize,
}

impl MorselOut<'_> {
    /// How many rows this morsel covers (always ≥ 1) — the one thing an
    /// [`crate::Evaluator::eval_morsels`] callback needs that the emits below,
    /// which cut their own windows, do not hand it.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.m
    }

    /// Register `reg`'s values for this morsel's rows, in row order. Crate-local:
    /// consumers outside this crate read the same lanes as bytes, through
    /// [`Self::reg_bytes`], which is what an 8-byte output slot stores.
    #[inline(always)]
    pub(crate) fn reg_values(&self, reg: usize) -> &[i64] {
        let base = reg * MORSEL;
        &self.regs[base..base + self.m]
    }

    /// The same values as their little-endian byte image, 8 bytes per row —
    /// what an 8-byte output slot stores. `check_emit_slot` holds every register sink
    /// destination to such a slot, so [`Self::emit_scalar_cells`] blits this
    /// straight into one.
    ///
    /// Only a *scalar* register has a lane here (`ResolvedProgram::scalar_lanes`
    /// sizes the buffer); a string one panics on the index.
    #[inline(always)]
    pub(crate) fn reg_bytes(&self, reg: usize) -> &[u8] {
        gnitz_wire::as_le_bytes(self.reg_values(reg))
    }

    /// Emit scalar register `reg` into output payload column `col`, whose rows
    /// start at `row0`: the register image blitted in at `stride` 8, or its low
    /// `stride` bytes per row into a narrower slot (`check_emit_slot` admits one
    /// only behind a cast to that width), then each NULL row's slot zeroed and
    /// its bit set at `out_payload` in the row-major bitmap `nb`.
    ///
    /// The narrow widths are unswitched rather than written once against a
    /// runtime `stride`: a runtime width makes each row's store an indirect
    /// `memcpy` call, where a constant one is a move the loop vectorizes.
    pub fn emit_scalar_cells(
        &self,
        reg: usize,
        col: &mut [u8],
        nb: &mut [u8],
        row0: usize,
        out_payload: usize,
        stride: usize,
    ) {
        let win = &mut col[row0 * stride..(row0 + self.m) * stride];
        // `stride` is `wire_stride` of a slot `check_emit_slot` passed, so it is
        // a fixed-int width; a 16-byte string slot goes to `emit_str_cells`.
        match stride {
            8 => win.copy_from_slice(self.reg_bytes(reg)),
            4 => Self::narrow_cells(win, self.reg_values(reg), |v| (v as u32).to_le_bytes()),
            2 => Self::narrow_cells(win, self.reg_values(reg), |v| (v as u16).to_le_bytes()),
            1 => Self::narrow_cells(win, self.reg_values(reg), |v| (v as u8).to_le_bytes()),
            _ => unreachable!("a scalar emit slot is 1, 2, 4 or 8 bytes, not {stride}"),
        }
        self.write_null_rows(reg, win, stride, nb, row0, out_payload);
    }

    /// [`MorselOut::emit_scalar_cells`]'s narrow half: each value's low `W`
    /// bytes, as `to_bytes` truncates them, into its own slot.
    fn narrow_cells<const W: usize>(win: &mut [u8], vals: &[i64], to_bytes: impl Fn(i64) -> [u8; W]) {
        for (dst, v) in win.as_chunks_mut::<W>().0.iter_mut().zip(vals) {
            *dst = to_bytes(*v);
        }
    }

    /// [`Self::emit_scalar_cells`] over a string register: one German-string
    /// cell per row, long bodies appended to `blob`, then the NULL rows blanked.
    pub fn emit_str_cells(
        &self,
        reg: usize,
        col: &mut [u8],
        nb: &mut [u8],
        blob: &mut Vec<u8>,
        row0: usize,
        out_payload: usize,
    ) {
        let win = &mut col[row0 * 16..(row0 + self.m) * 16];
        self.write_str_cells(reg, win, blob);
        self.write_null_rows(reg, win, 16, nb, row0, out_payload);
    }

    /// String register `reg`'s bytes for row `i` of this morsel.
    ///
    /// Unlike [`Self::reg_values`], which returns a slice already cut to `m`,
    /// this is a random-access getter and carries its own bound: an
    /// out-of-morsel read would otherwise hand back another value's bytes.
    #[inline(always)]
    pub(crate) fn str_bytes(&self, reg: usize, i: usize) -> &[u8] {
        debug_assert!(i < self.m, "str_bytes row {i} is outside the morsel's {} rows", self.m);
        view_bytes(self.str_views[reg * MORSEL + i], self.str_arena, self.bufs)
    }

    /// Encode string register `reg` into `win`, one German-string cell per row,
    /// long bodies appended to `blob`. Writes every row, NULL ones included;
    /// [`Self::write_null_rows`] zeroes those after.
    fn write_str_cells(&self, reg: usize, win: &mut [u8], blob: &mut Vec<u8>) {
        debug_assert_eq!(win.len(), self.m * 16, "a string emit window is 16 bytes per row");
        for (i, cell) in win.as_chunks_mut::<16>().0.iter_mut().enumerate() {
            *cell = gnitz_wire::encode_german_string(self.str_bytes(reg, i), blob);
        }
    }

    /// The NULL half of an emit: zero a NULL row's `stride`-byte cell in `win`
    /// and set its bit at `out_payload` in the row-major output bitmap `nb`,
    /// whose rows start at `row0`. The bit merge is a read-modify-write, which
    /// is what lets two emits compose.
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
    /// NULL rows are the exception, so consumers bit-scan rather than branch per
    /// row — a per-row branch would de-vectorize the surrounding value store.
    ///
    /// Every producer writes only rows `0..m`, so bits at index >= m are zero and
    /// the bit-scan stays in the morsel — no tail re-masking needed.
    #[inline(always)]
    pub(crate) fn for_each_null_row(&self, reg: usize, mut f: impl FnMut(usize)) {
        if self.null_bits.is_empty() {
            return;
        }
        let base = reg * NULL_WORDS_PER_REG;
        let words = self.m.div_ceil(64);
        for w in 0..words {
            let mut word = self.null_bits[base + w];
            debug_assert!(
                w + 1 < words || self.m.is_multiple_of(64) || (word >> (self.m % 64)) == 0,
                "null_bits tail word has bits set beyond m={}",
                self.m,
            );
            let lo = w * 64;
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                word &= word - 1;
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
pub(crate) fn scan_filter_bits(bits: &[u64], n: usize, out: &mut Vec<(usize, usize)>) {
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
/// `#[inline]` for [`maybe_pack_bool_bits`]' reason: once per instruction per
/// morsel, and a no-op on the `no_nulls` arm. Kept apart from [`null_or_all`]:
/// spelled through it, the `int_div` and `select` shapes of `expr_kernel_bench`
/// each retire 1.3 % more instructions.
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
/// is not a `const N`, so [`split_windows`] does not apply, and at four words
/// per morsel the bounds checks cost nothing that matters.
#[inline]
fn null_or_all(s: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, srcs: impl Iterator<Item = u16> + Clone) {
    if mo.no_nulls() {
        return;
    }
    let words = mo.m.div_ceil(64);
    for w in 0..words {
        let acc = srcs
            .clone()
            .fold(0u64, |acc, r| acc | s.null_bits[r as usize * NULL_WORDS_PER_REG + w]);
        s.null_bits[dst as usize * NULL_WORDS_PER_REG + w] = acc;
    }
}

/// Propagate unary null: dst_null = src_null (word-at-a-time). `#[inline]` for
/// [`null_or2`]'s reason.
#[inline]
fn null_copy1(s: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, src: u16) {
    if mo.no_nulls() {
        return;
    }
    let base_s = src as usize * NULL_WORDS_PER_REG;
    s.null_bits
        .copy_within(base_s..base_s + mo.m.div_ceil(64), dst as usize * NULL_WORDS_PER_REG);
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
    let base = di as usize * NULL_WORDS_PER_REG;
    let rows = mo.null_words();
    gather_null_words(rows, mask, &mut s.null_bits[base..base + mo.m.div_ceil(64)]);
}

/// Gather one bit per row of `rows` (the morsel's 8-byte null words) into `out`:
/// bit `j` of word `w` is set iff row `w * 64 + j` is null in any column of
/// `cols`. Whole words, so bits past the morsel's last row are 0. A column
/// *mask*, so the two-operand string compare gathers both columns in one pass.
fn gather_null_words(rows: &[u8], cols: u64, out: &mut [u64]) {
    for (w, block) in rows.chunks(64 * 8).enumerate() {
        let mut word: u64 = 0;
        for j in 0..block.len() / 8 {
            word |= ((read_u64_le(block, j * 8) & cols != 0) as u64) << j;
        }
        out[w] = word;
    }
}

/// `IS [NOT] NULL`: read payload column `pi`'s null bit per row, optionally
/// invert (`invert` for IS NOT NULL), and write the boolean into register `dst`,
/// whose own null lane is cleared — the result is never NULL.
///
/// The bitmap read is the *batch's*, so the column may be nullable even under
/// `no_nulls`: `IsNull` decodes no value and so does not force the arm.
///
/// A `NOT NULL` column collapses to a fill, on [`fill_null_bits_mask`]'s rule:
/// the declaration is believed over the bit, so the verdict cannot vary by row.
fn eval_is_null(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, pi: u8, invert: bool) {
    let pi = pi as usize;
    let col_nullable = null_word_get(mo.prog.nullable_slots, pi);
    if mo.no_nulls() {
        // Neither `null_bits` nor `bool_bits` exists on this arm, so there is
        // nothing to clear and nothing to pack — just the lane fill.
        return fill_is_null_regs(scratch, mo, dst, pi, invert, col_nullable);
    }
    // A destination read as a packed bit builds that word off the gather
    // directly; one read as a value takes the lane fill below. Gated because it
    // is not a win both ways: ungated, `is_null_arm_bench`'s register-sink `map`
    // shape costs +30 %, against −31 % on `bare` and −8 % on the 4-conjunct
    // chains.
    if mo.prog.needs_bool_pack(dst as usize) {
        return is_null_packed(scratch, mo, dst, pi, invert, col_nullable);
    }
    // No pack after this arm: its condition is the gate just above, so reaching
    // here means no reader takes `dst` as a packed bit.
    scratch.clear_null_reg(mo, dst);
    fill_is_null_regs(scratch, mo, dst, pi, invert, col_nullable);
}

/// The value half of [`eval_is_null`]: one definite boolean per row in `regs`.
#[inline]
fn fill_is_null_regs(s: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, pi: usize, invert: bool, col_nullable: bool) {
    if col_nullable {
        let rows = mo.null_words();
        let rd = s.reg_mut(dst, mo.m);
        for (i, r) in rd.iter_mut().enumerate() {
            *r = (null_word_get(read_u64_le(rows, i * 8), pi) ^ invert) as i64;
        }
    } else {
        s.reg_mut(dst, mo.m).fill(invert as i64);
    }
}

/// [`eval_is_null`]'s packed arm, kept out of line so the two arms above stay
/// the two loads that choose between them plus one loop.
#[inline(never)]
fn is_null_packed(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, pi: usize, invert: bool, col_nullable: bool) {
    let words = mo.m.div_ceil(64);
    let base = dst as usize * NULL_WORDS_PER_REG;
    let flip = if invert { u64::MAX } else { 0 };
    if col_nullable {
        gather_null_words(mo.null_words(), 1u64 << pi, &mut scratch.bool_bits[base..base + words]);
    } else {
        scratch.bool_bits[base..base + words].fill(0);
    }
    for w in 0..words {
        scratch.bool_bits[base + w] ^= flip;
        // Whole words: `for_each_null_row` asserts a clean tail here, and the
        // 1s `flip` leaves in `bool_bits` are `write_filter_words`' to mask.
        scratch.null_bits[base + w] = 0;
    }
    maybe_unpack_bool_to_regs(scratch, mo, dst);
}

/// `IS [NOT] NULL` over register `a`'s null lane, a definite boolean. Under
/// `no_nulls` there is no lane and nothing is NULL, so it is the constant
/// `invert`; otherwise the lane *is* the packed boolean (the `BoolNot` shape).
fn eval_is_null_reg(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, a: u16, invert: bool) {
    if mo.no_nulls() {
        scratch.reg_mut(dst, mo.m).fill(invert as i64);
        return;
    }
    let flip = if invert { u64::MAX } else { 0 };
    let base_a = a as usize * NULL_WORDS_PER_REG;
    let base_d = dst as usize * NULL_WORDS_PER_REG;
    for w in 0..mo.m.div_ceil(64) {
        scratch.bool_bits[base_d + w] = scratch.null_bits[base_a + w] ^ flip;
        scratch.null_bits[base_d + w] = 0;
    }
    maybe_unpack_bool_to_regs(scratch, mo, dst);
}

/// `BoolBinary`'s word-level 3VL kernel, both selectors (nullable arm).
/// `va`/`vb` come from `bool_bits`; `na`/`nb` from `null_bits`.
/// Writes `bool_bits[dst]` and `null_bits[dst]` at word granularity.
fn bool_and_or_word_loop(scratch: &mut EvalScratch, dst: u16, a: u16, b: u16, m: usize, is_or: bool) {
    let words = m.div_ceil(64);
    let base_a_n = a as usize * NULL_WORDS_PER_REG;
    let base_b_n = b as usize * NULL_WORDS_PER_REG;
    let base_d_n = dst as usize * NULL_WORDS_PER_REG;
    for w in 0..words {
        let va = scratch.bool_bits[base_a_n + w];
        let vb = scratch.bool_bits[base_b_n + w];
        let na = scratch.null_bits[base_a_n + w];
        let nb = scratch.null_bits[base_b_n + w];
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
        scratch.bool_bits[base_d_n + w] = result_bits;
        scratch.null_bits[base_d_n + w] = nd;
    }
}

/// Pack `src`'s truthy bits into `out`, one bit per row. Callers cut the
/// windows: indexing a scratch buffer through a base offset instead reloads the
/// `Vec` header and bounds-checks the whole buffer per row.
fn pack_truthy(src: &[i64], out: &mut [u64]) {
    for (w, block) in src.chunks(64).enumerate() {
        let mut bits: u64 = 0;
        for (j, &v) in block.iter().enumerate() {
            bits |= ((v != 0) as u64) << j;
        }
        out[w] = bits;
    }
}

/// Unpack `bool_bits[dst]` into `regs[dst]` unless `dst` is bit_only, whose
/// readers take the packed bits directly. The counterpart to
/// [`maybe_pack_bool_bits`].
fn maybe_unpack_bool_to_regs(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16) {
    if mo.prog.is_bit_only(dst as usize) {
        return;
    }
    let EvalScratch { regs, bool_bits, .. } = scratch;
    let bits = &bool_bits[dst as usize * NULL_WORDS_PER_REG..];
    let base_r = dst as usize * MORSEL;
    for (w, block) in regs[base_r..base_r + mo.m].chunks_mut(64).enumerate() {
        for (j, r) in block.iter_mut().enumerate() {
            *r = ((bits[w] >> j) & 1) as i64;
        }
    }
}

/// Bridge for producers that wrote `regs[dst]` and may have a downstream BOOL
/// consumer (or, for filters, a bit_only result_reg). Non-bool producers reach
/// a BOOL consumer through this path without restructuring their inner loop.
///
/// The two tests are `#[inline]` and the pack is not: this is called once per
/// instruction per morsel from `bin_op`/`un_op`, and on the `no_nulls` arm it is
/// a no-op — so the *test* has to fold into the caller, where inlining the pack
/// with it makes the whole thing too big for LLVM to do that. Worth 1.4 % of the
/// `expr_kernel_bench` map shape, measured.
#[inline]
fn maybe_pack_bool_bits(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16) {
    if !mo.no_nulls() && mo.prog.needs_bool_pack(dst as usize) {
        pack_bool_bits(scratch, mo.m, dst);
    }
}

fn pack_bool_bits(scratch: &mut EvalScratch, m: usize, dst: u16) {
    let EvalScratch { regs, bool_bits, .. } = scratch;
    let base_r = dst as usize * MORSEL;
    let base_b = dst as usize * NULL_WORDS_PER_REG;
    pack_truthy(
        &regs[base_r..base_r + m],
        &mut bool_bits[base_b..base_b + m.div_ceil(64)],
    );
}

/// OR a per-row failure flag into `dst`'s null words. The flags are collected a
/// byte per row and packed here in a second pass: folding the packing into the
/// value loop makes every iteration read-modify-write the same mask word, which
/// serialises the loop. On the numeric cast kernels, where that was measured,
/// the split was the difference between a scalar and a vectorised loop. The
/// string kernels reuse it for the same reason, unmeasured.
fn merge_fail_mask(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, bad: &[u8; MORSEL]) {
    let m = mo.m;
    // A failure — divide by zero, an out-of-range cast, unparsable text — is
    // exceptional, so the usual morsel has none and the vectorized scan replaces
    // the pack rather than adding to it.
    if bad[..m].iter().all(|&b| b == 0) {
        return;
    }
    // A worker abort under `panic = "abort"`, deliberately: this fires on an
    // engine misclassification, never on user data — the case
    // [`eval_str_concat`]'s totality rule governs. Dropping the flag loses a NULL.
    assert!(
        !mo.no_nulls(),
        "a kernel raised a failure flag under no_nulls: \
         its `Operands` entry does not set `makes_null`"
    );
    let base = dst as usize * NULL_WORDS_PER_REG;
    for w in 0..m.div_ceil(64) {
        let lo = w * 64;
        let n = core::cmp::min(64, m - lo);
        let mut word = 0u64;
        for j in 0..n {
            word |= (bad[lo + j] as u64) << j;
        }
        scratch.null_bits[base + w] |= word;
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
/// parameter, is `Default`-constructed once per evaluator, and outlives every
/// batch it is driven over.
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
/// the buffer table. Once per drive call, never per morsel: every region spans
/// the whole batch.
///
/// A scope function because the table is a fixed-width stack array whose borrow
/// has to outlive both [`eval_batch`] and the [`MorselOut`] built after it —
/// [`EvalScratch`] has no lifetime parameter to hold one, and `MorselOut`
/// borrows the scratch.
///
/// Filled unconditionally; only the loop is skipped for a program with no string
/// column. Handing `f` a shared static empty table on that arm instead costs
/// 0.4–0.6 % on every string shape of `expr_kernel_bench`: `cols` stops being one
/// traceable alloca, and the per-row [`StrBufs::region`] loads lose that.
pub(crate) fn with_str_bufs(prog: &ResolvedProgram, mb: &dyn BatchView, f: impl FnOnce(StrBufs<'_>)) {
    // The blob is read whatever `str_cols` says: `StrColConst` / `StrColCol`
    // reach it through their own operand without registering a column, so an
    // empty column mask is not "no blob reader".
    let blob = mb.blob();
    let mut cols: [&[u8]; STR_COL_BUFS] = [&[]; STR_COL_BUFS];
    let mut mask = prog.str_cols;
    while mask != 0 {
        let pi = mask.trailing_zeros() as usize;
        mask &= mask - 1;
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
}

/// `v`'s byte range in its buffer, degrading to an empty range when it runs past
/// the end — `gnitz_wire::german_string_content`'s corrupt-cell convention,
/// never a panic.
fn view_span(v: StrView, buf_len: usize) -> (usize, usize) {
    match gnitz_wire::blob_extent(buf_len, v.off, v.len as usize) {
        Some(r) => (r.start, r.len()),
        None => (0, 0),
    }
}

/// `v`'s bytes together with the offset they sit at. The sub-view producers
/// (SUBSTRING, TRIM) need both, and must read the offset from here rather than
/// from `v.off`: a clamped-away view reports offset 0.
fn view_bytes_at<'x>(v: StrView, arena: &'x [u8], bufs: StrBufs<'x>) -> (&'x [u8], usize) {
    let buf = bufs.region(v.src).unwrap_or(arena);
    let (o, l) = view_span(v, buf.len());
    (&buf[o..o + l], o)
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
    let (o, l) = view_span(v, bufs.region(v.src).map_or(arena.len(), <[u8]>::len));
    arena_push_span(arena, bufs, v.src, o, l);
    (start, arena.len() - start)
}

/// Append an already-resolved span to the arena. Callers that need the extent
/// for their own arithmetic resolve it once and come here, instead of paying
/// [`view_span`] a second time inside [`arena_push_view`].
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

/// Row `row` of a 16-byte German-string cell region as a view, no byte copied: a
/// long cell points into the blob, a short one at its own inline bytes, which
/// `german_string_inline` shows are the contiguous slice `cell[4..4 + len]`.
/// `buf` must be the [`StrBufs`] index holding `cells`. The out-of-range clamp is
/// applied here, so LENGTH, the compares, the transforms and the emits all agree on
/// the degraded value.
fn cell_to_view(cells: &[u8], row: usize, buf: u32, blob_len: usize) -> StrView {
    let o = row * 16;
    let cell = &cells[o..o + 16];
    match german_string_inline(cell) {
        Some(inline) => StrView::at(buf, o + 4, inline.len()),
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
    /// Row `i` of the register, widened. Each operand's magnitude is ≤ 2^64, so
    /// the sum of two cannot overflow.
    #[inline]
    fn read(self, regs: &[i64], i: usize) -> i128 {
        let v = regs[self.reg as usize * MORSEL + i];
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
/// bit_only (its producer skips the unpack to `regs`), so its truthiness comes
/// from `bool_bits`, not `regs`. `dst`'s null word is written here: `dst` is null
/// wherever the chosen branch is null.
fn select_take_mask(
    s: &mut EvalScratch,
    dst: u16,
    cond: u16,
    a: u16,
    b: u16,
    words: usize,
) -> [u64; NULL_WORDS_PER_REG] {
    let base_cond = cond as usize * NULL_WORDS_PER_REG;
    let mut take_a = [0u64; NULL_WORDS_PER_REG];
    for (w, t) in take_a.iter_mut().enumerate().take(words) {
        *t = s.bool_bits[base_cond + w] & !s.null_bits[base_cond + w];
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
/// No `no_nulls` arm: `null_bits` is unallocated there, so the fill below panics
/// rather than losing a NULL.
fn set_null_reg(s: &mut EvalScratch, mo: &Morsel<'_>, dst: u16) {
    debug_assert!(
        !mo.no_nulls(),
        "set_null_reg under no_nulls: its caller must set `makes_null`"
    );
    let m = mo.m;
    let words = m.div_ceil(64);
    let base = dst as usize * NULL_WORDS_PER_REG;
    s.null_bits[base..base + words].fill(u64::MAX);
    s.null_bits[base + words - 1] = tail_mask(m);
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
    f: impl Fn(&[u8]) -> i64,
) {
    let (d, ai) = (dst as usize, a as usize);
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        // Both windows cut to the morsel: the destination already was, and
        // relating the two is what lets LLVM drop the source's per-row bound.
        let (va, rd) = (
            &str_views[ai * MORSEL..ai * MORSEL + mo.m],
            &mut regs[d * MORSEL..d * MORSEL + mo.m],
        );
        for (i, r) in rd.iter_mut().enumerate() {
            *r = f(view_bytes(va[i], str_arena, bufs));
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
        let (va, rd) = (
            &str_views[ai * MORSEL..ai * MORSEL + mo.m],
            &mut regs[d * MORSEL..d * MORSEL + mo.m],
        );
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
        let (ra, vd) = (
            &regs[ai * MORSEL..ai * MORSEL + m],
            &mut str_views[d * MORSEL..d * MORSEL + m],
        );
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
    let (base_a, base_b, base_d) = (a as usize * MORSEL, b as usize * MORSEL, dst as usize * MORSEL);
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        // All three windows cut to the morsel: indexing the lane `Vec` directly
        // costs two bounds checks and two counter bumps per row.
        let (sa, sb) = (&str_views[base_a..base_a + mo.m], &str_views[base_b..base_b + mo.m]);
        let rd = &mut regs[base_d..base_d + mo.m];
        for (i, r) in rd.iter_mut().enumerate() {
            let va = view_bytes(sa[i], str_arena, bufs);
            let vb = view_bytes(sb[i], str_arena, bufs);
            *r = f(va, vb);
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

/// [`str_kernel`] for a closure that cannot fail. The return type is the whole
/// difference: with no `Option` there is no flag to raise, so none can be raised
/// and then dropped, and no fail mask is written or scanned — which is the 1.1 %
/// of `expr_kernel_bench`'s `str_upper` a dedicated one-string transform used to
/// buy.
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
        let (srcs, vd) = split_windows(str_views, MORSEL, strs.map(usize::from), dst as usize, mo.m);
        for (i, r) in vd.iter_mut().enumerate() {
            let views = srcs.map(|w| w[i]);
            let nums = ints.map(|r| r.read(regs, i));
            *r = f(str_arena, bufs, views, nums, i);
        }
    }
    null_or_all(scratch, mo, dst, strs.iter().copied().chain(ints.iter().map(|r| r.reg)));
}

/// `SELECT`'s value blend under a per-row take mask, over either lane array:
/// `buf[d][i] = if take_a bit i { buf[srcs[0]][i] } else { buf[srcs[1]][i] }`.
///
/// One flat row loop, the shape [`fill_null_bits_mask`] vectorizes in: a nested
/// word/row pair with a runtime `(lo, hi)` made LLVM branch on the mask bit and
/// select the base *pointer*, which is scalar and mispredicts once per row.
///
/// `select_take_mask` returns its words *by value*, so it holds no borrow while
/// this runs. The unsafe split is the one `regs`/`null_bits` already rely on,
/// sound because an instruction's register is its own index and it can only read
/// earlier ones — which covers `StrSelect` too.
#[inline]
fn blend_by_mask<T: Copy>(buf: &mut [T], srcs: [u16; 2], d: u16, take_a: &[u64], m: usize) {
    let ([ra, rb], rd) = split_windows(buf, MORSEL, srcs.map(usize::from), d as usize, m);
    for (i, r) in rd.iter_mut().enumerate() {
        *r = if (take_a[i / 64] >> (i % 64)) & 1 != 0 {
            ra[i]
        } else {
            rb[i]
        };
    }
}

/// `Select`'s value blend over string lanes instead of scalar registers.
fn eval_str_select(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, cond: u16, a: u16, b: u16) {
    let m = mo.m;
    if mo.no_nulls() {
        let base_c = cond as usize * MORSEL;
        let EvalScratch { regs, str_views, .. } = &mut *scratch;
        let ([va, vb], vd) = split_windows(str_views, MORSEL, [a as usize, b as usize], dst as usize, m);
        for (i, r) in vd.iter_mut().enumerate() {
            *r = if regs[base_c + i] != 0 { va[i] } else { vb[i] };
        }
        return;
    }
    let take_a = select_take_mask(scratch, dst, cond, a, b, m.div_ceil(64));
    blend_by_mask(&mut scratch.str_views, [a, b], dst, &take_a, m);
}

/// SUBSTRING: a sub-view of the source, the bytes never copied.
///
/// Kept apart from [`str_kernel`], whose `<1, 2>` case this is: through it,
/// the `str_substr` bench shape retires 19.6 % more instructions.
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
    let (base_s, base_d) = (si as usize * MORSEL, d as usize * MORSEL);
    let mut bad = [0u8; MORSEL];
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        for i in 0..m {
            let v = str_views[base_s + i];
            let (s, base_off) = view_bytes_at(v, str_arena, bufs);
            // Positions are 1-based and the window half-open, so both endpoints
            // clamp to `[1, past_end]`. The byte length only bounds the character
            // count; `char_offset` settles a window landing in the gap.
            let past_end = s.len() as i128 + 1;
            let lo = start.read(regs, i);
            let hi = match len {
                Some(l) => {
                    let len = l.read(regs, i);
                    bad[i] = (len < 0) as u8;
                    lo + len
                }
                None => past_end,
            };
            let (lo, hi) = (lo.clamp(1, past_end), hi.clamp(1, past_end));
            // One comparison covering a zero length, a start past the end, and a
            // window entirely below 1.
            str_views[base_d + i] = if lo >= hi {
                StrView::default()
            } else {
                let (t_lo, t_hi) = ((lo - 1) as usize, (hi - 1) as usize);
                let b_lo = char_offset(s, 0, t_lo);
                // A window ending at or past the byte length ends at the string's
                // end, since a character is ≥ one byte.
                let b_hi = if t_hi >= s.len() {
                    s.len()
                } else {
                    char_offset(s, b_lo, t_hi - t_lo)
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
    // `null_bits` read below is always in bounds.
    debug_assert!(
        !mo.no_nulls(),
        "eval_str_concat under no_nulls: `StrConcat` must set `makes_null`"
    );
    let m = mo.m;
    let mut bad = [0u8; MORSEL];
    {
        let EvalScratch { str_views, str_arena, null_bits, .. } = &mut *scratch;
        let b_null_base = b as usize * NULL_WORDS_PER_REG;
        let ([sa, sb], sd) = split_windows(str_views, MORSEL, [a as usize, b as usize], d as usize, m);
        for i in 0..m {
            let va = sa[i];
            // Under CONCAT's rule a NULL argument contributes the empty string,
            // whatever view its lane happens to carry.
            let b_is_null = skip_null && (null_bits[b_null_base + i / 64] >> (i % 64)) & 1 != 0;
            let vb = if b_is_null { StrView::default() } else { sb[i] };
            // Each operand's extent is resolved once and reused for both the
            // length sum and the copy. Widened before summing: two near-u32::MAX
            // operands wrap in u32 space, and an oversized length would trip
            // `encode_german_string`'s release assert — a worker abort, which the
            // totality rule forbids.
            let (oa, la) = view_span(va, bufs.region(va.src).map_or(str_arena.len(), <[u8]>::len));
            let (ob, lb) = view_span(vb, bufs.region(vb.src).map_or(str_arena.len(), <[u8]>::len));
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

/// One side of a German-string compare: 16-byte cells `stride` bytes apart over
/// `blob`, plus the null bit that makes the *result* null.
///
/// `stride == 0` pins every row to the same cell, which is what makes a resolved
/// constant and a payload column the same operand — the compare is one loop with
/// no per-row branch and no closure call, instead of a kernel per pairing. A
/// constant is never NULL, so its `null_bit` is 0 and contributes nothing to the
/// result's null mask.
#[derive(Clone, Copy)]
struct StrOperand<'a> {
    cells: &'a [u8],
    blob: &'a [u8],
    stride: usize,
    null_bit: u64,
}

impl<'a> StrOperand<'a> {
    /// A payload column's whole 16-byte-cell region, indexed by absolute row.
    fn column(mb: &'a dyn BatchView, blob: &'a [u8], pi_byte: u8) -> Self {
        let pi = pi_byte as usize;
        StrOperand {
            cells: mb.col_data(pi, 16),
            blob,
            stride: 16,
            null_bit: 1u64 << pi,
        }
    }

    /// The resolved constant cell, shared by every row. Its heap half, if it has
    /// one, lives in the program's own constant arena — never the batch's blob.
    fn constant(prog: &'a ResolvedProgram, cell_idx: usize) -> Self {
        StrOperand {
            cells: &prog.const_cells[cell_idx],
            blob: &prog.const_arena,
            stride: 0,
            null_bit: 0,
        }
    }

    fn cell(&self, row: usize) -> &[u8] {
        let o = row * self.stride;
        &self.cells[o..o + 16]
    }
}

/// The one string-compare kernel. The compare runs unconditionally, including on
/// rows where either operand's column is null, keeping the loop branch-free.
///
/// `#[inline(always)]` here and on [`str_cmp`]: only
/// at the call site is a constant operand's `stride` the literal 0, and only
/// then does the row loop fold `row * stride` away instead of bounds-checking
/// both cell windows per row. Left to the inliner's own judgement each predicate
/// has two call sites, loses the last-call-to-static bonus, and the loop is
/// outlined.
#[inline(always)]
fn eval_str_cmp(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    dst: u16,
    a: StrOperand<'_>,
    b: StrOperand<'_>,
    pred: impl Fn(Ordering) -> bool,
) {
    fill_null_bits_mask(scratch, dst, mo, a.null_bit | b.null_bit);
    // `StrOperand` borrows the batch and the program, never the scratch, so the
    // destination window is cut once outside the loop.
    let base_d = dst as usize * MORSEL;
    let rd = &mut scratch.regs[base_d..base_d + mo.m];
    for (i, r) in rd.iter_mut().enumerate() {
        let row = mo.start + i;
        *r = pred(compare_german_strings(a.cell(row), a.blob, b.cell(row), b.blob)) as i64;
    }
    maybe_pack_bool_bits(scratch, mo, dst);
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
        for (i, r) in rd.iter_mut().enumerate() {
            *r = f(ra[i], rb[i]);
        }
    }
    null_or2(scratch, mo, d, a, b);
    maybe_pack_bool_bits(scratch, mo, d);
}

/// Unary counterpart of [`bin_op`]: read one source register, write one, copy
/// the source null word, repack bool bits.
#[inline]
fn un_op(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, f: impl Fn(i64) -> i64) {
    {
        let ([ra], rd) = scratch.regs_split([a], d, mo.m);
        for (i, r) in rd.iter_mut().enumerate() {
            *r = f(ra[i]);
        }
    }
    null_copy1(scratch, mo, d, a);
    maybe_pack_bool_bits(scratch, mo, d);
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
        for (i, r) in rd.iter_mut().enumerate() {
            let (val, is_zero) = f(ra[i], rb[i]);
            *r = val;
            bad[i] = is_zero as u8;
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
        for (i, r) in rd.iter_mut().enumerate() {
            let (val, failed) = f(ra[i]);
            *r = val;
            bad[i] = failed as u8;
        }
    }
    null_copy1(scratch, mo, d, a);
    merge_fail_mask(scratch, mo, d, &bad);
    maybe_pack_bool_bits(scratch, mo, d);
}

/// Null-skipping 2-ary extremum. [`null_or2`]'s `a|b` rule is exactly wrong here
/// (the result is null only when BOTH operands are), so this cannot use
/// [`bin_op`]. `pick` returns true when `a` wins on value; the null-skip is
/// resolved from the two null words before it is consulted.
#[inline]
fn minmax2(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, b: u16, pick: impl Fn(i64, i64) -> bool) {
    let m = mo.m;
    let words = m.div_ceil(64);
    if mo.no_nulls() {
        let ([ra, rb], rd) = scratch.regs_split([a, b], d, m);
        for (i, r) in rd.iter_mut().enumerate() {
            let (x, y) = (ra[i], rb[i]);
            *r = if pick(x, y) { x } else { y };
        }
    } else {
        let base_a = a as usize * NULL_WORDS_PER_REG;
        let base_b = b as usize * NULL_WORDS_PER_REG;
        let mut na = [0u64; NULL_WORDS_PER_REG];
        let mut nb = [0u64; NULL_WORDS_PER_REG];
        na[..words].copy_from_slice(&scratch.null_bits[base_a..base_a + words]);
        nb[..words].copy_from_slice(&scratch.null_bits[base_b..base_b + words]);
        {
            let ([ra, rb], rd) = scratch.regs_split([a, b], d, m);
            for (i, r) in rd.iter_mut().enumerate() {
                let (x, y) = (ra[i], rb[i]);
                let a_null = (na[i / 64] >> (i % 64)) & 1 != 0;
                let b_null = (nb[i / 64] >> (i % 64)) & 1 != 0;
                // A null operand loses outright; only two live operands consult
                // `pick`. Spelled as one condition so the whole row is a select.
                *r = if !a_null && (b_null || pick(x, y)) { x } else { y };
            }
        }
        let base_d = d as usize * NULL_WORDS_PER_REG;
        for w in 0..words {
            scratch.null_bits[base_d + w] = na[w] & nb[w];
        }
    }
    maybe_pack_bool_bits(scratch, mo, d);
}

/// Dispatch a German-string compare on its operator, hoisting the six-way
/// branch out of the row loop: each arm instantiates [`eval_str_cmp`] with its
/// own `Ordering` predicate. `#[inline(always)]` for the reason stated there.
#[inline(always)]
fn str_cmp(scratch: &mut EvalScratch, mo: &Morsel<'_>, op: CmpOp, d: u16, a: StrOperand<'_>, b: StrOperand<'_>) {
    match op {
        CmpOp::Eq => eval_str_cmp(scratch, mo, d, a, b, |o| o == Ordering::Equal),
        CmpOp::Ne => eval_str_cmp(scratch, mo, d, a, b, |o| o != Ordering::Equal),
        CmpOp::Gt => eval_str_cmp(scratch, mo, d, a, b, |o| o == Ordering::Greater),
        CmpOp::Ge => eval_str_cmp(scratch, mo, d, a, b, |o| o != Ordering::Less),
        CmpOp::Lt => eval_str_cmp(scratch, mo, d, a, b, |o| o == Ordering::Less),
        CmpOp::Le => eval_str_cmp(scratch, mo, d, a, b, |o| o != Ordering::Greater),
    }
}

// ---------------------------------------------------------------------------
// eval_batch — single morsel
// ---------------------------------------------------------------------------

/// Evaluate `prog` over one morsel of `mb` (`morsel_start..morsel_start+m`).
/// Results land in `scratch.regs`; null bits in `scratch.null_bits`.
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
    let mo = Morsel {
        prog,
        null_bmp: mb.null_bmp(),
        start: morsel_start,
        m,
    };
    // Reset the string arena to the constant prefix `EvalScratch::new` installed.
    // Every view a previous morsel produced dies here, which is sound because the
    // emit phase runs inside `eval_morsels`' per-morsel callback, before the next
    // call reaches this line.
    scratch.str_arena.truncate(prog.const_arena.len());

    for instr in &prog.instrs {
        match *instr {
            // ----------------------------------------------------------------
            // Load operations
            // ----------------------------------------------------------------
            Instr::LoadPayloadInt { dst, pi, fi } => {
                // The width is a `const fn` of `fi`; derive it once per
                // instruction, outside the row loop (as `LoadPk` does).
                let col_data = mb.col_data(pi as usize, fi.width());
                let dst_reg = scratch.reg_mut(dst, m);
                // Widen `m` rows of a `SZ`-byte little-endian column into i64
                // registers. `SZ` is a compile-time constant per instantiation,
                // which is what `as_chunks` needs to vectorize.
                macro_rules! load_int {
                    ($ty:ty) => {{
                        const SZ: usize = std::mem::size_of::<$ty>();
                        let b = &col_data[morsel_start * SZ..(morsel_start + m) * SZ];
                        for (i, c) in b.as_chunks::<SZ>().0.iter().enumerate() {
                            dst_reg[i] = <$ty>::from_le_bytes(*c) as i64;
                        }
                    }};
                }
                // This match *is* `FixedInt`'s variants — total, with no wildcard,
                // which is what `FixedInt` exists for: a wide column can no longer
                // reach here, because `resolve_program` proved it could not.
                match fi {
                    // 8-byte: the one width with no widening and no signed/unsigned split —
                    // the i64 register IS the storage type, a bare bit-reinterpret. Kept inline
                    // so `load_int!` (which appends `as i64`) never emits a vacuous `i64 as i64`.
                    FixedInt::U64 | FixedInt::I64 => {
                        let b = &col_data[morsel_start * 8..(morsel_start + m) * 8];
                        for (i, c) in b.as_chunks::<8>().0.iter().enumerate() {
                            dst_reg[i] = i64::from_le_bytes(*c);
                        }
                    }
                    FixedInt::I32 => load_int!(i32),
                    FixedInt::U32 => load_int!(u32),
                    FixedInt::I16 => load_int!(i16),
                    FixedInt::U16 => load_int!(u16),
                    FixedInt::I8 => load_int!(i8),
                    FixedInt::U8 => load_int!(u8),
                }
                fill_null_bits_mask(scratch, dst, &mo, 1u64 << pi);
                maybe_pack_bool_bits(scratch, &mo, dst);
            }

            // Every float register holds an f64 image, so an F32 column widens
            // on load.
            Instr::LoadPayloadF32 { dst, pi } => {
                let col_data = mb.col_data(pi as usize, 4);
                let dst_reg = scratch.reg_mut(dst, m);
                let b = &col_data[morsel_start * 4..(morsel_start + m) * 4];
                for (i, c) in b.as_chunks::<4>().0.iter().enumerate() {
                    let bits = u32::from_le_bytes(*c);
                    dst_reg[i] = encode_f64(f32::from_bits(bits) as f64);
                }
                fill_null_bits_mask(scratch, dst, &mo, 1u64 << pi);
                maybe_pack_bool_bits(scratch, &mo, dst);
            }

            // PK-region load: undo the OPK encoding (big-endian, sign flipped).
            // Unswitched on `fi` so each width reads a fixed-size array and
            // becomes one load plus a byte swap; `decode_opk_i64` takes the width
            // as a slice length, which keeps it a per-row byte reconstruction.
            Instr::LoadPk { dst, off, fi } => {
                let (pk, stride) = (pk_region, pk_stride);
                let start = morsel_start * stride + off as usize;
                let dst_reg = scratch.reg_mut(dst, m);
                macro_rules! load_pk {
                    ($w:expr, |$c:ident| $body:expr) => {{
                        const W: usize = $w;
                        let cells = &pk[start..start + (m - 1) * stride + W];
                        for (r, k) in dst_reg.iter_mut().zip((0..m).map(|i| i * stride)) {
                            let $c: &[u8; W] = cells[k..k + W].try_into().unwrap();
                            *r = $body;
                        }
                    }};
                }
                match fi {
                    FixedInt::U8 => load_pk!(1, |c| c[0] as i64),
                    FixedInt::I8 => load_pk!(1, |c| (c[0] ^ 0x80) as i8 as i64),
                    FixedInt::U16 => load_pk!(2, |c| u16::from_be_bytes(*c) as i64),
                    FixedInt::I16 => load_pk!(2, |c| (u16::from_be_bytes(*c) ^ 0x8000) as i16 as i64),
                    FixedInt::U32 => load_pk!(4, |c| u32::from_be_bytes(*c) as i64),
                    FixedInt::I32 => load_pk!(4, |c| (u32::from_be_bytes(*c) ^ 0x8000_0000) as i32 as i64),
                    FixedInt::U64 => load_pk!(8, |c| u64::from_be_bytes(*c) as i64),
                    FixedInt::I64 => load_pk!(8, |c| (u64::from_be_bytes(*c) ^ (1u64 << 63)) as i64),
                }
                scratch.clear_null_reg(&mo, dst);
                maybe_pack_bool_bits(scratch, &mo, dst);
            }

            // ----------------------------------------------------------------
            // Integer arithmetic
            // ----------------------------------------------------------------
            // The operator match is OUTSIDE the row loop, so each arm is its own
            // branch-free loop.
            Instr::IntArith { op, dst, a, b, signed } => match op {
                IntArithOp::Add => bin_op(scratch, &mo, dst, a, b, |x, y| x.wrapping_add(y)),
                IntArithOp::Sub => bin_op(scratch, &mo, dst, a, b, |x, y| x.wrapping_sub(y)),
                IntArithOp::Mul => bin_op(scratch, &mo, dst, a, b, |x, y| x.wrapping_mul(y)),
                IntArithOp::Div => int_divmod::<false>(scratch, &mo, dst, a, b, signed),
                IntArithOp::Mod => int_divmod::<true>(scratch, &mo, dst, a, b, signed),
            },
            Instr::IntUnary { op, dst, a, signed } => match op {
                IntUnaryOp::Neg => un_op(scratch, &mo, dst, a, |x| x.wrapping_neg()),
                IntUnaryOp::Abs => un_op(scratch, &mo, dst, a, |x| x.wrapping_abs()),
                IntUnaryOp::Sign if signed => un_op(scratch, &mo, dst, a, |x| x.signum()),
                IntUnaryOp::Sign => un_op(scratch, &mo, dst, a, |x| (x != 0) as i64),
            },
            // `micros` is loop-invariant, so it is unswitched out of the kernel
            // like every neighbouring selector: each arm folds the unused half of
            // the day/time split, and the day arm folds its zero time-of-day
            // through the hour/minute/second ops. The one op that can NULL is the
            // only one that pays for the fail mask.
            Instr::Calendar { op, dst, a, micros } => match (op, micros) {
                (CalendarOp::ToMicros, _) => unary_null_like(scratch, &mo, dst, a, calendar::days_to_micros),
                (op, true) => un_op(scratch, &mo, dst, a, |x| calendar::eval(op, x, true)),
                (op, false) => un_op(scratch, &mo, dst, a, |x| calendar::eval(op, x, false)),
            },
            Instr::FloatUnary { op, dst, a } => match op {
                FloatUnaryOp::Neg => un_op(scratch, &mo, dst, a, |x| encode_f64(-decode_f64(x))),
                FloatUnaryOp::Abs => un_op(scratch, &mo, dst, a, |x| encode_f64(decode_f64(x).abs())),
                FloatUnaryOp::Floor => un_op(scratch, &mo, dst, a, |x| encode_f64(decode_f64(x).floor())),
                FloatUnaryOp::Ceil => un_op(scratch, &mo, dst, a, |x| encode_f64(decode_f64(x).ceil())),
                FloatUnaryOp::Round => un_op(scratch, &mo, dst, a, |x| encode_f64(decode_f64(x).round_ties_even())),
                FloatUnaryOp::Trunc => un_op(scratch, &mo, dst, a, |x| encode_f64(decode_f64(x).trunc())),
                FloatUnaryOp::Sqrt => un_op(scratch, &mo, dst, a, |x| encode_f64(decode_f64(x).sqrt())),
                FloatUnaryOp::Ln => un_op(scratch, &mo, dst, a, |x| encode_f64(decode_f64(x).ln())),
                FloatUnaryOp::Log10 => un_op(scratch, &mo, dst, a, |x| encode_f64(decode_f64(x).log10())),
                FloatUnaryOp::Exp => un_op(scratch, &mo, dst, a, |x| encode_f64(decode_f64(x).exp())),
                // `partial_cmp` against zero: -0.0 and +0.0 are `Equal`, NaN is
                // `None` and stays NaN, as PostgreSQL spells it.
                FloatUnaryOp::Sign => un_op(scratch, &mo, dst, a, |x| {
                    encode_f64(decode_f64(x).partial_cmp(&0.0).map_or(f64::NAN, |o| o as i32 as f64))
                }),
            },
            // A finite source whose rounded result is not finite overflowed f32's
            // range. Testing the ROUNDED value (not `|x| > f32::MAX`) keeps the
            // 2^28-1 doubles just above f32::MAX that round down to it.
            Instr::FloatToF32 { dst, a } => unary_null_like(scratch, &mo, dst, a, |x| {
                let f = decode_f64(x);
                let v32 = f as f32;
                (encode_f64(v32 as f64), f.is_finite() && v32.is_infinite())
            }),
            Instr::IntCast { dst, a, fi, src_signed } => {
                let (lo, hi, hi_u) = int_cast_bounds(fi);
                if src_signed {
                    unary_null_like(scratch, &mo, dst, a, |x| (x, x < lo || x > hi))
                } else {
                    unary_null_like(scratch, &mo, dst, a, |x| (x, (x as u64) > hi_u))
                }
            }
            // Truncate toward zero, then range-check in f64: NaN fails every
            // comparison and so fails the check, as ±inf and out-of-range do.
            Instr::FloatToInt { dst, a, fi } => {
                let (flo, fhi) = float_to_int_bounds(fi);
                if fi == FixedInt::U64 {
                    unary_null_like(scratch, &mo, dst, a, |x| {
                        let t = decode_f64(x).trunc();
                        let ok = t >= flo && t < fhi;
                        (if ok { t as u64 as i64 } else { 0 }, !ok)
                    })
                } else {
                    unary_null_like(scratch, &mo, dst, a, |x| {
                        let t = decode_f64(x).trunc();
                        let ok = t >= flo && t < fhi;
                        (if ok { t as i64 } else { 0 }, !ok)
                    })
                }
            }
            Instr::IntMinMax2 { dst, a, b, is_max, signed } => match (is_max, signed) {
                (true, true) => minmax2(scratch, &mo, dst, a, b, |x, y| x > y),
                (false, true) => minmax2(scratch, &mo, dst, a, b, |x, y| x < y),
                (true, false) => minmax2(scratch, &mo, dst, a, b, |x, y| (x as u64) > (y as u64)),
                (false, false) => minmax2(scratch, &mo, dst, a, b, |x, y| (x as u64) < (y as u64)),
            },
            // `total_cmp` and nothing else: -0.0 and +0.0 are `==`-equal but
            // total_cmp-distinct, so an `==`-based pick would let operand order
            // decide which bit pattern survives.
            Instr::FloatMinMax2 { dst, a, b, is_max } => {
                if is_max {
                    minmax2(scratch, &mo, dst, a, b, |x, y| {
                        decode_f64(x).total_cmp(&decode_f64(y)).is_gt()
                    })
                } else {
                    minmax2(scratch, &mo, dst, a, b, |x, y| {
                        decode_f64(x).total_cmp(&decode_f64(y)).is_lt()
                    })
                }
            }

            // ----------------------------------------------------------------
            // Integer set membership (col IN (…) as one opcode)
            // ----------------------------------------------------------------
            // Binary-search each row's i64 register image in the pool `resolve`
            // sorted (duplicates left in place — `binary_search` is correct over
            // them). `LoadPayloadInt` writes every row's register
            // (NULL rows too, tracked in `null_bits`), so the search reads a real
            // i64 for all rows and the NULL-row result is masked by `null_copy1`
            // — exactly how the int negate handles a NULL row.
            Instr::IntInSet { dst, value_reg, set_idx } => {
                let set = &prog.int_sets[set_idx as usize]; // decoded once, sorted ascending
                un_op(scratch, &mo, dst, value_reg, |x| set.binary_search(&x).is_ok() as i64)
            }

            // ----------------------------------------------------------------
            // Float arithmetic
            // ----------------------------------------------------------------
            Instr::FloatArith { op, dst, a, b } => match op {
                FloatArithOp::Add => bin_op(scratch, &mo, dst, a, b, |x, y| {
                    encode_f64(decode_f64(x) + decode_f64(y))
                }),
                FloatArithOp::Sub => bin_op(scratch, &mo, dst, a, b, |x, y| {
                    encode_f64(decode_f64(x) - decode_f64(y))
                }),
                FloatArithOp::Mul => bin_op(scratch, &mo, dst, a, b, |x, y| {
                    encode_f64(decode_f64(x) * decode_f64(y))
                }),
                FloatArithOp::Pow => bin_op(scratch, &mo, dst, a, b, |x, y| {
                    encode_f64(decode_f64(x).powf(decode_f64(y)))
                }),
                FloatArithOp::Div => div_like(scratch, &mo, dst, a, b, |x, y| {
                    let (fa, fb) = (decode_f64(x), decode_f64(y));
                    let is_zero = fb == 0.0;
                    let fb_safe = if is_zero { 1.0 } else { fb };
                    (encode_f64(fa / fb_safe), is_zero)
                }),
            },

            // ----------------------------------------------------------------
            // Integer comparisons
            // ----------------------------------------------------------------
            // `signed: false` compares the raw bits as u64 so values >= 2^63 order
            // correctly. Branch on op/signed outside the per-row loop.
            Instr::Cmp { op, dst, a, b, signed } => match op {
                CmpOp::Eq => bin_op(scratch, &mo, dst, a, b, |x, y| (x == y) as i64),
                CmpOp::Ne => bin_op(scratch, &mo, dst, a, b, |x, y| (x != y) as i64),
                CmpOp::Gt if signed => bin_op(scratch, &mo, dst, a, b, |x, y| (x > y) as i64),
                CmpOp::Gt => bin_op(scratch, &mo, dst, a, b, |x, y| ((x as u64) > (y as u64)) as i64),
                CmpOp::Ge if signed => bin_op(scratch, &mo, dst, a, b, |x, y| (x >= y) as i64),
                CmpOp::Ge => bin_op(scratch, &mo, dst, a, b, |x, y| ((x as u64) >= (y as u64)) as i64),
                CmpOp::Lt if signed => bin_op(scratch, &mo, dst, a, b, |x, y| (x < y) as i64),
                CmpOp::Lt => bin_op(scratch, &mo, dst, a, b, |x, y| ((x as u64) < (y as u64)) as i64),
                CmpOp::Le if signed => bin_op(scratch, &mo, dst, a, b, |x, y| (x <= y) as i64),
                CmpOp::Le => bin_op(scratch, &mo, dst, a, b, |x, y| ((x as u64) <= (y as u64)) as i64),
            },

            // ----------------------------------------------------------------
            // Float comparisons
            // ----------------------------------------------------------------
            Instr::FCmp { op, dst, a, b } => match op {
                CmpOp::Eq => bin_op(scratch, &mo, dst, a, b, |x, y| (decode_f64(x) == decode_f64(y)) as i64),
                CmpOp::Ne => bin_op(scratch, &mo, dst, a, b, |x, y| (decode_f64(x) != decode_f64(y)) as i64),
                CmpOp::Gt => bin_op(scratch, &mo, dst, a, b, |x, y| (decode_f64(x) > decode_f64(y)) as i64),
                CmpOp::Ge => bin_op(scratch, &mo, dst, a, b, |x, y| (decode_f64(x) >= decode_f64(y)) as i64),
                CmpOp::Lt => bin_op(scratch, &mo, dst, a, b, |x, y| (decode_f64(x) < decode_f64(y)) as i64),
                CmpOp::Le => bin_op(scratch, &mo, dst, a, b, |x, y| (decode_f64(x) <= decode_f64(y)) as i64),
            },

            // ----------------------------------------------------------------
            // Boolean 3VL
            // ----------------------------------------------------------------
            // The operator branch stays outside the row loop on both arms: each
            // `bin_op` instantiation is its own branch-free loop.
            Instr::BoolBinary { dst, a, b, is_or } => {
                if mo.no_nulls() {
                    if is_or {
                        bin_op(scratch, &mo, dst, a, b, |x, y| ((x != 0) || (y != 0)) as i64);
                    } else {
                        bin_op(scratch, &mo, dst, a, b, |x, y| ((x != 0) && (y != 0)) as i64);
                    }
                } else {
                    // Nullable arm: word-level u64 3VL on packed truthy bits.
                    // Upstream producers populate `bool_bits` via
                    // `needs_bool_pack`; the unpack to `regs[dst]` is skipped
                    // for bit_only destinations whose only readers are BOOL.
                    bool_and_or_word_loop(scratch, dst, a, b, m, is_or);
                    maybe_unpack_bool_to_regs(scratch, &mo, dst);
                }
            }
            Instr::BoolNot { dst, a } => {
                if mo.no_nulls() {
                    un_op(scratch, &mo, dst, a, |x| (x == 0) as i64);
                } else {
                    let words = m.div_ceil(64);
                    let base_a = a as usize * NULL_WORDS_PER_REG;
                    let base_d = dst as usize * NULL_WORDS_PER_REG;
                    for w in 0..words {
                        let va = scratch.bool_bits[base_a + w];
                        let na = scratch.null_bits[base_a + w];
                        // 3VL: NOT NULL = NULL. Result truthy = !va & !na (cleared
                        // in null positions); null-bits unchanged.
                        scratch.bool_bits[base_d + w] = !va & !na;
                        scratch.null_bits[base_d + w] = na;
                    }
                    maybe_unpack_bool_to_regs(scratch, &mo, dst);
                }
            }

            // ----------------------------------------------------------------
            // IS NULL / IS NOT NULL
            // ----------------------------------------------------------------
            Instr::IsNull { dst, pi, invert } => eval_is_null(scratch, &mo, dst, pi, invert),
            Instr::IsNullReg { dst, a, invert } => eval_is_null_reg(scratch, &mo, dst, a, invert),

            // ----------------------------------------------------------------
            // Type cast. `signed: false` reinterprets the register as u64 first
            // so values >= 2^63 cast to the correct large positive float.
            // ----------------------------------------------------------------
            Instr::IntToFloat { dst, a, signed } => match signed {
                true => un_op(scratch, &mo, dst, a, |x| encode_f64(x as f64)),
                false => un_op(scratch, &mo, dst, a, |x| encode_f64(x as u64 as f64)),
            },

            // ----------------------------------------------------------------
            // Conditional select (SQL CASE blend) / manufactured NULL
            // ----------------------------------------------------------------
            // Rows where `cond` is non-NULL and truthy take `a`'s value + null
            // bit; all others (false OR NULL cond) take `b`'s.
            Instr::Select { dst, cond, a, b } => {
                if mo.no_nulls() {
                    // Fast arm: cond truthiness lives in `regs` (no bool_bits in
                    // no_nulls mode); a straight per-row blend.
                    let ([rc, ra, rb], rd) = scratch.regs_split([cond, a, b], dst, m);
                    for i in 0..m {
                        rd[i] = if rc[i] != 0 { ra[i] } else { rb[i] };
                    }
                } else {
                    let take_a = select_take_mask(scratch, dst, cond, a, b, m.div_ceil(64));
                    blend_by_mask(&mut scratch.regs, [a, b], dst, &take_a, m);
                    maybe_pack_bool_bits(scratch, &mo, dst);
                }
            }
            // Manufacture a NULL: zero the value lane, set the null bit for every
            // live row. Only ever reached on the nullable arm (LoadNull forces
            // `no_nulls` off via its `Operands::makes_null` flag).
            Instr::LoadNull { dst } => {
                let base_d = dst as usize * MORSEL;
                scratch.regs[base_d..base_d + m].fill(0);
                set_null_reg(scratch, &mo, dst);
                maybe_pack_bool_bits(scratch, &mo, dst);
            }

            // ----------------------------------------------------------------
            // String comparisons (column vs constant / column vs column)
            // ----------------------------------------------------------------
            // Both arms are the same compare over two operands; only where
            // operand B comes from differs. `str_cmp` keeps the three-way
            // operator branch outside the row loop, so each operator gets its own
            // branch-free kernel.
            Instr::StrColConst { op, dst, pi, cell_idx } => str_cmp(
                scratch,
                &mo,
                op,
                dst,
                StrOperand::column(mb, bufs.blob, pi),
                StrOperand::constant(prog, cell_idx as usize),
            ),
            Instr::StrColCol { op, dst, pi_a, pi_b } => str_cmp(
                scratch,
                &mo,
                op,
                dst,
                StrOperand::column(mb, bufs.blob, pi_a),
                StrOperand::column(mb, bufs.blob, pi_b),
            ),

            // ----------------------------------------------------------------
            // String registers
            // ----------------------------------------------------------------
            // Every arm writes all `m` lanes, following the `unary_null_like`
            // discipline: a failed row gets a defined-but-irrelevant view plus a
            // null bit, never an untouched lane. Skipping NULL rows would leave
            // a previous morsel's view behind, and it would resolve against the
            // refilled arena.
            Instr::LoadColStr { dst, pi } => {
                let blob_len = bufs.blob.len();
                // The buffer slot *is* the payload slot: `with_str_bufs` filled
                // it, because `resolve` recorded `pi` in the same mask.
                let buf = SRC_COL_BASE + pi as u32;
                let cells = bufs.region(buf).expect("a column slot names a buffer");
                let base_d = dst as usize * MORSEL;
                // Destination `base_d + i`, source `morsel_start + i`: the two
                // offsets differ, so the lane window is cut and walked rather
                // than indexed twice.
                for (i, v) in scratch.str_views[base_d..base_d + m].iter_mut().enumerate() {
                    *v = cell_to_view(cells, morsel_start + i, buf, blob_len);
                }
                fill_null_bits_mask(scratch, dst, &mo, 1u64 << pi);
            }

            // The `LoadNull` shape: a defined empty lane plus the null bit for
            // every live row, with the tail word masked so stale high bits never
            // read as null.
            Instr::LoadNullStr { dst } => {
                let base_d = dst as usize * MORSEL;
                scratch.str_views[base_d..base_d + m].fill(StrView::default());
                set_null_reg(scratch, &mo, dst);
            }

            Instr::StrSelect { dst, cond, a, b } => eval_str_select(scratch, &mo, dst, cond, a, b),

            // The operator branch stays outside the row loop, as `str_cmp` does
            // for the `StrColConst` / `StrColCol` pair.
            Instr::StrCmp { op, dst, a, b } => match op {
                CmpOp::Eq => str2_to_scalar(scratch, &mo, bufs, dst, a, b, |x, y| (x == y) as i64),
                CmpOp::Ne => str2_to_scalar(scratch, &mo, bufs, dst, a, b, |x, y| (x != y) as i64),
                CmpOp::Gt => str2_to_scalar(scratch, &mo, bufs, dst, a, b, |x, y| (x > y) as i64),
                CmpOp::Ge => str2_to_scalar(scratch, &mo, bufs, dst, a, b, |x, y| (x >= y) as i64),
                CmpOp::Lt => str2_to_scalar(scratch, &mo, bufs, dst, a, b, |x, y| (x < y) as i64),
                CmpOp::Le => str2_to_scalar(scratch, &mo, bufs, dst, a, b, |x, y| (x <= y) as i64),
            },

            // Unswitched on `chars`, so each measure is its own monomorphised loop.
            Instr::StrLen { dst, a, chars } => {
                if chars {
                    str_to_scalar(scratch, &mo, bufs, dst, a, |s| char_count(s) as i64);
                } else {
                    str_to_scalar(scratch, &mo, bufs, dst, a, |s| s.len() as i64);
                }
            }

            // A fresh copy in the arena, folded in place. Unswitched on `upper`,
            // as `StrLen` and `int_divmod` are, so the direction is a constant
            // inside the byte loop rather than a test per byte.
            Instr::StrCase { dst, a, upper } => {
                macro_rules! fold_case {
                    (|$b:ident| $hit:expr) => {
                        str_kernel_total(scratch, &mo, bufs, dst, [a], [], |arena, bufs, [v], []| {
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

            Instr::StrSubstr { dst, src, start, len } => eval_str_substr(scratch, &mo, bufs, dst, src, start, len),

            // A sub-view of the source: the bytes are not copied, only the
            // offset and length narrowed.
            Instr::StrTrim { dst, a, mode, set_idx } => {
                let set = &prog.trim_sets[set_idx as usize];
                str_kernel_total(scratch, &mo, bufs, dst, [a], [], |arena, bufs, [v], []| {
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

            Instr::StrLike { dst, src, matcher_idx } => {
                let matcher = &prog.like_matchers[matcher_idx as usize];
                str_to_scalar(scratch, &mo, bufs, dst, src, |s| matcher.matches(s) as i64);
            }

            Instr::StrConcat { dst, a, b, skip_null } => eval_str_concat(scratch, &mo, bufs, dst, a, b, skip_null),

            // The three numeric→text arms share one loop, monomorphised per
            // closure so the signed/float branch stays outside it.
            // `unsigned_abs` rather than `-v`: `i64::MIN` has no positive i64.
            Instr::IntToStr { dst, a, signed } => {
                if signed {
                    num_to_str(scratch, &mo, dst, a, |arena, v| {
                        arena_push_int(arena, v.unsigned_abs(), v < 0)
                    });
                } else {
                    num_to_str(scratch, &mo, dst, a, |arena, v| arena_push_int(arena, v as u64, false));
                }
            }

            Instr::FloatToStr { dst, a } => {
                num_to_str(scratch, &mo, dst, a, |arena, v| arena_push_float(arena, decode_f64(v)));
            }

            Instr::StrToInt { dst, a, fi } => {
                let (lo, hi) = fi.range();
                // A U64 value above i64::MAX narrows to its own bit pattern,
                // which is what the register holds; the resolve-time U64
                // tracking makes downstream reads agree.
                str_parse_to_scalar(scratch, &mo, bufs, dst, a, |s| {
                    parse_decimal_i128(s).filter(|v| *v >= lo && *v <= hi).map(|v| v as i64)
                });
            }

            Instr::StrToFloat { dst, a } => {
                str_parse_to_scalar(scratch, &mo, bufs, dst, a, |s| {
                    std::str::from_utf8(s.trim_ascii())
                        .ok()
                        .and_then(|t| t.parse::<f64>().ok())
                        .map(encode_f64)
                });
            }
            Instr::StrPos { dst, hay, needle } => {
                str2_to_scalar(scratch, &mo, bufs, dst, hay, needle, |h, n| match find(h, n, false) {
                    Some(off) => char_count(&h[..off]) as i64 + 1,
                    None => 0,
                })
            }
            Instr::StrSide { dst, src, n, left } => {
                str_kernel_total(scratch, &mo, bufs, dst, [src], [n], |arena, bufs, [v], [n]| {
                    let (s, base) = view_bytes_at(v, arena, bufs);
                    // Clamped to one past the byte length, so it fits a `usize`.
                    let n_abs = n.unsigned_abs().min(s.len() as u128 + 1) as usize;
                    // A non-negative LEFT and a negative RIGHT count from the start.
                    let counts_from_start = (n >= 0) == left;
                    // Walked from the end the count is measured from, so
                    // `RIGHT(s, 10)` costs ten characters, not two passes.
                    let cut = if counts_from_start {
                        char_offset(s, 0, n_abs)
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
            Instr::StrReverse { dst, a } => {
                str_kernel_total(scratch, &mo, bufs, dst, [a], [], |arena, bufs, [v], []| {
                    let (o, l) = arena_push_view(arena, bufs, v);
                    reverse_chars(&mut arena[o..o + l]);
                    StrView::arena(o, l)
                })
            }
            Instr::StrReplace { dst, s, from, to } => {
                str_kernel(
                    scratch,
                    &mo,
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
            Instr::StrPad { dst, s, n, fill, left } => {
                str_kernel(scratch, &mo, bufs, dst, [s, fill], [n], |arena, bufs, [vs, vf], [n]| {
                    let (s, base) = view_bytes_at(vs, arena, bufs);
                    if n <= 0 {
                        return Some(StrView::default());
                    }
                    let s_chars = char_count(s);
                    // A width at or below the subject's own length truncates —
                    // the LEFT of `n` characters, a sub-view.
                    if n <= s_chars as i128 {
                        return Some(StrView::at(vs.src, base, char_offset(s, 0, n as usize)));
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
                    let rem_bytes = char_offset(fill, 0, rem);
                    let total = s.len() as u128 + (whole * fill.len()) as u128 + rem_bytes as u128;
                    if total > u32::MAX as u128 {
                        return None;
                    }
                    let (s_len, fill_len) = (s.len(), fill.len());
                    let out = arena.len();
                    let push_pad = |arena: &mut Vec<u8>| {
                        for _ in 0..whole {
                            arena_push_span(arena, bufs, vf.src, fill_base, fill_len);
                        }
                        arena_push_span(arena, bufs, vf.src, fill_base, rem_bytes);
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
            Instr::StrSplitPart { dst, s, delim, n } => str_kernel(
                scratch,
                &mo,
                bufs,
                dst,
                [s, delim],
                [n],
                |arena, bufs, [vs, vd], [n]| {
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
                },
            ),
        }
    }
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;
