//! The morsel evaluator: `Evaluator` runs a resolved program over a batch one
//! morsel (up to `MORSEL` rows) at a time.
//!
//! `eval_batch` applies each instruction as a columnar loop over register
//! buffers, reading the batch's regions from a table `Evaluator::eval_morsels`
//! fetches once per call. Base pointers are hoisted outside the inner loop,
//! which is what lets LLVM auto-vectorize the arithmetic opcodes. The loops that
//! end in packed bits or start from them are [`crate::simd`]'s; the string
//! kernels do not vectorize.
//!
//! [`FloatText`] is the float spelling `FloatToStr` writes, public so a client
//! prints a float as the engine casts it.

use std::cmp::Ordering;
use std::fmt;

use crate::chars::{char_count, char_offset, char_offset_back, reverse_chars};
use crate::map::{Emit, EmitStore};
use crate::program::{FloatUnaryOp, IntArithOp, IntOrder, IntReg, IntUnaryOp};
use crate::search::{fields, find};
use crate::simd::{self, decode_f64, encode_f64, LanePred, Level};
use crate::{calendar, BatchView, CalendarOp, CmpOp, FloatArithOp, Instr, ResolvedProgram};
use gnitz_wire::{
    compare_german_strings, german_string_content, german_string_heap, german_string_inline, low_bits_mask,
    null_word_get, FixedInt, Heap,
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

/// Live bits of the last of `m.div_ceil(64)` words, `m ≥ 1`: all-ones when the
/// morsel fills it, so a tail mask is written unconditionally.
fn tail_mask(m: usize) -> u64 {
    low_bits_mask((m - 1) % 64 + 1)
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
struct RegBuf {
    /// A NULL row's lane holds whatever its branch-free kernel computed. Lanes
    /// past a morsel's last row hold no row; a string register uses none.
    lanes: [i64; MORSEL],
    /// Zero for the scratch's life outside [`ResolvedProgram::null_regs`]. Bits
    /// past a morsel's last row are undefined.
    nulls: [u64; NULL_WORDS_PER_REG],
    /// Packed truth bits, what a boolean reader reads. Bits past a morsel's last
    /// row are undefined.
    bools: [u64; NULL_WORDS_PER_REG],
}

/// A resolved program and the register file built for it.
pub(crate) struct Evaluator {
    prog: ResolvedProgram,
    scratch: EvalScratch,
}

struct EvalScratch {
    regs: Vec<RegBuf>,
    /// One view per row, per string register.
    str_views: Vec<[StrView; MORSEL]>,
    /// The program's constant bytes, then the bytes this morsel computed.
    str_arena: Vec<u8>,
    /// The instruction set [`crate::simd`]'s kernels run at.
    level: Level,
}

/// String register `reg`'s first `m` views, cut from the flattened file: an
/// instruction a row cheaper than slicing its own array (`expr_kernel_bench`).
#[inline(always)]
fn str_lane(views: &[[StrView; MORSEL]], reg: usize, m: usize) -> &[StrView] {
    &views.as_flattened()[reg * MORSEL..][..m]
}

#[inline(always)]
fn str_lane_mut(views: &mut [[StrView; MORSEL]], reg: usize, m: usize) -> &mut [StrView] {
    &mut views.as_flattened_mut()[reg * MORSEL..][..m]
}

/// `N` shared registers plus one mutable register of the same file. Every
/// source lies below `d`: a register is the index of its writing instruction,
/// and `LogicalProgram::new` rejects an operand not written before its
/// reader.
#[inline(always)]
fn split_regs<T, const N: usize>(buf: &mut [T], srcs: [u16; N], d: u16) -> ([&T; N], &mut T) {
    let (written, rest) = buf.split_at_mut(d as usize);
    (srcs.map(|s| &written[s as usize]), &mut rest[0])
}

impl Evaluator {
    pub(crate) fn new(prog: ResolvedProgram) -> Self {
        let scratch = EvalScratch::new(&prog, Level::new());
        Evaluator { prog, scratch }
    }

    /// Evaluate rows `start..start + n` of `mb` a morsel at a time, handing `f`
    /// each morsel's registers and its first row's offset from `start`.
    #[inline]
    pub(crate) fn eval_morsels(
        &mut self,
        mb: &dyn BatchView,
        start: usize,
        n: usize,
        mut f: impl FnMut(usize, &MorselOut<'_>),
    ) {
        debug_assert!(
            start + n <= mb.row_count(),
            "eval window {start}..{} past the batch",
            start + n
        );
        let Evaluator { prog, scratch } = self;
        // Hoisted: LLVM cannot fold repeated `&dyn` calls.
        let mut cols: [&[u8]; PAYLOAD_SLOTS] = [&[]; PAYLOAD_SLOTS];
        for &(pi, width) in &prog.payload_reads {
            cols[pi as usize] = mb.col_data(pi as usize, width as usize);
        }
        let bufs = Regions { blob: mb.blob(), cols: &cols };
        let null_and_pk = (mb.null_bmp(), mb.pk_region());
        for rel_start in (0..n).step_by(MORSEL) {
            let m = MORSEL.min(n - rel_start);
            eval_batch(prog, bufs, null_and_pk, start + rel_start, m, scratch);
            debug_assert!(
                (scratch.regs.iter().enumerate())
                    .all(|(r, reg)| prog.may_null(r as u16) || reg.nulls == [0; NULL_WORDS_PER_REG]),
                "a null word was written outside null_regs",
            );
            f(rel_start, &MorselOut { scratch, bufs, m });
        }
    }

    #[cfg(test)]
    pub(crate) fn prog(&self) -> &ResolvedProgram {
        &self.prog
    }

    /// Test hook: track the null words of the registers in `null_regs`, its
    /// kernels at `level`. Sound for any superset of the resolved mask.
    #[cfg(test)]
    pub(crate) fn set_arm(&mut self, null_regs: u64, level: Level) {
        self.prog.null_regs = null_regs;
        self.scratch = EvalScratch::new(&self.prog, level);
    }
}

impl EvalScratch {
    fn new(prog: &ResolvedProgram, level: Level) -> Self {
        const ZERO: RegBuf = RegBuf {
            lanes: [0; MORSEL],
            nulls: [0; NULL_WORDS_PER_REG],
            bools: [0; NULL_WORDS_PER_REG],
        };
        let mut scratch = EvalScratch {
            regs: vec![ZERO; prog.num_regs()],
            str_views: vec![[StrView::default(); MORSEL]; prog.str_lanes as usize],
            str_arena: prog.const_arena.clone(),
            level,
        };
        scratch.install_consts(prog);
        scratch
    }

    /// Write the constant registers once, at full [`MORSEL`] width: no
    /// instruction writes them per morsel.
    fn install_consts(&mut self, prog: &ResolvedProgram) {
        for &(dst, val) in &prog.const_regs {
            let r = &mut self.regs[dst as usize];
            r.lanes.fill(val);
            if prog.needs_bool_pack(dst) {
                simd::truthy_bits(self.level, &r.lanes, &mut r.bools);
            }
        }
        for &(dst, off, len) in &prog.const_str_regs {
            self.str_views[dst as usize].fill(StrView::at(SRC_ARENA, off as usize, len as usize));
        }
        for dst in gnitz_wire::BitIter(prog.const_nulls) {
            self.regs[dst].nulls.fill(u64::MAX);
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
}

/// One morsel's results, read out of the register file.
pub(crate) struct MorselOut<'a> {
    scratch: &'a EvalScratch,
    bufs: Regions<'a>,
    m: usize,
}

impl MorselOut<'_> {
    /// How many rows this morsel covers (always ≥ 1).
    #[inline(always)]
    pub(crate) fn rows(&self) -> usize {
        self.m
    }

    /// Register `reg` as a filter verdict, one bit per row into `out`: set where
    /// the value is true and not NULL. Bits past the morsel's last row are
    /// undefined.
    pub(crate) fn filter_words(&self, reg: usize, out: &mut [u64]) {
        debug_assert_eq!(out.len(), self.m.div_ceil(64), "one bit per row");
        let r = &self.scratch.regs[reg];
        for (w, word) in out.iter_mut().enumerate() {
            *word = r.bools[w] & !r.nulls[w];
        }
    }

    /// Register `reg`'s values for this morsel's rows, in row order.
    #[inline(always)]
    pub(crate) fn reg_values(&self, reg: usize) -> &[i64] {
        &self.scratch.regs[reg].lanes[..self.m]
    }

    /// Write emit `e` for this morsel's rows into its slot from `row0`: each
    /// value's low bytes, its truth, or one German-string cell per row with long
    /// bodies appended to the slot's heap. NULL rows are zeroed with their bit set
    /// in `nb`.
    pub(crate) fn emit(&self, e: &Emit, (col, nb, blob): (&mut [u8], &mut [u8], &mut Vec<u8>), row0: usize) {
        let w = e.store.bytes();
        let win = &mut col[row0 * w..(row0 + self.m) * w];
        match e.store {
            EmitStore::Str => self.write_str_cells(e.reg, win, blob),
            EmitStore::W8 => win.copy_from_slice(gnitz_wire::as_le_bytes(self.reg_values(e.reg))),
            EmitStore::W4 => Self::narrow_cells(win, self.reg_values(e.reg), |v| (v as u32).to_le_bytes()),
            EmitStore::W2 => Self::narrow_cells(win, self.reg_values(e.reg), |v| (v as u16).to_le_bytes()),
            EmitStore::W1 => Self::narrow_cells(win, self.reg_values(e.reg), |v| (v as u8).to_le_bytes()),
            EmitStore::Truth => Self::narrow_cells(win, self.reg_values(e.reg), |v| [u8::from(v != 0)]),
        }
        self.write_null_rows(e.reg, win, w, nb, row0, e.slot);
    }

    /// Each value's low `W` bytes, as `to_bytes` truncates them, into its own cell.
    fn narrow_cells<const W: usize>(win: &mut [u8], vals: &[i64], to_bytes: impl Fn(i64) -> [u8; W]) {
        for (dst, v) in win.as_chunks_mut::<W>().0.iter_mut().zip(vals) {
            *dst = to_bytes(*v);
        }
    }

    fn write_str_cells(&self, reg: usize, win: &mut [u8], blob: &mut Vec<u8>) {
        let views = str_lane(&self.scratch.str_views, reg, self.m);
        for (cell, &v) in win.as_chunks_mut::<16>().0.iter_mut().zip(views) {
            *cell = gnitz_wire::encode_german_string(view_bytes(v, &self.scratch.str_arena, self.bufs), blob);
        }
    }

    /// String register `reg`'s bytes for row `i` of this morsel.
    #[inline(always)]
    pub(crate) fn str_bytes(&self, reg: usize, i: usize) -> &[u8] {
        debug_assert!(i < self.m, "str_bytes row {i} is outside the morsel's {} rows", self.m);
        view_bytes(self.scratch.str_views[reg][i], &self.scratch.str_arena, self.bufs)
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
        let words = self.m.div_ceil(64);
        let nulls = &self.scratch.regs[reg].nulls[..words];
        // The usual morsel holds no NULL — every morsel of a register outside
        // `null_regs`.
        // A fold rather than `any`: with no early exit it vectorizes.
        if nulls.iter().fold(0, |acc, &w| acc | w) == 0 {
            return;
        }
        for (w, &word) in nulls.iter().enumerate() {
            let word = if w + 1 == words { word & tail_mask(self.m) } else { word };
            for bit in gnitz_wire::BitIter(word) {
                f(w * 64 + bit);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Morsel context
// ---------------------------------------------------------------------------

/// One region-table entry per payload slot, the cap the null bitmap imposes.
pub(crate) const PAYLOAD_SLOTS: usize = u64::BITS as usize;

/// The batch regions a payload load or a string view reads, fetched once per
/// [`Evaluator::eval_morsels`] call: the blob, and the region of each payload
/// slot an instruction reads (empty elsewhere).
#[derive(Clone, Copy)]
struct Regions<'a> {
    blob: &'a [u8],
    cols: &'a [&'a [u8]; PAYLOAD_SLOTS],
}

/// What a kernel reads besides its operands: the program, the batch's regions,
/// and the morsel's rows.
struct Morsel<'a> {
    prog: &'a ResolvedProgram,
    /// The batch's null bitmap, whole.
    null_bmp: &'a [u8],
    bufs: Regions<'a>,
    /// The batch's PK region, whole, and its row stride.
    pk: (&'a [u8], usize),
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
    if !mo.prog.may_null(dst) {
        return;
    }
    let words = mo.m.div_ceil(64);
    let ([na, nb], nd) = s.null_split([a, b], dst, words);
    for w in 0..words {
        nd[w] = na[w] | nb[w];
    }
}

/// Propagate null over any sequence of source registers: dst_null = OR of
/// theirs (word-at-a-time).
#[inline]
fn null_or_all(s: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, srcs: impl Iterator<Item = u16> + Clone) {
    if !mo.prog.may_null(dst) {
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
    if !mo.prog.may_null(dst) {
        return;
    }
    let ([ns], nd) = s.null_split([src], dst, mo.m.div_ceil(64));
    nd.copy_from_slice(ns);
}

/// Register `di`'s null bits: a row is NULL iff a payload column in `cols` is.
/// A `NOT NULL` column drops out against `nullable_slots`.
fn fill_null_bits_mask(s: &mut EvalScratch, di: u16, mo: &Morsel<'_>, cols: u64) {
    let mask = cols & mo.prog.nullable_slots;
    debug_assert!(
        mask == 0 || mo.prog.may_null(di),
        "a nullable column read outside null_regs"
    );
    if mask != 0 {
        simd::null_bits(
            s.level,
            mo.null_words(),
            mask,
            &mut s.regs[di as usize].nulls[..mo.m.div_ceil(64)],
        );
    }
}

/// `IS [NOT] NULL` (`invert` for IS NOT NULL): payload column `pi`'s bit of the
/// batch bitmap, per row, as a boolean in register `dst` that is never NULL.
fn eval_is_null(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, pi: u8, invert: bool) {
    let pi = pi as usize;
    // A destination read as a packed bit builds its word off the gather directly.
    if mo.prog.needs_bool_pack(dst) {
        return is_null_packed(scratch, mo, dst, pi, invert);
    }
    let rd = scratch.reg_mut(dst, mo.m);
    for (r, w) in rd.iter_mut().zip(mo.null_words().as_chunks::<8>().0) {
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
    maybe_unpack_bool_to_regs(scratch, mo, dst);
}

/// `IS [NOT] NULL` over register `a`'s null words, a definite boolean: those
/// words *are* the packed boolean.
fn eval_is_null_reg(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, a: u16, invert: bool) {
    let flip = if invert { u64::MAX } else { 0 };
    let ([ra], rd) = split_regs(&mut scratch.regs, [a], dst);
    for (d, &n) in rd.bools[..mo.m.div_ceil(64)].iter_mut().zip(&ra.nulls) {
        *d = n ^ flip;
    }
    maybe_unpack_bool_to_regs(scratch, mo, dst);
}

/// `BoolBinary`'s word-level kernel, both selectors: three-valued over the
/// truth and null words, and plain AND / OR over the truth words outside
/// `null_regs`, which reads and writes no null word.
fn bool_and_or_word_loop(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16, a: u16, b: u16, is_or: bool) {
    let words = mo.m.div_ceil(64);
    if !mo.prog.may_null(dst) {
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
    if mo.prog.is_bit_only(dst) {
        return;
    }
    let words = mo.m.div_ceil(64);
    let r = &mut scratch.regs[dst as usize];
    simd::bit_lanes(scratch.level, &r.bools[..words], &mut r.lanes[..words * 64]);
}

/// Pack `dst`'s lanes into its truth bits when a reader needs them. The test
/// inlines and the pack does not (`map` of `expr_kernel_bench`).
#[inline]
fn maybe_pack_bool_bits(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: u16) {
    if mo.prog.needs_bool_pack(dst) {
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
        mo.prog.may_null(dst),
        "a kernel raised a failure flag outside null_regs: \
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

/// One string register lane: `len` bytes at `off` of buffer `src`. Offsets, so
/// a growing arena leaves every view valid.
#[derive(Clone, Copy, Default)]
struct StrView {
    off: u64,
    len: u32,
    /// [`Regions::region`]'s index. Varies row to row within one column: a short
    /// cell's view is its column region, a long one's the blob.
    src: u32,
}

const SRC_ARENA: u32 = 0;
const SRC_BLOB: u32 = 1;
/// A view into string payload slot `pi`'s column names [`Regions::cols`]`[pi]`
/// as `SRC_COL_BASE + pi`.
const SRC_COL_BASE: u32 = 2;

impl<'a> Regions<'a> {
    /// The buffer a [`StrView`]'s `src` names, or `None` for the arena — the
    /// one buffer a kernel may be growing as it resolves a view, so it never
    /// lives here.
    #[inline]
    fn region(&self, src: u32) -> Option<&'a [u8]> {
        match src {
            SRC_ARENA => None,
            SRC_BLOB => Some(self.blob),
            _ => Some(self.col(src - SRC_COL_BASE)),
        }
    }

    /// String payload slot `pi`'s German-string cells, the whole batch's.
    #[inline]
    fn col(&self, pi: u32) -> &'a [u8] {
        self.cols[pi as usize]
    }
}

impl StrView {
    /// A view over `[off, off + len)` of `src`'s buffer.
    fn at(src: u32, off: usize, len: usize) -> Self {
        StrView { off: off as u64, len: len as u32, src }
    }

    /// `None` past the `u32::MAX` bytes a German string can hold.
    fn arena(off: usize, len: usize) -> Option<Self> {
        Some(StrView {
            off: off as u64,
            len: u32::try_from(len).ok()?,
            src: SRC_ARENA,
        })
    }

    /// The `len` bytes at `lo` of this view, in the same buffer.
    fn sub(self, lo: usize, len: usize) -> Self {
        StrView {
            off: self.off + lo as u64,
            len: len as u32,
            src: self.src,
        }
    }

    /// `(offset, length)` in `src`'s buffer. A view is cut from a buffer that
    /// held it, and no buffer shrinks under a live view.
    fn span(self) -> (usize, usize) {
        (self.off as usize, self.len as usize)
    }
}

fn view_bytes<'x>(v: StrView, arena: &'x [u8], bufs: Regions<'x>) -> &'x [u8] {
    let (o, l) = v.span();
    &bufs.region(v.src).unwrap_or(arena)[o..o + l]
}

/// Append `v`'s bytes to the arena; the view over the copy.
#[inline]
fn arena_push_view(arena: &mut Vec<u8>, bufs: Regions<'_>, v: StrView) -> StrView {
    let (start, (o, l)) = (arena.len(), v.span());
    match bufs.region(v.src) {
        Some(buf) => arena.extend_from_slice(&buf[o..o + l]),
        None => arena.extend_from_within(o..o + l),
    }
    StrView::at(SRC_ARENA, start, l)
}

/// Copy `v`'s bytes to the arena and apply `f` to the copy; the view over it.
#[inline(always)]
fn arena_copy_with(arena: &mut Vec<u8>, bufs: Regions<'_>, v: StrView, f: impl FnOnce(&mut [u8])) -> StrView {
    let copy = arena_push_view(arena, bufs, v);
    let (o, l) = copy.span();
    f(&mut arena[o..o + l]);
    copy
}

/// The German-string cell at byte `o` of buffer `buf` as a view: a short one's
/// inline bytes, a long one's blob bytes, a corrupt one empty.
fn cell_to_view(cell: &[u8; 16], o: usize, buf: u32, blob_len: usize) -> StrView {
    match german_string_inline(cell) {
        Some(inline) => StrView::at(buf, o + gnitz_wire::GERMAN_INLINE_OFF, inline.len()),
        None => match german_string_heap(cell, blob_len) {
            Some(r) => StrView::at(SRC_BLOB, r.start, r.len()),
            None => StrView::default(),
        },
    }
}

fn in_trim_set(set: &[u64; 4], b: u8) -> bool {
    (set[(b >> 6) as usize] >> (b & 63)) & 1 != 0
}

impl IntReg {
    /// The register's first `m` rows.
    #[inline]
    fn lane(self, regs: &[RegBuf], m: usize) -> IntLane<'_> {
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

/// Trimmed ASCII decimal with an optional sign; `None` for anything else, or for
/// a magnitude past `u64::MAX`, which no integer type holds.
fn parse_decimal_i128(s: &[u8]) -> Option<i128> {
    let s = s.trim_ascii();
    let (neg, digits) = match s {
        [b'-', rest @ ..] => (true, rest),
        [b'+', rest @ ..] => (false, rest),
        _ => (false, s),
    };
    if digits.is_empty() {
        return None;
    }
    let mut v = 0u64;
    for &b in digits {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u64)?;
    }
    Some(if neg { -(v as i128) } else { v as i128 })
}

/// An integer's decimal text, written into the arena: several times cheaper
/// than `core::fmt` (`int_to_str` of `expr_kernel_bench`).
fn arena_push_int(arena: &mut Vec<u8>, magnitude: u64, neg: bool) -> StrView {
    let off = arena.len();
    // Least-significant digit first, from the end: 20 digits and a sign.
    let mut text = [0u8; 21];
    let mut at = text.len();
    let mut x = magnitude;
    loop {
        at -= 1;
        text[at] = b'0' + (x % 10) as u8;
        x /= 10;
        if x == 0 {
            break;
        }
    }
    if neg {
        at -= 1;
        text[at] = b'-';
    }
    arena.extend_from_slice(&text[at..]);
    StrView::at(SRC_ARENA, off, arena.len() - off)
}

/// A float's decimal text: shortest round-trip, scientific outside
/// `[1e-4, 1e15)` so the text stays short, non-finite values as PostgreSQL
/// spells them. An `f32` keeps its own shortest form.
pub struct FloatText<T>(pub T);

impl<T: Copy + Into<f64> + fmt::Display + fmt::LowerExp> fmt::Display for FloatText<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let v: f64 = self.0.into();
        if v.is_nan() {
            f.write_str("NaN")
        } else if v.is_infinite() {
            f.write_str(if v > 0.0 { "Infinity" } else { "-Infinity" })
        } else if v == 0.0 || (1e-4..1e15).contains(&v.abs()) {
            fmt::Display::fmt(&self.0, f)
        } else {
            fmt::LowerExp::fmt(&self.0, f)
        }
    }
}

/// The arena as a `fmt::Write` sink, which cannot fail. Formatting through
/// `io::Write for Vec<u8>` instead costs `expr_kernel_bench float_to_str` more
/// retired instructions.
struct ArenaText<'a>(&'a mut Vec<u8>);

impl fmt::Write for ArenaText<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

/// `t`'s text, formatted into the arena.
fn arena_push_float(arena: &mut Vec<u8>, t: impl fmt::Display) -> StrView {
    let off = arena.len();
    let _ = fmt::write(&mut ArenaText(arena), format_args!("{t}"));
    StrView::at(SRC_ARENA, off, arena.len() - off)
}

/// SELECT's per-row "take `a`" bits, `cond` true and not NULL, read off its
/// truth bits; `dst`'s null words are the chosen branch's.
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
    if !mo.prog.may_null(dst) {
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

/// Compute every row of scalar register `dst` from `S` string registers; `f`'s
/// `None` is a NULL. `f` runs over NULL rows too, keeping the loop
/// branch-free.
#[inline]
fn str_to_scalar<const S: usize>(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    dst: u16,
    strs: [u16; S],
    mut f: impl FnMut([&[u8]; S]) -> Option<i64>,
) {
    let mut bad = [0u8; MORSEL];
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        // Every window cut to the morsel: relating them is what lets LLVM drop
        // the sources' per-row bounds checks.
        let srcs = strs.map(|r| str_lane(str_views, r as usize, mo.m));
        let rd = &mut regs[dst as usize].lanes[..mo.m];
        for (i, r) in rd.iter_mut().enumerate() {
            *r = f(srcs.map(|v| view_bytes(v[i], str_arena, mo.bufs))).unwrap_or_else(|| {
                bad[i] = 1;
                0
            });
        }
    }
    null_or_all(scratch, mo, dst, strs.into_iter());
    merge_fail_mask(scratch, mo, dst, &bad);
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// The byte length of every row of string register `a`, read off its views.
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

/// Produce every row of string register `dst` from `S` string and `I` integer
/// operands; `f`'s `None` is a NULL.
#[inline]
fn str_kernel<const S: usize, const I: usize>(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    dst: u16,
    strs: [u16; S],
    ints: [IntReg; I],
    f: impl Fn(&mut Vec<u8>, [StrView; S], [i128; I]) -> Option<StrView>,
) {
    let mut bad = [0u8; MORSEL];
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        let (srcs, vd) = split_regs(str_views, strs, dst);
        let (srcs, vd) = (srcs.map(|s| &s[..mo.m]), &mut vd[..mo.m]);
        let lanes = ints.map(|r| r.lane(regs, vd.len()));
        for (i, r) in vd.iter_mut().enumerate() {
            let views = srcs.map(|w| w[i]);
            let nums = lanes.map(|l| l.get(i));
            *r = f(str_arena, views, nums).unwrap_or_else(|| {
                bad[i] = 1;
                StrView::default()
            });
        }
    }
    null_or_all(scratch, mo, dst, strs.iter().copied().chain(ints.iter().map(|r| r.reg)));
    merge_fail_mask(scratch, mo, dst, &bad);
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
fn eval_str_substr(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, si: u16, start: IntReg, len: Option<IntReg>) {
    let m = mo.m;
    let mut bad = [0u8; MORSEL];
    {
        let EvalScratch { regs, str_views, str_arena, .. } = &mut *scratch;
        let (start, len) = (start.lane(regs, m), len.map(|l| l.lane(regs, m)));
        let ([sv], dv) = split_regs(str_views, [si], d);
        let (sv, dv) = (&sv[..m], &mut dv[..m]);
        for i in 0..m {
            let v = sv[i];
            let s = view_bytes(v, str_arena, mo.bufs);
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
                v.sub(b_lo, b_hi - b_lo)
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
fn eval_str_concat(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, b: u16, skip_null: bool) {
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
            let b_is_null = skip_null && null_word_get(regs[b as usize].nulls[i / 64], i % 64);
            let vb = if b_is_null { StrView::default() } else { sb[i] };
            let Some(v) = StrView::arena(str_arena.len(), va.len as usize + vb.len as usize) else {
                bad[i] = 1;
                sd[i] = StrView::default();
                continue;
            };
            arena_push_view(str_arena, mo.bufs, va);
            arena_push_view(str_arena, mo.bufs, vb);
            sd[i] = v;
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
    fn column(pi: u8, mo: &Morsel<'a>) -> Self {
        let rows = &mo.bufs.col(pi as u32)[mo.start * 16..][..mo.m * 16];
        StrOperand {
            cells: ColumnCells(rows.as_chunks::<16>().0),
            blob: mo.bufs.blob,
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
    let mut hit = [0u8; MORSEL];
    for (i, h) in hit[..mo.m].iter_mut().enumerate() {
        *h = pred(compare_german_strings(
            a.cells.cell(i),
            Heap::Plain(a.blob),
            b.cells.cell(i),
            Heap::Plain(b.blob),
        )) as u8;
    }
    pack_bytes(&hit, &mut scratch.regs[dst as usize].bools);
    maybe_unpack_bool_to_regs(scratch, mo, dst);
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

/// String equality, `<>` when `ne`: one branch-free pass over the cells settles
/// every short row, and only a long row whose length and prefix agree reads the
/// heap.
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

// ---------------------------------------------------------------------------
// Register-loop shapes — one function per per-row calling convention
// ---------------------------------------------------------------------------
//
// Each takes the per-row body as a closure. `#[inline]` is what splices that
// closure into the loop rather than calling it: `[profile.dev]` leaves
// `codegen-units = 256`, and an instantiation landing in a different CGU from
// its caller cannot be inlined without the attribute.

/// A binary opcode that cannot fail: two source registers into one, their null
/// words ORed.
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

/// [`bin_op`] for a predicate; a bit_only destination takes its packed bits
/// straight off the operands (`cmp_value` of `expr_kernel_bench`). Inlined
/// always, per `filter_kernel_bench`'s `nullable` and `literals`.
#[inline(always)]
fn bin_pred<P: LanePred>(scratch: &mut EvalScratch, mo: &Morsel<'_>, d: u16, a: u16, b: u16) {
    if !mo.prog.is_bit_only(d) {
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

/// The largest IN set scanned a value at a time, by a morsel of at least as
/// many rows, rather than searched per row (the `in*` shapes of both benches).
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

/// A binary opcode that can fail: `f` returns `(value, failed)`, and a failed
/// row is NULL.
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
        if !mo.prog.may_null(d) {
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

/// A German-string compare, one row loop per operator, compiled out of line
/// from the dispatch (`str_const_filter_bench`).
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
        gnitz_wire::zip_cells::<{ FI.width() }, _>(rows, stride, off, dst_reg.iter_mut(), |c, r| {
            *r = gnitz_wire::decode_opk_i64(c, FI)
        })
    });
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// Evaluate `prog` over one morsel (`morsel_start..morsel_start+m`) of the
/// batch whose regions are `bufs` and `(null_bmp, pk)`. Results land in
/// `scratch.regs`: a register's values in its lanes, its null bits beside them.
/// [`Evaluator::eval_morsels`] calls it once per morsel.
fn eval_batch(
    prog: &ResolvedProgram,
    bufs: Regions<'_>,
    (null_bmp, pk): (&[u8], (&[u8], usize)),
    morsel_start: usize,
    m: usize,
    scratch: &mut EvalScratch,
) {
    let morsel = Morsel {
        prog,
        null_bmp,
        bufs,
        pk,
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
                let col_data = mo.bufs.cols[pi as usize];
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
                let col_data = mo.bufs.cols[pi as usize];
                let dst_reg = scratch.reg_mut(dst, m);
                let b = &col_data[morsel_start * 4..(morsel_start + m) * 4];
                for (r, c) in dst_reg.iter_mut().zip(b.as_chunks::<4>().0) {
                    *r = encode_f64(f32::from_le_bytes(*c) as f64);
                }
                fill_null_bits_mask(scratch, dst, mo, 1u64 << pi);
                maybe_pack_bool_bits(scratch, mo, dst);
            }

            Instr::LoadPk { off, fi } => load_pk(scratch, mo, dst, mo.pk, off as usize, fi),

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
            Instr::FloatToF32 { a } => unary_null_like(scratch, mo, dst, a, |x| {
                let f = gnitz_wire::narrow_f32(decode_f64(x));
                (encode_f64(f.map_or(0.0, f64::from)), f.is_none())
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
            // 3VL: NOT NULL = NULL, its truth bit cleared.
            Instr::BoolNot { a } => {
                let ([ra], rd) = split_regs(&mut scratch.regs, [a], dst);
                for w in 0..m.div_ceil(64) {
                    rd.bools[w] = !ra.bools[w] & !ra.nulls[w];
                    rd.nulls[w] = ra.nulls[w];
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

            // ----------------------------------------------------------------
            // String comparisons (column vs constant / column vs column)
            // ----------------------------------------------------------------
            Instr::StrColConst { op, pi, const_idx } => str_cmp(
                scratch,
                mo,
                op,
                dst,
                StrOperand::column(pi, mo),
                StrOperand::constant(prog, const_idx as usize),
            ),
            Instr::StrColCol { op, pi_a, pi_b } => str_cmp(
                scratch,
                mo,
                op,
                dst,
                StrOperand::column(pi_a, mo),
                StrOperand::column(pi_b, mo),
            ),

            // ----------------------------------------------------------------
            // String registers
            // ----------------------------------------------------------------
            Instr::LoadColStr { pi } => {
                let blob_len = bufs.blob.len();
                let buf = SRC_COL_BASE + pi as u32;
                let rows = bufs.col(pi as u32)[morsel_start * 16..(morsel_start + m) * 16]
                    .as_chunks::<16>()
                    .0;
                let lane = str_lane_mut(&mut scratch.str_views, dst as usize, m);
                for (i, (v, cell)) in lane.iter_mut().zip(rows).enumerate() {
                    *v = cell_to_view(cell, (morsel_start + i) * 16, buf, blob_len);
                }
                fill_null_bits_mask(scratch, dst, mo, 1u64 << pi);
            }

            Instr::StrSelect { cond, a, b } => eval_str_select(scratch, mo, dst, cond, a, b),

            Instr::StrCmp { op, a, b } => match op {
                CmpOp::Eq => str_to_scalar(scratch, mo, dst, [a, b], |[x, y]| Some((x == y) as i64)),
                CmpOp::Ne => str_to_scalar(scratch, mo, dst, [a, b], |[x, y]| Some((x != y) as i64)),
                CmpOp::Gt => str_to_scalar(scratch, mo, dst, [a, b], |[x, y]| Some((x > y) as i64)),
                CmpOp::Ge => str_to_scalar(scratch, mo, dst, [a, b], |[x, y]| Some((x >= y) as i64)),
                CmpOp::Lt => str_to_scalar(scratch, mo, dst, [a, b], |[x, y]| Some((x < y) as i64)),
                CmpOp::Le => str_to_scalar(scratch, mo, dst, [a, b], |[x, y]| Some((x <= y) as i64)),
            },

            // Characters are counted off the bytes; a byte length is the view's own.
            Instr::StrLen { a, chars } => {
                if chars {
                    str_to_scalar(scratch, mo, dst, [a], |[s]| Some(char_count(s) as i64));
                } else {
                    str_byte_len(scratch, mo, dst, a);
                }
            }

            // A fresh copy in the arena, folded in place; one loop per direction.
            Instr::StrCase { a, upper } => {
                if upper {
                    str_kernel(scratch, mo, dst, [a], [], |arena, [v], []| {
                        Some(arena_copy_with(arena, bufs, v, |s| s.make_ascii_uppercase()))
                    })
                } else {
                    str_kernel(scratch, mo, dst, [a], [], |arena, [v], []| {
                        Some(arena_copy_with(arena, bufs, v, |s| s.make_ascii_lowercase()))
                    })
                }
            }

            Instr::StrSubstr { src, start, len } => eval_str_substr(scratch, mo, dst, src, start, len),

            // A sub-view of the source: the bytes are not copied, only the
            // offset and length narrowed.
            Instr::StrTrim { a, mode, set_idx } => {
                let set = &prog.trim_sets[set_idx as usize];
                str_kernel(scratch, mo, dst, [a], [], |arena, [v], []| {
                    let s = view_bytes(v, arena, bufs);
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
                    Some(v.sub(lo, hi - lo))
                });
            }

            Instr::StrLike { src, matcher_idx } => {
                let matcher = &prog.like_matchers[matcher_idx as usize];
                let mut folded = Vec::new();
                str_to_scalar(scratch, mo, dst, [src], |[s]| {
                    Some(matcher.matches(s, &mut folded) as i64)
                });
            }

            Instr::StrConcat { a, b, skip_null } => eval_str_concat(scratch, mo, dst, a, b, skip_null),

            // One loop per signedness, so neither tests it per row.
            Instr::IntToStr { a } => match a.signed {
                true => str_kernel(scratch, mo, dst, [], [a], |arena, [], [v]| {
                    let v = v as i64;
                    Some(arena_push_int(arena, v.unsigned_abs(), v < 0))
                }),
                false => str_kernel(scratch, mo, dst, [], [a], |arena, [], [v]| {
                    Some(arena_push_int(arena, v as u64, false))
                }),
            },
            Instr::FloatToStr { a, f32 } => {
                let bits = IntReg { reg: a, signed: true };
                // The narrowing is exact: an `f32` register holds a widened f32.
                match f32 {
                    true => str_kernel(scratch, mo, dst, [], [bits], |arena, [], [v]| {
                        Some(arena_push_float(arena, FloatText(decode_f64(v as i64) as f32)))
                    }),
                    false => str_kernel(scratch, mo, dst, [], [bits], |arena, [], [v]| {
                        Some(arena_push_float(arena, FloatText(decode_f64(v as i64))))
                    }),
                }
            }

            Instr::StrToInt { a, fi } => {
                let (lo, hi) = fi.range();
                // A U64 value above i64::MAX narrows to its own bit pattern,
                // which is what the register holds; the resolve-time U64
                // tracking makes downstream reads agree.
                str_to_scalar(scratch, mo, dst, [a], |[s]| {
                    parse_decimal_i128(s).filter(|v| *v >= lo && *v <= hi).map(|v| v as i64)
                });
            }

            Instr::StrToFloat { a } => {
                str_to_scalar(scratch, mo, dst, [a], |[s]| {
                    std::str::from_utf8(s.trim_ascii())
                        .ok()
                        .and_then(|t| t.parse::<f64>().ok())
                        .map(encode_f64)
                });
            }
            Instr::StrPos { hay, needle } => str_to_scalar(scratch, mo, dst, [hay, needle], |[h, n]| {
                Some(find(h, n).map_or(0, |off| char_count(&h[..off]) as i64 + 1))
            }),
            Instr::StrSide { src, n, left } => {
                str_kernel(scratch, mo, dst, [src], [n], |arena, [v], [n]| {
                    let s = view_bytes(v, arena, bufs);
                    // Clamped to the byte length, which no character count exceeds.
                    let n_abs = n.unsigned_abs().min(s.len() as u128) as usize;
                    // A non-negative LEFT and a negative RIGHT count from the start.
                    let counts_from_start = (n >= 0) == left;
                    let cut = if counts_from_start {
                        char_offset(s, n_abs)
                    } else {
                        char_offset_back(s, n_abs)
                    };
                    Some(if left { v.sub(0, cut) } else { v.sub(cut, s.len() - cut) })
                })
            }
            Instr::StrReverse { a } => str_kernel(scratch, mo, dst, [a], [], |arena, [v], []| {
                Some(arena_copy_with(arena, bufs, v, reverse_chars))
            }),
            Instr::StrReplace { s, from, to } => {
                str_kernel(scratch, mo, dst, [s, from, to], [], |arena, [vs, vf, vt], []| {
                    if vf.len == 0 {
                        return Some(vs);
                    }
                    let out = arena.len();
                    let push = |arena: &mut Vec<u8>, v: StrView| {
                        if arena.len() - out + v.len as usize > u32::MAX as usize {
                            arena.truncate(out);
                            return false;
                        }
                        arena_push_view(arena, bufs, v);
                        true
                    };
                    let mut lo = 0usize;
                    while let Some(k) = find(&view_bytes(vs, arena, bufs)[lo..], view_bytes(vf, arena, bufs)) {
                        if !push(arena, vs.sub(lo, k)) || !push(arena, vt) {
                            return None;
                        }
                        lo += k + vf.len as usize;
                    }
                    if lo == 0 {
                        return Some(vs);
                    }
                    if !push(arena, vs.sub(lo, vs.len as usize - lo)) {
                        return None;
                    }
                    StrView::arena(out, arena.len() - out)
                })
            }
            Instr::StrPad { s, n, fill, left } => {
                str_kernel(scratch, mo, dst, [s, fill], [n], |arena, [vs, vf], [n]| {
                    let s = view_bytes(vs, arena, bufs);
                    if n <= 0 {
                        return Some(StrView::default());
                    }
                    let s_chars = char_count(s);
                    // A width at or below the subject's own length truncates —
                    // the LEFT of `n` characters, a sub-view.
                    if n <= s_chars as i128 {
                        return Some(vs.sub(0, char_offset(s, n as usize)));
                    }
                    let fill = view_bytes(vf, arena, bufs);
                    let fill_chars = char_count(fill);
                    if fill_chars == 0 {
                        return Some(vs);
                    }
                    let pad_chars = usize::try_from(n - s_chars as i128).ok()?;
                    let pad_len = (pad_chars / fill_chars)
                        .checked_mul(fill.len())?
                        .checked_add(char_offset(fill, pad_chars % fill_chars))?;
                    let result = StrView::arena(arena.len(), s.len().checked_add(pad_len)?)?;
                    let (fill_len, fill0) = (fill.len(), fill[0]);
                    // The pad doubles itself: `n` fills cost `log2 n` copies.
                    let push_pad = |arena: &mut Vec<u8>| {
                        let p = arena.len();
                        if fill_len == 1 {
                            arena.resize(p + pad_len, fill0);
                            return;
                        }
                        arena_push_view(arena, bufs, vf.sub(0, fill_len.min(pad_len)));
                        while arena.len() - p < pad_len {
                            let have = arena.len() - p;
                            arena.extend_from_within(p..p + have.min(pad_len - have));
                        }
                    };
                    if left {
                        push_pad(arena);
                        arena_push_view(arena, bufs, vs);
                    } else {
                        arena_push_view(arena, bufs, vs);
                        push_pad(arena);
                    }
                    Some(result)
                })
            }
            Instr::StrSplitPart { s, delim, n } => {
                str_kernel(scratch, mo, dst, [s, delim], [n], |arena, [vs, vd], [n]| {
                    if n == 0 {
                        return None;
                    }
                    let s = view_bytes(vs, arena, bufs);
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
                    Some(field.map_or(StrView::default(), |(lo, hi)| vs.sub(lo, hi - lo)))
                })
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;
