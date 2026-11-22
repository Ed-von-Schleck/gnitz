//! The morsel evaluator: `Evaluator` runs a resolved program over a batch one
//! morsel (up to `MORSEL` rows) at a time.
//!
//! `eval_morsel` applies each instruction as a columnar loop over register
//! buffers, reading the batch's regions from a table `Evaluator::eval_morsels`
//! fetches once per call. Base pointers are hoisted outside the inner loop,
//! which is what lets LLVM auto-vectorize the arithmetic opcodes. The loops that
//! end in packed bits or start from them are [`crate::simd`]'s; the string
//! kernels do not vectorize.

use std::cmp::Ordering;
use std::fmt;

use crate::chars::{char_count, char_offset, char_offset_back, reverse_chars};
use crate::map::{Emit, EmitStore};
use crate::program::{FloatUnaryOp, IntArithOp, IntUnaryOp, PAYLOAD_SLOTS};
use crate::search::{fields, find, lit_eq};
use crate::simd::{self, LanePred, Level, decode_f64, encode_f64};
use crate::{
    BatchView, CalendarOp, CmpOp, FloatArithOp, Instr, LogicalInstr as L, Reg, ResolvedProgram, TrimMode, calendar,
};
use gnitz_wire::{
    FixedInt, FloatText, Heap, compare_german_strings, date_bytes, german_string_content, german_string_heap,
    german_string_inline, low_bits_mask, null_word_get, scaled_bytes, timestamp_bytes,
};

/// Integer-cast bounds for the target type: the signed-source window `[lo, hi]`
/// and the unsigned-source ceiling `hi_u`. Only U64 needs the two to differ —
/// its `hi` is clamped to `i64::MAX` because the signed test runs in i64.
fn int_cast_bounds(fi: FixedInt) -> (i64, i64, u64) {
    let (lo, hi) = fi.range();
    (lo as i64, hi.min(i64::MAX as i128) as i64, hi as u64)
}

pub(crate) const MORSEL: usize = 256;
const _: () = assert!(MORSEL.is_multiple_of(64), "a morsel owns whole bitmap words");
const NULL_WORDS_PER_REG: usize = MORSEL / 64;

// ---------------------------------------------------------------------------
// EvalScratch — the SoA register file for batch evaluation
// ---------------------------------------------------------------------------

/// One register: a lane per row, and a null bit and a truth bit per row.
///
/// On a cache-line boundary: lanes that start off a 32-byte one split the
/// loaders' stores and the kernels' loads across lines.
#[derive(Clone)]
#[repr(align(64))]
struct RegBuf {
    /// A NULL row's lane holds whatever its branch-free kernel computed. Lanes
    /// past a morsel's last row hold no row; a string register uses none.
    lanes: [i64; MORSEL],
    /// Zero for the scratch's life outside
    /// [`ResolvedProgram::null_regs`](crate::program::ResolvedProgram::null_regs).
    /// Bits past a morsel's last row are undefined.
    nulls: [u64; NULL_WORDS_PER_REG],
    /// Packed truth bits, what a boolean reader reads. Bits past a morsel's last
    /// row are undefined.
    bools: [u64; NULL_WORDS_PER_REG],
}

/// A resolved program and the register file built for it.
pub(crate) struct Evaluator {
    prog: ResolvedProgram,
    scratch: EvalScratch,
    /// The instruction set [`crate::simd`]'s kernels run at.
    level: Level,
}

/// A clone gets a register file of its own, built fresh: nothing in one outlives
/// the morsel that wrote it.
impl Clone for Evaluator {
    fn clone(&self) -> Self {
        Evaluator::new(self.prog.clone())
    }
}

struct EvalScratch {
    regs: Vec<RegBuf>,
    /// One view per row, per string register.
    str_views: Vec<[StrView; MORSEL]>,
    /// The program's constant bytes, then the bytes this morsel computed.
    str_arena: Vec<u8>,
}

/// String register `reg`'s first `m` views, cut from the flattened file, which
/// is cheaper than slicing its own array (`expr_kernel_bench`).
#[inline(always)]
fn str_lane(views: &[[StrView; MORSEL]], reg: usize, m: usize) -> &[StrView] {
    &views.as_flattened()[reg * MORSEL..][..m]
}

/// The first `m` views of `N` source string registers and of destination `d`,
/// cut from the flattened file as [`str_lane`] is. Every source lies below `d`,
/// as [`Step`] says.
#[inline(always)]
fn split_views<const N: usize>(
    views: &mut [[StrView; MORSEL]],
    srcs: [u16; N],
    d: u16,
    m: usize,
) -> ([&[StrView]; N], &mut [StrView]) {
    let (below, rest) = views.as_flattened_mut().split_at_mut(d as usize * MORSEL);
    let below = &*below;
    (srcs.map(|r| &below[r as usize * MORSEL..][..m]), &mut rest[..m])
}

impl Evaluator {
    pub(crate) fn new(prog: ResolvedProgram) -> Self {
        let level = Level::new();
        Evaluator {
            scratch: EvalScratch::new(&prog, level),
            prog,
            level,
        }
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
        let Evaluator { prog, scratch, level } = self;
        #[cfg(debug_assertions)]
        if n > 0 {
            let reads = gnitz_wire::BitIter(prog.read_mask).map(|pi| (pi, prog.read_widths[pi] as usize));
            crate::view::debug_assert_coherent(mb, start, reads);
        }
        // Hoisted: LLVM cannot fold repeated `&dyn` calls.
        let mut cols: [&[u8]; PAYLOAD_SLOTS] = [&[]; PAYLOAD_SLOTS];
        for pi in gnitz_wire::BitIter(prog.read_mask) {
            cols[pi] = mb.col_data(pi, prog.read_widths[pi] as usize);
        }
        let blob = mb.heap().plain().expect("a batch view holds its long values verbatim");
        let bufs = Regions { blob, cols: &cols };
        let mut morsel = Morsel {
            prog,
            null_bmp: mb.null_bmp(),
            bufs,
            pk: mb.pk_region(),
            start,
            m: 0,
            level: *level,
        };
        for rel_start in (0..n).step_by(MORSEL) {
            let m = MORSEL.min(n - rel_start);
            morsel.start = start + rel_start;
            morsel.m = m;
            eval_morsel(&morsel, scratch);
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
        self.level = level;
        self.scratch = EvalScratch::new(&self.prog, level);
    }
}

impl EvalScratch {
    /// The register file with its constant registers written, once and at full
    /// [`MORSEL`] width: no instruction writes them per morsel.
    fn new(prog: &ResolvedProgram, level: Level) -> Self {
        const ZERO: RegBuf = RegBuf {
            lanes: [0; MORSEL],
            nulls: [0; NULL_WORDS_PER_REG],
            bools: [0; NULL_WORDS_PER_REG],
        };
        let mut regs = vec![ZERO; prog.num_regs as usize];
        let mut str_views = vec![[StrView::default(); MORSEL]; prog.str_lanes as usize];
        for &(dst, val) in &prog.const_regs {
            let r = &mut regs[dst as usize];
            r.lanes.fill(val);
            if prog.needs_bool_pack(dst) {
                pack_lanes(level, NULL_WORDS_PER_REG, r);
            }
        }
        for &(dst, off, len) in &prog.const_str_regs {
            str_views[dst as usize].fill(StrView::at(SRC_ARENA, off as usize, len as usize));
        }
        for dst in gnitz_wire::BitIter(prog.const_nulls) {
            regs[dst].nulls.fill(u64::MAX);
        }
        EvalScratch {
            regs,
            str_views,
            str_arena: prog.const_arena.clone(),
        }
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

    /// The first non-NULL value of integer register `reg` in this morsel outside
    /// `fi`'s range, read as unsigned under `unsigned`; the bounds are the ones
    /// `IntCast` tests.
    pub(crate) fn first_outside(&self, reg: usize, fi: FixedInt, unsigned: bool) -> Option<i128> {
        let (lo, hi, hi_u) = int_cast_bounds(fi);
        let outside = |x: i64| if unsigned { x as u64 > hi_u } else { x < lo || x > hi };
        let vals = self.reg_values(reg);
        // A fold rather than `any`: with no early exit it vectorizes, and the
        // usual morsel holds no value outside.
        if !vals.iter().fold(false, |acc, &x| acc | outside(x)) {
            return None;
        }
        let i = (0..self.m).find(|&i| outside(vals[i]) && !self.is_null(reg, i))?;
        Some(if unsigned {
            i128::from(vals[i] as u64)
        } else {
            i128::from(vals[i])
        })
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

    /// Whether register `reg` is NULL in row `i` of this morsel.
    #[inline(always)]
    pub(crate) fn is_null(&self, reg: usize, i: usize) -> bool {
        debug_assert!(i < self.m, "is_null row {i} is outside the morsel's {} rows", self.m);
        gnitz_wire::null_word_get(self.scratch.regs[reg].nulls[i / 64], i % 64)
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
            for bit in gnitz_wire::BitIter(word & low_bits_mask(self.m - w * 64)) {
                f(w * 64 + bit);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Morsel context
// ---------------------------------------------------------------------------

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
    /// The instruction set [`crate::simd`]'s kernels run at.
    level: Level,
}

impl Morsel<'_> {
    /// This morsel's null-bitmap rows, 8 bytes each. Cut once so the per-row
    /// bounds check is against the morsel, not the whole batch.
    fn null_words(&self) -> &[u8] {
        &self.null_bmp[self.start * 8..(self.start + self.m) * 8]
    }
}

/// One instruction's share of the register file: its destination `d`, every
/// register below it — among them each source it reads, since a register is the
/// index of its writing instruction and `LogicalProgram::new` rejects an operand
/// not written before its reader — and the string lanes and arena.
struct Step<'s, 'a> {
    mo: &'s Morsel<'a>,
    /// The morsel's row count, read once: through `mo`, which is opaque to the
    /// optimizer, it would be reloaded in every row loop.
    m: usize,
    d: u16,
    rd: &'s mut RegBuf,
    below: &'s [RegBuf],
    views: &'s mut [[StrView; MORSEL]],
    arena: &'s mut Vec<u8>,
}

/// What the dispatch hands a kernel: the morsel, and the register file the
/// kernel splits at its destination into a [`Step`]. Two words, so a kernel
/// compiled out of line takes it in registers, where a `Step` would go through
/// memory.
struct Regs<'s, 'a> {
    mo: &'s Morsel<'a>,
    x: &'s mut EvalScratch,
}

impl<'s, 'a> Regs<'s, 'a> {
    /// The register file split at destination `d`.
    #[inline(always)]
    fn at(self, d: u16) -> Step<'s, 'a> {
        let EvalScratch { regs, str_views, str_arena } = self.x;
        let (below, rest) = regs.split_at_mut(d as usize);
        Step {
            mo: self.mo,
            m: self.mo.m,
            d,
            rd: &mut rest[0],
            below,
            views: str_views,
            arena: str_arena,
        }
    }
}

impl<'s> Step<'s, '_> {
    /// The bitmap words that cover the morsel's rows.
    #[inline(always)]
    fn words(&self) -> usize {
        self.m.div_ceil(64)
    }

    /// Source register `r`.
    #[inline(always)]
    fn reg(&self, r: u16) -> &'s RegBuf {
        &self.below[r as usize]
    }

    /// The destination's null words: the OR of those of source registers
    /// `srcs`, and none outside `null_regs`.
    #[inline(always)]
    fn or_nulls(&mut self, srcs: impl IntoIterator<Item = u16> + Clone) {
        if !self.mo.prog.may_null(self.d) {
            return;
        }
        let (below, words) = (self.below, self.words());
        for (w, nd) in self.rd.nulls[..words].iter_mut().enumerate() {
            *nd = srcs
                .clone()
                .into_iter()
                .fold(0, |acc, r| acc | below[r as usize].nulls[w]);
        }
    }

    /// OR a per-row failure flag into the destination's null words. A byte per
    /// row, packed here in a second pass: packing inside the value loop would
    /// read-modify-write one mask word every iteration, which serialises it.
    #[inline(always)]
    fn or_failures(&mut self, bad: &[u8; MORSEL]) {
        // The usual morsel has no failure. A fold rather than `any`: with no
        // early exit it vectorizes.
        if bad.iter().fold(0u8, |acc, &b| acc | b) == 0 {
            return;
        }
        assert!(
            self.mo.prog.may_null(self.d),
            "a kernel raised a failure flag outside null_regs: \
             its `Operands` entry does not set `makes_null`"
        );
        let mut failed = [0u64; NULL_WORDS_PER_REG];
        pack_bytes(bad, &mut failed);
        let words = self.words();
        for (n, f) in self.rd.nulls[..words].iter_mut().zip(failed) {
            *n |= f;
        }
    }

    /// The destination's null words: a row is NULL iff a payload column in
    /// `cols` is. A `NOT NULL` column drops out against `nullable_slots`.
    #[inline(always)]
    fn load_nulls(&mut self, cols: u64) {
        let mask = cols & self.mo.prog.nullable_slots;
        debug_assert!(
            mask == 0 || self.mo.prog.may_null(self.d),
            "a nullable column read outside null_regs"
        );
        if mask != 0 {
            let words = self.words();
            simd::null_bits(self.mo.level, self.mo.null_words(), mask, &mut self.rd.nulls[..words]);
        }
    }

    /// Pack the destination's lanes into its truth bits when a reader needs
    /// them. The test inlines and the pack does not (`map` of
    /// `expr_kernel_bench`).
    #[inline]
    fn pack_bools(&mut self) {
        if self.mo.prog.needs_bool_pack(self.d) {
            pack_lanes(self.mo.level, self.words(), self.rd);
        }
    }

    /// Unpack the destination's truth bits into its lanes unless it is
    /// bit_only, whose readers take the packed bits directly.
    #[inline(always)]
    fn unpack_bools(&mut self) {
        if self.mo.prog.is_bit_only(self.d) {
            return;
        }
        let words = self.words();
        simd::bit_lanes(self.mo.level, &self.rd.bools[..words], &mut self.rd.lanes[..words * 64]);
    }
}

/// `words` words of `rd`'s lanes packed into its truth bits.
fn pack_lanes(level: Level, words: usize, rd: &mut RegBuf) {
    simd::truthy_bits(level, &rd.lanes[..words * 64], &mut rd.bools[..words]);
}

/// One bit per byte of `src`, each byte 0 or 1: eight bytes fold to eight bits
/// in one multiply.
fn pack_bytes(src: &[u8; MORSEL], out: &mut [u64; NULL_WORDS_PER_REG]) {
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
            _ => Some(self.cols[(src - SRC_COL_BASE) as usize]),
        }
    }

    /// Payload slot `pi`'s cells, `W` bytes each, for the `m` rows from `start`.
    #[inline(always)]
    fn cells<const W: usize>(&self, pi: u8, start: usize, m: usize) -> &'a [[u8; W]] {
        self.cols[pi as usize][start * W..(start + m) * W].as_chunks::<W>().0
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

fn in_trim_set(set: &[u64; 4], b: u8) -> bool {
    (set[(b >> 6) as usize] >> (b & 63)) & 1 != 0
}

impl<'s> Step<'s, '_> {
    /// Source register `r`'s rows, read as a count or a bound.
    #[inline]
    fn int_lane(&self, r: u16) -> IntLane<'s> {
        IntLane {
            rows: &self.below[r as usize].lanes[..self.m],
            signed: !self.mo.prog.is_u64(r),
        }
    }
}

/// A scalar register's rows in a morsel, read as a count or a bound: widened to
/// `i128` per the register's U64 tracking, so a large unsigned value never
/// reads as negative.
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
        if self.signed { v as i128 } else { v as u64 as i128 }
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

/// The text a value-text writer spells, written into the arena.
fn arena_push_text(arena: &mut Vec<u8>, write: impl FnOnce(&mut [u8; gnitz_wire::VALUE_TEXT_MAX]) -> &[u8]) -> StrView {
    let off = arena.len();
    let mut buf = [0; gnitz_wire::VALUE_TEXT_MAX];
    arena.extend_from_slice(write(&mut buf));
    StrView::at(SRC_ARENA, off, arena.len() - off)
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

// ---------------------------------------------------------------------------
// Register-loop shapes — one function per per-row calling convention
// ---------------------------------------------------------------------------
//
// Each takes the per-row body as a closure, and is instantiated once per
// closure, so the body is spliced into the loop rather than called. An
// out-of-line shape is still one instantiation per opcode arm.

/// `d[i] = f(row i of ins)`, as one zipped loop per arity: indexing each input
/// per row instead costs `expr_kernel_bench float_round` a sixth more
/// instructions.
#[inline(always)]
fn map_lanes<const N: usize, T>(d: &mut [i64], ins: [&[i64]; N], mut f: impl FnMut(&mut i64, [i64; N]) -> T) {
    match ins.as_slice() {
        [a] => d.iter_mut().zip(*a).for_each(|(r, &x)| {
            f(r, [x; N]);
        }),
        [a, b] => (d.iter_mut().zip(*a).zip(*b)).for_each(|((r, &x), &y)| {
            f(r, std::array::from_fn(|k| [x, y][k]));
        }),
        _ => unreachable!("an opcode reads one or two scalar lanes"),
    }
}

impl Regs<'_, '_> {
    /// A scalar opcode that cannot fail: the destination's lanes from those of
    /// `srcs`, row by row, its null words the OR of theirs.
    #[inline(never)]
    fn lanes<const N: usize>(self, d: u16, srcs: [u16; N], f: impl Fn([i64; N]) -> i64) {
        let mut s = self.at(d);
        let (ins, m) = (srcs.map(|r| s.reg(r)), s.m);
        map_lanes(&mut s.rd.lanes[..m], ins.map(|r| &r.lanes[..m]), |r, x| *r = f(x));
        s.or_nulls(srcs);
        s.pack_bools();
    }

    /// [`Self::lanes`] for an opcode that can fail: `f` returns `(value,
    /// failed)`, and a failed row is NULL. The value is always defined, so a
    /// failed row never leaves uninitialised bits behind.
    #[inline(never)]
    fn lanes_or_null<const N: usize>(self, d: u16, srcs: [u16; N], f: impl Fn([i64; N]) -> (i64, bool)) {
        let mut s = self.at(d);
        let (ins, m) = (srcs.map(|r| s.reg(r)), s.m);
        let mut bad = [0u8; MORSEL];
        let mut i = 0;
        map_lanes(&mut s.rd.lanes[..m], ins.map(|r| &r.lanes[..m]), |r, x| {
            let (val, failed) = f(x);
            *r = val;
            bad[i] = failed as u8;
            i += 1;
        });
        s.or_nulls(srcs);
        s.or_failures(&bad);
        s.pack_bools();
    }

    /// [`Self::lanes`] for a predicate; a bit_only destination takes its packed
    /// bits straight off the operands (`cmp_value` of `expr_kernel_bench`).
    /// Inlined always, per `filter_kernel_bench`'s `nullable` and `literals`.
    #[inline(always)]
    fn pred<P: LanePred>(self, d: u16, a: u16, b: u16) {
        if !self.mo.prog.is_bit_only(d) {
            return self.lanes(d, [a, b], |[x, y]| P::scalar(x, y) as i64);
        }
        let mut s = self.at(d);
        let (ra, rb, words) = (s.reg(a), s.reg(b), s.words());
        let (la, lb) = (&ra.lanes[..words * 64], &rb.lanes[..words * 64]);
        simd::pred_bits::<P>(s.mo.level, la, lb, &mut s.rd.bools[..words]);
        s.or_nulls([a, b]);
    }

    /// `IntArith`'s `Div` (`MOD = false`) and `Mod`, over the four
    /// (operator × signedness) whole loops — neither branch is inside the row
    /// loop; a U64 destination divides unsigned. A zero divisor is substituted with 1 and the row marked NULL, so
    /// `wrapping_*` never divides by zero and `i64::MIN / -1` wraps rather than
    /// trapping.
    #[inline]
    fn int_divmod<const MOD: bool>(self, d: u16, a: u16, b: u16) {
        if !self.mo.prog.is_u64(d) {
            self.lanes_or_null(d, [a, b], |[x, y]| {
                let is_zero = y == 0;
                let dd = if is_zero { 1 } else { y };
                (if MOD { x.wrapping_rem(dd) } else { x.wrapping_div(dd) }, is_zero)
            })
        } else {
            self.lanes_or_null(d, [a, b], |[x, y]| {
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

    /// Null-skipping 2-ary extremum: NULL only when both operands are. `P`
    /// holds where `a` wins on value; a NULL operand loses whatever its lane
    /// holds.
    #[inline(never)]
    fn minmax2<P: LanePred>(self, d: u16, a: u16, b: u16) {
        let mut s = self.at(d);
        let (ra, rb, m, words) = (s.reg(a), s.reg(b), s.m, s.words());
        if !s.mo.prog.may_null(s.d) {
            for ((r, &x), &y) in s.rd.lanes[..m].iter_mut().zip(&ra.lanes[..m]).zip(&rb.lanes[..m]) {
                *r = if P::scalar(x, y) { x } else { y };
            }
        } else {
            let (la, lb) = (&ra.lanes[..words * 64], &rb.lanes[..words * 64]);
            let mut take_a = [0u64; NULL_WORDS_PER_REG];
            simd::pred_bits::<P>(s.mo.level, la, lb, &mut take_a[..words]);
            for (w, take) in take_a[..words].iter_mut().enumerate() {
                *take = !ra.nulls[w] & (rb.nulls[w] | *take);
                s.rd.nulls[w] = ra.nulls[w] & rb.nulls[w];
            }
            simd::blend(s.mo.level, &take_a[..words], la, lb, &mut s.rd.lanes[..words * 64]);
        }
        s.pack_bools();
    }
}

impl Step<'_, '_> {
    /// SELECT's per-row "take `a`" bits, `cond` true and not NULL, read off its
    /// truth bits; the destination's null words are the chosen branch's.
    fn select_take(&mut self, cond: u16, a: u16, b: u16) -> [u64; NULL_WORDS_PER_REG] {
        let (rc, words) = (self.reg(cond), self.words());
        let mut take_a = [0u64; NULL_WORDS_PER_REG];
        if !self.mo.prog.may_null(self.d) {
            take_a[..words].copy_from_slice(&rc.bools[..words]);
            return take_a;
        }
        let (ra, rb) = (self.reg(a), self.reg(b));
        for (w, t) in take_a[..words].iter_mut().enumerate() {
            *t = rc.bools[w] & !rc.nulls[w];
            self.rd.nulls[w] = (*t & ra.nulls[w]) | (!*t & rb.nulls[w]);
        }
        take_a
    }
}

// ---------------------------------------------------------------------------
// String kernels
// ---------------------------------------------------------------------------

impl Regs<'_, '_> {
    /// A German-string column's cells as views: a short cell's its inline
    /// bytes in the column region, a long one's its blob bytes, a corrupt one
    /// empty.
    #[inline(never)]
    fn load_col_str(self, d: u16, pi: u8) {
        let mut s = self.at(d);
        let (start, m, bufs) = (s.mo.start, s.m, s.mo.bufs);
        let buf = SRC_COL_BASE + pi as u32;
        let rows = bufs.cells::<16>(pi, start, m);
        let ([], lane) = split_views(s.views, [], d, m);
        for (i, (v, cell)) in lane.iter_mut().zip(rows).enumerate() {
            *v = match german_string_inline(cell) {
                Some(inline) => StrView::at(buf, (start + i) * 16 + gnitz_wire::GERMAN_INLINE_OFF, inline.len()),
                None => match german_string_heap(cell, bufs.blob.len()) {
                    Some(r) => StrView::at(SRC_BLOB, r.start, r.len()),
                    None => StrView::default(),
                },
            };
        }
        s.load_nulls(1u64 << pi);
    }

    /// Compute every row of the scalar destination from string register `a`;
    /// `f`'s `None` is a NULL. `f` runs over NULL rows too, keeping the loop
    /// branch-free.
    #[inline]
    fn str_to_scalar(self, d: u16, a: u16, mut f: impl FnMut(&[u8]) -> Option<i64>) {
        let mut s = self.at(d);
        let (m, bufs) = (s.m, s.mo.bufs);
        let mut bad = [0u8; MORSEL];
        let (views, arena) = (&*s.views, &**s.arena);
        // Every window cut to the morsel: relating them is what lets LLVM drop
        // the source's per-row bounds checks.
        let src = str_lane(views, a as usize, m);
        for (i, r) in s.rd.lanes[..m].iter_mut().enumerate() {
            *r = f(view_bytes(src[i], arena, bufs)).unwrap_or_else(|| {
                bad[i] = 1;
                0
            });
        }
        s.or_nulls([a]);
        s.or_failures(&bad);
        s.pack_bools();
    }

    /// [`Self::str_to_scalar`] over string registers `a` and `b`, inlined into
    /// the dispatch (`str_const_filter_bench`'s `registers` shapes).
    #[inline(always)]
    fn str_pair_to_scalar(self, d: u16, a: u16, b: u16, mut f: impl FnMut(&[u8], &[u8]) -> Option<i64>) {
        let mut s = self.at(d);
        let (m, bufs) = (s.m, s.mo.bufs);
        let mut bad = [0u8; MORSEL];
        let arena = &**s.arena;
        // Each operand resolved to bytes a column at a time: resolving both per
        // row is an out-of-line call per operand per row.
        let mut cols: [[&[u8]; MORSEL]; 2] = [[&[]; MORSEL]; 2];
        for (c, reg) in cols.iter_mut().zip([a, b]) {
            for (d, &v) in c[..m].iter_mut().zip(str_lane(s.views, reg as usize, m)) {
                *d = view_bytes(v, arena, bufs);
            }
        }
        for (i, r) in s.rd.lanes[..m].iter_mut().enumerate() {
            *r = f(cols[0][i], cols[1][i]).unwrap_or_else(|| {
                bad[i] = 1;
                0
            });
        }
        s.or_nulls([a, b]);
        s.or_failures(&bad);
        s.pack_bools();
    }

    /// Produce every row of the string destination from `S` string and `I`
    /// integer operands; `f`'s `None` is a NULL.
    #[inline]
    fn str_kernel<const S: usize, const I: usize>(
        self,
        d: u16,
        strs: [u16; S],
        ints: [u16; I],
        f: impl Fn(&mut Vec<u8>, [StrView; S], [i128; I]) -> Option<StrView>,
    ) {
        let mut s = self.at(d);
        let mut bad = [0u8; MORSEL];
        let lanes = ints.map(|r| s.int_lane(r));
        let (srcs, vd) = split_views(s.views, strs, d, s.m);
        for (i, r) in vd.iter_mut().enumerate() {
            let views = srcs.map(|w| w[i]);
            let nums = lanes.map(|l| l.get(i));
            *r = f(s.arena, views, nums).unwrap_or_else(|| {
                bad[i] = 1;
                StrView::default()
            });
        }
        s.or_nulls(strs.into_iter().chain(ints));
        s.or_failures(&bad);
    }

    /// SUBSTRING: a sub-view of the source, the bytes never copied.
    ///
    /// Kept apart from [`Self::str_kernel`], whose `<1, 2>` case this is; the
    /// `str_substr` shape of `expr_kernel_bench` measures the split.
    fn str_substr(self, d: u16, si: u16, start: u16, len: Option<u16>) {
        let mut s = self.at(d);
        let (m, bufs) = (s.m, s.mo.bufs);
        let mut bad = [0u8; MORSEL];
        let (start_l, len_l) = (s.int_lane(start), len.map(|l| s.int_lane(l)));
        let ([sv], dv) = split_views(s.views, [si], d, m);
        for i in 0..m {
            let v = sv[i];
            let s = view_bytes(v, s.arena, bufs);
            // 0-based character indices, clamped to the byte length, which
            // bounds the character count.
            let n = s.len() as i128;
            let lo = start_l.get(i) - 1;
            let hi = match len_l {
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
        // The fail flag is a negative length, so the no-FOR form has none —
        // matching its `Operands` entry, which leaves `makes_null` unset.
        s.or_nulls([Some(si), Some(start), len].into_iter().flatten());
        s.or_failures(&bad);
    }

    /// `||` and the CONCAT fold step. `skip_null` is CONCAT's asymmetric rule:
    /// a NULL `b` contributes the empty string, a NULL `a` propagates.
    #[inline(never)]
    fn str_concat(self, d: u16, a: u16, b: u16, skip_null: bool) {
        let mut s = self.at(d);
        let m = s.m;
        let mut bad = [0u8; MORSEL];
        let rb = s.reg(b);
        let ([sa, sb], sd) = split_views(s.views, [a, b], d, m);
        for i in 0..m {
            let va = sa[i];
            // Under CONCAT's rule a NULL argument contributes the empty string,
            // whatever view its lane happens to carry.
            let b_is_null = skip_null && null_word_get(rb.nulls[i / 64], i % 64);
            let vb = if b_is_null { StrView::default() } else { sb[i] };
            let Some(v) = StrView::arena(s.arena.len(), va.len as usize + vb.len as usize) else {
                bad[i] = 1;
                sd[i] = StrView::default();
                continue;
            };
            arena_push_view(s.arena, s.mo.bufs, va);
            arena_push_view(s.arena, s.mo.bufs, vb);
            sd[i] = v;
        }
        if skip_null {
            s.or_nulls([a]);
        } else {
            s.or_nulls([a, b]);
        }
        s.or_failures(&bad);
    }
}

// ---------------------------------------------------------------------------
// String comparison — one kernel over two interchangeable operands
// ---------------------------------------------------------------------------

/// Where a compare operand's cell for morsel row `i` comes from.
trait Cells: Copy {
    fn cell(&self, i: usize) -> &[u8; 16];
    /// Every row's cell, in row order.
    fn iter(&self) -> impl Iterator<Item = &[u8; 16]>;
    /// Each row's content, a long body read out of `blob`.
    fn contents<'b>(&'b self, blob: &'b [u8]) -> impl Fn(usize) -> &'b [u8];
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
    #[inline(always)]
    fn contents<'b>(&'b self, blob: &'b [u8]) -> impl Fn(usize) -> &'b [u8] {
        move |i| german_string_content(&self.0[i], blob)
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
    /// Resolved once, not in each row's heap read.
    #[inline(always)]
    fn contents<'b>(&'b self, blob: &'b [u8]) -> impl Fn(usize) -> &'b [u8] {
        let content = german_string_content(self.0, blob);
        move |_| content
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
        StrOperand {
            cells: ColumnCells(mo.bufs.cells::<16>(pi, mo.start, mo.m)),
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

/// A cell's two words: its length under its prefix, then its suffix or heap
/// offset.
#[inline(always)]
fn cell_words(cell: &[u8; 16]) -> (u64, u64) {
    (
        u64::from_le_bytes(cell[..8].try_into().unwrap()),
        u64::from_le_bytes(cell[8..].try_into().unwrap()),
    )
}

impl Regs<'_, '_> {
    /// A German-string compare, one row loop per operator, compiled out of line
    /// from the dispatch (`str_const_filter_bench`). The verdicts are computed
    /// before the register file is split, so only the operands are live across
    /// the heap reads.
    #[inline(never)]
    fn str_cmp<A: Cells, B: Cells>(self, d: u16, op: CmpOp, a: StrOperand<'_, A>, b: StrOperand<'_, B>) {
        let m = self.mo.m;
        let bits = match op {
            CmpOp::Eq => eq_verdicts(m, a, b, false),
            CmpOp::Ne => eq_verdicts(m, a, b, true),
            CmpOp::Gt => order_verdicts(m, a, b, |o| o == Ordering::Greater),
            CmpOp::Ge => order_verdicts(m, a, b, |o| o != Ordering::Less),
            CmpOp::Lt => order_verdicts(m, a, b, |o| o == Ordering::Less),
            CmpOp::Le => order_verdicts(m, a, b, |o| o != Ordering::Greater),
        };
        self.at(d).set_verdicts(a.null_bit | b.null_bit, &bits);
    }

    /// `LIKE 'lit%'` over a German-string column, compiled out of line as
    /// [`Self::str_cmp`] is.
    #[inline(never)]
    fn str_prefix<A: Cells>(self, d: u16, a: StrOperand<'_, A>, lit: &[u8]) {
        let bits = prefix_verdicts(self.mo.m, a, lit);
        self.at(d).set_verdicts(a.null_bit, &bits);
    }
}

impl Step<'_, '_> {
    /// A compare's verdicts as the destination's truth bits, NULL where a
    /// payload column of `null_cols` is.
    #[inline(always)]
    fn set_verdicts(mut self, null_cols: u64, bits: &[u64; NULL_WORDS_PER_REG]) {
        self.load_nulls(null_cols);
        self.rd.bools = *bits;
        self.unpack_bools();
    }
}

/// The ordering string compare's verdicts over `m` rows. The compare runs
/// unconditionally, including on rows where either operand's column is null,
/// keeping the loop branch-free. `str_const_filter_bench` measures the inlining.
#[inline(always)]
fn order_verdicts<A: Cells, B: Cells>(
    m: usize,
    a: StrOperand<'_, A>,
    b: StrOperand<'_, B>,
    pred: impl Fn(Ordering) -> bool,
) -> [u64; NULL_WORDS_PER_REG] {
    let mut hit = [0u8; MORSEL];
    for (i, h) in hit[..m].iter_mut().enumerate() {
        *h = pred(compare_german_strings(
            a.cells.cell(i),
            Heap::Plain(a.blob),
            b.cells.cell(i),
            Heap::Plain(b.blob),
        )) as u8;
    }
    let mut bits = [0u64; NULL_WORDS_PER_REG];
    pack_bytes(&hit, &mut bits);
    bits
}

/// String equality's verdicts over `m` rows, `<>` when `ne`: one branch-free
/// pass over the cells settles every short row, and only a long row whose length
/// and prefix agree reads the heap.
#[inline(always)]
fn eq_verdicts<A: Cells, B: Cells>(
    m: usize,
    a: StrOperand<'_, A>,
    b: StrOperand<'_, B>,
    ne: bool,
) -> [u64; NULL_WORDS_PER_REG] {
    let (mut eq, mut long_match) = ([0u8; MORSEL], [0u8; MORSEL]);
    let cells = a.cells.iter().zip(b.cells.iter()).take(m);
    for ((eq, long_match), (ca, cb)) in eq.iter_mut().zip(&mut long_match).zip(cells) {
        let ((a0, a1), (b0, b1)) = (cell_words(ca), cell_words(cb));
        let long = a0 as u32 > gnitz_wire::SHORT_STRING_THRESHOLD as u32;
        *eq = ((a0 == b0) & (a1 == b1) & !long) as u8;
        *long_match = ((a0 == b0) & long) as u8;
    }
    let flip = if ne { u64::MAX } else { 0 };
    settle_long(m, &eq, &long_match, || {
        let (ca, cb) = (a.cells.contents(a.blob), b.cells.contents(b.blob));
        move |i| ca(i) == cb(i)
    })
    .map(|w| w ^ flip)
}

/// `LIKE 'lit%'`'s verdicts over `m` rows: one branch-free pass over the cells
/// settles every short row, and every row when the literal fits the 4-byte
/// prefix; only a long row whose length and prefix admit the literal reads the
/// heap.
#[inline(always)]
fn prefix_verdicts<A: Cells>(m: usize, a: StrOperand<'_, A>, lit: &[u8]) -> [u64; NULL_WORDS_PER_REG] {
    let l = lit.len();
    let in_cell = l.min(gnitz_wire::SHORT_STRING_THRESHOLD);
    let mut want = [0u8; 16];
    want[gnitz_wire::GERMAN_INLINE_OFF..][..in_cell].copy_from_slice(&lit[..in_cell]);
    // The literal's bytes in each cell word: the prefix half of the first, the
    // inline tail of the second. A short cell's own bytes past the literal are
    // masked off, so its zero padding is never read.
    let m0 = low_bits_mask(8 * in_cell.min(4)) << 32;
    let m1 = low_bits_mask(8 * in_cell.saturating_sub(4));
    let (w0, w1) = cell_words(&want);
    let (l0, l1) = (w0 & m0, w1 & m1);
    let short_lit = l <= 4;
    let (mut hit, mut long_match) = ([0u8; MORSEL], [0u8; MORSEL]);
    let cells = a.cells.iter().take(m);
    for ((hit, long_match), c) in hit.iter_mut().zip(&mut long_match).zip(cells) {
        let (c0, c1) = cell_words(c);
        let len = c0 as u32 as usize;
        let long = len > gnitz_wire::SHORT_STRING_THRESHOLD;
        let head = (len >= l) & ((c0 & m0) == l0);
        *hit = (head & ((c1 & m1) == l1) & (!long | short_lit)) as u8;
        *long_match = (head & long & !short_lit) as u8;
    }
    settle_long(m, &hit, &long_match, || {
        let ca = a.cells.contents(a.blob);
        move |i| ca(i).get(..l).is_some_and(|h| lit_eq(h, lit))
    })
}

/// A string compare's verdicts: true at each row of `hit`, which the cell pass
/// settled, and at each row of `long` that the heap read `resolver()` builds
/// holds; it is built only for a morsel with such a row. Whole words; a
/// verdict's reader masks past the last row.
#[inline(always)]
fn settle_long<R: Fn(usize) -> bool>(
    m: usize,
    hit: &[u8; MORSEL],
    long: &[u8; MORSEL],
    resolver: impl FnOnce() -> R,
) -> [u64; NULL_WORDS_PER_REG] {
    let mut bits = [0u64; NULL_WORDS_PER_REG];
    pack_bytes(hit, &mut bits);
    // A fold rather than `any`: with no early exit it vectorizes.
    if long.iter().fold(0u8, |acc, &c| acc | c) != 0 {
        let mut pending = [0u64; NULL_WORDS_PER_REG];
        pack_bytes(long, &mut pending);
        let resolve = resolver();
        for (w, &word) in pending.iter().enumerate().take(m.div_ceil(64)) {
            for j in gnitz_wire::BitIter(word) {
                bits[w] |= (resolve(w * 64 + j) as u64) << j;
            }
        }
    }
    bits
}

// ---------------------------------------------------------------------------
// eval_morsel — single morsel
// ---------------------------------------------------------------------------

/// How a `Cmp` reads its two registers' bits.
#[derive(Clone, Copy)]
enum IntOrder {
    Signed,
    Unsigned,
    /// `a` is unsigned and `b` signed.
    UnsignedSigned,
}

/// The largest IN set scanned a value at a time, by a morsel of at least as
/// many rows, rather than searched per row (the `in*` shapes of
/// `filter_kernel_bench` and `expr_kernel_bench`).
const IN_SET_BITS_MAX: usize = 64;

/// Evaluate the program over one morsel's rows. Results land in
/// `scratch.regs`: a register's values in its lanes, its null bits beside them.
/// [`Evaluator::eval_morsels`] calls it once per morsel.
fn eval_morsel(morsel: &Morsel<'_>, scratch: &mut EvalScratch) {
    let (prog, bufs) = (morsel.prog, morsel.bufs);
    // Back to the constant prefix: the previous morsel's views die here.
    scratch.str_arena.truncate(prog.const_arena.len());

    use Instr::Logical;
    for &(dst, instr) in &prog.instrs {
        // Opaque per opcode, or LLVM hoists every arm's morsel arithmetic into one
        // prologue each morsel pays; `filter_kernel_bench` measures it.
        let mo = std::hint::black_box(morsel);
        let (start, m) = (mo.start, mo.m);
        let k = Regs { mo, x: &mut *scratch };
        match instr {
            // ----------------------------------------------------------------
            // Load operations
            // ----------------------------------------------------------------
            Instr::LoadPayloadInt { pi, fi } => {
                let mut s = k.at(dst);
                gnitz_wire::for_each_fixed_int!(fi, |FI| {
                    const W: usize = FI.width();
                    for (r, c) in s.rd.lanes[..m].iter_mut().zip(bufs.cells::<W>(pi, start, m)) {
                        *r = FI.decode_le_i64(c);
                    }
                });
                s.load_nulls(1u64 << pi);
                s.pack_bools();
            }

            // Every float register holds an f64 image, so an F32 column widens
            // on load.
            Instr::LoadPayloadF32 { pi } => {
                let mut s = k.at(dst);
                for (r, c) in s.rd.lanes[..m].iter_mut().zip(bufs.cells::<4>(pi, start, m)) {
                    *r = encode_f64(f32::from_le_bytes(*c) as f64);
                }
                s.load_nulls(1u64 << pi);
                s.pack_bools();
            }

            // A PK column undoes the OPK encoding; it is never NULL.
            Instr::LoadPk { off, fi } => {
                let mut s = k.at(dst);
                let (pk, stride) = mo.pk;
                let rows = &pk[start * stride..(start + m) * stride];
                gnitz_wire::for_each_fixed_int!(fi, |FI| {
                    let lanes = s.rd.lanes[..m].iter_mut();
                    gnitz_wire::zip_cells::<{ FI.width() }, _>(rows, stride, off as usize, lanes, |c, r| {
                        *r = gnitz_wire::decode_opk_i64(c, FI)
                    })
                });
                s.pack_bools();
            }

            // ----------------------------------------------------------------
            // Integer arithmetic
            // ----------------------------------------------------------------
            // The operator match is OUTSIDE the row loop, so each arm is its own
            // branch-free loop.
            Logical(L::IntArith { op, a: Reg(a), b: Reg(b) }) => match op {
                IntArithOp::Add => k.lanes(dst, [a, b], |[x, y]| x.wrapping_add(y)),
                IntArithOp::Sub => k.lanes(dst, [a, b], |[x, y]| x.wrapping_sub(y)),
                IntArithOp::Mul => k.lanes(dst, [a, b], |[x, y]| x.wrapping_mul(y)),
                IntArithOp::Div => k.int_divmod::<false>(dst, a, b),
                IntArithOp::Mod => k.int_divmod::<true>(dst, a, b),
            },
            Logical(L::IntUnary { op, a: Reg(a) }) => match op {
                IntUnaryOp::Neg => k.lanes(dst, [a], |[x]| x.wrapping_neg()),
                IntUnaryOp::Abs => k.lanes(dst, [a], |[x]| x.wrapping_abs()),
                IntUnaryOp::Sign if !prog.is_u64(a) => k.lanes(dst, [a], |[x]| x.signum()),
                IntUnaryOp::Sign => k.lanes(dst, [a], |[x]| (x != 0) as i64),
            },
            // One loop per (op, unit), so each inlines `calendar::eval` as that
            // op's arithmetic alone. An op that may NULL runs through the fail mask.
            Logical(L::Calendar { op, a: Reg(a), micros }) => {
                macro_rules! per_op {
                    ($($v:ident)*) => {
                        match (op, micros) {
                            $(
                                (CalendarOp::$v, true) => per_op!(@arm $v, true),
                                (CalendarOp::$v, false) => per_op!(@arm $v, false),
                            )*
                        }
                    };
                    (@arm $v:ident, $m:literal) => {
                        if const { CalendarOp::$v.may_null() } {
                            k.lanes_or_null(dst, [a], |[x]| calendar::eval(CalendarOp::$v, x, $m))
                        } else {
                            k.lanes(dst, [a], |[x]| calendar::eval(CalendarOp::$v, x, $m).0)
                        }
                    };
                }
                per_op!(Year Quarter Month Week Day Dow Isodow Doy Hour Minute Second Epoch TruncYear
                    TruncQuarter TruncMonth TruncWeek TruncDay TruncHour TruncMinute TruncSecond ToMicros ToDays)
            }
            Logical(L::FloatUnary { op, a: Reg(a) }) => match op {
                FloatUnaryOp::Neg => k.lanes(dst, [a], |[x]| encode_f64(-decode_f64(x))),
                FloatUnaryOp::Abs => k.lanes(dst, [a], |[x]| encode_f64(decode_f64(x).abs())),
                FloatUnaryOp::Floor => k.lanes(dst, [a], |[x]| encode_f64(decode_f64(x).floor())),
                FloatUnaryOp::Ceil => k.lanes(dst, [a], |[x]| encode_f64(decode_f64(x).ceil())),
                FloatUnaryOp::Round => k.lanes(dst, [a], |[x]| encode_f64(decode_f64(x).round_ties_even())),
                FloatUnaryOp::Trunc => k.lanes(dst, [a], |[x]| encode_f64(decode_f64(x).trunc())),
                FloatUnaryOp::Sqrt => k.lanes(dst, [a], |[x]| encode_f64(decode_f64(x).sqrt())),
                FloatUnaryOp::Ln => k.lanes(dst, [a], |[x]| encode_f64(decode_f64(x).ln())),
                FloatUnaryOp::Log10 => k.lanes(dst, [a], |[x]| encode_f64(decode_f64(x).log10())),
                FloatUnaryOp::Exp => k.lanes(dst, [a], |[x]| encode_f64(decode_f64(x).exp())),
                // `partial_cmp` against zero: -0.0 and +0.0 are `Equal`, NaN is
                // `None` and stays NaN, as PostgreSQL spells it.
                FloatUnaryOp::Sign => k.lanes(dst, [a], |[x]| {
                    encode_f64(decode_f64(x).partial_cmp(&0.0).map_or(f64::NAN, |o| o as i32 as f64))
                }),
            },
            Logical(L::FloatToF32 { a: Reg(a) }) => k.lanes_or_null(dst, [a], |[x]| {
                let f = gnitz_wire::narrow_f32(decode_f64(x));
                (encode_f64(f.map_or(0.0, f64::from)), f.is_none())
            }),
            Logical(L::IntCast { a: Reg(a), fi }) => {
                let (lo, hi, hi_u) = int_cast_bounds(fi);
                if !prog.is_u64(a) {
                    k.lanes_or_null(dst, [a], |[x]| (x, x < lo || x > hi))
                } else {
                    k.lanes_or_null(dst, [a], |[x]| (x, (x as u64) > hi_u))
                }
            }
            // Truncate toward zero, then range-check in f64: NaN fails every
            // comparison and so fails the check, as ±inf and out-of-range do. The
            // window is half-open at every width: after `trunc` the value is
            // integral, so `t < 2^k` is exactly `t <= 2^k - 1` — and it avoids
            // naming `2^63-1`/`2^64-1`, neither of which is representable in f64.
            // Both bounds are then powers of two, so the f64 conversion is exact.
            Logical(L::FloatToInt { a: Reg(a), fi }) => {
                let (lo, hi) = fi.range();
                let (flo, fhi) = (lo as f64, (hi + 1) as f64);
                if fi == FixedInt::U64 {
                    k.lanes_or_null(dst, [a], |[x]| {
                        let t = decode_f64(x).trunc();
                        let ok = t >= flo && t < fhi;
                        (if ok { t as u64 as i64 } else { 0 }, !ok)
                    })
                } else {
                    k.lanes_or_null(dst, [a], |[x]| {
                        let t = decode_f64(x).trunc();
                        let ok = t >= flo && t < fhi;
                        (if ok { t as i64 } else { 0 }, !ok)
                    })
                }
            }
            Logical(L::IntMinMax2 { a: Reg(a), b: Reg(b), is_max }) => match (is_max, prog.is_u64(dst)) {
                (true, false) => k.minmax2::<simd::GtSigned>(dst, a, b),
                (false, false) => k.minmax2::<simd::Not<simd::GtSigned>>(dst, a, b),
                (true, true) => k.minmax2::<simd::GtUnsigned>(dst, a, b),
                (false, true) => k.minmax2::<simd::Not<simd::GtUnsigned>>(dst, a, b),
            },
            // `total_cmp` and nothing else: -0.0 and +0.0 are `==`-equal but
            // total_cmp-distinct, so an `==`-based pick would let operand order
            // decide which bit pattern survives.
            Logical(L::FloatMinMax2 { a: Reg(a), b: Reg(b), is_max }) => match is_max {
                true => k.minmax2::<simd::GtTotal>(dst, a, b),
                false => k.minmax2::<simd::Not<simd::GtTotal>>(dst, a, b),
            },

            // ----------------------------------------------------------------
            // Integer set membership (col IN (…) as one opcode)
            // ----------------------------------------------------------------
            // A small set is scanned, the packed bits straight off the operand.
            Instr::IntInSet { value_reg, set_idx } => {
                let set = &prog.int_sets[set_idx as usize];
                if set.len() <= IN_SET_BITS_MAX.min(m) {
                    let mut s = k.at(dst);
                    let (ra, words) = (s.reg(value_reg), s.words());
                    simd::in_set_bits(mo.level, &ra.lanes[..words * 64], set, &mut s.rd.bools[..words]);
                    s.or_nulls([value_reg]);
                    s.unpack_bools();
                } else {
                    k.lanes(dst, [value_reg], |[x]| set.binary_search(&x).is_ok() as i64)
                }
            }

            // ----------------------------------------------------------------
            // Float arithmetic
            // ----------------------------------------------------------------
            Logical(L::FloatArith { op, a: Reg(a), b: Reg(b) }) => match op {
                FloatArithOp::Add => k.lanes(dst, [a, b], |[x, y]| encode_f64(decode_f64(x) + decode_f64(y))),
                FloatArithOp::Sub => k.lanes(dst, [a, b], |[x, y]| encode_f64(decode_f64(x) - decode_f64(y))),
                FloatArithOp::Mul => k.lanes(dst, [a, b], |[x, y]| encode_f64(decode_f64(x) * decode_f64(y))),
                FloatArithOp::Pow => k.lanes(dst, [a, b], |[x, y]| encode_f64(decode_f64(x).powf(decode_f64(y)))),
                FloatArithOp::Div => k.lanes_or_null(dst, [a, b], |[x, y]| {
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
            Logical(L::Cmp { op, a: Reg(a), b: Reg(b) }) => {
                // Each operand is read with its own signedness, so a U64 value
                // past 2^63 never equals or orders against a negative one; a
                // signed/unsigned pair is swapped into the `UnsignedSigned`
                // orientation.
                let (op, a, b, order) = match (prog.is_u64(a), prog.is_u64(b)) {
                    (false, false) => (op, a, b, IntOrder::Signed),
                    (true, true) => (op, a, b, IntOrder::Unsigned),
                    (true, false) => (op, a, b, IntOrder::UnsignedSigned),
                    (false, true) => (op.converse(), b, a, IntOrder::UnsignedSigned),
                };
                match (op, order) {
                    (CmpOp::Eq, IntOrder::Signed | IntOrder::Unsigned) => k.pred::<simd::Eq>(dst, a, b),
                    (CmpOp::Ne, IntOrder::Signed | IntOrder::Unsigned) => k.pred::<simd::Not<simd::Eq>>(dst, a, b),
                    (CmpOp::Gt, IntOrder::Signed) => k.pred::<simd::GtSigned>(dst, a, b),
                    (CmpOp::Ge, IntOrder::Signed) => k.pred::<simd::Not<simd::GtSigned>>(dst, b, a),
                    (CmpOp::Lt, IntOrder::Signed) => k.pred::<simd::GtSigned>(dst, b, a),
                    (CmpOp::Le, IntOrder::Signed) => k.pred::<simd::Not<simd::GtSigned>>(dst, a, b),
                    (CmpOp::Gt, IntOrder::Unsigned) => k.pred::<simd::GtUnsigned>(dst, a, b),
                    (CmpOp::Ge, IntOrder::Unsigned) => k.pred::<simd::Not<simd::GtUnsigned>>(dst, b, a),
                    (CmpOp::Lt, IntOrder::Unsigned) => k.pred::<simd::GtUnsigned>(dst, b, a),
                    (CmpOp::Le, IntOrder::Unsigned) => k.pred::<simd::Not<simd::GtUnsigned>>(dst, a, b),
                    (CmpOp::Eq, IntOrder::UnsignedSigned) => k.pred::<simd::EqUnsignedSigned>(dst, a, b),
                    (CmpOp::Ne, IntOrder::UnsignedSigned) => k.pred::<simd::Not<simd::EqUnsignedSigned>>(dst, a, b),
                    (CmpOp::Lt, IntOrder::UnsignedSigned) => k.pred::<simd::LtUnsignedSigned>(dst, a, b),
                    (CmpOp::Le, IntOrder::UnsignedSigned) => k.pred::<simd::LeUnsignedSigned>(dst, a, b),
                    (CmpOp::Gt, IntOrder::UnsignedSigned) => k.pred::<simd::Not<simd::LeUnsignedSigned>>(dst, a, b),
                    (CmpOp::Ge, IntOrder::UnsignedSigned) => k.pred::<simd::Not<simd::LtUnsignedSigned>>(dst, a, b),
                }
            }

            // ----------------------------------------------------------------
            // Float comparisons
            // ----------------------------------------------------------------
            // `<` and `<=` are the mirrored comparison, which a NaN fails as it
            // fails them; `<>` is the complement of `=`, which a NaN passes.
            Logical(L::FCmp { op, a: Reg(a), b: Reg(b) }) => match op {
                CmpOp::Eq => k.pred::<simd::EqFloat>(dst, a, b),
                CmpOp::Ne => k.pred::<simd::Not<simd::EqFloat>>(dst, a, b),
                CmpOp::Gt => k.pred::<simd::GtFloat>(dst, a, b),
                CmpOp::Ge => k.pred::<simd::GeFloat>(dst, a, b),
                CmpOp::Lt => k.pred::<simd::GtFloat>(dst, b, a),
                CmpOp::Le => k.pred::<simd::GeFloat>(dst, b, a),
            },

            // ----------------------------------------------------------------
            // Boolean 3VL
            // ----------------------------------------------------------------
            // Word operations over the packed bits. Outside `null_regs` it is
            // plain AND / OR over the truth words, reading and writing no null
            // word. The unpack to `dst`'s lanes is skipped for a bit_only
            // destination, whose only readers are boolean.
            Logical(L::BoolBinary { a: Reg(a), b: Reg(b), is_or }) => {
                let mut s = k.at(dst);
                let (ra, rb, words) = (s.reg(a), s.reg(b), s.words());
                if !prog.may_null(dst) {
                    for ((d, &va), &vb) in s.rd.bools[..words]
                        .iter_mut()
                        .zip(&ra.bools[..words])
                        .zip(&rb.bools[..words])
                    {
                        *d = if is_or { va | vb } else { va & vb };
                    }
                } else {
                    for w in 0..words {
                        let (va, vb, na, nb) = (ra.bools[w], rb.bools[w], ra.nulls[w], rb.nulls[w]);
                        // SQL 3VL: the result is NULL where neither side settles
                        // it — a non-NULL true for OR, a non-NULL false for AND —
                        // and one side is NULL.
                        let (bits, nd) = if is_or {
                            let definite_true = (!na & va) | (!nb & vb);
                            (definite_true, !definite_true & (na | nb))
                        } else {
                            let definite_false = (!na & !va) | (!nb & !vb);
                            let nd = !definite_false & (na | nb);
                            (!nd & va & vb, nd)
                        };
                        s.rd.bools[w] = bits;
                        s.rd.nulls[w] = nd;
                    }
                }
                s.unpack_bools();
            }
            // 3VL: NOT NULL = NULL, its truth bit cleared.
            Logical(L::BoolNot { a: Reg(a) }) => {
                let mut s = k.at(dst);
                let (ra, words) = (s.reg(a), s.words());
                for w in 0..words {
                    s.rd.bools[w] = !ra.bools[w] & !ra.nulls[w];
                    s.rd.nulls[w] = ra.nulls[w];
                }
                s.unpack_bools();
            }

            // ----------------------------------------------------------------
            // IS NULL / IS NOT NULL — a definite boolean, never NULL itself
            // ----------------------------------------------------------------
            // Payload column `pi`'s bit of the batch bitmap, per row.
            Instr::IsNull { pi, invert } => {
                let mut s = k.at(dst);
                let (flip, words) = (if invert { u64::MAX } else { 0 }, s.words());
                let bools = &mut s.rd.bools[..words];
                simd::null_bits(mo.level, mo.null_words(), 1u64 << pi, bools);
                for w in bools {
                    *w ^= flip;
                }
                s.unpack_bools();
            }
            // Register `a`'s null words *are* the packed boolean.
            Logical(L::IsNullReg { a: Reg(a), invert }) => {
                let mut s = k.at(dst);
                let (flip, ra, words) = (if invert { u64::MAX } else { 0 }, s.reg(a), s.words());
                for (d, &n) in s.rd.bools[..words].iter_mut().zip(&ra.nulls) {
                    *d = n ^ flip;
                }
                s.unpack_bools();
            }

            // ----------------------------------------------------------------
            // Type cast. A U64 register is read as u64 first, so values >= 2^63
            // cast to the correct large positive float.
            // ----------------------------------------------------------------
            Logical(L::IntToFloat { a: Reg(a) }) => match prog.is_u64(a) {
                false => k.lanes(dst, [a], |[x]| encode_f64(x as f64)),
                true => k.lanes(dst, [a], |[x]| encode_f64(x as u64 as f64)),
            },

            // ----------------------------------------------------------------
            // Conditional select (SQL CASE blend): rows where `cond` is true and
            // not NULL take `a`'s value and null bit, every other row `b`'s.
            // ----------------------------------------------------------------
            Logical(L::Select { cond: Reg(cond), a: Reg(a), b: Reg(b) }) => {
                let mut s = k.at(dst);
                let (take_a, words) = (s.select_take(cond, a, b), s.words());
                let (la, lb) = (&s.reg(a).lanes[..words * 64], &s.reg(b).lanes[..words * 64]);
                simd::blend(mo.level, &take_a[..words], la, lb, &mut s.rd.lanes[..words * 64]);
                s.pack_bools();
            }
            Logical(L::StrSelect { cond: Reg(cond), a: Reg(a), b: Reg(b) }) => {
                let mut s = k.at(dst);
                let take_a = s.select_take(cond, a, b);
                let ([sa, sb], sd) = split_views(s.views, [a, b], dst, m);
                for ((sd, (sa, sb)), &word) in sd.chunks_mut(64).zip(sa.chunks(64).zip(sb.chunks(64))).zip(&take_a) {
                    for (j, (v, (&x, &y))) in sd.iter_mut().zip(sa.iter().zip(sb)).enumerate() {
                        *v = if (word >> j) & 1 != 0 { x } else { y };
                    }
                }
            }

            // ----------------------------------------------------------------
            // String comparisons (column vs constant / column vs column)
            // ----------------------------------------------------------------
            // Resolution rewrites every opcode that names a column or a pool
            // entry, and lifts out every constant.
            Logical(
                L::LoadCol { .. }
                | L::LoadConst { .. }
                | L::LoadNull
                | L::LoadNullStr
                | L::LoadConstStr { .. }
                | L::IsNull { .. }
                | L::StrColConst { .. }
                | L::StrColCol { .. }
                | L::StrColPrefix { .. }
                | L::IntInSet { .. }
                | L::LoadColStr { .. }
                | L::StrTrim { .. }
                | L::StrLike { .. },
            ) => unreachable!("resolution lowers {instr:?}"),
            Instr::StrColConst { op, pi, const_idx } => k.str_cmp(
                dst,
                op,
                StrOperand::column(pi, mo),
                StrOperand::constant(prog, const_idx as usize),
            ),
            Instr::StrColCol { op, pi_a, pi_b } => {
                k.str_cmp(dst, op, StrOperand::column(pi_a, mo), StrOperand::column(pi_b, mo))
            }
            Instr::StrColPrefix { pi, off, len } => k.str_prefix(
                dst,
                StrOperand::column(pi, mo),
                &prog.const_arena[off as usize..][..len as usize],
            ),

            // ----------------------------------------------------------------
            // String registers
            // ----------------------------------------------------------------
            // A short cell's view is its inline bytes in the column region, a
            // long one's its blob bytes, a corrupt one empty.
            Instr::LoadColStr { pi } => k.load_col_str(dst, pi),

            Logical(L::StrCmp { op, a: Reg(a), b: Reg(b) }) => match op {
                CmpOp::Eq => k.str_pair_to_scalar(dst, a, b, |x, y| Some((x == y) as i64)),
                CmpOp::Ne => k.str_pair_to_scalar(dst, a, b, |x, y| Some((x != y) as i64)),
                CmpOp::Gt => k.str_pair_to_scalar(dst, a, b, |x, y| Some((x > y) as i64)),
                CmpOp::Ge => k.str_pair_to_scalar(dst, a, b, |x, y| Some((x >= y) as i64)),
                CmpOp::Lt => k.str_pair_to_scalar(dst, a, b, |x, y| Some((x < y) as i64)),
                CmpOp::Le => k.str_pair_to_scalar(dst, a, b, |x, y| Some((x <= y) as i64)),
            },

            // Characters are counted off the bytes; a byte length is the view's own.
            Logical(L::StrLen { a: Reg(a), chars: true }) => k.str_to_scalar(dst, a, |x| Some(char_count(x) as i64)),
            Logical(L::StrLen { a: Reg(a), chars: false }) => {
                let mut s = k.at(dst);
                for (r, v) in s.rd.lanes[..m].iter_mut().zip(str_lane(s.views, a as usize, m)) {
                    *r = v.len as i64;
                }
                s.or_nulls([a]);
                s.pack_bools();
            }

            // A fresh copy in the arena, folded in place; one loop per direction.
            Logical(L::StrCase { a: Reg(a), upper }) => {
                if upper {
                    k.str_kernel(dst, [a], [], |arena, [v], []| {
                        Some(arena_copy_with(arena, bufs, v, |s| s.make_ascii_uppercase()))
                    })
                } else {
                    k.str_kernel(dst, [a], [], |arena, [v], []| {
                        Some(arena_copy_with(arena, bufs, v, |s| s.make_ascii_lowercase()))
                    })
                }
            }

            Logical(L::StrSubstr { src: Reg(src), start_reg, len_reg }) => {
                k.str_substr(dst, src, start_reg.0, len_reg.map(|l| l.0))
            }

            // A sub-view of the source: the bytes are not copied, only the
            // offset and length narrowed. The closure holds its own copy of
            // `bufs`, which keeps the per-row view resolution out of a stack
            // copy; the `str_trim` shape of `expr_kernel_bench` measures it.
            Instr::StrTrim { a, mode, set_idx } => {
                let set = &prog.trim_sets[set_idx as usize];
                k.str_kernel(dst, [a], [], move |arena, [v], []| {
                    let s = view_bytes(v, arena, bufs);
                    let (mut lo, mut hi) = (0usize, s.len());
                    if matches!(mode, TrimMode::Both | TrimMode::Leading) {
                        while lo < hi && in_trim_set(set, s[lo]) {
                            lo += 1;
                        }
                    }
                    if matches!(mode, TrimMode::Both | TrimMode::Trailing) {
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
                k.str_to_scalar(dst, src, |x| Some(matcher.matches(x, &mut folded) as i64));
            }

            Logical(L::StrConcat { a: Reg(a), b: Reg(b), skip_null }) => k.str_concat(dst, a, b, skip_null),

            // One loop per signedness and per integer-or-DECIMAL, so none tests
            // either per row. A DECIMAL is signed; resolve admits its scale only
            // up to `MAX_DECIMAL_SCALE`. Each kernel reads the register's bits
            // back out of the widened lane, whichever way it widened.
            Logical(L::IntToStr { a: Reg(a), scale }) => match (prog.is_u64(a), scale) {
                (false, 0) => k.str_kernel(dst, [], [a], |arena, [], [v]| {
                    let v = v as i64;
                    Some(arena_push_text(arena, |b| scaled_bytes(v.unsigned_abs(), v < 0, 0, b)))
                }),
                (true, 0) => k.str_kernel(dst, [], [a], |arena, [], [v]| {
                    Some(arena_push_text(arena, |b| scaled_bytes(v as u64, false, 0, b)))
                }),
                (_, sc) => k.str_kernel(dst, [], [a], |arena, [], [v]| {
                    let v = v as i64;
                    Some(arena_push_text(arena, |b| scaled_bytes(v.unsigned_abs(), v < 0, sc, b)))
                }),
            },
            Logical(L::TemporalToStr { a: Reg(a), micros }) => match micros {
                true => k.str_kernel(dst, [], [a], |arena, [], [v]| {
                    Some(arena_push_text(arena, |b| timestamp_bytes(v as i64, b)))
                }),
                false => k.str_kernel(dst, [], [a], |arena, [], [v]| {
                    Some(arena_push_text(arena, |b| date_bytes(v as i64, b)))
                }),
            },
            // The narrowing is exact: an `f32` register holds a widened f32.
            Logical(L::FloatToStr { a: Reg(a), f32 }) => match f32 {
                true => k.str_kernel(dst, [], [a], |arena, [], [v]| {
                    Some(arena_push_float(arena, FloatText(decode_f64(v as i64) as f32)))
                }),
                false => k.str_kernel(dst, [], [a], |arena, [], [v]| {
                    Some(arena_push_float(arena, FloatText(decode_f64(v as i64))))
                }),
            },

            Logical(L::StrToInt { a: Reg(a), fi }) => {
                let (lo, hi) = fi.range();
                // A U64 value above i64::MAX narrows to its own bit pattern,
                // which is what the register holds; the resolve-time U64
                // tracking makes downstream reads agree.
                k.str_to_scalar(dst, a, |x| {
                    parse_decimal_i128(x).filter(|v| *v >= lo && *v <= hi).map(|v| v as i64)
                });
            }

            Logical(L::StrToFloat { a: Reg(a) }) => {
                k.str_to_scalar(dst, a, |x| {
                    std::str::from_utf8(x.trim_ascii())
                        .ok()
                        .and_then(|t| t.parse::<f64>().ok())
                        .map(encode_f64)
                });
            }
            Logical(L::StrPos { hay: Reg(hay), needle: Reg(needle) }) => {
                k.str_pair_to_scalar(dst, hay, needle, |h, n| {
                    Some(find(h, n).map_or(0, |off| char_count(&h[..off]) as i64 + 1))
                })
            }
            Logical(L::StrSide { src: Reg(src), n_reg, left }) => {
                k.str_kernel(dst, [src], [n_reg.0], |arena, [v], [n]| {
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
            Logical(L::StrReverse { a: Reg(a) }) => k.str_kernel(dst, [a], [], |arena, [v], []| {
                Some(arena_copy_with(arena, bufs, v, reverse_chars))
            }),
            Logical(L::StrReplace {
                s: Reg(subj),
                from: Reg(from),
                to: Reg(to),
            }) => k.str_kernel(dst, [subj, from, to], [], |arena, [vs, vf, vt], []| {
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
            }),
            Logical(L::StrPad {
                s: Reg(subj),
                n_reg,
                fill: Reg(fill),
                left,
            }) => {
                k.str_kernel(dst, [subj, fill], [n_reg.0], |arena, [vs, vf], [n]| {
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
            Logical(L::StrSplitPart { s: Reg(subj), delim: Reg(delim), n_reg }) => {
                k.str_kernel(dst, [subj, delim], [n_reg.0], |arena, [vs, vd], [n]| {
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
#[path = "tests/morsel.rs"]
mod tests;
