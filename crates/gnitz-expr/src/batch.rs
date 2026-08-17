//! Morsel-oriented batch expression evaluator.
//!
//! `eval_batch` processes one morsel at a time (up to MORSEL rows), applying
//! all expression opcodes as columnar loops over register buffers.  Base
//! pointers are hoisted outside the inner loop, letting LLVM auto-vectorize
//! every arithmetic opcode.

use std::cmp::Ordering;
use std::fmt::{self, Write as _};

use crate::program::{FloatUnaryOp, IntUnaryOp};
use crate::{BatchView, CmpOp, Instr, ResolvedProgram, StrOp};
use gnitz_wire::{
    blob_extent, compare_german_strings, null_word_get, read_u32_le, read_u64_le, FixedInt, SHORT_STRING_THRESHOLD,
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
pub(crate) const NULL_WORDS_PER_REG: usize = MORSEL / 64; // 4

// ---------------------------------------------------------------------------
// EvalScratch — the SoA register file for batch evaluation
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct EvalScratch {
    /// Register buffers, register-major layout: regs[reg * MORSEL + row].
    ///
    /// A lane at a NULL row holds whatever its kernel computed from the row's
    /// stored bytes: kernels run unconditionally to stay branch-free, so a
    /// consumer must read `regs` against [`Self::null_bits`], never alone.
    pub(crate) regs: Vec<i64>,
    /// Null bitmask, register-major: null_bits[reg * NULL_WORDS_PER_REG + word].
    /// Empty (capacity 0) when `no_nulls` is true.
    pub(crate) null_bits: Vec<u64>,
    /// Packed truthy bits, register-major; bridges boolean producers and
    /// consumers on the nullable arm without per-row repack from `regs`.
    /// Empty (capacity 0) when `no_nulls` is true.
    pub(crate) bool_bits: Vec<u64>,
    /// Per-row filter bitmask; written only by the filter path.
    pub(crate) filter_bits: Vec<u64>,
    /// String register lanes, register-major like [`Self::regs`]:
    /// `str_views[reg * MORSEL + row]`. Empty (capacity 0) unless the program
    /// has string instructions; a zero-length default view reads as `""`.
    pub(crate) str_views: Vec<StrView>,
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
    pub(crate) str_arena: Vec<u8>,
    no_nulls: bool,
}

/// `N` shared windows plus one mutable window into the same register-major
/// buffer, each `len` elements wide at `index * stride`. The one unsafe split in
/// this file: `regs` and `null_bits` differ only in element type and stride, and
/// the binary and ternary opcodes only in how many sources they read.
///
/// Sound because the windows are disjoint: SSA register allocation never reuses
/// a destination as one of its own sources, and `LogicalProgram::validate`
/// rejects a program that does for every opcode routed through here — in every
/// profile, since `new`/`from_wire` both run the structure-only pass. The
/// `debug_assert`s restate both halves of that (disjointness, and every window
/// inside the buffer `ensure_capacity` sized).
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
    /// A scratch seeded for `prog`: the string arena starts as a copy of the
    /// program's constant prefix, which the per-morsel reset truncates back to
    /// rather than rebuilding. Installed once here because a scratch belongs to
    /// exactly one [`crate::Evaluator`], hence to one program.
    pub(crate) fn new(prog: &ResolvedProgram) -> Self {
        EvalScratch {
            str_arena: prog.const_arena.clone(),
            ..Default::default()
        }
    }

    /// Ensure the scratch buffer can hold `prog`'s registers and `(n+63)/64`
    /// filter words (`n = 0` for a driver that reads no filter bitmap). Does not
    /// shrink. A zero `null_cap`/`str_cap` is how those buffers stay unallocated
    /// for a program that never touches them.
    ///
    /// Caching `prog.no_nulls` here is what pairs the scratch to one program:
    /// `eval_batch` asserts the two still agree.
    pub(crate) fn ensure_capacity(&mut self, prog: &ResolvedProgram, n: usize) {
        let num_regs = prog.num_regs as usize;
        self.no_nulls = prog.no_nulls;
        let reg_cap = num_regs * MORSEL;
        let null_cap = if prog.no_nulls {
            0
        } else {
            num_regs * NULL_WORDS_PER_REG
        };
        let str_cap = prog.str_lanes as usize * MORSEL;
        let filter_words = n.div_ceil(64);
        if self.regs.len() < reg_cap
            || self.null_bits.len() < null_cap
            || self.bool_bits.len() < null_cap
            || self.str_views.len() < str_cap
            || self.filter_bits.len() < filter_words
        {
            self.grow(reg_cap, null_cap, str_cap, filter_words);
        }
    }

    /// The growth half of [`Self::ensure_capacity`], taken once per evaluator.
    #[cold]
    fn grow(&mut self, reg_cap: usize, null_cap: usize, str_cap: usize, filter_words: usize) {
        if self.regs.len() < reg_cap {
            self.regs.resize(reg_cap, 0);
        }
        if self.null_bits.len() < null_cap {
            self.null_bits.resize(null_cap, 0);
        }
        if self.bool_bits.len() < null_cap {
            self.bool_bits.resize(null_cap, 0);
        }
        if self.str_views.len() < str_cap {
            self.str_views.resize(str_cap, StrView::default());
        }
        if self.filter_bits.len() < filter_words {
            self.filter_bits.resize(filter_words, 0);
        }
    }

    fn reg_mut(&mut self, reg: usize, m: usize) -> &mut [i64] {
        &mut self.regs[reg * MORSEL..reg * MORSEL + m]
    }

    /// Split borrows over `regs`: `N` shared source windows + one mutable
    /// destination. Backs every binary opcode (`N = 2`) and SELECT's no-nulls
    /// value blend (`N = 3`).
    fn regs_split<const N: usize>(&mut self, srcs: [usize; N], d: usize, m: usize) -> ([&[i64]; N], &mut [i64]) {
        split_windows(&mut self.regs, MORSEL, srcs, d, m)
    }

    /// The same split over `null_bits`, whose windows are `NULL_WORDS_PER_REG`
    /// words rather than `MORSEL` values.
    fn null_split<const N: usize>(&mut self, srcs: [usize; N], d: usize, words: usize) -> ([&[u64]; N], &mut [u64]) {
        split_windows(&mut self.null_bits, NULL_WORDS_PER_REG, srcs, d, words)
    }

    /// String register `reg`'s bytes for row `i` of the current morsel — the
    /// single-row read behind [`crate::Evaluator::eval_row_str`]. A lane outside
    /// the allocated file means the program has no string instructions, i.e. a
    /// caller drove a scalar program through the string entry point; the
    /// `debug_assert` catches that without letting a release build panic a
    /// worker over it.
    pub(crate) fn str_reg_bytes<'a>(&'a self, reg: usize, i: usize, blob: &'a [u8]) -> &'a [u8] {
        let idx = reg * MORSEL + i;
        debug_assert!(
            idx < self.str_views.len(),
            "string read on a program with no string lanes"
        );
        match self.str_views.get(idx) {
            Some(&v) => view_bytes(v, &self.str_arena, blob),
            None => &[],
        }
    }

    /// Zero the null bits for one register's morsel region.
    fn clear_null_reg(&mut self, reg: usize, m: usize) {
        if self.no_nulls {
            return;
        }
        let base = reg * NULL_WORDS_PER_REG;
        self.null_bits[base..base + m.div_ceil(64)].fill(0);
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
}

// ---------------------------------------------------------------------------
// Null-bit helpers
// ---------------------------------------------------------------------------

/// Propagate binary null: dst_null = a_null | b_null (word-at-a-time).
fn null_or2(s: &mut EvalScratch, dst: usize, a: usize, b: usize, m: usize) {
    if s.no_nulls {
        return;
    }
    let words = m.div_ceil(64);
    let ([na, nb], nd) = s.null_split([a, b], dst, words);
    for w in 0..words {
        nd[w] = na[w] | nb[w];
    }
}

/// Ternary counterpart: dst_null = a_null | b_null | c_null. SUBSTRING's three
/// operands need it in one pass — chaining [`null_or2`] would make `dst` its own
/// source and trip [`split_windows`]' disjointness precondition.
fn null_or3(s: &mut EvalScratch, dst: usize, a: usize, b: usize, c: usize, m: usize) {
    if s.no_nulls {
        return;
    }
    let words = m.div_ceil(64);
    let ([na, nb, nc], nd) = s.null_split([a, b, c], dst, words);
    for w in 0..words {
        nd[w] = na[w] | nb[w] | nc[w];
    }
}

/// Propagate unary null: dst_null = src_null (word-at-a-time).
fn null_copy1(s: &mut EvalScratch, dst: usize, src: usize, m: usize) {
    if s.no_nulls {
        return;
    }
    let base_s = src * NULL_WORDS_PER_REG;
    s.null_bits
        .copy_within(base_s..base_s + m.div_ceil(64), dst * NULL_WORDS_PER_REG);
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
fn fill_null_bits_mask(s: &mut EvalScratch, di: usize, mo: &Morsel<'_>, cols: u64) {
    if s.no_nulls {
        return;
    }
    let mask = cols & mo.prog.nullable_slots;
    if mask == 0 {
        s.clear_null_reg(di, mo.m);
        return;
    }
    let base = di * NULL_WORDS_PER_REG;
    let rows = mo.null_words();
    let out = &mut s.null_bits[base..base + mo.m.div_ceil(64)];
    for (w, block) in rows.chunks(64 * 8).enumerate() {
        let mut word: u64 = 0;
        for j in 0..block.len() / 8 {
            word |= ((read_u64_le(block, j * 8) & mask != 0) as u64) << j;
        }
        out[w] = word;
    }
}

/// IS [NOT] NULL: read payload column `pi`'s null bit per row, optionally invert
/// (`invert` for IS NOT NULL), and write the boolean into register `dst`. The result
/// register is always non-null (`clear_null_reg`).
///
/// The bitmap read here is the *batch's* — `Morsel` carries it whatever the
/// arm — and the two scratch calls either side no-op under `no_nulls`. So the
/// result is a definite boolean in `regs` on both arms, which is what lets
/// `is_strictly_non_nullable` classify the opcode never-null.
fn eval_is_null(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: usize, pi: usize, invert: bool) {
    scratch.clear_null_reg(dst, mo.m);
    {
        let rows = mo.null_words();
        let rd = scratch.reg_mut(dst, mo.m);
        for (i, r) in rd.iter_mut().enumerate() {
            *r = (null_word_get(read_u64_le(rows, i * 8), pi) ^ invert) as i64;
        }
    }
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// Shared BOOL_AND / BOOL_OR word-level 3VL kernel (nullable arm).
/// `va`/`vb` come from `bool_bits`; `na`/`nb` from `null_bits`.
/// Writes `bool_bits[dst]` and `null_bits[dst]` at word granularity.
fn bool_and_or_word_loop(scratch: &mut EvalScratch, dst: usize, ai: usize, bi: usize, m: usize, is_or: bool) {
    let words = m.div_ceil(64);
    let base_a_n = ai * NULL_WORDS_PER_REG;
    let base_b_n = bi * NULL_WORDS_PER_REG;
    let base_d_n = dst * NULL_WORDS_PER_REG;
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
pub(crate) fn pack_truthy(src: &[i64], out: &mut [u64]) {
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
fn maybe_unpack_bool_to_regs(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: usize) {
    if mo.prog.is_bit_only(dst) {
        return;
    }
    unpack_bool_to_regs(scratch, dst, mo.m);
}

fn unpack_bool_to_regs(scratch: &mut EvalScratch, dst: usize, m: usize) {
    let EvalScratch { regs, bool_bits, .. } = scratch;
    let bits = &bool_bits[dst * NULL_WORDS_PER_REG..];
    let base_r = dst * MORSEL;
    for (w, block) in regs[base_r..base_r + m].chunks_mut(64).enumerate() {
        for (j, r) in block.iter_mut().enumerate() {
            *r = ((bits[w] >> j) & 1) as i64;
        }
    }
}

/// Pack one register's i64 truthy bits into `bool_bits[dst]`.
fn pack_to_bool_bits(scratch: &mut EvalScratch, dst: usize, m: usize) {
    let EvalScratch { regs, bool_bits, .. } = scratch;
    let base_r = dst * MORSEL;
    let base_b = dst * NULL_WORDS_PER_REG;
    pack_truthy(
        &regs[base_r..base_r + m],
        &mut bool_bits[base_b..base_b + m.div_ceil(64)],
    );
}

/// Bridge for producers that wrote `regs[dst]` and may have a downstream BOOL
/// consumer (or, for filters, a bit_only result_reg). Non-bool producers reach
/// a BOOL consumer through this path without restructuring their inner loop.
fn maybe_pack_bool_bits(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: usize) {
    if scratch.no_nulls {
        return;
    }
    if mo.prog.needs_bool_pack(dst) {
        pack_to_bool_bits(scratch, dst, mo.m);
    }
}

/// Call `f(i)` for each row `i` of a morsel of `m` rows whose null bit is set in
/// the register-major `null_bits` window starting at `base`. NULL rows are the
/// exception, so every consumer — zeroing a register's null entries, EMIT's
/// value-slot zero plus output-bitmap merge — scans the set bits rather than
/// branching per row. Per instruction per morsel.
pub(crate) fn for_each_null_row(null_bits: &[u64], base: usize, m: usize, mut f: impl FnMut(usize)) {
    let words = m.div_ceil(64);
    for w in 0..words {
        let mut word = null_bits[base + w];
        // Every producer writes only rows `0..m`, so bits at index >= m are zero
        // and the bit-scan stays in the morsel — no tail re-masking needed.
        debug_assert!(
            w + 1 < words || m.is_multiple_of(64) || (word >> (m % 64)) == 0,
            "null_bits tail word has bits set beyond m={m}",
        );
        let lo = w * 64;
        while word != 0 {
            let bit = word.trailing_zeros() as usize;
            word &= word - 1;
            f(lo + bit);
        }
    }
}

/// OR a per-row failure flag into `dst`'s null words. The flags are collected a
/// byte per row and packed here in a second pass: folding the packing into the
/// value loop makes every iteration read-modify-write the same mask word, which
/// serialises the loop. On the numeric cast kernels, where that was measured,
/// the split was the difference between a scalar and a vectorised loop. The
/// string kernels reuse it for the same reason, unmeasured.
fn merge_fail_mask(scratch: &mut EvalScratch, dst: usize, bad: &[u8; MORSEL], m: usize) {
    if scratch.no_nulls {
        // `no_nulls` comes from `is_strictly_non_nullable`, which decides per
        // opcode whether it can introduce a NULL. If it said no and a row failed
        // anyway, the flag would be dropped here and the row would carry a wrong
        // value with no null bit — silently. Fail the test run instead.
        debug_assert!(
            bad[..m].iter().all(|&b| b == 0),
            "a kernel raised a failure flag under no_nulls: \
             `is_strictly_non_nullable` does not list this opcode as null-producing",
        );
        return;
    }
    let base = dst * NULL_WORDS_PER_REG;
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
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub(crate) struct StrView {
    off: u64,
    len: u32,
    /// [`SRC_ARENA`] or [`SRC_BLOB`]. A column load puts inline cells in the
    /// arena and long cells in the blob, so this varies row to row within one
    /// lane on a mixed-width column — the branch in [`view_bytes_at`] is
    /// data-dependent, not hoistable.
    src: u32,
}

const SRC_ARENA: u32 = 0;
const SRC_BLOB: u32 = 1;

impl StrView {
    /// A view over `[off, off + len)` of `src`'s buffer.
    fn at(src: u32, off: usize, len: usize) -> Self {
        StrView {
            off: off as u64,
            len: len as u32,
            src,
        }
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
fn view_bytes_at<'x>(v: StrView, arena: &'x [u8], blob: &'x [u8]) -> (&'x [u8], usize) {
    let buf = if v.src == SRC_ARENA { arena } else { blob };
    let (o, l) = view_span(v, buf.len());
    (&buf[o..o + l], o)
}

pub(crate) fn view_bytes<'x>(v: StrView, arena: &'x [u8], blob: &'x [u8]) -> &'x [u8] {
    view_bytes_at(v, arena, blob).0
}

/// Append `v`'s bytes to the arena and return the appended span.
///
/// The arena→arena case goes through `extend_from_within`, not
/// `extend_from_slice`: the source borrows the very buffer being grown. Growth
/// never invalidates a view either way — views are offsets, not pointers.
fn arena_push_view(arena: &mut Vec<u8>, blob: &[u8], v: StrView) -> (usize, usize) {
    let start = arena.len();
    let buf_len = if v.src == SRC_ARENA { arena.len() } else { blob.len() };
    let (o, l) = view_span(v, buf_len);
    arena_push_span(arena, blob, v.src, o, l);
    (start, arena.len() - start)
}

/// Append an already-resolved span to the arena. Callers that need the extent
/// for their own arithmetic resolve it once and come here, instead of paying
/// [`view_span`] a second time inside [`arena_push_view`].
fn arena_push_span(arena: &mut Vec<u8>, blob: &[u8], src: u32, o: usize, l: usize) {
    if src == SRC_ARENA {
        arena.extend_from_within(o..o + l);
    } else {
        arena.extend_from_slice(&blob[o..o + l]);
    }
}

/// A 16-byte German-string cell as a view. A long cell points into the blob; a
/// short one's ≤ 12 inline bytes are copied into the arena, so every view is a
/// uniform (buffer, offset, length) triple. The out-of-range clamp is applied
/// here, so LENGTH, the compares, the transforms and EMIT all agree on the
/// degraded value.
fn cell_to_view(cell: &[u8], blob_len: usize, arena: &mut Vec<u8>) -> StrView {
    let length = read_u32_le(cell, 0) as usize;
    if length <= SHORT_STRING_THRESHOLD {
        let off = arena.len();
        arena.extend_from_slice(&cell[4..4 + length]);
        return StrView::arena(off, length);
    }
    match blob_extent(blob_len, read_u64_le(cell, 8), length) {
        Some(r) => StrView::at(SRC_BLOB, r.start, length),
        None => StrView::default(),
    }
}

/// The byte index of every character start, which is the engine's one definition
/// of where a character begins: a byte whose top bits are not `10`. A
/// continuation byte belongs to the character it follows, so a *leading* one
/// belongs to no character at all. On valid UTF-8 these are exactly the codepoint
/// boundaries; on arbitrary bytes it stays total and panic-free, which is what a
/// byte-transparent engine needs.
fn char_starts(s: &[u8]) -> impl Iterator<Item = usize> + '_ {
    s.iter()
        .enumerate()
        .filter(|(_, &b)| (b & 0xC0) != 0x80)
        .map(|(k, _)| k)
}

/// Characters as the engine counts them — on valid UTF-8, the codepoint count.
fn char_count(s: &[u8]) -> usize {
    char_starts(s).count()
}

/// Byte offset of the `n`-th character start at or after `from`, or `s.len()`
/// when the string has fewer — the clamp SUBSTRING's window relies on. `[0x80]`
/// has no character starts, so every offset into it is `s.len()`.
///
/// Resuming from a known character start is what keeps a bounded window's cost
/// proportional to the window rather than to the string.
pub(crate) fn char_offset(s: &[u8], from: usize, n: usize) -> usize {
    char_starts(&s[from..]).nth(n).map_or(s.len(), |k| k + from)
}

fn in_trim_set(set: &[u64; 4], b: u8) -> bool {
    (set[(b >> 6) as usize] >> (b & 63)) & 1 != 0
}

/// Widen a register to the i128 the SUBSTRING window is computed in. Each
/// operand's magnitude is ≤ 2^64, so the sum of two cannot overflow.
fn widen_reg(v: i64, signed: bool) -> i128 {
    if signed {
        v as i128
    } else {
        v as u64 as i128
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
    dst: usize,
    cond: usize,
    a: usize,
    b: usize,
    words: usize,
) -> [u64; NULL_WORDS_PER_REG] {
    let base_cond = cond * NULL_WORDS_PER_REG;
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
/// and what backs both LOAD_NULL opcodes.
fn set_null_reg(s: &mut EvalScratch, dst: usize, m: usize) {
    if s.no_nulls {
        return;
    }
    let words = m.div_ceil(64);
    let base = dst * NULL_WORDS_PER_REG;
    s.null_bits[base..base + words].fill(u64::MAX);
    if !m.is_multiple_of(64) {
        s.null_bits[base + words - 1] = (1u64 << (m % 64)) - 1;
    }
}

/// Transform every row of string register `a` into string register `dst`. `f`
/// gets the arena, the batch blob, and the source view, and returns the result
/// view — it may grow the arena (a fresh copy) or narrow the source in place (a
/// sub-view). The operand's null bit is the only NULL either way.
fn str_to_str(
    scratch: &mut EvalScratch,
    blob: &[u8],
    dst: u16,
    a: u16,
    m: usize,
    f: impl Fn(&mut Vec<u8>, &[u8], StrView) -> StrView,
) {
    let (d, ai) = (dst as usize, a as usize);
    let (base_a, base_d) = (ai * MORSEL, d * MORSEL);
    {
        let EvalScratch {
            str_views, str_arena, ..
        } = &mut *scratch;
        for i in 0..m {
            str_views[base_d + i] = f(str_arena, blob, str_views[base_a + i]);
        }
    }
    null_copy1(scratch, d, ai, m);
}

/// Measure every row of string register `a` into scalar register `dst`. `f` is
/// total — the operand's null bit is the only NULL, so no fail mask is packed —
/// and runs over NULL rows too, on whatever view they carry, keeping the loop
/// branch-free.
fn str_to_scalar(scratch: &mut EvalScratch, mo: &Morsel<'_>, blob: &[u8], dst: u16, a: u16, f: impl Fn(&[u8]) -> i64) {
    let (d, ai) = (dst as usize, a as usize);
    let (base_a, base_d) = (ai * MORSEL, d * MORSEL);
    {
        let EvalScratch {
            regs,
            str_views,
            str_arena,
            ..
        } = &mut *scratch;
        for i in 0..mo.m {
            regs[base_d + i] = f(view_bytes(str_views[base_a + i], str_arena, blob));
        }
    }
    null_copy1(scratch, d, ai, mo.m);
    maybe_pack_bool_bits(scratch, mo, d);
}

/// Parse every row of string register `a` into scalar register `dst`. `f`
/// returns `None` for an unparsable or out-of-range value, which becomes a NULL
/// — the [`unary_null_like`] shape over a view instead of a register.
fn str_parse_to_scalar(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    blob: &[u8],
    dst: u16,
    a: u16,
    f: impl Fn(&[u8]) -> Option<i64>,
) {
    let (d, ai) = (dst as usize, a as usize);
    let (base_a, base_d) = (ai * MORSEL, d * MORSEL);
    let mut bad = [0u8; MORSEL];
    {
        let EvalScratch {
            regs,
            str_views,
            str_arena,
            ..
        } = &mut *scratch;
        for i in 0..mo.m {
            match f(view_bytes(str_views[base_a + i], str_arena, blob)) {
                Some(v) => regs[base_d + i] = v,
                None => {
                    regs[base_d + i] = 0;
                    bad[i] = 1;
                }
            }
        }
    }
    null_copy1(scratch, d, ai, mo.m);
    merge_fail_mask(scratch, d, &bad, mo.m);
    maybe_pack_bool_bits(scratch, mo, d);
}

/// Render every row of scalar register `a` into string register `dst` via `f`,
/// then propagate the operand's null bit. The three numeric→text opcodes differ
/// only in `f`.
fn num_to_str(scratch: &mut EvalScratch, dst: u16, a: u16, m: usize, f: impl Fn(&mut Vec<u8>, i64) -> StrView) {
    let (d, ai) = (dst as usize, a as usize);
    let (base_a, base_d) = (ai * MORSEL, d * MORSEL);
    {
        let EvalScratch {
            regs,
            str_views,
            str_arena,
            ..
        } = &mut *scratch;
        for i in 0..m {
            str_views[base_d + i] = f(str_arena, regs[base_a + i]);
        }
    }
    null_copy1(scratch, d, ai, m);
}

/// The register-channel string compare — the [`eval_str_cmp`] shape, over views
/// instead of column cells.
fn eval_str_reg_cmp(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    blob: &[u8],
    dst: usize,
    a: usize,
    b: usize,
    pred: impl Fn(Ordering) -> bool,
) {
    let (base_a, base_b, base_d) = (a * MORSEL, b * MORSEL, dst * MORSEL);
    {
        let EvalScratch {
            regs,
            str_views,
            str_arena,
            ..
        } = &mut *scratch;
        for i in 0..mo.m {
            let ord = view_bytes(str_views[base_a + i], str_arena, blob).cmp(view_bytes(
                str_views[base_b + i],
                str_arena,
                blob,
            ));
            regs[base_d + i] = pred(ord) as i64;
        }
    }
    null_or2(scratch, dst, a, b, mo.m);
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// `Select`'s value blend over string lanes instead of scalar registers.
fn eval_str_select(scratch: &mut EvalScratch, dst: usize, ci: usize, ai: usize, bi: usize, m: usize) {
    let (base_a, base_b, base_d) = (ai * MORSEL, bi * MORSEL, dst * MORSEL);
    if scratch.no_nulls {
        let base_c = ci * MORSEL;
        let EvalScratch { regs, str_views, .. } = &mut *scratch;
        for i in 0..m {
            str_views[base_d + i] = if regs[base_c + i] != 0 {
                str_views[base_a + i]
            } else {
                str_views[base_b + i]
            };
        }
        return;
    }
    let words = m.div_ceil(64);
    let take_a = select_take_mask(scratch, dst, ci, ai, bi, words);
    let str_views = &mut scratch.str_views;
    for (w, &ta) in take_a.iter().enumerate().take(words) {
        let lo = w * 64;
        let hi = (lo + 64).min(m);
        for i in lo..hi {
            str_views[base_d + i] = if (ta >> (i - lo)) & 1 != 0 {
                str_views[base_a + i]
            } else {
                str_views[base_b + i]
            };
        }
    }
}

/// Which registers `StrSubstr` addresses, and how each scalar bound is read.
/// Grouped so the kernel takes one operand record rather than six positional
/// arguments.
struct SubstrOperands {
    dst: usize,
    src: usize,
    start_reg: usize,
    len_reg: Option<usize>,
    start_signed: bool,
    len_signed: bool,
}

/// SUBSTRING: a sub-view of the source, the bytes never copied.
fn eval_str_substr(scratch: &mut EvalScratch, blob: &[u8], op: SubstrOperands, m: usize) {
    let SubstrOperands {
        dst: d,
        src: si,
        start_reg: sr,
        len_reg,
        start_signed,
        len_signed,
    } = op;
    let (base_s, base_start, base_d) = (si * MORSEL, sr * MORSEL, d * MORSEL);
    let base_len = len_reg.map(|l| l * MORSEL);
    let mut bad = [0u8; MORSEL];
    {
        let EvalScratch {
            regs,
            str_views,
            str_arena,
            ..
        } = &mut *scratch;
        for i in 0..m {
            let v = str_views[base_s + i];
            let (s, base_off) = view_bytes_at(v, str_arena, blob);
            // The clamp order is what makes this total. Widen to i128 per the
            // register's signed flag and never compute in i64/u64; clamp each
            // endpoint to `[1, n1]` before narrowing to a byte scan. `n1` is one
            // past the ceiling because the window is half-open — clamping to the
            // ceiling itself would make `SUBSTRING('abc' FROM -1)` yield `'ab'`.
            //
            // The ceiling is the *byte* length, which merely bounds the character
            // count; counting characters first would cost a full pass over every
            // row. A window landing in the gap between the two resolves to the
            // string's end in `char_offset` below, which is the same answer.
            let start = widen_reg(regs[base_start + i], start_signed);
            let n1 = s.len() as i128 + 1;
            let (lo, hi) = match base_len {
                Some(bl) => {
                    let len = widen_reg(regs[bl + i], len_signed);
                    bad[i] = (len < 0) as u8;
                    (start, start + len)
                }
                None => (start, n1),
            };
            let (lo, hi) = (lo.clamp(1, n1), hi.clamp(1, n1));
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
    // `is_strictly_non_nullable`, which classifies it as introducing no NULL of
    // its own.
    match len_reg {
        Some(l) => {
            null_or3(scratch, d, si, sr, l, m);
            merge_fail_mask(scratch, d, &bad, m);
        }
        None => null_or2(scratch, d, si, sr, m),
    }
}

/// `||` and the CONCAT fold step. `skip_null` is CONCAT's asymmetric rule: a
/// NULL `b` contributes the empty string, a NULL `a` propagates.
fn eval_str_concat(scratch: &mut EvalScratch, blob: &[u8], d: usize, ai: usize, bi: usize, skip_null: bool, m: usize) {
    let (base_a, base_b, base_d) = (ai * MORSEL, bi * MORSEL, d * MORSEL);
    let mut bad = [0u8; MORSEL];
    {
        let EvalScratch {
            str_views,
            str_arena,
            null_bits,
            no_nulls,
            ..
        } = &mut *scratch;
        let b_null_base = bi * NULL_WORDS_PER_REG;
        for i in 0..m {
            let va = str_views[base_a + i];
            // Under CONCAT's rule a NULL argument contributes the empty string,
            // whatever view its lane happens to carry.
            let b_is_null = skip_null && !*no_nulls && (null_bits[b_null_base + i / 64] >> (i % 64)) & 1 != 0;
            let vb = if b_is_null {
                StrView::default()
            } else {
                str_views[base_b + i]
            };
            // Each operand's extent is resolved once and reused for both the
            // length sum and the copy. Widened before summing: two near-u32::MAX
            // operands wrap in u32 space, and an oversized length would trip
            // `encode_german_string`'s release assert — a worker abort, which the
            // totality rule forbids.
            let (oa, la) = view_span(
                va,
                if va.src == SRC_ARENA {
                    str_arena.len()
                } else {
                    blob.len()
                },
            );
            let (ob, lb) = view_span(
                vb,
                if vb.src == SRC_ARENA {
                    str_arena.len()
                } else {
                    blob.len()
                },
            );
            let total = la as u64 + lb as u64;
            if total > u32::MAX as u64 {
                bad[i] = 1;
                str_views[base_d + i] = StrView::default();
                continue;
            }
            let o = str_arena.len();
            arena_push_span(str_arena, blob, va.src, oa, la);
            arena_push_span(str_arena, blob, vb.src, ob, lb);
            str_views[base_d + i] = StrView {
                off: o as u64,
                len: total as u32,
                src: SRC_ARENA,
            };
        }
    }
    if skip_null {
        null_copy1(scratch, d, ai, m);
    } else {
        null_or2(scratch, d, ai, bi, m);
    }
    merge_fail_mask(scratch, d, &bad, m);
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
    fn column<B: BatchView>(mb: &'a B, pi_byte: u8) -> Self {
        let pi = pi_byte as usize;
        StrOperand {
            cells: mb.col_data(pi, 16),
            blob: mb.blob(),
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
fn eval_str_cmp(
    scratch: &mut EvalScratch,
    mo: &Morsel<'_>,
    dst: usize,
    a: StrOperand<'_>,
    b: StrOperand<'_>,
    pred: impl Fn(Ordering) -> bool,
) {
    fill_null_bits_mask(scratch, dst, mo, a.null_bit | b.null_bit);
    let base_d = dst * MORSEL;
    for i in 0..mo.m {
        let row = mo.start + i;
        let ord = compare_german_strings(a.cell(row), a.blob, b.cell(row), b.blob);
        scratch.regs[base_d + i] = pred(ord) as i64;
    }
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// Reinterpret a register's raw i64 bits as the `f64` they encode, and back.
fn decode_f64(bits: i64) -> f64 {
    f64::from_bits(bits as u64)
}
fn encode_f64(f: f64) -> i64 {
    f64::to_bits(f) as i64
}

// ---------------------------------------------------------------------------
// eval_batch — single morsel
// ---------------------------------------------------------------------------

/// Evaluate `prog` over one morsel of `mb` (`morsel_start..morsel_start+m`).
/// Results land in `scratch.regs`; null bits in `scratch.null_bits`.
///
/// Callers loop over morsels and call this function once per morsel.
#[allow(clippy::needless_range_loop)]
pub(crate) fn eval_batch<B: BatchView>(
    prog: &ResolvedProgram,
    mb: &B,
    morsel_start: usize,
    m: usize,
    scratch: &mut EvalScratch,
) {
    // The scratch caches `no_nulls` so the helpers that only take `&mut
    // EvalScratch` can branch on it. Sizing it for one arm and evaluating on the
    // other reads null buffers that were never allocated, so the two copies are
    // checked rather than assumed to agree.
    debug_assert_eq!(
        scratch.no_nulls, prog.no_nulls,
        "scratch was sized for a different program's nullability arm",
    );
    // Declared before the macros below so their bodies can name it: a macro can
    // only capture bindings that already exist where it is defined.
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
    // Every binary arithmetic/comparison opcode shares one shape: read two source
    // registers, write one, OR the source null words, repack bool bits. `bin_op!`
    // collapses them to one line each — the per-row `|x, y| body` is spliced inline
    // (not a closure), so each arm vectorizes exactly as the hand-written loop did.
    // Bodies must stay branch-free; float ops reinterpret register bits via
    // `decode_f64`/`encode_f64`. `div_like!` stays separate: it additionally merges
    // a zero-divisor mask into the destination null word.
    macro_rules! bin_op {
        ($a:expr, $b:expr, $d:expr, |$x:ident, $y:ident| $body:expr) => {{
            let ai = $a as usize;
            let bi = $b as usize;
            {
                let ([ra, rb], rd) = scratch.regs_split([ai, bi], $d, m);
                for i in 0..m {
                    let $x = ra[i];
                    let $y = rb[i];
                    rd[i] = $body;
                }
            }
            null_or2(scratch, $d, ai, bi, m);
            maybe_pack_bool_bits(scratch, &mo, $d);
        }};
    }
    // Unary counterpart of `bin_op!`: read one source register, write one, copy
    // the source null word, repack bool bits.
    macro_rules! un_op {
        ($a:expr, $d:expr, |$x:ident| $body:expr) => {{
            let ai = $a as usize;
            {
                let ([ra], rd) = scratch.regs_split([ai], $d, m);
                for i in 0..m {
                    let $x = ra[i];
                    rd[i] = $body;
                }
            }
            null_copy1(scratch, $d, ai, m);
            maybe_pack_bool_bits(scratch, &mo, $d);
        }};
    }
    // DIV-shaped op: compute per-row, mark divide-by-zero rows null, substitute
    // a safe divisor so the computed slot holds a defined (irrelevant) value.
    // `body(a, b)` must return `(result: i64, is_zero: bool)`. Flags go a byte
    // per row to `merge_fail_mask`, which states why.
    macro_rules! div_like {
        ($a:expr, $b:expr, $d:expr, |$a_i:ident, $b_i:ident| $body:expr) => {{
            let ai = $a as usize;
            let bi = $b as usize;
            let mut bad = [0u8; MORSEL];
            {
                let ([ra, rb], rd) = scratch.regs_split([ai, bi], $d, m);
                for i in 0..m {
                    let $a_i = ra[i];
                    let $b_i = rb[i];
                    let (val, is_zero): (i64, bool) = $body;
                    rd[i] = val;
                    bad[i] = is_zero as u8;
                }
            }
            null_or2(scratch, $d, ai, bi, m);
            merge_fail_mask(scratch, $d, &bad, m);
            maybe_pack_bool_bits(scratch, &mo, $d);
        }};
    }
    // IntDiv / IntMod: one `div_like!` body each, differing only in the i64 op
    // (`wrapping_div` / `wrapping_rem`) and the u64 reinterpret. `signed` branches
    // OUTSIDE the row loop — each arm expands to its own whole loop, exactly as
    // the four hand-written bodies did. A zero divisor is substituted with 1 and
    // the row marked NULL, so `wrapping_*` never divides by zero and `i64::MIN /
    // -1` wraps rather than trapping.
    macro_rules! divmod {
        ($a:expr, $b:expr, $d:expr, $signed:expr, $op:ident) => {{
            if $signed {
                div_like!($a, $b, $d, |x, y| {
                    let is_zero = y == 0;
                    let dd = if is_zero { 1 } else { y };
                    (x.$op(dd), is_zero)
                })
            } else {
                div_like!($a, $b, $d, |x, y| {
                    let is_zero = y == 0;
                    let dd = if is_zero { 1u64 } else { y as u64 };
                    (((x as u64).$op(dd)) as i64, is_zero)
                })
            }
        }};
    }

    // Unary counterpart of `div_like!`: compute every row unconditionally, then
    // mark the failures NULL. `body` returns `(value, failed)`; the value is
    // always defined, so a failed row never leaves uninitialised bits behind.
    // The failure flags go to `merge_fail_mask`, which states why they are
    // packed in a second pass.
    macro_rules! unary_null_like {
        ($a:expr, $d:expr, |$x:ident| $body:expr) => {{
            let ai = $a as usize;
            let mut bad = [0u8; MORSEL];
            {
                let ([ra], rd) = scratch.regs_split([ai], $d, m);
                for i in 0..m {
                    let $x = ra[i];
                    let (val, failed): (i64, bool) = $body;
                    rd[i] = val;
                    bad[i] = failed as u8;
                }
            }
            null_copy1(scratch, $d, ai, m);
            merge_fail_mask(scratch, $d, &bad, m);
            maybe_pack_bool_bits(scratch, &mo, $d);
        }};
    }

    // Null-skipping 2-ary extremum. `null_or2`'s `a|b` rule is exactly wrong here
    // (the result is null only when BOTH operands are), so this cannot use
    // `bin_op!`. `pick` returns true when `a` wins on value; the null-skip is
    // resolved from the two null words before it is consulted.
    macro_rules! minmax2 {
        ($a:expr, $b:expr, $d:expr, |$x:ident, $y:ident| $pick:expr) => {{
            let ai = $a as usize;
            let bi = $b as usize;
            let words = m.div_ceil(64);
            if scratch.no_nulls {
                let ([ra, rb], rd) = scratch.regs_split([ai, bi], $d, m);
                for i in 0..m {
                    let $x = ra[i];
                    let $y = rb[i];
                    rd[i] = if $pick { $x } else { $y };
                }
            } else {
                let base_a = ai * NULL_WORDS_PER_REG;
                let base_b = bi * NULL_WORDS_PER_REG;
                let mut na = [0u64; NULL_WORDS_PER_REG];
                let mut nb = [0u64; NULL_WORDS_PER_REG];
                na[..words].copy_from_slice(&scratch.null_bits[base_a..base_a + words]);
                nb[..words].copy_from_slice(&scratch.null_bits[base_b..base_b + words]);
                {
                    let ([ra, rb], rd) = scratch.regs_split([ai, bi], $d, m);
                    for i in 0..m {
                        let $x = ra[i];
                        let $y = rb[i];
                        let a_null = (na[i / 64] >> (i % 64)) & 1 != 0;
                        let b_null = (nb[i / 64] >> (i % 64)) & 1 != 0;
                        rd[i] = if a_null {
                            $y
                        } else if b_null {
                            $x
                        } else if $pick {
                            $x
                        } else {
                            $y
                        };
                    }
                }
                let base_d = $d * NULL_WORDS_PER_REG;
                for w in 0..words {
                    scratch.null_bits[base_d + w] = na[w] & nb[w];
                }
            }
            maybe_pack_bool_bits(scratch, &mo, $d);
        }};
    }

    // Dispatch a German-string compare on its operator, hoisting the three-way
    // branch out of the row loop: each arm instantiates `eval_str_cmp` with its
    // own `Ordering` predicate.
    macro_rules! str_cmp {
        ($op:expr, $d:expr, $a:expr, $b:expr) => {{
            let (d, a, b) = ($d, $a, $b);
            match $op {
                StrOp::Eq => eval_str_cmp(scratch, &mo, d, a, b, |o| o == Ordering::Equal),
                StrOp::Lt => eval_str_cmp(scratch, &mo, d, a, b, |o| o == Ordering::Less),
                StrOp::Le => eval_str_cmp(scratch, &mo, d, a, b, |o| o != Ordering::Greater),
            }
        }};
    }

    for instr in &prog.instrs {
        match *instr {
            // ----------------------------------------------------------------
            // Output instructions — materialized at batch level, not here
            // ----------------------------------------------------------------
            Instr::CopyCol { .. } | Instr::Emit { .. } => {}

            // ----------------------------------------------------------------
            // Load operations
            // ----------------------------------------------------------------
            Instr::LoadPayloadInt { dst, pi, fi } => {
                let dst = dst as usize;
                let pi = pi as usize;
                // The width is a `const fn` of `fi`; derive it once per
                // instruction, outside the row loop (as `LoadPk` does).
                let col_data = mb.col_data(pi, fi.width());
                let dst_reg = scratch.reg_mut(dst, m);
                // Widen `m` rows of a `SZ`-byte little-endian column into i64 registers.
                // `SZ` is a compile-time constant per instantiation, so each expansion is
                // monomorphic and vectorizes like the hand-written loop did.
                macro_rules! load_int {
                    ($ty:ty) => {{
                        const SZ: usize = std::mem::size_of::<$ty>();
                        let b = &col_data[morsel_start * SZ..(morsel_start + m) * SZ];
                        for (i, c) in b.chunks_exact(SZ).enumerate() {
                            dst_reg[i] = <$ty>::from_le_bytes(c.try_into().unwrap()) as i64;
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
                        for (i, c) in b.chunks_exact(8).enumerate() {
                            dst_reg[i] = i64::from_le_bytes(c.try_into().unwrap());
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
                let dst = dst as usize;
                let pi = pi as usize;
                let col_data = mb.col_data(pi, 4);
                let dst_reg = scratch.reg_mut(dst, m);
                let b = &col_data[morsel_start * 4..(morsel_start + m) * 4];
                for (i, c) in b.chunks_exact(4).enumerate() {
                    let bits = u32::from_le_bytes(c.try_into().unwrap());
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
                let dst = dst as usize;
                let (pk, stride) = mb.pk_region();
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
                scratch.clear_null_reg(dst, m);
                maybe_pack_bool_bits(scratch, &mo, dst);
            }

            Instr::LoadConst { dst, val } => {
                let dst = dst as usize;
                scratch.reg_mut(dst, m).fill(val);
                scratch.clear_null_reg(dst, m);
                maybe_pack_bool_bits(scratch, &mo, dst);
            }

            // ----------------------------------------------------------------
            // Integer arithmetic
            // ----------------------------------------------------------------
            Instr::IntAdd { dst, a, b } => bin_op!(a, b, dst as usize, |x, y| x.wrapping_add(y)),
            Instr::IntSub { dst, a, b } => bin_op!(a, b, dst as usize, |x, y| x.wrapping_sub(y)),
            Instr::IntMul { dst, a, b } => bin_op!(a, b, dst as usize, |x, y| x.wrapping_mul(y)),
            // `signed: false` reinterprets operands as u64 so the quotient is
            // correct for dividends >= 2^63. Zero divisor marks NULL.
            Instr::IntDiv { dst, a, b, signed } => divmod!(a, b, dst as usize, signed, wrapping_div),
            Instr::IntMod { dst, a, b, signed } => divmod!(a, b, dst as usize, signed, wrapping_rem),
            // The operator match is OUTSIDE the row loop, so each arm expands to
            // its own monomorphic, branch-free loop (the `Instr::Cmp` pattern).
            Instr::IntUnary { op, dst, a } => {
                let d = dst as usize;
                match op {
                    IntUnaryOp::Neg => un_op!(a, d, |x| x.wrapping_neg()),
                    IntUnaryOp::Abs => un_op!(a, d, |x| x.wrapping_abs()),
                }
            }
            Instr::FloatUnary { op, dst, a } => {
                let d = dst as usize;
                match op {
                    FloatUnaryOp::Neg => un_op!(a, d, |x| encode_f64(-decode_f64(x))),
                    FloatUnaryOp::Abs => un_op!(a, d, |x| encode_f64(decode_f64(x).abs())),
                    FloatUnaryOp::Floor => un_op!(a, d, |x| encode_f64(decode_f64(x).floor())),
                    FloatUnaryOp::Ceil => un_op!(a, d, |x| encode_f64(decode_f64(x).ceil())),
                    FloatUnaryOp::Round => un_op!(a, d, |x| encode_f64(decode_f64(x).round_ties_even())),
                    FloatUnaryOp::Trunc => un_op!(a, d, |x| encode_f64(decode_f64(x).trunc())),
                }
            }
            // A finite source whose rounded result is not finite overflowed f32's
            // range. Testing the ROUNDED value (not `|x| > f32::MAX`) keeps the
            // 2^28-1 doubles just above f32::MAX that round down to it.
            Instr::FloatToF32 { dst, a } => unary_null_like!(a, dst as usize, |x| {
                let f = decode_f64(x);
                let v32 = f as f32;
                (encode_f64(v32 as f64), f.is_finite() && v32.is_infinite())
            }),
            Instr::IntCast { dst, a, fi, src_signed } => {
                let d = dst as usize;
                let (lo, hi, hi_u) = int_cast_bounds(fi);
                if src_signed {
                    unary_null_like!(a, d, |x| (x, x < lo || x > hi))
                } else {
                    unary_null_like!(a, d, |x| (x, (x as u64) > hi_u))
                }
            }
            // Truncate toward zero, then range-check in f64: NaN fails every
            // comparison and so fails the check, as ±inf and out-of-range do.
            Instr::FloatToInt { dst, a, fi } => {
                let d = dst as usize;
                let (flo, fhi) = float_to_int_bounds(fi);
                if fi == FixedInt::U64 {
                    unary_null_like!(a, d, |x| {
                        let t = decode_f64(x).trunc();
                        let ok = t >= flo && t < fhi;
                        (if ok { t as u64 as i64 } else { 0 }, !ok)
                    })
                } else {
                    unary_null_like!(a, d, |x| {
                        let t = decode_f64(x).trunc();
                        let ok = t >= flo && t < fhi;
                        (if ok { t as i64 } else { 0 }, !ok)
                    })
                }
            }
            Instr::IntMinMax2 {
                dst,
                a,
                b,
                is_max,
                signed,
            } => {
                let d = dst as usize;
                match (is_max, signed) {
                    (true, true) => minmax2!(a, b, d, |x, y| x > y),
                    (false, true) => minmax2!(a, b, d, |x, y| x < y),
                    (true, false) => minmax2!(a, b, d, |x, y| (x as u64) > (y as u64)),
                    (false, false) => minmax2!(a, b, d, |x, y| (x as u64) < (y as u64)),
                }
            }
            // `total_cmp` and nothing else: -0.0 and +0.0 are `==`-equal but
            // total_cmp-distinct, so an `==`-based pick would let operand order
            // decide which bit pattern survives.
            Instr::FloatMinMax2 { dst, a, b, is_max } => {
                let d = dst as usize;
                if is_max {
                    minmax2!(a, b, d, |x, y| decode_f64(x).total_cmp(&decode_f64(y)).is_gt())
                } else {
                    minmax2!(a, b, d, |x, y| decode_f64(x).total_cmp(&decode_f64(y)).is_lt())
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
            Instr::IntInSet {
                dst,
                value_reg,
                set_idx,
            } => {
                let set = &prog.int_sets[set_idx as usize]; // decoded once, sorted ascending
                un_op!(value_reg, dst as usize, |x| set.binary_search(&x).is_ok() as i64)
            }

            // ----------------------------------------------------------------
            // Float arithmetic
            // ----------------------------------------------------------------
            Instr::FloatAdd { dst, a, b } => {
                bin_op!(a, b, dst as usize, |x, y| encode_f64(decode_f64(x) + decode_f64(y)))
            }
            Instr::FloatSub { dst, a, b } => {
                bin_op!(a, b, dst as usize, |x, y| encode_f64(decode_f64(x) - decode_f64(y)))
            }
            Instr::FloatMul { dst, a, b } => {
                bin_op!(a, b, dst as usize, |x, y| encode_f64(decode_f64(x) * decode_f64(y)))
            }
            Instr::FloatDiv { dst, a, b } => div_like!(a, b, dst as usize, |a, b| {
                let fa = decode_f64(a);
                let fb = decode_f64(b);
                let is_zero = fb == 0.0;
                let fb_safe = if is_zero { 1.0 } else { fb };
                (encode_f64(fa / fb_safe), is_zero)
            }),

            // ----------------------------------------------------------------
            // Integer comparisons
            // ----------------------------------------------------------------
            // `signed: false` compares the raw bits as u64 so values >= 2^63 order
            // correctly. Branch on op/signed outside the per-row loop.
            Instr::Cmp { op, dst, a, b, signed } => {
                let d = dst as usize;
                match op {
                    CmpOp::Eq => bin_op!(a, b, d, |x, y| (x == y) as i64),
                    CmpOp::Ne => bin_op!(a, b, d, |x, y| (x != y) as i64),
                    CmpOp::Gt if signed => bin_op!(a, b, d, |x, y| (x > y) as i64),
                    CmpOp::Gt => bin_op!(a, b, d, |x, y| ((x as u64) > (y as u64)) as i64),
                    CmpOp::Ge if signed => bin_op!(a, b, d, |x, y| (x >= y) as i64),
                    CmpOp::Ge => bin_op!(a, b, d, |x, y| ((x as u64) >= (y as u64)) as i64),
                    CmpOp::Lt if signed => bin_op!(a, b, d, |x, y| (x < y) as i64),
                    CmpOp::Lt => bin_op!(a, b, d, |x, y| ((x as u64) < (y as u64)) as i64),
                    CmpOp::Le if signed => bin_op!(a, b, d, |x, y| (x <= y) as i64),
                    CmpOp::Le => bin_op!(a, b, d, |x, y| ((x as u64) <= (y as u64)) as i64),
                }
            }

            // ----------------------------------------------------------------
            // Float comparisons
            // ----------------------------------------------------------------
            Instr::FCmp { op, dst, a, b } => {
                let d = dst as usize;
                match op {
                    CmpOp::Eq => bin_op!(a, b, d, |x, y| (decode_f64(x) == decode_f64(y)) as i64),
                    CmpOp::Ne => bin_op!(a, b, d, |x, y| (decode_f64(x) != decode_f64(y)) as i64),
                    CmpOp::Gt => bin_op!(a, b, d, |x, y| (decode_f64(x) > decode_f64(y)) as i64),
                    CmpOp::Ge => bin_op!(a, b, d, |x, y| (decode_f64(x) >= decode_f64(y)) as i64),
                    CmpOp::Lt => bin_op!(a, b, d, |x, y| (decode_f64(x) < decode_f64(y)) as i64),
                    CmpOp::Le => bin_op!(a, b, d, |x, y| (decode_f64(x) <= decode_f64(y)) as i64),
                }
            }

            // ----------------------------------------------------------------
            // Boolean 3VL
            // ----------------------------------------------------------------
            Instr::BoolAnd { dst, a, b } => {
                let dst = dst as usize;
                let ai = a as usize;
                let bi = b as usize;
                if scratch.no_nulls {
                    bin_op!(a, b, dst, |x, y| ((x != 0) && (y != 0)) as i64);
                } else {
                    // Nullable arm: word-level u64 3VL on packed truthy bits.
                    // Upstream producers populate `bool_bits` via
                    // `needs_bool_pack`; the unpack to `regs[dst]` is skipped
                    // for bit_only destinations whose only readers are BOOL.
                    bool_and_or_word_loop(scratch, dst, ai, bi, m, /* is_or = */ false);
                    maybe_unpack_bool_to_regs(scratch, &mo, dst);
                }
            }
            Instr::BoolOr { dst, a, b } => {
                let dst = dst as usize;
                let ai = a as usize;
                let bi = b as usize;
                if scratch.no_nulls {
                    bin_op!(a, b, dst, |x, y| ((x != 0) || (y != 0)) as i64);
                } else {
                    bool_and_or_word_loop(scratch, dst, ai, bi, m, /* is_or = */ true);
                    maybe_unpack_bool_to_regs(scratch, &mo, dst);
                }
            }
            Instr::BoolNot { dst, a } => {
                let dst = dst as usize;
                let ai = a as usize;
                if scratch.no_nulls {
                    un_op!(a, dst, |x| (x == 0) as i64);
                } else {
                    let words = m.div_ceil(64);
                    let base_a = ai * NULL_WORDS_PER_REG;
                    let base_d = dst * NULL_WORDS_PER_REG;
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
            Instr::IsNull { dst, pi, invert } => eval_is_null(scratch, &mo, dst as usize, pi as usize, invert),

            // ----------------------------------------------------------------
            // Type cast. `signed: false` reinterprets the register as u64 first
            // so values >= 2^63 cast to the correct large positive float.
            // ----------------------------------------------------------------
            Instr::IntToFloat { dst, a, signed } => {
                let d = dst as usize;
                match signed {
                    true => un_op!(a, d, |x| encode_f64(x as f64)),
                    false => un_op!(a, d, |x| encode_f64(x as u64 as f64)),
                }
            }

            // ----------------------------------------------------------------
            // Conditional select (SQL CASE blend) / manufactured NULL
            // ----------------------------------------------------------------
            // Rows where `cond` is non-NULL and truthy take `a`'s value + null
            // bit; all others (false OR NULL cond) take `b`'s.
            Instr::Select { dst, cond, a, b } => {
                let dst = dst as usize;
                let ci = cond as usize;
                let ai = a as usize;
                let bi = b as usize;
                if scratch.no_nulls {
                    // Fast arm: cond truthiness lives in `regs` (no bool_bits in
                    // no_nulls mode); a straight per-row blend.
                    let ([rc, ra, rb], rd) = scratch.regs_split([ci, ai, bi], dst, m);
                    for i in 0..m {
                        rd[i] = if rc[i] != 0 { ra[i] } else { rb[i] };
                    }
                } else {
                    let words = m.div_ceil(64);
                    let take_a = select_take_mask(scratch, dst, ci, ai, bi, words);
                    // Value blend, row-level within each word.
                    {
                        let ([ra, rb], rd) = scratch.regs_split([ai, bi], dst, m);
                        for w in 0..words {
                            let lo = w * 64;
                            let hi = (lo + 64).min(m);
                            let ta = take_a[w];
                            for i in lo..hi {
                                rd[i] = if (ta >> (i - lo)) & 1 != 0 { ra[i] } else { rb[i] };
                            }
                        }
                    }
                    maybe_pack_bool_bits(scratch, &mo, dst);
                }
            }
            // Manufacture a NULL: zero the value lane, set the null bit for every
            // live row. Only ever reached on the nullable arm (LoadNull forces
            // `no_nulls` off via `is_strictly_non_nullable`).
            Instr::LoadNull { dst } => {
                let dst = dst as usize;
                let base_d = dst * MORSEL;
                scratch.regs[base_d..base_d + m].fill(0);
                set_null_reg(scratch, dst, m);
                maybe_pack_bool_bits(scratch, &mo, dst);
            }

            // ----------------------------------------------------------------
            // String comparisons (column vs constant / column vs column)
            // ----------------------------------------------------------------
            // Both arms are the same compare over two operands; only where
            // operand B comes from differs. `str_cmp!` keeps the three-way
            // operator branch outside the row loop, so each `StrOp` still gets
            // its own monomorphic, branch-free kernel.
            Instr::StrColConst { op, dst, pi, cell_idx } => str_cmp!(
                op,
                dst as usize,
                StrOperand::column(mb, pi),
                StrOperand::constant(prog, cell_idx as usize)
            ),
            Instr::StrColCol { op, dst, pi_a, pi_b } => str_cmp!(
                op,
                dst as usize,
                StrOperand::column(mb, pi_a),
                StrOperand::column(mb, pi_b)
            ),

            // ----------------------------------------------------------------
            // String registers
            // ----------------------------------------------------------------
            // Every arm writes all `m` lanes, following the `unary_null_like!`
            // discipline: a failed row gets a defined-but-irrelevant view plus a
            // null bit, never an untouched lane. Skipping NULL rows would leave
            // a previous morsel's view behind, and it would resolve against the
            // refilled arena.
            Instr::LoadColStr { dst, pi } => {
                let dst = dst as usize;
                let pi = pi as usize;
                let cells = mb.col_data(pi, 16);
                let blob_len = mb.blob().len();
                let base_d = dst * MORSEL;
                {
                    let EvalScratch {
                        str_views, str_arena, ..
                    } = &mut *scratch;
                    for i in 0..m {
                        let o = (morsel_start + i) * 16;
                        str_views[base_d + i] = cell_to_view(&cells[o..o + 16], blob_len, str_arena);
                    }
                }
                fill_null_bits_mask(scratch, dst, &mo, 1u64 << pi);
            }

            Instr::LoadConstStr { dst, off, len } => {
                let dst = dst as usize;
                let base_d = dst * MORSEL;
                let v = StrView {
                    off: off as u64,
                    len,
                    src: SRC_ARENA,
                };
                scratch.str_views[base_d..base_d + m].fill(v);
                scratch.clear_null_reg(dst, m);
            }

            // The `LoadNull` shape: a defined empty lane plus the null bit for
            // every live row, with the tail word masked so stale high bits never
            // read as null.
            Instr::LoadNullStr { dst } => {
                let dst = dst as usize;
                let base_d = dst * MORSEL;
                scratch.str_views[base_d..base_d + m].fill(StrView::default());
                set_null_reg(scratch, dst, m);
            }

            Instr::StrSelect { dst, cond, a, b } => {
                eval_str_select(scratch, dst as usize, cond as usize, a as usize, b as usize, m)
            }

            // The operator branch stays outside the row loop, as `str_cmp!` does
            // for the `EXPR_STR_COL_*` family.
            Instr::StrCmp { op, dst, a, b } => {
                let (d, ai, bi) = (dst as usize, a as usize, b as usize);
                let blob = mb.blob();
                match op {
                    StrOp::Eq => eval_str_reg_cmp(scratch, &mo, blob, d, ai, bi, |o| o == Ordering::Equal),
                    StrOp::Lt => eval_str_reg_cmp(scratch, &mo, blob, d, ai, bi, |o| o == Ordering::Less),
                    StrOp::Le => eval_str_reg_cmp(scratch, &mo, blob, d, ai, bi, |o| o != Ordering::Greater),
                }
            }

            // Unswitched on `chars`, so each measure is its own monomorphised loop.
            Instr::StrLen { dst, a, chars } => {
                if chars {
                    str_to_scalar(scratch, &mo, mb.blob(), dst, a, |s| char_count(s) as i64);
                } else {
                    str_to_scalar(scratch, &mo, mb.blob(), dst, a, |s| s.len() as i64);
                }
            }

            // A fresh copy in the arena, folded in place.
            Instr::StrCase { dst, a, upper } => {
                str_to_str(scratch, mb.blob(), dst, a, m, |arena, blob, v| {
                    let (o, l) = arena_push_view(arena, blob, v);
                    for byte in &mut arena[o..o + l] {
                        let hit = if upper {
                            byte.is_ascii_lowercase()
                        } else {
                            byte.is_ascii_uppercase()
                        };
                        // `b ^ 0x20` on a hit and `b ^ 0` otherwise — the fold
                        // with no branch in the byte loop.
                        *byte ^= (hit as u8) << 5;
                    }
                    StrView::arena(o, l)
                });
            }

            Instr::StrSubstr {
                dst,
                src,
                start_reg,
                len_reg,
                start_signed,
                len_signed,
            } => eval_str_substr(
                scratch,
                mb.blob(),
                SubstrOperands {
                    dst: dst as usize,
                    src: src as usize,
                    start_reg: start_reg as usize,
                    len_reg: len_reg.map(|l| l as usize),
                    start_signed,
                    len_signed,
                },
                m,
            ),

            // A sub-view of the source: the bytes are not copied, only the
            // offset and length narrowed.
            Instr::StrTrim { dst, a, mode, set_idx } => {
                let set = &prog.trim_sets[set_idx as usize];
                str_to_str(scratch, mb.blob(), dst, a, m, |arena, blob, v| {
                    let (s, base_off) = view_bytes_at(v, arena, blob);
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
                str_to_scalar(scratch, &mo, mb.blob(), dst, src, |s| matcher.matches(s) as i64);
            }

            Instr::StrConcat { dst, a, b, skip_null } => {
                eval_str_concat(scratch, mb.blob(), dst as usize, a as usize, b as usize, skip_null, m)
            }

            // The three numeric→text arms share one loop, monomorphised per
            // closure so the signed/float branch stays outside it.
            Instr::IntToStr { dst, a, signed } => {
                if signed {
                    num_to_str(scratch, dst, a, m, |arena, v| {
                        arena_push_text(arena, format_args!("{v}"))
                    });
                } else {
                    num_to_str(scratch, dst, a, m, |arena, v| {
                        arena_push_text(arena, format_args!("{}", v as u64))
                    });
                }
            }

            Instr::FloatToStr { dst, a } => {
                num_to_str(scratch, dst, a, m, |arena, v| arena_push_float(arena, decode_f64(v)));
            }

            Instr::StrToInt { dst, a, fi } => {
                let (lo, hi) = fi.range();
                // A U64 value above i64::MAX narrows to its own bit pattern,
                // which is what the register holds; the resolve-time U64
                // tracking makes downstream reads agree.
                str_parse_to_scalar(scratch, &mo, mb.blob(), dst, a, |s| {
                    parse_decimal_i128(s).filter(|v| *v >= lo && *v <= hi).map(|v| v as i64)
                });
            }

            Instr::StrToFloat { dst, a } => {
                str_parse_to_scalar(scratch, &mo, mb.blob(), dst, a, |s| {
                    std::str::from_utf8(s.trim_ascii())
                        .ok()
                        .and_then(|t| t.parse::<f64>().ok())
                        .map(encode_f64)
                });
            }
        }
    }
}

#[cfg(test)]
mod tests;
