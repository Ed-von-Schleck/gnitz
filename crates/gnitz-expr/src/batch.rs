//! Morsel-oriented batch expression evaluator.
//!
//! `eval_batch` processes one morsel at a time (up to MORSEL rows), applying
//! all expression opcodes as columnar loops over register buffers.  Base
//! pointers are hoisted outside the inner loop, letting LLVM auto-vectorize
//! every arithmetic opcode.

use std::cmp::Ordering;

use crate::program::{FloatUnaryOp, IntUnaryOp};
use crate::{BatchView, CmpOp, Instr, ResolvedProgram, StrOp, PAYLOAD_MAPPING_PK_SENTINEL};
use gnitz_wire::{compare_german_strings, null_word_get, read_u64_le, FixedInt};

/// Integer-cast bounds for the target type: the signed-source window `[lo, hi]`
/// and the unsigned-source ceiling `hi_u`. Only U64 needs the two to differ —
/// its `hi` is clamped to `i64::MAX` because the signed test runs in i64.
#[inline]
fn int_cast_bounds(fi: FixedInt) -> (i64, i64, u64) {
    let (lo, hi) = fi.range();
    (lo as i64, hi.min(i64::MAX as i128) as i64, hi as u64)
}

/// Float→int bounds as an f64 half-open window `[lo, hi)`. The upper bound is
/// exclusive at every width: after `trunc` the value is integral, so `t < 2^k`
/// is exactly `t <= 2^k - 1` — and it avoids naming `2^63-1`/`2^64-1`, neither
/// of which is representable in f64. Both bounds are then powers of two, so
/// the f64 conversion is exact.
#[inline]
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
    /// Ensure the scratch buffer can hold `prog`'s registers and `(n+63)/64`
    /// filter words (`n = 0` for a driver that reads no filter bitmap). Does not
    /// shrink.
    ///
    /// Takes the program rather than its register count and nullability arm:
    /// the cached `no_nulls` decides which buffers exist *and* which arm every
    /// kernel takes, so reading it from anywhere but the program that is about
    /// to run would size the scratch for one arm and evaluate on the other.
    ///
    /// Split so the steady-state path — every call after the first, and the one
    /// a map driver makes per *range* (which on an alternating predicate is per
    /// row) — is four length compares with no call. `null_cap = 0` under
    /// `no_nulls` makes its two compares statically false, which is exactly the
    /// `!no_nulls` guard the un-split form spelled out: the null buffers stay at
    /// capacity 0, and the kernels' `no_nulls` arms never touch them.
    #[inline(always)]
    pub(crate) fn ensure_capacity(&mut self, prog: &ResolvedProgram, n: usize) {
        let num_regs = prog.num_regs as usize;
        self.no_nulls = prog.no_nulls;
        let reg_cap = num_regs * MORSEL;
        let null_cap = if prog.no_nulls {
            0
        } else {
            num_regs * NULL_WORDS_PER_REG
        };
        let filter_words = n.div_ceil(64);
        if self.regs.len() < reg_cap
            || self.null_bits.len() < null_cap
            || self.bool_bits.len() < null_cap
            || self.filter_bits.len() < filter_words
        {
            self.grow(reg_cap, null_cap, filter_words);
        }
    }

    /// The growth half of [`Self::ensure_capacity`]. `#[cold] #[inline(never)]`
    /// is a code-size measure: both are no-ops at `-O0` and release inlines
    /// regardless, but they keep the four `Vec::resize` sequences out of every
    /// always-inlined call site.
    #[cold]
    #[inline(never)]
    fn grow(&mut self, reg_cap: usize, null_cap: usize, filter_words: usize) {
        if self.regs.len() < reg_cap {
            self.regs.resize(reg_cap, 0);
        }
        if self.null_bits.len() < null_cap {
            self.null_bits.resize(null_cap, 0);
        }
        if self.bool_bits.len() < null_cap {
            self.bool_bits.resize(null_cap, 0);
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

    /// Zero the null bits for one register's morsel region.
    fn clear_null_reg(&mut self, reg: usize, m: usize) {
        if self.no_nulls {
            return;
        }
        let words = m.div_ceil(64);
        let base = reg * NULL_WORDS_PER_REG;
        for w in 0..words {
            self.null_bits[base + w] = 0;
        }
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

/// Propagate unary null: dst_null = src_null (word-at-a-time).
fn null_copy1(s: &mut EvalScratch, dst: usize, src: usize, m: usize) {
    if s.no_nulls {
        return;
    }
    if dst == src {
        return;
    }
    let words = m.div_ceil(64);
    let base_s = src * NULL_WORDS_PER_REG;
    let base_d = dst * NULL_WORDS_PER_REG;
    for w in 0..words {
        s.null_bits[base_d + w] = s.null_bits[base_s + w];
    }
}

/// Fill null bits into register `di` for the payload columns selected by
/// `mask` (bit N = payload slot N): a row is null iff any masked column is
/// null. A single-column caller passes `1 << pi`; the two-operand string
/// compare passes both columns' bits in one pass.
fn fill_null_bits_mask(s: &mut EvalScratch, di: usize, mo: &Morsel<'_>, mask: u64) {
    if s.no_nulls {
        return;
    }
    let words = mo.m.div_ceil(64);
    let base = di * NULL_WORDS_PER_REG;
    for w in 0..words {
        let lo = w * 64;
        let hi = (lo + 64).min(mo.m);
        let mut word: u64 = 0;
        for i in lo..hi {
            let row_null = read_u64_le(mo.null_bmp, (mo.start + i) * 8);
            if row_null & mask != 0 {
                word |= 1u64 << (i - lo);
            }
        }
        s.null_bits[base + w] = word;
    }
}

/// IS [NOT] NULL: read payload column `pi`'s null bit per row, optionally invert
/// (`invert` for IS NOT NULL), and write the boolean into register `dst`. The result
/// register is always non-null (`clear_null_reg`).
fn eval_is_null(scratch: &mut EvalScratch, mo: &Morsel<'_>, dst: usize, pi: usize, invert: bool) {
    debug_assert_ne!(
        pi, PAYLOAD_MAPPING_PK_SENTINEL as usize,
        "IS [NOT] NULL operand resolved to a PK column; the binder must const-fold \
         null tests on non-nullable (incl. PK) columns",
    );
    scratch.clear_null_reg(dst, mo.m);
    let base_d = dst * MORSEL;
    for i in 0..mo.m {
        let row_null = read_u64_le(mo.null_bmp, (mo.start + i) * 8);
        scratch.regs[base_d + i] = (null_word_get(row_null, pi) ^ invert) as i64;
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

/// Unpack `bool_bits[dst]` into `regs[dst]` as 0/1 i64 per row.
fn unpack_bool_to_regs(scratch: &mut EvalScratch, dst: usize, m: usize) {
    let words = m.div_ceil(64);
    let base_r = dst * MORSEL;
    let base_b = dst * NULL_WORDS_PER_REG;
    for w in 0..words {
        let lo = w * 64;
        let hi = (lo + 64).min(m);
        let bits = scratch.bool_bits[base_b + w];
        for i in lo..hi {
            scratch.regs[base_r + i] = ((bits >> (i - lo)) & 1) as i64;
        }
    }
}

/// Pack one register's i64 truthy bits into `bool_bits[dst]`.
///
/// Deliberately **not** `#[inline(always)]`, unlike its always-inlined caller:
/// it is a word loop, not a guard, so it follows the crate-root rule — the same
/// split as `EvalScratch::ensure_capacity`/`grow`. It runs once per instruction
/// per morsel, not per row, so the caller's frame size is the cost that matters.
fn pack_to_bool_bits(scratch: &mut EvalScratch, dst: usize, m: usize) {
    let words = m.div_ceil(64);
    let base_r = dst * MORSEL;
    let base_b = dst * NULL_WORDS_PER_REG;
    for w in 0..words {
        let lo = w * 64;
        let hi = (lo + 64).min(m);
        let mut bits: u64 = 0;
        for i in lo..hi {
            bits |= ((scratch.regs[base_r + i] != 0) as u64) << (i - lo);
        }
        scratch.bool_bits[base_b + w] = bits;
    }
}

/// Bridge for producers that wrote `regs[dst]` and may have a downstream BOOL
/// consumer (or, for filters, a bit_only result_reg). Non-bool producers reach
/// a BOOL consumer through this path without restructuring their inner loop.
///
/// Per instruction per morsel, and on the `no_nulls` arm (or a register with no
/// BOOL consumer) it is *only* the two-branch test — which is why the test
/// inlines while [`pack_to_bool_bits`] stays out of line.
#[inline(always)]
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
#[inline(always)]
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

/// Walk `dst`'s null mask and zero the matching register entries. Used by
/// the string-comparison helpers, which compute results unconditionally for
/// vectorization and then clear null rows in a post-pass.
fn zero_null_rows(scratch: &mut EvalScratch, dst: usize, m: usize) {
    if scratch.no_nulls {
        return;
    }
    let base_d = dst * MORSEL;
    let EvalScratch { regs, null_bits, .. } = scratch;
    for_each_null_row(null_bits, dst * NULL_WORDS_PER_REG, m, |i| regs[base_d + i] = 0);
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
        debug_assert!(
            pi_byte != PAYLOAD_MAPPING_PK_SENTINEL,
            "string compare operand resolved to a PK column; a PK column is never a string",
        );
        let pi = pi_byte as usize;
        StrOperand {
            cells: mb.col_data(pi, 16),
            blob: mb.blob(),
            stride: 16,
            null_bit: 1u64 << pi,
        }
    }

    /// The resolved constant cell, shared by every row.
    fn constant(prog: &'a ResolvedProgram, const_idx: usize) -> Self {
        StrOperand {
            cells: &prog.const_cells[const_idx],
            blob: &prog.const_blob,
            stride: 0,
            null_bit: 0,
        }
    }

    #[inline(always)]
    fn cell(&self, row: usize) -> &[u8] {
        let o = row * self.stride;
        &self.cells[o..o + 16]
    }
}

/// The one string-compare kernel. Rows where either operand's column is null get
/// their result cleared in a post-pass; the compare itself runs unconditionally
/// (a valid but irrelevant value), keeping the loop branch-free.
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
    zero_null_rows(scratch, dst, mo.m);
    maybe_pack_bool_bits(scratch, mo, dst);
}

/// Reinterpret a register's raw i64 bits as the `f64` they encode, and back.
/// `#[inline(always)]` so each use folds into its per-row loop with no call,
/// keeping every float arm branch-free and vectorizable.
#[inline(always)]
fn decode_f64(bits: i64) -> f64 {
    f64::from_bits(bits as u64)
}
#[inline(always)]
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
    // Declared before the macros below so their bodies can name it: a macro can
    // only capture bindings that already exist where it is defined.
    let mo = Morsel {
        prog,
        null_bmp: mb.null_bmp(),
        start: morsel_start,
        m,
    };
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
    // the source null word, repack bool bits. Indexed rather than via `reg3` —
    // `validate` anti-aliases only the binary opcodes, so a unary `dst == a` is
    // legal (and `null_copy1` has its own `dst == src` early-out).
    macro_rules! un_op {
        ($a:expr, $d:expr, |$x:ident| $body:expr) => {{
            let ai = $a as usize;
            let base_a = ai * MORSEL;
            let base_d = $d * MORSEL;
            for i in 0..m {
                let $x = scratch.regs[base_a + i];
                scratch.regs[base_d + i] = $body;
            }
            null_copy1(scratch, $d, ai, m);
            maybe_pack_bool_bits(scratch, &mo, $d);
        }};
    }
    // DIV-shaped op: compute per-row, mark divide-by-zero rows null, substitute
    // a safe divisor so the computed slot holds a defined (irrelevant) value.
    // `body(a, b)` must return `(result: i64, is_zero: bool)`.
    macro_rules! div_like {
        ($a:expr, $b:expr, $d:expr, |$a_i:ident, $b_i:ident| $body:expr) => {{
            let ai = $a as usize;
            let bi = $b as usize;
            let mut zero_mask = [0u64; NULL_WORDS_PER_REG];
            {
                let ([ra, rb], rd) = scratch.regs_split([ai, bi], $d, m);
                for i in 0..m {
                    let $a_i = ra[i];
                    let $b_i = rb[i];
                    let (val, is_zero): (i64, bool) = $body;
                    rd[i] = val;
                    if is_zero {
                        zero_mask[i / 64] |= 1u64 << (i % 64);
                    }
                }
            }
            null_or2(scratch, $d, ai, bi, m);
            if !scratch.no_nulls {
                let base_null_d = $d * NULL_WORDS_PER_REG;
                let words = m.div_ceil(64);
                for w in 0..words {
                    scratch.null_bits[base_null_d + w] |= zero_mask[w];
                }
            }
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
    // mark the failures NULL. Indexed rather than via `regs_split` — `validate`
    // anti-aliases only the binary opcodes, so a unary `dst == a` is legal (see
    // `un_op!`). `body` returns `(value, failed)`; the value is always defined,
    // so a failed row never leaves uninitialised bits behind.
    //
    // The failure bits are accumulated in a register-resident word and merged
    // with the operand's null word once per 64 rows, the shape
    // `fill_null_bits_mask` uses — one store per word, and no per-row aliasing
    // between the value writes and the mask.
    macro_rules! unary_null_like {
        ($a:expr, $d:expr, |$x:ident| $body:expr) => {{
            let ai = $a as usize;
            let base_a = ai * MORSEL;
            let base_d = $d * MORSEL;
            let nulls = !scratch.no_nulls;
            let (base_null_a, base_null_d) = (ai * NULL_WORDS_PER_REG, $d * NULL_WORDS_PER_REG);
            for w in 0..m.div_ceil(64) {
                let lo = w * 64;
                let hi = (lo + 64).min(m);
                let mut bad = 0u64;
                for i in lo..hi {
                    let $x = scratch.regs[base_a + i];
                    let (val, failed): (i64, bool) = $body;
                    scratch.regs[base_d + i] = val;
                    bad |= (failed as u64) << (i - lo);
                }
                // `no_nulls` keeps the null buffers at capacity 0, so the merge
                // must stay behind the flag even though every caller of this
                // macro is classified null-producing.
                if nulls {
                    scratch.null_bits[base_null_d + w] = scratch.null_bits[base_null_a + w] | bad;
                }
            }
            maybe_pack_bool_bits(scratch, &mo, $d);
        }};
    }

    // Null-skipping 2-ary extremum. `null_or2`'s `a|b` rule is exactly wrong here
    // (the result is null only when BOTH operands are), so this cannot use
    // `bin_op!`. `pick` returns true when `a` wins on value.
    //
    // The value loop compares unconditionally and stays branch-free; the rows
    // where exactly one operand is NULL are the exception, so they are bit-
    // scanned out of the null words afterwards and overwritten — the same
    // compare-then-fix-up shape `eval_str_cmp` and `zero_null_rows` use. Doing
    // the null test per row inside the loop instead costs about twice the
    // instructions and blocks vectorisation.
    macro_rules! minmax2 {
        ($a:expr, $b:expr, $d:expr, |$x:ident, $y:ident| $pick:expr) => {{
            let ai = $a as usize;
            let bi = $b as usize;
            {
                let ([ra, rb], rd) = scratch.regs_split([ai, bi], $d, m);
                for i in 0..m {
                    let $x = ra[i];
                    let $y = rb[i];
                    rd[i] = if $pick { $x } else { $y };
                }
            }
            if !scratch.no_nulls {
                let EvalScratch { regs, null_bits, .. } = scratch;
                let (base_a, base_b, base_d) = (
                    ai * NULL_WORDS_PER_REG,
                    bi * NULL_WORDS_PER_REG,
                    $d * NULL_WORDS_PER_REG,
                );
                for w in 0..m.div_ceil(64) {
                    let (wa, wb) = (null_bits[base_a + w], null_bits[base_b + w]);
                    // A NULL operand yields the other one, so only the rows where
                    // exactly one side is NULL need their value replaced.
                    for (mask, src) in [(wa & !wb, bi), (wb & !wa, ai)] {
                        let mut rest = mask;
                        while rest != 0 {
                            let i = w * 64 + rest.trailing_zeros() as usize;
                            regs[$d * MORSEL + i] = regs[src * MORSEL + i];
                            rest &= rest - 1;
                        }
                    }
                    null_bits[base_d + w] = wa & wb;
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

            Instr::LoadPayloadFloat { dst, pi, wide } => {
                debug_assert_ne!(
                    pi, PAYLOAD_MAPPING_PK_SENTINEL,
                    "LOAD_COL_FLOAT operand resolved to a PK column; a PK column is never float",
                );
                let dst = dst as usize;
                let pi = pi as usize;
                let col_data = mb.col_data(pi, if wide { 8 } else { 4 });
                let dst_reg = scratch.reg_mut(dst, m);
                // `wide` is the resolve-time F64-vs-F32 verdict; `validate` pins
                // this column to F32/F64 (`ColKind::Float`), so the two arms are
                // total. An F32 widens to f64 on load — every float register holds
                // an f64 image, which is why a computed float column is F64.
                if wide {
                    let b = &col_data[morsel_start * 8..(morsel_start + m) * 8];
                    for (i, c) in b.chunks_exact(8).enumerate() {
                        dst_reg[i] = i64::from_le_bytes(c.try_into().unwrap());
                    }
                } else {
                    let b = &col_data[morsel_start * 4..(morsel_start + m) * 4];
                    for (i, c) in b.chunks_exact(4).enumerate() {
                        let bits = u32::from_le_bytes(c.try_into().unwrap());
                        dst_reg[i] = encode_f64(f32::from_bits(bits) as f64);
                    }
                }
                fill_null_bits_mask(scratch, dst, &mo, 1u64 << pi);
                maybe_pack_bool_bits(scratch, &mo, dst);
            }

            // PK-region integer load: one `decode_opk_i64` per row, which is the
            // exact inverse of the OPK encoding fused with `fi`'s widening. The
            // region, its stride and the slice width are all derived once per
            // instruction, exactly as `LoadPayloadInt` derives `col_data` — going
            // through the per-row `get_pk_bytes` instead would re-multiply and
            // re-bounds-check the same address on every row, which `-O0` cannot
            // hoist. The 8-arm match stays inside `decode_opk_i64` rather than
            // unswitching this loop into eight width-specialised copies: at `-O0`
            // it is a handful of instructions on a loop-invariant 1-byte enum,
            // and at `-O1`+ LLVM unswitches it.
            Instr::LoadPk { dst, off, fi } => {
                let dst = dst as usize;
                let w = fi.width();
                let (pk, stride) = mb.pk_region();
                let base_d = dst * MORSEL;
                let mut cell = morsel_start * stride + off as usize;
                for i in 0..m {
                    scratch.regs[base_d + i] = gnitz_wire::decode_opk_i64(&pk[cell..cell + w], fi);
                    cell += stride;
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
            // Testing the ROUNDED value rather than `|x| > f32::MAX` keeps the
            // doubles just above f32::MAX that round back down to it finite.
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
                    if !prog.is_bit_only(dst) {
                        unpack_bool_to_regs(scratch, dst, m);
                    }
                    // Dead-tail skip: this AND is a chain trigger and the whole
                    // morsel is definite-FALSE, so the AND-chain filter is FALSE —
                    // write the terminal (`result_reg`) all-FALSE and stop. The mask
                    // is keyed by register, so `dst < num_regs ≤ 64` keeps the shift
                    // in range; `!= 0` fast-skips no-chain filters. Nullable arm only:
                    // it already keeps packed bits, so the all-FALSE test is a cheap
                    // word-OR (the `no_nulls` arm has none to reduce over).
                    if prog.chain_trigger_mask != 0 && (prog.chain_trigger_mask >> dst) & 1 != 0 {
                        let base = dst * NULL_WORDS_PER_REG;
                        let words = m.div_ceil(64);
                        let mut alive = 0u64;
                        for w in 0..words {
                            alive |= scratch.bool_bits[base + w] | scratch.null_bits[base + w];
                        }
                        if alive == 0 {
                            let tb = prog.result_reg as usize * NULL_WORDS_PER_REG;
                            for w in 0..words {
                                scratch.bool_bits[tb + w] = 0;
                                scratch.null_bits[tb + w] = 0;
                            }
                            break;
                        }
                    }
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
                    if !prog.is_bit_only(dst) {
                        unpack_bool_to_regs(scratch, dst, m);
                    }
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
                    if !prog.is_bit_only(dst) {
                        unpack_bool_to_regs(scratch, dst, m);
                    }
                }
            }

            // ----------------------------------------------------------------
            // IS NULL / IS NOT NULL
            // ----------------------------------------------------------------
            // The two opcodes carry identical fields and differ only in whether
            // the bit is inverted, so they share one kernel.
            Instr::IsNull { dst, pi } => {
                eval_is_null(scratch, &mo, dst as usize, pi as usize, /* invert = */ false)
            }
            Instr::IsNotNull { dst, pi } => {
                eval_is_null(scratch, &mo, dst as usize, pi as usize, /* invert = */ true)
            }

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
                    // Nullable arm. `cond` may be bit_only (its producer skips the
                    // unpack to regs), so read its truthiness from `bool_bits`, not
                    // `regs`. take_a = cond truthy AND non-null, per row bit.
                    let words = m.div_ceil(64);
                    let base_cond_n = ci * NULL_WORDS_PER_REG;
                    let mut take_a = [0u64; NULL_WORDS_PER_REG];
                    for w in 0..words {
                        take_a[w] = scratch.bool_bits[base_cond_n + w] & !scratch.null_bits[base_cond_n + w];
                    }
                    // Null mask: dst is null wherever the chosen branch is null.
                    {
                        let ([na, nb], nd) = scratch.null_split([ai, bi], dst, words);
                        for w in 0..words {
                            nd[w] = (take_a[w] & na[w]) | (!take_a[w] & nb[w]);
                        }
                    }
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
                if !scratch.no_nulls {
                    let words = m.div_ceil(64);
                    let base_null = dst * NULL_WORDS_PER_REG;
                    scratch.null_bits[base_null..base_null + words].fill(u64::MAX);
                    // Mask the tail word so stale high bits past `m` never read as null.
                    if !m.is_multiple_of(64) {
                        scratch.null_bits[base_null + words - 1] = (1u64 << (m % 64)) - 1;
                    }
                    maybe_pack_bool_bits(scratch, &mo, dst);
                }
            }

            // ----------------------------------------------------------------
            // String comparisons (column vs constant / column vs column)
            // ----------------------------------------------------------------
            // Both arms are the same compare over two operands; only where
            // operand B comes from differs. `str_cmp!` keeps the three-way
            // operator branch outside the row loop, so each `StrOp` still gets
            // its own monomorphic, branch-free kernel.
            Instr::StrColConst { op, dst, pi, const_idx } => str_cmp!(
                op,
                dst as usize,
                StrOperand::column(mb, pi),
                StrOperand::constant(prog, const_idx as usize)
            ),
            Instr::StrColCol { op, dst, pi_a, pi_b } => str_cmp!(
                op,
                dst as usize,
                StrOperand::column(mb, pi_a),
                StrOperand::column(mb, pi_b)
            ),
        }
    }
}

#[cfg(test)]
mod tests;
