//! Morsel-oriented batch expression evaluator.
//!
//! `eval_batch` processes one morsel at a time (up to MORSEL rows), applying
//! all expression opcodes as columnar loops over register buffers.  Base
//! pointers are hoisted outside the inner loop, letting LLVM auto-vectorize
//! every arithmetic opcode.

use std::cmp::Ordering;

use crate::{BatchView, CmpOp, Instr, ResolvedProgram, StrOp, PAYLOAD_MAPPING_PK_SENTINEL};
use gnitz_wire::{compare_german_strings, null_word_get, read_u64_le, FixedInt};

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

    /// Split borrows: two shared sources + one mutable destination.
    /// Safety: SSA guarantees d != a and d != b (the debug_assert enforces this).
    fn reg3(&mut self, a: usize, b: usize, d: usize, m: usize) -> (&[i64], &[i64], &mut [i64]) {
        debug_assert!(d != a && d != b, "reg3: dst aliases src register");
        unsafe {
            let ptr = self.regs.as_mut_ptr();
            let ra = std::slice::from_raw_parts(ptr.add(a * MORSEL), m);
            let rb = std::slice::from_raw_parts(ptr.add(b * MORSEL), m);
            let rd = std::slice::from_raw_parts_mut(ptr.add(d * MORSEL), m);
            (ra, rb, rd)
        }
    }

    /// Split borrows for null bit words.
    /// Safety: SSA guarantees d != a and d != b.
    fn null_words3(&mut self, a: usize, b: usize, d: usize, words: usize) -> (&[u64], &[u64], &mut [u64]) {
        debug_assert!(d != a && d != b, "null_words3: dst aliases src register");
        unsafe {
            let ptr = self.null_bits.as_mut_ptr();
            let ra = std::slice::from_raw_parts(ptr.add(a * NULL_WORDS_PER_REG), words);
            let rb = std::slice::from_raw_parts(ptr.add(b * NULL_WORDS_PER_REG), words);
            let rd = std::slice::from_raw_parts_mut(ptr.add(d * NULL_WORDS_PER_REG), words);
            (ra, rb, rd)
        }
    }

    /// Split borrows: three shared sources + one mutable destination. Backs the
    /// SELECT no-nulls value blend (`cond`, `a`, `b` → `dst`).
    /// Safety: SELECT's SSA anti-alias assert guarantees d != a, b, c.
    #[allow(clippy::type_complexity)]
    fn reg4(&mut self, a: usize, b: usize, c: usize, d: usize, m: usize) -> (&[i64], &[i64], &[i64], &mut [i64]) {
        debug_assert!(d != a && d != b && d != c, "reg4: dst aliases src register");
        unsafe {
            let ptr = self.regs.as_mut_ptr();
            let ra = std::slice::from_raw_parts(ptr.add(a * MORSEL), m);
            let rb = std::slice::from_raw_parts(ptr.add(b * MORSEL), m);
            let rc = std::slice::from_raw_parts(ptr.add(c * MORSEL), m);
            let rd = std::slice::from_raw_parts_mut(ptr.add(d * MORSEL), m);
            (ra, rb, rc, rd)
        }
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
// Null-bit helpers
// ---------------------------------------------------------------------------

/// Propagate binary null: dst_null = a_null | b_null (word-at-a-time).
fn null_or2(s: &mut EvalScratch, dst: usize, a: usize, b: usize, m: usize) {
    if s.no_nulls {
        return;
    }
    let words = m.div_ceil(64);
    let (na, nb, nd) = s.null_words3(a, b, dst, words);
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
fn fill_null_bits_mask(s: &mut EvalScratch, di: usize, null_bmp: &[u8], morsel_start: usize, m: usize, mask: u64) {
    if s.no_nulls {
        return;
    }
    let words = m.div_ceil(64);
    let base = di * NULL_WORDS_PER_REG;
    for w in 0..words {
        let lo = w * 64;
        let hi = (lo + 64).min(m);
        let mut word: u64 = 0;
        for i in lo..hi {
            let row_null = read_u64_le(null_bmp, (morsel_start + i) * 8);
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
#[allow(clippy::too_many_arguments)]
fn eval_is_null(
    scratch: &mut EvalScratch,
    null_bmp: &[u8],
    prog: &ResolvedProgram,
    dst: usize,
    morsel_start: usize,
    m: usize,
    pi: usize,
    invert: bool,
) {
    debug_assert_ne!(
        pi, PAYLOAD_MAPPING_PK_SENTINEL as usize,
        "IS [NOT] NULL operand resolved to a PK column; the binder must const-fold \
         null tests on non-nullable (incl. PK) columns",
    );
    scratch.clear_null_reg(dst, m);
    let base_d = dst * MORSEL;
    for i in 0..m {
        let row_null = read_u64_le(null_bmp, (morsel_start + i) * 8);
        scratch.regs[base_d + i] = (null_word_get(row_null, pi) ^ invert) as i64;
    }
    maybe_pack_bool_bits(scratch, prog, dst, m);
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
fn maybe_pack_bool_bits(scratch: &mut EvalScratch, prog: &ResolvedProgram, dst: usize, m: usize) {
    if scratch.no_nulls {
        return;
    }
    if prog.needs_bool_pack(dst) {
        pack_to_bool_bits(scratch, dst, m);
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
// String comparison helpers — shared by *_CONST and *_COL match arms
// ---------------------------------------------------------------------------

/// The one string-compare kernel: operand A is payload column cell data
/// (`col_a` over `blob_a`), operand B comes from `b_of(row)` — a per-row
/// column cell (col-vs-col) or the resolved const cell (col-vs-const). Rows
/// whose `null_mask` columns are null get their result cleared in a post-pass;
/// the compare itself runs unconditionally (a valid but irrelevant value),
/// keeping the loop branch-free.
#[allow(clippy::too_many_arguments)]
fn eval_str_cmp<'x>(
    scratch: &mut EvalScratch,
    null_bmp: &[u8],
    prog: &ResolvedProgram,
    dst: usize,
    morsel_start: usize,
    m: usize,
    null_mask: u64,
    col_a: &[u8],
    blob_a: &[u8],
    b_of: impl Fn(usize) -> (&'x [u8], &'x [u8]),
    pred: impl Fn(Ordering) -> bool,
) {
    fill_null_bits_mask(scratch, dst, null_bmp, morsel_start, m, null_mask);
    let base_d = dst * MORSEL;
    for i in 0..m {
        let row = morsel_start + i;
        let s1 = &col_a[row * 16..row * 16 + 16];
        let (s2, blob_b) = b_of(row);
        scratch.regs[base_d + i] = pred(compare_german_strings(s1, blob_a, s2, blob_b)) as i64;
    }
    zero_null_rows(scratch, dst, m);
    maybe_pack_bool_bits(scratch, prog, dst, m);
}

#[allow(clippy::too_many_arguments)]
fn eval_str_col_vs_const<B: BatchView>(
    scratch: &mut EvalScratch,
    mb: &B,
    prog: &ResolvedProgram,
    dst: usize,
    morsel_start: usize,
    m: usize,
    pi_byte: u8,
    const_idx: usize,
    pred: impl Fn(Ordering) -> bool,
) {
    debug_assert!(
        pi_byte != PAYLOAD_MAPPING_PK_SENTINEL,
        "eval_str_col_vs_const: PK column is never a string",
    );
    let pi = pi_byte as usize;
    let cell = &prog.const_cells[const_idx];
    eval_str_cmp(
        scratch,
        mb.null_bmp(),
        prog,
        dst,
        morsel_start,
        m,
        1u64 << pi,
        mb.col_data(pi, 16),
        mb.blob(),
        |_| (&cell[..], &prog.const_blob[..]),
        pred,
    );
}

#[allow(clippy::too_many_arguments)]
fn eval_str_col_vs_col<B: BatchView>(
    scratch: &mut EvalScratch,
    mb: &B,
    prog: &ResolvedProgram,
    dst: usize,
    morsel_start: usize,
    m: usize,
    pi_byte_a: u8,
    pi_byte_b: u8,
    pred: impl Fn(Ordering) -> bool,
) {
    debug_assert!(
        pi_byte_a != PAYLOAD_MAPPING_PK_SENTINEL && pi_byte_b != PAYLOAD_MAPPING_PK_SENTINEL,
        "eval_str_col_vs_col: PK column is never a string",
    );
    // Result is null where either operand column is null (one masked pass).
    let pi_a = pi_byte_a as usize;
    let pi_b = pi_byte_b as usize;
    let col_b = mb.col_data(pi_b, 16);
    let blob = mb.blob();
    eval_str_cmp(
        scratch,
        mb.null_bmp(),
        prog,
        dst,
        morsel_start,
        m,
        (1u64 << pi_a) | (1u64 << pi_b),
        mb.col_data(pi_a, 16),
        blob,
        move |row| (&col_b[row * 16..row * 16 + 16], blob),
        pred,
    );
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
                let (ra, rb, rd) = scratch.reg3(ai, bi, $d, m);
                for i in 0..m {
                    let $x = ra[i];
                    let $y = rb[i];
                    rd[i] = $body;
                }
            }
            null_or2(scratch, $d, ai, bi, m);
            maybe_pack_bool_bits(scratch, prog, $d, m);
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
            maybe_pack_bool_bits(scratch, prog, $d, m);
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
                let (ra, rb, rd) = scratch.reg3(ai, bi, $d, m);
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
            maybe_pack_bool_bits(scratch, prog, $d, m);
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

    let null_bmp = mb.null_bmp();
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
                fill_null_bits_mask(scratch, dst, null_bmp, morsel_start, m, 1u64 << pi);
                maybe_pack_bool_bits(scratch, prog, dst, m);
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
                fill_null_bits_mask(scratch, dst, null_bmp, morsel_start, m, 1u64 << pi);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            // PK-region integer load: one `decode_opk_i64` per row, which is the
            // exact inverse of the OPK encoding fused with `fi`'s widening. The
            // slice width is derived once per instruction, outside the row loop.
            // The 8-arm match stays inside `decode_opk_i64` rather than
            // unswitching this loop into eight width-specialised copies: at `-O0`
            // it is a handful of instructions on a loop-invariant 1-byte enum,
            // and at `-O1`+ LLVM unswitches it.
            Instr::LoadPk { dst, off, fi } => {
                let dst = dst as usize;
                let byte_offset = off as usize;
                let w = fi.width();
                let base_d = dst * MORSEL;
                for i in 0..m {
                    let opk = mb.get_pk_bytes(morsel_start + i);
                    scratch.regs[base_d + i] = gnitz_wire::decode_opk_i64(&opk[byte_offset..byte_offset + w], fi);
                }
                scratch.clear_null_reg(dst, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            Instr::LoadConst { dst, val } => {
                let dst = dst as usize;
                scratch.reg_mut(dst, m).fill(val);
                scratch.clear_null_reg(dst, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
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
            Instr::IntNeg { dst, a } => un_op!(a, dst as usize, |x| x.wrapping_neg()),

            // ----------------------------------------------------------------
            // Integer set membership (col IN (…) as one opcode)
            // ----------------------------------------------------------------
            // Binary-search each row's i64 register image in the pool `resolve`
            // sorted (duplicates left in place — `binary_search` is correct over
            // them). `LoadPayloadInt` writes every row's register
            // (NULL rows too, tracked in `null_bits`), so the search reads a real
            // i64 for all rows and the NULL-row result is masked by `null_copy1`
            // — exactly how `IntNeg` handles a NULL row.
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
            Instr::FloatNeg { dst, a } => un_op!(a, dst as usize, |x| encode_f64(-decode_f64(x))),

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
            Instr::IsNull { dst, pi } => eval_is_null(
                scratch,
                null_bmp,
                prog,
                dst as usize,
                morsel_start,
                m,
                pi as usize,
                /* invert = */ false,
            ),
            Instr::IsNotNull { dst, pi } => eval_is_null(
                scratch,
                null_bmp,
                prog,
                dst as usize,
                morsel_start,
                m,
                pi as usize,
                /* invert = */ true,
            ),

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
                    let (rc, ra, rb, rd) = scratch.reg4(ci, ai, bi, dst, m);
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
                        let (na, nb, nd) = scratch.null_words3(ai, bi, dst, words);
                        for w in 0..words {
                            nd[w] = (take_a[w] & na[w]) | (!take_a[w] & nb[w]);
                        }
                    }
                    // Value blend, row-level within each word.
                    {
                        let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                        for w in 0..words {
                            let lo = w * 64;
                            let hi = (lo + 64).min(m);
                            let ta = take_a[w];
                            for i in lo..hi {
                                rd[i] = if (ta >> (i - lo)) & 1 != 0 { ra[i] } else { rb[i] };
                            }
                        }
                    }
                    maybe_pack_bool_bits(scratch, prog, dst, m);
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
                    maybe_pack_bool_bits(scratch, prog, dst, m);
                }
            }

            // ----------------------------------------------------------------
            // String comparisons (column vs constant / column vs column)
            // ----------------------------------------------------------------
            Instr::StrColConst { op, dst, pi, const_idx } => {
                let d = dst as usize;
                let ci = const_idx as usize;
                match op {
                    StrOp::Eq => {
                        eval_str_col_vs_const(scratch, mb, prog, d, morsel_start, m, pi, ci, |o| o == Ordering::Equal)
                    }
                    StrOp::Lt => {
                        eval_str_col_vs_const(scratch, mb, prog, d, morsel_start, m, pi, ci, |o| o == Ordering::Less)
                    }
                    StrOp::Le => eval_str_col_vs_const(scratch, mb, prog, d, morsel_start, m, pi, ci, |o| {
                        o != Ordering::Greater
                    }),
                }
            }
            Instr::StrColCol { op, dst, pi_a, pi_b } => {
                let d = dst as usize;
                match op {
                    StrOp::Eq => eval_str_col_vs_col(scratch, mb, prog, d, morsel_start, m, pi_a, pi_b, |o| {
                        o == Ordering::Equal
                    }),
                    StrOp::Lt => eval_str_col_vs_col(scratch, mb, prog, d, morsel_start, m, pi_a, pi_b, |o| {
                        o == Ordering::Less
                    }),
                    StrOp::Le => eval_str_col_vs_col(scratch, mb, prog, d, morsel_start, m, pi_a, pi_b, |o| {
                        o != Ordering::Greater
                    }),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
