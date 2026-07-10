//! Morsel-oriented batch expression evaluator.
//!
//! `eval_batch` processes one morsel at a time (up to MORSEL rows), applying
//! all expression opcodes as columnar loops over register buffers.  Base
//! pointers are hoisted outside the inner loop, letting LLVM auto-vectorize
//! every arithmetic opcode.

use std::cmp::Ordering;

use super::program::{compare_col_string_vs_const, CmpOp, Instr, ResolvedProgram, StrOp};
use crate::foundation::codec::read_u64_le;
use crate::schema::{compare_german_strings, PAYLOAD_MAPPING_PK_SENTINEL};
use crate::storage::MemBatch;

pub(in crate::expr) const MORSEL: usize = 256;
pub(in crate::expr) const NULL_WORDS_PER_REG: usize = MORSEL / 64; // 4

// ---------------------------------------------------------------------------
// EvalScratch — the SoA register file for batch evaluation
// ---------------------------------------------------------------------------

pub(in crate::expr) struct EvalScratch {
    /// Register buffers, register-major layout: regs[reg * MORSEL + row].
    pub(in crate::expr) regs: Vec<i64>,
    /// Null bitmask, register-major: null_bits[reg * NULL_WORDS_PER_REG + word].
    /// Empty (capacity 0) when `no_nulls` is true.
    pub(in crate::expr) null_bits: Vec<u64>,
    /// Packed truthy bits, register-major; bridges boolean producers and
    /// consumers on the nullable arm without per-row repack from `regs`.
    /// Empty (capacity 0) when `no_nulls` is true.
    pub(in crate::expr) bool_bits: Vec<u64>,
    /// Per-row filter bitmask; set by run_filter, ignored by evaluate_map_batch.
    pub(in crate::expr) filter_bits: Vec<u64>,
    pub(in crate::expr) no_nulls: bool,
}

impl EvalScratch {
    pub(in crate::expr) fn new() -> Self {
        EvalScratch {
            regs: Vec::new(),
            null_bits: Vec::new(),
            bool_bits: Vec::new(),
            filter_bits: Vec::new(),
            no_nulls: false,
        }
    }

    /// Ensure the scratch buffer can hold `num_regs` registers and `(n+63)/64`
    /// filter words.  Does not shrink.
    pub(in crate::expr) fn ensure_capacity(&mut self, num_regs: usize, no_nulls: bool, n: usize) {
        let reg_cap = num_regs * MORSEL;
        if self.regs.len() < reg_cap {
            self.regs.resize(reg_cap, 0);
        }
        self.no_nulls = no_nulls;
        if !no_nulls {
            let null_cap = num_regs * NULL_WORDS_PER_REG;
            if self.null_bits.len() < null_cap {
                self.null_bits.resize(null_cap, 0);
            }
            if self.bool_bits.len() < null_cap {
                self.bool_bits.resize(null_cap, 0);
            }
        }
        let filter_words = n.div_ceil(64);
        if self.filter_bits.len() < filter_words {
            self.filter_bits.resize(filter_words, 0);
        }
    }

    pub(in crate::expr) fn reg_mut(&mut self, reg: usize, m: usize) -> &mut [i64] {
        &mut self.regs[reg * MORSEL..reg * MORSEL + m]
    }

    /// Split borrows: two shared sources + one mutable destination.
    /// Safety: SSA guarantees d != a and d != b (the debug_assert enforces this).
    pub(in crate::expr) fn reg3(&mut self, a: usize, b: usize, d: usize, m: usize) -> (&[i64], &[i64], &mut [i64]) {
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
    pub(in crate::expr) fn null_words3(
        &mut self,
        a: usize,
        b: usize,
        d: usize,
        words: usize,
    ) -> (&[u64], &[u64], &mut [u64]) {
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
    pub(in crate::expr) fn reg4(
        &mut self,
        a: usize,
        b: usize,
        c: usize,
        d: usize,
        m: usize,
    ) -> (&[i64], &[i64], &[i64], &mut [i64]) {
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
    pub(in crate::expr) fn clear_null_reg(&mut self, reg: usize, m: usize) {
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

/// Fill null bits for a physical payload column `pi` into register `di`.
fn fill_null_bits_pi(s: &mut EvalScratch, di: usize, null_bmp: &[u8], morsel_start: usize, m: usize, pi: usize) {
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
            if (row_null >> pi) & 1 != 0 {
                word |= 1u64 << (i - lo);
            }
        }
        s.null_bits[base + w] = word;
    }
}

/// IS [NOT] NULL: read payload column `pi`'s null bit per row, optionally invert
/// (`invert = 1` for IS NOT NULL), and write the boolean into register `dst`. The result
/// register is always non-null (`clear_null_reg`).
#[allow(clippy::too_many_arguments)]
fn eval_is_null(
    scratch: &mut EvalScratch,
    mb: &MemBatch,
    prog: &ResolvedProgram,
    dst: usize,
    morsel_start: usize,
    m: usize,
    pi: usize,
    invert: u64,
) {
    debug_assert_ne!(
        pi, PAYLOAD_MAPPING_PK_SENTINEL as usize,
        "IS [NOT] NULL operand resolved to a PK column; the binder must const-fold \
         null tests on non-nullable (incl. PK) columns",
    );
    scratch.clear_null_reg(dst, m);
    let base_d = dst * MORSEL;
    let null_bmp = mb.null_bmp();
    for i in 0..m {
        let row_null = read_u64_le(null_bmp, (morsel_start + i) * 8);
        scratch.regs[base_d + i] = (((row_null >> pi) & 1) ^ invert) as i64;
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
#[inline]
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
#[inline]
fn maybe_pack_bool_bits(scratch: &mut EvalScratch, prog: &ResolvedProgram, dst: usize, m: usize) {
    if scratch.no_nulls {
        return;
    }
    if prog.needs_bool_pack(dst) {
        pack_to_bool_bits(scratch, dst, m);
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
    let base_null = dst * NULL_WORDS_PER_REG;
    let words = m.div_ceil(64);
    for w in 0..words {
        let mut null_word = scratch.null_bits[base_null + w];
        let lo = w * 64;
        while null_word != 0 {
            let bit = null_word.trailing_zeros() as usize;
            scratch.regs[base_d + lo + bit] = 0;
            null_word &= null_word - 1;
        }
    }
}

// ---------------------------------------------------------------------------
// String comparison helpers — shared by *_CONST and *_COL match arms
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn eval_str_col_vs_const(
    scratch: &mut EvalScratch,
    mb: &MemBatch,
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
    fill_null_bits_pi(scratch, dst, mb.null_bmp(), morsel_start, m, pi);
    let col_data = mb.col_data(pi, 16);
    let blob = mb.blob;
    let const_bytes = &prog.const_strings[const_idx];
    let const_prefix = prog.const_prefixes[const_idx];
    let const_len = prog.const_lengths[const_idx];
    let base_d = dst * MORSEL;
    // Compute results unconditionally (null rows produce a valid but irrelevant value).
    // Keeping the comparison out of a branch-per-row allows LLVM to vectorize this loop.
    for i in 0..m {
        let off = (morsel_start + i) * 16;
        let s = &col_data[off..off + 16];
        scratch.regs[base_d + i] = pred(compare_col_string_vs_const(
            s,
            blob,
            const_bytes,
            const_prefix,
            const_len,
        )) as i64;
    }
    zero_null_rows(scratch, dst, m);
    maybe_pack_bool_bits(scratch, prog, dst, m);
}

#[allow(clippy::too_many_arguments)]
fn eval_str_col_vs_col(
    scratch: &mut EvalScratch,
    mb: &MemBatch,
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
    // Result is null where either operand column is null; fold both columns'
    // null bits into dst in one pass over the null bitmap.
    let pi_a = pi_byte_a as usize;
    let pi_b = pi_byte_b as usize;
    if !scratch.no_nulls {
        let words = m.div_ceil(64);
        let base = dst * NULL_WORDS_PER_REG;
        let null_bmp = mb.null_bmp();
        for w in 0..words {
            let lo = w * 64;
            let hi = (lo + 64).min(m);
            let mut word: u64 = 0;
            for i in lo..hi {
                let row_null = read_u64_le(null_bmp, (morsel_start + i) * 8);
                if ((row_null >> pi_a) | (row_null >> pi_b)) & 1 != 0 {
                    word |= 1u64 << (i - lo);
                }
            }
            scratch.null_bits[base + w] = word;
        }
    }
    let col_a = mb.col_data(pi_a, 16);
    let col_b = mb.col_data(pi_b, 16);
    let blob = mb.blob;
    let base_d = dst * MORSEL;
    // Compute results unconditionally; null rows produce a valid but irrelevant value.
    for i in 0..m {
        let row = morsel_start + i;
        let s1 = &col_a[row * 16..row * 16 + 16];
        let s2 = &col_b[row * 16..row * 16 + 16];
        scratch.regs[base_d + i] = pred(compare_german_strings(s1, blob, s2, blob)) as i64;
    }
    zero_null_rows(scratch, dst, m);
    maybe_pack_bool_bits(scratch, prog, dst, m);
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
pub(in crate::expr) fn eval_batch(
    prog: &ResolvedProgram,
    mb: &MemBatch,
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

    for instr in &prog.instrs {
        match *instr {
            // ----------------------------------------------------------------
            // Output instructions — materialized at batch level, not here
            // ----------------------------------------------------------------
            Instr::CopyCol { .. } | Instr::EmitNull { .. } | Instr::Emit { .. } => {}

            // ----------------------------------------------------------------
            // Load operations
            // ----------------------------------------------------------------
            Instr::LoadPayloadInt { dst, pi } => {
                let dst = dst as usize;
                let pi = pi as usize;
                let (size_u8, col_tc) = prog.payload_col_info[pi];
                let col_size = size_u8 as usize;
                let is_signed = crate::schema::is_signed_int(col_tc);
                let col_data = mb.col_data(pi, col_size);
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
                match (col_size, is_signed) {
                    // 8-byte: the one width with no widening and no signed/unsigned split —
                    // the i64 register IS the storage type, a bare bit-reinterpret. Kept inline
                    // so `load_int!` (which appends `as i64`) never emits a vacuous `i64 as i64`.
                    (8, _) => {
                        let b = &col_data[morsel_start * 8..(morsel_start + m) * 8];
                        for (i, c) in b.chunks_exact(8).enumerate() {
                            dst_reg[i] = i64::from_le_bytes(c.try_into().unwrap());
                        }
                    }
                    (4, true) => load_int!(i32),
                    (4, false) => load_int!(u32),
                    (2, true) => load_int!(i16),
                    (2, false) => load_int!(u16),
                    (1, true) => load_int!(i8),
                    (1, false) => load_int!(u8),
                    _ => {}
                }
                fill_null_bits_pi(scratch, dst, mb.null_bmp(), morsel_start, m, pi);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            Instr::LoadPayloadFloat { dst, pi } => {
                let dst = dst as usize;
                let pi = pi as usize;
                let col_size = prog.payload_col_info[pi].0 as usize;
                let col_data = mb.col_data(pi, col_size);
                let dst_reg = scratch.reg_mut(dst, m);
                if col_size == 4 {
                    let b = &col_data[morsel_start * 4..(morsel_start + m) * 4];
                    for (i, c) in b.chunks_exact(4).enumerate() {
                        let bits = u32::from_le_bytes(c.try_into().unwrap());
                        dst_reg[i] = encode_f64(f32::from_bits(bits) as f64);
                    }
                } else {
                    let b = &col_data[morsel_start * 8..(morsel_start + m) * 8];
                    for (i, c) in b.chunks_exact(8).enumerate() {
                        dst_reg[i] = i64::from_le_bytes(c.try_into().unwrap());
                    }
                }
                fill_null_bits_pi(scratch, dst, mb.null_bmp(), morsel_start, m, pi);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            // PK-region integer load. `signed` selects the decode; the unsigned
            // and signed branches delegate to `widen_pk_be` / `read_signed`.
            // Width and signedness are `const fn`s of `tc`; derive them once per
            // instruction, outside the row loop.
            Instr::LoadPk { dst, off, tc } => {
                let dst = dst as usize;
                let byte_offset = off as usize;
                let col_size = crate::schema::type_size(tc) as usize;
                let signed = crate::schema::is_signed_int(tc);
                let base_d = dst * MORSEL;
                if signed {
                    // Decode the OPK column back to native LE (un-flips the sign
                    // bit), then sign-extend to i64 at the exact column width.
                    for i in 0..m {
                        let opk = mb.get_pk_bytes(morsel_start + i);
                        let mut le = [0u8; 8];
                        gnitz_wire::decode_pk_column(
                            &opk[byte_offset..byte_offset + col_size],
                            tc,
                            &mut le[..col_size],
                        );
                        scratch.regs[base_d + i] = crate::schema::read_signed(&le, col_size);
                    }
                } else {
                    // The addressed OPK column is big-endian; `widen_pk_be` right-
                    // aligns it into a u128, recovering the native value.
                    for i in 0..m {
                        let opk = mb.get_pk_bytes(morsel_start + i);
                        scratch.regs[base_d + i] =
                            gnitz_wire::widen_pk_be(&opk[byte_offset..byte_offset + col_size], col_size) as i64;
                    }
                }
                scratch.clear_null_reg(dst, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            Instr::LoadConst { dst, val } => {
                let dst = dst as usize;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    scratch.regs[base_d + i] = val;
                }
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
            Instr::IntNeg { dst, a } => {
                let dst = dst as usize;
                let ai = a as usize;
                let base_a = ai * MORSEL;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    scratch.regs[base_d + i] = scratch.regs[base_a + i].wrapping_neg();
                }
                null_copy1(scratch, dst, ai, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
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
            Instr::FloatNeg { dst, a } => {
                let dst = dst as usize;
                let ai = a as usize;
                let base_a = ai * MORSEL;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    scratch.regs[base_d + i] = encode_f64(-decode_f64(scratch.regs[base_a + i]));
                }
                null_copy1(scratch, dst, ai, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

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
                    let base_a = ai * MORSEL;
                    let base_b = bi * MORSEL;
                    let base_d = dst * MORSEL;
                    for i in 0..m {
                        scratch.regs[base_d + i] =
                            ((scratch.regs[base_a + i] != 0) && (scratch.regs[base_b + i] != 0)) as i64;
                    }
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
                    let base_a = ai * MORSEL;
                    let base_b = bi * MORSEL;
                    let base_d = dst * MORSEL;
                    for i in 0..m {
                        scratch.regs[base_d + i] =
                            ((scratch.regs[base_a + i] != 0) || (scratch.regs[base_b + i] != 0)) as i64;
                    }
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
                    let base_a = ai * MORSEL;
                    let base_d = dst * MORSEL;
                    for i in 0..m {
                        scratch.regs[base_d + i] = (scratch.regs[base_a + i] == 0) as i64;
                    }
                    null_copy1(scratch, dst, ai, m);
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
            Instr::IsNull { dst, pi } => eval_is_null(scratch, mb, prog, dst as usize, morsel_start, m, pi as usize, 0),
            Instr::IsNotNull { dst, pi } => {
                eval_is_null(scratch, mb, prog, dst as usize, morsel_start, m, pi as usize, 1)
            }

            // ----------------------------------------------------------------
            // Type cast. `signed: false` reinterprets the register as u64 first
            // so values >= 2^63 cast to the correct large positive float.
            // ----------------------------------------------------------------
            Instr::IntToFloat { dst, a, signed } => {
                let dst = dst as usize;
                let ai = a as usize;
                let base_a = ai * MORSEL;
                let base_d = dst * MORSEL;
                if signed {
                    for i in 0..m {
                        scratch.regs[base_d + i] = encode_f64(scratch.regs[base_a + i] as f64);
                    }
                } else {
                    for i in 0..m {
                        scratch.regs[base_d + i] = encode_f64(scratch.regs[base_a + i] as u64 as f64);
                    }
                }
                null_copy1(scratch, dst, ai, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
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
