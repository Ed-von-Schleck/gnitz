//! [`Evaluator`] — a resolved program plus the register file it runs in, and the
//! three ways to drive it: [`Evaluator::filter`] over a whole batch,
//! [`Evaluator::eval_row`] over one row, [`Evaluator::eval_morsels`] a morsel at
//! a time with the raw registers exposed through [`MorselOut`].
//!
//! The evaluator owns its register file so that no caller can size the scratch
//! for one nullability arm and then read the other: a `no_nulls` register read
//! out of a nullable-sized scratch reads a register that arm never unpacked.
//!
//! Row ranges are half-open everywhere in this file: `end` is EXCLUSIVE.

use std::cell::RefCell;

use crate::batch::{
    eval_batch, for_each_null_row, pack_truthy, view_bytes, EvalScratch, StrView, MORSEL, NULL_WORDS_PER_REG,
};
use crate::{BatchView, ColumnLocator, ExprValidateErr, Instr, LogicalProgram, ResolvedProgram, SchemaFacts};

/// A resolved expression program together with the register file it evaluates
/// into: the runnable form the [`LogicalProgram`] constructors below produce,
/// and the whole surface a caller outside this crate needs.
///
/// The scratch is a `RefCell` because every entry point takes `&self` — a plan
/// node is reached through a shared handle (the VM interns `*const ScalarFunc`)
/// while its evaluator mutates registers. Single-threaded by construction: an
/// evaluator is owned by one operator on one worker, and the drive methods are
/// **not re-entrant** — calling one from inside [`Self::eval_morsels`]'s
/// callback panics on the live borrow.
pub struct Evaluator {
    pub(crate) prog: ResolvedProgram,
    scratch: RefCell<EvalScratch>,
}

/// Validating constructors — the only way to obtain an [`Evaluator`].
///
/// There is no unvalidated public `resolve`: resolution bakes in payload slots,
/// PK byte offsets, type codes and the nullability verdict, so a program
/// resolved against a schema it was never checked against addresses columns that
/// may not exist — the kernels would trip an assertion, or in release read a
/// neighbouring slot. Pairing each validation rule with the resolution it guards
/// makes "this program was checked against the schema it runs on" a property of
/// the type rather than a convention every consuming crate has to remember.
///
/// The three differ in which rules apply, which is also what fixes the
/// `is_filter` classification each one resolves under — so no caller passes a
/// bare boolean.
impl LogicalProgram {
    /// A filter predicate: checked against the schema it reads, and required to
    /// own a result register. Resolved with `result_reg` eligible for the
    /// bit_only path, which [`Evaluator::filter`] reads as packed bits.
    pub fn resolve_filter(self, schema: &dyn SchemaFacts) -> Result<Evaluator, ExprValidateErr> {
        self.validate_predicate(schema)?;
        Ok(self.into_evaluator(schema, /* is_filter = */ true))
    }

    /// A map: checked against both the schema it reads and the one it writes,
    /// so every declared output payload slot is covered exactly once.
    pub fn resolve_map(
        self,
        in_schema: &dyn SchemaFacts,
        out_schema: &dyn SchemaFacts,
    ) -> Result<Evaluator, ExprValidateErr> {
        self.validate(Some(in_schema), Some(out_schema))?;
        Ok(self.into_evaluator(in_schema, /* is_filter = */ false))
    }

    /// A scalar expression evaluated row at a time through
    /// [`Evaluator::eval_row`] — a DML SET right-hand side. Same checks as a map
    /// minus the output plan. `result_reg` is not marked as a filter's, so it is
    /// excluded from `bool_input`; a bare non-boolean result therefore has no
    /// `bool_bits` bit, which is why a predicate must go through
    /// [`Self::resolve_filter`] instead.
    ///
    /// A boolean-valued RHS (`SET flag = a AND b`) still reads back correctly:
    /// nothing demotes `result_reg` out of bit_only, but `bool_pack_mask` covers
    /// every bit_only register, so `read_reg_row0` finds the packed bit.
    pub fn resolve_scalar(self, schema: &dyn SchemaFacts) -> Result<Evaluator, ExprValidateErr> {
        self.validate(Some(schema), None)?;
        Ok(self.into_evaluator(schema, /* is_filter = */ false))
    }

    fn into_evaluator(self, schema: &dyn SchemaFacts, is_filter: bool) -> Evaluator {
        let prog = self.resolve_program(schema, is_filter);
        let scratch = RefCell::new(EvalScratch::new(&prog));
        Evaluator { prog, scratch }
    }
}

impl Evaluator {
    /// Evaluate over a single row and report `(value, is_null)`.
    ///
    /// A register-free program (`num_regs == 0`) has no result to read and
    /// returns `(0, true)`: `validate` bounds `result_reg` only when
    /// `num_regs != 0`, so the `bit_only_mask >> result_reg` inside
    /// [`read_reg_row0`] would address a register that was never allocated.
    pub fn eval_row<B: BatchView>(&self, mb: &B, row: usize) -> (i64, bool) {
        if self.prog.num_regs == 0 {
            return (0, true);
        }
        debug_assert!(
            !self.prog.result_is_str,
            "string-valued program read through eval_row; use eval_row_str"
        );
        let no_nulls = self.prog.no_nulls;
        let scratch = &mut *self.scratch.borrow_mut();
        scratch.ensure_capacity(&self.prog, 1);
        eval_batch(&self.prog, mb, row, 1, scratch);
        let r = self.prog.result_reg as usize;
        // `ensure_capacity` allocates no `null_bits` under `no_nulls`, so the
        // read must stay behind the guard — an unguarded index hits an empty
        // `Vec`.
        let is_null = !no_nulls && (scratch.null_bits[r * NULL_WORDS_PER_REG] & 1) != 0;
        // Normalized so a NULL row returns a fixed pair rather than whatever its
        // kernel left in the register (see `EvalScratch::regs`).
        let val = if is_null {
            0
        } else {
            read_reg_row0(&self.prog, scratch, r)
        };
        (val, is_null)
    }

    /// Run as a filter over all `n` rows of `mb`, invoking `append_range` once
    /// per maximal contiguous run of passing rows. The bitmap stays inside the
    /// scratch — no per-call `Vec<u64>` allocation.
    pub fn filter<B: BatchView>(&self, mb: &B, n: usize, mut append_range: impl FnMut(usize, usize)) {
        let no_nulls = self.prog.no_nulls;
        // `validate_predicate` rejects a register-free program, so a predicate's
        // `result_reg` always names an allocated register.
        let result_reg = self.prog.result_reg as usize;
        let scratch = &mut *self.scratch.borrow_mut();
        scratch.ensure_capacity(&self.prog, n);

        for morsel_start in (0..n).step_by(MORSEL) {
            let m = MORSEL.min(n - morsel_start);
            eval_batch(&self.prog, mb, morsel_start, m, scratch);

            // MORSEL is 64-aligned, so each morsel maps onto a contiguous run of
            // whole `filter_bits` words — every word is written in full, sparing
            // both the read-modify-write and the up-front zero-fill.
            let filter_word_base = morsel_start / 64;
            if no_nulls {
                // `no_nulls` allocates no `bool_bits`, so the verdict is read out
                // of `regs`.
                let EvalScratch { regs, filter_bits, .. } = &mut *scratch;
                let base_r = result_reg * MORSEL;
                pack_truthy(
                    &regs[base_r..base_r + m],
                    &mut filter_bits[filter_word_base..filter_word_base + m.div_ceil(64)],
                );
            } else {
                // Word-level merge: filter bit = truthy & !null.
                let base = result_reg * NULL_WORDS_PER_REG;
                let words_m = m.div_ceil(64);
                for w in 0..words_m {
                    scratch.filter_bits[filter_word_base + w] =
                        scratch.bool_bits[base + w] & !scratch.null_bits[base + w];
                }
                // Mask the dirty tail, once rather than per word: BOOL_NOT
                // complements whole words (`bool_bits = !va & !na`), leaving 1s
                // beyond `m % 64`. This upholds `scan_filter_bits`' precondition
                // that no bit past `n` is set. Those phantom bits sit at row `n`
                // and above, so they cannot pass a real row; unmasked they append
                // the degenerate range `(n, n)`, breaking the maximal-non-empty-run
                // contract `filter` documents.
                let tail_bits = m % 64;
                if tail_bits != 0 {
                    scratch.filter_bits[filter_word_base + words_m - 1] &= (1u64 << tail_bits) - 1;
                }
            }
        }

        scan_filter_bits(&scratch.filter_bits[..n.div_ceil(64)], n, &mut append_range);
    }

    /// Drive `n` rows starting at `start`, one morsel at a time, calling
    /// `f(rel_start, out)` per morsel — `rel_start` is the offset from `start`
    /// and `out` reads the morsel's results (and its row count) out of the
    /// register file. One closure call per *morsel*, not per row.
    ///
    /// One of the two morsel-chunking loops; [`Self::filter`] has the other,
    /// which packs a bitmap instead of handing the registers out.
    pub fn eval_morsels<B: BatchView>(&self, mb: &B, start: usize, n: usize, mut f: impl FnMut(usize, &MorselOut<'_>)) {
        let no_nulls = self.prog.no_nulls;
        // Dereferenced once, as in `filter`: at opt-level=0 every `&mut scratch`
        // through the guard is an out-of-line `RefMut::deref_mut` call.
        let scratch = &mut *self.scratch.borrow_mut();
        // No filter bitmap on this path — `filter` is the only writer of one.
        scratch.ensure_capacity(&self.prog, 0);
        for rel_start in (0..n).step_by(MORSEL) {
            let m = MORSEL.min(n - rel_start);
            eval_batch(&self.prog, mb, start + rel_start, m, scratch);
            let out = MorselOut {
                regs: &scratch.regs,
                null_bits: &scratch.null_bits,
                str_views: &scratch.str_views,
                str_arena: &scratch.str_arena,
                blob: mb.blob(),
                no_nulls,
                m,
            };
            f(rel_start, &out);
        }
    }

    /// Every `CopyCol` in the resolved stream as `(source locator, output payload
    /// slot)` — the verbatim column moves a map materializes columnar-side.
    pub fn copy_moves(&self) -> impl Iterator<Item = (ColumnLocator, u32)> + '_ {
        self.prog.instrs.iter().filter_map(|i| match *i {
            Instr::CopyCol { src, out } => Some((src, out)),
            _ => None,
        })
    }

    /// Every `Emit` in the resolved stream as `(source register, output payload
    /// slot, is_str)` — the computed columns a map writes out of the register
    /// file, and which register class each reads.
    ///
    /// The `Instr::EmitStr` arm is mandatory rather than convenient: this is a
    /// `filter_map` with a `_ => None` fallthrough, so a missing arm silently
    /// drops a string column's writer and ships its region uninitialized.
    pub fn emit_targets(&self) -> impl Iterator<Item = (u16, u32, bool)> + '_ {
        self.prog.instrs.iter().filter_map(|i| match *i {
            Instr::Emit { src, out } => Some((src, out, false)),
            Instr::EmitStr { src, out } => Some((src, out, true)),
            _ => None,
        })
    }

    /// Whether the result register holds a string, i.e. whether the result must
    /// be read through [`Self::eval_row_str`] rather than [`Self::eval_row`].
    /// Resolution knows the answer, so a caller never has to carry it alongside.
    pub fn result_is_str(&self) -> bool {
        self.prog.result_is_str
    }

    /// Evaluate over a single row and append the string result's bytes to `out`,
    /// reporting whether the row is NULL.
    ///
    /// The bytes are copied rather than borrowed because the arena lives behind
    /// the evaluator's `RefCell`: a borrowed return would have to hand back a
    /// live `RefMut` guard with it. The caller supplies the buffer so a DML row
    /// loop reuses one allocation across rows.
    pub fn eval_row_str<B: BatchView>(&self, mb: &B, row: usize, out: &mut Vec<u8>) -> bool {
        if self.prog.num_regs == 0 {
            return true;
        }
        debug_assert!(
            self.prog.result_is_str,
            "scalar program read through eval_row_str; use eval_row"
        );
        let no_nulls = self.prog.no_nulls;
        let scratch = &mut *self.scratch.borrow_mut();
        scratch.ensure_capacity(&self.prog, 1);
        eval_batch(&self.prog, mb, row, 1, scratch);
        let r = self.prog.result_reg as usize;
        out.extend_from_slice(scratch.str_reg_bytes(r, 0, mb.blob()));
        !no_nulls && (scratch.null_bits[r * NULL_WORDS_PER_REG] & 1) != 0
    }
}

/// One morsel's results, read out of the register file. Handed to
/// [`Evaluator::eval_morsels`]'s callback for the lifetime of that call.
pub struct MorselOut<'a> {
    regs: &'a [i64],
    null_bits: &'a [u64],
    str_views: &'a [StrView],
    str_arena: &'a [u8],
    blob: &'a [u8],
    no_nulls: bool,
    m: usize,
}

impl MorselOut<'_> {
    /// How many rows this morsel covers (always ≥ 1).
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.m
    }

    /// Register `reg`'s values for this morsel's rows, in row order.
    #[inline(always)]
    pub fn reg_values(&self, reg: usize) -> &[i64] {
        let base = reg * MORSEL;
        &self.regs[base..base + self.m]
    }

    /// The same values as their little-endian byte image, 8 bytes per row —
    /// what an 8-byte EMIT slot stores. `check_emit_slot` holds every EMIT
    /// destination to such a slot, so a caller can blit this straight into one.
    #[inline(always)]
    pub fn reg_bytes(&self, reg: usize) -> &[u8] {
        let regs = self.reg_values(reg);
        // SAFETY: `i64` has no padding and no invalid bit patterns, and the
        // crate is little-endian only, so the slice's byte image *is* its
        // `to_le_bytes()` sequence. `u8` is 1-aligned.
        unsafe { std::slice::from_raw_parts(regs.as_ptr().cast::<u8>(), regs.len() * 8) }
    }

    /// String register `reg`'s bytes for row `i` of this morsel.
    ///
    /// Unlike [`Self::reg_values`], which returns a slice already cut to `m`,
    /// this is a random-access getter and carries its own bound: an
    /// out-of-morsel read would otherwise hand back another value's bytes.
    #[inline(always)]
    pub fn str_bytes(&self, reg: usize, i: usize) -> &[u8] {
        debug_assert!(i < self.m, "str_bytes row {i} is outside the morsel's {} rows", self.m);
        view_bytes(self.str_views[reg * MORSEL + i], self.str_arena, self.blob)
    }

    /// Call `f(i)` for each of the morsel's rows where register `reg` is NULL.
    /// NULL rows are the exception, so consumers bit-scan rather than branch per
    /// row — a per-row branch would de-vectorize the surrounding value store.
    #[inline(always)]
    pub fn for_each_null_row(&self, reg: usize, f: impl FnMut(usize)) {
        if self.no_nulls {
            return;
        }
        for_each_null_row(self.null_bits, reg * NULL_WORDS_PER_REG, self.m, f);
    }
}

/// Read register `r`'s value after an m=1 [`eval_batch`]. On the nullable arm a
/// bit_only register is never unpacked into `regs`, so its truth value lives at
/// bit 0 of `bool_bits` instead.
pub(crate) fn read_reg_row0(prog: &ResolvedProgram, scratch: &EvalScratch, r: usize) -> i64 {
    if !prog.no_nulls && prog.is_bit_only(r) {
        i64::from((scratch.bool_bits[r * NULL_WORDS_PER_REG] & 1) != 0)
    } else {
        scratch.regs[r * MORSEL]
    }
}

/// Call `append_range(start, end)` for every maximal run of set bits. Steps run
/// to run, so a word costs one step per run in it rather than 64.
///
/// Needs no clamp against `n`: both `filter` arms leave bits past `n` clear.
fn scan_filter_bits<F: FnMut(usize, usize)>(bits: &[u64], n: usize, append_range: &mut F) {
    let mut open: Option<usize> = None;
    for (w, &word) in bits.iter().enumerate() {
        let row_base = w * 64;
        // An empty word ends any run carried in from the previous one. The loop
        // below never runs here, so it cannot do this itself.
        if word == 0 {
            if let Some(s) = open.take() {
                append_range(s, row_base);
            }
            continue;
        }
        let (mut rest, mut pos) = (word, 0usize);
        while rest != 0 {
            let zeros = rest.trailing_zeros() as usize;
            if zeros != 0 {
                if let Some(s) = open.take() {
                    append_range(s, row_base + pos);
                }
                pos += zeros;
                rest >>= zeros;
            }
            let ones = rest.trailing_ones() as usize;
            open.get_or_insert(row_base + pos);
            pos += ones;
            rest = rest.checked_shr(ones as u32).unwrap_or(0);
            // A run reaching the word's top may continue into the next word.
            if pos < 64 {
                append_range(open.take().expect("run just opened"), row_base + pos);
            }
        }
    }
    if let Some(s) = open {
        append_range(s, n);
    }
}

#[cfg(test)]
mod tests;
