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

use crate::batch::{eval_batch, scan_filter_bits, with_str_bufs, EvalScratch, MorselOut, MORSEL};
use crate::program::Role;
use crate::{BatchView, ColumnLocator, ExprValidateErr, LogicalProgram, ResolvedProgram, SchemaFacts};

/// A resolved expression program together with the register file it evaluates
/// into: the runnable form the [`LogicalProgram`] constructors below produce,
/// and the whole surface a caller outside this crate needs.
///
/// The scratch is a `RefCell` because every entry point takes `&self` — a plan
/// node is reached through a shared handle while its evaluator mutates
/// registers. Single-threaded by construction: an evaluator is owned by one
/// operator on one worker, and the drive methods are **not re-entrant** —
/// calling one from inside [`Self::eval_morsels`]'s callback panics on the live
/// borrow.
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
/// The three differ in which rules apply, which is also what fixes the [`Role`]
/// each one resolves under — so no caller passes a bare classification.
impl LogicalProgram {
    /// A filter predicate: checked against the schema it reads, and required to
    /// own a result register. Resolved with `result_reg` eligible for the
    /// bit_only path, which [`Evaluator::filter`] reads as packed bits.
    pub fn resolve_filter(self, schema: &dyn SchemaFacts) -> Result<Evaluator, ExprValidateErr> {
        let str_class = self.validate_predicate(schema)?;
        Ok(self.into_evaluator(schema, None, Role::Filter, str_class))
    }

    /// A map: checked against both the schema it reads and the one it writes,
    /// so every declared output payload slot is covered exactly once.
    pub fn resolve_map(
        self,
        in_schema: &dyn SchemaFacts,
        out_schema: &dyn SchemaFacts,
    ) -> Result<Evaluator, ExprValidateErr> {
        let str_class = self.validate(Some(in_schema), Some(out_schema))?;
        Ok(self.into_evaluator(in_schema, Some(out_schema), Role::Map, str_class))
    }

    /// A scalar expression evaluated row at a time through
    /// [`Evaluator::eval_row`] — a DML SET right-hand side. Same checks as a map
    /// minus the output plan, plus the filter's rule that the program must own a
    /// result register: both row readers below read one back.
    ///
    /// `result_reg` is not marked as a filter's, so it is excluded from
    /// `bool_input` and a bare non-boolean result has no `bool_bits` bit — which
    /// is why a predicate must go through [`Self::resolve_filter`] instead. A
    /// boolean-valued RHS (`SET flag = a AND b`) still reads back correctly:
    /// `bool_pack_mask` covers every bit_only register, so `row0_value` finds
    /// the packed bit.
    pub fn resolve_scalar(self, schema: &dyn SchemaFacts) -> Result<Evaluator, ExprValidateErr> {
        let str_class = self.validate_result_reg(schema)?;
        Ok(self.into_evaluator(schema, None, Role::Scalar, str_class))
    }

    /// `out_schema` is `Some` only for a map; the other two roles write no
    /// output slots and so resolve no copy destination widths.
    fn into_evaluator(
        self,
        schema: &dyn SchemaFacts,
        out_schema: Option<&dyn SchemaFacts>,
        role: Role,
        str_class: u64,
    ) -> Evaluator {
        let prog = self.resolve_program(schema, out_schema, role, str_class);
        let scratch = RefCell::new(EvalScratch::new(&prog));
        Evaluator { prog, scratch }
    }
}

impl Evaluator {
    /// Evaluate over a single row: the result's value, or `None` for SQL NULL.
    ///
    /// `Option` rather than a `(value, is_null)` pair, so a NULL row has no
    /// value half for a caller to read past the flag — and no normalization
    /// rule is needed to give that half a defined value.
    pub fn eval_row<B: BatchView>(&self, mb: &B, row: usize) -> Option<i64> {
        debug_assert!(row < mb.row_count(), "eval_row row {row} is past the batch's end");
        debug_assert!(
            !self.prog.result_is_str(),
            "string-valued program read through eval_row; use eval_row_str"
        );
        let scratch = &mut *self.scratch.borrow_mut();
        scratch.ensure_capacity(&self.prog, 1);
        with_str_bufs(&self.prog, mb, |bufs| eval_batch(&self.prog, mb, bufs, row, 1, scratch));
        scratch.row0_value(&self.prog)
    }

    /// Run as a filter over all of `mb`'s rows, invoking `append_range` once per
    /// maximal contiguous run of passing rows. The bitmap stays inside the
    /// scratch — no per-call `Vec<u64>` allocation.
    ///
    /// The row count comes from the view, so no caller can drive it past the
    /// batch's end. Bound once here rather than in the morsel loop: `filter` is
    /// monomorphized in the calling crate at opt-level 0, where only the
    /// always-inline pass runs.
    pub fn filter<B: BatchView>(&self, mb: &B, mut append_range: impl FnMut(usize, usize)) {
        let n = mb.row_count();
        let scratch = &mut *self.scratch.borrow_mut();
        scratch.ensure_capacity(&self.prog, n);
        with_str_bufs(&self.prog, mb, |bufs| {
            for morsel_start in (0..n).step_by(MORSEL) {
                let m = MORSEL.min(n - morsel_start);
                eval_batch(&self.prog, mb, bufs, morsel_start, m, scratch);
                scratch.write_filter_words(&self.prog, morsel_start, m);
            }
        });
        scan_filter_bits(scratch.filter_words(n), n, &mut append_range);
    }

    /// Drive `n` rows starting at `start`, one morsel at a time, calling
    /// `f(rel_start, out)` per morsel — `rel_start` is the offset from `start`
    /// and `out` reads the morsel's results (and its row count) out of the
    /// register file. One closure call per *morsel*, not per row.
    ///
    /// One of the two morsel-chunking loops; [`Self::filter`] has the other,
    /// which packs a bitmap instead of handing the registers out.
    pub fn eval_morsels<B: BatchView>(&self, mb: &B, start: usize, n: usize, mut f: impl FnMut(usize, &MorselOut<'_>)) {
        debug_assert!(
            start + n <= mb.row_count(),
            "eval_morsels window {start}..{} is past the batch's end",
            start + n,
        );
        // Dereferenced once, as in `filter`: at opt-level=0 every `&mut scratch`
        // through the guard is an out-of-line `RefMut::deref_mut` call.
        let scratch = &mut *self.scratch.borrow_mut();
        // No filter bitmap on this path — `filter` is the only writer of one.
        scratch.ensure_capacity(&self.prog, 0);
        with_str_bufs(&self.prog, mb, |bufs| {
            for rel_start in (0..n).step_by(MORSEL) {
                let m = MORSEL.min(n - rel_start);
                eval_batch(&self.prog, mb, bufs, start + rel_start, m, scratch);
                f(rel_start, &scratch.morsel_out(bufs, m));
            }
        });
    }

    /// The verbatim column moves a map materializes columnar-side, as
    /// `(source locator, output payload slot, destination write width)`. A slice
    /// rather than an iterator: at `opt-level=0` the iterator's `next` would be
    /// an out-of-line call per (range × move).
    pub fn copies(&self) -> &[(ColumnLocator, u32, u8)] {
        &self.prog.copies
    }

    /// The computed columns a map writes out of the register file, as
    /// `(source register, output payload slot, is_str)`.
    pub fn emit_targets(&self) -> impl Iterator<Item = (u16, u32, bool)> + '_ {
        self.prog.emit_targets()
    }

    /// The scalar-register emits: one bulk copy of the register image each.
    pub fn scalar_emits(&self) -> &[(u16, u32)] {
        self.prog.scalar_emits()
    }

    /// The string-register emits: a German-string cell encoded per row each.
    pub fn str_emits(&self) -> &[(u16, u32)] {
        self.prog.str_emits()
    }

    /// True iff the program writes any output slot out of the register file, so
    /// driving the kernel can change the output. A pure projection emits
    /// nothing.
    pub fn emits_anything(&self) -> bool {
        !self.prog.emits.is_empty()
    }

    /// Bit `N` set iff payload slot `N` of the schema this program was resolved
    /// against admits NULL — the mask the resolution's own nullability verdict
    /// rests on, so a columnar consumer reads it rather than recomputing it.
    pub fn nullable_slots(&self) -> u64 {
        self.prog.nullable_slots
    }

    /// The surviving row ranges of `mb`, collected into `out` (cleared first) —
    /// what every range-driven consumer reads instead of driving
    /// [`Self::filter`]'s callback, which cannot carry a `?` out or be cut
    /// against a `LIMIT` window. `out` is caller-owned so it can be reused
    /// across chunks; `[(0, n)]` is the agreed spelling of "no predicate".
    pub fn filter_ranges<B: BatchView>(&self, mb: &B, out: &mut Vec<(usize, usize)>) {
        out.clear();
        self.filter(mb, |start, end| out.push((start, end)));
    }

    /// Whether the result register holds a string, i.e. whether the result must
    /// be read through [`Self::eval_row_str`] rather than [`Self::eval_row`].
    /// Resolution knows the answer, so a caller never has to carry it alongside.
    pub fn result_is_str(&self) -> bool {
        self.prog.result_is_str()
    }

    /// Evaluate over a single row, append the string result's bytes to `out`, and
    /// return **true iff the row is NULL** — the opposite polarity from
    /// [`Self::eval_row`]'s `Option`, stated here because the two readers sit on
    /// one type. `Option` would have to wrap the append rather than the value,
    /// and a name advertising the flag would hide it.
    ///
    /// The bytes are copied rather than borrowed because the arena lives behind
    /// the evaluator's `RefCell`: a borrowed return would have to hand back a
    /// live `RefMut` guard with it. The caller supplies the buffer so a DML row
    /// loop reuses one allocation across rows.
    #[must_use]
    pub fn eval_row_str<B: BatchView>(&self, mb: &B, row: usize, out: &mut Vec<u8>) -> bool {
        debug_assert!(row < mb.row_count(), "eval_row_str row {row} is past the batch's end");
        debug_assert!(
            self.prog.result_is_str(),
            "scalar program read through eval_row_str; use eval_row"
        );
        let scratch = &mut *self.scratch.borrow_mut();
        scratch.ensure_capacity(&self.prog, 1);
        let r = self.prog.result_reg as usize;
        with_str_bufs(&self.prog, mb, |bufs| {
            eval_batch(&self.prog, mb, bufs, row, 1, scratch);
            out.extend_from_slice(scratch.morsel_out(bufs, 1).str_bytes(r, 0));
        });
        scratch.row0_is_null(r)
    }
}

#[cfg(test)]
#[path = "tests/eval.rs"]
mod tests;
