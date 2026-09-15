//! [`Evaluator`] — a resolved program plus the register file it runs in, and the
//! ways to drive it: one private morsel loop ([`Evaluator::drive`]) under one
//! read-back per result class — a filter's ranges, an [`ExprResults`], or the
//! raw registers through [`MorselOut`]. There is no single-row arity; a caller
//! that wants one row drives its batch and indexes the result.
//!
//! The evaluator owns its register file so that no caller can size the scratch
//! for one nullability arm and then read the other: a `no_nulls` register read
//! out of a nullable-sized scratch reads a register that arm never unpacked.
//!
//! Row ranges are half-open everywhere in this file: `end` is EXCLUSIVE.

use std::cell::RefCell;

use crate::batch::{eval_batch, scan_filter_bits, with_str_bufs, EvalScratch, MorselOut, StrBufs, MORSEL};
use crate::program::Role;
use crate::{BatchView, ColumnLocator, ExprValidateErr, LogicalProgram, NullPerm, ResolvedProgram, SchemaFacts};

/// One program's result for every row of a batch, in the shape its result
/// register's class fixes. A consumer reads the class off the value it was
/// handed, so it never asks the program and never carries the answer alongside.
pub enum ExprResults {
    Scalar(Vec<Option<i64>>),
    /// Every row's bytes concatenated, addressed by `(offset, length)`; `None`
    /// for SQL NULL, which contributes no bytes.
    Str {
        bytes: Vec<u8>,
        spans: Vec<Option<(usize, usize)>>,
    },
}

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
/// The three differ only in the [`Role`] they name, which fixes both the extra
/// rules and the resolution.
impl LogicalProgram {
    /// A filter predicate: checked against the schema it reads, and required to
    /// own a non-string result register. Resolved with `result_reg` eligible for
    /// the bit_only path, which [`Evaluator::filter_ranges`] reads as packed
    /// bits.
    pub fn resolve_filter(self, schema: &dyn SchemaFacts) -> Result<Evaluator, ExprValidateErr> {
        self.into_evaluator(schema, Role::Filter)
    }

    /// A map: checked against both the schema it reads and the one it writes,
    /// so every declared output payload slot is covered exactly once.
    pub fn resolve_map(
        self,
        in_schema: &dyn SchemaFacts,
        out_schema: &dyn SchemaFacts,
    ) -> Result<Evaluator, ExprValidateErr> {
        self.into_evaluator(in_schema, Role::Map(out_schema))
    }

    /// A scalar expression read back through [`Evaluator::eval_all`] — a DML SET
    /// right-hand side. Same checks as a map minus the output plan, plus the
    /// filter's rule that the program must own a result register, which is what
    /// the result is read out of.
    ///
    /// `result_reg` is not marked as a filter's, so it is excluded from
    /// `bool_input` and a bare non-boolean result has no `bool_bits` bit — which
    /// is why a predicate must go through [`Self::resolve_filter`] instead, and
    /// why [`Evaluator::filter_ranges`] refuses an evaluator resolved here. A
    /// boolean-valued RHS (`SET flag = a AND b`) reads back off the `regs` lane
    /// like any other: `analyze` puts a non-filter `result_reg` in
    /// `non_bool_read`, so it is never bit_only and its producer unpacks.
    pub fn resolve_scalar(self, schema: &dyn SchemaFacts) -> Result<Evaluator, ExprValidateErr> {
        self.into_evaluator(schema, Role::Scalar)
    }

    fn into_evaluator(self, schema: &dyn SchemaFacts, role: Role<'_>) -> Result<Evaluator, ExprValidateErr> {
        self.validate_for(schema, role)?;
        let prog = self.resolve_program(schema, role);
        let scratch = RefCell::new(EvalScratch::new(&prog));
        Ok(Evaluator { prog, scratch })
    }
}

impl Evaluator {
    /// The one morsel-chunking loop, behind every drive method below: evaluate
    /// `start..start + n` a morsel at a time, calling `per_morsel(scratch, bufs,
    /// rel_start, m)` after each — `rel_start` is the offset from `start`.
    ///
    /// The scratch is dereferenced once and the string column regions resolved
    /// once, rather than per morsel: at opt-level=0 each `&mut scratch` through
    /// the `RefCell` guard is an out-of-line `RefMut::deref_mut` call.
    fn drive(
        &self,
        mb: &dyn BatchView,
        start: usize,
        n: usize,
        mut per_morsel: impl FnMut(&mut EvalScratch, StrBufs<'_>, usize, usize),
    ) {
        debug_assert!(
            start + n <= mb.row_count(),
            "drive window {start}..{} is past the batch's end",
            start + n,
        );
        let scratch = &mut *self.scratch.borrow_mut();
        with_str_bufs(&self.prog, mb, |bufs| {
            for rel_start in (0..n).step_by(MORSEL) {
                let m = MORSEL.min(n - rel_start);
                eval_batch(&self.prog, mb, bufs, start + rel_start, m, scratch);
                per_morsel(scratch, bufs, rel_start, m);
            }
        });
    }

    /// Drive `n` rows starting at `start`, one morsel at a time, calling
    /// `f(rel_start, out)` per morsel, where `out` reads the morsel's results
    /// out of the register file. One closure call per *morsel*, not per row.
    ///
    /// `f` stays an `impl FnMut` where the batch is a `&dyn`: a `&mut dyn FnMut`
    /// would force the `MorselOut` to materialise and stop the emit lists
    /// hoisting out of the morsel loop.
    pub fn eval_morsels(&self, mb: &dyn BatchView, start: usize, n: usize, mut f: impl FnMut(usize, &MorselOut<'_>)) {
        self.drive(mb, start, n, |scratch, bufs, rel_start, m| {
            f(rel_start, &scratch.morsel_out(bufs, m))
        });
    }

    /// The verbatim column moves a map materializes columnar-side, as
    /// `(source locator, output payload slot, destination write width)`. A slice
    /// rather than an iterator: at `opt-level=0` the iterator's `next` would be
    /// an out-of-line call per (range × move).
    pub fn copies(&self) -> &[(ColumnLocator, u32, u8)] {
        &self.prog.copies
    }

    /// The scalar-register emits: one bulk copy of the register image each,
    /// or its low bytes into a narrower slot.
    pub fn scalar_emits(&self) -> &[(u16, u32, u8)] {
        &self.prog.scalar_emits
    }

    /// The string-register emits: a German-string cell encoded per row each.
    pub fn str_emits(&self) -> &[(u16, u32)] {
        &self.prog.str_emits
    }

    /// True iff the program writes any output slot out of the register file, so
    /// driving the kernel can change the output. A pure projection emits
    /// nothing.
    pub fn emits_anything(&self) -> bool {
        !self.scalar_emits().is_empty() || !self.str_emits().is_empty()
    }

    /// How a map moves a copied column's null bit — the copy list and the
    /// schema's nullable slots, resolved into one permutation.
    pub fn null_perm(&self) -> &NullPerm {
        &self.prog.null_perm
    }

    /// Run as a filter over all of `mb`'s rows, collecting each maximal
    /// contiguous run of passing rows into `out` (cleared first, and reusable
    /// across chunks). The bitmap stays inside the scratch. `[(0, n)]` is the
    /// agreed spelling of "no predicate".
    pub fn filter_ranges(&self, mb: &dyn BatchView, out: &mut Vec<(usize, usize)>) {
        assert!(
            self.prog.is_filter(),
            "filter_ranges on an evaluator that did not resolve as a filter: only \
             `resolve_filter` forces the result register into `bool_pack`, so no \
             `bool_bits` word is written for it and every row would read as failing",
        );
        out.clear();
        let n = mb.row_count();
        self.scratch.borrow_mut().ensure_filter_words(n);
        self.drive(mb, 0, n, |scratch, _, morsel_start, m| {
            scratch.write_filter_words(&self.prog, morsel_start, m)
        });
        scan_filter_bits(self.scratch.borrow().filter_words(n), n, out);
    }

    /// Evaluate every row of `mb` in one morsel-at-a-time pass, where
    /// `row_count()` single-row drives would each pay `eval_batch`'s
    /// straight-line prologue for one row.
    pub fn eval_all(&self, mb: &dyn BatchView) -> ExprResults {
        assert!(
            !self.prog.is_filter(),
            "eval_all on a filter-resolved evaluator: its result register may be bit_only, \
             whose producer skips the unpack, so the `regs` lane holds no value to read back",
        );
        let n = mb.row_count();
        if !self.prog.result_is_str() {
            let mut vals = Vec::with_capacity(n);
            self.drive(mb, 0, n, |scratch, _, _, m| {
                scratch.append_result_values(&self.prog, m, &mut vals)
            });
            return ExprResults::Scalar(vals);
        }
        let r = self.prog.result_reg as usize;
        let (mut bytes, mut spans) = (Vec::new(), Vec::with_capacity(n));
        self.drive(mb, 0, n, |scratch, bufs, _, m| {
            let out = scratch.morsel_out(bufs, m);
            // Copy every row, then blank the NULL ones — the two-pass shape every
            // emit uses, since a per-row nullness branch would need a `null_bits`
            // that the `no_nulls` arm does not allocate.
            let first = spans.len();
            for i in 0..m {
                let b = out.str_bytes(r, i);
                spans.push(Some((bytes.len(), b.len())));
                bytes.extend_from_slice(b);
            }
            out.for_each_null_row(r, |i| spans[first + i] = None);
        });
        ExprResults::Str { bytes, spans }
    }

    /// Whether the result register holds a string, i.e. which arm
    /// [`Self::eval_all`] will hand back. Resolution knows the answer, so a
    /// caller deciding what a program may be assigned to never re-derives it.
    pub fn result_is_str(&self) -> bool {
        self.prog.result_is_str()
    }

    /// Whether a [`ExprResults::Scalar`] value is a `u64` bit pattern rather than
    /// an `i64`: the resolve-time U64 tracking of the result register.
    pub fn result_is_u64(&self) -> bool {
        self.prog.result_is_u64()
    }

    /// Move this evaluator onto the nullable arm, rebuilding the scratch — which
    /// [`EvalScratch::new`] sizes for one arm, so flipping the flag alone would
    /// leave the null buffers unallocated. Behind `test_support::both_arms`.
    #[cfg(test)]
    pub(crate) fn force_nullable_arm(&mut self) {
        self.prog.no_nulls = false;
        *self.scratch.get_mut() = EvalScratch::new(&self.prog);
    }
}

#[cfg(test)]
#[path = "tests/eval.rs"]
mod tests;
