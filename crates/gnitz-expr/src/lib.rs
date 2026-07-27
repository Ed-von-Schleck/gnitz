//! The one expression evaluator GnitzDB runs — on the engine and in the SQL
//! client alike.
//!
//! A leaf crate below the entire engine: its only dependency is `gnitz-wire`
//! (type codes, the OPK codec, the German-string layout), so both the server
//! and the client-side planner can link it without pulling in storage, the
//! catalog, or the runtime. Whatever the engine computes for an expression, the
//! client computes bit-for-bit, because it is the same code.
//!
//! The crate is organized into topic modules with every item re-exported flat at
//! the crate root (`gnitz_expr::FOO`), matching `gnitz-wire`'s leaf-crate shape.
//!
//! What lives here is the whole path from a wire expression blob to evaluated
//! values: [`LogicalProgram`] (the wire-mirroring form), its resolution against a
//! schema, and the morsel-oriented vectorized kernels that evaluate the resolved
//! form — plus the two contracts they read through, [`ColumnLocator`] /
//! [`RowSource`] / [`BatchView`] for *where a value physically sits* and
//! [`SchemaFacts`] for *what the schema says about it*.
//!
//! # Inlining
//!
//! **Every per-row item here is `#[inline(always)]`, and a plain `#[inline]` is
//! never used.** The workspace declares only `[profile.release]` (opt 3, LTO)
//! and `[profile.dev]` (opt 0) — there is no `[profile.test]`/`[profile.bench]`,
//! so no build of this crate ever runs at "optimized without LTO", the one
//! setting where a plain `#[inline]` changes anything. Release inlines across
//! the LTO boundary regardless of the hint; at `opt-level=0` LLVM runs no
//! inliner at all except the always-inline pass, so `#[inline]` is a no-op and
//! the callee stays a real call — and the debug server binary is what the entire
//! E2E suite runs. A hint that is either redundant or inert in every profile is
//! decoration, so an item that runs per row (or per instruction per morsel)
//! carries `#[inline(always)]` and everything else carries nothing.
//!
//! **Frequency is not the whole rule: what gets inlined is the *test*, not the
//! body.** A per-row item that is a guard, a forwarder, or a two-arm dispatch
//! folds into its caller. A per-row item whose body is a *loop* stays out of
//! line behind an always-inlined guard, because at `-O0` there is no inliner to
//! undo the duplication and the caller's frame pays for every call site.
//! `EvalScratch::ensure_capacity` (always-inlined length check, `#[cold]
//! #[inline(never)] grow`) and `maybe_pack_bool_bits` (always-inlined
//! `no_nulls`/`needs_bool_pack` test, out-of-line `pack_to_bool_bits`) are the
//! two instances: promoting the bodies too nearly doubled `eval_batch`'s `-O0`
//! instruction count.
//!
//! `-O0` wall-clock on the expr benches here swings well over 30% run to run, so
//! judge an inlining change on `eval_batch`'s instruction count and call
//! targets, not on a timing delta.
//!
//! One deliberate exception: `for_each_null_row` is a nested loop and is still
//! `#[inline(always)]`. It has two call sites, its out-of-line copies were about
//! as large as the splice, and inlining removes a call per instruction per
//! morsel — so the frame-size argument above does not bite.

mod batch;
mod eval;
mod locator;
mod program;
mod schema_facts;
mod view;

pub use eval::*;
pub use locator::*;
pub use program::*;
pub use schema_facts::*;
pub use view::*;

#[cfg(test)]
mod test_support;
