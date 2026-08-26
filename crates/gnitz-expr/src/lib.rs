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
//! values: [`ExprBuilder`] (which emits the blob), [`LogicalProgram`] (the
//! wire-mirroring form it decodes back to), its resolution against a schema, and
//! the morsel-oriented vectorized kernels that evaluate the resolved form — plus
//! the two contracts they read through, [`ColumnLocator`] / [`RowSource`] /
//! [`BatchView`] for *where a value physically sits* and [`SchemaFacts`] for
//! *what the schema says about it*.
//!
//! `LogicalInstr::to_wire` and `LogicalProgram::decode_quad` are two tables over
//! `gnitz_wire::ExprOp`; both live in `program.rs`, and `program/tests.rs`'s
//! drift tests are what check them against each other.
//!
//! # Inlining
//!
//! This crate builds at `opt-level = 1` even in dev (`[profile.dev.package]` in
//! the workspace manifest), but that profile follows the **codegen unit**, not
//! the source file:
//!
//! > A non-generic item is codegen'd in this crate's rlib, at opt-level 1. A
//! > generic one is re-instantiated in the *consuming* crate, at that crate's
//! > opt-level — 0 for gnitz-engine in dev.
//!
//! So moving a body out of a generic function into a non-generic one *improves*
//! the debug build, and the reverse costs. `nm` on the debug server is what
//! shows which: a generic body appears there as a local (`t`) symbol, once per
//! consuming crate. Anything reached from another crate per row is annotated
//! `#[inline(always)]` regardless, since its caller is an opt-0 codegen unit:
//! [`ColumnLocator`]'s methods, [`MorselOut`]'s, `RowSource::row_count`, and the
//! generic drive methods' own preamble.
//!
//! Judge an inlining or kernel change on retired instructions
//! (`perf stat -e instructions:u`), never on wall-clock: timings on the
//! development machines swing far wider than the effects being measured.

mod batch;
mod builder;
mod chars;
mod eval;
mod like;
mod locator;
mod program;
mod schema_facts;
mod view;

pub use batch::MorselOut;
pub use builder::*;
pub use eval::*;
pub use like::*;
pub use locator::*;
pub use program::*;
pub use schema_facts::*;
pub use view::*;

#[cfg(test)]
mod test_support;
