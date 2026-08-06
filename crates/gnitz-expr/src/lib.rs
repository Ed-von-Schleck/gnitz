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
//! This crate builds at `opt-level = 1` even in dev (`[profile.dev.package]` in
//! the workspace manifest), so its own helpers inline without being annotated.
//!
//! The two types reached from *other* crates per row — [`ColumnLocator`] and
//! [`MorselOut`] — are the exception: gnitz-engine calls them at opt-level 0,
//! where only the always-inline pass runs, so their methods keep
//! `#[inline(always)]`.
//!
//! Judge an inlining or kernel change on retired instructions
//! (`perf stat -e instructions:u`), never on wall-clock: timings on the
//! development machines swing far wider than the effects being measured.

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
