//! The one expression evaluator GnitzDB runs — on the engine and in the SQL
//! client alike.
//!
//! A leaf crate below the entire engine: its only in-workspace dependency is
//! `gnitz-wire` (type codes, the OPK codec, the German-string layout), so both
//! the server and the client-side planner can link it without pulling in
//! storage, the catalog, or the runtime. Its one external crate, `memchr`, is
//! `no_std` and dependency-free and supplies the substring scan behind LIKE.
//! Whatever the engine computes for an expression, the client computes
//! bit-for-bit, because it is the same code.
//!
//! The crate is organized into topic modules with every item re-exported flat at
//! the crate root (`gnitz_expr::FOO`), matching `gnitz-wire`'s leaf-crate shape.
//!
//! What lives here is the whole path from a wire expression blob to evaluated
//! values: [`ExprBuilder`] (which assembles the program), [`LogicalProgram`]
//! (the wire-mirroring form, which both encodes to a blob and decodes back from
//! one), its resolution against a schema, and
//! the morsel-oriented vectorized kernels that evaluate the resolved form — plus
//! the two contracts they read through, [`ColumnLocator`] / [`RowSource`] /
//! [`BatchView`] for *where a value physically sits* and [`SchemaFacts`] for
//! *what the schema says about it*.
//!
//! `LogicalInstr::to_wire` and `LogicalProgram::decode_instr` are two tables over
//! [`ExprOp`]; the vocabulary and both tables live in `program.rs`, and
//! `tests/program.rs`'s drift tests check them against each other. `gnitz-wire`
//! keeps only the framing, which counts those words without interpreting one.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items. `tests/bench.rs` is the exception: its `#[ignore]`d
//! retired-instruction benches span several modules' kernels, so it attaches at
//! the crate root instead.
//!
//! # Inlining
//!
//! This crate builds at `opt-level = 1` even in dev (`[profile.dev.package]` in
//! the workspace manifest), but that profile follows the **codegen unit**, not
//! the source file:
//!
//! > A non-generic item is codegen'd in this crate's rlib, at opt-level 1. A
//! > generic one is re-instantiated in the *consuming* crate, at that crate's
//! > opt-level — 0 for gnitz-store and gnitz-server in dev.
//!
//! So moving a body out of a generic function into a non-generic one *improves*
//! the debug build, and the reverse costs. `nm -C` on the rlibs is what shows
//! which: a generic body appears as a local (`t`) symbol in each consuming
//! crate's. `&dyn BatchView` on [`Evaluator`]'s drive methods shrinks that set
//! but does not empty it: `Evaluator::drive`, its closure,
//! [`Evaluator::eval_morsels`] and `batch::with_str_bufs` are all defined in
//! `libgnitz_store` at its opt-level 0. Anything reached per row from another
//! crate is `#[inline(always)]` regardless — [`ColumnLocator`]'s methods and
//! [`MorselOut`]'s, and `RowSource::row_count`.
//!
//! Judge an inlining or kernel change on retired instructions
//! (`perf stat -e instructions:u`), never on wall-clock: timings on the
//! development machines swing far wider than the effects being measured.

mod batch;
mod builder;
pub mod calendar;
mod chars;
mod eval;
mod like;
mod locator;
mod program;
mod schema_facts;
mod view;

pub use batch::MorselOut;
pub use builder::*;
pub use calendar::CalendarOp;
pub use eval::*;
pub use like::*;
pub use locator::*;
pub use program::*;
pub use schema_facts::*;
pub use view::*;

#[cfg(test)]
mod test_support;

#[cfg(test)]
#[path = "tests/bench.rs"]
mod bench;
