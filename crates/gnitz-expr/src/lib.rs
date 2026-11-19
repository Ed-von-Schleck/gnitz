//! The one expression evaluator GnitzDB runs — on the engine and in the SQL
//! client alike — and the vocabulary it resolves against and reads rows
//! through.
//!
//! A leaf crate below the entire engine: its only in-workspace dependency is
//! `gnitz-wire` (type codes, the OPK codec, the German-string layout, and the
//! bounds-checked byte cursor its blob framing reads through), so both
//! the server and the client-side planner can link it without pulling in
//! storage, the catalog, or the runtime. Its two external crates are
//! dependency-free: `memchr` supplies the substring scan, and `fearless_simd`
//! the vector types and run-time dispatch under the kernels of `simd.rs`.
//! Whatever the engine computes for an expression, the client computes
//! bit-for-bit, because it is the same code.
//!
//! The crate is organized into topic modules with every public item re-exported
//! flat at the crate root (`gnitz_expr::FOO`), matching `gnitz-wire`'s
//! leaf-crate shape.
//!
//! # The evaluator
//!
//! [`ExprBuilder`] assembles a [`LogicalProgram`], the wire form, which encodes
//! to a blob and decodes back from one. Resolved against a schema, it becomes a
//! [`RowFilter`], a [`ScalarEval`] or a [`MapEval`], each running the morsel
//! kernels of `morsel.rs` and `simd.rs`. Each instruction's wire layout is
//! declared on its `LogicalInstr` variant in `program.rs`, from which
//! `blob.rs`'s `expr_instrs!` derives the opcodes and both instruction tables;
//! `blob.rs` also holds the blob framing, and `tests/blob.rs` pins all of it to
//! `EXPR_BLOB_VERSION`. The selector families are declared in
//! `program.rs`, except `CalendarOp`, which `calendar.rs` declares beside the
//! arithmetic it selects. `gnitz-wire` keeps only `EXPR_BLOB_VERSION`, the word
//! its `SYS_SCHEMA_DIGEST` const fold must see — it cannot depend on this crate
//! to reach one.
//!
//! A map's sinks are each a column copied verbatim or a register the program
//! computes; [`LogicalProgram::copy_cols`] builds a map of copies alone, and
//! resolving a map yields its [`MapEval`] beside its copies as [`ColCopy`]s,
//! which `gnitz-zset` carries out. A program carries a LIKE pattern as a
//! [`LikePattern`], compiled per instruction into a [`LikeMatcher`].
//!
//! # Rows
//!
//! The evaluator reads rows through [`BatchView`], a [`gnitz_wire::RowSource`]
//! that also hands out whole regions, addressed by [`ColumnLocator`] (where a
//! value sits), and describes them with [`SchemaDescriptor`] (what the schema
//! says about it). A map writes into a [`MapTarget`]. The batch types implementing
//! them are `gnitz-zset`'s, a crate above this one.
//!
//! # Layout
//!
//! A module's unit tests live in `tests/<module>.rs` and its `#[ignore]`d
//! instruction-count benches in `benches/<module>.rs`, each attached with
//! `#[path]` to the module it covers, so it stays that module's child and reaches
//! its private items. The bolero fuzz target is `fuzz/program.rs`, attached to
//! `program.rs` the same way. The helpers they share are in `test_support.rs`.
//!
//! # Inlining
//!
//! This crate builds at `opt-level = 1` even in dev (`[profile.dev.package]` in
//! the workspace manifest), but that profile follows the **codegen unit**, not
//! the source file:
//!
//! > A non-generic item is codegen'd in this crate's rlib, at opt-level 1. A
//! > generic one is re-instantiated in the *consuming* crate, at that crate's
//! > opt-level — 1 for gnitz-zset, 0 for gnitz-trace, gnitz-store and
//! > gnitz-server in dev.
//!
//! So moving a body out of a generic function into a non-generic one *improves*
//! the debug build, and the reverse costs. `nm -C` on the rlibs is what shows
//! which: a generic body appears as a local (`t`) symbol in each consuming
//! crate's. Anything a generic entry point reaches per row is
//! `#[inline(always)]`, unless it is deliberately out of line.
//!
//! Judge an inlining or kernel change on retired instructions — what the
//! benches print — never on wall-clock: timings on the development machines
//! swing far wider than the effects being measured.

mod blob;
mod builder;
mod calendar;
mod chars;
mod eval;
mod like;
mod locator;
mod map;
mod morsel;
mod program;
mod range;
mod schema;
mod search;
mod simd;
mod view;

pub use builder::*;
pub use calendar::CalendarOp;
pub use eval::*;
pub use like::*;
pub use locator::*;
pub use map::{ColCopy, MapEval};
pub use program::*;
pub use schema::*;
pub use simd::{Level, null_bits};
pub use view::*;

#[cfg(test)]
mod test_support;
