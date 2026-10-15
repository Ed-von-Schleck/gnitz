//! `gnitz`, the interactive SQL shell: a line editor with highlighting and
//! catalog-aware completion over the SQL front end, psql-style meta-commands,
//! and `\watch` over a view's delta feed.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover; the crate-root `tests/` is the integration suite, gated on the
//! `integration` feature.

pub mod catalog;
pub mod complete;
pub mod editor;
pub mod highlight;
pub mod lex;
pub mod meta;
pub mod render;
pub mod shell;
pub mod value;
pub mod watch;
