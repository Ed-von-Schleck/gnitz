//! Physical-encoding substrate shared by the compile and execute sides: what a
//! literal means against a column type (`literal`), column-value writing, and
//! projection-layout resolution.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

pub(crate) mod colwrite;
pub(crate) mod literal;
pub(crate) mod project_schema;
