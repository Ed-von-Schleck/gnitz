//! Physical-encoding substrate shared by the compile and execute sides: what a
//! literal means against a column type (`literal`), column-value writing, and
//! projection-layout resolution.

pub(crate) mod colwrite;
pub(crate) mod literal;
pub(crate) mod project_schema;
