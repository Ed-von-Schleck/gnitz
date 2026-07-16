//! Physical-encoding substrate shared by the compile and execute sides: SQL
//! literal parsing, PK/seek key packing, column-value writing, and
//! projection-layout resolution. Carving these out keeps the partition-routing
//! rules (`pk_codec`) in one place, so a later verb-split cannot reintroduce
//! routing divergence.

pub(crate) mod colwrite;
pub(crate) mod pk_codec;
pub(crate) mod project_schema;
