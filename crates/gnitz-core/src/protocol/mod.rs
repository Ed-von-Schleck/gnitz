//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

pub(crate) mod error;
pub(crate) mod message;
pub(crate) mod transport;
pub(crate) mod types;
pub(crate) mod wal_block;
