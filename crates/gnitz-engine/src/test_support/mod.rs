//! Test helpers, split by how far they may reach.
//!
//! [`shared`] is the half `gnitz-server`'s tests also use; it is compiled a
//! second time, from that one source, as the whole of `gnitz-engine-testkit`.
//! [`internal`] is this crate's own, and may name crate-internals.
//!
//! **The split is what keeps a test helper from widening the library.** A
//! helper in `shared` sees only `gnitz-engine`'s public API, so one that
//! reaches an internal fails to build in the testkit. Putting an engine-only
//! helper there instead resolves that pressure the other way — by publishing
//! the internal — which is how `Batch::extend_pk_opk`, `encode_leading_opk` and
//! `SchemaDescriptor::pk_columns` each briefly became permanent public API for
//! helpers no other crate calls. Add a helper to `internal` unless
//! `gnitz-server` needs it.
//!
//! Both are re-exported here, so every call site names `crate::test_support::X`
//! and moving a helper between them touches no test.

pub mod internal;
pub mod shared;

pub use internal::*;
pub use shared::*;
