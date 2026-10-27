//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.
//!
//! `src/tests/` holds those; the crate-root `tests/` beside `src/` is the
//! integration suite, gated on the `integration` feature.

mod catalog;
mod client;
mod connection;
mod error;
pub mod local_server;
mod mirror;
mod protocol;
mod pushed;
pub mod recording;
mod rel_name;
mod snapshot;
#[cfg(test)]
mod test_support;
mod view_def;

pub use catalog::{Catalog, CatalogRelation, IndexRow, RelProps};
pub use client::{
    block_on, key_reply, not_found, segment_id, serve, BlockingHost, BoxFut, FkTarget, GnitzClient, Host,
    InlineUniqueIndex, Job, Op, ParkHook, Pending, PlannedView, TableDef, ViewBundle, RMW_MAX_ATTEMPTS,
};
pub use connection::{
    promise, DeltaCursor, Interest, Promise, RelDescriptor, Request, ScanReply, Sent, Session, SnapshotPage, Target,
    MAX_IN_FLIGHT, MAX_QUEUED_BYTES,
};
pub use error::{ClientError, ProtocolError};
pub use mirror::{MirrorError, MirrorStore, Planner, PollOutcome, PollResult, Subscription};
pub use protocol::message::{encode_ddl_txn, PushFamily};
pub(crate) use protocol::message::{encode_frame, encode_push_txn};
pub use protocol::transport::ClientTransport;
pub use protocol::types::{
    push_zero_cell, sys_schema, BatchAppender, BatchMark, PayloadColumn, PkColumn, Schema, ZSetBatch,
};
pub use protocol::wal_block::append_own_regions;
pub use pushed::{Pushed, Synced};
pub use rel_name::RelName;
pub use snapshot::DEFAULT_PAGE_ROWS;
pub use view_def::{RelationRef, ViewDefinition};
