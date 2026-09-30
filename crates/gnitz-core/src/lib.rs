//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.
//!
//! `src/tests/` holds those; the crate-root `tests/` beside `src/` is the
//! integration suite, gated on the `integration` feature.

mod client;
mod connection;
mod error;
mod mirror;
mod protocol;
#[cfg(test)]
mod test_support;

pub use client::{
    key_reply, not_found, qualified_name, retraction_batch, segment_id, GnitzClient, InlineForeignKey,
    InlineUniqueIndex, ParkHook, PlannedView, ViewBundle, MAX_CHAIN_SEGMENTS, RMW_MAX_ATTEMPTS,
};
pub use connection::{
    Completions, DeltaCursor, IdRun, Interest, RelDescriptor, RelTarget, Reply, Request, ScanReply, ScanResult,
    Session, SlotId, MAX_IN_FLIGHT,
};
pub use error::ClientError;
pub use mirror::{Invalidate, MirrorError, MirrorStore, PollOutcome, PollResult};
pub use protocol::error::ProtocolError;
pub use protocol::message::{encode_ddl_txn, PushFamily};
pub(crate) use protocol::message::{encode_frame, encode_push_txn};
pub(crate) use protocol::transport::{hello_handshake, ClientTransport};
pub use protocol::types::{
    push_zero_cell, sys_schema, BatchAppender, BatchMark, FkTarget, PayloadColumn, PkColumn, Schema, ZSetBatch,
};
pub use protocol::wal_block::decode_regions_into;
