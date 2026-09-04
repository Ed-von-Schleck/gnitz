//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.
//!
//! `src/tests/` holds those; the crate-root `tests/` beside `src/` is the
//! integration suite, gated on the `integration` feature.

#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

pub mod circuit;
pub mod client;
pub mod connection;
pub mod error;
pub mod mirror;
pub mod protocol;
#[cfg(test)]
mod test_support;
pub mod types;

// The crate prelude: the names a client of gnitz-core names by hand. Every
// module is `pub`, so anything omitted here is still reachable by its own path —
// this list carries no meaning beyond "spelled often enough to be worth
// shortening".
pub use circuit::{
    agg_output_type, segment_id, Circuit, CircuitBuilder, NodeId, OpNode, RangeRel, ReduceOutKey, ReindexRole,
};
pub use client::{
    delta_reply_schema, qualified_name, retraction_batch, CatalogSnapshot, DeltaCursor, GnitzClient, IndexMeta,
    InlineUniqueIndex, PlannedView, RelDescriptor, TxnBuffer, TxnReads, MAX_CHAIN_SEGMENTS,
};
pub use connection::{
    Completions, Interest, LocalScanReply, ParkHook, RawBlock, RelTarget, Reply, ReplyTrain, Request, ScanReply,
    Session, SlotId, FIRST_USER_TABLE_ID, MAX_IN_FLIGHT, MAX_QUEUED_BYTES, TABLE_TAB,
};
pub use error::{ClientError, ConflictClass};
pub use gnitz_wire::{
    validate_dist_prefix, validate_user_identifier, Cut, PkColList, RangeDescriptor, RelClass, TableProps,
};
pub use mirror::{Invalidate, MirrorError, MirrorStore, PollOutcome, PollResult, Shape};
pub use protocol::{
    null_word_get, null_word_set, wire_flags_get_schema_version, wire_flags_set_conflict_mode, BatchAppender, ColData,
    ColumnDef, FixedInt, PkColumn, PkTuple, ProtocolError, ScalarKind, Schema, TypeCode, ViewBuffers, WireConflictMode,
    ZSetBatch, ZSetBatchView, FLAG_PUSH, MAX_COLUMNS, MAX_PK_BYTES, PK_LIST_MAX_COLS,
};
