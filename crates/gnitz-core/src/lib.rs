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
    agg_output_type, segment_id, Circuit, CircuitBuilder, MapKind, NodeId, OpNode, RangeRel, ReindexRole,
};
pub use client::{
    delta_reply_schema, qualified_name, retraction_batch, CatalogSnapshot, DeltaCursor, GnitzClient, IndexMeta,
    InlineUniqueIndex, ParkHook, PlannedView, TxnBuffer, TxnReads, MAX_CHAIN_SEGMENTS,
};
pub use connection::{
    Completions, Interest, RawBlock, RelDescriptor, RelTarget, Reply, Request, ScanReply, Session, SlotId,
    MAX_IN_FLIGHT, MAX_QUEUED_BYTES,
};
pub use error::ClientError;
pub use gnitz_wire::{
    validate_dist_prefix, validate_user_identifier, Cut, PkColList, RangeDescriptor, ReindexSlot, RelClass, TableProps,
    FIRST_USER_TABLE_ID, TABLE_TAB,
};
pub use mirror::{Invalidate, MirrorError, MirrorStore, PollOutcome, PollResult, Shape};
pub use protocol::{
    native_le_key, opk_key_cols, opk_key_packed, push_zero_cell, wire_flags_get_schema_version,
    wire_flags_set_conflict_mode, BatchAppender, ColType, ColumnDef, FixedInt, FkTarget, PayloadColumn, PkBuf,
    PkColumn, ProtocolError, ReduceOutKey, ScalarKind, Schema, TypeCode, WireConflictMode, ZSetBatch, FLAG_PUSH,
    MAX_COLUMNS, MAX_PK_BYTES, PK_LIST_MAX_COLS,
};
