#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

pub mod circuit;
pub mod client;
pub mod connection;
pub mod error;
pub mod protocol;
pub mod read_target;
pub mod types;

// The crate prelude: the names a client of gnitz-core names by hand. Every
// module is `pub`, so anything omitted here is still reachable by its own path —
// this list carries no meaning beyond "spelled often enough to be worth
// shortening".
pub use circuit::{
    agg_output_type, is_segment_id, segment_id, Circuit, CircuitBuilder, CircuitRows, NodeId, OpNode, RangeRel,
    ReduceOutKey, ReindexRole,
};
pub use client::{
    delta_reply_schema, hidden_view_name, qualified_name, retraction_batch, CatalogSnapshot, DeltaCursor, GnitzClient,
    IndexMeta, InlineUniqueIndex, PlannedView, RelDescriptor, TxnBuffer, ViewName, MAX_CHAIN_SEGMENTS,
};
pub use connection::{
    Completions, Interest, ParkHook, RawBlock, Reply, ReplyTrain, Request, Session, SlotId, FIRST_USER_TABLE_ID,
    MAX_IN_FLIGHT, TABLE_TAB,
};
pub use error::{ClientError, ConflictClass};
pub use gnitz_wire::{
    index_key_types, validate_dist_prefix, validate_user_identifier, Cut, PkColList, RangeDescriptor, RelClass,
    TableProps,
};
pub use protocol::{
    encode_message_parts, null_word_get, null_word_set, wire_flags_get_schema_version, wire_flags_set_conflict_mode,
    BatchAppender, ColData, ColumnDef, FixedInt, MessageParts, PkColumn, PkTuple, ProtocolError, Schema,
    TransportWaker, TypeCode, ViewBuffers, WireConflictMode, ZSetBatch, ZSetBatchView, FLAG_PUSH, MAX_COLUMNS,
    MAX_PK_BYTES, PK_LIST_MAX_COLS,
};
pub use read_target::ReadTarget;
