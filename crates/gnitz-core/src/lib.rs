#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

pub mod circuit;
pub mod client;
pub mod connection;
pub mod error;
pub mod protocol;
pub mod types;

// The crate prelude: the names a client of gnitz-core names by hand. Every
// module is `pub`, so anything omitted here is still reachable by its own path —
// this list carries no meaning beyond "spelled often enough to be worth
// shortening".
pub use circuit::{
    agg_output_type, Circuit, CircuitBuilder, CircuitRows, NodeId, OpNode, RangeRel, ReduceOutKey, ReindexRole,
};
pub use client::{
    hidden_view_name, retraction_batch, GnitzClient, IndexMeta, InlineUniqueIndex, PlannedView, RelKind, TxnBuffer,
    MAX_CHAIN_SEGMENTS,
};
pub use connection::{Session, FIRST_USER_TABLE_ID, TABLE_TAB};
pub use error::{ClientError, ConflictClass};
pub use gnitz_wire::{RelClass, TableProps};
// The expression emitter and the resolved column addressing it reads through
// live in `gnitz-expr`, beside the decoder they must agree with. Re-exported
// here because `gnitz-capi` exposes 18 `gnitz_expr_*` C symbols over the former
// and reads batch cells through the latter, without depending on that crate.
pub use gnitz_expr::{CmpOp, ColumnLocator, ExprBuilder, ExprProgram, SchemaFacts, StrOp};
pub use gnitz_wire::{
    index_key_types, validate_dist_prefix, validate_user_identifier, Cut, PkColList, RangeDescriptor, FK_INDEX_INFIX,
};
pub use protocol::{
    encode_message_parts, null_word_get, null_word_set, wire_flags_get_schema_version, wire_flags_set_conflict_mode,
    BatchAppender, ColData, ColumnDef, FixedInt, MessageParts, PkColumn, PkTuple, ProtocolError, Schema,
    TransportWaker, TypeCode, ViewBuffers, WireConflictMode, ZSetBatch, ZSetBatchView, FLAG_PUSH, MAX_COLUMNS,
    MAX_PK_BYTES, PK_LIST_MAX_COLS,
};
