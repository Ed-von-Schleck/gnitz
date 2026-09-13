//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

pub mod codec;
pub mod error;
pub mod message;
pub mod regions;
pub(crate) mod transport;
pub mod types;
pub mod wal_block;

pub use codec::ReplySchema;
pub use error::ProtocolError;
// The wire protocol's own vocabulary, re-exported at the module root so the
// client's protocol code has one import path for it. `Header` is
// `gnitz_wire`'s `ControlHeader`: the control block's seven routing scalars,
// defined once for both sides of the wire.
pub use gnitz_wire::control::ControlHeader as Header;
pub(crate) use gnitz_wire::WAL_HEADER_SIZE as WAL_BLOCK_HEADER_SIZE;
pub use gnitz_wire::{
    wire_flags_get_schema_version, wire_flags_set_conflict_mode, wire_flags_set_schema_version, WireConflictMode,
    FLAG_ALLOCATE_INDEX_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_SERIAL_RANGE, FLAG_ALLOCATE_TABLE_ID,
    FLAG_CONTINUATION, FLAG_DDL_TXN, FLAG_HAS_DATA, FLAG_HAS_SCHEMA, FLAG_PUSH, FLAG_PUSH_TXN, FLAG_RESOLVE,
    FLAG_SCAN_MULTI, FLAG_SCAN_SPEC, FLAG_SEEK, FLAG_SEEK_BY_INDEX, MAX_COLUMNS, STATUS_DELTA_EXPIRED, STATUS_ERROR,
    STATUS_NOT_FOUND, STATUS_NO_INDEX, STATUS_OK, STATUS_SAL_FULL, STATUS_SCHEMA_MISMATCH, STATUS_TXN_CONFLICT,
};
#[cfg(any(test, feature = "integration"))]
pub use message::parse_response;
pub use message::{
    decode_control_block, encode_ddl_txn, encode_message_noschema_parts, encode_message_parts, encode_push_txn,
    Message, MessageParts,
};
pub(crate) use message::{encode_control_frame, parse_response_frame};
pub use regions::ZSetBatchView;
// Only the `integration` suite drives a raw transport from outside; a shipped
// build keeps it crate-private. The cfgs are complementary because two `use`
// statements binding one name is `E0252` whatever their visibility.
#[cfg(any(test, feature = "integration"))]
pub use transport::{hello_handshake, ClientTransport};
#[cfg(not(any(test, feature = "integration")))]
pub(crate) use transport::{hello_handshake, ClientTransport};
pub use types::{
    native_le_key, opk_key_cols, opk_key_packed, push_zero_cell, BatchAppender, ColType, ColumnDef, FixedInt, FkTarget,
    PkBuf, PkColumn, ReduceOutKey, ScalarKind, Schema, TypeCode, ZSetBatch, MAX_PK_BYTES, PK_LIST_MAX_COLS,
};
pub use wal_block::decode_wal_block;
