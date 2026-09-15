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
// client's protocol code has one import path for it.
pub use gnitz_wire::{ClientVerb, WireConflictMode, WireFlags, WireStatus, MAX_COLUMNS};
#[cfg(any(test, feature = "integration"))]
pub use message::parse_response;
pub(crate) use message::parse_response_frame;
pub use message::{encode_ddl_txn, encode_frame, encode_push_txn, Message, MessageParts};
// Only the `integration` suite drives a raw transport from outside; a shipped
// build keeps it crate-private. The cfgs are complementary because two `use`
// statements binding one name is `E0252` whatever their visibility.
#[cfg(any(test, feature = "integration"))]
pub use transport::{hello_handshake, set_sockopt_int, ClientTransport};
#[cfg(not(any(test, feature = "integration")))]
pub(crate) use transport::{hello_handshake, ClientTransport};
pub use types::{
    native_le_key, opk_key_cols, push_zero_cell, BatchAppender, ColType, ColumnDef, FixedInt, FkTarget, PayloadColumn,
    PkBuf, PkColumn, ReduceOutKey, ScalarKind, Schema, TypeCode, ZSetBatch, MAX_PK_BYTES, PK_LIST_MAX_COLS,
};
pub use wal_block::{decode_regions_into, decode_wal_block};
