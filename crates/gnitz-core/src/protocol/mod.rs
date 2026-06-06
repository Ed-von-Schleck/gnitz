pub mod error;
pub mod types;
pub mod header;
pub mod codec;
pub mod transport;
pub mod message;
pub mod wal_block;

pub use error::ProtocolError;
pub use types::{TypeCode, ColumnDef, PkColumn, PkTuple, Schema, ColData, ZSetBatch, BatchAppender, meta_schema,
    MAX_PK_BYTES, MAX_PK_COLUMNS, PK_LIST_MAX_COLS};
pub use header::{
    Header,
    FLAG_ALLOCATE_TABLE_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_SHUTDOWN,
    FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_PUSH, FLAG_HAS_PK, FLAG_SEEK,
    FLAG_SEEK_BY_INDEX, FLAG_ALLOCATE_INDEX_ID,
    WireConflictMode,
    wire_flags_set_conflict_mode, wire_flags_get_conflict_mode,
    wire_flags_set_schema_version, wire_flags_get_schema_version,
    FLAG_HAS_SCHEMA, FLAG_HAS_DATA, FLAG_CONTINUATION,
    STATUS_OK, STATUS_ERROR, STATUS_SCHEMA_MISMATCH, META_FLAG_NULLABLE, META_FLAG_IS_PK,
    WAL_BLOCK_HEADER_SIZE, IPC_CONTROL_TID,
    MAX_COLUMNS,
};
pub use wal_block::{
    encode_wal_block, decode_wal_block, VerifyChecksum,
    recompute_block_checksum, get_region_offset_size,
};
pub use codec::{schema_to_batch, batch_to_schema};
pub use transport::{
    connect, connect_unix, send_framed, send_framed_iov, send_framed_batch, recv_framed, close_fd,
    hello_handshake,
};
pub use message::{
    Message, send_message, send_message_noschema, recv_message, encode_message,
    parse_response, encode_control_block, decode_control_block,
};
