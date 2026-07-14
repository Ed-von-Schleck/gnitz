pub mod codec;
pub mod error;
pub mod header;
pub mod message;
pub mod transport;
pub mod types;
pub mod wal_block;

pub use codec::{batch_to_schema, schema_to_batch};
pub use error::ProtocolError;
pub use header::{
    wire_flags_get_conflict_mode, wire_flags_get_index_version, wire_flags_get_schema_version,
    wire_flags_set_conflict_mode, wire_flags_set_index_version, wire_flags_set_schema_version, Header,
    WireConflictMode, FLAG_ALLOCATE_INDEX_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_SERIAL_RANGE,
    FLAG_ALLOCATE_TABLE_ID, FLAG_CONTINUATION, FLAG_DDL_SYNC, FLAG_DDL_TXN, FLAG_EXCHANGE, FLAG_GET_INDICES,
    FLAG_HAS_DATA, FLAG_HAS_PK, FLAG_HAS_SCHEMA, FLAG_PUSH, FLAG_PUSH_TXN, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
    FLAG_SEEK_BY_INDEX_RANGE, FLAG_SHUTDOWN, IPC_CONTROL_TID, MAX_COLUMNS, META_FLAG_IS_PK, META_FLAG_NULLABLE,
    STATUS_ERROR, STATUS_NO_INDEX, STATUS_OK, STATUS_SCHEMA_MISMATCH, STATUS_TXN_CONFLICT, WAL_BLOCK_HEADER_SIZE,
};
pub use message::{
    decode_control_block, encode_control_block, encode_ddl_txn, encode_message, encode_message_noschema_parts,
    encode_message_parts, encode_push_txn, encode_schema_block, parse_response, recv_message, send_message,
    send_message_noschema, send_message_with_extra, Message, MessageParts,
};
pub use transport::{hello_handshake, ClientTransport, FrameSegments, TransportWaker, FRAME_SEGMENTS};
pub use types::{
    meta_schema, BatchAppender, ColData, ColumnDef, FixedInt, PkColumn, PkTuple, Schema, TypeCode, ZSetBatch,
    MAX_PK_BYTES, MAX_PK_COLUMNS, PK_LIST_MAX_COLS,
};
pub use wal_block::{decode_wal_block, decode_wal_block_verified, encode_wal_block};
