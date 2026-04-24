pub mod error;
pub mod types;
pub mod header;
pub mod codec;
pub mod transport;
pub mod message;
pub mod wal_block;

pub use error::ProtocolError;
pub use types::{TypeCode, ColumnDef, PkColumn, Schema, ColData, ZSetBatch, BatchAppender, meta_schema};
pub use header::{
    Header,
    FLAG_ALLOCATE_TABLE_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_SHUTDOWN,
    FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_PUSH, FLAG_HAS_PK, FLAG_SEEK,
    FLAG_SEEK_BY_INDEX, FLAG_ALLOCATE_INDEX_ID,
    FLAG_CONFLICT_MODE_PRESENT, WireConflictMode,
    FLAG_HAS_SCHEMA, FLAG_HAS_DATA,
    STATUS_OK, STATUS_ERROR, META_FLAG_NULLABLE, META_FLAG_IS_PK,
    WAL_BLOCK_HEADER_SIZE, IPC_CONTROL_TID,
};
pub use wal_block::{
    encode_wal_block, decode_wal_block,
    recompute_block_checksum, get_region_offset_size,
};
pub use codec::{schema_to_batch, batch_to_schema};
pub use transport::{connect, send_framed, send_framed_iov, recv_framed, close_fd};
pub use message::{
    Message, send_message, recv_message, encode_message,
    parse_response, encode_control_block, decode_control_block,
};
