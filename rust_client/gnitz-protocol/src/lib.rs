// gnitz-protocol: wire codec and transport for gnitz IPC v2.
// Populated in Phase 1 (codec) and Phase 2 (transport).

pub mod error;
pub mod types;
pub mod header;
pub mod codec;
pub mod transport;
pub mod message;

pub use error::ProtocolError;
pub use types::{TypeCode, ColumnDef, Schema, ColData, ZSetBatch, meta_schema};
pub use header::{
    Header, MAGIC_V2, HEADER_SIZE, ALIGNMENT,
    FLAG_ALLOCATE_TABLE_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_SHUTDOWN,
    FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_PUSH, FLAG_HAS_PK,
    STATUS_OK, STATUS_ERROR, META_FLAG_NULLABLE, META_FLAG_IS_PK,
    IPC_STRING_STRIDE, IPC_NULL_STRING_OFFSET,
};
pub use codec::{align_up, Layout, layout, encode_zset, decode_zset,
                schema_to_batch, batch_to_schema};
pub use transport::{connect, send_memfd, recv_memfd, close_fd};
pub use message::{Message, send_message, recv_message};
