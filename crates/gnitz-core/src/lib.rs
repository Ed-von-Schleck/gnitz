#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

pub mod circuit;
pub mod client;
pub mod connection;
pub mod error;
pub mod expr;
pub mod protocol;
pub mod types;

pub use circuit::{
    agg_output_type, AggFunc, AggKind, Circuit, CircuitBuilder, CircuitRows, JoinKind, MapKind, NodeId, OpNode, Port,
    RangeRel, ReduceOutKey, TableId,
};
pub use client::{hidden_view_name, GnitzClient, IndexMeta, InlineUniqueIndex, PlannedView, MAX_CHAIN_SEGMENTS};
pub use connection::{
    ScanResult, Session, COL_TAB, DEP_TAB, FIRST_USER_SCHEMA_ID, FIRST_USER_TABLE_ID, IDX_TAB, SCHEMA_TAB, SEQ_TAB,
    TABLE_TAB, VIEW_TAB,
};
pub use error::ClientError;
pub use expr::{ExprBuilder, ExprProgram};
pub use gnitz_wire::{
    index_key_types, pack_table_flags, table_flags_dist_prefix, validate_dist_prefix, validate_user_identifier, Cut,
    PkColList, RangeDescriptor, FK_INDEX_INFIX,
};
pub use protocol::{
    batch_to_schema, decode_control_block, decode_wal_block, encode_control_block, encode_message,
    encode_message_noschema_parts, encode_message_parts, encode_wal_block, hello_handshake, meta_schema,
    parse_response, recv_message, schema_to_batch, send_message, send_message_noschema, wire_flags_get_conflict_mode,
    wire_flags_get_schema_version, wire_flags_set_conflict_mode, wire_flags_set_schema_version, BatchAppender,
    ClientTransport, ColData, ColumnDef, FixedInt, FrameSegments, Header, Message, MessageParts, PkColumn, PkTuple,
    ProtocolError, Schema, TransportWaker, TypeCode, WireConflictMode, ZSetBatch, FLAG_ALLOCATE_INDEX_ID,
    FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_TABLE_ID, FLAG_CONTINUATION, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_HAS_DATA,
    FLAG_HAS_PK, FLAG_HAS_SCHEMA, FLAG_PUSH, FLAG_SEEK, FLAG_SEEK_BY_INDEX, FLAG_SHUTDOWN, IPC_CONTROL_TID,
    MAX_COLUMNS, MAX_PK_BYTES, MAX_PK_COLUMNS, META_FLAG_IS_PK, META_FLAG_NULLABLE, PK_LIST_MAX_COLS, STATUS_ERROR,
    STATUS_OK, WAL_BLOCK_HEADER_SIZE,
};
pub use types::{
    circuit_edges_schema, circuit_node_columns_schema, circuit_nodes_schema, col_tab_schema, dep_tab_schema,
    schema_tab_schema, table_tab_schema, view_tab_schema,
};
