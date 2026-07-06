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
    AggFunc, AggKind, Circuit, CircuitBuilder, CircuitRows, JoinKind, MapKind, NodeId, OpNode, Port, RangeRel, TableId,
};
pub use client::{hidden_view_name, GnitzClient, IndexMeta, InlineUniqueIndex, PlannedView, MAX_CHAIN_SEGMENTS};
pub use connection::{
    Connection, COL_TAB, DEP_TAB, FIRST_USER_SCHEMA_ID, FIRST_USER_TABLE_ID, IDX_TAB, SCHEMA_TAB, SEQ_TAB, TABLE_TAB,
    VIEW_TAB,
};
pub use error::ClientError;
pub use expr::{ExprBuilder, ExprDecodeErr, ExprProgram, EXPR_BLOB_MAGIC, EXPR_BLOB_VERSION};
pub use gnitz_wire::{
    index_key_types, pack_table_flags, table_flags_dist_prefix, validate_dist_prefix, validate_user_identifier, Cut,
    PkColList, RangeDescriptor, FK_INDEX_INFIX,
};
pub use protocol::{
    batch_to_schema, close_fd, connect, connect_unix, decode_control_block, decode_wal_block, encode_control_block,
    encode_message, encode_wal_block, get_region_offset_size, hello_handshake, meta_schema, parse_response,
    recompute_block_checksum, recv_framed, recv_message, schema_to_batch, send_framed, send_framed_batch,
    send_framed_iov, send_message, send_message_noschema, wire_flags_get_conflict_mode, wire_flags_get_schema_version,
    wire_flags_set_conflict_mode, wire_flags_set_schema_version, BatchAppender, ColData, ColumnDef, FixedInt, Header,
    Message, PkColumn, PkTuple, ProtocolError, Schema, TypeCode, VerifyChecksum, WireConflictMode, ZSetBatch,
    FLAG_ALLOCATE_INDEX_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_TABLE_ID, FLAG_CONTINUATION, FLAG_DDL_SYNC,
    FLAG_EXCHANGE, FLAG_HAS_DATA, FLAG_HAS_PK, FLAG_HAS_SCHEMA, FLAG_PUSH, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
    FLAG_SHUTDOWN, IPC_CONTROL_TID, MAX_COLUMNS, MAX_PK_BYTES, MAX_PK_COLUMNS, META_FLAG_IS_PK, META_FLAG_NULLABLE,
    PK_LIST_MAX_COLS, STATUS_ERROR, STATUS_OK, WAL_BLOCK_HEADER_SIZE,
};
pub use types::{
    circuit_edges_schema, circuit_node_columns_schema, circuit_nodes_schema, col_tab_schema, dep_tab_schema,
    schema_tab_schema, table_tab_schema, view_tab_schema, AGG_COUNT, AGG_COUNT_NON_NULL, AGG_MAX, AGG_MIN, AGG_SUM,
    AGG_SUM_ZERO, CIRCUIT_EDGES_TAB, CIRCUIT_NODES_TAB, CIRCUIT_NODE_COLUMNS_TAB, OPCODE_DISTINCT,
    OPCODE_EXCHANGE_GATHER, OPCODE_EXCHANGE_SHARD, OPCODE_FILTER, OPCODE_INTEGRATE, OPCODE_INTEGRATE_TRACE,
    OPCODE_JOIN_DELTA_TRACE, OPCODE_JOIN_DELTA_TRACE_RANGE, OPCODE_MAP, OPCODE_MAP_EXPR, OPCODE_MAP_PROJ,
    OPCODE_NEGATE, OPCODE_NULL_EXTEND, OPCODE_PARTITION_FILTER, OPCODE_POSITIVE_PART, OPCODE_REDUCE, OPCODE_SCAN_DELTA,
    OPCODE_UNION, OWNER_KIND_TABLE, OWNER_KIND_VIEW, PORT_IN, PORT_IN_A, PORT_IN_B, PORT_TRACE,
};
