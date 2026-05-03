pub mod protocol;
pub mod error;
pub mod connection;
pub mod types;
pub mod client;
pub mod expr;
pub mod circuit;

pub use error::ClientError;
pub use connection::{
    Connection,
    SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, IDX_TAB, DEP_TAB, SEQ_TAB,
    FIRST_USER_TABLE_ID, FIRST_USER_SCHEMA_ID,
};
pub use client::GnitzClient;
pub use protocol::{
    ProtocolError,
    TypeCode, ColumnDef, PkColumn, Schema, ColData, ZSetBatch, BatchAppender, meta_schema,
    Header,
    FLAG_ALLOCATE_TABLE_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_SHUTDOWN,
    FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_PUSH, FLAG_HAS_PK, FLAG_SEEK,
    FLAG_SEEK_BY_INDEX, FLAG_ALLOCATE_INDEX_ID,
    FLAG_CONFLICT_MODE_PRESENT, WireConflictMode,
    FLAG_HAS_SCHEMA, FLAG_HAS_DATA, FLAG_CONTINUATION,
    STATUS_OK, STATUS_ERROR, META_FLAG_NULLABLE, META_FLAG_IS_PK, MAX_COLUMNS,
    WAL_BLOCK_HEADER_SIZE, IPC_CONTROL_TID,
    encode_wal_block, decode_wal_block, VerifyChecksum, recompute_block_checksum, get_region_offset_size,
    schema_to_batch, batch_to_schema,
    connect, send_framed, send_framed_iov, recv_framed, close_fd,
    Message, send_message, recv_message, encode_message,
    parse_response, encode_control_block, decode_control_block,
};
pub use expr::{ExprBuilder, ExprProgram};
pub use circuit::{CircuitBuilder, NodeId};
pub use types::{
    CircuitGraph,
    CIRCUIT_NODES_TAB, CIRCUIT_EDGES_TAB, CIRCUIT_SOURCES_TAB,
    CIRCUIT_PARAMS_TAB, CIRCUIT_GROUP_COLS_TAB,
    OWNER_KIND_TABLE, OWNER_KIND_VIEW,
    OPCODE_SCAN_TRACE, OPCODE_INTEGRATE,
    OPCODE_FILTER, OPCODE_MAP, OPCODE_NEGATE, OPCODE_UNION,
    OPCODE_JOIN_DELTA_TRACE, OPCODE_JOIN_DELTA_DELTA, OPCODE_JOIN_DELTA_TRACE_OUTER,
    OPCODE_DELAY, OPCODE_REDUCE, OPCODE_DISTINCT,
    OPCODE_ANTI_JOIN_DELTA_TRACE, OPCODE_ANTI_JOIN_DELTA_DELTA,
    OPCODE_SEMI_JOIN_DELTA_TRACE, OPCODE_SEMI_JOIN_DELTA_DELTA,
    OPCODE_EXCHANGE_SHARD, OPCODE_EXCHANGE_GATHER,
    PORT_IN, PORT_TRACE, PORT_IN_A, PORT_IN_B,
    PARAM_FUNC_ID, PARAM_AGG_FUNC_ID, PARAM_AGG_COL_IDX,
    PARAM_PROJ_BASE, PARAM_EXPR_BASE, PARAM_EXPR_NUM_REGS,
    PARAM_EXPR_RESULT_REG, PARAM_SHARD_COL_BASE, PARAM_GATHER_WORKER,
    PARAM_REINDEX_COL, PARAM_JOIN_SOURCE_TABLE, PARAM_CONST_STR_BASE,
    PARAM_TABLE_ID, PARAM_AGG_COUNT, PARAM_AGG_SPEC_BASE,
    AGG_COUNT, AGG_SUM, AGG_MIN, AGG_MAX, AGG_COUNT_NON_NULL,
    schema_tab_schema, table_tab_schema, col_tab_schema, view_tab_schema,
    dep_tab_schema, circuit_nodes_schema, circuit_edges_schema,
    circuit_sources_schema, circuit_params_schema, circuit_group_cols_schema,
};
