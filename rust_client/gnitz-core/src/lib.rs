pub mod error;
pub mod connection;
pub mod ops;
pub mod types;
pub mod client;

pub use error::ClientError;
pub use connection::Connection;
pub use ops::{
    alloc_table_id, alloc_schema_id, push, scan,
    SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, DEP_TAB, SEQ_TAB,
    FIRST_USER_TABLE_ID, FIRST_USER_SCHEMA_ID,
};
pub use client::GnitzClient;
pub use types::{
    CircuitGraph,
    CIRCUIT_NODES_TAB, CIRCUIT_EDGES_TAB, CIRCUIT_SOURCES_TAB,
    CIRCUIT_PARAMS_TAB, CIRCUIT_GROUP_COLS_TAB,
    OWNER_KIND_TABLE, OWNER_KIND_VIEW,
    OPCODE_SCAN_TRACE, OPCODE_INTEGRATE,
    OPCODE_FILTER, OPCODE_MAP, OPCODE_NEGATE, OPCODE_UNION,
    OPCODE_JOIN_DELTA_TRACE, OPCODE_JOIN_DELTA_DELTA,
    OPCODE_DELAY, OPCODE_REDUCE, OPCODE_DISTINCT,
    OPCODE_ANTI_JOIN_DELTA_TRACE, OPCODE_ANTI_JOIN_DELTA_DELTA,
    OPCODE_SEMI_JOIN_DELTA_TRACE, OPCODE_SEMI_JOIN_DELTA_DELTA,
    OPCODE_EXCHANGE_SHARD, OPCODE_EXCHANGE_GATHER,
    PORT_IN, PORT_TRACE, PORT_IN_A, PORT_IN_B,
    PARAM_FUNC_ID, PARAM_AGG_FUNC_ID, PARAM_AGG_COL_IDX,
    PARAM_PROJ_BASE, PARAM_EXPR_BASE, PARAM_EXPR_NUM_REGS,
    PARAM_EXPR_RESULT_REG, PARAM_SHARD_COL_BASE, PARAM_GATHER_WORKER,
    schema_tab_schema, table_tab_schema, col_tab_schema, view_tab_schema,
    dep_tab_schema, circuit_nodes_schema, circuit_edges_schema,
    circuit_sources_schema, circuit_params_schema, circuit_group_cols_schema,
};
