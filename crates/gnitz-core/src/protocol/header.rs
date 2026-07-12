pub use gnitz_wire::{
    wire_flags_get_conflict_mode, wire_flags_get_index_version, wire_flags_get_schema_version,
    wire_flags_set_conflict_mode, wire_flags_set_index_version, wire_flags_set_schema_version, WireConflictMode,
    FLAG_ALLOCATE_INDEX_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_SERIAL_RANGE, FLAG_ALLOCATE_TABLE_ID,
    FLAG_CONTINUATION, FLAG_DDL_SYNC, FLAG_DDL_TXN, FLAG_EXCHANGE, FLAG_GET_INDICES, FLAG_HAS_DATA, FLAG_HAS_PK,
    FLAG_HAS_SCHEMA, FLAG_PUSH, FLAG_SEEK, FLAG_SEEK_BY_INDEX, FLAG_SEEK_BY_INDEX_RANGE, FLAG_SHUTDOWN,
    IPC_CONTROL_TID, MAX_COLUMNS, META_FLAG_HIDDEN, META_FLAG_IS_PK, META_FLAG_NULLABLE, META_FLAG_PK_POS_MASK,
    META_FLAG_PK_POS_SHIFT, STATUS_ERROR, STATUS_NO_INDEX, STATUS_OK, STATUS_SCHEMA_MISMATCH,
    WAL_HEADER_SIZE as WAL_BLOCK_HEADER_SIZE,
};

/// Logical header decoded from the IPC control WAL block.
///
/// Fields align with CONTROL_SCHEMA column indices defined in
/// `gnitz_wire::control`; no wire-format struct.
#[derive(Clone, Debug, Default)]
pub struct Header {
    pub status: u32,
    pub target_id: u64,
    pub client_id: u64,
    pub flags: u64,
    pub seek_pk: u128,
    pub seek_col_idx: u64,
    /// Master-allocated reply-routing key. Clients send 0; the master
    /// sets a per-request value when fanning out to workers, and workers
    /// echo it back in their W2M reply. Reserved values:
    ///   0        — unsolicited / untagged
    ///   u64::MAX — broadcast reply
    pub request_id: u64,
}
