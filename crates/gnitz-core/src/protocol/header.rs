pub use gnitz_wire::{
    FLAG_SHUTDOWN, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_PUSH,
    FLAG_HAS_PK, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
    FLAG_CONFLICT_MODE_PRESENT,
    FLAG_HAS_SCHEMA, FLAG_HAS_DATA, FLAG_CONTINUATION,
    STATUS_OK, STATUS_ERROR,
    META_FLAG_NULLABLE, META_FLAG_IS_PK,
    IPC_CONTROL_TID,
    WAL_HEADER_SIZE as WAL_BLOCK_HEADER_SIZE,
    WireConflictMode,
    MAX_COLUMNS,
};

pub const FLAG_ALLOCATE_TABLE_ID:  u64 = 1;
pub const FLAG_ALLOCATE_SCHEMA_ID: u64 = 2;
pub const FLAG_ALLOCATE_INDEX_ID:  u64 = 512;

/// Logical header decoded from the IPC control WAL block.
///
/// Fields align with CONTROL_SCHEMA column indices defined in
/// `gnitz_wire::control`; no wire-format struct.
#[derive(Clone, Debug)]
pub struct Header {
    pub status:     u32,
    pub target_id:  u64,
    pub client_id:  u64,
    pub flags:      u64,
    pub seek_pk:    u128,
    pub seek_col_idx: u64,
    /// Master-allocated reply-routing key. Clients send 0; the master
    /// sets a per-request value when fanning out to workers, and workers
    /// echo it back in their W2M reply. Reserved values:
    ///   0        — unsolicited / untagged
    ///   u64::MAX — broadcast reply
    pub request_id: u64,
}

impl Header {
    pub fn set_seek_by_index(&mut self, col_idx: u64, key: u128) {
        self.seek_col_idx = col_idx;
        self.seek_pk      = key;
    }
}

impl Default for Header {
    fn default() -> Self {
        Header {
            status: 0, target_id: 0, client_id: 0, flags: 0,
            seek_pk: 0, seek_col_idx: 0, request_id: 0,
        }
    }
}
