//! System-catalog wire layout: the shared `WireSysCol` descriptor, system
//! table column lists and IDs, schema sizing caps, and the compound-PK
//! column-list codec for the persisted `TABLE_TAB.pk_col_idx` u64.

use crate::type_code;

// ---------------------------------------------------------------------------
// System table column descriptors — shared single source of truth
// ---------------------------------------------------------------------------

pub struct WireSysCol {
    pub name:      &'static str,
    pub type_code: u8,
    pub nullable:  bool,
}

pub const CIRCUIT_NODES_COLS: &[WireSysCol] = &[
    WireSysCol { name: "node_pk",      type_code: type_code::U128, nullable: false },
    WireSysCol { name: "view_id",      type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "node_id",      type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "opcode",       type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "source_table", type_code: type_code::U64,  nullable: true  },
    WireSysCol { name: "reindex_col",  type_code: type_code::U64,  nullable: true  },
    WireSysCol { name: "expr_program", type_code: type_code::BLOB, nullable: true  },
];

pub const CIRCUIT_EDGES_COLS: &[WireSysCol] = &[
    WireSysCol { name: "edge_pk",  type_code: type_code::U128, nullable: false },
    WireSysCol { name: "view_id",  type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "dst_node", type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "dst_port", type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "src_node", type_code: type_code::U64,  nullable: false },
];

pub const CIRCUIT_NODE_COLUMNS_COLS: &[WireSysCol] = &[
    WireSysCol { name: "node_col_pk", type_code: type_code::U128, nullable: false },
    WireSysCol { name: "view_id",     type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "node_id",     type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "kind",        type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "position",    type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "value1",      type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "value2",      type_code: type_code::U64,  nullable: false },
];

// ---------------------------------------------------------------------------
// System table IDs
// ---------------------------------------------------------------------------

pub const SCHEMA_TAB:           u64 = 1;
pub const TABLE_TAB:            u64 = 2;
pub const VIEW_TAB:             u64 = 3;
pub const COL_TAB:              u64 = 4;
pub const IDX_TAB:              u64 = 5;
pub const DEP_TAB:              u64 = 6;
pub const SEQ_TAB:              u64 = 7;
pub const CIRCUIT_NODES_TAB:        u64 = 11;
pub const CIRCUIT_EDGES_TAB:        u64 = 12;
pub const CIRCUIT_NODE_COLUMNS_TAB: u64 = 13;
// IDs 14 and 15 were previously CIRCUIT_PARAMS_TAB and CIRCUIT_GROUP_COLS_TAB,
// now folded into CircuitNodes / CircuitNodeColumns.

pub const FIRST_USER_TABLE_ID:  u64 = 16;
pub const FIRST_USER_SCHEMA_ID: u64 = 3;

pub const OWNER_KIND_TABLE: u64 = 0;
pub const OWNER_KIND_VIEW:  u64 = 1;

// ---------------------------------------------------------------------------
// Schema sizing caps
// ---------------------------------------------------------------------------

/// Maximum number of columns (PK + payload) in any table or view schema.
/// Capped at 65 by the row-major null bitmap: each row stores one u64 word
/// with one bit per nullable payload column, so payload columns ≤ 64.
pub const MAX_COLUMNS: usize = 65;

/// Sizing cap for the compound-PK column list.
///
/// Set to 5 to cover the user-facing PK cap (4 columns, planner-enforced)
/// plus one indexed-column prefix used by secondary index schemas — modeled
/// as `(indexed_col, src_pk_0, …, src_pk_{k-1})`.
pub const MAX_PK_COLUMNS: usize = 5;

/// Maximum byte width of a PK region per row. Product of `MAX_PK_COLUMNS`
/// and the per-column ceiling (16 == max wire stride of any type valid as a
/// PK column — U128, UUID; STRING and BLOB are rejected by schema
/// validation). Auto-tracks growth of `MAX_PK_COLUMNS`.
pub const MAX_PK_BYTES: usize = MAX_PK_COLUMNS * 16;

// ---------------------------------------------------------------------------
// Compound-PK list encoding for the persisted `TABLE_TAB.pk_col_idx` u64.
//
// Two forms share the same column slot:
//   * Bare scalar (flag bit clear): a single PK column index in bits [0..63).
//     Written by an unmodified single-PK client and by engine-side bootstrap
//     for system tables.
//   * Packed list (flag bit set):
//        bit 63        : PK_LIST_PACKED_FLAG
//        bits [0..4)   : decoded count (1..=4 valid; >4 reserved for tests)
//        bits [4+7i..) : i-th column index, 7 bits each
//
// Both client (gnitz-core) and engine (gnitz-engine catalog) MUST share this
// encoder/decoder so they cannot drift on the encoding.
// ---------------------------------------------------------------------------

pub const PK_LIST_PACKED_FLAG: u64 = 1 << 63;

/// Decoded PK column list — backing storage sized to 4 entries.
/// `decoded_count()` returns the raw decoded count from the wire (may be 0
/// or 5..=15 for a crafted packed value); `as_slice()` is panic-free and
/// clamps the slice to at most 4 entries. Out-of-range counts must reach
/// schema-validation code as `Err`, not as a panic here.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct PkColList {
    cols: [u32; 4],
    len: usize,
}

impl PkColList {
    /// Single-column PK with `len = 1`.
    pub fn single(idx: u32) -> Self {
        PkColList { cols: [idx, 0, 0, 0], len: 1 }
    }
    /// The count exactly as decoded from the wire. May be 0 or >4 for a
    /// malformed/crafted packed value — deliberately NOT clamped, because
    /// `validate_pk_cols` gates on this raw value to reject out-of-range
    /// counts. Not a safe slice length: iterate `as_slice()` instead.
    pub fn decoded_count(&self) -> usize { self.len }
    /// Always in bounds: indexes at most the 4-element backing array even
    /// when the decoded count is out of range. A crafted wire count of
    /// 5..=15 must NOT panic here — it has to survive long enough to
    /// reach `validate_pk_cols` and be returned as `Err`.
    pub fn as_slice(&self) -> &[u32] { &self.cols[..self.len.min(4)] }
}

/// Pack a PK column-index list into the persisted `u64` form. Panics on a
/// violated contract because a silent truncation here corrupts the
/// catalog encoding; callers (client + engine) must reject out-of-range
/// lists before calling this.
pub fn pack_pk_cols(pk_cols: &[u32]) -> u64 {
    assert!((1..=4).contains(&pk_cols.len()), "pack_pk_cols: count out of range 1..=4");
    let mut v = pk_cols.len() as u64;            // bits [0..4)
    for (i, &c) in pk_cols.iter().enumerate() {
        assert!(c < 128, "pack_pk_cols: column index {c} exceeds 7-bit field");
        v |= (c as u64 & 0x7f) << (4 + 7 * i);
    }
    v | PK_LIST_PACKED_FLAG
}

/// Decode the persisted `u64` PK-list form. Handles both the bare scalar
/// (flag bit clear → single index) and packed list forms. Out-of-range
/// counts are returned as-is via `decoded_count()` so the catalog
/// validator can reject them.
pub fn unpack_pk_cols(packed: u64) -> PkColList {
    if packed & PK_LIST_PACKED_FLAG == 0 {
        // Bare single index: an unmodified gnitz-core client, or an
        // engine-written system-table row (always bare `0`).
        return PkColList { cols: [packed as u32, 0, 0, 0], len: 1 };
    }
    let n = (packed & 0xf) as usize;             // 0..=15, validated later
    let mut cols = [0u32; 4];
    for (i, slot) in cols.iter_mut().enumerate().take(n.min(4)) {
        *slot = ((packed >> (4 + 7 * i)) & 0x7f) as u32;
    }
    PkColList { cols, len: n }
}
