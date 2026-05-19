//! System table constants, schema definitions, and PK packing helpers.
//!
//! Pure data — no state, no CatalogEngine dependency.

use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const SYSTEM_SCHEMA_ID: i64 = 1;
pub(crate) const PUBLIC_SCHEMA_ID: i64 = 2;
pub(super) const FIRST_USER_SCHEMA_ID: i64 = gnitz_wire::FIRST_USER_SCHEMA_ID as i64;

pub(super) const OWNER_KIND_TABLE: i64 = gnitz_wire::OWNER_KIND_TABLE as i64;
pub(super) const OWNER_KIND_VIEW: i64 = gnitz_wire::OWNER_KIND_VIEW as i64;

pub(crate) const SEQ_ID_SCHEMAS: i64 = 1;
pub(crate) const SEQ_ID_TABLES: i64 = 2;
pub(crate) const SEQ_ID_INDICES: i64 = 3;
pub(super) const SEQ_ID_PROGRAMS: i64 = 4;

pub(crate) const FIRST_USER_TABLE_ID: i64 = gnitz_wire::FIRST_USER_TABLE_ID as i64;
pub(super) const FIRST_USER_INDEX_ID: i64 = 1;

pub(super) const SYS_CATALOG_DIRNAME: &str = "_system_catalog";
pub(super) const NUM_PARTITIONS: u32 = 256;

pub(super) const SCHEMA_TAB_ID: i64             = gnitz_wire::SCHEMA_TAB as i64;
pub(super) const TABLE_TAB_ID: i64              = gnitz_wire::TABLE_TAB as i64;
pub(super) const VIEW_TAB_ID: i64               = gnitz_wire::VIEW_TAB as i64;
pub(super) const COL_TAB_ID: i64                = gnitz_wire::COL_TAB as i64;
pub(super) const IDX_TAB_ID: i64                = gnitz_wire::IDX_TAB as i64;
pub(super) const DEP_TAB_ID: i64                = gnitz_wire::DEP_TAB as i64;
pub(super) const SEQ_TAB_ID: i64                = gnitz_wire::SEQ_TAB as i64;
pub(super) const CIRCUIT_NODES_TAB_ID: i64        = gnitz_wire::CIRCUIT_NODES_TAB as i64;
pub(super) const CIRCUIT_EDGES_TAB_ID: i64        = gnitz_wire::CIRCUIT_EDGES_TAB as i64;
pub(super) const CIRCUIT_NODE_COLUMNS_TAB_ID: i64 = gnitz_wire::CIRCUIT_NODE_COLUMNS_TAB as i64;

// Column indices for system tables (matching COL_* constants).
pub(super) const TABLETAB_COL_SCHEMA_ID: usize = 1;
pub(super) const TABLETAB_COL_NAME: usize = 2;
pub(super) const TABLETAB_COL_DIRECTORY: usize = 3;
pub(super) const TABLETAB_COL_PK_COL_IDX: usize = 4;
pub(super) const TABLETAB_COL_CREATED_LSN: usize = 5;
pub(super) const TABLETAB_COL_FLAGS: usize = 6;
pub(super) const TABLETAB_FLAG_UNIQUE_PK: u64 = 1;

pub(super) const PK_LIST_PACKED_FLAG: u64 = 1 << 63;

pub(super) struct PkColList {
    cols: [u32; 4],
    len: usize,
}

impl PkColList {
    /// The count exactly as decoded from the wire. May be 0 or >4 for a
    /// malformed/crafted packed value — deliberately NOT clamped, because
    /// `validate_pk_cols` gates on this raw value to reject out-of-range
    /// counts. Not a safe slice length: iterate `as_slice()` instead.
    pub(super) fn decoded_count(&self) -> usize { self.len }
    /// Always in bounds: indexes at most the 4-element backing array even
    /// when the decoded count is out of range. A crafted wire count of
    /// 5..=15 must NOT panic here — it has to survive long enough to
    /// reach `validate_pk_cols` and be returned as `Err`.
    pub(super) fn as_slice(&self) -> &[u32] { &self.cols[..self.len.min(4)] }
}

pub(super) fn pack_pk_cols(pk_cols: &[u32]) -> u64 {
    // assert! (not debug_assert!): a violated contract here corrupts the
    // persisted PK encoding via the `& 0x7f` mask, so it must fail in
    // release too rather than silently truncate.
    assert!((1..=4).contains(&pk_cols.len()), "pack_pk_cols: count out of range 1..=4");
    let mut v = pk_cols.len() as u64;            // bits [0..4)
    for (i, &c) in pk_cols.iter().enumerate() {
        assert!(c < 128, "pack_pk_cols: column index {c} exceeds 7-bit field");
        v |= (c as u64 & 0x7f) << (4 + 7 * i);
    }
    v | PK_LIST_PACKED_FLAG
}

pub(super) fn unpack_pk_cols(packed: u64) -> PkColList {
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

/// Hard-validate a decoded PK list against the table's columns. Shared by
/// the production wire path (`hook_table_register`) and the test-only
/// `ddl.rs::create_table` so both reject identically rather than falling
/// through to a `SchemaDescriptor::new` `assert!`. `pk.decoded_count()`
/// (the raw decoded count, not the clamped slice length) is what gates
/// 1..=4, so a crafted count of 5..=15 is rejected rather than silently
/// truncated.
pub(super) fn validate_pk_cols(
    col_defs: &[super::types::ColumnDef],
    pk: &PkColList,
) -> Result<(), String> {
    if !(1..=4).contains(&pk.decoded_count()) {
        return Err(format!(
            "Primary Key column count {} out of range 1..=4", pk.decoded_count()));
    }
    let cols = pk.as_slice();
    for (j, &c) in cols.iter().enumerate() {
        if (c as usize) >= col_defs.len() {
            return Err("Primary Key index out of bounds".into());
        }
        let cd = &col_defs[c as usize];
        if cd.type_code != type_code::U64
            && cd.type_code != type_code::U128
            && cd.type_code != type_code::UUID {
            return Err(format!(
                "Primary Key must be TYPE_U64, TYPE_U128, or UUID, \
                 got type_code={}", cd.type_code));
        }
        if cd.is_nullable {
            return Err("Primary Key column must not be nullable".into());
        }
        if cols[..j].contains(&c) {
            return Err("Primary Key has duplicate column".into());
        }
    }
    Ok(())
}

pub(super) const SCHEMATAB_COL_NAME: usize = 1;

pub(super) const VIEWTAB_COL_SCHEMA_ID: usize = 1;
pub(super) const VIEWTAB_COL_NAME: usize = 2;
pub(super) const VIEWTAB_COL_SQL_DEFINITION: usize = 3;
pub(super) const VIEWTAB_COL_CACHE_DIRECTORY: usize = 4;
pub(super) const VIEWTAB_COL_CREATED_LSN: usize = 5;

pub(super) const COLTAB_COL_OWNER_ID: usize = 1;
pub(super) const COLTAB_COL_OWNER_KIND: usize = 2;
pub(super) const COLTAB_COL_COL_IDX: usize = 3;
pub(super) const COLTAB_COL_NAME: usize = 4;
pub(super) const COLTAB_COL_TYPE_CODE: usize = 5;
pub(super) const COLTAB_COL_IS_NULLABLE: usize = 6;
pub(super) const COLTAB_COL_FK_TABLE_ID: usize = 7;
pub(super) const COLTAB_COL_FK_COL_IDX: usize = 8;

pub(super) const IDXTAB_COL_OWNER_ID: usize = 1;
pub(super) const IDXTAB_COL_OWNER_KIND: usize = 2;
pub(super) const IDXTAB_COL_SOURCE_COL_IDX: usize = 3;
pub(super) const IDXTAB_COL_NAME: usize = 4;
pub(super) const IDXTAB_COL_IS_UNIQUE: usize = 5;
pub(super) const IDXTAB_COL_CACHE_DIRECTORY: usize = 6;

pub(super) const DEPTAB_COL_VIEW_ID: usize = 1;
pub(super) const DEPTAB_COL_DEP_VIEW_ID: usize = 2;
pub(super) const DEPTAB_COL_DEP_TABLE_ID: usize = 3;

pub(super) const SEQTAB_COL_VALUE: usize = 1;

// Default arena sizes for system tables and user tables
pub(super) const SYS_TABLE_ARENA: u64 = 256 * 1024;          // 256 KB

// ---------------------------------------------------------------------------
// Schema builder helpers
// ---------------------------------------------------------------------------

pub(super) const fn u64_col() -> SchemaColumn {
    SchemaColumn::new(type_code::U64, 0)
}
pub(super) const fn u64_col_nullable() -> SchemaColumn {
    SchemaColumn::new(type_code::U64, 1)
}
pub(super) const fn u128_col() -> SchemaColumn {
    SchemaColumn::new(type_code::U128, 0)
}
pub(super) const fn str_col() -> SchemaColumn {
    SchemaColumn::new(type_code::STRING, 0)
}
pub(super) const fn str_col_nullable() -> SchemaColumn {
    SchemaColumn::new(type_code::STRING, 1)
}
pub(super) const fn blob_col_nullable() -> SchemaColumn {
    SchemaColumn::new(type_code::BLOB, 1)
}
pub(super) const fn zero_col() -> SchemaColumn {
    SchemaColumn::new(0, 0)
}

pub(super) const fn make_schema(cols: &[SchemaColumn], pk_index: u32) -> SchemaDescriptor {
    SchemaDescriptor::new(cols, &[pk_index])
}

/// Build a `SchemaDescriptor` from the wire-neutral column slice defined in
/// `gnitz-wire`. Called at compile time — zero runtime allocation.
const fn from_wire_cols(cols: &[gnitz_wire::WireSysCol], pk_index: u32) -> SchemaDescriptor {
    let mut buf = [zero_col(); crate::schema::MAX_COLUMNS];
    let mut i = 0;
    while i < cols.len() {
        buf[i] = SchemaColumn::new(cols[i].type_code, if cols[i].nullable { 1 } else { 0 });
        i += 1;
    }
    let (head, _) = buf.split_at(cols.len());
    SchemaDescriptor::new(head, &[pk_index])
}

// ---------------------------------------------------------------------------
// System table schema definitions
// ---------------------------------------------------------------------------

pub(super) const fn schema_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), str_col()], 0)
}
pub(super) const fn table_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), str_col(), str_col(), u64_col(), u64_col(), u64_col()], 0)
}
pub(super) const fn view_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), str_col(), str_col(), str_col(), u64_col()], 0)
}
pub(super) const fn col_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), u64_col(), u64_col(), str_col(), u64_col(), u64_col(), u64_col(), u64_col()], 0)
}
pub(super) const fn idx_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col(), u64_col(), u64_col(), str_col(), u64_col(), str_col()], 0)
}
pub(super) const fn dep_tab_schema() -> SchemaDescriptor {
    make_schema(&[u128_col(), u64_col(), u64_col(), u64_col()], 0)
}
pub(super) const fn seq_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col()], 0)
}
pub(super) const fn circuit_nodes_schema() -> SchemaDescriptor {
    from_wire_cols(gnitz_wire::CIRCUIT_NODES_COLS, 0)
}
pub(super) const fn circuit_edges_schema() -> SchemaDescriptor {
    from_wire_cols(gnitz_wire::CIRCUIT_EDGES_COLS, 0)
}
pub(super) const fn circuit_node_columns_schema() -> SchemaDescriptor {
    from_wire_cols(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS, 0)
}

// Pre-computed statics — initialised once at program start, never reconstructed.
static S_SCHEMA_TAB:        SchemaDescriptor = schema_tab_schema();
static S_TABLE_TAB:         SchemaDescriptor = table_tab_schema();
static S_VIEW_TAB:          SchemaDescriptor = view_tab_schema();
static S_COL_TAB:           SchemaDescriptor = col_tab_schema();
static S_IDX_TAB:           SchemaDescriptor = idx_tab_schema();
static S_DEP_TAB:           SchemaDescriptor = dep_tab_schema();
static S_SEQ_TAB:           SchemaDescriptor = seq_tab_schema();
static S_CIRCUIT_NODES:        SchemaDescriptor = circuit_nodes_schema();
static S_CIRCUIT_EDGES:        SchemaDescriptor = circuit_edges_schema();
static S_CIRCUIT_NODE_COLUMNS: SchemaDescriptor = circuit_node_columns_schema();

// ---------------------------------------------------------------------------
// PK packing helpers
// ---------------------------------------------------------------------------

pub(super) fn pack_column_id(owner_id: i64, col_idx: i64) -> u64 {
    ((owner_id as u64) << 9) | (col_idx as u64)
}

pub(super) fn pack_node_pk(view_id: i64, node_id: i64) -> (u64, u64) {
    (node_id as u64, view_id as u64)
}

pub(super) fn pack_edge_pk(view_id: i64, edge_id: i64) -> (u64, u64) {
    (edge_id as u64, view_id as u64)
}

pub(super) fn pack_param_pk(view_id: i64, node_id: i64, slot: i64) -> (u64, u64) {
    let lo = ((node_id as u64) << 8) | (slot as u64);
    (lo, view_id as u64)
}

pub(super) fn pack_dep_pk(view_id: i64, dep_tid: i64) -> (u64, u64) {
    (dep_tid as u64, view_id as u64)
}

pub(super) fn pack_gcol_pk(view_id: i64, node_id: i64, col_idx: i64) -> (u64, u64) {
    let lo = ((node_id as u64) << 16) | (col_idx as u64);
    (lo, view_id as u64)
}

/// Packs a CircuitNodeColumns PK from (view_id, node_id, kind, position).
///
/// Encoding: `hi = view_id`, `lo = (node_id << 24) | (kind << 16) | position`.
/// `kind` fits in 8 bits (values 0–4); `position` fits in 16 bits (≤ 65535
/// columns per kind per node); `node_id` fits in 40 bits for sequential
/// allocation. The insertion path enforces these caps via debug_assert!s
/// at the call site.
pub(super) fn pack_node_col_pk(view_id: i64, node_id: i64, kind: i64, position: i64) -> (u64, u64) {
    debug_assert!((position as u64) <= 0xFFFF, "position {} exceeds 16-bit cap", position);
    debug_assert!((kind as u64) <= 0xFF, "kind {} exceeds 8-bit cap", kind);
    debug_assert!((node_id as u64) <= 0xFF_FFFF_FFFF, "node_id {} exceeds 40-bit cap", node_id);
    let lo = ((node_id as u64) << 24) | ((kind as u64) << 16) | (position as u64);
    (lo, view_id as u64)
}

// ---------------------------------------------------------------------------
// System table subdirectory names
// ---------------------------------------------------------------------------

pub(super) struct SysTabInfo {
    pub(super) id: i64,
    pub(super) subdir: &'static str,
    pub(super) name: &'static str,
}

pub(super) const SYS_TAB_INFOS: &[SysTabInfo] = &[
    SysTabInfo { id: SCHEMA_TAB_ID, subdir: "_schemas", name: "_schemas" },
    SysTabInfo { id: TABLE_TAB_ID, subdir: "_tables", name: "_tables" },
    SysTabInfo { id: VIEW_TAB_ID, subdir: "_views", name: "_views" },
    SysTabInfo { id: COL_TAB_ID, subdir: "_columns", name: "_columns" },
    SysTabInfo { id: IDX_TAB_ID, subdir: "_indices", name: "_indices" },
    SysTabInfo { id: DEP_TAB_ID, subdir: "_view_deps", name: "_view_deps" },
    SysTabInfo { id: SEQ_TAB_ID, subdir: "_sequences", name: "_sequences" },
    SysTabInfo { id: CIRCUIT_NODES_TAB_ID, subdir: "_circuit_nodes", name: "_circuit_nodes" },
    SysTabInfo { id: CIRCUIT_EDGES_TAB_ID, subdir: "_circuit_edges", name: "_circuit_edges" },
    SysTabInfo { id: CIRCUIT_NODE_COLUMNS_TAB_ID, subdir: "_circuit_node_columns", name: "_circuit_node_columns" },
];

pub(super) fn sys_tab_schema(id: i64) -> SchemaDescriptor {
    match id {
        SCHEMA_TAB_ID                => S_SCHEMA_TAB,
        TABLE_TAB_ID                 => S_TABLE_TAB,
        VIEW_TAB_ID                  => S_VIEW_TAB,
        COL_TAB_ID                   => S_COL_TAB,
        IDX_TAB_ID                   => S_IDX_TAB,
        DEP_TAB_ID                   => S_DEP_TAB,
        SEQ_TAB_ID                   => S_SEQ_TAB,
        CIRCUIT_NODES_TAB_ID         => S_CIRCUIT_NODES,
        CIRCUIT_EDGES_TAB_ID         => S_CIRCUIT_EDGES,
        CIRCUIT_NODE_COLUMNS_TAB_ID  => S_CIRCUIT_NODE_COLUMNS,
        _ => unreachable!("Unknown system table ID: {}", id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pk_col_packing() {
        for case in [
            vec![0u32],
            vec![7],
            vec![0, 1],
            vec![3, 9, 40, 64],
        ] {
            let list = unpack_pk_cols(pack_pk_cols(&case));
            assert_eq!(list.decoded_count(), case.len());
            assert_eq!(list.as_slice(), case.as_slice());
        }

        // Reserved bits [32..63) are zero, bit 63 is set on packed values.
        let packed = pack_pk_cols(&[3, 9, 40, 64]);
        assert_eq!(packed >> 63, 1);
        assert_eq!((packed >> 32) & 0x7FFF_FFFF, 0);

        // Bare-index fallback (flag clear → single index).
        assert_eq!(unpack_pk_cols(0).as_slice(), &[0]);
        assert_eq!(unpack_pk_cols(0).decoded_count(), 1);
        assert_eq!(unpack_pk_cols(7).as_slice(), &[7]);
        assert_eq!(unpack_pk_cols(7).decoded_count(), 1);

        // Malformed flag-set value with an out-of-range count: as_slice
        // and decoded_count must be panic-free, slice clamped by min(4).
        let malformed = unpack_pk_cols(PK_LIST_PACKED_FLAG | 15);
        assert_eq!(malformed.decoded_count(), 15);
        assert_eq!(malformed.as_slice(), &[0, 0, 0, 0]);
    }
}
