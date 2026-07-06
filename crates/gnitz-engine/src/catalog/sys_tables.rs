//! System table constants, schema definitions, and PK packing helpers.
//!
//! Pure data — no state, no CatalogEngine dependency.

use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const SYSTEM_SCHEMA_ID: i64 = 1;
pub(crate) const PUBLIC_SCHEMA_ID: i64 = 2;
pub(super) const FIRST_USER_SCHEMA_ID: i64 = gnitz_wire::FIRST_USER_SCHEMA_ID as i64;

pub(super) const OWNER_KIND_TABLE: i64 = gnitz_wire::OWNER_KIND_TABLE as i64;
// Production code never writes view column records directly (they arrive via
// the wire path); only the catalog tests do.
#[cfg(test)]
pub(super) const OWNER_KIND_VIEW: i64 = gnitz_wire::OWNER_KIND_VIEW as i64;

pub(crate) const SEQ_ID_SCHEMAS: i64 = 1;
pub(crate) const SEQ_ID_TABLES: i64 = 2;
pub(crate) const SEQ_ID_INDICES: i64 = 3;
/// Committed checkpoint generation (monotonic). Falls in the ignored 4..16 gap
/// of `observe_user_sequence`, so a fresh DB writing no row defaults it to 0.
pub(crate) const SEQ_ID_CHECKPOINT_GEN: i64 = 4;
/// Cluster topology: `(worker_count as u64) << 32 | STATE_FORMAT as u64`.
pub(crate) const SEQ_ID_TOPOLOGY: i64 = 5;

pub(crate) const FIRST_USER_TABLE_ID: i64 = gnitz_wire::FIRST_USER_TABLE_ID as i64;
pub(super) const FIRST_USER_INDEX_ID: i64 = 1;

pub(super) const SYS_CATALOG_DIRNAME: &str = "_system_catalog";
pub(super) const NUM_PARTITIONS: u32 = 256;

pub(super) const SCHEMA_TAB_ID: i64 = gnitz_wire::SCHEMA_TAB as i64;
pub(crate) const TABLE_TAB_ID: i64 = gnitz_wire::TABLE_TAB as i64;
pub(crate) const VIEW_TAB_ID: i64 = gnitz_wire::VIEW_TAB as i64;
pub(super) const COL_TAB_ID: i64 = gnitz_wire::COL_TAB as i64;
pub(crate) const IDX_TAB_ID: i64 = gnitz_wire::IDX_TAB as i64;
pub(super) const DEP_TAB_ID: i64 = gnitz_wire::DEP_TAB as i64;
pub(crate) const SEQ_TAB_ID: i64 = gnitz_wire::SEQ_TAB as i64;
pub(super) const CIRCUIT_NODES_TAB_ID: i64 = gnitz_wire::CIRCUIT_NODES_TAB as i64;
pub(super) const CIRCUIT_EDGES_TAB_ID: i64 = gnitz_wire::CIRCUIT_EDGES_TAB as i64;
pub(super) const CIRCUIT_NODE_COLUMNS_TAB_ID: i64 = gnitz_wire::CIRCUIT_NODE_COLUMNS_TAB as i64;

// PK list encoding lives in gnitz-wire so the client and engine cannot
// drift on the on-disk format. Re-export under the historical paths so
// existing `pub(super)` callers in this crate keep working unchanged.
// The `unused_imports` allow covers two cases: (1) symbols used only by
// sibling modules via `use sys_tables::*`, and (2) symbols referenced
// only from the cfg(test) block below.
pub(crate) use gnitz_wire::PkColList;
#[allow(unused_imports)]
pub(super) use gnitz_wire::PK_LIST_PACKED_FLAG;
#[allow(unused_imports)]
pub(super) use gnitz_wire::{pack_pk_cols, unpack_pk_cols, PK_LIST_MAX_COLS};

/// Hard-validate a decoded PK list against the table's columns. Shared by
/// the production wire path (`hook_table_register`) and the test-only
/// `ddl.rs::create_table` so both reject identically rather than falling
/// through to a `SchemaDescriptor::new` `assert!`. `pk.decoded_count()`
/// (the raw decoded count, not the clamped slice length) is what gates
/// `1..=PK_LIST_MAX_COLS`, so a crafted over-range count is rejected rather
/// than silently truncated.
pub(super) fn validate_pk_cols(col_defs: &[super::types::ColumnDef], pk: &PkColList) -> Result<(), String> {
    if !pk.is_well_formed() {
        return Err(format!(
            "Primary Key column count {} out of range 1..={}",
            pk.decoded_count(),
            gnitz_wire::PK_LIST_MAX_COLS
        ));
    }
    let cols = pk.as_slice();
    for (j, &c) in cols.iter().enumerate() {
        if (c as usize) >= col_defs.len() {
            return Err("Primary Key index out of bounds".into());
        }
        let cd = &col_defs[c as usize];
        if !gnitz_wire::is_pk_eligible(cd.type_code) {
            return Err(format!(
                "Primary Key must be a fixed-width integer, U128, UUID, or I128 column; \
                 got type_code={} (String, Blob, and float columns cannot be PK)",
                cd.type_code
            ));
        }
        if cd.is_nullable {
            return Err("Primary Key column must not be nullable".into());
        }
        if cols[..j].contains(&c) {
            return Err("Primary Key has duplicate column".into());
        }
    }
    // The PK region must fit MAX_PK_BYTES. Strides ≤ 16 widen to a `u128` value
    // via `gnitz_wire::widen_pk_be`; wider compound PKs (stride > 16) route
    // through the byte-path accessors (`get_pk_bytes` / `compare_pk_bytes`). The
    // `PK_LIST_MAX_COLS` cap above bounds a valid PK at 64 bytes (four `U128` at
    // the current cap of 4); MAX_PK_BYTES is the
    // ceiling, defending the catalog worker against a crafted SAL-replayed
    // `TABLE_TAB` ingest whose decoded PK list packs an oversized region.
    // `pk_stride == 0` is unreachable once `is_pk_eligible` passed (every
    // eligible type is ≥ 1 byte) but is rejected explicitly as defence in depth.
    let pk_stride: usize = cols
        .iter()
        .map(|&c| gnitz_wire::wire_stride(col_defs[c as usize].type_code))
        .sum();
    if pk_stride == 0 || pk_stride > gnitz_wire::MAX_PK_BYTES {
        return Err(format!(
            "Primary Key total stride must be 1..={} bytes, got {pk_stride}",
            gnitz_wire::MAX_PK_BYTES
        ));
    }
    Ok(())
}

pub(super) const VIEWTAB_COL_PK_COL_IDX: usize = 6;
// Payload-column index (PK column 0 excluded) for the readers in
// `apply_pk_col_of` / `hook_view_register`, mirroring the IDXTAB_PAY_* pattern.
pub(crate) const VIEWTAB_PAY_PK_COL_IDX: usize = VIEWTAB_COL_PK_COL_IDX - 1;

pub(super) const COLTAB_COL_NAME: usize = 4;
pub(super) const COLTAB_COL_TYPE_CODE: usize = 5;
pub(super) const COLTAB_COL_IS_NULLABLE: usize = 6;
pub(super) const COLTAB_COL_FK_TABLE_ID: usize = 7;
pub(super) const COLTAB_COL_FK_COL_IDX: usize = 8;

pub(super) const IDXTAB_COL_OWNER_ID: usize = 1;
// Holds `pack_pk_cols(&col_indices)` for every row (single- and multi-column
// indexes alike); decoded via `unpack_pk_cols`.
pub(super) const IDXTAB_COL_SOURCE_COLS: usize = 3;
pub(super) const IDXTAB_COL_NAME: usize = 4;
pub(super) const IDXTAB_COL_IS_UNIQUE: usize = 5;

// Payload-column indices into an IDX_TAB *batch* — the layout seen by
// `read_batch_u64`/`read_batch_string`, where the single-column PK `index_id`
// (full-schema column 0) is excluded. Each is the matching IDXTAB_COL_*
// full-schema index minus one.
pub(crate) const IDXTAB_PAY_OWNER_ID: usize = IDXTAB_COL_OWNER_ID - 1;
pub(crate) const IDXTAB_PAY_SOURCE_COLS: usize = IDXTAB_COL_SOURCE_COLS - 1;
pub(crate) const IDXTAB_PAY_NAME: usize = IDXTAB_COL_NAME - 1;
pub(crate) const IDXTAB_PAY_IS_UNIQUE: usize = IDXTAB_COL_IS_UNIQUE - 1;

pub(super) const SEQTAB_COL_VALUE: usize = 1;
/// Payload index of the `value` column — the sole non-PK column — for
/// `read_batch_u64`, which indexes payload (not logical) columns. Mirrors the
/// `*_PAY_* = *_COL_* - 1` pattern above.
pub(super) const SEQTAB_PAY_VALUE: usize = SEQTAB_COL_VALUE - 1;

// Default arena sizes for system tables and user tables
pub(super) const SYS_TABLE_ARENA: u64 = 256 * 1024; // 256 KB

// ---------------------------------------------------------------------------
// Schema builder helpers
// ---------------------------------------------------------------------------

pub(super) const fn u64_col() -> SchemaColumn {
    SchemaColumn::new(type_code::U64, 0)
}
pub(super) const fn str_col() -> SchemaColumn {
    SchemaColumn::new(type_code::STRING, 0)
}
pub(super) const fn zero_col() -> SchemaColumn {
    SchemaColumn::new(0, 0)
}

pub(super) const fn make_schema(cols: &[SchemaColumn], pk_index: u32) -> SchemaDescriptor {
    SchemaDescriptor::new(cols, &[pk_index])
}

/// Build a `SchemaDescriptor` from the wire-neutral column slice defined in
/// `gnitz-wire`. Called at compile time — zero runtime allocation.
const fn from_wire_cols(cols: &[gnitz_wire::WireSysCol], pk_indices: &[u32]) -> SchemaDescriptor {
    let mut buf = [zero_col(); crate::schema::MAX_COLUMNS];
    let mut i = 0;
    while i < cols.len() {
        buf[i] = SchemaColumn::new(cols[i].type_code as u8, if cols[i].nullable { 1 } else { 0 });
        i += 1;
    }
    let (head, _) = buf.split_at(cols.len());
    SchemaDescriptor::new(head, pk_indices)
}

// ---------------------------------------------------------------------------
// System table schema definitions
// ---------------------------------------------------------------------------

pub(super) const fn schema_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), str_col()], 0)
}
pub(super) const fn table_tab_schema() -> SchemaDescriptor {
    make_schema(
        &[
            u64_col(),
            u64_col(),
            str_col(),
            str_col(),
            u64_col(),
            u64_col(),
            u64_col(),
        ],
        0,
    )
}
pub(super) const fn view_tab_schema() -> SchemaDescriptor {
    // Trailing pk_col_idx (U64) carries the packed view-PK column list, mirroring
    // TABLE_TAB. A bare `0` decodes as the single-column PK `[0]`.
    make_schema(
        &[
            u64_col(),
            u64_col(),
            str_col(),
            str_col(),
            str_col(),
            u64_col(),
            u64_col(),
        ],
        0,
    )
}
pub(super) const fn col_tab_schema() -> SchemaDescriptor {
    // 10 columns (indices 0-9). The trailing u64 (index 9) is the client's
    // `is_serial` marker; the engine stores it verbatim and never reads it
    // (`scan_column_defs` reads only indices 4-8), so it has no index constant.
    make_schema(
        &[
            u64_col(),
            u64_col(),
            u64_col(),
            u64_col(),
            str_col(),
            u64_col(),
            u64_col(),
            u64_col(),
            u64_col(),
            u64_col(),
        ],
        0,
    )
}
pub(super) const fn idx_tab_schema() -> SchemaDescriptor {
    make_schema(
        &[
            u64_col(),
            u64_col(),
            u64_col(),
            u64_col(),
            str_col(),
            u64_col(),
            str_col(),
        ],
        0,
    )
}
pub(super) const fn dep_tab_schema() -> SchemaDescriptor {
    // Compound PK (view_id, dep_table_id); dep_view_id is the only payload.
    SchemaDescriptor::new(&[u64_col(), u64_col(), u64_col()], &[0, 1])
}
pub(super) const fn seq_tab_schema() -> SchemaDescriptor {
    make_schema(&[u64_col(), u64_col()], 0)
}
pub(super) const fn circuit_nodes_schema() -> SchemaDescriptor {
    from_wire_cols(gnitz_wire::CIRCUIT_NODES_COLS, &[0, 1])
}
pub(super) const fn circuit_edges_schema() -> SchemaDescriptor {
    from_wire_cols(gnitz_wire::CIRCUIT_EDGES_COLS, &[0, 1])
}
pub(super) const fn circuit_node_columns_schema() -> SchemaDescriptor {
    from_wire_cols(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS, &[0, 1])
}

// Pre-computed statics — initialised once at program start, never reconstructed.
static S_SCHEMA_TAB: SchemaDescriptor = schema_tab_schema();
static S_TABLE_TAB: SchemaDescriptor = table_tab_schema();
static S_VIEW_TAB: SchemaDescriptor = view_tab_schema();
static S_COL_TAB: SchemaDescriptor = col_tab_schema();
static S_IDX_TAB: SchemaDescriptor = idx_tab_schema();
static S_DEP_TAB: SchemaDescriptor = dep_tab_schema();
static S_SEQ_TAB: SchemaDescriptor = seq_tab_schema();
static S_CIRCUIT_NODES: SchemaDescriptor = circuit_nodes_schema();
static S_CIRCUIT_EDGES: SchemaDescriptor = circuit_edges_schema();
static S_CIRCUIT_NODE_COLUMNS: SchemaDescriptor = circuit_node_columns_schema();

// ---------------------------------------------------------------------------
// PK packing helpers
// ---------------------------------------------------------------------------

pub(super) fn pack_column_id(owner_id: i64, col_idx: i64) -> u64 {
    ((owner_id as u64) << 9) | (col_idx as u64)
}

/// Pack a circuit/dep compound PK `(view_id, sub)` into a `u128` whose
/// `extend_pk` (big-endian) at-rest image is OPK column order: `view_id_BE`
/// (bytes 0..8) then `sub_BE` (bytes 8..16). `view_id` is PK column 0, so it
/// must occupy the high `u128` half to land in the leading at-rest bytes — the
/// prefix that `retract_rows_by_view` / `load_circuit` / `get_dep_map` seek and
/// decode. `sub` is the per-view secondary (node_id, dep_table_id, or an
/// edge/node-column field pack).
///
/// CONVERGENCE INVARIANT: the client (`create_view_chain`) packs the same
/// compound PK with `view_id` in the LOW u128 half (`vid | (sub << 64)`),
/// the byte-order dual of this `(vid << 64) | sub`. The two reach storage
/// through different encoders — the client OPK-encodes each 8-byte PK column
/// independently (low u128 bytes → first column), the engine writes the whole
/// u128 big-endian — and produce the IDENTICAL view_id-major at-rest image
/// only because every circuit-PK column is exactly 8 bytes and unsigned.
/// Changing either encoder, the column widths, or the signedness breaks view
/// loading (prefix seeks on `view_id.to_be_bytes()`).
///
/// Production circuit rows arrive pre-packed over the wire; only the tests
/// (here and in `reopen_rebuild_tests`) build them engine-side.
#[cfg(test)]
pub(super) fn pack_view_pk(view_id: i64, sub: u64) -> u128 {
    ((view_id as u64 as u128) << 64) | (sub as u128)
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
    SysTabInfo {
        id: SCHEMA_TAB_ID,
        subdir: "_schemas",
        name: "_schemas",
    },
    SysTabInfo {
        id: TABLE_TAB_ID,
        subdir: "_tables",
        name: "_tables",
    },
    SysTabInfo {
        id: VIEW_TAB_ID,
        subdir: "_views",
        name: "_views",
    },
    SysTabInfo {
        id: COL_TAB_ID,
        subdir: "_columns",
        name: "_columns",
    },
    SysTabInfo {
        id: IDX_TAB_ID,
        subdir: "_indices",
        name: "_indices",
    },
    SysTabInfo {
        id: DEP_TAB_ID,
        subdir: "_view_deps",
        name: "_view_deps",
    },
    SysTabInfo {
        id: SEQ_TAB_ID,
        subdir: "_sequences",
        name: "_sequences",
    },
    SysTabInfo {
        id: CIRCUIT_NODES_TAB_ID,
        subdir: "_circuit_nodes",
        name: "_circuit_nodes",
    },
    SysTabInfo {
        id: CIRCUIT_EDGES_TAB_ID,
        subdir: "_circuit_edges",
        name: "_circuit_edges",
    },
    SysTabInfo {
        id: CIRCUIT_NODE_COLUMNS_TAB_ID,
        subdir: "_circuit_node_columns",
        name: "_circuit_node_columns",
    },
];

pub(crate) fn sys_tab_schema(id: i64) -> SchemaDescriptor {
    match id {
        SCHEMA_TAB_ID => S_SCHEMA_TAB,
        TABLE_TAB_ID => S_TABLE_TAB,
        VIEW_TAB_ID => S_VIEW_TAB,
        COL_TAB_ID => S_COL_TAB,
        IDX_TAB_ID => S_IDX_TAB,
        DEP_TAB_ID => S_DEP_TAB,
        SEQ_TAB_ID => S_SEQ_TAB,
        CIRCUIT_NODES_TAB_ID => S_CIRCUIT_NODES,
        CIRCUIT_EDGES_TAB_ID => S_CIRCUIT_EDGES,
        CIRCUIT_NODE_COLUMNS_TAB_ID => S_CIRCUIT_NODE_COLUMNS,
        _ => unreachable!("Unknown system table ID: {}", id),
    }
}

// ---------------------------------------------------------------------------
// Typed system family
// ---------------------------------------------------------------------------

/// A catalog system-table family (every id below `FIRST_USER_TABLE_ID`). Used
/// at the applier's mutation API in place of a bare `i64`, so the `fire_hooks`
/// dispatch is an exhaustive `match` a newly-added family cannot silently skip.
/// Convert to/from `i64` only at the storage edge.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SysFamily {
    Schema,
    Table,
    View,
    Column,
    Index,
    ViewDep,
    Sequence,
    CircuitNodes,
    CircuitEdges,
    CircuitNodeColumns,
}

impl SysFamily {
    /// The `*_TAB_ID` constant for this family.
    pub(crate) const fn id(self) -> i64 {
        match self {
            SysFamily::Schema => SCHEMA_TAB_ID,
            SysFamily::Table => TABLE_TAB_ID,
            SysFamily::View => VIEW_TAB_ID,
            SysFamily::Column => COL_TAB_ID,
            SysFamily::Index => IDX_TAB_ID,
            SysFamily::ViewDep => DEP_TAB_ID,
            SysFamily::Sequence => SEQ_TAB_ID,
            SysFamily::CircuitNodes => CIRCUIT_NODES_TAB_ID,
            SysFamily::CircuitEdges => CIRCUIT_EDGES_TAB_ID,
            SysFamily::CircuitNodeColumns => CIRCUIT_NODE_COLUMNS_TAB_ID,
        }
    }

    /// Inverse of [`Self::id`]; `None` for any id that is not a system family.
    pub(crate) const fn from_id(id: i64) -> Option<Self> {
        match id {
            SCHEMA_TAB_ID => Some(SysFamily::Schema),
            TABLE_TAB_ID => Some(SysFamily::Table),
            VIEW_TAB_ID => Some(SysFamily::View),
            COL_TAB_ID => Some(SysFamily::Column),
            IDX_TAB_ID => Some(SysFamily::Index),
            DEP_TAB_ID => Some(SysFamily::ViewDep),
            SEQ_TAB_ID => Some(SysFamily::Sequence),
            CIRCUIT_NODES_TAB_ID => Some(SysFamily::CircuitNodes),
            CIRCUIT_EDGES_TAB_ID => Some(SysFamily::CircuitEdges),
            CIRCUIT_NODE_COLUMNS_TAB_ID => Some(SysFamily::CircuitNodeColumns),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pk_col_packing() {
        for case in [vec![0u32], vec![7], vec![0, 1], vec![3, 9, 40, 64]] {
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

        // Malformed flag-set value with an out-of-range count: as_slice and
        // decoded_count must be panic-free, slice clamped to PK_LIST_MAX_COLS.
        // `15` is the max the 4-bit count field can hold (independent of the cap).
        let malformed = unpack_pk_cols(PK_LIST_PACKED_FLAG | 15);
        assert_eq!(malformed.decoded_count(), 15);
        assert_eq!(malformed.as_slice(), vec![0u32; PK_LIST_MAX_COLS].as_slice());
    }

    #[test]
    fn circuit_tables_have_compound_view_id_sub_pk() {
        // from_wire_cols(&[0, 1]) must produce a 2-column PK whose stride is the
        // sum of the first two columns (U64 + U64 = 16 bytes).
        for schema in [
            circuit_nodes_schema(),
            circuit_edges_schema(),
            circuit_node_columns_schema(),
            dep_tab_schema(),
        ] {
            assert_eq!(schema.pk_indices(), &[0, 1], "circuit/dep PK must be (col0, col1)");
            assert_eq!(schema.pk_stride(), 16, "two U64 PK columns pack to 16 bytes");
        }
    }

    #[test]
    fn pack_view_pk_at_rest_is_view_id_leading_opk() {
        // The at-rest OPK image (extend_pk → big-endian) is view_id_BE then
        // sub_BE, so a view_id prefix seek lands on the leading bytes.
        let pk = pack_view_pk(0x1122, 0xAABB);
        let at_rest = pk.to_be_bytes();
        assert_eq!(
            u64::from_be_bytes(at_rest[0..8].try_into().unwrap()),
            0x1122,
            "view_id (PK col 0) must lead the at-rest OPK region",
        );
        assert_eq!(
            u64::from_be_bytes(at_rest[8..16].try_into().unwrap()),
            0xAABB,
            "sub (PK col 1) follows view_id",
        );
    }
}
