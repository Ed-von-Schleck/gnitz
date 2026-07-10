//! System table constants, the per-family descriptor table, and PK packing
//! helpers.
//!
//! Pure data — no state, no CatalogEngine dependency.

use crate::schema::{SchemaColumn, SchemaDescriptor};

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

/// One admissibility check for a relation's column records + PK list, shared
/// by the TABLE/VIEW precheck arms, both register hooks (the deliberate
/// precheck/hook double-run — boot replay and worker ddl_sync skip precheck),
/// and the test-only `ddl.rs::create_table`, so every layer rejects
/// identically. Order is load-bearing: col-defs non-empty (the cross-family
/// COL_TAB-before-TABLE/VIEW ordering contract) → [`validate_pk_cols`] →
/// MAX_COLUMNS.
pub(super) fn validate_relation_defs(
    kind: &str,
    id: i64,
    name: &str,
    col_defs: &[super::types::ColumnDef],
    pk: &PkColList,
) -> Result<(), String> {
    if col_defs.is_empty() {
        return Err(format!(
            "catalog invariant violated: {kind} '{name}' (id={id}) registered \
             before its column records. COL_TAB writes must precede \
             TABLE_TAB/VIEW_TAB writes (see hooks.rs dispatch doc)."
        ));
    }
    validate_pk_cols(col_defs, pk)?;
    if col_defs.len() > crate::schema::MAX_COLUMNS {
        return Err(format!(
            "{kind} '{name}' (id={id}) has {} columns (max {})",
            col_defs.len(),
            crate::schema::MAX_COLUMNS
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Positional column constants — derived at compile time from the shared
// gnitz-wire column slices, so a reshaped table re-derives every index.
// `*_COL_*` are full-schema indices (cursor reads); `*_PAY_*` are payload
// indices (batch reads, single leading PK column excluded).
// ---------------------------------------------------------------------------

use gnitz_wire::col_index_in;

/// Payload-column index (single leading PK excluded) of `name` in `cols` —
/// the layout `Batch::read_u64`-style decoders see. Valid only for the
/// single-PK families (every family below has PK = column 0).
const fn pay_index_in(cols: &[gnitz_wire::WireSysCol], name: &str) -> usize {
    col_index_in(cols, name) - 1
}

pub(super) const SCHEMATAB_PAY_NAME: usize = pay_index_in(gnitz_wire::SCHEMA_TAB_COLS, "name");

pub(super) const TABTAB_PAY_SCHEMA_ID: usize = pay_index_in(gnitz_wire::TABLE_TAB_COLS, "schema_id");
pub(super) const TABTAB_PAY_NAME: usize = pay_index_in(gnitz_wire::TABLE_TAB_COLS, "name");
pub(super) const TABTAB_PAY_PK_COL_IDX: usize = pay_index_in(gnitz_wire::TABLE_TAB_COLS, "pk_col_idx");
pub(super) const TABTAB_PAY_FLAGS: usize = pay_index_in(gnitz_wire::TABLE_TAB_COLS, "flags");

pub(super) const VIEWTAB_PAY_SCHEMA_ID: usize = pay_index_in(gnitz_wire::VIEW_TAB_COLS, "schema_id");
pub(super) const VIEWTAB_PAY_NAME: usize = pay_index_in(gnitz_wire::VIEW_TAB_COLS, "name");
pub(crate) const VIEWTAB_PAY_PK_COL_IDX: usize = pay_index_in(gnitz_wire::VIEW_TAB_COLS, "pk_col_idx");

// `apply_entity_caches` / `apply_schema_of` decode TABLE_TAB and VIEW_TAB
// batches through one code path (reading the TABTAB_PAY_* positions); the two
// families must agree on the leading `(schema_id, name)` payload prefix.
const _: () = {
    assert!(TABTAB_PAY_SCHEMA_ID == VIEWTAB_PAY_SCHEMA_ID);
    assert!(TABTAB_PAY_NAME == VIEWTAB_PAY_NAME);
};

pub(super) const COLTAB_COL_NAME: usize = col_index_in(gnitz_wire::COL_TAB_COLS, "name");
pub(super) const COLTAB_COL_TYPE_CODE: usize = col_index_in(gnitz_wire::COL_TAB_COLS, "type_code");
pub(super) const COLTAB_COL_IS_NULLABLE: usize = col_index_in(gnitz_wire::COL_TAB_COLS, "is_nullable");
pub(super) const COLTAB_COL_FK_TABLE_ID: usize = col_index_in(gnitz_wire::COL_TAB_COLS, "fk_table_id");
pub(super) const COLTAB_COL_FK_COL_IDX: usize = col_index_in(gnitz_wire::COL_TAB_COLS, "fk_col_idx");
pub(super) const COLTAB_COL_IS_HIDDEN: usize = col_index_in(gnitz_wire::COL_TAB_COLS, "is_hidden");
pub(super) const COLTAB_PAY_OWNER_ID: usize = pay_index_in(gnitz_wire::COL_TAB_COLS, "owner_id");
pub(super) const COLTAB_PAY_COL_IDX: usize = pay_index_in(gnitz_wire::COL_TAB_COLS, "col_idx");
pub(super) const COLTAB_PAY_FK_TABLE_ID: usize = pay_index_in(gnitz_wire::COL_TAB_COLS, "fk_table_id");
pub(super) const COLTAB_PAY_FK_COL_IDX: usize = pay_index_in(gnitz_wire::COL_TAB_COLS, "fk_col_idx");

pub(super) const IDXTAB_COL_OWNER_ID: usize = col_index_in(gnitz_wire::IDX_TAB_COLS, "owner_id");
// Holds `pack_pk_cols(&col_indices)` for every row (single- and multi-column
// indexes alike); decoded via `unpack_pk_cols`.
pub(super) const IDXTAB_COL_SOURCE_COLS: usize = col_index_in(gnitz_wire::IDX_TAB_COLS, "source_col_idx");
pub(super) const IDXTAB_COL_IS_UNIQUE: usize = col_index_in(gnitz_wire::IDX_TAB_COLS, "is_unique");
pub(crate) const IDXTAB_PAY_OWNER_ID: usize = pay_index_in(gnitz_wire::IDX_TAB_COLS, "owner_id");
pub(crate) const IDXTAB_PAY_SOURCE_COLS: usize = pay_index_in(gnitz_wire::IDX_TAB_COLS, "source_col_idx");
pub(crate) const IDXTAB_PAY_NAME: usize = pay_index_in(gnitz_wire::IDX_TAB_COLS, "name");
pub(crate) const IDXTAB_PAY_IS_UNIQUE: usize = pay_index_in(gnitz_wire::IDX_TAB_COLS, "is_unique");

pub(super) const SEQTAB_COL_VALUE: usize = col_index_in(gnitz_wire::SEQ_TAB_COLS, "next_val");
pub(super) const SEQTAB_PAY_VALUE: usize = pay_index_in(gnitz_wire::SEQ_TAB_COLS, "next_val");

// Default arena sizes for system tables and user tables
pub(super) const SYS_TABLE_ARENA: u64 = 256 * 1024; // 256 KB

// ---------------------------------------------------------------------------
// Schema derivation from the shared wire column slices
// ---------------------------------------------------------------------------

pub(super) const fn zero_col() -> SchemaColumn {
    SchemaColumn::new(0, 0)
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

// Pre-computed schema statics, one per family, indexed by `SysFamily`
// discriminant — initialised at compile time, never reconstructed.
static SCHEMAS: [SchemaDescriptor; SysFamily::COUNT] = {
    let mut arr = [from_wire_cols(SYS_FAMILIES[0].cols, SYS_FAMILIES[0].pk_cols); SysFamily::COUNT];
    let mut i = 1;
    while i < SysFamily::COUNT {
        arr[i] = from_wire_cols(SYS_FAMILIES[i].cols, SYS_FAMILIES[i].pk_cols);
        i += 1;
    }
    arr
};

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
// Per-family descriptor table
// ---------------------------------------------------------------------------

pub(crate) struct SysFamilyInfo {
    pub(crate) id: i64,
    /// The table's name AND its subdirectory under `_system_catalog/`.
    pub(crate) name: &'static str,
    /// The shared wire column slice the schema, the COL_TAB self-description
    /// rows, and the client's `Schema` all derive from.
    pub(crate) cols: &'static [gnitz_wire::WireSysCol],
    pub(crate) pk_cols: &'static [u32],
    /// Topological creation priority. Lower = earlier in the dependency chain
    /// (created first, destroyed last). Orders the `DDL_TXN` handler's
    /// ascending forward ingest (so every register/index hook sees its
    /// dependencies already in the memtable) and rollback's descending negate.
    /// Distinct for Table and View so their relative order is stable; 99 =
    /// order-neutral (Sequence, matching the non-family default).
    pub(crate) topo_priority: u8,
}

/// One descriptor per system family, indexed by `SysFamily` discriminant
/// (asserted below).
pub(crate) const SYS_FAMILIES: [SysFamilyInfo; SysFamily::COUNT] = [
    SysFamilyInfo {
        id: SCHEMA_TAB_ID,
        name: "_schemas",
        cols: gnitz_wire::SCHEMA_TAB_COLS,
        pk_cols: &[0],
        topo_priority: 0,
    },
    SysFamilyInfo {
        id: TABLE_TAB_ID,
        name: "_tables",
        cols: gnitz_wire::TABLE_TAB_COLS,
        pk_cols: &[0],
        topo_priority: 6,
    },
    SysFamilyInfo {
        id: VIEW_TAB_ID,
        name: "_views",
        cols: gnitz_wire::VIEW_TAB_COLS,
        pk_cols: &[0],
        topo_priority: 7,
    },
    SysFamilyInfo {
        id: COL_TAB_ID,
        name: "_columns",
        cols: gnitz_wire::COL_TAB_COLS,
        pk_cols: &[0],
        topo_priority: 1,
    },
    SysFamilyInfo {
        id: IDX_TAB_ID,
        name: "_indices",
        cols: gnitz_wire::IDX_TAB_COLS,
        pk_cols: &[0],
        topo_priority: 8,
    },
    SysFamilyInfo {
        id: DEP_TAB_ID,
        name: "_view_deps",
        cols: gnitz_wire::DEP_TAB_COLS,
        pk_cols: &[0, 1],
        topo_priority: 2,
    },
    SysFamilyInfo {
        id: SEQ_TAB_ID,
        name: "_sequences",
        cols: gnitz_wire::SEQ_TAB_COLS,
        pk_cols: &[0],
        topo_priority: 99,
    },
    SysFamilyInfo {
        id: CIRCUIT_NODES_TAB_ID,
        name: "_circuit_nodes",
        cols: gnitz_wire::CIRCUIT_NODES_COLS,
        pk_cols: &[0, 1],
        topo_priority: 3,
    },
    SysFamilyInfo {
        id: CIRCUIT_EDGES_TAB_ID,
        name: "_circuit_edges",
        cols: gnitz_wire::CIRCUIT_EDGES_COLS,
        pk_cols: &[0, 1],
        topo_priority: 4,
    },
    SysFamilyInfo {
        id: CIRCUIT_NODE_COLUMNS_TAB_ID,
        name: "_circuit_node_columns",
        cols: gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS,
        pk_cols: &[0, 1],
        topo_priority: 5,
    },
];

// `SYS_FAMILIES[f.index()]` must describe family `f`: verify the array order
// against `from_id` (the ground-truth id mapping) at compile time.
const _: () = {
    let mut i = 0;
    while i < SysFamily::COUNT {
        match SysFamily::from_id(SYS_FAMILIES[i].id) {
            Some(f) => assert!(
                f as usize == i,
                "SYS_FAMILIES order must match SysFamily discriminant order"
            ),
            None => panic!("SYS_FAMILIES entry id is not a system family"),
        }
        i += 1;
    }
};

/// The fixed schema for system-family `id`. Panics on a non-family id; callers
/// holding an untrusted id go through `sys_family_schema` / `SysFamily::from_id`.
pub(crate) fn sys_tab_schema(id: i64) -> SchemaDescriptor {
    SysFamily::from_id(id)
        .unwrap_or_else(|| panic!("Unknown system table ID: {id}"))
        .schema()
}

// ---------------------------------------------------------------------------
// Typed system family
// ---------------------------------------------------------------------------

/// A catalog system-table family (every id below `FIRST_USER_TABLE_ID`). Used
/// at the applier's mutation API in place of a bare `i64`, so the `fire_hooks`
/// dispatch is an exhaustive `match` a newly-added family cannot silently skip.
/// Convert to/from `i64` only at the storage edge. The discriminant indexes
/// `SYS_FAMILIES`, `SCHEMAS`, and `CatalogEngine::sys_stores`.
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
    pub(crate) const COUNT: usize = 10;

    /// Discriminant index into the per-family arrays.
    #[inline]
    pub(crate) const fn index(self) -> usize {
        self as usize
    }

    /// The `*_TAB_ID` constant for this family.
    #[inline]
    pub(crate) const fn id(self) -> i64 {
        SYS_FAMILIES[self.index()].id
    }

    /// This family's descriptor entry.
    #[inline]
    pub(crate) fn info(self) -> &'static SysFamilyInfo {
        &SYS_FAMILIES[self.index()]
    }

    /// This family's fixed schema.
    #[inline]
    pub(crate) fn schema(self) -> SchemaDescriptor {
        SCHEMAS[self.index()]
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
            SysFamily::CircuitNodes.schema(),
            SysFamily::CircuitEdges.schema(),
            SysFamily::CircuitNodeColumns.schema(),
            SysFamily::ViewDep.schema(),
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
