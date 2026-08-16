//! System table constants, the per-family descriptor table, PK packing
//! helpers, and the per-family row codecs (batch row-view decoders and
//! row builders) over those constants.
//!
//! Pure data and stateless codecs — no state, no CatalogEngine dependency.

use super::ColumnDef;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, BatchBuilder};
use gnitz_wire::sys_rows::{ColTabRow, IdxTabRow, TableTabRow};

use gnitz_wire::{
    IDXTAB_COL_IS_UNIQUE, IDXTAB_COL_OWNER_ID, IDXTAB_COL_SOURCE_COLS, IDXTAB_PAY_IS_UNIQUE, IDXTAB_PAY_OWNER_ID,
    IDXTAB_PAY_SOURCE_COLS, TABTAB_PAY_FLAGS, TABTAB_PAY_NAME, TABTAB_PAY_PK_COL_IDX, TABTAB_PAY_SCHEMA_ID,
    VIEWTAB_PAY_NAME, VIEWTAB_PAY_PK_COL_IDX, VIEWTAB_PAY_SCHEMA_ID,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(super) const SYSTEM_SCHEMA_ID: i64 = 1;
pub(super) const PUBLIC_SCHEMA_ID: i64 = 2;
pub(super) const FIRST_USER_SCHEMA_ID: i64 = gnitz_wire::FIRST_USER_SCHEMA_ID as i64;

pub(super) const OWNER_KIND_TABLE: i64 = gnitz_wire::OWNER_KIND_TABLE as i64;
// Production code never writes view column records directly (they arrive via
// the wire path); only the catalog tests do.
#[cfg(test)]
pub(super) const OWNER_KIND_VIEW: i64 = gnitz_wire::OWNER_KIND_VIEW as i64;

pub(super) const SEQ_ID_SCHEMAS: i64 = 1;
pub(super) const SEQ_ID_TABLES: i64 = 2;
pub(super) const SEQ_ID_INDICES: i64 = 3;
/// Committed checkpoint generation (monotonic). Falls in the ignored 4..16 gap
/// of `observe_user_sequence`, so a fresh DB writing no row defaults it to 0.
pub(super) const SEQ_ID_CHECKPOINT_GEN: i64 = 4;
/// Cluster topology: `(worker_count as u64) << 32 | STATE_FORMAT as u64`.
pub(super) const SEQ_ID_TOPOLOGY: i64 = 5;

pub(crate) const FIRST_USER_TABLE_ID: i64 = gnitz_wire::FIRST_USER_TABLE_ID as i64;
pub(super) const FIRST_USER_INDEX_ID: i64 = 1;

/// A conservative tripwire on durable relation-id allocation, not a live limit —
/// reaching it needs 2^31 durable CREATEs. Durable relations live in
/// `[FIRST_USER_TABLE_ID, 1<<31)`; `allocate_table_id` and `precheck_family` (the
/// point an id enters `dag.tables`) reject any id at or above this ceiling.
///
/// It sits well below the true `u32` physical ceiling a relation id narrows to at
/// every boundary — the SAL group header carries it as a u32 (`sal_begin_group`),
/// `Table` stores `table_id: u32`, `Table::new`/`ShardIndex::new` take
/// a u32, `Batch::encode_to_wire` stamps a u32, and shard **file names on disk**
/// embed it (`shard_{tid}_{lsn}.db`). The `i64` used for `dag.tables` keys and
/// `target_id` parameters is a convenience width over that u32. `1<<31` is a safe
/// tripwire far short of any narrowing: an id there round-trips every one of them
/// exactly.
pub(super) const RELATION_ID_CEILING: i64 = 1 << 31;

pub(super) const SYS_CATALOG_DIRNAME: &str = "_system_catalog";

pub(super) const SCHEMA_TAB_ID: i64 = gnitz_wire::SCHEMA_TAB as i64;
pub(super) const TABLE_TAB_ID: i64 = gnitz_wire::TABLE_TAB as i64;
pub(super) const VIEW_TAB_ID: i64 = gnitz_wire::VIEW_TAB as i64;
pub(super) const COL_TAB_ID: i64 = gnitz_wire::COL_TAB as i64;
pub(super) const IDX_TAB_ID: i64 = gnitz_wire::IDX_TAB as i64;
pub(crate) const SEQ_TAB_ID: i64 = gnitz_wire::SEQ_TAB as i64;
pub(super) const CIRCUIT_NODES_TAB_ID: i64 = gnitz_wire::CIRCUIT_NODES_TAB as i64;
pub(super) const CIRCUIT_EDGES_TAB_ID: i64 = gnitz_wire::CIRCUIT_EDGES_TAB as i64;
pub(super) const CIRCUIT_NODE_COLUMNS_TAB_ID: i64 = gnitz_wire::CIRCUIT_NODE_COLUMNS_TAB as i64;

// PK list encoding lives in gnitz-wire so the client and engine cannot drift
// on the on-disk format. Production code spells the packers `gnitz_wire::…` (or
// reaches them via the row decoders below); the unqualified names are used only
// by tests, so their re-exports are test-scoped.
pub(super) use gnitz_wire::unpack_pk_cols;
pub(crate) use gnitz_wire::PkColList;
#[cfg(test)]
pub(super) use gnitz_wire::{pack_pk_cols, PK_LIST_MAX_COLS, PK_LIST_PACKED_FLAG};

/// Hard-validate a decoded PK list against the table's columns. Shared by
/// the production wire path (`hook_table_register`) and the test-only
/// `ddl.rs::create_table` so both reject identically rather than falling
/// through to a `SchemaDescriptor::new` `assert!`. `pk.decoded_count()`
/// (the raw decoded count, not the clamped slice length) is what gates
/// `1..=PK_LIST_MAX_COLS`, so a crafted over-range count is rejected rather
/// than silently truncated.
pub(super) fn validate_pk_cols(col_defs: &[ColumnDef], pk: &PkColList) -> Result<(), String> {
    if !pk.is_well_formed() {
        return Err(format!(
            "Primary Key column count {} out of range 1..={}",
            pk.decoded_count(),
            gnitz_wire::PK_LIST_MAX_COLS
        ));
    }
    // The rule set is `gnitz-wire`'s, shared with the SQL planner's CREATE TABLE
    // pre-check and the client's `validate_parts`; only the wording below is the
    // catalog's. The stride bound defends the catalog worker against a crafted
    // SAL-replayed `TABLE_TAB` ingest whose decoded PK list packs an oversized
    // region.
    gnitz_wire::validate_pk_tuple(pk.as_slice(), col_defs.len(), |c| {
        let cd = &col_defs[c as usize];
        (cd.type_code, cd.is_nullable)
    })
    .map(|_stride| ())
    .map_err(|rule| match rule {
        gnitz_wire::PkRule::IndexOutOfRange { .. } => "Primary Key index out of bounds".to_string(),
        gnitz_wire::PkRule::NotEligible { type_code, .. } => format!(
            "Primary Key must be a fixed-width integer, U128, UUID, or I128 column; \
             got type_code={type_code} (String, Blob, and float columns cannot be PK)"
        ),
        gnitz_wire::PkRule::Nullable { .. } => "Primary Key column must not be nullable".to_string(),
        gnitz_wire::PkRule::Duplicate { .. } => "Primary Key has duplicate column".to_string(),
        gnitz_wire::PkRule::StrideOutOfRange { stride } => format!(
            "Primary Key total stride must be 1..={} bytes, got {stride}",
            crate::schema::MAX_PK_BYTES
        ),
        other => other.to_string(),
    })
}

/// The two rules a relation's column records must satisfy on their own, with no
/// PK list in hand. Sole home, called both by [`validate_relation_defs`] (which
/// only decorates the message with the relation's kind/name/id) and by
/// `build_schema_from_col_defs`, so the precheck arms, the register hooks, boot
/// replay and worker `ddl_sync` all reject identically without either entry
/// point owning a copy.
///
/// `validate_pk_cols` allow-lists the PK columns' type codes; nothing checked a
/// *payload* column's, and this is the trust boundary where COL_TAB rows become
/// a `SchemaDescriptor` (see `gnitz_wire::is_valid_type_code` for why an unknown
/// code is not inert).
pub(super) fn check_col_defs(col_defs: &[ColumnDef]) -> Result<(), String> {
    if col_defs.len() > crate::schema::MAX_COLUMNS {
        return Err(format!(
            "has {} columns (max {})",
            col_defs.len(),
            crate::schema::MAX_COLUMNS
        ));
    }
    if let Some(cd) = col_defs.iter().find(|cd| !gnitz_wire::is_valid_type_code(cd.type_code)) {
        return Err(format!("column '{}' has invalid type code {}", cd.name, cd.type_code));
    }
    Ok(())
}

/// One admissibility check for a relation's column records + PK list, shared
/// by the TABLE/VIEW precheck arms, both register hooks (the deliberate
/// precheck/hook double-run — boot replay and worker ddl_sync skip precheck),
/// and the test-only `ddl.rs::create_table`, so every layer rejects
/// identically. Non-empty col-defs first (the cross-family
/// COL_TAB-before-TABLE/VIEW ordering contract), then [`check_col_defs`], then
/// [`validate_pk_cols`].
pub(super) fn validate_relation_defs(
    kind: &str,
    id: i64,
    name: &str,
    col_defs: &[ColumnDef],
    pk: &PkColList,
) -> Result<(), String> {
    if col_defs.is_empty() {
        return Err(format!(
            "catalog invariant violated: {kind} '{name}' (id={id}) registered \
             before its column records. COL_TAB writes must precede \
             TABLE_TAB/VIEW_TAB writes (see hooks.rs dispatch doc)."
        ));
    }
    check_col_defs(col_defs).map_err(|e| format!("{kind} '{name}' (id={id}) {e}"))?;
    validate_pk_cols(col_defs, pk)
}

// ---------------------------------------------------------------------------
// Per-family row-view decoders — the one reading of each family's payload
// layout, shared by the precheck arms (`precheck_family`) and the register
// hooks (which re-decode on the paths that skip precheck: boot replay and
// worker ddl_sync).
// ---------------------------------------------------------------------------

/// Decode TABLE_TAB row `i`: `(schema_id, name, pk_list, flags)`.
pub(super) fn read_table_tab_row(batch: &Batch, i: usize) -> (i64, String, PkColList, u64) {
    (
        batch.read_payload_u64(i, TABTAB_PAY_SCHEMA_ID) as i64,
        batch.read_payload_string(i, TABTAB_PAY_NAME),
        unpack_pk_cols(batch.read_payload_u64(i, TABTAB_PAY_PK_COL_IDX)),
        batch.read_payload_u64(i, TABTAB_PAY_FLAGS),
    )
}

/// Decode VIEW_TAB row `i`: `(schema_id, name, pk_list)`. The pk_list is the
/// view's persisted leading-k column list; a bare `0` decodes back to `[0]`.
pub(super) fn read_view_tab_row(batch: &Batch, i: usize) -> (i64, String, PkColList) {
    (
        batch.read_payload_u64(i, VIEWTAB_PAY_SCHEMA_ID) as i64,
        batch.read_payload_string(i, VIEWTAB_PAY_NAME),
        unpack_pk_cols(batch.read_payload_u64(i, VIEWTAB_PAY_PK_COL_IDX)),
    )
}

/// Decode IDX_TAB row `i`: `(owner_id, source_cols, is_unique)`. `source_cols`
/// carries `pack_pk_cols(&col_indices)` (a single-column index is the
/// 1-element degenerate case). The name column is deliberately not decoded —
/// the hook path never reads it, and it would be a wasted allocation there.
pub(super) fn read_idx_tab_row(batch: &Batch, i: usize) -> (i64, PkColList, bool) {
    (
        batch.read_payload_u64(i, IDXTAB_PAY_OWNER_ID) as i64,
        unpack_pk_cols(batch.read_payload_u64(i, IDXTAB_PAY_SOURCE_COLS)),
        batch.read_payload_u64(i, IDXTAB_PAY_IS_UNIQUE) != 0,
    )
}

/// Cursor sibling of [`read_idx_tab_row`]: `(owner_id, source_cols, is_unique)`
/// of the row a cursor is positioned on.
pub(super) fn read_idx_tab_cursor_row(cursor: &crate::storage::ReadCursor) -> (i64, PkColList, bool) {
    (
        super::cursor_read_u64(cursor, IDXTAB_COL_OWNER_ID) as i64,
        unpack_pk_cols(super::cursor_read_u64(cursor, IDXTAB_COL_SOURCE_COLS)),
        super::cursor_read_u64(cursor, IDXTAB_COL_IS_UNIQUE) != 0,
    )
}

/// The `(owner_id, packed_source_cols, col_indices)` of every UNIQUE index this
/// IDX_TAB family creates — positive-weight rows whose column list is
/// well-formed. The DDL driver pre-flights each one before the bundle is made
/// durable; `packed` is the same word the unique-filter map is keyed by.
pub(crate) fn idx_tab_unique_creates(batch: &Batch) -> Vec<(i64, u64, PkColList)> {
    (0..batch.count)
        .filter(|&i| batch.get_weight(i) > 0)
        .filter_map(|i| {
            let (owner_id, cols, is_unique) = read_idx_tab_row(batch, i);
            let packed = batch.read_payload_u64(i, IDXTAB_PAY_SOURCE_COLS);
            (is_unique && cols.is_well_formed()).then_some((owner_id, packed, cols))
        })
        .collect()
}

/// The `(owner_id, packed_source_cols)` of every index this IDX_TAB family drops
/// — its negative-weight rows. The DDL driver clears each pair's unique filter
/// once the drop is durable.
pub(crate) fn idx_tab_drops(batch: &Batch) -> Vec<(i64, u64)> {
    (0..batch.count)
        .filter(|&i| batch.get_weight(i) < 0)
        .map(|i| {
            (
                batch.read_payload_u64(i, IDXTAB_PAY_OWNER_ID) as i64,
                batch.read_payload_u64(i, IDXTAB_PAY_SOURCE_COLS),
            )
        })
        .collect()
}

/// What one delta does to one PK: where its `-1` and `+1` rows are, and the
/// summed weight. A PK carrying both signs is a **rewrite pair** (a rename) —
/// the shape §3.1/§3.2 key on. This is the one decoding of that shape; every
/// precheck guard and pair-sensitive hook reads it instead of rescanning.
pub(super) struct PkSignature {
    pub(super) pk: u128,
    /// First row index carrying this PK; its OPK bytes address the live row.
    pub(super) row: usize,
    /// First `-1` / `+1` row index for this PK.
    pub(super) neg: Option<usize>,
    pub(super) pos: Option<usize>,
    /// A sign occurs on more than one row (never legitimate for COL_TAB).
    pub(super) repeats_a_sign: bool,
    pub(super) sum: i64,
}

impl PkSignature {
    /// A rewrite pair — both signs on one PK, whose net stays live.
    pub(super) fn is_pair(&self) -> bool {
        self.neg.is_some() && self.pos.is_some()
    }

    /// The DDL verb this shape performs, for guard messages.
    pub(super) fn verb(&self) -> &'static str {
        match (self.neg.is_some(), self.pos.is_some()) {
            (true, true) => "ALTER",
            (true, false) => "DROP",
            _ => "CREATE",
        }
    }
}

/// One [`PkSignature`] per distinct PK, in first-appearance order. Zero-weight
/// rows are skipped. Quadratic in the batch, which is bounded by one DDL bundle
/// (`MAX_COLUMNS` rows for COL_TAB, a handful of relations otherwise).
pub(super) fn pk_signatures(batch: &Batch) -> Vec<PkSignature> {
    let mut sigs: Vec<PkSignature> = Vec::new();
    for i in 0..batch.count {
        let pk = batch.get_pk(i);
        let w = batch.get_weight(i);
        if w == 0 {
            continue;
        }
        let sig = match sigs.iter_mut().find(|s| s.pk == pk) {
            Some(s) => s,
            None => {
                sigs.push(PkSignature {
                    pk,
                    row: i,
                    neg: None,
                    pos: None,
                    repeats_a_sign: false,
                    sum: 0,
                });
                sigs.last_mut().expect("just pushed")
            }
        };
        sig.sum += w;
        // Keep the FIRST row of each sign; a later one only sets the flag.
        match (w < 0, if w < 0 { sig.neg } else { sig.pos }) {
            (_, Some(_)) => sig.repeats_a_sign = true,
            (true, None) => sig.neg = Some(i),
            (false, None) => sig.pos = Some(i),
        }
    }
    sigs
}

/// The PKs this family creates (`positive`) or drops, EXCLUDING any PK carrying
/// BOTH signs — a rewrite pair, e.g. a rename's `-1,+1`. A weight-homogeneous
/// CREATE/DROP family has no such pair; a rename's paired PK is filtered from
/// both answers, so a view rename triggers no backfill and a table rename's tid
/// is never treated as dropped. Batch-local: the whole family (both signs of a
/// pair) arrives as one batch on every path.
pub(crate) fn family_pks_by_sign(batch: &Batch, positive: bool) -> Vec<i64> {
    pk_signatures(batch)
        .into_iter()
        .filter(|s| !s.is_pair() && if positive { s.pos.is_some() } else { s.neg.is_some() })
        .map(|s| s.pk as i64)
        .collect()
}

// ---------------------------------------------------------------------------
// Per-family row builders — the engine's entry points into the shared
// `gnitz_wire::sys_rows` codecs, which own each family's payload layout for
// both sides of the wire. These adapt the catalog's `i64` ids and `ColumnDef`
// to a codec row, for every engine-side writer: bootstrap's self-description,
// the DDL emitters, and the test fixtures that drive the applier directly.
// ---------------------------------------------------------------------------

/// Append one COL_TAB row for column `col_idx` of `owner_id`. Takes the whole
/// `ColumnDef` rather than its fields, mirroring `registry.rs`'s read side,
/// which reassembles exactly this struct.
pub(super) fn push_col_tab_row(
    bb: &mut BatchBuilder,
    owner_id: i64,
    owner_kind: i64,
    col_idx: i64,
    cd: &ColumnDef,
    weight: i64,
) {
    // Catalog callers pass DDL-validated ids, so an out-of-range one here is
    // catalog corruption — abort rather than alias another column's record.
    gnitz_wire::sys_rows::write_col_tab_row(
        bb,
        &ColTabRow {
            owner_id: owner_id as u64,
            owner_kind: owner_kind as u64,
            col_idx: col_idx as u64,
            name: &cd.name,
            type_code: cd.type_code as u64,
            is_nullable: cd.is_nullable,
            fk_table_id: cd.fk_table_id as u64,
            fk_col_idx: cd.fk_col_idx as u64,
            is_serial: cd.is_serial,
            is_hidden: cd.is_hidden,
        },
        weight,
    )
    .expect("catalog col-id packing out of range");
}

/// Append one TABLE_TAB row at `weight`.
pub(super) fn push_table_tab_row(
    bb: &mut BatchBuilder,
    tid: i64,
    schema_id: i64,
    name: &str,
    pk_col_idx: u64,
    flags: u64,
    weight: i64,
) {
    gnitz_wire::sys_rows::write_table_tab_row(
        bb,
        &TableTabRow {
            table_id: tid as u64,
            schema_id: schema_id as u64,
            name,
            pk_col_idx,
            flags,
        },
        weight,
    );
}

/// The one-row IDX_TAB batch at `weight` — `+1` registers an index, `-1`
/// retracts one. A `-1` must reproduce the `+1`'s payload exactly: the
/// retraction CAS rejects a mismatch, and only byte-equal `(PK, payload)` rows
/// cancel. Every engine-side IDX_TAB write is a single row; multi-row batches
/// come from the client.
pub(super) fn idx_tab_batch(
    index_id: i64,
    owner_id: i64,
    packed_cols: u64,
    name: &str,
    is_unique: bool,
    weight: i64,
) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Index.schema());
    gnitz_wire::sys_rows::write_idx_tab_row(
        &mut bb,
        &IdxTabRow {
            index_id: index_id as u64,
            owner_id: owner_id as u64,
            source_col_idx: packed_cols,
            name,
            is_unique: is_unique as u64,
        },
        weight,
    );
    bb.finish()
}

// ---------------------------------------------------------------------------
// Schema derivation from the shared wire column slices
// ---------------------------------------------------------------------------

use crate::schema::from_wire_cols;

// Pre-computed schema statics, one per family, indexed by `SysFamily`
// discriminant — initialised at compile time, never reconstructed. `from_wire_cols`
// places every family `Replicated`, so a reader single-sources one copy instead of
// gathering N (`DagEngine::relation_is_replicated`).
static SCHEMAS: [SchemaDescriptor; SysFamily::COUNT] = {
    let mut arr = [from_wire_cols(SYS_FAMILIES[0].wire.cols, SYS_FAMILIES[0].wire.pk_cols); SysFamily::COUNT];
    let mut i = 1;
    while i < SysFamily::COUNT {
        arr[i] = from_wire_cols(SYS_FAMILIES[i].wire.cols, SYS_FAMILIES[i].wire.pk_cols);
        i += 1;
    }
    arr
};

// ---------------------------------------------------------------------------
// PK packing helpers
// ---------------------------------------------------------------------------

/// Delegates to the shared `gnitz_wire::pack_col_id` codec. Catalog callers
/// pass DDL-validated ids, so an out-of-range value here is catalog
/// corruption — abort rather than alias another column's record.
pub(super) fn pack_column_id(owner_id: i64, col_idx: i64) -> u64 {
    gnitz_wire::pack_col_id(owner_id as u64, col_idx as u64).expect("catalog col-id packing out of range")
}

/// Pack a circuit compound PK `(view_id, sub)` into the `u128` whose
/// `extend_pk` (big-endian) at-rest image is OPK column order: `view_id_BE`
/// (bytes 0..8) then `sub_BE` (bytes 8..16), so a view_id prefix seek lands on
/// the leading bytes. Pinned by `pack_view_pk_at_rest_is_view_id_leading_opk`.
/// `sub` is the per-view secondary (node_id or an edge/node-column field pack).
///
/// Test-only: production circuit rows arrive pre-packed from the client, which
/// reaches the same at-rest image through a different encoder (see
/// `create_view_chain`).
#[cfg(test)]
pub(super) fn pack_view_pk(view_id: i64, sub: u64) -> u128 {
    ((view_id as u64 as u128) << 64) | (sub as u128)
}

// ---------------------------------------------------------------------------
// Per-family descriptor table
// ---------------------------------------------------------------------------

pub(crate) struct SysFamilyInfo {
    /// The family's shared wire identity: its table id, its name (which is also
    /// its subdirectory under `_system_catalog/`), and the column shape the
    /// schema, the COL_TAB self-description rows, and the client's `Schema` all
    /// derive from. Held by reference so the engine restates none of it.
    pub(crate) wire: &'static gnitz_wire::WireSysFamily,
    /// Topological creation priority. Lower = earlier in the dependency chain
    /// (created first, destroyed last). Orders the `DDL_TXN` handler's
    /// ascending forward ingest (so every register/index hook sees its
    /// dependencies already in the memtable) and rollback's descending negate.
    /// Distinct for Table and View so their relative order is stable; 99 =
    /// order-neutral (Sequence, matching the non-family default).
    pub(crate) topo_priority: u8,
}

impl SysFamilyInfo {
    /// This family's table id, narrowed to the `i64` the catalog storage edge
    /// and every `sys_*` signature use.
    #[inline]
    pub(crate) const fn id(&self) -> i64 {
        self.wire.id as i64
    }
}

/// Bind a family's shared wire descriptor to its engine-only topological
/// priority. Const-panics on an id `gnitz-wire` does not know, so the table
/// below cannot name a family that does not exist.
const fn fam(id: u64, topo_priority: u8) -> SysFamilyInfo {
    match gnitz_wire::sys_family(id) {
        Some(wire) => SysFamilyInfo { wire, topo_priority },
        None => panic!("not a wire system family"),
    }
}

/// One descriptor per system family, indexed by `SysFamily` discriminant
/// (asserted below).
pub(crate) const SYS_FAMILIES: [SysFamilyInfo; SysFamily::COUNT] = [
    fam(gnitz_wire::SCHEMA_TAB, 0),
    fam(gnitz_wire::TABLE_TAB, 5),
    fam(gnitz_wire::VIEW_TAB, 6),
    fam(gnitz_wire::COL_TAB, 1),
    fam(gnitz_wire::IDX_TAB, 7),
    fam(gnitz_wire::SEQ_TAB, 99),
    fam(gnitz_wire::CIRCUIT_NODES_TAB, 2),
    fam(gnitz_wire::CIRCUIT_EDGES_TAB, 3),
    fam(gnitz_wire::CIRCUIT_NODE_COLUMNS_TAB, 4),
];

// `SYS_FAMILIES[f.index()]` must describe family `f`: verify the array order
// against `from_id` (the ground-truth id mapping) at compile time.
const _: () = {
    let mut i = 0;
    while i < SysFamily::COUNT {
        match SysFamily::from_id(SYS_FAMILIES[i].id()) {
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
    Sequence,
    CircuitNodes,
    CircuitEdges,
    CircuitNodeColumns,
}

impl SysFamily {
    pub(crate) const COUNT: usize = 9;

    /// Discriminant index into the per-family arrays.
    #[inline]
    pub(crate) const fn index(self) -> usize {
        self as usize
    }

    /// The `*_TAB_ID` constant for this family.
    #[inline]
    pub(crate) const fn id(self) -> i64 {
        SYS_FAMILIES[self.index()].id()
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

    /// Topological creation priority (see [`SysFamilyInfo::topo_priority`]).
    #[inline]
    pub(crate) fn topo_priority(self) -> u8 {
        self.info().topo_priority
    }

    /// Inverse of [`Self::id`]; `None` for any id that is not a system family.
    pub(crate) const fn from_id(id: i64) -> Option<Self> {
        match id {
            SCHEMA_TAB_ID => Some(SysFamily::Schema),
            TABLE_TAB_ID => Some(SysFamily::Table),
            VIEW_TAB_ID => Some(SysFamily::View),
            COL_TAB_ID => Some(SysFamily::Column),
            IDX_TAB_ID => Some(SysFamily::Index),
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
        ] {
            assert_eq!(schema.pk_indices(), &[0, 1], "circuit PK must be (col0, col1)");
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

    #[test]
    fn family_pks_by_sign_separates_a_drop_from_a_rename() {
        // A plain `-1` TABLE_TAB row is a DROP.
        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        push_table_tab_row(&mut bb, 42, PUBLIC_SCHEMA_ID, "t", 0, 0, -1);
        let dropped = bb.finish();
        assert_eq!(family_pks_by_sign(&dropped, false), vec![42]);
        assert!(family_pks_by_sign(&dropped, true).is_empty());

        // A rename is a `(-1, +1)` rewrite pair on one PK: neither a create nor a
        // drop, so DDL paths keyed off these lists leave a renamed table alone.
        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        push_table_tab_row(&mut bb, 42, PUBLIC_SCHEMA_ID, "t", 0, 0, -1);
        push_table_tab_row(&mut bb, 42, PUBLIC_SCHEMA_ID, "t2", 0, 0, 1);
        let renamed = bb.finish();
        assert!(family_pks_by_sign(&renamed, false).is_empty());
        assert!(family_pks_by_sign(&renamed, true).is_empty());
    }
}
