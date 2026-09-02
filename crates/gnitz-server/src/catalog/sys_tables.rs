//! System table constants, the per-family descriptor table, PK packing
//! helpers, and the per-family row codecs (batch row-view decoders and
//! row builders) over those constants.
//!
//! Pure data and stateless codecs — no state, no CatalogEngine dependency.

use super::ColumnDef;
use gnitz_store::relation::RelationKind;
use gnitz_store::schema::{Placement, SchemaDescriptor};
use gnitz_store::storage::{Batch, BatchBuilder};
use gnitz_wire::sys_rows::{ColTabRow, IdxTabRow, TableTabRow};

use gnitz_expr::RowSource;
use gnitz_wire::{
    COLTAB_PAY_COL_IDX, COLTAB_PAY_FK_COL_IDX, COLTAB_PAY_FK_TABLE_ID, COLTAB_PAY_IS_HIDDEN, COLTAB_PAY_IS_NULLABLE,
    COLTAB_PAY_IS_SERIAL, COLTAB_PAY_NAME, COLTAB_PAY_OWNER_ID, COLTAB_PAY_OWNER_KIND, COLTAB_PAY_TYPE_CODE,
    IDXTAB_PAY_FLAGS, IDXTAB_PAY_OWNER_ID, IDXTAB_PAY_SOURCE_COLS, TABTAB_PAY_FLAGS, TABTAB_PAY_NAME,
    TABTAB_PAY_PK_COL_IDX, TABTAB_PAY_SCHEMA_ID, VIEWTAB_PAY_CAPACITY, VIEWTAB_PAY_DELTA, VIEWTAB_PAY_NAME,
    VIEWTAB_PAY_OWNER_VIEW_ID, VIEWTAB_PAY_PK_COL_IDX, VIEWTAB_PAY_SCHEMA_ID,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(super) const SYSTEM_SCHEMA_ID: i64 = 1;
pub(crate) const PUBLIC_SCHEMA_ID: i64 = 2;
pub(super) const FIRST_USER_SCHEMA_ID: i64 = gnitz_wire::FIRST_USER_SCHEMA_ID as i64;

pub(super) const OWNER_KIND_TABLE: i64 = gnitz_wire::OWNER_KIND_TABLE as i64;
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

/// The durable relation-id tripwire in this crate's `i64` id width; the value and
/// its rationale live on [`gnitz_wire::RELATION_ID_CEILING`].
/// `allocate_table_id` and `precheck_family` (the point an id enters the registry)
/// reject any id at or above it.
pub(super) const RELATION_ID_CEILING: i64 = gnitz_wire::RELATION_ID_CEILING as i64;

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
pub(in crate::catalog) use gnitz_wire::PkColList;
#[cfg(test)]
pub(super) use gnitz_wire::{pack_pk_cols, PK_LIST_MAX_COLS, PK_LIST_PACKED_FLAG};

/// Hard-validate a decoded PK list against the table's columns. Shared by
/// the production wire path (`hook_relation_register`) and the test-only
/// `create_table` fixture so both reject identically rather than falling
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
    validate_pk_against_cols(col_defs, pk.as_slice())
}

/// The PK rule set a column-record list must satisfy, rendered in catalog
/// vocabulary. The rules themselves are `gnitz-wire`'s, shared with the SQL
/// planner's CREATE TABLE pre-check and the client's `validate_parts`; only the
/// wording here is the catalog's. The stride bound defends the catalog worker
/// against a crafted SAL-replayed `TABLE_TAB` ingest whose decoded PK list packs
/// an oversized region.
///
/// The one catalog-side spelling: [`validate_pk_cols`] applies it as the early
/// client-facing reject, and `build_schema_from_col_defs` applies it again over
/// the *same* pair it is about to construct from — which is what makes that
/// builder total where `SchemaDescriptor::new_with_placement` would abort.
pub(super) fn validate_pk_against_cols(col_defs: &[ColumnDef], pk_cols: &[u32]) -> Result<(), String> {
    gnitz_wire::validate_pk_tuple(pk_cols, col_defs.len(), |c| {
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
            gnitz_store::schema::MAX_PK_BYTES
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
    // Reachable from a plain view as well as a wide CREATE TABLE: a compound-PK
    // plain projection prepends the k source PK columns, so `SELECT *` over a wide
    // compound-PK table can cross MAX_COLUMNS.
    if col_defs.len() > gnitz_store::schema::MAX_COLUMNS {
        return Err(format!(
            "has {} columns (max {})",
            col_defs.len(),
            gnitz_store::schema::MAX_COLUMNS
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
/// and the test-only `create_table` fixture, so every layer rejects
/// identically. Non-empty col-defs first (the cross-family
/// COL_TAB-before-TABLE/VIEW ordering contract), then [`check_col_defs`], then
/// [`validate_pk_cols`].
pub(super) fn validate_relation_defs(
    kind: gnitz_store::relation::RelationKind,
    id: i64,
    name: &str,
    col_defs: &[ColumnDef],
    pk: &PkColList,
) -> Result<(), String> {
    let noun = kind.noun();
    if col_defs.is_empty() {
        return Err(format!(
            "catalog invariant violated: {noun} '{name}' (id={id}) registered \
             before its column records: the register hook reads them back \
             through sys_columns storage, which held none for this id."
        ));
    }
    check_col_defs(col_defs).map_err(|e| format!("{noun} '{name}' (id={id}) {e}"))?;
    validate_pk_cols(col_defs, pk)
}

// ---------------------------------------------------------------------------
// Per-family row-view decoders — the one reading of each family's payload
// layout, shared by the precheck arms (`precheck_family`) and the register
// hooks (which re-decode on the paths that skip precheck: boot replay and
// worker ddl_sync).
//
// All four are generic over `RowSource`, so a wire `Batch`, a `StoredRow` a PK
// probe located, and a positioned `ReadCursor`'s entry decode through one
// reading each.
// ---------------------------------------------------------------------------

/// Decode TABLE_TAB `row`: `(schema_id, name, pk_list, kind, placement)`. The
/// raw `flags` word does not escape — every registration crosses this reader, so
/// decoding it here is what makes the rejection below unskippable on the paths
/// that bypass the precheck. `id` names the row in that message.
pub(super) fn read_table_tab_row<S: RowSource>(
    src: &S,
    row: usize,
    id: i64,
) -> Result<(i64, String, PkColList, RelationKind, Placement), String> {
    let name = sys_string(src, row, TABTAB_PAY_NAME);
    let props = gnitz_wire::TableProps::from_flags(sys_u64(src, row, TABTAB_PAY_FLAGS));
    let placement = if props.replicated {
        if props.dist_prefix_len != 0 {
            return Err(format!(
                "catalog invariant violated: table '{name}' (tid={id}) is replicated and carries \
                 a non-default distribution prefix (k={}); these are mutually exclusive.",
                props.dist_prefix_len
            ));
        }
        Placement::Replicated
    } else {
        Placement::Keyed {
            prefix_len: props.dist_prefix_len as u8,
        }
    };
    Ok((
        sys_u64(src, row, TABTAB_PAY_SCHEMA_ID) as i64,
        name,
        unpack_pk_cols(sys_u64(src, row, TABTAB_PAY_PK_COL_IDX)),
        if props.stream {
            RelationKind::Stream
        } else {
            RelationKind::BaseTable
        },
        placement,
    ))
}

/// Decode VIEW_TAB `row`: `(schema_id, name, pk_list, budgets, owner_view_id)`.
/// A bare `0` pk_list decodes back to `[0]`; a `0` budget word decodes to `None`
/// here so no caller repeats the sentinel. `owner_view_id` is `0` for a user
/// view.
pub(super) fn read_view_tab_row<S: RowSource>(
    src: &S,
    row: usize,
) -> (i64, String, PkColList, gnitz_store::relation::ViewBudgets, i64) {
    (
        sys_u64(src, row, VIEWTAB_PAY_SCHEMA_ID) as i64,
        sys_string(src, row, VIEWTAB_PAY_NAME),
        unpack_pk_cols(sys_u64(src, row, VIEWTAB_PAY_PK_COL_IDX)),
        gnitz_store::relation::ViewBudgets {
            capacity_bytes: Some(sys_u64(src, row, VIEWTAB_PAY_CAPACITY)).filter(|&b| b != 0),
            delta_bytes: Some(sys_u64(src, row, VIEWTAB_PAY_DELTA)).filter(|&b| b != 0),
        },
        sys_u64(src, row, VIEWTAB_PAY_OWNER_VIEW_ID) as i64,
    )
}

/// One row's fixed 8-byte payload slot `pi`, little-endian. `RowSource` is the
/// whole surface a system row needs, which is what lets the decoders below read
/// a wire `Batch`, a probed `StoredRow` and a positioned `ReadCursor` alike.
fn sys_u64<S: RowSource>(src: &S, row: usize, pi: usize) -> u64 {
    let cell = src.get_col_ptr(row, pi, 8);
    u64::from_le_bytes(cell.try_into().unwrap_or([0; 8]))
}

/// One row's German-string payload slot `pi`, resolved through the source's own
/// blob heap (so a value over 12 bytes reads back whole).
fn sys_string<S: RowSource>(src: &S, row: usize, pi: usize) -> String {
    let cell = src.get_col_ptr(row, pi, 16);
    String::from_utf8(gnitz_wire::german_string_content(cell, src.blob()).to_vec()).unwrap_or_default()
}

/// Decode IDX_TAB `row`: `(owner_id, source_cols, props)`. `source_cols`
/// carries `pack_pk_cols(&col_indices)` (a single-column index is the
/// 1-element degenerate case).
pub(super) fn read_idx_tab_row<S: RowSource>(src: &S, row: usize) -> (i64, PkColList, gnitz_wire::IndexProps) {
    (
        sys_u64(src, row, IDXTAB_PAY_OWNER_ID) as i64,
        unpack_pk_cols(sys_u64(src, row, IDXTAB_PAY_SOURCE_COLS)),
        gnitz_wire::IndexProps::from_flags(sys_u64(src, row, IDXTAB_PAY_FLAGS)),
    )
}

/// Decode COL_TAB `row` into the `ColumnDef` the schema builder consumes.
pub(super) fn read_col_tab_row<S: RowSource>(src: &S, row: usize) -> ColumnDef {
    ColumnDef {
        name: sys_string(src, row, COLTAB_PAY_NAME),
        type_code: sys_u64(src, row, COLTAB_PAY_TYPE_CODE) as u8,
        is_nullable: sys_u64(src, row, COLTAB_PAY_IS_NULLABLE) != 0,
        fk_table_id: sys_u64(src, row, COLTAB_PAY_FK_TABLE_ID) as i64,
        fk_col_idx: sys_u64(src, row, COLTAB_PAY_FK_COL_IDX) as u32,
        is_serial: sys_u64(src, row, COLTAB_PAY_IS_SERIAL) != 0,
        is_hidden: sys_u64(src, row, COLTAB_PAY_IS_HIDDEN) != 0,
    }
}

/// What a COL_TAB row claims about its own identity: the `(owner, column)` its
/// payload names, and the FK target it declares. Separate from
/// [`read_col_tab_row`] so the cache appliers and the bundle guard never pay
/// that decoder's `name` allocation.
pub(super) struct ColTabIdent {
    pub(super) owner_id: i64,
    pub(super) owner_kind: i64,
    pub(super) col_idx: u64,
    pub(super) fk_table_id: i64,
    pub(super) fk_col_idx: u32,
}

impl ColTabIdent {
    /// Does this row declare a foreign key? An FK constrains a *base table's*
    /// column; a view's COL_TAB rows are clones of the projected source defs,
    /// so they carry the source's `fk_table_id` without being a constraint
    /// themselves — reading one as a child would put a view id in a base
    /// table's lock set and fail every parent DELETE on the view's missing FK
    /// index.
    pub(super) fn declares_fk(&self) -> bool {
        self.fk_table_id != 0 && self.owner_kind == OWNER_KIND_TABLE
    }
}

/// Decode COL_TAB `row`'s identity fields. A struct rather than a 5-tuple:
/// `(owner_id, owner_kind, col_idx, fk_table_id)` are four adjacent integers
/// and `(col_idx, fk_col_idx)` index two different column spaces, so a
/// transposed field would type-check.
pub(super) fn read_col_tab_ident<S: RowSource>(src: &S, row: usize) -> ColTabIdent {
    ColTabIdent {
        owner_id: sys_u64(src, row, COLTAB_PAY_OWNER_ID) as i64,
        owner_kind: sys_u64(src, row, COLTAB_PAY_OWNER_KIND) as i64,
        col_idx: sys_u64(src, row, COLTAB_PAY_COL_IDX),
        fk_table_id: sys_u64(src, row, COLTAB_PAY_FK_TABLE_ID) as i64,
        fk_col_idx: sys_u64(src, row, COLTAB_PAY_FK_COL_IDX) as u32,
    }
}

/// The `(owner_id, packed_source_cols, col_indices)` of every UNIQUE index this
/// IDX_TAB family creates — positive-weight rows whose column list is
/// well-formed. The DDL driver pre-flights each one before the bundle is made
/// durable; `packed` is the same word the unique-filter map is keyed by.
pub(crate) fn idx_tab_unique_creates(batch: &Batch) -> Vec<(i64, u64, PkColList)> {
    (0..batch.len())
        .filter(|&i| batch.get_weight(i) > 0)
        .filter_map(|i| {
            let (owner_id, cols, props) = read_idx_tab_row(batch, i);
            let packed = batch.read_payload_u64(i, IDXTAB_PAY_SOURCE_COLS);
            (props.is_unique && cols.is_well_formed()).then_some((owner_id, packed, cols))
        })
        .collect()
}

/// The `(owner_id, packed_source_cols)` of every index this IDX_TAB family drops
/// — its negative-weight rows. The DDL driver clears each pair's unique filter
/// once the drop is durable.
pub(crate) fn idx_tab_drops(batch: &Batch) -> Vec<(i64, u64)> {
    (0..batch.len())
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
/// the shape the sign-partition and the net-live gates key on. This is the
/// one decoding of that shape; every
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
/// rows are skipped.
///
/// Linear in the batch: the boot replay hands `fire_hooks` a full-family scan
/// (`replay_system_table`), so the input is every live row of COL_TAB or
/// TABLE_TAB, not one DDL bundle — a scan over accumulated signatures would make
/// boot quadratic in the total column count.
pub(super) fn pk_signatures(batch: &Batch) -> Vec<PkSignature> {
    let mut sigs: Vec<PkSignature> = Vec::new();
    let mut by_pk: rustc_hash::FxHashMap<u128, usize> = rustc_hash::FxHashMap::default();
    for i in 0..batch.len() {
        let pk = batch.get_pk(i);
        let w = batch.get_weight(i);
        if w == 0 {
            continue;
        }
        let slot = *by_pk.entry(pk).or_insert_with(|| {
            sigs.push(PkSignature {
                pk,
                row: i,
                neg: None,
                pos: None,
                repeats_a_sign: false,
                sum: 0,
            });
            sigs.len() - 1
        });
        let sig = &mut sigs[slot];
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
    props: gnitz_wire::IndexProps,
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
            flags: props.pack(),
        },
        weight,
    );
    bb.finish()
}

// ---------------------------------------------------------------------------
// Schema derivation from the shared wire column slices
// ---------------------------------------------------------------------------

use gnitz_store::schema::from_wire_cols;

// Pre-computed schema statics, one per family, indexed by `SysFamily`
// discriminant — initialised at compile time, never reconstructed. `from_wire_cols`
// places every family `Replicated`, so a reader single-sources one copy instead of
// gathering N (`RelationRegistry::relation_is_replicated`).
static SCHEMAS: [SchemaDescriptor; SysFamily::COUNT] = {
    let w = gnitz_wire::SYS_FAMILIES;
    let mut arr = [from_wire_cols(w[0].cols, w[0].pk_cols); SysFamily::COUNT];
    let mut i = 1;
    while i < SysFamily::COUNT {
        arr[i] = from_wire_cols(w[i].cols, w[i].pk_cols);
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

/// The `(view_id, sub)` halves of a circuit family's compound PK, which
/// [`sys_opk`](super::sys_opk) lays down as `(view_id << 64) | sub`.
pub(super) fn unpack_circuit_pk(pk: u128) -> (i64, u64) {
    ((pk >> 64) as i64, pk as u64)
}

/// The half-open COL_TAB key band `[pack(owner, 0), pack(owner + 1, 0))` holding
/// exactly `owner_id`'s column records — stated once so the read scan and the
/// drop cascade cannot disagree on the upper bound.
pub(super) fn column_id_band(owner_id: i64) -> (u64, u64) {
    (pack_column_id(owner_id, 0), pack_column_id(owner_id + 1, 0))
}

// ---------------------------------------------------------------------------
// Typed system family
// ---------------------------------------------------------------------------

/// Topological creation priority per family, indexed by discriminant. Lower =
/// earlier in the dependency chain (created first, destroyed last). Orders the
/// `DDL_TXN` handler's forward ingest — ascending for a bundle that creates, so
/// every register/index hook sees its dependencies already in the memtable;
/// descending for an all-negative one, where a dependent must be retired before
/// what it depends on — and rollback's own two-phase negate. Table and View
/// differ so their relative order is stable; 99 is order-neutral.
const TOPO_PRIORITY: [u8; SysFamily::COUNT] = [
    0,  // Schema
    5,  // Table
    6,  // View
    1,  // Column
    7,  // Index
    99, // Sequence
    2,  // CircuitNodes
    3,  // CircuitEdges
    4,  // CircuitNodeColumns
];

/// A catalog system-table family (every id below `FIRST_USER_TABLE_ID`). Used
/// at the applier's mutation API in place of a bare `i64`, so the `fire_hooks`
/// dispatch is an exhaustive `match` a newly-added family cannot silently skip.
/// Convert to/from `i64` only at the storage edge. The discriminant indexes
/// `gnitz_wire::SYS_FAMILIES`, `TOPO_PRIORITY` and `SCHEMAS`; the family's store
/// is reached by [`SysFamily::id`] through the relation registry.
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

    /// Every family in discriminant order, for the open/bootstrap/flush walks.
    pub(crate) const ALL: [SysFamily; Self::COUNT] = [
        SysFamily::Schema,
        SysFamily::Table,
        SysFamily::View,
        SysFamily::Column,
        SysFamily::Index,
        SysFamily::Sequence,
        SysFamily::CircuitNodes,
        SysFamily::CircuitEdges,
        SysFamily::CircuitNodeColumns,
    ];

    /// Discriminant index into the per-family arrays.
    #[inline]
    pub(crate) const fn index(self) -> usize {
        self as usize
    }

    /// This family's shared wire identity: its table id, its name (which is also
    /// its subdirectory under `_system_catalog/`), and the column shape the
    /// schema, the COL_TAB self-description rows, and the client's `Schema` all
    /// derive from. Held in `gnitz-wire`, so the engine restates none of it.
    #[inline]
    pub(in crate::catalog) fn wire(self) -> &'static gnitz_wire::WireSysFamily {
        &gnitz_wire::SYS_FAMILIES[self.index()]
    }

    /// The `*_TAB_ID` constant for this family, narrowed to the `i64` the
    /// catalog storage edge and every `sys_*` signature use.
    #[inline]
    pub(crate) fn id(self) -> i64 {
        self.wire().id as i64
    }

    /// This family's store subdirectory name.
    #[inline]
    pub(crate) fn name(self) -> &'static str {
        self.wire().name
    }

    /// This family's fixed schema.
    #[inline]
    pub(crate) fn schema(self) -> SchemaDescriptor {
        SCHEMAS[self.index()]
    }

    /// Topological creation priority (see [`TOPO_PRIORITY`]).
    #[inline]
    pub(crate) fn topo_priority(self) -> u8 {
        TOPO_PRIORITY[self.index()]
    }

    /// The lowest id a client may write in this family's id space; everything
    /// below is bootstrap-owned. Column's floor is on the *packed* word, since
    /// that is what its PK is. `None` where the PK is no id space at all.
    pub(super) fn first_user_id(self) -> Option<i64> {
        match self {
            SysFamily::Schema => Some(FIRST_USER_SCHEMA_ID),
            SysFamily::Table | SysFamily::View => Some(FIRST_USER_TABLE_ID),
            SysFamily::Column => Some(pack_column_id(FIRST_USER_TABLE_ID, 0) as i64),
            SysFamily::Index => Some(FIRST_USER_INDEX_ID),
            SysFamily::Sequence => Some(FIRST_USER_TABLE_ID),
            SysFamily::CircuitNodes | SysFamily::CircuitEdges | SysFamily::CircuitNodeColumns => None,
        }
    }

    /// The exclusive upper bound on an id a client may write. Index shares the
    /// relation ceiling although its ids are a separate space: it needs *a*
    /// bound, because `hook_index_register` raises the index-id counter off the
    /// ingested row and `allocate_index_ids` carries no assertion — and a second
    /// near-duplicate constant would only invite the two to drift.
    pub(super) fn id_ceiling(self) -> Option<i64> {
        match self {
            SysFamily::Table | SysFamily::View | SysFamily::Index => Some(RELATION_ID_CEILING),
            SysFamily::Schema
            | SysFamily::Column
            | SysFamily::Sequence
            | SysFamily::CircuitNodes
            | SysFamily::CircuitEdges
            | SysFamily::CircuitNodeColumns => None,
        }
    }

    /// The payload columns a rewrite pair (`-1` + `+1` on one PK) may change, as
    /// a payload-index bit mask; `None` where no emitter produces a pair —
    /// there is no schema-rename or index-rename surface.
    pub(super) fn pair_change_mask(self) -> Option<u64> {
        match self {
            // TABLE_TAB and VIEW_TAB agree on the name slot (asserted in
            // gnitz-wire), so one constant serves both.
            SysFamily::Table | SysFamily::View => Some(1 << TABTAB_PAY_NAME),
            SysFamily::Column => {
                Some((1 << COLTAB_PAY_NAME) | (1 << COLTAB_PAY_IS_HIDDEN) | (1 << COLTAB_PAY_IS_NULLABLE))
            }
            SysFamily::Sequence => Some(1 << gnitz_wire::SEQTAB_PAY_VALUE),
            SysFamily::Schema
            | SysFamily::Index
            | SysFamily::CircuitNodes
            | SysFamily::CircuitEdges
            | SysFamily::CircuitNodeColumns => None,
        }
    }

    /// May a client-pushed `DDL_TXN` bundle carry a block for this family?
    /// `false` for Sequence alone: boot feeds its rows straight into the id
    /// counters and the resume verdict, where a forged value aborts every
    /// subsequent start. Every legitimate sequence write is engine-built.
    pub(crate) fn client_writable(self) -> bool {
        match self {
            SysFamily::Sequence => false,
            SysFamily::Schema
            | SysFamily::Table
            | SysFamily::View
            | SysFamily::Column
            | SysFamily::Index
            | SysFamily::CircuitNodes
            | SysFamily::CircuitEdges
            | SysFamily::CircuitNodeColumns => true,
        }
    }

    /// Is this family's PK the identity of at most one live row — the premise
    /// the retraction CAS and the per-PK net bound rest on? False for the
    /// circuit families, whose `(view_id, sub)` addresses a node of a circuit.
    pub(super) fn pk_is_live_row_identity(self) -> bool {
        !matches!(
            self,
            SysFamily::CircuitNodes | SysFamily::CircuitEdges | SysFamily::CircuitNodeColumns
        )
    }

    /// What one of this family's rows is called in a guard message.
    pub(in crate::catalog) fn row_noun(self) -> &'static str {
        match self {
            SysFamily::Schema => "schema",
            SysFamily::Table => "table",
            SysFamily::View => "view",
            SysFamily::Column => "column",
            SysFamily::Index => "index",
            SysFamily::Sequence => "sequence",
            SysFamily::CircuitNodes | SysFamily::CircuitEdges | SysFamily::CircuitNodeColumns => "circuit row",
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
            SEQ_TAB_ID => Some(SysFamily::Sequence),
            CIRCUIT_NODES_TAB_ID => Some(SysFamily::CircuitNodes),
            CIRCUIT_EDGES_TAB_ID => Some(SysFamily::CircuitEdges),
            CIRCUIT_NODE_COLUMNS_TAB_ID => Some(SysFamily::CircuitNodeColumns),
            _ => None,
        }
    }
}

// The discriminant must index `gnitz_wire::SYS_FAMILIES`, and `ALL` must list
// the families in that same order — checked against `from_id`, the ground-truth
// id mapping, at compile time.
const _: () = {
    assert!(SysFamily::COUNT == gnitz_wire::SYS_FAMILIES.len());
    let mut i = 0;
    while i < SysFamily::COUNT {
        assert!(
            SysFamily::ALL[i] as usize == i,
            "SysFamily::ALL must be in discriminant order"
        );
        match SysFamily::from_id(gnitz_wire::SYS_FAMILIES[i].id as i64) {
            Some(f) => assert!(
                f as usize == i,
                "gnitz_wire::SYS_FAMILIES order must match SysFamily discriminant order"
            ),
            None => panic!("gnitz_wire::SYS_FAMILIES entry is not a known system family"),
        }
        i += 1;
    }
};

#[cfg(test)]
#[path = "tests/sys_tables.rs"]
mod tests;
