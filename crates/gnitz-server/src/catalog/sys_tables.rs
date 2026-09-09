//! System table constants, the per-family descriptor table, PK packing
//! helpers, and the per-family row codecs (batch row-view decoders and
//! row builders) over those constants.
//!
//! Pure data and stateless codecs — no state, no CatalogEngine dependency.

use rustc_hash::FxHashMap;

use super::ColumnDef;
use gnitz_expr::RowSource;
use gnitz_store::relation::{RelationKind, ViewBudgets};
use gnitz_store::schema::{Placement, SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{payload_string, payload_u64, Batch, BatchBuilder};
use gnitz_wire::sys_rows::{ColTabRow, IdxTabRow, TableTabRow};
use gnitz_wire::MAX_COLUMNS;
use gnitz_wire::{
    COLTAB_PAY_COL_IDX, COLTAB_PAY_FK_COL_IDX, COLTAB_PAY_FK_TABLE_ID, COLTAB_PAY_IS_HIDDEN, COLTAB_PAY_IS_NULLABLE,
    COLTAB_PAY_IS_SERIAL, COLTAB_PAY_NAME, COLTAB_PAY_OWNER_ID, COLTAB_PAY_OWNER_KIND, COLTAB_PAY_SCALE,
    COLTAB_PAY_TYPE_CODE, IDXTAB_PAY_FLAGS, IDXTAB_PAY_OWNER_ID, IDXTAB_PAY_SOURCE_COLS, RELTAB_PAY_NAME,
    RELTAB_PAY_SCHEMA_ID, TABTAB_PAY_FLAGS, TABTAB_PAY_PK_COL_IDX, VIEWTAB_PAY_CAPACITY, VIEWTAB_PAY_DELTA,
    VIEWTAB_PAY_OWNER_VIEW_ID, VIEWTAB_PAY_PK_COL_IDX,
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
/// `precheck_family` — the point an id enters the registry — rejects any id at
/// or above it.
pub(super) const RELATION_ID_CEILING: i64 = gnitz_wire::RELATION_ID_CEILING as i64;

pub(super) const SYS_CATALOG_DIRNAME: &str = "_system_catalog";

// The families' table ids in the catalog's `i64` width. Production code names
// the family — [`SysFamily::id`] *is* the wire id, by discriminant — so these
// only spell what a test batch's `ingest_to_family` argument needs.
#[cfg(test)]
pub(super) const SCHEMA_TAB_ID: i64 = gnitz_wire::SCHEMA_TAB as i64;
#[cfg(test)]
pub(super) const TABLE_TAB_ID: i64 = gnitz_wire::TABLE_TAB as i64;
#[cfg(test)]
pub(super) const VIEW_TAB_ID: i64 = gnitz_wire::VIEW_TAB as i64;
#[cfg(test)]
pub(super) const COL_TAB_ID: i64 = gnitz_wire::COL_TAB as i64;
#[cfg(test)]
pub(super) const IDX_TAB_ID: i64 = gnitz_wire::IDX_TAB as i64;
#[cfg(test)]
pub(crate) const SEQ_TAB_ID: i64 = gnitz_wire::SEQ_TAB as i64;
#[cfg(test)]
pub(super) const CIRCUIT_NODES_TAB_ID: i64 = gnitz_wire::CIRCUIT_NODES_TAB as i64;

// PK list encoding lives in gnitz-wire so the client and engine cannot drift
// on the on-disk format. Production code spells the packers `gnitz_wire::…` (or
// reaches them via the row decoders below); the unqualified names are used only
// by tests, so their re-exports are test-scoped.
use gnitz_wire::unpack_pk_cols;
pub(super) use gnitz_wire::PkColList;
#[cfg(test)]
pub(super) use gnitz_wire::{pack_pk_cols, PK_LIST_PACKED_FLAG};

// ---------------------------------------------------------------------------
// Per-family row-view decoders — the one reading of each family's *full row
// shape*, shared by the precheck arms (`precheck_family`) and the register
// hooks (which re-decode on the paths that skip precheck: boot replay and
// worker ddl_sync). A caller that wants one named field still reads it straight
// through its `*_PAY_*` constant; that costs one `payload_u64` where a decode
// here costs the whole row.
//
// Every one is generic over `RowSource`, so a wire `Batch`, a `StoredRow` a PK
// probe located, and a positioned `ReadCursor`'s entry decode through one
// reading each.
// ---------------------------------------------------------------------------

/// Decode TABLE_TAB `row`: `(schema_id, name, pk_list, kind, placement)`. The
/// raw `flags` word does not escape — every registration crosses this reader, so
/// decoding it here is what makes the rejection below unskippable on the paths
/// that bypass the precheck. A pure function of `(src, row)`; a caller that has
/// an id to name decorates the message with it.
pub(super) fn read_table_tab_row<S: RowSource>(
    src: &S,
    row: usize,
) -> Result<(i64, String, PkColList, RelationKind, Placement), String> {
    let name = payload_string(src, row, RELTAB_PAY_NAME);
    let props = gnitz_wire::TableProps::from_flags(payload_u64(src, row, TABTAB_PAY_FLAGS));
    props
        .validate()
        .map_err(|e| format!("catalog invariant violated: table '{name}' {e}"))?;
    // The PK list is decoded before the placement is built: `dist_prefix_len` is
    // a leading-prefix length into it, and nothing downstream re-checks it —
    // `Placement::resolve` normalizes only the `0` sentinel.
    let pk = unpack_pk_cols(payload_u64(src, row, TABTAB_PAY_PK_COL_IDX))
        .map_err(|rule| format!("catalog invariant violated: table '{name}' {rule}"))?;
    props
        .validate_against_pk(pk.as_slice().len())
        .map_err(|e| format!("catalog invariant violated: table '{name}' {e}"))?;
    let placement = if props.replicated {
        Placement::Replicated
    } else {
        Placement::Keyed { prefix_len: props.dist_prefix_len as u8 }
    };
    Ok((
        payload_u64(src, row, RELTAB_PAY_SCHEMA_ID) as i64,
        name,
        pk,
        if props.stream {
            RelationKind::Stream
        } else {
            RelationKind::BaseTable
        },
        placement,
    ))
}

/// Decode VIEW_TAB `row`: `(schema_id, name, pk_list, budgets, owner_view_id)`.
/// A `0` budget word decodes to `None` here so no caller repeats the sentinel;
/// `owner_view_id` is `0` for a user view.
pub(super) fn read_view_tab_row<S: RowSource>(
    src: &S,
    row: usize,
) -> Result<(i64, String, PkColList, ViewBudgets, i64), String> {
    let name = payload_string(src, row, RELTAB_PAY_NAME);
    let pk = unpack_pk_cols(payload_u64(src, row, VIEWTAB_PAY_PK_COL_IDX))
        .map_err(|rule| format!("catalog invariant violated: view '{name}' {rule}"))?;
    Ok((
        payload_u64(src, row, RELTAB_PAY_SCHEMA_ID) as i64,
        name,
        pk,
        ViewBudgets {
            capacity_bytes: Some(payload_u64(src, row, VIEWTAB_PAY_CAPACITY)).filter(|&b| b != 0),
            delta_bytes: Some(payload_u64(src, row, VIEWTAB_PAY_DELTA)).filter(|&b| b != 0),
        },
        payload_u64(src, row, VIEWTAB_PAY_OWNER_VIEW_ID) as i64,
    ))
}

/// Decode IDX_TAB `row`: `(owner_id, col_indices, props)` — the one decoding of
/// `source_col_idx`, so no consumer holds its undecoded word.
pub(super) fn read_idx_tab_row<S: RowSource>(
    src: &S,
    row: usize,
) -> Result<(i64, PkColList, gnitz_wire::IndexProps), gnitz_wire::PkRule> {
    Ok((
        payload_u64(src, row, IDXTAB_PAY_OWNER_ID) as i64,
        unpack_pk_cols(payload_u64(src, row, IDXTAB_PAY_SOURCE_COLS))?,
        gnitz_wire::IndexProps::from_flags(payload_u64(src, row, IDXTAB_PAY_FLAGS)),
    ))
}

/// Decode COL_TAB `row` into the `ColumnDef` the schema builder consumes.
pub(super) fn read_col_tab_row<S: RowSource>(src: &S, row: usize) -> ColumnDef {
    ColumnDef {
        name: payload_string(src, row, COLTAB_PAY_NAME),
        type_code: payload_u64(src, row, COLTAB_PAY_TYPE_CODE) as u8,
        is_nullable: payload_u64(src, row, COLTAB_PAY_IS_NULLABLE) != 0,
        fk_table_id: payload_u64(src, row, COLTAB_PAY_FK_TABLE_ID) as i64,
        fk_col_idx: payload_u64(src, row, COLTAB_PAY_FK_COL_IDX) as u32,
        is_serial: payload_u64(src, row, COLTAB_PAY_IS_SERIAL) != 0,
        is_hidden: payload_u64(src, row, COLTAB_PAY_IS_HIDDEN) != 0,
        scale: payload_u64(src, row, COLTAB_PAY_SCALE) as u8,
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
        owner_id: payload_u64(src, row, COLTAB_PAY_OWNER_ID) as i64,
        owner_kind: payload_u64(src, row, COLTAB_PAY_OWNER_KIND) as i64,
        col_idx: payload_u64(src, row, COLTAB_PAY_COL_IDX),
        fk_table_id: payload_u64(src, row, COLTAB_PAY_FK_TABLE_ID) as i64,
        fk_col_idx: payload_u64(src, row, COLTAB_PAY_FK_COL_IDX) as u32,
    }
}

/// The `(owner_id, col_indices)` of every UNIQUE index this IDX_TAB family
/// creates — positive-weight rows whose column list decodes. The DDL driver
/// pre-flights each one before the bundle is made durable.
pub(crate) fn idx_tab_unique_creates(batch: &Batch) -> Vec<(i64, PkColList)> {
    (0..batch.len())
        .filter(|&i| batch.get_weight(i) > 0)
        .filter_map(|i| {
            let (owner_id, cols, props) = read_idx_tab_row(batch, i).ok()?;
            props.is_unique.then_some((owner_id, cols))
        })
        .collect()
}

/// The `(owner_id, col_indices)` of every index this IDX_TAB family drops — its
/// negative-weight rows whose column list decodes. The DDL driver clears each
/// pair's unique filter once the drop is durable.
pub(crate) fn idx_tab_drops(batch: &Batch) -> Vec<(i64, PkColList)> {
    (0..batch.len())
        .filter(|&i| batch.get_weight(i) < 0)
        .filter_map(|i| read_idx_tab_row(batch, i).ok().map(|(owner, cols, _)| (owner, cols)))
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
/// The map, not an adjacent-run scan: `canonicalize_for_hooks` sign-partitions
/// the batch, so a pair's two rows are never adjacent.
pub(super) fn pk_signatures(batch: &Batch) -> Vec<PkSignature> {
    let mut sigs: Vec<PkSignature> = Vec::new();
    let mut by_pk: FxHashMap<u128, usize> = FxHashMap::default();
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
        let sign_slot = if w < 0 { &mut sig.neg } else { &mut sig.pos };
        sig.repeats_a_sign |= sign_slot.is_some();
        sign_slot.get_or_insert(i);
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
        .filter(|s| !s.is_pair() && s.pos.is_some() == positive)
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
    // The abort is [`pack_column_id`]'s trust decision, over the same packing.
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
            scale: cd.scale,
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
    let mut bb = BatchBuilder::new(*SysFamily::Index.schema());
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

// Pre-computed schema statics, one per family, indexed by `SysFamily::index` —
// initialised at compile time, never reconstructed. `from_wire_cols`
// places every family `Replicated`, so a reader single-sources one copy instead of
// gathering N (the relation's own `Placement`).
/// Build a `SchemaDescriptor` from one of `gnitz-wire`'s canonical system-table
/// column arrays. `const`, so [`SCHEMAS`] below costs nothing at runtime. Every
/// such family is [`Placement::Replicated`]: DDL is master-broadcast, so each
/// worker holds an identical full copy.
pub(crate) const fn from_wire_cols(cols: &[gnitz_wire::WireSysCol], pk_indices: &[u32]) -> SchemaDescriptor {
    let mut buf = [SchemaColumn::EMPTY; MAX_COLUMNS];
    let mut i = 0;
    while i < cols.len() {
        buf[i] = SchemaColumn::new(cols[i].type_code as u8, if cols[i].nullable { 1 } else { 0 });
        i += 1;
    }
    let (head, _) = buf.split_at(cols.len());
    SchemaDescriptor::new_with_placement(head, pk_indices, Placement::Replicated)
}

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

/// The `(view_id, node_id)` halves of a circuit row's compound PK as
/// `Batch::get_pk` reads it back out of the PK region: `view_id` in the high
/// u128 half, `node_id` in the low one.
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

/// Topological creation priority per family, indexed by [`SysFamily::index`].
/// Lower =
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
];

/// A catalog system-table family (every id below `FIRST_USER_TABLE_ID`). Used
/// at the applier's mutation API in place of a bare `i64`, so the `fire_hooks`
/// dispatch is an exhaustive `match` a newly-added family cannot silently skip.
/// Convert to/from `i64` only at the storage edge.
///
/// **The discriminant is the wire table id**, so `SysFamily::Table` *is*
/// `TABLE_TAB` and no mapping can disagree. Declaration order here carries
/// nothing; the per-family arrays are indexed by [`Self::index`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SysFamily {
    Schema = gnitz_wire::SCHEMA_TAB as isize,
    Table = gnitz_wire::TABLE_TAB as isize,
    View = gnitz_wire::VIEW_TAB as isize,
    Column = gnitz_wire::COL_TAB as isize,
    Index = gnitz_wire::IDX_TAB as isize,
    Sequence = gnitz_wire::SEQ_TAB as isize,
    CircuitNodes = gnitz_wire::CIRCUIT_NODES_TAB as isize,
}

impl SysFamily {
    pub(crate) const COUNT: usize = gnitz_wire::SYS_FAMILIES.len();

    /// Every family in `gnitz_wire::SYS_FAMILIES` order — the index the
    /// per-family arrays share — for the open/bootstrap/flush walks.
    pub(crate) const ALL: [SysFamily; Self::COUNT] = [
        SysFamily::Schema,
        SysFamily::Table,
        SysFamily::View,
        SysFamily::Column,
        SysFamily::Index,
        SysFamily::Sequence,
        SysFamily::CircuitNodes,
    ];

    /// This family's position in `gnitz_wire::SYS_FAMILIES` — the index every
    /// per-family array here is laid out by. Const-evaluated for a literal
    /// receiver; a seven-entry scan otherwise, at DDL-bundle or boot rate.
    #[inline]
    pub(crate) const fn index(self) -> usize {
        match gnitz_wire::sys_family_index(self as u64) {
            Some(i) => i,
            None => panic!("SysFamily variant is not a wire family"),
        }
    }

    /// This family's shared wire identity: its table id, its name (which is also
    /// its subdirectory under `_system_catalog/`), and the column shape the
    /// schema, the COL_TAB self-description rows, and the client's `Schema` all
    /// derive from. Held in `gnitz-wire`, so the engine restates none of it.
    #[inline]
    pub(in crate::catalog) fn wire(self) -> &'static gnitz_wire::WireSysFamily {
        &gnitz_wire::SYS_FAMILIES[self.index()]
    }

    /// This family's table id — the discriminant itself — in the `i64` the
    /// catalog storage edge and every `sys_*` signature use.
    #[inline]
    pub(crate) const fn id(self) -> i64 {
        self as i64
    }

    /// This family's store subdirectory name.
    #[inline]
    pub(crate) fn name(self) -> &'static str {
        self.wire().name
    }

    /// This family's fixed schema. Borrowed from the `static` that holds it —
    /// a `SchemaDescriptor` is 360 bytes, so returning it by value put a copy on
    /// every catalog write.
    #[inline]
    pub(crate) fn schema(self) -> &'static SchemaDescriptor {
        &SCHEMAS[self.index()]
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
            SysFamily::CircuitNodes => None,
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
            SysFamily::Schema | SysFamily::Column | SysFamily::Sequence | SysFamily::CircuitNodes => None,
        }
    }

    /// The payload columns a rewrite pair (`-1` + `+1` on one PK) may change, as
    /// a payload-index bit mask; `None` where no emitter produces a pair —
    /// there is no schema-rename or index-rename surface.
    pub(super) fn pair_change_mask(self) -> Option<u64> {
        match self {
            SysFamily::Table | SysFamily::View => Some(1 << RELTAB_PAY_NAME),
            SysFamily::Column => {
                Some((1 << COLTAB_PAY_NAME) | (1 << COLTAB_PAY_IS_HIDDEN) | (1 << COLTAB_PAY_IS_NULLABLE))
            }
            SysFamily::Sequence => Some(1 << gnitz_wire::SEQTAB_PAY_VALUE),
            SysFamily::Schema | SysFamily::Index | SysFamily::CircuitNodes => None,
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
            | SysFamily::CircuitNodes => true,
        }
    }

    /// Is this family's PK the identity of at most one live row — the premise
    /// the retraction CAS and the per-PK net bound rest on? False for the circuit
    /// family, whose `(view_id, node_id)` addresses a node of a circuit.
    pub(super) fn pk_is_live_row_identity(self) -> bool {
        !matches!(self, SysFamily::CircuitNodes)
    }

    /// How a guard message names one row of this family. A COL_TAB PK packs
    /// `(owner_id, col_idx)` and a circuit PK packs `(view_id, node_id)`, so
    /// neither is meaningful rendered as the one number it is stored as.
    pub(in crate::catalog) fn pk_label(self, pk: u128) -> String {
        match self {
            SysFamily::Column => {
                let (owner_id, col_idx) = gnitz_wire::unpack_col_id(pk as u64);
                format!("column {col_idx} of owner {owner_id}")
            }
            SysFamily::CircuitNodes => {
                let (view_id, node_id) = unpack_circuit_pk(pk);
                format!("view {view_id} node {node_id}")
            }
            _ => format!("id {pk}"),
        }
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
            SysFamily::CircuitNodes => "circuit row",
        }
    }

    /// Inverse of [`Self::id`]; `None` for any id that is not a system family.
    pub(crate) const fn from_id(id: i64) -> Option<Self> {
        if id < 0 {
            return None;
        }
        match gnitz_wire::sys_family_index(id as u64) {
            Some(i) => Some(Self::ALL[i]),
            None => None,
        }
    }
}

// `ALL` indexes the per-family arrays, so it must be in `SYS_FAMILIES` order.
// `index()` also panics for an id absent from `SYS_FAMILIES`, so this fails the
// build on a variant naming no wire family, and on two sharing an id.
const _: () = {
    let mut i = 0;
    while i < SysFamily::COUNT {
        assert!(
            SysFamily::ALL[i].index() == i,
            "SysFamily::ALL must be in gnitz_wire::SYS_FAMILIES order"
        );
        i += 1;
    }
};

#[cfg(test)]
#[path = "tests/sys_tables.rs"]
mod tests;
