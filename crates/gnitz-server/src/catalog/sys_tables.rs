//! System table constants, the per-family descriptor table, the per-family row
//! decoders over those constants, and the batch-shape analyses (per-PK
//! signatures, sign partitions) every guard and hook keys on.
//!
//! Pure data, stateless codecs and batch-local analysis — no state, no
//! CatalogEngine dependency.

use rustc_hash::FxHashMap;
use std::sync::LazyLock;

use gnitz_store::relation::RelationKind;
use gnitz_wire::sys_rows::{
    ColTabRow, ColTabSlot, FkRef, IdxTabRow, SchemaTabRow, SysRow, SysRowSink, TableTabRow, ViewTabRow,
};
use gnitz_wire::{ColumnDef, TableDistribution, ViewProps, MAX_COLUMNS};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(super) const SYSTEM_SCHEMA_ID: u64 = 1;
pub(crate) const PUBLIC_SCHEMA_ID: u64 = 2;

/// The next catalog object id to allocate.
pub(super) const SEQ_ID_NEXT_ID: u64 = 1;
/// Committed checkpoint generation (monotonic).
pub(super) const SEQ_ID_CHECKPOINT_GEN: u64 = 2;
/// Cluster topology: `(worker_count as u64) << 32 | STATE_FORMAT as u64`.
pub(super) const SEQ_ID_TOPOLOGY: u64 = 3;

/// The first id `allocate_ids` hands out.
pub(super) const FIRST_ALLOCATED_ID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;

// PK list encoding lives in gnitz-wire so the client and engine cannot drift on
// the on-disk format.
pub(super) use gnitz_wire::PkColList;
use gnitz_wire::{bool_word, PkListRole};

// ---------------------------------------------------------------------------
// Per-family row-view decoders — the one reading of each family's *full row
// shape*, run by the precheck arms and again by the register hooks, each over
// the row `gnitz_wire::sys_rows` reads back.
//
// `read_rel_row` decodes both relation families off a wire `Batch`, which is
// what every caller holds; the rest are generic over `RowSource`, so a stored
// row or a positioned cursor decodes too.
// ---------------------------------------------------------------------------

/// A TABLE_TAB or VIEW_TAB row as decoded.
pub(super) struct RelRow<'a> {
    pub(super) id: u64,
    pub(super) schema_id: u64,
    pub(super) name: &'a str,
    pub(super) pk: PkColList,
    pub(super) kind: RelationKind,
    pub(super) detail: RelDetail,
}

pub(super) enum RelDetail {
    Table {
        distribution: TableDistribution,
        serial: bool,
    },
    /// `owner` is the user view this row is a chain segment of; `None` for a user
    /// view.
    View { owner: Option<u64>, pk_repeats: bool },
}

impl RelRow<'_> {
    /// Whether two of the relation's rows may share a PK.
    pub(super) fn pk_repeats(&self) -> bool {
        match self.detail {
            // A base table's PK is unique by `enforce_unique_pk`; a stream's is not.
            RelDetail::Table { .. } => self.kind == RelationKind::Stream,
            RelDetail::View { pk_repeats, .. } => pk_repeats,
        }
    }

    /// [`gnitz_wire::TableProps::serial`].
    pub(super) fn serial(&self) -> bool {
        matches!(self.detail, RelDetail::Table { serial: true, .. })
    }
}

/// `table 'orders' (id=17)` — the subject of a registration or precheck message.
impl std::fmt::Display for RelRow<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} '{}' (id={})", self.kind.noun(), self.name, self.id)
    }
}

/// Decode row `row` of relation family `family` (Table or View). The raw `flags`
/// word does not escape, so the rejections below hold for every caller.
pub(super) fn read_rel_row(family: SysFamily, batch: &Batch, row: usize) -> Result<RelRow<'_>, String> {
    let unreadable = |e: String| {
        let (noun, id) = (family.row_noun(), batch.get_pk(row) as u64);
        format!("catalog invariant violated: {noun} (id={id}) {e}")
    };
    match family {
        SysFamily::Table => rel_from_table_row(TableTabRow::read(batch, row).map_err(unreadable)?),
        SysFamily::View => rel_from_view_row(ViewTabRow::read(batch, row).map_err(unreadable)?),
        _ => unreachable!("{} is not a relation family", family.name()),
    }
}

fn rel_violated(noun: &str, name: &str, id: u64, e: String) -> String {
    format!("catalog invariant violated: {noun} '{name}' (id={id}) {e}")
}

fn rel_from_table_row(r: TableTabRow<'_>) -> Result<RelRow<'_>, String> {
    let (id, name) = (r.table_id, r.name);
    let props = gnitz_wire::TableProps::from_flags(r.flags)
        .map_err(|e| rel_violated(SysFamily::Table.row_noun(), name, id, e))?;
    let kind = if props.stream {
        RelationKind::Stream
    } else {
        RelationKind::BaseTable
    };
    let pk = PkColList::unpack(r.pk_col_idx)
        .map_err(|rule| rel_violated(kind.noun(), name, id, rule.for_role(PkListRole::PrimaryKey)))?;
    props
        .validate(pk.as_slice().len())
        .map_err(|e| rel_violated(kind.noun(), name, id, e))?;
    Ok(RelRow {
        id,
        schema_id: r.schema_id,
        name,
        pk,
        kind,
        detail: RelDetail::Table {
            distribution: props.distribution,
            serial: props.serial,
        },
    })
}

fn rel_from_view_row(r: ViewTabRow<'_>) -> Result<RelRow<'_>, String> {
    let (id, name, noun) = (r.view_id, r.name, SysFamily::View.row_noun());
    let pk = PkColList::unpack(r.pk_col_idx)
        .map_err(|rule| rel_violated(noun, name, id, rule.for_role(PkListRole::PrimaryKey)))?;
    let props =
        ViewProps::from_row(r.capacity_bytes, r.delta_bytes).map_err(|e| format!("{noun} '{name}' (id={id}): {e}"))?;
    let pk_repeats = bool_word(r.pk_repeats).map_err(|e| rel_violated(noun, name, id, format!("pk_repeats: {e}")))?;
    let owner = (r.owner_view_id != 0).then_some(r.owner_view_id);
    // A chain segment is a relation the planner mints, never one an option
    // clause may name.
    if props != ViewProps::Plain && owner.is_some() {
        return Err(format!(
            "catalog invariant violated: internal segment '{name}' (id={id}) carries a WITH option"
        ));
    }
    Ok(RelRow {
        id,
        schema_id: r.schema_id,
        name,
        pk,
        kind: RelationKind::View(props),
        detail: RelDetail::View { owner, pk_repeats },
    })
}

/// A catalog column: the logical column plus its FK, already resolved.
#[derive(Clone, Debug)]
pub(crate) struct CatalogColumn {
    pub(crate) def: ColumnDef,
    pub(crate) fk: Option<FkRef>,
}

impl CatalogColumn {
    /// The column COL_TAB row `r` describes — [`Self::write_col_tab_row`]'s inverse.
    pub(super) fn from_row(r: &ColTabRow<'_>) -> Result<Self, String> {
        let (def, fk) = r.column()?;
        Ok(CatalogColumn { def, fk })
    }

    /// Write this column as COL_TAB row `(owner_id, col_idx)`.
    pub(crate) fn write_col_tab_row(&self, sink: &mut impl SysRowSink, owner_id: u64, col_idx: usize, weight: i64) {
        ColTabRow::of(owner_id, col_idx as u64, &self.def, self.fk).write(sink, weight);
    }
}

/// `defs` as `owner_id`'s COL_TAB rows, keyed by position, at `weight`.
pub(crate) fn write_col_tab_rows(bb: &mut BatchBuilder, owner_id: u64, defs: &[CatalogColumn], weight: i64) {
    for (i, cd) in defs.iter().enumerate() {
        cd.write_col_tab_row(bb, owner_id, i, weight);
    }
}

/// What one delta does to one PK: where its `-1` and `+1` rows are, and the
/// summed weight. A PK carrying both signs is a **rewrite pair** (a rename).
pub(super) struct PkSignature {
    pub(super) pk: u128,
    /// [`SysFamily::leading_id`] of `pk`, so no consumer re-derives it.
    pub(super) leading: u64,
    /// First row index carrying this PK; its OPK bytes address the live row.
    pub(super) row: usize,
    /// First `-1` / `+1` row index for this PK.
    pub(super) neg: Option<usize>,
    pub(super) pos: Option<usize>,
    /// A sign occurs on more than one row.
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

/// One [`PkSignature`] per distinct PK, in first-appearance order, over a batch in
/// any row order. Zero-weight rows are skipped.
pub(super) fn pk_signatures(family: SysFamily, batch: &Batch) -> Vec<PkSignature> {
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
                leading: family.leading_id(pk),
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

/// One family delta split by sign, rewrite pairs excluded — a rename's `-1,+1`
/// on one PK is neither a create nor a drop. Batch-local: both signs of a pair
/// arrive as one batch on every path.
#[derive(Default)]
pub(crate) struct PkPartition {
    pub(crate) creates: Vec<u64>,
    pub(crate) drops: Vec<u64>,
}

pub(crate) fn family_pk_partition(family: SysFamily, batch: &Batch) -> PkPartition {
    let mut out = PkPartition::default();
    for sig in pk_signatures(family, batch) {
        if sig.is_pair() {
            continue;
        }
        if sig.pos.is_some() {
            &mut out.creates
        } else {
            &mut out.drops
        }
        .push(sig.leading);
    }
    out
}

/// [`PkPartition`] for IDX_TAB, carrying each row's decoded column list — what
/// the DDL driver's unique pre-flight and filter teardown key on. A row whose
/// list does not decode is skipped; the precheck rejects the batch over it.
#[derive(Default)]
pub(crate) struct IdxPartition {
    /// `(owner_id, cols, is_unique)`.
    pub(crate) creates: Vec<(u64, PkColList, bool)>,
    pub(crate) drops: Vec<(u64, PkColList)>,
}

pub(crate) fn idx_tab_partition(batch: &Batch) -> IdxPartition {
    let mut out = IdxPartition::default();
    for sig in pk_signatures(SysFamily::Index, batch) {
        if sig.is_pair() {
            continue;
        }
        let row = sig.pos.or(sig.neg).expect("pk_signatures skips a zero-weight row");
        let Ok((owner_id, cols, is_unique)) = IdxTabRow::read(batch, row).and_then(|r| r.parts()) else {
            continue;
        };
        if sig.pos.is_some() {
            out.creates.push((owner_id, cols, is_unique));
        } else {
            out.drops.push((owner_id, cols));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Schema derivation from the shared wire column slices
// ---------------------------------------------------------------------------

/// One schema per family, indexed by [`SysFamily::index`], each built from
/// `gnitz-wire`'s canonical system-table column array.
static SCHEMAS: LazyLock<[SchemaDescriptor; SysFamily::COUNT]> = LazyLock::new(|| {
    std::array::from_fn(|i| {
        let f = &gnitz_wire::SYS_FAMILIES[i];
        let cols: Vec<SchemaColumn> = f.cols.iter().map(|c| SchemaColumn::new(c.type_code, false)).collect();
        SchemaDescriptor::new(&cols, f.pk_cols)
    })
});

/// The rules a relation's column records must satisfy on their own, with no PK
/// list in hand; messages are bare predicates, and callers add the context. The
/// duplicate-name rule is ingestion points only — a view segment legitimately
/// carries two visible columns of one name (a join chain's output).
pub(super) fn check_col_defs(kind: RelationKind, col_defs: &[CatalogColumn]) -> Result<(), String> {
    if col_defs.is_empty() {
        return Err("has no column records".into());
    }
    // Reachable from a plain view as well as a wide CREATE TABLE: a compound-PK
    // plain projection prepends the k source PK columns.
    if col_defs.len() > MAX_COLUMNS {
        return Err(format!("has {} columns (max {MAX_COLUMNS})", col_defs.len()));
    }
    if kind.is_ingestion_point() {
        let visible = col_defs.iter().filter(|c| !c.def.is_hidden);
        if let Some(name) = gnitz_wire::first_duplicate(visible.map(|c| c.def.name.as_str())) {
            return Err(format!("has duplicate column name '{name}'"));
        }
    }
    Ok(())
}

/// The `SchemaDescriptor` COL_TAB records describe, or the first admissibility
/// rule they break.
pub(super) fn build_schema_from_col_defs(
    kind: RelationKind,
    col_defs: &[CatalogColumn],
    pk_cols: &[u32],
) -> Result<SchemaDescriptor, String> {
    check_col_defs(kind, col_defs)?;
    let cols: Vec<SchemaColumn> = col_defs
        .iter()
        .map(|cd| SchemaColumn::new(cd.def.ty.tc, cd.def.is_nullable))
        .collect();
    SchemaDescriptor::try_new(&cols, pk_cols)
}

// ---------------------------------------------------------------------------
// Typed system family
// ---------------------------------------------------------------------------

/// A catalog system-table family (every id below `FIRST_USER_TABLE_ID`). Used
/// at the applier's mutation API in place of a bare `u64`, so the `fire_hooks`
/// dispatch is an exhaustive `match` a newly-added family cannot silently skip.
///
/// **The discriminant is the wire table id**, so `SysFamily::Table` *is*
/// `TABLE_TAB` and no mapping can disagree. Declaration order here carries
/// nothing; the per-family arrays are indexed by [`Self::index`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u64)]
pub(crate) enum SysFamily {
    Schema = gnitz_wire::SCHEMA_TAB,
    Table = gnitz_wire::TABLE_TAB,
    View = gnitz_wire::VIEW_TAB,
    Column = gnitz_wire::COL_TAB,
    Index = gnitz_wire::IDX_TAB,
    Sequence = gnitz_wire::SEQ_TAB,
    Circuit = gnitz_wire::CIRCUIT_TAB,
}

impl SysFamily {
    pub(crate) const COUNT: usize = gnitz_wire::SYS_FAMILIES.len();

    /// Every family in `gnitz_wire::SYS_FAMILIES` order: the index the
    /// per-family arrays share, and the order a bundle's families and a boot's
    /// replay are applied in.
    pub(crate) const ALL: [SysFamily; Self::COUNT] = [
        SysFamily::Schema,
        SysFamily::Column,
        SysFamily::Circuit,
        SysFamily::Table,
        SysFamily::View,
        SysFamily::Index,
        SysFamily::Sequence,
    ];

    /// This family's position in `gnitz_wire::SYS_FAMILIES` — the index every
    /// per-family array here is laid out by. A seven-entry scan, at DDL-bundle
    /// or boot rate.
    #[inline]
    pub(crate) const fn index(self) -> usize {
        match gnitz_wire::sys_family_index(self as u64) {
            Some(i) => i,
            None => panic!("SysFamily variant is not a wire family"),
        }
    }

    /// This family's shared wire identity: its table id, its name, and the column
    /// shape its schema, COL_TAB self-description rows and the client's `Schema`
    /// derive from. Held in `gnitz-wire`, so the engine restates none of it.
    #[inline]
    pub(in crate::catalog) fn wire(self) -> &'static gnitz_wire::WireSysFamily {
        &gnitz_wire::SYS_FAMILIES[self.index()]
    }

    /// This family's table id — the discriminant itself.
    #[inline]
    pub(crate) const fn id(self) -> u64 {
        self as u64
    }

    /// This family's name.
    #[inline]
    pub(crate) fn name(self) -> &'static str {
        self.wire().name
    }

    /// This family's fixed schema.
    #[inline]
    pub(crate) fn schema(self) -> &'static SchemaDescriptor {
        &SCHEMAS[self.index()]
    }

    /// This family's column definitions, from the same wire slice its schema is
    /// built from — compile-time data, never read back from COL_TAB.
    pub(in crate::catalog) fn column_defs(self) -> Vec<CatalogColumn> {
        self.wire()
            .cols
            .iter()
            .map(|c| CatalogColumn {
                def: ColumnDef::new(c.name, c.type_code, false),
                fk: None,
            })
            .collect()
    }

    /// The rows a fresh database's store of this family starts with.
    pub(in crate::catalog) fn write_seed_rows(self, bb: &mut BatchBuilder) {
        match self {
            SysFamily::Schema => {
                for (schema_id, name) in [
                    (SYSTEM_SCHEMA_ID, gnitz_wire::SYSTEM_SCHEMA),
                    (PUBLIC_SCHEMA_ID, "public"),
                ] {
                    SchemaTabRow { schema_id, name }.write(bb, 1);
                }
            }
            SysFamily::Table => {
                for family in SysFamily::ALL {
                    let row = TableTabRow {
                        table_id: family.id(),
                        schema_id: SYSTEM_SCHEMA_ID,
                        name: family.name(),
                        pk_col_idx: PkColList::from_slice(family.wire().pk_cols).pack(),
                        flags: gnitz_wire::TableProps::default().pack(),
                    };
                    row.write(bb, 1);
                }
            }
            SysFamily::Column => {
                for family in SysFamily::ALL {
                    write_col_tab_rows(bb, family.id(), &family.column_defs(), 1);
                }
            }
            SysFamily::View | SysFamily::Index | SysFamily::Sequence | SysFamily::Circuit => {}
        }
    }

    /// The families this family's hook reads, which are applied before it.
    const fn applies_after(self) -> &'static [SysFamily] {
        match self {
            SysFamily::Schema | SysFamily::Column | SysFamily::Circuit | SysFamily::Sequence => &[],
            SysFamily::Table => &[SysFamily::Schema, SysFamily::Column],
            SysFamily::View => &[SysFamily::Schema, SysFamily::Column, SysFamily::Circuit],
            SysFamily::Index => &[SysFamily::Table, SysFamily::View],
        }
    }

    /// The leading id of one of this family's PKs: the whole key where it is one
    /// column, its high half where the key is a pair — where `pk as u64` would
    /// take the trailing half instead (a column index).
    #[inline]
    pub(super) fn leading_id(self, pk: u128) -> u64 {
        match self {
            SysFamily::Column => gnitz_wire::unpack_pair_pk(pk).0,
            SysFamily::Schema
            | SysFamily::Table
            | SysFamily::View
            | SysFamily::Index
            | SysFamily::Sequence
            | SysFamily::Circuit => pk as u64,
        }
    }

    /// Does this family's PK draw from the catalog object-id counter?
    pub(super) fn allocates_ids(self) -> bool {
        match self {
            SysFamily::Schema | SysFamily::Table | SysFamily::View | SysFamily::Index => true,
            SysFamily::Column | SysFamily::Sequence | SysFamily::Circuit => false,
        }
    }

    /// The payload columns a rewrite pair (`-1` + `+1` on one PK) may change, as
    /// a payload-index bit mask; `None` where no emitter produces a pair —
    /// there is no schema-rename or index-rename surface.
    pub(super) fn pair_change_mask(self) -> Option<u64> {
        match self {
            SysFamily::Table | SysFamily::View => Some(1 << gnitz_wire::RELTAB_PAY_NAME),
            SysFamily::Column => Some(
                (1 << ColTabSlot::name as usize)
                    | (1 << ColTabSlot::is_hidden as usize)
                    | (1 << ColTabSlot::is_nullable as usize),
            ),
            SysFamily::Schema | SysFamily::Index | SysFamily::Sequence | SysFamily::Circuit => None,
        }
    }

    /// Are this family's rows retracted only with the relation that owns them — as a band,
    /// by the drop cascade? A client delta may then carry no unpaired `-1` for it.
    pub(super) fn retracts_with_owner(self) -> bool {
        match self {
            SysFamily::Column | SysFamily::Circuit => true,
            SysFamily::Schema | SysFamily::Table | SysFamily::View | SysFamily::Index | SysFamily::Sequence => false,
        }
    }

    /// Does this family hold the master's positions rather than a set? `true`
    /// for Sequence alone: its rows move with every SERIAL reservation and
    /// checkpoint and feed the id counters and the resume verdict at boot. So no
    /// client bundle may carry one, no worker is sent one, and no view scans it.
    pub(crate) fn master_only(self) -> bool {
        match self {
            SysFamily::Sequence => true,
            SysFamily::Schema
            | SysFamily::Table
            | SysFamily::View
            | SysFamily::Column
            | SysFamily::Index
            | SysFamily::Circuit => false,
        }
    }

    /// How a guard message names the row at `pk`: a pair-keyed family's two ids
    /// are meaningless rendered as the one number the widened key is.
    pub(in crate::catalog) fn pk_label(self, pk: u128) -> String {
        let (hi, lo) = gnitz_wire::unpack_pair_pk(pk);
        match self {
            SysFamily::Column => format!("column {lo} of owner {hi}"),
            _ => format!("{} {pk}", self.row_noun()),
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
            SysFamily::Circuit => "circuit",
        }
    }

    /// Inverse of [`Self::id`]; `None` for any id that is not a system family.
    pub(crate) const fn from_id(id: u64) -> Option<Self> {
        match gnitz_wire::sys_family_index(id) {
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
        let family = SysFamily::ALL[i];
        assert!(
            family.index() == i,
            "SysFamily::ALL must be in gnitz_wire::SYS_FAMILIES order"
        );
        let deps = family.applies_after();
        let mut d = 0;
        while d < deps.len() {
            assert!(
                deps[d].index() < i,
                "gnitz_wire::SYS_FAMILIES must order a family after every family its hook reads"
            );
            d += 1;
        }
        i += 1;
    }
};

#[cfg(test)]
#[path = "tests/sys_tables.rs"]
mod tests;
