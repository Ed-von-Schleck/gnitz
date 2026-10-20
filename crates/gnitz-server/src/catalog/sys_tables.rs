//! System table constants, the per-family descriptor table, the per-family row
//! decoders over those constants, and the batch-shape analyses (per-PK
//! signatures and their shapes) every guard and hook keys on.
//!
//! Pure data, stateless codecs and batch-local analysis — no state, no
//! CatalogEngine dependency.

use rustc_hash::FxHashMap;
use std::borrow::Cow;
use std::sync::LazyLock;

use gnitz_store::relation::RelationKind;
pub(crate) use gnitz_wire::sys_rows::CatalogColumn;
use gnitz_wire::sys_rows::{ColTabSlot, IdxTabRow, SchemaTabRow, SysRow, TableTabRow, ViewParts, ViewTabRow};
use gnitz_wire::{ColumnDef, TableProps, MAX_COLUMNS};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(super) const SYSTEM_SCHEMA_ID: u64 = 1;
pub(crate) const PUBLIC_SCHEMA_ID: u64 = 2;

// PK list encoding lives in gnitz-wire so the client and engine cannot drift on
// the on-disk format.
pub(super) use gnitz_wire::PkColList;

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
    pub(super) name: Cow<'a, str>,
    pub(super) pk: PkColList,
    pub(super) detail: RelDetail,
}

pub(super) enum RelDetail {
    Table(TableProps),
    View(ViewParts),
}

impl RelRow<'_> {
    pub(super) fn kind(&self) -> RelationKind {
        match self.detail {
            RelDetail::Table(TableProps { stream: true, .. }) => RelationKind::Stream,
            RelDetail::Table(_) => RelationKind::BaseTable,
            RelDetail::View(v) => RelationKind::View(v.props.budget),
        }
    }

    /// Whether two of the relation's rows may share a PK.
    pub(super) fn pk_repeats(&self) -> bool {
        match self.detail {
            // A base table's PK is unique by `enforce_unique_pk`; a stream's is not.
            RelDetail::Table(t) => t.stream,
            RelDetail::View(v) => v.pk_repeats,
        }
    }

    /// [`TableProps::serial`].
    pub(super) fn serial(&self) -> bool {
        matches!(self.detail, RelDetail::Table(TableProps { serial: true, .. }))
    }

    /// `WITH (compress = true)`: its rows' store codes its heaps.
    pub(super) fn compress(&self) -> bool {
        match self.detail {
            RelDetail::Table(t) => t.compress,
            RelDetail::View(v) => v.props.compress,
        }
    }
}

/// `table 'orders' (id=17)` — the subject of a registration or precheck message.
impl std::fmt::Display for RelRow<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} '{}' (id={})", self.kind().noun(), self.name, self.id)
    }
}

/// Decode row `row` of relation family `family` (Table or View). The raw `flags`
/// word does not escape, so the rejections below hold for every caller.
pub(super) fn read_rel_row(family: SysFamily, batch: &Batch, row: usize) -> Result<RelRow<'_>, String> {
    let (noun, id) = (family.row_noun(), batch.get_pk(row) as u64);
    let unreadable = |e: String| format!("catalog invariant violated: {noun} (id={id}) {e}");
    let (schema_id, name, parts) = match family {
        SysFamily::Table => {
            let r = TableTabRow::read(batch, row).map_err(unreadable)?;
            let parts = r.parts().map(|(pk, props)| (pk, RelDetail::Table(props)));
            (r.schema_id, r.name, parts)
        }
        SysFamily::View => {
            let r = ViewTabRow::read(batch, row).map_err(unreadable)?;
            let parts = r.parts().map(|(pk, view)| (pk, RelDetail::View(view)));
            (r.schema_id, r.name, parts)
        }
        _ => unreachable!("{} is not a relation family", family.name()),
    };
    let (pk, detail) = parts.map_err(|e| format!("catalog invariant violated: {noun} '{name}' (id={id}) {e}"))?;
    Ok(RelRow { id, schema_id, name, pk, detail })
}

/// What one delta does to one PK: where its `-1` and `+1` rows are, and the
/// summed weight. A PK carrying both signs is a **rewrite pair** (a rename).
pub(super) struct PkSignature {
    pub(super) pk: u128,
    /// `sys_leading_id` of the key, so no consumer re-derives it.
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

/// Which signs one delta carries on one PK.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PkShape {
    Create,
    Drop,
    /// Both signs on one PK — a rewrite (a rename), whose net stays live.
    Pair,
}

impl PkSignature {
    pub(super) fn shape(&self) -> PkShape {
        match (self.neg, self.pos) {
            (Some(_), Some(_)) => PkShape::Pair,
            (Some(_), None) => PkShape::Drop,
            (None, _) => PkShape::Create,
        }
    }

    /// The DDL verb this shape performs, for guard messages.
    pub(super) fn verb(&self) -> &'static str {
        match self.shape() {
            PkShape::Pair => "ALTER",
            PkShape::Drop => "DROP",
            PkShape::Create => "CREATE",
        }
    }
}

/// One [`PkSignature`] per distinct PK, in first-appearance order, over a batch in
/// any row order. Zero-weight rows are skipped.
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
                leading: gnitz_wire::sys_leading_id(batch.get_pk_bytes(i)),
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

/// `(owner, cols)` of every unique index `batch` (an IDX_TAB delta) creates: the
/// DDL driver's pre-flight input, read before the precheck. A row that does not
/// decode is skipped; the precheck refuses the bundle over it.
pub(crate) fn unique_index_creates(batch: &Batch) -> Vec<(u64, PkColList)> {
    pk_signatures(batch)
        .into_iter()
        .filter(|s| s.shape() == PkShape::Create)
        .filter_map(|s| IdxTabRow::read(batch, s.row).and_then(|r| r.parts()).ok())
        .filter_map(|(owner, cols, unique)| unique.then_some((owner, cols)))
        .collect()
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

/// One named record per family, indexed by [`SysFamily::index`]: what every
/// DdlSync group of the family carries.
static RECORDS: LazyLock<[Box<[u8]>; SysFamily::COUNT]> = LazyLock::new(|| {
    std::array::from_fn(|i| {
        let family = SysFamily::ALL[i];
        encode_record(family.wire().pk_cols, &family.column_defs())
    })
});

/// The named record of a relation keyed `pk` over `defs` — the one a RESOLVE
/// reply and every push/DDL SAL slot carries.
pub(in crate::catalog) fn encode_record(pk: &[u32], defs: &[CatalogColumn]) -> Box<[u8]> {
    gnitz_wire::schema_block::encode(defs.iter().map(|c| c.def.block_col()), pk).into()
}

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
    Ok(SchemaDescriptor::try_new(&cols, pk_cols)?)
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

    /// This family's named record.
    pub(crate) fn record(self) -> &'static [u8] {
        &RECORDS[self.index()]
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
                    (PUBLIC_SCHEMA_ID, gnitz_wire::PUBLIC_SCHEMA),
                ] {
                    SchemaTabRow { schema_id, name: name.into() }.write(bb, 1);
                }
            }
            SysFamily::Table => {
                for family in SysFamily::ALL {
                    let row = TableTabRow {
                        table_id: family.id(),
                        schema_id: SYSTEM_SCHEMA_ID,
                        name: family.name().into(),
                        pk_col_idx: PkColList::from_slice(family.wire().pk_cols).pack(),
                        flags: gnitz_wire::TableProps::default().pack(),
                    };
                    row.write(bb, 1);
                }
            }
            SysFamily::Column => {
                for family in SysFamily::ALL {
                    for (i, cd) in family.column_defs().iter().enumerate() {
                        cd.write_col_tab_row(bb, family.id(), i, 1);
                    }
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
