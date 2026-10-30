//! System table constants, the per-family descriptor table, the per-family row
//! decoders over those constants, and the batch-shape analyses (per-PK
//! signatures and their shapes) every guard and hook keys on; the `DDL_TXN`
//! bundle and its wire-shape rules; and `IdSet`, the sorted id set the guards
//! and hooks probe.
//!
//! Pure data, stateless codecs and batch-local analysis — no state, no
//! CatalogEngine dependency.

use rustc_hash::FxHashMap;
use std::borrow::Cow;
use std::sync::LazyLock;

use gnitz_store::relation::RelationKind;
use gnitz_wire::{
    CatalogColumn, ColTabRow, ColTabSlot, IdxTabRow, PkColList, SchemaTabRow, SysRow, TableProps, TableTabRow,
    ViewParts, ViewTabRow,
};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(super) const SYSTEM_SCHEMA_ID: u64 = 1;
pub(crate) const PUBLIC_SCHEMA_ID: u64 = 2;

// ---------------------------------------------------------------------------
// Relation rows — TABLE_TAB and VIEW_TAB decoded as one shape, by the precheck
// and again by the register hook.
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
}

/// `table 'orders' (id=17)` — the subject of a registration or precheck message.
impl std::fmt::Display for RelRow<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} '{}' (id={})", self.kind().noun(), self.name, self.id)
    }
}

/// Decode row `row` of relation family `family` (Table or View) and hold it to
/// the rules its words cannot encode against its key, so every caller gets the
/// same refusals.
pub(super) fn read_rel_row(family: SysFamily, batch: &Batch, row: usize) -> Result<RelRow<'_>, String> {
    let (noun, id) = (family.row_noun(), gnitz_wire::sys_leading_id(batch.get_pk_bytes(row)));
    let unreadable = |e: String| format!("catalog invariant violated: {noun} (id={id}) {e}");
    let (schema_id, name, parts) = match family {
        SysFamily::Table => {
            let r = TableTabRow::read(batch, row).map_err(unreadable)?;
            let parts = r.props().map(|props| (r.pk_col_idx, RelDetail::Table(props)));
            (r.schema_id, r.name, parts)
        }
        SysFamily::View => {
            let r = ViewTabRow::read(batch, row).map_err(unreadable)?;
            let parts = r.parts().map(|view| (r.pk_col_idx, RelDetail::View(view)));
            (r.schema_id, r.name, parts)
        }
        _ => unreachable!("{} is not a relation family", family.name()),
    };
    let (pk, detail) = parts.map_err(|e| format!("catalog invariant violated: {noun} '{name}' (id={id}) {e}"))?;
    Ok(RelRow { id, schema_id, name, pk, detail })
}

/// What one delta does to one PK: where its `-1` and `+1` rows are. A PK
/// carrying both signs is a **rewrite pair** (a rename).
pub(super) struct PkSignature {
    /// The key's leading column: the id of the object the row describes.
    pub(super) leading: u64,
    /// A pair-keyed family's second key column (COL_TAB's `col_idx`).
    pub(super) member: Option<u64>,
    /// First row index carrying this PK; its OPK bytes address the live row.
    pub(super) row: usize,
    /// First `-1` / `+1` row index for this PK.
    pub(super) neg: Option<usize>,
    pub(super) pos: Option<usize>,
    /// A sign occurs on more than one row.
    pub(super) repeats_a_sign: bool,
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
    let mut by_pk: FxHashMap<(u64, Option<u64>), usize> = FxHashMap::default();
    for i in 0..batch.len() {
        let w = batch.get_weight(i);
        if w == 0 {
            continue;
        }
        let key = batch.get_pk_bytes(i);
        let (leading, member) = (
            gnitz_wire::sys_leading_id(key),
            (key.len() > 8).then(|| gnitz_wire::sys_leading_id(&key[8..])),
        );
        let slot = *by_pk.entry((leading, member)).or_insert_with(|| {
            sigs.push(PkSignature {
                leading,
                member,
                row: i,
                neg: None,
                pos: None,
                repeats_a_sign: false,
            });
            sigs.len() - 1
        });
        let sig = &mut sigs[slot];
        // Keep the FIRST row of each sign; a later one only sets the flag.
        let sign_slot = if w < 0 { &mut sig.neg } else { &mut sig.pos };
        sig.repeats_a_sign |= sign_slot.is_some();
        sign_slot.get_or_insert(i);
    }
    sigs
}

/// One `DDL_TXN` bundle: at most one block per system family, and none for a
/// family only the master writes (see [`SysFamily::master_only`]).
#[derive(Default)]
pub(crate) struct DdlBundle([Option<Batch>; SysFamily::COUNT]);

impl DdlBundle {
    /// Add `family`'s block; `Err` for a family the wire may not write, or when
    /// the bundle already carries one.
    pub(crate) fn insert(&mut self, family: SysFamily, batch: Batch) -> Result<(), String> {
        if family.master_only() {
            return Err(format!(
                "family {} ({}) is not writable from the wire",
                family.id(),
                family.name()
            ));
        }
        match &mut self.0[family.index()] {
            Some(_) => Err(format!("bundle carries two blocks for family {}", family.id())),
            slot => {
                *slot = Some(batch);
                Ok(())
            }
        }
    }

    /// `family`'s block, if the bundle carries one.
    pub(crate) fn get(&self, family: SysFamily) -> Option<&Batch> {
        self.0[family.index()].as_ref()
    }

    /// How many families the bundle carries a block for.
    pub(crate) fn family_count(&self) -> usize {
        self.0.iter().flatten().count()
    }

    /// The distinct `(owner, cols)` of every unique index the bundle's IDX_TAB
    /// block creates: the DDL driver's pre-flight input, read before the
    /// precheck. A row that does not decode is skipped; the precheck refuses the
    /// bundle over it.
    pub(crate) fn unique_index_creates(&self) -> Vec<(u64, PkColList)> {
        let Some(batch) = self.get(SysFamily::Index) else {
            return Vec::new();
        };
        let mut creates: Vec<_> = pk_signatures(batch)
            .into_iter()
            .filter(|s| s.shape() == PkShape::Create)
            .filter_map(|s| IdxTabRow::read(batch, s.row).ok())
            .filter_map(|r| r.is_unique.then_some((r.owner_id, r.source_col_idx)))
            .collect();
        creates.sort_unstable();
        creates.dedup();
        creates
    }

    /// The blocks in [`SysFamily::ALL`] order.
    pub(crate) fn into_blocks(self) -> impl Iterator<Item = (SysFamily, Batch)> {
        SysFamily::ALL
            .into_iter()
            .zip(self.0)
            .filter_map(|(f, b)| Some((f, b?)))
    }
}

// ---------------------------------------------------------------------------
// Schema derivation from the shared wire column slices
// ---------------------------------------------------------------------------

/// Each family's schema and named record, indexed by [`SysFamily::index`], both
/// derived from `gnitz-wire`'s system-table columns as a user relation's are
/// from its COL_TAB records. The record is what every DdlSync group of the
/// family carries.
static SCHEMAS: LazyLock<[(SchemaDescriptor, Box<[u8]>); SysFamily::COUNT]> = LazyLock::new(|| {
    SysFamily::ALL.map(|family| {
        let (defs, pk) = (family.column_defs(), family.wire().pk_cols);
        let schema = build_schema_from_col_defs(RelationKind::SystemCatalog, &defs, pk)
            .expect("a system family is an admissible schema");
        (schema, encode_record(pk, &defs))
    })
});

/// The named record of a relation keyed `pk` over `defs` — the one a RESOLVE
/// reply and every push/DDL SAL slot carries.
pub(in crate::catalog) fn encode_record(pk: &[u32], defs: &[CatalogColumn]) -> Box<[u8]> {
    gnitz_wire::schema_block::encode(defs.iter().map(|c| c.def.block_col()), pk).into()
}

/// The `SchemaDescriptor` COL_TAB records describe, or the first admissibility
/// rule they break; messages are bare predicates, and callers add the context.
/// The duplicate-name rule is ingestion points only — a view segment
/// legitimately carries two visible columns of one name (a join chain's output).
pub(super) fn build_schema_from_col_defs(
    kind: RelationKind,
    col_defs: &[CatalogColumn],
    pk_cols: &[u32],
) -> Result<SchemaDescriptor, String> {
    if col_defs.is_empty() {
        return Err("has no column records".into());
    }
    if kind.is_ingestion_point() {
        let visible = col_defs.iter().filter(|c| !c.def.is_hidden);
        if let Some(name) = gnitz_wire::first_duplicate(visible.map(|c| c.def.name.as_str())) {
            return Err(format!("has duplicate column name '{name}'"));
        }
    }
    Ok(SchemaDescriptor::try_new(
        &col_defs
            .iter()
            .map(|c| SchemaColumn::of_def(&c.def))
            .collect::<Vec<_>>(),
        pk_cols,
    )?)
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
    pub(in crate::catalog) const COUNT: usize = gnitz_wire::SYS_FAMILIES.len();

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
    pub(in crate::catalog) const fn index(self) -> usize {
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
    pub(in crate::catalog) fn name(self) -> &'static str {
        self.wire().name
    }

    /// This family's fixed schema.
    #[inline]
    pub(crate) fn schema(self) -> &'static SchemaDescriptor {
        &SCHEMAS[self.index()].0
    }

    /// This family's named record.
    pub(crate) fn record(self) -> &'static [u8] {
        &SCHEMAS[self.index()].1
    }

    /// This family's column definitions, from the same wire slice its schema is
    /// built from — compile-time data, never read back from COL_TAB.
    pub(in crate::catalog) fn column_defs(self) -> Vec<CatalogColumn> {
        self.wire()
            .column_defs()
            .map(|def| CatalogColumn { def, fk: None })
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
                        pk_col_idx: PkColList::from_slice(family.wire().pk_cols),
                        flags: gnitz_wire::TableProps::default(),
                    };
                    row.write(bb, 1);
                }
            }
            SysFamily::Column => {
                for family in SysFamily::ALL {
                    for (i, def) in family.wire().column_defs().enumerate() {
                        ColTabRow::of(family.id(), i as u64, &def, None).write(bb, 1);
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

    /// How a guard message names the row `sig` keys.
    pub(in crate::catalog) fn pk_label(self, sig: &PkSignature) -> String {
        match sig.member {
            // COL_TAB's key is (owner_id, col_idx).
            Some(col_idx) => format!("{} {col_idx} of owner {}", self.row_noun(), sig.leading),
            None => format!("{} {}", self.row_noun(), sig.leading),
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

/// A set of catalog ids, sorted once so every probe is a binary search: the
/// probes run per row of a client-supplied block bounded only by the frame.
#[derive(Debug, Default)]
pub(in crate::catalog) struct IdSet(Vec<u64>);

impl IdSet {
    pub(in crate::catalog) fn new(ids: impl IntoIterator<Item = u64>) -> Self {
        let mut ids: Vec<u64> = ids.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        IdSet(ids)
    }

    pub(in crate::catalog) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(in crate::catalog) fn contains(&self, id: u64) -> bool {
        self.0.binary_search(&id).is_ok()
    }

    /// The ids, strictly ascending.
    pub(in crate::catalog) fn ids(&self) -> &[u64] {
        &self.0
    }

    /// These ids and `more`.
    pub(in crate::catalog) fn with(self, more: impl IntoIterator<Item = u64>) -> Self {
        IdSet::new(self.0.into_iter().chain(more))
    }
}
