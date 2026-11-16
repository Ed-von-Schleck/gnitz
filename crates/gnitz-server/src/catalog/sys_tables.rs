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
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor, encode_schema_record};

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
    let unreadable = |e: String| format!("{noun} (id={id}) {e}");
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
    let (pk, detail) = parts.map_err(|e| format!("{noun} '{name}' (id={id}) {e}"))?;
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

    /// How a guard message names the `family` row this key addresses.
    pub(super) fn label(&self, family: SysFamily) -> String {
        match self.member {
            // COL_TAB's key is (owner_id, col_idx).
            Some(col_idx) => format!("{} {col_idx} of owner {}", family.row_noun(), self.leading),
            None => format!("{} {}", family.row_noun(), self.leading),
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
/// family only the master writes (see [`SysFamilyExt::master_only`]).
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
    pub(in crate::catalog) fn family_count(&self) -> usize {
        self.0.iter().flatten().count()
    }

    /// The ids of the rows the bundle's `family` block creates.
    pub(in crate::catalog) fn creates(&self, family: SysFamily) -> impl Iterator<Item = u64> {
        let sigs = self.get(family).map(pk_signatures).unwrap_or_default();
        sigs.into_iter()
            .filter(|s| s.shape() == PkShape::Create)
            .map(|s| s.leading)
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
    pub(in crate::catalog) fn into_blocks(self) -> impl Iterator<Item = (SysFamily, Batch)> {
        SysFamily::ALL
            .into_iter()
            .zip(self.0)
            .filter_map(|(f, b)| Some((f, b?)))
    }
}

// ---------------------------------------------------------------------------
// Schema derivation from the shared wire column slices
// ---------------------------------------------------------------------------

/// Each family's schema and SAL record, indexed by [`SysFamily::index`], both
/// derived from `gnitz-wire`'s system-table columns as a user relation's are
/// from its COL_TAB records. The record is what every DdlSync group of the
/// family carries.
static SCHEMAS: LazyLock<[(SchemaDescriptor, Box<[u8]>); SysFamily::COUNT]> = LazyLock::new(|| {
    SysFamily::ALL.map(|family| {
        let (defs, pk) = (family.column_defs(), family.def().pk_cols);
        let schema = build_schema_from_col_defs(&defs, pk).expect("a system family is an admissible schema");
        let record = encode_schema_record(&schema).into();
        (schema, record)
    })
});

/// The named record of a relation keyed `pk` over `defs`: the one a RESOLVE
/// reply carries.
pub(in crate::catalog) fn encode_record(pk: &[u32], defs: &[CatalogColumn]) -> Box<[u8]> {
    gnitz_wire::schema_record::encode(defs.iter().map(|c| c.def.record_col()), pk).into()
}

/// The `SchemaDescriptor` COL_TAB records describe, or the first admissibility
/// rule they break; messages are bare predicates, and callers add the context.
pub(super) fn build_schema_from_col_defs(
    col_defs: &[CatalogColumn],
    pk_cols: &[u32],
) -> Result<SchemaDescriptor, String> {
    if col_defs.is_empty() {
        return Err("has no column records".into());
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
// Engine policy per system family
// ---------------------------------------------------------------------------

pub(crate) use gnitz_wire::SysFamily;

/// What the engine does with each system family: its schema, its seed rows, and
/// the rules its guards hold a bundle's rows to. Each `match` is exhaustive, so a
/// newly-added family cannot silently skip one.
pub(crate) trait SysFamilyExt: Copy {
    /// This family's fixed schema.
    fn schema(self) -> &'static SchemaDescriptor;

    /// This family's [`encode_schema_record`]: the record its DdlSync groups carry.
    fn sal_record(self) -> &'static [u8];

    /// This family's column definitions, from the same wire slice its schema is
    /// built from — compile-time data, never read back from COL_TAB.
    fn column_defs(self) -> Vec<CatalogColumn>;

    /// The rows a fresh database's store of this family starts with.
    fn write_seed_rows(self, bb: &mut BatchBuilder);

    /// Does this family's PK draw from the catalog object-id counter?
    fn allocates_ids(self) -> bool;

    /// The payload columns a rewrite pair (`-1` + `+1` on one PK) may change, as
    /// a payload-index bit mask; `None` where no emitter produces a pair —
    /// there is no schema-rename or index-rename surface.
    fn pair_change_mask(self) -> Option<u64>;

    /// Are this family's rows retracted only with the relation that owns them — as a band,
    /// by the drop cascade? A client delta may then carry no unpaired `-1` for it.
    fn retracts_with_owner(self) -> bool;

    /// Does this family hold the master's positions rather than a set? `true`
    /// for Sequence alone: its rows move with every id allocation, SERIAL
    /// reservation and checkpoint, and are the id counters and the checkpoint
    /// generation the resume verdict reads at boot. So no
    /// client bundle may carry one, no worker is sent one, and no view scans it.
    fn master_only(self) -> bool;
}

impl SysFamilyExt for SysFamily {
    fn schema(self) -> &'static SchemaDescriptor {
        &SCHEMAS[self.index()].0
    }

    fn sal_record(self) -> &'static [u8] {
        &SCHEMAS[self.index()].1
    }

    fn column_defs(self) -> Vec<CatalogColumn> {
        self.def()
            .column_defs()
            .map(|def| CatalogColumn { def, fk: None })
            .collect()
    }

    fn write_seed_rows(self, bb: &mut BatchBuilder) {
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
                    // Replicated, as registered at open.
                    let props = TableProps {
                        distribution: gnitz_wire::TableDistribution::Replicated,
                        ..Default::default()
                    };
                    let pk = PkColList::from_slice(family.def().pk_cols);
                    let row = TableTabRow::of(family.id(), SYSTEM_SCHEMA_ID, family.name(), pk, props);
                    row.write(bb, 1);
                }
            }
            SysFamily::Column => {
                for family in SysFamily::ALL {
                    for (i, def) in family.def().column_defs().enumerate() {
                        ColTabRow::of(family.id(), i as u64, &def, None).write(bb, 1);
                    }
                }
            }
            SysFamily::View | SysFamily::Index | SysFamily::Sequence | SysFamily::Circuit => {}
        }
    }

    fn allocates_ids(self) -> bool {
        match self {
            SysFamily::Schema | SysFamily::Table | SysFamily::View | SysFamily::Index => true,
            SysFamily::Column | SysFamily::Sequence | SysFamily::Circuit => false,
        }
    }

    fn pair_change_mask(self) -> Option<u64> {
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

    fn retracts_with_owner(self) -> bool {
        match self {
            SysFamily::Column | SysFamily::Circuit => true,
            SysFamily::Schema | SysFamily::Table | SysFamily::View | SysFamily::Index | SysFamily::Sequence => false,
        }
    }

    fn master_only(self) -> bool {
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
}

/// The families applied before `family`: those whose rows its register hook
/// reads from the store (COL_TAB, CIRCUIT_TAB), whose registrations it reads
/// (an index's owner), or whose rows its precheck reads (a relation's schema,
/// which only a live bundle needs applied).
const fn applies_after(family: SysFamily) -> &'static [SysFamily] {
    match family {
        SysFamily::Schema | SysFamily::Column | SysFamily::Circuit | SysFamily::Sequence => &[],
        SysFamily::Table => &[SysFamily::Schema, SysFamily::Column],
        SysFamily::View => &[SysFamily::Schema, SysFamily::Column, SysFamily::Circuit],
        SysFamily::Index => &[SysFamily::Table, SysFamily::View],
    }
}

// `SysFamily::ALL` is the apply order, so it must order a family after every
// family it applies after.
const _: () = {
    let mut i = 0;
    while i < SysFamily::COUNT {
        let deps = applies_after(SysFamily::ALL[i]);
        let mut d = 0;
        while d < deps.len() {
            assert!(
                deps[d].index() < i,
                "SysFamily::ALL must order a family after every family it applies after"
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
