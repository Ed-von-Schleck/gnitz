//! System table constants, the per-family descriptor table, the per-family row
//! decoders over those constants, and the batch-shape analyses (per-PK
//! signatures, sign partitions) every guard and hook keys on.
//!
//! Pure data, stateless codecs and batch-local analysis — no state, no
//! CatalogEngine dependency.

use rustc_hash::FxHashMap;

use super::{CatalogColumn, RelFacts};
use gnitz_expr::RowSource;
use gnitz_expr::{payload_str, payload_string, payload_u64};
use gnitz_store::relation::RelationKind;
use gnitz_store::schema::{Placement, SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, BatchBuilder};
use gnitz_wire::sys_rows::{
    write_col_tab_row, write_schema_tab_row, write_table_tab_row, ColTabRow, SchemaTabRow, SysRowSink, TableTabRow,
};
use gnitz_wire::MAX_COLUMNS;
use gnitz_wire::{ColType, ColumnDef, TableDistribution, ViewProps};
use gnitz_wire::{
    COLTAB_PAY_FK_COL_IDX, COLTAB_PAY_FK_TABLE_ID, COLTAB_PAY_IS_HIDDEN, COLTAB_PAY_IS_NULLABLE, COLTAB_PAY_NAME,
    COLTAB_PAY_SCALE, COLTAB_PAY_TYPE_CODE, IDXTAB_PAY_FLAGS, IDXTAB_PAY_OWNER_ID, IDXTAB_PAY_SOURCE_COLS,
    RELTAB_PAY_NAME, RELTAB_PAY_SCHEMA_ID, TABTAB_PAY_FLAGS, TABTAB_PAY_PK_COL_IDX, VIEWTAB_PAY_CAPACITY,
    VIEWTAB_PAY_DELTA, VIEWTAB_PAY_FLAGS, VIEWTAB_PAY_OWNER_VIEW_ID, VIEWTAB_PAY_PK_COL_IDX,
};

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
const _: () = assert!(FIRST_ALLOCATED_ID >= gnitz_wire::FIRST_USER_SCHEMA_ID);

// PK list encoding lives in gnitz-wire so the client and engine cannot drift on
// the on-disk format. Every site spells the packers `gnitz_wire::…`, or reaches
// them through the row decoders below.
use gnitz_wire::unpack_pk_cols;
pub(super) use gnitz_wire::PkColList;

// ---------------------------------------------------------------------------
// Per-family row-view decoders — the one reading of each family's *full row
// shape*, run by the precheck arms and again by the register hooks. A caller
// after one named field reads it through that field's `*_PAY_*` constant
// instead.
//
// The two relation families decode a wire `Batch`, which is what every caller
// holds; the rest are generic over `RowSource`, so a stored row or a positioned
// cursor decodes too.
// ---------------------------------------------------------------------------

/// What `register_relation` needs to build and register one relation: the values
/// decoded off its TABLE_TAB / VIEW_TAB row, plus the placement its own family
/// derives.
pub(super) struct RelationRegistration<'a> {
    pub(super) kind: RelationKind,
    pub(super) id: u64,
    pub(super) schema_id: u64,
    pub(super) name: &'a str,
    pub(super) pk: PkColList,
    pub(super) placement: Placement,
    pub(super) facts: RelFacts,
}

/// Decode TABLE_TAB `row` into the whole registration it describes, `id` off the
/// row's own PK included. The raw `flags` word does not escape, so the
/// rejections below hold on the paths that bypass the precheck too.
pub(super) fn read_table_tab_row(batch: &Batch, row: usize) -> Result<RelationRegistration<'_>, String> {
    let name = payload_str(batch, row, RELTAB_PAY_NAME);
    let props = gnitz_wire::TableProps::from_flags(payload_u64(batch, row, TABTAB_PAY_FLAGS))
        .map_err(|e| format!("catalog invariant violated: relation '{name}' {e}"))?;
    let kind = if props.stream {
        RelationKind::Stream
    } else {
        RelationKind::BaseTable
    };
    let noun = kind.noun();
    // The PK list is decoded before the placement is built: a `Keyed` prefix is
    // a leading-prefix length into it, and nothing downstream re-checks it —
    // `Placement::resolve` normalizes only the `0` sentinel.
    let pk = unpack_pk_cols(payload_u64(batch, row, TABTAB_PAY_PK_COL_IDX))
        .map_err(|rule| format!("catalog invariant violated: {noun} '{name}' {rule}"))?;
    props
        .validate(pk.as_slice().len())
        .map_err(|e| format!("catalog invariant violated: {noun} '{name}' {e}"))?;
    Ok(RelationRegistration {
        kind,
        id: batch.get_pk(row) as u64,
        schema_id: payload_u64(batch, row, RELTAB_PAY_SCHEMA_ID),
        name,
        pk,
        placement: match props.distribution {
            TableDistribution::Replicated => Placement::Replicated,
            TableDistribution::Keyed { prefix_len } => Placement::Keyed { prefix_len },
        },
        facts: RelFacts {
            // A base table's PK is unique by `enforce_unique_pk`; a stream's is not.
            pk_repeats: props.stream,
            serial: props.serial,
        },
    })
}

/// A VIEW_TAB row as decoded. Not yet a [`RelationRegistration`]: a view's
/// placement is a fold over its sources, which this reader cannot see.
pub(super) struct ViewRegistration<'a> {
    pub(super) schema_id: u64,
    pub(super) name: &'a str,
    pub(super) pk: PkColList,
    pub(super) props: ViewProps,
    /// The user view this row is an internal chain segment of; `0` for a user
    /// view.
    pub(super) owner_view_id: u64,
    /// [`gnitz_wire::ViewFlags::pk_repeats`].
    pub(super) pk_repeats: bool,
}

/// Decode VIEW_TAB `row`.
pub(super) fn read_view_tab_row(batch: &Batch, row: usize) -> Result<ViewRegistration<'_>, String> {
    let name = payload_str(batch, row, RELTAB_PAY_NAME);
    let pk = unpack_pk_cols(payload_u64(batch, row, VIEWTAB_PAY_PK_COL_IDX))
        .map_err(|rule| format!("catalog invariant violated: view '{name}' {rule}"))?;
    let props = ViewProps::from_row(
        payload_u64(batch, row, VIEWTAB_PAY_CAPACITY),
        payload_u64(batch, row, VIEWTAB_PAY_DELTA),
    )
    .map_err(|e| format!("view '{name}': {e}"))?;
    let flags = gnitz_wire::ViewFlags::from_flags(payload_u64(batch, row, VIEWTAB_PAY_FLAGS))
        .map_err(|e| format!("catalog invariant violated: view '{name}' {e}"))?;
    Ok(ViewRegistration {
        schema_id: payload_u64(batch, row, RELTAB_PAY_SCHEMA_ID),
        name,
        pk,
        props,
        owner_view_id: payload_u64(batch, row, VIEWTAB_PAY_OWNER_VIEW_ID),
        pk_repeats: flags.pk_repeats,
    })
}

/// Decode IDX_TAB `row`: `(owner_id, col_indices, props)` — the one decoding of
/// `source_col_idx`, so no consumer holds its undecoded word.
pub(super) fn read_idx_tab_row<S: RowSource>(
    src: &S,
    row: usize,
) -> Result<(u64, PkColList, gnitz_wire::IndexProps), String> {
    Ok((
        payload_u64(src, row, IDXTAB_PAY_OWNER_ID),
        unpack_pk_cols(payload_u64(src, row, IDXTAB_PAY_SOURCE_COLS)).map_err(|rule| format!("column list {rule}"))?,
        gnitz_wire::IndexProps::from_flags(payload_u64(src, row, IDXTAB_PAY_FLAGS))?,
    ))
}

/// Decode COL_TAB `row` into the `CatalogColumn` the schema builder consumes. Every
/// word must fit the width it is stored at and decode to a value
/// `write_col_tab_row` could emit: an overflowing one would leave a stored row no
/// client can reproduce, and so no later `-1` can retract.
pub(super) fn read_col_tab_row<S: RowSource>(src: &S, row: usize) -> Result<CatalogColumn, String> {
    let word = |field: &str, pi: usize, max: u64| {
        let w = payload_u64(src, row, pi);
        if w > max {
            return Err(format!(
                "column record carries {field} = {w}, past the {max} its stored width holds"
            ));
        }
        Ok(w)
    };
    let flag = |field: &str, pi: usize| word(field, pi, 1).map(|w| w == 1);
    let code = word("type_code", COLTAB_PAY_TYPE_CODE, u8::MAX as u64)? as u8;
    let scale = word("scale", COLTAB_PAY_SCALE, u8::MAX as u64)? as u8;
    let ty = ColType::from_wire(code, scale)
        .ok_or_else(|| format!("column record carries an invalid column type {code}/{scale}"))?;
    Ok(CatalogColumn {
        def: ColumnDef {
            name: payload_string(src, row, COLTAB_PAY_NAME),
            ty,
            is_nullable: flag("is_nullable", COLTAB_PAY_IS_NULLABLE)?,
            is_hidden: flag("is_hidden", COLTAB_PAY_IS_HIDDEN)?,
        },
        fk_table_id: payload_u64(src, row, COLTAB_PAY_FK_TABLE_ID),
        fk_col_idx: word("fk_col_idx", COLTAB_PAY_FK_COL_IDX, u32::MAX as u64)? as u32,
    })
}

impl CatalogColumn {
    /// Write this column as COL_TAB row `(owner_id, col_idx)` — the inverse of
    /// [`read_col_tab_row`].
    pub(crate) fn write_col_tab_row(&self, sink: &mut impl SysRowSink, owner_id: u64, col_idx: usize, weight: i64) {
        let row = ColTabRow {
            owner_id,
            col_idx: col_idx as u64,
            col: &self.def,
            fk_table_id: self.fk_table_id,
            fk_col_idx: self.fk_col_idx as u64,
        };
        write_col_tab_row(sink, &row, weight);
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
    pub(crate) creates: Vec<(u64, PkColList, gnitz_wire::IndexProps)>,
    pub(crate) drops: Vec<(u64, PkColList)>,
}

pub(crate) fn idx_tab_partition(batch: &Batch) -> IdxPartition {
    let mut out = IdxPartition::default();
    for sig in pk_signatures(SysFamily::Index, batch) {
        if sig.is_pair() {
            continue;
        }
        let row = sig.pos.or(sig.neg).expect("pk_signatures skips a zero-weight row");
        let Ok((owner_id, cols, props)) = read_idx_tab_row(batch, row) else {
            continue;
        };
        if sig.pos.is_some() {
            out.creates.push((owner_id, cols, props));
        } else {
            out.drops.push((owner_id, cols));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Schema derivation from the shared wire column slices
// ---------------------------------------------------------------------------

/// Build a `SchemaDescriptor` from one of `gnitz-wire`'s canonical system-table
/// column arrays. `const`, so [`SCHEMAS`] below costs nothing at runtime. Every
/// such family is [`Placement::Replicated`]: DDL is master-broadcast, so each
/// worker holds an identical full copy, and a reader single-sources one copy
/// instead of gathering N.
const fn from_wire_cols(cols: &[gnitz_wire::WireSysCol], pk_indices: &[u32]) -> SchemaDescriptor {
    let mut buf = [SchemaColumn::EMPTY; MAX_COLUMNS];
    let mut i = 0;
    while i < cols.len() {
        buf[i] = SchemaColumn::new(cols[i].type_code, cols[i].nullable);
        i += 1;
    }
    let (head, _) = buf.split_at(cols.len());
    SchemaDescriptor::new_with_placement(head, pk_indices, Placement::Replicated)
}

/// Pre-computed schema statics, one per family, indexed by [`SysFamily::index`]
/// — initialised at compile time, never reconstructed.
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
    CircuitNodes = gnitz_wire::CIRCUIT_NODES_TAB,
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

    /// This family's fixed schema. Borrowed from the `static` that holds it —
    /// a `SchemaDescriptor` is 360 bytes, so returning it by value put a copy on
    /// every catalog write.
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
            .map(|c| ColumnDef::new(c.name, c.type_code, c.nullable).into())
            .collect()
    }

    /// The rows a fresh database's store of this family starts with.
    pub(in crate::catalog) fn write_seed_rows(self, bb: &mut BatchBuilder) {
        match self {
            SysFamily::Schema => {
                for (schema_id, name) in [(SYSTEM_SCHEMA_ID, "_system"), (PUBLIC_SCHEMA_ID, "public")] {
                    write_schema_tab_row(bb, &SchemaTabRow { schema_id, name }, 1);
                }
            }
            SysFamily::Table => {
                for family in SysFamily::ALL {
                    let row = TableTabRow {
                        table_id: family.id(),
                        schema_id: SYSTEM_SCHEMA_ID,
                        name: family.name(),
                        pk_col_idx: gnitz_wire::pack_pk_cols(family.wire().pk_cols),
                        flags: 0,
                    };
                    write_table_tab_row(bb, &row, 1);
                }
            }
            SysFamily::Column => {
                for family in SysFamily::ALL {
                    write_col_tab_rows(bb, family.id(), &family.column_defs(), 1);
                }
            }
            SysFamily::View | SysFamily::Index | SysFamily::Sequence | SysFamily::CircuitNodes => {}
        }
    }

    /// Topological creation priority: lower = created first, destroyed last.
    /// A creating `DDL_TXN` bundle ingests ascending, so every register hook
    /// sees its dependencies in the memtable; an all-negative one descending,
    /// retiring a dependent before what it depends on.
    #[inline]
    pub(crate) fn topo_priority(self) -> u8 {
        match self {
            SysFamily::Schema => 0,
            SysFamily::Column => 1,
            SysFamily::CircuitNodes => 2,
            SysFamily::Table => 5,
            SysFamily::View => 6,
            SysFamily::Index => 7,
            SysFamily::Sequence => 99,
        }
    }

    /// The leading id of one of this family's PKs: the whole key where it is one
    /// column, its high half where the key is a pair — where `pk as u64` would
    /// take the trailing half instead (a column index, or a node id).
    #[inline]
    pub(super) fn leading_id(self, pk: u128) -> u64 {
        match self {
            SysFamily::Column | SysFamily::CircuitNodes => gnitz_wire::unpack_pair_pk(pk).0,
            SysFamily::Schema | SysFamily::Table | SysFamily::View | SysFamily::Index | SysFamily::Sequence => {
                pk as u64
            }
        }
    }

    /// The lowest id a client may write in this family's id space; everything
    /// below is bootstrap-owned. Read against [`Self::leading_id`], so Column's
    /// floor is its owner's. `None` where the PK is no id space at all.
    pub(super) fn first_user_id(self) -> Option<u64> {
        match self {
            SysFamily::Schema => Some(gnitz_wire::FIRST_USER_SCHEMA_ID),
            SysFamily::Table | SysFamily::View | SysFamily::Column | SysFamily::Sequence | SysFamily::Index => {
                Some(gnitz_wire::FIRST_USER_TABLE_ID)
            }
            SysFamily::CircuitNodes => None,
        }
    }

    /// The exclusive upper bound on an id a client may write: the ceiling
    /// `allocate_ids` allocates under.
    pub(super) fn id_ceiling(self) -> Option<u64> {
        self.allocates_ids().then_some(gnitz_wire::CATALOG_ID_CEILING)
    }

    /// Does this family's PK draw from the catalog object-id counter?
    pub(super) fn allocates_ids(self) -> bool {
        match self {
            SysFamily::Schema | SysFamily::Table | SysFamily::View | SysFamily::Index => true,
            SysFamily::Column | SysFamily::Sequence | SysFamily::CircuitNodes => false,
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

    /// Are this family's rows retracted only with the relation that owns them — as a band,
    /// by the drop cascade? A client delta may then carry no unpaired `-1` for it.
    pub(super) fn retracts_with_owner(self) -> bool {
        match self {
            SysFamily::Column | SysFamily::CircuitNodes => true,
            SysFamily::Schema | SysFamily::Table | SysFamily::View | SysFamily::Index | SysFamily::Sequence => false,
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

    /// How a guard message names one row of this family: a pair-keyed family's
    /// two ids are meaningless rendered as the one number the widened key is.
    pub(in crate::catalog) fn pk_label(self, pk: u128) -> String {
        let (hi, lo) = gnitz_wire::unpack_pair_pk(pk);
        match self {
            SysFamily::Column => format!("column {lo} of owner {hi}"),
            SysFamily::CircuitNodes => format!("view {hi} node {lo}"),
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
