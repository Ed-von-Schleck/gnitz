//! Schema descriptor types and the schema-shaping free functions derived from
//! them.

use gnitz_wire::schema_block::SchemaBlockCol;
use gnitz_wire::ColType;

/// The refusal of column `c` of the `what` list, out of range for `schema`.
pub(crate) fn oob_col(what: &str, c: u32, schema: &SchemaDescriptor) -> String {
    format!("{what} {c} out of range ({} cols)", schema.num_columns())
}

pub(crate) use gnitz_wire::ReduceOutKey;
pub(crate) use gnitz_wire::TypeCode;
pub(crate) use gnitz_wire::MAX_COLUMNS;
pub(crate) use gnitz_wire::{MAX_PK_BYTES, MAX_PK_COLUMNS};

/// Resolved column addressing, homed in the leaf `gnitz-expr` crate so the
/// expression evaluator (and, through it, the SQL client) shares one definition
/// with the engine. Re-exported here because it *is* a schema fact —
/// `SchemaFacts::locate` produces one.
pub(crate) use gnitz_expr::ColumnLocator;

pub(crate) use gnitz_expr::ColumnTable;
/// Re-exported so a crate linking `gnitz-zset` alone can name it.
pub use gnitz_expr::SchemaFacts;

/// Order-preserving primary-key (OPK) primitives — every native→OPK encoder
/// (whole PK, index leading span), compare/pack, and the
/// re-export of the width-tagged `PkBuf` those encoders return. The **one**
/// import path: a second re-export would
/// leave the byte-order rule spelled two ways in adjacent lines of one call site.
pub mod key;

/// The payload row order — the second term of the (PK, payload) total order —
/// and the per-schema selection between its two comparators.
pub(crate) mod payload_order;

/// The precomputed per-row read/encode plan for an index's OPK leading-key span.
/// Lives in [`key`] with the rest of the native→OPK encoders it shares its byte
/// contract with; re-exported here because a spec is derived from a pair of
/// schemas, so call sites keep naming `crate::schema::KeySpec`.
pub use key::KeySpec;

mod route;
pub(crate) use route::worker_for_key;
pub use route::{ground_owner, worker_for_pk_bytes, Placement, Slot};

/// Accumulator for an operator's output schema: its PK columns, then its
/// payload columns.
pub(crate) struct DerivedSchema {
    cols: Vec<SchemaColumn>,
    pk_len: usize,
}

impl DerivedSchema {
    pub(crate) fn new() -> Self {
        DerivedSchema { cols: Vec::new(), pk_len: 0 }
    }

    /// Append one payload column.
    pub(crate) fn push(&mut self, col: SchemaColumn) {
        self.cols.push(col);
    }

    /// Append one PK column. Panics behind a payload column: `finish` numbers the
    /// PK `0..pk_len`.
    pub(crate) fn push_pk(&mut self, col: SchemaColumn) {
        assert_eq!(
            self.pk_len,
            self.cols.len(),
            "DerivedSchema: key column pushed behind a payload column"
        );
        self.cols.push(col);
        self.pk_len += 1;
    }

    /// Append `schema`'s PK columns in PK-list order.
    pub(crate) fn push_pk_of(&mut self, schema: &SchemaDescriptor) {
        schema.pk_columns().for_each(|(_, c)| self.push_pk(*c));
    }

    /// Append `schema`'s payload columns in schema order.
    pub(crate) fn push_payload_of(&mut self, schema: &SchemaDescriptor) {
        schema.payload_columns().for_each(|(_, c)| self.push(*c));
    }

    /// The schema pushed so far, as [`SchemaDescriptor::try_new`] admits it.
    pub(crate) fn finish(&self) -> Result<SchemaDescriptor, String> {
        let pk: Vec<u32> = (0..self.pk_len as u32).collect();
        SchemaDescriptor::try_new(&self.cols, &pk)
    }
}

// ---------------------------------------------------------------------------
// Schema descriptor
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SchemaColumn {
    pub type_code: TypeCode,
    size: u8,
    pub nullable: bool,
    is_signed: u8,
}

// `SchemaDescriptor` holds `MAX_COLUMNS` of these by value.
const _: () = assert!(std::mem::size_of::<SchemaColumn>() == 4);

impl SchemaColumn {
    /// The filler of a schema's unused column slots.
    pub const EMPTY: SchemaColumn = SchemaColumn {
        type_code: TypeCode::U8,
        size: 0,
        nullable: false,
        is_signed: 0,
    };

    pub const fn new(type_code: TypeCode, nullable: bool) -> Self {
        SchemaColumn {
            type_code,
            size: type_code.wire_stride() as u8,
            nullable,
            is_signed: type_code.is_signed_int() as u8,
        }
    }

    /// On-disk byte width of one cell of this column.
    #[inline(always)]
    pub const fn size(&self) -> u8 {
        self.size
    }

    /// True iff this column's type is [`TypeCode::is_signed_int`].
    #[inline(always)]
    pub(crate) const fn is_signed(&self) -> bool {
        self.is_signed != 0
    }

    /// This column's type as a ≤ 8-byte integer, or `None` for any other type —
    /// the one rule the shard writer packs FoR by and the reader accepts it by.
    #[inline]
    pub(crate) fn fixed_int(&self) -> Option<gnitz_wire::FixedInt> {
        gnitz_wire::FixedInt::from_type_code(self.type_code)
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct SchemaDescriptor {
    num_columns: u32,
    pk_count: u32,
    pk_indices: [u32; MAX_PK_COLUMNS],
    /// Total bytes per row of the PK region.
    pk_stride: u8,
    /// Payload slot → logical column index.
    payload_to_ci: [u8; MAX_COLUMNS],
    /// Which comparator orders this schema's payload.
    pub(crate) payload_cmp: payload_order::PayloadCmpKind,
    /// Whether any column is a German string.
    has_german_string: bool,
    pub columns: [SchemaColumn; MAX_COLUMNS],
}

// Every `Batch` embeds a `SchemaDescriptor` by value, so a field added here is
// paid for at every batch copy.
const _: () = assert!(std::mem::size_of::<SchemaDescriptor>() <= 356);

// No column is wider than 16 bytes, so a PK within `MAX_PK_COLUMNS` is within
// `MAX_PK_BYTES` — what every `[0u8; MAX_PK_BYTES]` scratch key relies on — and
// its stride fits the `u8` field.
const _: () = {
    let mut i = 0;
    while i < TypeCode::ALL.len() {
        assert!(TypeCode::ALL[i].wire_stride() <= 16);
        i += 1;
    }
    assert!(MAX_PK_COLUMNS * 16 <= MAX_PK_BYTES && MAX_PK_BYTES <= u8::MAX as usize);
};

impl SchemaDescriptor {
    /// The schema of `cols` keyed by `pk_indices`. `Err` for a list over the
    /// column limit and for a PK list that breaks the PK admission rules.
    pub fn try_new(cols: &[SchemaColumn], pk_indices: &[u32]) -> Result<Self, String> {
        if cols.len() > MAX_COLUMNS {
            return Err(format!(
                "column count {} exceeds MAX_COLUMNS ({MAX_COLUMNS})",
                cols.len()
            ));
        }
        gnitz_wire::validate_pk_tuple(pk_indices, cols.len(), MAX_PK_COLUMNS, |c| {
            let col = &cols[c as usize];
            (col.type_code, col.nullable)
        })
        .map_err(|rule| rule.for_role(gnitz_wire::PkListRole::PrimaryKey))?;
        let pk_stride: usize = pk_indices.iter().map(|&c| cols[c as usize].size() as usize).sum();
        let mut columns = [SchemaColumn::EMPTY; MAX_COLUMNS];
        columns[..cols.len()].copy_from_slice(cols);
        let mut pk = [0u32; MAX_PK_COLUMNS];
        pk[..pk_indices.len()].copy_from_slice(pk_indices);
        let mut payload_to_ci = [0u8; MAX_COLUMNS];
        for ci in 0..cols.len() {
            if let Some(pi) = gnitz_wire::payload_slot(pk_indices, ci) {
                payload_to_ci[pi] = ci as u8;
            }
        }
        let num_payload = cols.len() - pk_indices.len();
        let payload = payload_to_ci[..num_payload].iter().map(|&ci| cols[ci as usize]);
        Ok(SchemaDescriptor {
            num_columns: cols.len() as u32,
            pk_count: pk_indices.len() as u32,
            pk_indices: pk,
            pk_stride: pk_stride as u8,
            payload_to_ci,
            payload_cmp: payload_order::PayloadCmpKind::of(payload),
            has_german_string: cols.iter().any(|c| c.type_code.is_german_string()),
            columns,
        })
    }

    /// [`Self::try_new`] for a column list the caller has already admitted.
    #[track_caller]
    pub fn new(cols: &[SchemaColumn], pk_indices: &[u32]) -> Self {
        match Self::try_new(cols, pk_indices) {
            Ok(schema) => schema,
            Err(e) => panic!("SchemaDescriptor::new: {e}"),
        }
    }

    /// Number of logical columns in this schema (PK + payload).
    #[inline]
    pub const fn num_columns(&self) -> usize {
        self.num_columns as usize
    }

    /// True when `self` is `prev` with zero or more columns appended — every
    /// column `prev` had keeping its position, `type_code` and PK membership.
    /// What leaves a baked span-encode plan's offsets and payload slots valid.
    pub fn is_trailing_append_of(&self, prev: &SchemaDescriptor) -> bool {
        self.pk_cols() == prev.pk_cols()
            && self.num_columns() >= prev.num_columns()
            && (0..prev.num_columns()).all(|i| self.columns[i].type_code == prev.columns[i].type_code)
    }

    /// The PK columns in PK-list order, as `(col_idx, &SchemaColumn)`.
    #[inline]
    pub(crate) fn pk_columns(&self) -> impl Iterator<Item = (usize, &SchemaColumn)> {
        self.pk_cols()
            .iter()
            .map(move |&ci| (ci as usize, &self.columns[ci as usize]))
    }

    /// Total bytes per row of the PK region.
    #[inline]
    pub const fn pk_stride(&self) -> usize {
        self.pk_stride as usize
    }

    /// Render a PK from its raw OPK byte form, for error messages: the per-column
    /// native values in PK-list order, comma-separated.
    pub fn format_pk_bytes(&self, pk_bytes: &[u8]) -> String {
        let native = self.native_le_key(pk_bytes);
        let mut off = 0usize;
        self.pk_columns()
            .map(|(_, col)| {
                let le = &native[off..off + col.size() as usize];
                off += le.len();
                // Through the storage type: a calendar column renders as the signed
                // integer it is, not as the unsigned fallthrough.
                match col.type_code.storage_type() {
                    TypeCode::UUID => gnitz_wire::format_uuid(u128::from_le_bytes(le.try_into().unwrap())),
                    TypeCode::U128 => format!("{}", u128::from_le_bytes(le.try_into().unwrap())),
                    TypeCode::I128 => format!("{}", i128::from_le_bytes(le.try_into().unwrap())),
                    t if t.is_signed_int() => format!("{}", gnitz_wire::read_signed_exact(le)),
                    _ => format!("{}", gnitz_wire::read_unsigned_exact(le)),
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Number of non-PK ("payload") columns. `#[inline(always)]`: `Batch` derives
    /// its region count from this, so at `-O0` a plain `#[inline]` puts a real
    /// call on the per-row appenders that read it as a loop bound.
    #[inline(always)]
    pub const fn num_payload_cols(&self) -> usize {
        self.num_columns as usize - self.pk_count as usize
    }

    /// The non-PK ("payload") columns in schema order, as `(payload_idx,
    /// &SchemaColumn)`: `payload_idx` is the dense index of the column's batch
    /// region and null-bitmap bit.
    #[inline]
    pub fn payload_columns(&self) -> impl Iterator<Item = (usize, &SchemaColumn)> {
        (0..self.num_payload_cols()).map(move |pi| (pi, &self.columns[self.payload_to_ci[pi] as usize]))
    }

    /// Whether the schema carries a STRING/BLOB (German-string) column, and so
    /// whether a batch over it has a live blob region.
    #[inline]
    pub fn has_german_string(&self) -> bool {
        self.has_german_string
    }

    /// The column at `ci`, or `None` when it is out of range. The bounded read
    /// for a client-supplied index: `columns[ci]` and `columns.get(ci)` both
    /// answer for the `[num_columns, MAX_COLUMNS)` slots, which hold
    /// [`SchemaColumn::EMPTY`].
    #[inline]
    pub fn column(&self, ci: usize) -> Option<SchemaColumn> {
        (ci < self.num_columns()).then(|| self.columns[ci])
    }

    /// Bound every `(what, column)` of `cols` against this schema. Ahead of the
    /// derivations an operator's `from_wire` runs, each of which indexes the
    /// fixed column array raw; `what` names the list the index came from.
    pub(crate) fn check_cols<'a>(&self, cols: impl IntoIterator<Item = (&'a str, u32)>) -> Result<(), String> {
        for (what, c) in cols {
            if self.column(c as usize).is_none() {
                return Err(oob_col(what, c, self));
            }
        }
        Ok(())
    }

    /// True when `cols` holds every PK column. The span such a list encodes is
    /// a fixed-width OPK concatenation and widening is injective, so the span
    /// determines the row's PK: a unique index on `cols` cannot collide.
    pub fn covers_pk(&self, cols: &[u32]) -> bool {
        self.pk_cols().iter().all(|p| cols.contains(p))
    }

    /// This schema's PK columns alone, in PK-list order.
    pub fn pk_only(&self) -> SchemaDescriptor {
        let mut b = DerivedSchema::new();
        b.push_pk_of(self);
        b.finish().expect("a schema's own PK is admissible")
    }

    /// [`SchemaFacts::payload_col_idx`], from a table filled at construction.
    #[inline]
    pub fn payload_col_idx(&self, pi: usize) -> usize {
        debug_assert!(pi < self.num_payload_cols(), "payload_col_idx: pi out of range");
        self.payload_to_ci[pi] as usize
    }
}

impl ColumnTable for SchemaDescriptor {
    #[inline]
    fn pk_cols(&self) -> &[u32] {
        &self.pk_indices[..self.pk_count as usize]
    }

    #[inline]
    fn num_columns(&self) -> usize {
        self.num_columns as usize
    }

    #[inline]
    fn col_type_code(&self, ci: usize) -> TypeCode {
        self.columns[ci].type_code
    }

    #[inline]
    fn col_nullable(&self, ci: usize) -> bool {
        self.columns[ci].nullable
    }
}

impl std::fmt::Debug for SchemaDescriptor {
    // The fixed-size `columns` / `pk_indices` arrays make a derive useless (it
    // would dump all MAX_COLUMNS slots). Print only the live columns — their
    // type, a `?` for nullable, and `pk` for PK columns — plus the PK index list.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SchemaDescriptor {{ columns: [")?;
        for ci in 0..self.num_columns() {
            if ci > 0 {
                write!(f, ", ")?;
            }
            let col = self.columns[ci];
            write!(f, "{:?}", col.type_code)?;
            if col.nullable {
                write!(f, "?")?;
            }
            if self.is_pk_col(ci) {
                write!(f, " pk")?;
            }
        }
        write!(f, "], pk_indices: {:?} }}", self.pk_cols())
    }
}

impl PartialEq for SchemaDescriptor {
    fn eq(&self, other: &Self) -> bool {
        if self.num_columns() != other.num_columns() || self.pk_cols() != other.pk_cols() {
            return false;
        }
        self.columns[..self.num_columns()] == other.columns[..other.num_columns()]
    }
}

impl Eq for SchemaDescriptor {}

// ---------------------------------------------------------------------------
// Schema-shaping free functions
//
// Derived purely from a `SchemaDescriptor` (no catalog or storage state), and
// shared across layers — an output schema with one caller belongs at that
// caller, not here.
// ---------------------------------------------------------------------------

/// The [`KeySpec`] of a secondary index on `source_cols` of `source`, with the
/// index schema its entries land in: the promoted key columns in declared order,
/// then the source PK columns, all in the PK.
pub fn index_spec_and_schema(
    source_cols: &[u32],
    source: &SchemaDescriptor,
) -> Result<(KeySpec, SchemaDescriptor), String> {
    let spec = KeySpec::new(source_cols, source)?;
    Ok((spec, spec.output_schema(source)))
}

/// Rebuild a [`SchemaDescriptor`] from a meta-schema record. Column names are
/// carried on the wire but nothing engine-side reads one.
pub fn decode_schema_block(data: &[u8]) -> Result<SchemaDescriptor, String> {
    let mut cols = [SchemaColumn::EMPTY; MAX_COLUMNS];
    let mut n = 0;
    let pk = gnitz_wire::schema_block::decode(data, |c| {
        cols[n] = SchemaColumn::new(c.ty.tc, c.nullable);
        n += 1;
        Ok(())
    })?;
    SchemaDescriptor::try_new(&cols[..n], pk.as_slice())
}

/// Encode `schema`'s physical column shape — the inverse of
/// [`decode_schema_block`]: no names, no hidden flag, every scale zero.
pub fn encode_schema_block(schema: &SchemaDescriptor) -> Vec<u8> {
    let cols = schema.columns[..schema.num_columns()].iter().map(|c| SchemaBlockCol {
        ty: ColType::of(c.type_code),
        nullable: c.nullable,
        hidden: false,
        name: b"",
    });
    gnitz_wire::schema_block::encode(cols, schema.pk_cols())
}

/// `schema`'s PK columns, then the payload columns `project` names, in that
/// order. `Err` for an entry that names no payload column, and for a list
/// overflowing the column limit — `project` may repeat an index.
pub fn project_schema(schema: &SchemaDescriptor, project: &[u32]) -> Result<SchemaDescriptor, String> {
    let mut b = DerivedSchema::new();
    b.push_pk_of(schema);
    for &p in project {
        if schema.payload_slot(p as usize).is_none() {
            return Err(format!(
                "column {p} is not a payload column of a {}-column schema",
                schema.num_columns()
            ));
        }
        b.push(schema.columns[p as usize]);
    }
    b.finish()
}

/// `schema` keyed by `prefix` followed by its own key, over its payload space —
/// the layout `Batch::with_key_prefix` copies into. `None` when the key has no
/// column to spare.
pub fn key_prefixed_schema(prefix: SchemaColumn, schema: &SchemaDescriptor) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    b.push_pk(prefix);
    b.push_pk_of(schema);
    b.push_payload_of(schema);
    b.finish().ok()
}

#[cfg(test)]
#[path = "tests/schema.rs"]
mod tests;
