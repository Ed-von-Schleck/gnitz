//! SQL type constants, schema descriptor types, and the schema-shaping free
//! functions derived from them.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

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
pub use route::{ground_owner, worker_for_pk_bytes, Slot};

/// Which fixed bound a [`DerivedSchema`] push hit. Callers prefix it with what
/// they were building.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SchemaBound {
    Columns,
    /// A key column behind a payload one, which [`DerivedSchema::finish`]'s
    /// dense `0..pk_len` PK list cannot express.
    PkAfterPayload,
    PkColumns,
    /// A key column whose type has no order-preserving encoding.
    PkType(TypeCode),
    PkNullable,
}

impl std::fmt::Display for SchemaBound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaBound::Columns => write!(f, "exceeds MAX_COLUMNS ({MAX_COLUMNS})"),
            SchemaBound::PkAfterPayload => write!(f, "places a key column behind a payload column"),
            SchemaBound::PkColumns => write!(f, "key exceeds MAX_PK_COLUMNS ({MAX_PK_COLUMNS})"),
            SchemaBound::PkType(tc) => write!(f, "keys on column type {tc}, which is not PK-eligible"),
            SchemaBound::PkNullable => write!(f, "keys on a nullable column"),
        }
    }
}

/// Accumulator for a derived schema whose PK is its leading `pk_len` columns —
/// the shape of every schema the compiler and the reduce planner build
/// (join / map / reindex / hash-row / null-extend / reduce outputs). PK columns
/// always occupy the leading positions, so the PK index list is dense
/// (`0..pk_len`) and `finish` re-derives it rather than tracking it.
///
/// Every push is bounded and returns `None` on overflow, so the fixed-array
/// bound lives with the array instead of being re-derived — differently, and
/// PK-inclusively — at each caller. Node/column lists are client-supplied
/// catalog data, so an overflowing one must fail the compile, not abort.
pub(crate) struct DerivedSchema {
    cols: [SchemaColumn; MAX_COLUMNS],
    n: usize,
    pk_len: usize,
}

impl DerivedSchema {
    pub(crate) fn new() -> Self {
        DerivedSchema {
            cols: [SchemaColumn::EMPTY; MAX_COLUMNS],
            n: 0,
            pk_len: 0,
        }
    }

    /// Append one payload column.
    pub(crate) fn push(&mut self, col: SchemaColumn) -> Result<(), SchemaBound> {
        *self.cols.get_mut(self.n).ok_or(SchemaBound::Columns)? = col;
        self.n += 1;
        Ok(())
    }

    /// Append one PK column, which [`Self::finish`] numbers `0..pk_len` — so a
    /// PK column pushed after a payload one is rejected here rather than
    /// producing a descriptor whose PK list names payload columns.
    ///
    /// Rejects everything `SchemaDescriptor::new` *asserts* (see
    /// [`SchemaDescriptor::new_with_placement`]), so a caller passing an unvetted
    /// column type gets a [`SchemaBound`] rather than an abort inside `finish()`.
    pub(crate) fn push_pk(&mut self, col: SchemaColumn) -> Result<(), SchemaBound> {
        if self.pk_len != self.n {
            return Err(SchemaBound::PkAfterPayload);
        }
        if self.pk_len == MAX_PK_COLUMNS {
            return Err(SchemaBound::PkColumns);
        }
        if !col.type_code.is_pk_eligible() {
            return Err(SchemaBound::PkType(col.type_code));
        }
        if col.nullable {
            return Err(SchemaBound::PkNullable);
        }
        self.push(col)?;
        self.pk_len += 1;
        Ok(())
    }

    /// Append `schema`'s PK columns in PK-list order — the shared prologue of
    /// every builder that inherits its input's key.
    pub(crate) fn push_pk_of(&mut self, schema: &SchemaDescriptor) -> Result<(), SchemaBound> {
        for (_, c) in schema.pk_columns() {
            self.push_pk(*c)?;
        }
        Ok(())
    }

    /// Append `schema`'s payload columns in schema order.
    pub(crate) fn push_payload_of(&mut self, schema: &SchemaDescriptor) -> Result<(), SchemaBound> {
        schema.payload_columns().try_for_each(|(_, c)| self.push(*c))
    }

    pub(crate) fn finish(&self) -> SchemaDescriptor {
        let pk_idx: [u32; MAX_PK_COLUMNS] = std::array::from_fn(|i| i as u32);
        SchemaDescriptor::new(&self.cols[..self.n], &pk_idx[..self.pk_len])
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
    /// The unused-slot filler for the fixed `[SchemaColumn; MAX_COLUMNS]` arrays
    /// every schema and schema builder carries. Padding: `size == 0`; nothing
    /// reads past `num_columns()`.
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

    /// On-disk byte width of one cell of this column. Derived from `type_code`
    /// via `SchemaColumn::new` and never written independently.
    #[inline(always)]
    pub const fn size(&self) -> u8 {
        self.size
    }

    /// True iff this column's type is [`TypeCode::is_signed_int`]. Derived from
    /// `type_code` in `new()` (like `size`); read by the fixed-int fast-path
    /// comparator to pick the order-preserving sign-flip mask without a
    /// per-column type-code branch.
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

/// Where a relation's rows live — the one value every consumer reads, so the
/// store shape, the read routing, and the join/reduce co-partition analyzers
/// cannot answer differently. Stamped at registration on base tables (from
/// `TABLE_TAB.flags`), on system catalog families, and on views (folded from the
/// sources' own stamped placements, which is what makes the property transitive
/// up a view chain).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Placement {
    /// Every worker holds an identical full copy; writes broadcast, reads
    /// single-source worker 0.
    Replicated,
    /// Rows live on whichever worker produced them and are **not** keyed by
    /// `worker_for_pk` at all — a view over any source that is itself not
    /// `Keyed` (a replicated table, or another `Local` view). The union across
    /// workers is the relation; a read must gather every worker.
    Local,
    /// Hash-distributed: a row's owner is
    /// `worker_for_pk_bytes(OPK(pk_cols()[..prefix_len]))`.
    /// `prefix_len == pk_count` is the default (full-PK) distribution.
    Keyed { prefix_len: u8 },
}

impl Placement {
    /// The persisted "default distribution" sentinel: `prefix_len == 0` means
    /// the full PK. Constructors normalize it against the schema's PK arity, so a
    /// `Keyed` prefix read back off a descriptor is never the sentinel — it is
    /// `pk_count` for the default and `1..pk_count` for a `CLUSTER BY` prefix.
    /// An out-of-range prefix is not normalized here: `TableProps::validate`
    /// rejects it at the decode boundary, where the corrupt row can be named.
    pub const KEYED_DEFAULT: Placement = Placement::Keyed { prefix_len: 0 };

    /// The worker whose copy of a replicated relation is the counted one: it
    /// alone captures a replicated view's delta feed, and it answers reads.
    pub const REPLICA_OWNER: u32 = 0;

    /// True iff worker `rank`'s copy is a counted one: every worker's for a
    /// partitioned relation, [`Self::REPLICA_OWNER`]'s alone for a replicated one.
    #[inline]
    pub const fn counts_on(self, rank: u32) -> bool {
        !self.is_replicated() || rank == Self::REPLICA_OWNER
    }

    /// True iff a row's owning worker is derived from its key. A relation that
    /// is not key-routed still holds one store per worker; what differs is which
    /// rows arrive there — a broadcast copy (`Replicated`) or whatever that
    /// worker produced (`Local`), rather than the key's own hash slice.
    #[inline]
    pub const fn is_key_routed(self) -> bool {
        matches!(self, Placement::Keyed { .. })
    }

    /// True iff a full identical copy lives on every worker.
    #[inline]
    pub const fn is_replicated(self) -> bool {
        matches!(self, Placement::Replicated)
    }

    /// The normalized placement and the number of leading PK columns the router
    /// hashes. One function, so the width can only be read off a normalized
    /// placement — over the raw `Keyed { 0 }` sentinel it would be `0`, a
    /// zero-length routing key every row hashes the same. A relation that is not
    /// key-routed is sliced by nothing and takes the full-PK width.
    const fn resolve(self, pk_count: usize) -> (Placement, usize) {
        match self {
            Placement::Keyed { prefix_len } => {
                let k = if prefix_len == 0 { pk_count } else { prefix_len as usize };
                (Placement::Keyed { prefix_len: k as u8 }, k)
            }
            other => (other, pk_count),
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct SchemaDescriptor {
    num_columns: u32,
    pk_count: u32,
    pk_indices: [u32; MAX_PK_COLUMNS],
    /// Total bytes per row of the PK region — sum of
    /// `columns[pk_indices[k]].size()` for k in 0..pk_count. Precomputed once in
    /// `new()` so per-row hot loops never re-run the sum. `u8` matches
    /// `Batch::pk_stride()`; the const assert below proves the width holds.
    pk_stride: u8,
    /// Byte width of the **distribution prefix** — the OPK bytes of the leading
    /// PK columns the placement keys by, the slice every write-side table-key
    /// router (`worker_for_pk_bytes`) hashes to pick a partition. Derived from
    /// `placement` by walking the PK columns in PK-list order, so it matches the
    /// OPK encoder's tight big-endian layout exactly. `dist_stride == pk_stride`
    /// for the default (full-PK) distribution and for a non-`Keyed` placement
    /// (whose rows no router slices at all), making every sliced route
    /// byte-identical to full-PK routing.
    dist_stride: u8,
    /// Where this relation's rows live — see [`Placement`]. Stamped by the
    /// caller; every derived/intermediate schema (join/map/reduce/projection
    /// output, built via `new`) gets the full-PK `Keyed` default.
    placement: Placement,
    /// Dense payload slot → logical column index, so `payload_columns()` walks
    /// `0..num_payload` with one byte load per element and no predicate. Slots
    /// past `num_payload_cols()` are zero fill nothing reads.
    payload_to_ci: [u8; MAX_COLUMNS],
    /// Pre-computed payload comparator strategy. Derived from column types in
    /// `new()`; read by every merge/sort/join dispatch (via `with_payload_cmp!`).
    pub(crate) payload_cmp: payload_order::PayloadCmpKind,
    /// Whether any payload column is a German string. Cached for the same reason
    /// `payload_cmp` is: the blob-cache and append paths ask per batch, and the
    /// answer is a walk of the payload columns. Free in the struct's tail padding
    /// — a per-slot mask would not be, and would break the size pin.
    has_german_string: bool,
    pub columns: [SchemaColumn; MAX_COLUMNS],
}

// `SchemaDescriptor` is `Copy` and embedded by value all over the engine
// (`Batch` among them, which the VM replaces several times per instruction), so
// a field added here is paid for at every copy — a cost the three fixed-capacity
// arrays make invisible at the definition. The bound is a boundary, not slack.
const _: () = assert!(std::mem::size_of::<SchemaDescriptor>() <= 360);

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

const fn compute_payload_to_ci(num_columns: usize, pk_indices: &[u32]) -> [u8; MAX_COLUMNS] {
    let mut payload_to_ci = [0u8; MAX_COLUMNS];
    let mut ci = 0;
    while ci < num_columns {
        if let Some(pi) = gnitz_wire::payload_slot(pk_indices, ci) {
            payload_to_ci[pi] = ci as u8;
        }
        ci += 1;
    }
    payload_to_ci
}

impl SchemaDescriptor {
    /// [`Self::new`] as an `Err` rather than an abort.
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
        .map_err(|rule| rule.to_string())?;
        Ok(Self::new(cols, pk_indices))
    }

    /// Construct a SchemaDescriptor from a column list and PK index list.
    /// Accepts up to `MAX_PK_COLUMNS` entries.
    #[track_caller]
    pub const fn new(cols: &[SchemaColumn], pk_indices: &[u32]) -> Self {
        // Default placement = hash-distributed by the full PK.
        Self::new_with_placement(cols, pk_indices, Placement::KEYED_DEFAULT)
    }

    /// Construct a `SchemaDescriptor` with a stamped [`Placement`] — for a base
    /// table, the one decoded from `TABLE_TAB.flags` (see
    /// `gnitz_wire::TableProps::pack`); for a view, the one folded from its
    /// sources.
    ///
    /// **A `const fn` whose `assert!`s fire in release and abort the process.**
    /// An untrusted column list goes through [`Self::try_new`], which rejects
    /// what this would abort on.
    /// A `Keyed { prefix_len: 0 }` (the persisted "default" sentinel) is
    /// normalized to the full PK, so `dist_stride == pk_stride` and routing is
    /// byte-identical to the full-PK default. Every derived schema
    /// (join/map/reduce/projection output, built via `new`) gets that default and
    /// is never table-key-routed.
    #[track_caller]
    pub const fn new_with_placement(cols: &[SchemaColumn], pk_indices: &[u32], placement: Placement) -> Self {
        assert!(cols.len() <= MAX_COLUMNS, "new: too many columns");
        assert!(
            !pk_indices.is_empty() && pk_indices.len() <= MAX_PK_COLUMNS,
            "new: pk_indices.len() is outside 1..=MAX_PK_COLUMNS",
        );

        let (placement, dist_k) = placement.resolve(pk_indices.len());

        let mut columns = [SchemaColumn::EMPTY; MAX_COLUMNS];
        let mut i = 0;
        while i < cols.len() {
            columns[i] = cols[i];
            i += 1;
        }

        let mut pk_arr = [0u32; MAX_PK_COLUMNS];
        let mut stride_acc: u16 = 0;
        let mut dist_stride_acc: u16 = 0;
        let mut k = 0;
        while k < pk_indices.len() {
            assert!((pk_indices[k] as usize) < cols.len(), "new: pk index out of range",);
            // Duplicate-PK guard: a duplicate would desynchronise
            // num_payload_cols (counts the dup twice), pk_columns
            // (yields duplicates), and pk_stride (double-counts).
            // O(pk_count²) over pk_count ≤ MAX_PK_COLUMNS — negligible.
            let mut j = 0;
            while j < k {
                assert!(pk_indices[j] != pk_indices[k], "new: duplicate PK column index",);
                j += 1;
            }
            // One allow-list, shared with the catalog DDL and wire layers, so a
            // newly added type code is PK-ineligible until explicitly vetted
            // rather than silently admitted by a deny-list that forgot it.
            // STRING/BLOB carry a heap offset the bulk-copied PK region cannot
            // relocate; IEEE-754 floats break the byte-equal key contract that
            // `compare_pk_bytes` and the order-preserving encoder rest on.
            assert!(
                (cols[pk_indices[k] as usize].type_code).is_pk_eligible(),
                "new: only integer scalar columns can be PK columns \
                 (the PK region is compared and bulk-copied as raw bytes)",
            );
            // `compare_pk_bytes` reads PK bytes with no null-bit handling; a
            // nullable PK would silently corrupt the merge comparison.
            assert!(
                !cols[pk_indices[k] as usize].nullable,
                "new: PK columns must be non-nullable",
            );
            pk_arr[k] = pk_indices[k];
            let col_size = cols[pk_indices[k] as usize].size() as u16;
            stride_acc += col_size;
            // PK-list order with no inter-column padding ⇒ the running sum of the
            // first `dist_k` PK column widths is exactly the OPK byte width of the
            // distribution prefix (`gnitz_wire::encode_pk_tuple` layout).
            if k < dist_k {
                dist_stride_acc += col_size;
            }
            k += 1;
        }
        let pk_stride = stride_acc as u8;
        let payload_to_ci = compute_payload_to_ci(cols.len(), pk_indices);
        let payload_cmp = payload_order::compute_payload_cmp(cols, &payload_to_ci, cols.len() - pk_indices.len());
        let has_german_string = {
            let mut i = 0;
            let mut found = false;
            while i < cols.len() {
                // No PK column can be one: `is_pk_eligible`, asserted above on
                // every PK column, admits only integer scalars.
                if cols[i].type_code.is_german_string() {
                    found = true;
                }
                i += 1;
            }
            found
        };
        SchemaDescriptor {
            num_columns: cols.len() as u32,
            pk_count: pk_indices.len() as u32,
            pk_indices: pk_arr,
            pk_stride,
            dist_stride: dist_stride_acc as u8,
            placement,
            payload_to_ci,
            payload_cmp,
            has_german_string,
            columns,
        }
    }

    /// Rebuild this schema with a different [`Placement`]. Every site that knows
    /// its placement up front passes it to `new_with_placement` /
    /// `build_schema_from_col_defs` instead; this re-stamps a descriptor that is
    /// already built — `make_delta_schema`, and tests re-stamping a shared
    /// fixture. It delegates, so there is one derivation of the route, not two.
    pub const fn with_placement(&self, placement: Placement) -> Self {
        let (cols, _) = self.columns.split_at(self.num_columns as usize);
        let (pk, _) = self.pk_indices.split_at(self.pk_count as usize);
        Self::new_with_placement(cols, pk, placement)
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

    /// Iterate over PK columns in pk-list order, yielding `(col_idx,
    /// &SchemaColumn)`. Mirror of `payload_columns()`. The pk-list position is
    /// the iteration index, so callers that need it use `.enumerate()`.
    #[inline]
    pub(crate) fn pk_columns(&self) -> impl Iterator<Item = (usize, &SchemaColumn)> {
        self.pk_cols()
            .iter()
            .map(move |&ci| (ci as usize, &self.columns[ci as usize]))
    }

    /// Total bytes per row of the PK region. Precomputed in `new()`;
    /// O(1) field load. The field stays a `u8` to match `SchemaColumn::size()`
    /// and the storage-layer `pk_stride` caches; almost every reader wants a
    /// buffer offset, so the widening happens here rather than at each use site.
    #[inline]
    pub const fn pk_stride(&self) -> usize {
        self.pk_stride as usize
    }

    /// Byte width of the distribution prefix (the leading PK slice that
    /// `worker_for_pk` hashes); `pk_stride()` for the full-PK default. Prefer
    /// `worker_for_pk` over reading this and slicing by hand.
    #[inline]
    pub const fn dist_stride(&self) -> usize {
        self.dist_stride as usize
    }

    /// The single **table-key router**: maps a row's full OPK PK bytes to its
    /// owning worker by hashing only the leading distribution prefix
    /// (`key[..dist_stride()]`), so the slicing rule lives in one place. `key` is
    /// the full PK, and for the full-PK default this is byte-identical to hashing
    /// all of it. An exchange scatter resolves its own key in `ScatterPlan`.
    #[inline]
    pub fn worker_for_pk(&self, key: &[u8], num_workers: usize) -> usize {
        worker_for_pk_bytes(&key[..self.dist_stride()], num_workers)
    }

    /// Where this relation's rows live — the one value every placement decision
    /// reads, so no two can disagree. `SchemaDescriptor::eq` ignores it, so a
    /// rebuilt descriptor must be constructed with it again.
    #[inline]
    pub const fn placement(&self) -> Placement {
        self.placement
    }

    /// Render a PK from its raw OPK byte form, for error messages: the per-column
    /// native values in PK-list order, comma-separated. Here rather than in a
    /// caller because it is the inverse of the OPK encoding this module owns, and
    /// it works for a PK wider than a `u128`.
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

    /// Iterate over the non-PK ("payload") columns, yielding `(payload_idx,
    /// &SchemaColumn)` where `payload_idx` is the dense 0-based index used for
    /// batch payload regions and null-bitmap bits. Callers needing the logical
    /// column index resolve it with [`Self::payload_col_idx`].
    ///
    /// Walks a contiguous `0..num_payload` range with one byte load per
    /// element via `payload_to_ci` — no per-row predicate.
    #[inline]
    pub fn payload_columns(&self) -> impl Iterator<Item = (usize, &SchemaColumn)> {
        (0..self.num_payload_cols()).map(move |pi| (pi, &self.columns[self.payload_to_ci[pi] as usize]))
    }

    /// Whether the schema carries a STRING/BLOB (German-string) column, and so
    /// whether a batch over it has a live blob region. Cached by `new()`, which
    /// scans every column without filtering — such a column is always payload,
    /// since `is_pk_eligible` excludes them.
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
        // Compare all four bytes of each active column; `size` and `is_signed` are
        // derived from `type_code` in `new()`, so they never disagree when type_code matches.
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

/// Build a compound-PK index schema for a secondary index on `source_cols`
/// of `source`, validating the column list along the way.
///
/// Layout: `(promoted_c0, promoted_c1, …, src_pk_0, src_pk_1, …)` — every
/// indexed column promoted independently and packed in declared order, then the
/// source PK columns, all in the PK with zero payload columns. The leading
/// indexed-key region is `Σ promoted widths`; an index range read prefix-scans it
/// (full or leading-prefix), then reads the source PK bytes directly out of the
/// index PK suffix. The 1-element list is the single-column index.
///
/// The schema is the [`KeySpec`]'s own, so a caller needing both builds the
/// spec once and takes the schema off it rather than calling here.
///
/// Total: every rejection is an `Err`, never the constructor's abort. An
/// over-limit schema is reachable only for a *composite* index (a single-column
/// index — including every FK auto-index — always fits, since
/// `PK_LIST_MAX_COLS < MAX_PK_COLUMNS` reserves the prefix slot), via a raw
/// `gnitz-core` client or a crafted persisted row replayed at boot, neither of
/// which goes through the SQL planner's pre-check.
pub fn make_index_schema(source_cols: &[u32], source: &SchemaDescriptor) -> Result<SchemaDescriptor, String> {
    index_spec_and_schema(source_cols, source).map(|(_, schema)| schema)
}

/// [`make_index_schema`] keeping the [`KeySpec`] it derives the schema from.
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

/// `schema`'s PK columns (in pk-list order, so the packed PK round-trips
/// identically) followed by the columns `project` names, in that order, as
/// payload. The one projection builder — the compiler's projection MAP, the LSM
/// skeleton shard, the catalog's FK gather and the master's matching expected
/// reply schema all derive their layout here.
///
/// An entry naming no payload column — a PK index, or one out of range — is
/// **skipped**: the PK region already carries it, and a projection MAP's
/// `copy_cols` numbers destinations densely over the payload it does write.
/// `project` may repeat an index, so its length bounds neither the payload count
/// nor the total; `None` on overflow, which only a client-supplied circuit
/// column list can reach.
pub fn project_schema(schema: &SchemaDescriptor, project: &[u32]) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    b.push_pk_of(schema).ok()?;
    for &p in project {
        let i = p as usize;
        if schema.payload_slot(i).is_some() {
            b.push(schema.columns[i]).ok()?;
        }
    }
    Some(b.finish())
}

/// `schema` keyed by `prefix` followed by its own key, over its payload space —
/// the layout `Batch::with_key_prefix` copies into. `None` when the key has no
/// column to spare.
pub fn key_prefixed_schema(prefix: SchemaColumn, schema: &SchemaDescriptor) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    b.push_pk(prefix).ok()?;
    b.push_pk_of(schema).ok()?;
    b.push_payload_of(schema).ok()?;
    Some(b.finish())
}

#[cfg(test)]
#[path = "tests/schema.rs"]
mod tests;
