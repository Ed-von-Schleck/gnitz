//! SQL type constants, schema descriptor types, the schema-shaping free
//! functions derived from them, and [`OpBuildErr`] — the one vocabulary every
//! store-side operator constructor rejects untrusted parameters through.
//!
//! These are shared across the storage, IPC, and query layers.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::borrow::Cow;

use gnitz_wire::{is_fixed_int, is_signed_int};

/// Why a store-side operator constructor refused the parameters it was handed —
/// one vocabulary for all of them, so the constructor rather than each caller
/// owns the wording of its own trust boundary.
#[derive(Debug)]
pub enum OpBuildErr {
    /// The parameters do not fit the schema: an out-of-range column, an arity or
    /// byte-budget overrun, a column type the operator has no image for. `Cow`
    /// because most of these interpolate the offending value.
    Shape(Cow<'static, str>),
    /// An expression program was refused. The phrase names the guard and the
    /// payload carries the validator's own reason, so the rejection says which
    /// limit the program exceeded and not only which guard fired.
    Program(&'static str, gnitz_expr::ExprValidateErr),
}

impl OpBuildErr {
    /// A [`OpBuildErr::Shape`] from either a literal or an interpolated reason.
    pub(crate) fn shape(why: impl Into<Cow<'static, str>>) -> Self {
        OpBuildErr::Shape(why.into())
    }

    /// A client-supplied column index out of range for the schema it indexes;
    /// `what` names the list it came from. The bound is always `num_columns()`,
    /// never `MAX_COLUMNS`: the slots between read back as an undecodable
    /// [`SchemaColumn::EMPTY`].
    pub fn oob_col(what: &str, c: u32, schema: &SchemaDescriptor) -> Self {
        OpBuildErr::shape(format!("{what} {c} out of range ({} cols)", schema.num_columns()))
    }
}

impl std::fmt::Display for OpBuildErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpBuildErr::Shape(why) => f.write_str(why),
            OpBuildErr::Program(guard, e) => write!(f, "{guard}: {e}"),
        }
    }
}

/// The server's error plumbing is string-typed; this keeps `?` working where a
/// compile hands an operator constructor's rejection on.
impl From<OpBuildErr> for String {
    fn from(e: OpBuildErr) -> String {
        e.to_string()
    }
}

pub(crate) use gnitz_wire::type_code;
pub(crate) use gnitz_wire::ReduceOutKey;
pub(crate) use gnitz_wire::TypeCode;
pub(crate) use gnitz_wire::MAX_COLUMNS;
pub(crate) use gnitz_wire::{MAX_PK_BYTES, MAX_PK_COLUMNS};

/// Resolved column addressing, homed in the leaf `gnitz-expr` crate so the
/// expression evaluator (and, through it, the SQL client) shares one definition
/// with the engine. Re-exported here because it *is* a schema fact —
/// `SchemaDescriptor::locate` produces one.
pub(crate) use gnitz_expr::ColumnLocator;

/// Order-preserving primary-key (OPK) primitives — every native→OPK encoder
/// (whole PK, seek wire pair, index leading span), compare/pack, and the
/// re-export of the width-tagged `PkBuf` those encoders return. Sits below both
/// schema and storage, and is the **one** import path: a second re-export would
/// leave the byte-order rule spelled two ways in adjacent lines of one call site.
pub mod key;

/// The precomputed per-row read/encode plan for an index's OPK leading-key span.
/// Lives in [`key`] with the rest of the native→OPK encoders it shares its byte
/// contract with; re-exported here because a spec is derived from a pair of
/// schemas, so call sites keep naming `crate::schema::IndexKeySpec`.
pub use key::IndexKeySpec;

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
    pk_bytes: usize,
}

impl Default for DerivedSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl DerivedSchema {
    pub(crate) fn new() -> Self {
        DerivedSchema {
            cols: [SchemaColumn::EMPTY; MAX_COLUMNS],
            n: 0,
            pk_len: 0,
            pk_bytes: 0,
        }
    }

    /// Append one payload column.
    pub(crate) fn push(&mut self, col: SchemaColumn) -> Option<()> {
        *self.cols.get_mut(self.n)? = col;
        self.n += 1;
        Some(())
    }

    /// Append one PK column, which [`Self::finish`] numbers `0..pk_len` — so a
    /// PK column pushed after a payload one is rejected here rather than
    /// producing a descriptor whose PK list names payload columns.
    ///
    /// Rejects everything `SchemaDescriptor::new` *asserts* (see
    /// [`SchemaDescriptor::new_with_placement`]), so a caller passing an unvetted
    /// column type gets a `None` rather than an abort inside `finish()`.
    pub(crate) fn push_pk(&mut self, col: SchemaColumn) -> Option<()> {
        if self.pk_len != self.n
            || self.pk_len == MAX_PK_COLUMNS
            || self.pk_bytes + col.size() as usize > MAX_PK_BYTES
            || !gnitz_wire::is_pk_eligible(col.type_code)
            || col.nullable != 0
        {
            return None;
        }
        self.push(col)?;
        self.pk_len += 1;
        self.pk_bytes += col.size() as usize;
        Some(())
    }

    /// Append `schema`'s PK columns in PK-list order — the shared prologue of
    /// every builder that inherits its input's key.
    pub(crate) fn push_pk_of(&mut self, schema: &SchemaDescriptor) -> Option<()> {
        for (_, c) in schema.pk_columns() {
            self.push_pk(*c)?;
        }
        Some(())
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
    pub type_code: u8,
    size: u8,
    pub nullable: u8,
    is_signed: u8,
}

impl SchemaColumn {
    /// The unused-slot filler for the fixed `[SchemaColumn; MAX_COLUMNS]` arrays
    /// every schema and schema builder carries. Spelled out rather than built
    /// through [`Self::new`], whose assert rejects the undecodable type code `0`
    /// that marks a slot as padding.
    pub const EMPTY: SchemaColumn = SchemaColumn {
        type_code: 0,
        size: 0,
        nullable: 0,
        is_signed: 0,
    };

    /// A real column of type `type_code`, which must decode (see
    /// [`gnitz_wire::is_valid_type_code`] for why an unknown one is not inert).
    /// Client-supplied codes are screened at their decode boundary; the assert is
    /// the tripwire for a path that forgets to. Debug-only — the release engine
    /// must still *survive* a corrupt code, which is what the expression
    /// validator's `check_col` and the catalog's `check_col_defs` are for, and
    /// what `wire_stride`'s 8-byte fallback gives such a code here.
    pub const fn new(type_code: u8, nullable: u8) -> Self {
        debug_assert!(gnitz_wire::is_valid_type_code(type_code), "invalid column type code");
        // A flag, not a count: the two parameters are bare integers, so a caller
        // passing a column ordinal here otherwise builds a silently nullable column.
        debug_assert!(nullable <= 1, "column nullable flag must be 0 or 1");
        SchemaColumn {
            type_code,
            size: gnitz_wire::wire_stride(type_code) as u8,
            nullable,
            is_signed: is_signed_int(type_code) as u8,
        }
    }

    /// On-disk byte width of one cell of this column. Derived from `type_code`
    /// via `SchemaColumn::new` and never written independently.
    #[inline(always)]
    pub const fn size(&self) -> u8 {
        self.size
    }

    /// True iff this column is a signed integer (I8/I16/I32/I64). Derived from
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
        gnitz_wire::FixedInt::from_type_code(gnitz_wire::TypeCode::from_validated_u8(self.type_code))
    }
}

/// Pre-computed payload row-comparator strategy for a schema. Stored on
/// `SchemaDescriptor` and computed once in `new()` so every merge/sort/join
/// dispatch reads a single field instead of iterating over columns.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PayloadCmpKind {
    /// All payload columns are non-nullable fixed-width ints ≤ 8 bytes (any sign).
    /// Vacuously true for zero-payload (all-PK) schemas.
    FixedIntNonnull,
    /// Any schema with a disqualifying payload column: nullable, float, string,
    /// blob, or U128/UUID. (Mixed signed/unsigned fixed ints stay in the fast
    /// path — only a non-fixed-int column falls back here.)
    Generic,
}

/// Walks payload columns only. PK columns must not be examined: U128/UUID are
/// PK-eligible but not `is_fixed_int`, so a U128 PK would wrongly force `Generic`.
const fn compute_payload_cmp(
    cols: &[SchemaColumn],
    payload_to_ci: &[u8; MAX_COLUMNS],
    num_payload: usize,
) -> PayloadCmpKind {
    let mut pi = 0;
    while pi < num_payload {
        let col = cols[payload_to_ci[pi] as usize];
        if !(col.nullable == 0 && is_fixed_int(col.type_code)) {
            return PayloadCmpKind::Generic;
        }
        pi += 1;
    }
    PayloadCmpKind::FixedIntNonnull
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
    /// `worker_for_pk_bytes(OPK(pk_indices()[..prefix_len]))`.
    /// `prefix_len == pk_count` is the default (full-PK) distribution.
    Keyed { prefix_len: u8 },
}

impl Placement {
    /// The persisted "default distribution" sentinel: `prefix_len == 0` means
    /// the full PK. Constructors normalize it against the schema's PK arity, so a
    /// `Keyed` prefix read back off a descriptor is never the sentinel — it is
    /// `pk_count` for the default and `1..pk_count` for a `CLUSTER BY` prefix.
    /// An out-of-range prefix is not normalized here: `TableProps::validate_against_pk`
    /// rejects it at the decode boundary, where the corrupt row can be named.
    pub const KEYED_DEFAULT: Placement = Placement::Keyed { prefix_len: 0 };

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
    /// `Batch::pk_stride()`; `new` asserts the width holds.
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
    /// `new()`; read by every merge/sort/join dispatch (via `with_payload_cmp!`)
    /// in place of calling `schema_is_fixedint_nonnull` at each site.
    pub payload_cmp: PayloadCmpKind,
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

const fn compute_payload_to_ci(num_columns: usize, pk_indices: &[u32]) -> [u8; MAX_COLUMNS] {
    let mut payload_to_ci = [0u8; MAX_COLUMNS];
    let mut pi: u8 = 0;
    let mut ci: usize = 0;
    while ci < num_columns {
        let mut is_pk = false;
        let mut k = 0;
        while k < pk_indices.len() {
            if pk_indices[k] as usize == ci {
                is_pk = true;
            }
            k += 1;
        }
        if !is_pk {
            payload_to_ci[pi as usize] = ci as u8;
            pi += 1;
        }
        ci += 1;
    }
    payload_to_ci
}

impl SchemaDescriptor {
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
    /// Untrusted column lists never reach it directly: `DerivedSchema::push_pk`,
    /// `build_schema_from_col_defs`, `decode_schema_block`,
    /// `union_nullability_merge` and `unique_preflight_wire_schema` are the total
    /// front doors that reject what this would abort on. A `Keyed` prefix is **normalized**: `0` (the persisted "default"
    /// sentinel) and any value past `|PK|` (only reachable from a corrupted
    /// catalog flag) both clamp to the full PK, so `dist_stride == pk_stride` and
    /// routing stays byte-identical to the full-PK default. Every derived schema
    /// (join/map/reduce/projection output, built via `new`) gets that default and
    /// is never table-key-routed.
    #[track_caller]
    pub const fn new_with_placement(cols: &[SchemaColumn], pk_indices: &[u32], placement: Placement) -> Self {
        assert!(cols.len() <= MAX_COLUMNS, "new: too many columns");
        assert!(
            pk_indices.len() <= MAX_PK_COLUMNS,
            "new: pk_indices.len() exceeds MAX_PK_COLUMNS",
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
                gnitz_wire::is_pk_eligible(cols[pk_indices[k] as usize].type_code),
                "new: only integer scalar columns can be PK columns \
                 (the PK region is compared and bulk-copied as raw bytes)",
            );
            // `compare_pk_bytes` reads PK bytes with no null-bit handling; a
            // nullable PK would silently corrupt the merge comparison.
            assert!(
                cols[pk_indices[k] as usize].nullable == 0,
                "new: PK columns must be non-nullable",
            );
            pk_arr[k] = pk_indices[k];
            let col_size = cols[pk_indices[k] as usize].size() as u16;
            stride_acc += col_size;
            // PK-list order with no inter-column padding ⇒ the running sum of the
            // first `dist_k` PK column widths is exactly the OPK byte width of the
            // distribution prefix (`key::encode_order_preserving_pk` layout).
            if k < dist_k {
                dist_stride_acc += col_size;
            }
            k += 1;
        }
        assert!(stride_acc <= u8::MAX as u16, "new: pk_stride exceeds u8 width",);
        // `seek_opk_bytes` and other wide-path routines allocate
        // `[0u8; MAX_PK_BYTES]` and index up to `stride`; a stride in
        // (MAX_PK_BYTES, 255] would construct here but panic at runtime.
        assert!(
            stride_acc as usize <= MAX_PK_BYTES,
            "new: pk_stride exceeds MAX_PK_BYTES",
        );
        let pk_stride = stride_acc as u8;
        let payload_to_ci = compute_payload_to_ci(cols.len(), pk_indices);
        let payload_cmp = compute_payload_cmp(cols, &payload_to_ci, cols.len() - pk_indices.len());
        let has_german_string = {
            let mut i = 0;
            let mut found = false;
            while i < cols.len() {
                // No PK column can be one: `is_pk_eligible`, asserted above on
                // every PK column, admits only integer scalars.
                if gnitz_wire::is_german_string(cols[i].type_code) {
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

    /// All PK column indices, in compound-key order. Length is the PK arity:
    /// 1 for a single-column PK, or the full sequence for a compound table PK
    /// (and the co-partition analyzers compare against this whole sequence).
    #[inline]
    pub fn pk_indices(&self) -> &[u32] {
        &self.pk_indices[..self.pk_count as usize]
    }

    /// True when `self` is `prev` with zero or more columns appended — every
    /// column `prev` had keeping its position, `type_code` and PK membership.
    /// What leaves a baked span-encode plan's offsets and payload slots valid.
    pub(crate) fn is_trailing_append_of(&self, prev: &SchemaDescriptor) -> bool {
        self.pk_indices() == prev.pk_indices()
            && self.num_columns() >= prev.num_columns()
            && (0..prev.num_columns()).all(|i| self.columns[i].type_code == prev.columns[i].type_code)
    }

    /// Same column count, PK indices, and per-column `type_code`.
    ///
    /// Deliberately ignores per-column `nullable` (and `size`/`is_signed`, which
    /// are derived from `type_code`) — this is **weaker** than the `PartialEq`
    /// impl below, which compares the full column bytes including `nullable`.
    /// Used for identity-MAP elision and sink type-safety, both of which must
    /// treat a nullability-only difference as "same layout". Do not "simplify"
    /// to `self == other`: that would compare `nullable` and change elision
    /// semantics.
    pub fn same_physical_layout(&self, other: &SchemaDescriptor) -> bool {
        if self.num_columns() != other.num_columns() || self.pk_indices() != other.pk_indices() {
            return false;
        }
        for i in 0..self.num_columns() {
            if self.columns[i].type_code != other.columns[i].type_code {
                return false;
            }
        }
        true
    }

    /// Iterate over PK columns in pk-list order, yielding `(col_idx,
    /// &SchemaColumn)`. Mirror of `payload_columns()`. The pk-list position is
    /// the iteration index, so callers that need it use `.enumerate()`.
    #[inline]
    pub(crate) fn pk_columns(&self) -> impl Iterator<Item = (usize, &SchemaColumn)> {
        self.pk_indices()
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
    /// all of it.
    ///
    /// Not for **join-key** routing: the exchange relay scatters route an already
    /// reindexed `_join_pk` over a derived schema and call `worker_for_pk_bytes`
    /// directly (their key is the whole region, never a table prefix).
    #[inline]
    pub fn worker_for_pk(&self, key: &[u8], num_workers: usize) -> usize {
        gnitz_wire::worker_for_pk_bytes(&key[..self.dist_stride()], num_workers)
    }

    /// Where this relation's rows live — the one value every placement decision
    /// reads, so no two can disagree. Crate-visible so the
    /// ALTER … DROP NOT NULL descriptor rebuild (`hook_column_alter`) can carry it
    /// across the swap: `SchemaDescriptor::eq` ignores it, so a rebuilt
    /// descriptor must be constructed with it again.
    #[inline]
    pub const fn placement(&self) -> Placement {
        self.placement
    }

    /// Render a PK from its raw OPK byte form, for error messages: the per-column
    /// native values in PK-list order, comma-separated. Here rather than in a
    /// caller because it is the inverse of the OPK encoding this module owns, and
    /// it works for a PK wider than a `u128`.
    pub fn format_pk_bytes(&self, pk_bytes: &[u8]) -> String {
        let mut parts: Vec<String> = Vec::new();
        let mut off = 0usize;
        for (_, col) in self.pk_columns() {
            let size = col.size() as usize;
            let mut le = [0u8; 16];
            gnitz_wire::decode_pk_column(&pk_bytes[off..off + size], col.type_code, &mut le[..size]);
            let le = &le[..size];
            // Through the storage type: a calendar column renders as the signed
            // integer it is, not as the unsigned fallthrough.
            parts.push(match gnitz_wire::storage_type_code(col.type_code) {
                type_code::UUID => gnitz_wire::format_uuid(u128::from_le_bytes(le.try_into().unwrap())),
                type_code::U128 => format!("{}", u128::from_le_bytes(le.try_into().unwrap())),
                type_code::I128 => format!("{}", i128::from_le_bytes(le.try_into().unwrap())),
                t if gnitz_wire::is_signed_int(t) => format!("{}", gnitz_wire::read_signed_exact(le)),
                _ => format!("{}", gnitz_wire::read_unsigned_exact(le)),
            });
            off += size;
        }
        parts.join(", ")
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

    /// Dense payload slot (batch payload region + null-bitmap bit position) for
    /// `col_idx`, or `None` when it names no payload column of this schema —
    /// a PK column, or out of range. Total, so it doubles as the "is this a real
    /// payload column" test. Derived from [`Self::locate`], so "count the PK
    /// columns below `col_idx`" has one implementation.
    #[inline]
    pub(crate) fn try_payload_idx(&self, col_idx: usize) -> Option<usize> {
        match self.try_locate(col_idx)? {
            ColumnLocator::Payload { slot, .. } => Some(slot as usize),
            ColumnLocator::Pk { .. } => None,
        }
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
    pub(crate) fn check_cols<'a>(&self, cols: impl IntoIterator<Item = (&'a str, u32)>) -> Result<(), OpBuildErr> {
        for (what, c) in cols {
            if self.column(c as usize).is_none() {
                return Err(OpBuildErr::oob_col(what, c, self));
            }
        }
        Ok(())
    }

    /// Where column `ci` lives, or `None` when it is out of range — the total
    /// [`Self::locate`], whose own bound is a release-active panic.
    #[inline]
    pub(crate) fn try_locate(&self, ci: usize) -> Option<ColumnLocator> {
        (ci < self.num_columns()).then(|| self.locate(ci))
    }

    /// True iff column `ci` is a PK column. Total: every PK index is in range,
    /// so an out-of-range `ci` matches none of them. [`Self::locate`] is the
    /// partial one — it resolves a column and requires it to exist.
    #[inline]
    pub fn is_pk_col(&self, ci: usize) -> bool {
        // Widen the stored index rather than narrowing `ci`: `ci as u32` would
        // truncate a large index onto a real PK column.
        self.pk_indices().iter().any(|&p| p as usize == ci)
    }

    /// True iff column `ci` is this schema's *only* PK column — the shape an
    /// FK target must have for the parent probe to read the referenced value
    /// straight out of the packed PK region. Same widen-don't-narrow contract as
    /// [`Self::is_pk_col`].
    #[inline]
    pub fn is_lone_pk_col(&self, ci: usize) -> bool {
        let pk = self.pk_indices();
        pk.len() == 1 && pk[0] as usize == ci
    }

    /// Inverse of `payload_idx`: dense payload slot → logical column index.
    /// Caller must ensure `pi < num_payload_cols()`.
    #[inline]
    pub(crate) fn payload_col_idx(&self, pi: usize) -> usize {
        debug_assert!(pi < self.num_payload_cols(), "payload_col_idx: pi out of range");
        self.payload_to_ci[pi] as usize
    }

    /// The output-key kind a reduce grouped by `cols` over this schema warrants.
    pub(crate) fn reduce_out_key(&self, cols: &[u32]) -> ReduceOutKey {
        ReduceOutKey::for_group_cols(self.pk_indices(), cols, |c| {
            let col = &self.columns[c as usize];
            (col.type_code, col.nullable != 0)
        })
    }

    /// Byte width of the leading `n` columns — the sum, never just `columns[0]`,
    /// since a multi-column span can exceed 16 bytes. Its one production caller
    /// takes a join trace schema's equi-key prefix width (`ops/join.rs`); index
    /// code reads `IndexKeySpec::key_size()` instead.
    #[inline]
    pub fn leading_key_size(&self, n: usize) -> usize {
        self.columns[..n].iter().map(|c| c.size() as usize).sum()
    }

    /// Resolve where column `col_idx`'s value lives. The canonical entry point
    /// for reading a column whose index is not statically a payload column.
    #[inline]
    pub fn locate(&self, col_idx: usize) -> ColumnLocator {
        // Release-active: an out-of-range `col_idx` otherwise falls through to
        // the payload arm with a slot no batch region answers for. A last-line
        // guard against an internal bug — an untrusted index is bounded by the
        // operator constructor that took it — and free, never running per row.
        assert!(
            col_idx < self.num_columns(),
            "locate: col_idx {col_idx} out of bounds (num_columns = {})",
            self.num_columns(),
        );
        let col = self.columns[col_idx];
        let (mut byte_off, mut pk_below) = (0u8, 0usize);
        for (ci, c) in self.pk_columns() {
            if ci == col_idx {
                return ColumnLocator::Pk {
                    byte_off,
                    size: col.size(),
                    type_code: col.type_code,
                };
            }
            byte_off += c.size();
            pk_below += usize::from(ci < col_idx);
        }
        // `byte_off` has reached `pk_stride` here, which `new_with_placement`
        // asserts fits `MAX_PK_BYTES` — so the accumulation above cannot wrap.
        ColumnLocator::Payload {
            slot: (col_idx - pk_below) as u8,
            size: col.size(),
            type_code: col.type_code,
        }
    }
}

/// The schema surface the expression compiler resolves and validates against.
/// Every method forwards to the inherent one — written UFCS, because most
/// collide by name and Rust prefers the inherent method in receiver-dot
/// position, which would recurse instead of forwarding. `payload_slot` and
/// `is_pk_col` are not restated: the trait derives both from `locate`, inverting
/// the inherent chain (`is_pk_col` → `try_payload_idx` → `locate`). The two
/// agree in range; out of range the trait panics in `locate`, exactly as
/// `gnitz_core::Schema`, the other implementor, does.
///
/// No `#[inline]` on the forwarders — every caller reaches them through
/// `&dyn SchemaFacts`, so the hint cannot fire through the vtable.
impl gnitz_expr::SchemaFacts for SchemaDescriptor {
    fn locate(&self, ci: usize) -> ColumnLocator {
        SchemaDescriptor::locate(self, ci)
    }

    fn payload_col_idx(&self, pi: usize) -> usize {
        SchemaDescriptor::payload_col_idx(self, pi)
    }

    fn num_payload_cols(&self) -> usize {
        SchemaDescriptor::num_payload_cols(self)
    }

    fn num_columns(&self) -> usize {
        SchemaDescriptor::num_columns(self)
    }

    // The last two read the column table directly: the field *is* the fact, so
    // there is no inherent method to forward to and nothing to recompute.
    fn col_type_code(&self, ci: usize) -> u8 {
        self.columns[ci].type_code
    }

    fn col_nullable(&self, ci: usize) -> bool {
        self.columns[ci].nullable != 0
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
            match TypeCode::try_from_u8(col.type_code) {
                Some(t) => write!(f, "{t:?}")?,
                None => write!(f, "type({})", col.type_code)?,
            }
            if col.nullable != 0 {
                write!(f, "?")?;
            }
            if self.is_pk_col(ci) {
                write!(f, " pk")?;
            }
        }
        write!(f, "], pk_indices: {:?} }}", self.pk_indices())
    }
}

impl PartialEq for SchemaDescriptor {
    fn eq(&self, other: &Self) -> bool {
        if self.num_columns() != other.num_columns() || self.pk_indices() != other.pk_indices() {
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
/// The schema is the [`IndexKeySpec`]'s own, so a caller needing both builds the
/// spec once and takes the schema off it rather than calling here.
///
/// Total: every rejection is an `Err`, never the constructor's abort. An
/// over-limit schema is reachable only for a *composite* index (a single-column
/// index — including every FK auto-index — always fits, since
/// `PK_LIST_MAX_COLS < MAX_PK_COLUMNS` reserves the prefix slot), via a raw
/// `gnitz-core` client or a crafted persisted row replayed at boot, neither of
/// which goes through the SQL planner's pre-check.
pub fn make_index_schema(source_cols: &[u32], source: &SchemaDescriptor) -> Result<SchemaDescriptor, String> {
    IndexKeySpec::new(source_cols, source)?
        .output_schema(source)
        .ok_or_else(|| "Index: composite key is not a valid primary key".to_string())
}

/// Rebuild a [`SchemaDescriptor`] from a meta-schema block. Every wire rule —
/// region shape, `col_idx` ordering, type-code validity, PK eligibility and
/// arity — is enforced by the shared codec, so this is only the projection onto
/// the engine's own type; column names are carried but nothing engine-side reads
/// one. The arity bound is `MAX_PK_COLUMNS`, the engine's own limit, deliberately
/// wider than the client's `PK_LIST_MAX_COLS`.
pub fn decode_schema_block(data: &[u8], verify_checksum: bool) -> Result<SchemaDescriptor, &'static str> {
    let sb = gnitz_wire::schema_block::SchemaBlock::decode(data, verify_checksum, MAX_PK_COLUMNS)?;
    let mut cols = [SchemaColumn::EMPTY; MAX_COLUMNS];
    for (col, c) in cols[..sb.num_columns()].iter_mut().zip(sb.columns()) {
        *col = SchemaColumn::new(c.type_code, c.meta.nullable as u8);
    }
    Ok(SchemaDescriptor::new(&cols[..sb.num_columns()], sb.pk_indices()))
}

/// The delta store's stamp column: the `_tick` round number, leading the delta
/// schema's PK. Named here so the stamp/strip pair below reads its width off the
/// column rather than repeating the literal at each offset.
pub(crate) const DELTA_TICK_COL: SchemaColumn = SchemaColumn::new(type_code::U64, 0);
// `stamped_with_pk_prefix` writes the stamp as a `u64`'s big-endian image, which
// is this column's OPK image only at this width.
const _: () = assert!(DELTA_TICK_COL.size() as usize == 8);

/// The delta store's schema for a fed view: a `_tick` U64 key column, then the
/// view's PK columns in PK-list order, then the view's payload columns in schema
/// order. Derived rather than persisted, exactly as [`make_index_schema`] is —
/// the delta store is registered and recovered the way a secondary index is.
///
/// The order itself is `gnitz_wire::delta_schema_order`, which the client's
/// `delta_reply_schema` applies to build the reply schema it ships; the two sides
/// therefore cannot permute differently.
///
/// A `SchemaDescriptor` holds types, nullability and a PK-index list and **no
/// column names at all**, so `_tick` is a position, not an identifier: there is
/// nothing for a user column name to collide with.
///
/// This is a **reordering** of the view's columns, not a shift of them, and that
/// is what lets a stamp copy the payload regions verbatim: the set of columns
/// excluded from the payload space is the same set plus `_tick`, which is not one
/// of them, so the *k*-th payload column of the view is the *k*-th payload column
/// of the delta — null bitmap included, which indexes by payload position.
///
/// `None` — never an abort — for the one limit that binds: a view already at
/// `MAX_COLUMNS` cannot carry a feed, because the stamp is one more column.
/// Neither PK limit can, which the assertion below holds to rather than leaving
/// to prose.
///
/// Stamped [`Placement::Local`] — "rows live on whichever worker produced them
/// and are not keyed by `worker_for_pk` at all", which is what these rows are.
/// Nothing routes by it (a delta read is routed off the *view's* schema), so this
/// is not a second line of defence; it is what keeps a future router from being
/// told something false.
pub fn make_delta_schema(view: &SchemaDescriptor) -> Option<SchemaDescriptor> {
    use gnitz_wire::DeltaCol;
    let mut b = DerivedSchema::new();
    for c in gnitz_wire::delta_schema_order(view.pk_indices(), view.num_columns()) {
        match c {
            DeltaCol::Tick => b.push_pk(DELTA_TICK_COL)?,
            DeltaCol::Key(i) => b.push_pk(view.columns[i])?,
            DeltaCol::Payload(i) => b.push(view.columns[i])?,
        }
    }
    Some(b.finish().with_placement(Placement::Local))
}

const _: () = {
    // The stamp is one U64 column ahead of a view PK, which is at most
    // `PK_LIST_MAX_COLS` columns of at most 16 bytes each.
    const STAMPED_COLS: usize = 1 + gnitz_wire::PK_LIST_MAX_COLS;
    const STAMPED_BYTES: usize = 8 + gnitz_wire::PK_LIST_MAX_COLS * 16;
    assert!(
        STAMPED_COLS <= MAX_PK_COLUMNS && STAMPED_BYTES <= MAX_PK_BYTES,
        "a _tick stamp on the widest view PK no longer fits the PK limits"
    );
};

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
    b.push_pk_of(schema)?;
    for &p in project {
        let i = p as usize;
        if schema.try_payload_idx(i).is_some() {
            b.push(schema.columns[i])?;
        }
    }
    Some(b.finish())
}

#[cfg(test)]
#[path = "tests/schema.rs"]
mod tests;
