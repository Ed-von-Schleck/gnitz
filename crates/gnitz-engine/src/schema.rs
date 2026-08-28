//! SQL type constants, schema descriptor types, and row-format utilities.
//!
//! These are shared across the storage, IPC, and query layers.

use gnitz_wire::{is_fixed_int, is_signed_int};

// One rule for what this module re-exports: a *schema fact* — what a column's
// type is, how many of them there can be, how a key is shaped — comes through
// `crate::schema::X` at every engine call site. A *byte primitive* — how a value
// is encoded, decoded, or hashed to a partition — never does; those are named
// `gnitz_wire::X` directly, including inside this file.
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
/// width-tagged `PkBuf` those encoders return. Sits below both schema and
/// storage, and is the **one** import path: a second re-export would leave the
/// byte-order rule spelled two ways in adjacent lines of one call site.
pub mod key;

/// The precomputed per-row read/encode plan for an index's OPK leading-key span.
/// Lives in [`key`] with the rest of the native→OPK encoders it shares its byte
/// contract with; re-exported here because a spec is derived from a pair of
/// schemas, so call sites keep naming `crate::schema::IndexKeySpec`.
pub use key::IndexKeySpec;

/// Build a `SchemaDescriptor` from a wire-neutral `WireSysCol` slice (the
/// canonical system-table column arrays in `gnitz-wire`). The single builder
/// behind every consumer of those arrays — chiefly the catalog's compile-time
/// `SCHEMAS` statics — homed here (L1) so they can never drift. `const`: zero
/// runtime allocation.
///
/// Every such family is [`Placement::Replicated`]: DDL is master-broadcast, so
/// each worker holds an identical full copy.
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

    /// Append one PK column. Must precede every [`Self::push`]: the PK occupies
    /// the leading slots.
    ///
    /// Rejects everything `SchemaDescriptor::new` *asserts* (see
    /// [`SchemaDescriptor::new_with_placement`]), so a caller passing an unvetted
    /// column type gets a `None` rather than an abort inside `finish()`.
    pub(crate) fn push_pk(&mut self, col: SchemaColumn) -> Option<()> {
        debug_assert_eq!(self.pk_len, self.n, "PK columns must precede payload columns");
        if self.pk_len == MAX_PK_COLUMNS
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
    /// every schema and schema builder carries. Distinct from [`Self::new`] so
    /// that padding — the one legitimate use of the undecodable type code `0` —
    /// cannot be confused with a real column, and `new` can hold every column it
    /// builds to a decodable code.
    pub const EMPTY: SchemaColumn = Self::raw(0, 0);

    /// A real column of type `type_code`, which must decode (see
    /// [`gnitz_wire::is_valid_type_code`] for why an unknown one is not inert).
    /// Client-supplied codes are screened at their decode boundary; the assert is
    /// the tripwire for a path that forgets to. Debug-only — the release engine
    /// must still *survive* a corrupt code, which is what the expression
    /// validator's `check_col` and the catalog's `check_col_defs` are for.
    pub const fn new(type_code: u8, nullable: u8) -> Self {
        debug_assert!(gnitz_wire::is_valid_type_code(type_code), "invalid column type code");
        Self::raw(type_code, nullable)
    }

    const fn raw(type_code: u8, nullable: u8) -> Self {
        let is_signed = is_signed_int(type_code) as u8;
        SchemaColumn {
            type_code,
            size: gnitz_wire::wire_stride(type_code) as u8,
            nullable,
            is_signed,
        }
    }

    /// On-disk byte width of one cell of this column. Derived from `type_code`
    /// via `SchemaColumn::new` and never written independently.
    #[inline]
    pub const fn size(&self) -> u8 {
        self.size
    }

    /// True iff this column is a signed integer (I8/I16/I32/I64). Derived from
    /// `type_code` in `new()` (like `size`); read by the fixed-int fast-path
    /// comparator to pick the order-preserving sign-flip mask without a
    /// per-column type-code branch.
    #[inline]
    pub(crate) const fn is_signed(&self) -> bool {
        self.is_signed != 0
    }
}

/// Pre-computed payload row-comparator strategy for a schema. Stored on
/// `SchemaDescriptor` and computed once in `new()` so every merge/sort/join
/// dispatch reads a single field instead of iterating over columns.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PayloadCmpKind {
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
    /// the full PK. Constructors normalize it (and any out-of-range value a
    /// corrupt catalog flag could carry) against the schema's PK arity, so a
    /// `Keyed` prefix read back off a descriptor is never the sentinel — it is
    /// `pk_count` for the default and `1..pk_count` for a `CLUSTER BY` prefix.
    pub const KEYED_DEFAULT: Placement = Placement::Keyed { prefix_len: 0 };

    /// Decode a relation's placement out of a `TABLE_TAB.flags` word. The flags
    /// cannot make the replicated/prefix combination unrepresentable
    /// (`replicated` is a bit, `k` a byte), so the conflict is rejected here at
    /// the catalog trust boundary rather than silently resolved in favour of one.
    pub(crate) fn from_table_flags(flags: u64) -> Result<Placement, String> {
        let props = gnitz_wire::TableProps::from_flags(flags);
        let prefix_len = props.dist_prefix_len;
        if props.replicated {
            if prefix_len != 0 {
                return Err(format!(
                    "replicated and carries a non-default distribution prefix (k={prefix_len}); \
                     these are mutually exclusive"
                ));
            }
            return Ok(Placement::Replicated);
        }
        Ok(Placement::Keyed {
            prefix_len: prefix_len as u8,
        })
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
                let k = if prefix_len == 0 || prefix_len as usize > pk_count {
                    pk_count
                } else {
                    prefix_len as usize
                };
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
    /// `Batch::pk_stride()` and the cached `pk_stride: u8` fields on
    /// `MappedShard`/`DirectWriter`/`MemBatch`; `new` asserts the width holds.
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
    pub(crate) payload_cmp: PayloadCmpKind,
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
// arrays make invisible at the definition. The bound is a boundary, not slack:
// `4+4+20+1+1+2+65+1+1+260 = 359`, aligned to 360.
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
    /// `gnitz_wire::pack_table_flags`); for a view, the one folded from its
    /// sources.
    ///
    /// **A `const fn` whose `assert!`s fire in release and abort the process.**
    /// Untrusted column lists never reach it directly: `DerivedSchema::push_pk`
    /// and `build_schema_from_col_defs` are the total front doors that reject
    /// what this would abort on. A `Keyed` prefix is **normalized**: `0` (the persisted "default"
    /// sentinel) and any value past `|PK|` (only reachable from a corrupted
    /// catalog flag) both clamp to the full PK, so `dist_stride == pk_stride` and
    /// routing stays byte-identical to the full-PK default. Every derived schema
    /// (join/map/reduce/projection output, built via `new`) gets that default and
    /// is never table-key-routed.
    #[track_caller]
    pub(crate) const fn new_with_placement(cols: &[SchemaColumn], pk_indices: &[u32], placement: Placement) -> Self {
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

    /// The narrowest well-formed schema: one non-nullable U64 PK column and no
    /// payload. The placeholder for a slot that must hold *a* descriptor before
    /// the real one is known (the emitter's register-meta resize), and the
    /// default fixture wherever a test needs a schema it does not read.
    pub const fn minimal_u64() -> Self {
        Self::new(&[SchemaColumn::new(type_code::U64, 0)], &[0])
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

    /// Same column count, PK indices, and per-column `type_code`.
    ///
    /// Deliberately ignores per-column `nullable` (and `size`/`is_signed`, which
    /// are derived from `type_code`) — this is **weaker** than the `PartialEq`
    /// impl below, which compares the full column bytes including `nullable`.
    /// Used for identity-MAP elision and sink type-safety, both of which must
    /// treat a nullability-only difference as "same layout". Do not "simplify"
    /// to `self == other`: that would compare `nullable` and change elision
    /// semantics.
    pub(crate) fn same_physical_layout(&self, other: &SchemaDescriptor) -> bool {
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
    /// O(1) field load. Returns `u8` to mirror `SchemaColumn::size()`
    /// and the storage-layer `pk_stride` caches; callers that need
    /// `usize` for buffer arithmetic cast at the use site.
    #[inline]
    pub const fn pk_stride(&self) -> u8 {
        self.pk_stride
    }

    /// Byte width of the distribution prefix (the leading PK slice that
    /// `worker_for_pk` hashes); `pk_stride()` for the full-PK default. Prefer
    /// `worker_for_pk` over reading this and slicing by hand.
    #[inline]
    pub(crate) const fn dist_stride(&self) -> u8 {
        self.dist_stride
    }

    /// The single **table-key router**: maps a row's full OPK PK bytes to its
    /// owning worker by hashing only the leading distribution prefix
    /// (`key[..dist_stride()]`). Every write-side scatter, ingest/probe, and seek
    /// routes a base-table PK through here, so the "slice to the distribution
    /// prefix" contract — the load-bearing half of the prefix-distribution feature
    /// — lives in one place and cannot be forgotten by a new caller. For the
    /// full-PK default `dist_stride() == pk_stride()`, so this is byte-identical to
    /// hashing the whole PK. `key` is the full PK (`key.len() >= dist_stride()`).
    ///
    /// Not for **join-key** routing: the exchange relay scatters route an already
    /// reindexed `_join_pk` over a derived schema and call `worker_for_pk_bytes`
    /// directly (their key is the whole region, never a table prefix).
    #[inline]
    pub fn worker_for_pk(&self, key: &[u8], num_workers: usize) -> usize {
        gnitz_wire::worker_for_pk_bytes(&key[..self.dist_stride() as usize], num_workers)
    }

    /// Where this relation's rows live — the one value the store shape
    /// (`build_relation_store`), the write scatter (broadcast vs
    /// partition-scatter), the read gather / seek unicast, and the join and
    /// exchange analyzers all read, so they cannot disagree. Crate-visible so the
    /// ALTER … DROP NOT NULL descriptor rebuild (`hook_column_alter`) can carry it
    /// across the swap: `SchemaDescriptor::eq` ignores it, so a rebuilt
    /// descriptor must be constructed with it again.
    #[inline]
    pub const fn placement(&self) -> Placement {
        self.placement
    }

    /// Number of non-PK ("payload") columns.
    #[inline]
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

    /// Whether the schema carries a STRING/BLOB (German-string) column. Such a
    /// column is always payload — `is_pk_eligible` excludes them — so `new()`
    /// scans every column without filtering. Callers use this to decide whether
    /// a batch's blob region is live.
    #[inline]
    pub(crate) fn has_german_string(&self) -> bool {
        self.has_german_string
    }

    /// Dense payload slot (batch payload region + null-bitmap bit position) for
    /// `col_idx`, or `None` when it names no payload column of this schema —
    /// a PK column, or out of range. Total, so it doubles as the "is this a real
    /// payload column" test; `read_cursor::col_bytes` feeds the result straight
    /// to `get_col_ptr`.
    #[inline]
    pub(crate) fn try_payload_idx(&self, col_idx: usize) -> Option<usize> {
        if col_idx >= self.num_columns() || self.is_pk_col(col_idx) {
            return None;
        }
        // Count the PK columns below `col_idx`, not `col_idx - pk_count`: the PK
        // list is in general neither a prefix of the columns nor sorted.
        Some(col_idx - self.pk_indices().iter().filter(|&&p| (p as usize) < col_idx).count())
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

    /// Inverse of `payload_idx`: dense payload slot → logical column index.
    /// Caller must ensure `pi < num_payload_cols()`.
    #[inline]
    pub(crate) fn payload_col_idx(&self, pi: usize) -> usize {
        debug_assert!(pi < self.num_payload_cols(), "payload_col_idx: pi out of range");
        self.payload_to_ci[pi] as usize
    }

    /// The output-key kind a reduce grouped by `cols` over this schema warrants
    /// — re-derived by the engine compiler only to *validate* the planner's
    /// shipped [`ReduceOutKey`], through the same shared [`ReduceOutKey::decide`]
    /// chain the planner decided with.
    pub(crate) fn reduce_out_key(&self, cols: &[u32]) -> ReduceOutKey {
        ReduceOutKey::for_group_cols(self.pk_indices(), cols, |c| {
            let col = &self.columns[c as usize];
            (col.type_code, col.nullable != 0)
        })
    }

    /// Byte offset of `col_idx` within the row's PK region. Walks
    /// `pk_columns()` in pk-list order; caller must ensure `col_idx` is
    /// a PK column.
    pub fn pk_byte_offset(&self, col_idx: usize) -> u8 {
        debug_assert!(self.is_pk_col(col_idx), "pk_byte_offset: col_idx must be a pk column");
        let mut off: u16 = 0;
        for (pk_ci, c) in self.pk_columns() {
            if pk_ci == col_idx {
                return off as u8;
            }
            off += c.size() as u16;
        }
        unreachable!("pk_byte_offset: col_idx is a pk column but not found in pk_columns()");
    }

    /// Byte width of the leading `n` columns. For an index schema this is the
    /// OPK leading-key span width (`idx_key_size`) — the sum of every promoted
    /// column's width, never just `columns[0]` (a composite `UNIQUE (a, b)`
    /// span can exceed 16 bytes); the source-PK suffix begins there.
    #[inline]
    pub fn leading_key_size(&self, n: usize) -> usize {
        self.columns[..n].iter().map(|c| c.size() as usize).sum()
    }

    /// Resolve where column `col_idx`'s value lives. The canonical entry point
    /// for reading a column whose index is not statically a payload column.
    #[inline]
    pub fn locate(&self, col_idx: usize) -> ColumnLocator {
        // Release-active bound. An out-of-range `col_idx` otherwise resolves to
        // the PK arm and dies in `pk_byte_offset`'s `unreachable!()` with a
        // message naming neither `locate` nor the bad index. A `debug_assert`
        // would let that ship in release, so this is a hard `assert!`. It is a
        // last-line guard against an internal bug, distinct from untrusted-index
        // rejection (a client-supplied circuit naming an OOB column). `locate`
        // runs at extractor/program setup and, at worst, once per group
        // (`GroupKeyCols::new`) — never per row — so the check and the PK arm's
        // `pk_byte_offset` walk are both free.
        assert!(
            col_idx < self.num_columns(),
            "locate: col_idx {col_idx} out of bounds (num_columns = {})",
            self.num_columns(),
        );
        let size = self.columns[col_idx].size();
        let type_code = self.columns[col_idx].type_code;
        match self.try_payload_idx(col_idx) {
            None => ColumnLocator::Pk {
                byte_off: self.pk_byte_offset(col_idx),
                size,
                type_code,
            },
            Some(slot) => ColumnLocator::Payload {
                slot: slot as u8,
                size,
                type_code,
            },
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
// Row-format utilities
// ---------------------------------------------------------------------------

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
/// indexed-key region is `Σ promoted widths`; `seek_by_index` prefix-scans it
/// (full or leading-prefix), then reads the source PK bytes directly out of the
/// index PK suffix. The 1-element list is the single-column index.
///
/// Total: every rejection is an `Err`, never the constructor's abort. An
/// over-limit schema is reachable only for a *composite* index (a single-column
/// index — including every FK auto-index — always fits, since
/// `PK_LIST_MAX_COLS < MAX_PK_COLUMNS` reserves the prefix slot), via a raw
/// `gnitz-core` client or a crafted persisted row replayed at boot, neither of
/// which goes through the SQL planner's pre-check.
pub fn make_index_schema(source_cols: &[u32], source: &SchemaDescriptor) -> Result<SchemaDescriptor, String> {
    let mut col_types: Vec<u8> = Vec::with_capacity(source_cols.len());
    for &c in source_cols {
        if c as usize >= source.num_columns() {
            return Err(format!(
                "Index: column index {} out of bounds (columns={})",
                c,
                source.num_columns()
            ));
        }
        col_types.push(source.columns[c as usize].type_code);
    }
    let src_pk = source.pk_indices();
    // Shared with the SQL planner's CREATE INDEX pre-check, so the promotion
    // rule and the arity/stride limits can never disagree across the layers.
    let promoted = gnitz_wire::index_key_types(&col_types, src_pk.len(), source.pk_stride() as usize)?;
    let mut b = DerivedSchema::new();
    // Structural backstop: `index_key_types` above already rejects arity, stride
    // and ineligible types, so this cannot know which rule the builder tripped.
    let over = || "Index: composite key is not a valid primary key".to_string();
    for &t in &promoted {
        b.push_pk(SchemaColumn::new(t, 0)).ok_or_else(over)?;
    }
    for &ci in src_pk {
        b.push_pk(SchemaColumn::new(source.columns[ci as usize].type_code, 0))
            .ok_or_else(over)?;
    }
    Ok(b.finish())
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
        *col = SchemaColumn::new(c.type_code, gnitz_wire::col_meta_nullable(c.flags) as u8);
    }
    Ok(SchemaDescriptor::new(&cols[..sb.num_columns()], sb.pk_indices()))
}

/// The delta store's schema for a fed view: a `_tick` U64 key column, then the
/// view's PK columns in PK-list order, then the view's payload columns in schema
/// order. Derived rather than persisted, exactly as [`make_index_schema`] is —
/// the delta store is registered and recovered the way a secondary index is.
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
    let mut b = DerivedSchema::new();
    b.push_pk(SchemaColumn::new(type_code::U64, 0))?;
    b.push_pk_of(view)?;
    for (_, c) in view.payload_columns() {
        b.push(*c)?;
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
mod tests {
    use super::*;

    // ── Derived operator-output schemas ─────────────────────────────────────

    #[test]
    fn test_project_schema_compound_pk() {
        // Compound-PK input: 4 columns, PK = (col1, col2). Project [0, 3].
        let input = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[1, 2],
        );
        let out = project_schema(&input, &[0, 3]).unwrap();
        // Two PK columns + two non-PK projected columns = 4 total.
        assert_eq!(out.num_columns(), 4);
        assert_eq!(out.pk_indices(), &[0, 1]);

        // Single-PK input collapses back to pk_indices = [0].
        let input_single = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let out_single = project_schema(&input_single, &[1]).unwrap();
        assert_eq!(out_single.pk_indices(), &[0]);

        // The bound is PK-inclusive: a payload count that alone fits still
        // overflows once the PK columns are prepended.
        let wide: Vec<u32> = vec![1; crate::schema::MAX_COLUMNS];
        assert_eq!(project_schema(&input_single, &wide), None);
    }

    #[test]
    fn test_identity_map_detection() {
        let a = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let b = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        assert!(a.same_physical_layout(&b));

        let c = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        );
        assert!(!a.same_physical_layout(&c));
    }

    // ── Reduce output key ────────────────────────────────────────────────────

    /// A nullable single group column must NOT be promoted to the natural PK
    /// (the PK region has no null bitmap); a non-nullable one is. Grouping by
    /// the PK itself takes precedence as `PkPermutation`.
    #[test]
    fn nullable_group_col_is_not_natural_reduce_key() {
        let nullable = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 1),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[1],
        );
        assert_eq!(nullable.reduce_out_key(&[0]), ReduceOutKey::SyntheticFold);

        let non_nullable = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        assert_eq!(non_nullable.reduce_out_key(&[1]), ReduceOutKey::SingleNaturalCol);
        assert_eq!(non_nullable.reduce_out_key(&[0]), ReduceOutKey::PkPermutation);
    }

    // ── Placement / distribution prefix (CLUSTER BY) ────────────────────────

    /// 3-column compound PK `(U32, U64, U64)` + one payload, so the columns have
    /// distinct widths and a prefix stride is unambiguous.
    fn three_col_pk_schema(dist_k: u8) -> SchemaDescriptor {
        three_col_placed(Placement::Keyed { prefix_len: dist_k })
    }

    fn three_col_placed(placement: Placement) -> SchemaDescriptor {
        SchemaDescriptor::new_with_placement(
            &[
                SchemaColumn::new(type_code::U32, 0), // col 0: 4 bytes
                SchemaColumn::new(type_code::U64, 0), // col 1: 8 bytes
                SchemaColumn::new(type_code::U64, 0), // col 2: 8 bytes
                SchemaColumn::new(type_code::I64, 0), // payload
            ],
            &[0, 1, 2],
            placement,
        )
    }

    #[test]
    fn default_dist_is_full_pk() {
        // `new` (no clause), `Keyed { prefix_len: 0 }`, and `Keyed { prefix_len:
        // |PK| }` all yield dist_stride == pk_stride and a normalized k == pk_count.
        let pk_stride = 4 + 8 + 8; // U32 + U64 + U64
        for s in [
            three_col_pk_schema(0), // 0 = persisted default sentinel
            three_col_pk_schema(3), // explicit full PK
            SchemaDescriptor::new(
                // bare `new`
                &[
                    SchemaColumn::new(type_code::U32, 0),
                    SchemaColumn::new(type_code::U64, 0),
                    SchemaColumn::new(type_code::U64, 0),
                    SchemaColumn::new(type_code::I64, 0),
                ],
                &[0, 1, 2],
            ),
        ] {
            assert_eq!(s.pk_stride() as usize, pk_stride);
            assert_eq!(s.dist_stride(), s.pk_stride(), "default: dist == full PK");
            assert_eq!(s.placement(), Placement::Keyed { prefix_len: 3 });
        }
    }

    #[test]
    fn dist_stride_sums_leading_prefix_columns() {
        // k=1 ⇒ just col 0 (U32 = 4 bytes).
        let s1 = three_col_pk_schema(1);
        assert_eq!(s1.placement(), Placement::Keyed { prefix_len: 1 });
        assert_eq!(s1.dist_stride(), 4);
        // k=2 ⇒ col 0 + col 1 (U32 + U64 = 12 bytes).
        let s2 = three_col_pk_schema(2);
        assert_eq!(s2.placement(), Placement::Keyed { prefix_len: 2 });
        assert_eq!(s2.dist_stride(), 12);
    }

    #[test]
    fn dist_prefix_clamps_out_of_range() {
        // A k past |PK| (only reachable from a corrupted catalog flag) clamps to
        // the full PK rather than overflowing the prefix sum.
        let s = three_col_pk_schema(99);
        assert_eq!(s.placement(), Placement::Keyed { prefix_len: 3 });
        assert_eq!(s.dist_stride(), s.pk_stride());
    }

    /// A relation the router slices by nothing takes the full-PK width, so every
    /// `worker_for_pk` slice over it stays in range.
    #[test]
    fn unkeyed_placement_takes_the_full_pk_width() {
        for p in [Placement::Replicated, Placement::Local] {
            let s = three_col_placed(p);
            assert_eq!(s.placement(), p);
            assert_eq!(s.dist_stride(), s.pk_stride(), "{p:?}");
        }
    }

    #[test]
    fn test_schema_column_layout_and_is_signed() {
        // `SchemaColumn` is 4 bytes, and `SchemaDescriptor` holds MAX_COLUMNS of
        // them by value: a fifth field here would breach the size pin.
        assert_eq!(std::mem::size_of::<SchemaColumn>(), 4);

        // `is_signed` is derived from `type_code` in `new()`: true for I8..I64,
        // false for every unsigned / float / string / blob type.
        for tc in [type_code::I8, type_code::I16, type_code::I32, type_code::I64] {
            assert!(SchemaColumn::new(tc, 0).is_signed(), "type_code {tc} must be signed");
            // Nullability does not change signedness.
            assert!(
                SchemaColumn::new(tc, 1).is_signed(),
                "nullable type_code {tc} must be signed"
            );
        }
        for tc in [
            type_code::U8,
            type_code::U16,
            type_code::U32,
            type_code::U64,
            type_code::U128,
            type_code::UUID,
            type_code::F32,
            type_code::F64,
            type_code::STRING,
            type_code::BLOB,
        ] {
            assert!(
                !SchemaColumn::new(tc, 0).is_signed(),
                "type_code {tc} must not be signed"
            );
        }
    }

    #[test]
    fn test_new_constructs_schema() {
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::STRING, 1),
        ];
        let s = SchemaDescriptor::new(&cols, &[0]);
        assert_eq!(s.num_columns(), 3);
        assert_eq!(s.pk_indices(), &[0]);
        assert_eq!(s.columns[0].type_code, type_code::U64);
        assert_eq!(s.columns[1].type_code, type_code::I64);
        assert_eq!(s.columns[2].type_code, type_code::STRING);

        // Trailing slots are `SchemaColumn::EMPTY` — the padding type code
        // nothing reads.
        assert_eq!(s.columns[3].type_code, 0);

        // payload_columns() walks non-PK indices in logical order.
        let payload: Vec<usize> = s.payload_columns().map(|(pi, _)| s.payload_col_idx(pi)).collect();
        assert_eq!(payload, vec![1, 2]);

        // Non-zero pk_index round-trips (use I64 col at index 1, not STRING).
        let s2 = SchemaDescriptor::new(&cols, &[1]);
        assert_eq!(s2.pk_indices(), &[1]);

        // Empty placeholder (Default-style).
        let empty = SchemaDescriptor::new(&[], &[]);
        assert_eq!(empty.num_columns(), 0);
    }

    #[test]
    fn test_try_payload_idx_around_pk() {
        // pk_index = 1: col 0 maps to payload 0, col 2 maps to payload 1, and
        // the PK column (1) has no payload slot.
        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[1],
        );
        assert_eq!(s.try_payload_idx(0), Some(0));
        assert_eq!(s.try_payload_idx(2), Some(1));
        assert_eq!(s.try_payload_idx(1), None);
    }

    #[test]
    #[should_panic(expected = "locate: col_idx 3 out of bounds")]
    fn test_locate_out_of_bounds_panics() {
        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        let _ = s.locate(3);
    }

    /// The descriptor constructor admits exactly the wire allow-list — no more
    /// (STRING/BLOB carry an unrelocatable heap offset, floats break the
    /// byte-equal key contract) and no less. Driven off `is_pk_eligible` rather
    /// than a hand-listed set so a newly added type code is covered the moment
    /// it exists.
    #[test]
    fn test_pk_eligibility_matches_the_wire_allow_list() {
        for tc in 0u8..=255 {
            let Some(size) = TypeCode::try_from_u8(tc).map(|t| t.wire_stride()) else {
                continue;
            };
            assert!(size > 0, "type_code {tc} has no width");
            let cols = [SchemaColumn::new(tc, 0)];
            let built = std::panic::catch_unwind(|| SchemaDescriptor::new(&cols, &[0])).is_ok();
            assert_eq!(
                built,
                gnitz_wire::is_pk_eligible(tc),
                "type_code {tc}: descriptor and wire allow-list disagree on PK eligibility",
            );
        }
    }

    /// `pk_stride` sums the PK columns' widths, and `pk_columns()` yields them in
    /// pk-list order — at both arities, and at a PK index that is not 0.
    #[test]
    fn test_pk_stride_and_pk_columns() {
        let pk_cols = |s: &SchemaDescriptor| -> Vec<(usize, usize, u8)> {
            s.pk_columns()
                .enumerate()
                .map(|(ord, (ci, c))| (ord, ci, c.type_code))
                .collect()
        };

        // Compound: [U64, U32], both PK. Stride = 8 + 4.
        let compound = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U32, 0),
            ],
            &[0, 1],
        );
        assert_eq!(compound.pk_stride(), 12);
        assert_eq!(pk_cols(&compound), vec![(0, 0, type_code::U64), (1, 1, type_code::U32)]);

        // Single PK at column 1, so the pk-list position and the column index
        // differ.
        let single = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::U128, 0),
            ],
            &[1],
        );
        assert_eq!(single.pk_stride(), 8);
        assert_eq!(pk_cols(&single), vec![(0, 1, type_code::I64)]);
    }

    #[test]
    fn test_max_pk_columns_boundary() {
        // Construct exactly MAX_PK_COLUMNS PK columns so a future bump
        // of the constant keeps exercising the boundary case.
        let cols = [SchemaColumn::new(type_code::U64, 0); MAX_PK_COLUMNS];
        let pks: Vec<u32> = (0..MAX_PK_COLUMNS as u32).collect();
        let s = SchemaDescriptor::new(&cols, &pks);
        assert_eq!(s.pk_indices().len(), MAX_PK_COLUMNS);
        let collected: Vec<(usize, usize)> = s.pk_columns().enumerate().map(|(ord, (ci, _))| (ord, ci)).collect();
        let expected: Vec<(usize, usize)> = (0..MAX_PK_COLUMNS).map(|k| (k, k)).collect();
        assert_eq!(collected, expected);
        assert_eq!(s.pk_stride() as usize, MAX_PK_COLUMNS * 8);
    }

    #[test]
    #[should_panic(expected = "duplicate PK column index")]
    fn test_duplicate_pk_guard_panics_in_release() {
        // No cfg(debug_assertions) gate — guard is a hard assert! and
        // must fire in release too.
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ];
        let _ = SchemaDescriptor::new(&cols, &[0, 0]);
    }

    // ── SchemaFacts conformance ──────────────────────────────────────────────

    /// `SchemaDescriptor`'s `SchemaFacts` forwarders must report exactly what
    /// the schema itself does, over the shared shape matrix. The harness is the
    /// only way to reach the trait methods: most collide by name with an
    /// inherent method that Rust would prefer in receiver-dot position, so a
    /// forwarder that silently reimplements — and thereby flips `no_nulls` or
    /// the `check_col` admissibility dispatch — would go unnoticed by a direct
    /// check.
    #[test]
    fn schema_descriptor_conforms_to_schema_facts() {
        gnitz_expr::assert_schema_facts_matrix(|cols, pk| {
            let scols: Vec<SchemaColumn> = cols
                .iter()
                .map(|&(tc, nullable)| SchemaColumn::new(tc, nullable as u8))
                .collect();
            let pk_idx: Vec<u32> = pk.iter().map(|&i| i as u32).collect();
            SchemaDescriptor::new(&scols, &pk_idx)
        });
    }
}
