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
use gnitz_expr::PAYLOAD_MAPPING_PK_SENTINEL;

/// Order-preserving primary-key (OPK) primitives — every native→OPK encoder
/// (whole PK, seek wire pair, index leading span), compare/pack, and the
/// width-tagged `PkBuf` those encoders return. Sits below both schema and
/// storage, and is the one import path: `storage` used to re-export the cluster
/// so its call sites read `crate::storage::X`, which left the §1/§6 byte-order
/// rule spelled two ways in adjacent lines of the same file.
pub(crate) mod key;

/// The precomputed per-row read/encode plan for an index's OPK leading-key span.
/// Lives in [`key`] with the rest of the native→OPK encoders it shares its byte
/// contract with; re-exported here because a spec is derived from a pair of
/// schemas, so call sites keep naming `crate::schema::IndexKeySpec`.
pub(crate) use key::IndexKeySpec;

/// Build a `SchemaDescriptor` from a wire-neutral `WireSysCol` slice (the
/// canonical system-table column arrays in `gnitz-wire`). The single builder
/// behind every consumer of those arrays — chiefly the catalog's compile-time
/// `SCHEMAS` statics — homed here (L1) so they can never drift. `const`: zero
/// runtime allocation.
pub(crate) const fn from_wire_cols(cols: &[gnitz_wire::WireSysCol], pk_indices: &[u32]) -> SchemaDescriptor {
    let mut buf = [SchemaColumn::EMPTY; MAX_COLUMNS];
    let mut i = 0;
    while i < cols.len() {
        buf[i] = SchemaColumn::new(cols[i].type_code as u8, if cols[i].nullable { 1 } else { 0 });
        i += 1;
    }
    let (head, _) = buf.split_at(cols.len());
    SchemaDescriptor::new(head, pk_indices)
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
    pub(crate) fn push(&mut self, col: SchemaColumn) -> Option<()> {
        *self.cols.get_mut(self.n)? = col;
        self.n += 1;
        Some(())
    }

    /// Append one PK column. Must precede every [`Self::push`]: the PK occupies
    /// the leading slots.
    pub(crate) fn push_pk(&mut self, col: SchemaColumn) -> Option<()> {
        debug_assert_eq!(self.pk_len, self.n, "PK columns must precede payload columns");
        if self.pk_len == MAX_PK_COLUMNS {
            return None;
        }
        self.push(col)?;
        self.pk_len += 1;
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
pub(crate) struct SchemaColumn {
    pub(crate) type_code: u8,
    size: u8,
    pub(crate) nullable: u8,
    is_signed: u8,
}

impl SchemaColumn {
    /// The unused-slot filler for the fixed `[SchemaColumn; MAX_COLUMNS]` arrays
    /// every schema and schema builder carries. Distinct from [`Self::new`] so
    /// that padding — the one legitimate use of the undecodable type code `0` —
    /// cannot be confused with a real column, and `new` can hold every column it
    /// builds to a decodable code.
    pub(crate) const EMPTY: SchemaColumn = Self::raw(0, 0);

    /// A real column of type `type_code`, which must decode (see
    /// [`gnitz_wire::is_valid_type_code`] for why an unknown one is not inert).
    /// Client-supplied codes are screened at their decode boundary; the assert is
    /// the tripwire for a path that forgets to. Debug-only — the release engine
    /// must still *survive* a corrupt code, which is what the expression
    /// validator's `check_col` and the catalog's `check_col_defs` are for.
    pub(crate) const fn new(type_code: u8, nullable: u8) -> Self {
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
    pub(crate) const fn size(&self) -> u8 {
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

const fn compute_payload_cmp(cols: &[SchemaColumn], payload_mapping: &[u8; MAX_COLUMNS]) -> PayloadCmpKind {
    let mut ci = 0;
    while ci < cols.len() {
        if payload_mapping[ci] != PAYLOAD_MAPPING_PK_SENTINEL {
            let col = cols[ci];
            if !(col.nullable == 0 && is_fixed_int(col.type_code)) {
                return PayloadCmpKind::Generic;
            }
        }
        ci += 1;
    }
    PayloadCmpKind::FixedIntNonnull
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct SchemaDescriptor {
    num_columns: u32,
    pk_count: u32,
    pk_indices: [u32; MAX_PK_COLUMNS],
    /// Total bytes per row of the PK region — sum of
    /// `columns[pk_indices[k]].size()` for k in 0..pk_count. Precomputed
    /// once in `new()` so per-row hot loops never re-run the sum. `u8`
    /// matches the existing `storage::batch::pk_stride()` helper and the
    /// cached `pk_stride: u8` fields on `MappedShard`/`DirectWriter`/
    /// `MemBatch`; today's worst case is 5 × 16 = 80, well under 255.
    pk_stride: u8,
    /// Byte width of the **distribution prefix** — the OPK bytes of the first
    /// `dist_prefix_len` PK columns, the leading slice every write-side
    /// table-key router (`partition_for_pk_bytes`) hashes to pick a partition.
    /// Summed alongside `pk_stride` in `new_with_dist`, walking the PK columns
    /// in PK-list order, so it matches the OPK encoder's tight big-endian layout
    /// exactly. `dist_stride == pk_stride` for the default (full-PK) distribution,
    /// making every sliced route byte-identical to today's full-PK routing.
    dist_stride: u8,
    /// Distribution prefix length `k`: the number of leading PK columns rows are
    /// hash-distributed by (`1 ≤ k ≤ pk_count`). Read by
    /// `shard_cols_match_dist_key` to detect co-partitioned joins / local GROUP
    /// BY. `k == pk_count` is the default (full-PK) distribution.
    dist_prefix_len: u8,
    /// `true` iff this relation is **replicated**: every worker holds an identical
    /// full copy (single partition at index 0), writes broadcast, and reads
    /// single-source. Set on a base table from `TABLE_TAB.flags` (see
    /// `gnitz_wire::table_flags_replicated`), on every system catalog family (DDL
    /// is master-broadcast), and on a view all of whose sources are replicated
    /// (`hook_view_register`) — which makes the property transitive up a view
    /// chain. Every derived/intermediate schema (join/map/reduce/projection
    /// output, built via `new`/`new_with_dist`) is non-replicated. Mutually
    /// exclusive with a non-default `dist_prefix_len` (DDL-enforced).
    replicated: bool,
    /// payload_mapping[ci] = dense payload index, or PAYLOAD_MAPPING_PK_SENTINEL:
    /// PK columns hold the sentinel, payload columns hold their dense payload
    /// slot. The sentinel is this table's *encoding* of "no payload slot" and
    /// stops here — `try_payload_idx` and `is_pk_col` are how the rest of the
    /// engine reads it, so no call site handles a poisoned byte.
    payload_mapping: [u8; MAX_COLUMNS],
    /// payload_to_ci[pi] = logical column index for dense payload slot `pi`.
    /// Inverse of `payload_mapping` over the non-PK columns; the trailing
    /// `num_columns - pk_count`..MAX_COLUMNS slots hold the sentinel.
    /// Lets `payload_columns()` walk a contiguous `0..num_payload` range
    /// with one byte load per element, no per-row predicate.
    payload_to_ci: [u8; MAX_COLUMNS],
    /// Pre-computed payload comparator strategy. Derived from column types in
    /// `new()`; read by every merge/sort/join dispatch (via `with_payload_cmp!`)
    /// in place of calling `schema_is_fixedint_nonnull` at each site.
    pub(crate) payload_cmp: PayloadCmpKind,
    pub(crate) columns: [SchemaColumn; MAX_COLUMNS],
}

// `SchemaDescriptor` is `Copy` and embedded by value in ~20 structs (`Batch`
// among them, which the VM takes and replaces several times per instruction),
// so a field added here is paid for at every one of those copies. Pinned rather
// than merely documented: the three fixed-capacity arrays make the cost
// invisible at the definition.
const _: () = assert!(std::mem::size_of::<SchemaDescriptor>() <= 424);

const fn compute_mappings(num_columns: usize, pk_indices: &[u32]) -> ([u8; MAX_COLUMNS], [u8; MAX_COLUMNS]) {
    let mut payload_mapping = [PAYLOAD_MAPPING_PK_SENTINEL; MAX_COLUMNS];
    let mut payload_to_ci = [PAYLOAD_MAPPING_PK_SENTINEL; MAX_COLUMNS];
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
            payload_mapping[ci] = pi;
            payload_to_ci[pi as usize] = ci as u8;
            pi += 1;
        }
        ci += 1;
    }
    (payload_mapping, payload_to_ci)
}

impl SchemaDescriptor {
    /// Construct a SchemaDescriptor from a column list and PK index list.
    /// The empty case `pk_indices = &[]` is reserved for the placeholder
    /// produced by `Default::default()` and is structurally invalid for real
    /// use. Accepts up to `MAX_PK_COLUMNS` entries.
    #[track_caller]
    pub(crate) const fn new(cols: &[SchemaColumn], pk_indices: &[u32]) -> Self {
        // Default distribution = the full PK: today's behavior, byte-for-byte.
        Self::new_with_dist(cols, pk_indices, pk_indices.len())
    }

    /// Construct a `SchemaDescriptor` whose hash-distribution key is the leading
    /// `dist_prefix_len` PK columns (a per-table choice persisted in
    /// `TABLE_TAB.flags`; see `gnitz_wire::pack_table_flags`). `dist_prefix_len`
    /// is **normalized**: `0` (the persisted "default" sentinel) and any value
    /// past `|PK|` (only reachable from a corrupted catalog flag) both clamp to
    /// the full PK, so `dist_stride == pk_stride` and routing stays byte-identical
    /// to the full-PK default. Only base-table schemas carry a chosen prefix;
    /// every derived schema (join/map/reduce/projection output, built via `new`)
    /// gets the full-PK default and is never table-key-routed.
    #[track_caller]
    pub(crate) const fn new_with_dist(cols: &[SchemaColumn], pk_indices: &[u32], dist_prefix_len: usize) -> Self {
        assert!(cols.len() <= MAX_COLUMNS, "new: too many columns");
        assert!(
            pk_indices.len() <= MAX_PK_COLUMNS,
            "new: pk_indices.len() exceeds MAX_PK_COLUMNS",
        );

        // 0 is the persisted default ("full PK"); a value past |PK| can only
        // come from a corrupted catalog flag — clamp it rather than index out of
        // bounds in the `dist_stride` sum below.
        let dist_k = if dist_prefix_len == 0 || dist_prefix_len > pk_indices.len() {
            pk_indices.len()
        } else {
            dist_prefix_len
        };

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
        let (payload_mapping, payload_to_ci) = compute_mappings(cols.len(), pk_indices);
        let payload_cmp = compute_payload_cmp(cols, &payload_mapping);
        SchemaDescriptor {
            num_columns: cols.len() as u32,
            pk_count: pk_indices.len() as u32,
            pk_indices: pk_arr,
            pk_stride,
            dist_stride: dist_stride_acc as u8,
            dist_prefix_len: dist_k as u8,
            // Stamped by the caller via `with_replicated`; every constructor
            // defaults it off.
            replicated: false,
            payload_mapping,
            payload_to_ci,
            payload_cmp,
            columns,
        }
    }

    /// Return a copy of this schema marked replicated (`true`) or partitioned
    /// (`false`). A replicated relation must carry the default full-PK
    /// distribution (`dist_prefix_len == pk_count`); the DDL layer enforces that,
    /// so this is a pure tag.
    #[inline]
    pub(crate) const fn with_replicated(mut self, replicated: bool) -> Self {
        self.replicated = replicated;
        self
    }

    pub(crate) const fn minimal_u64() -> Self {
        Self::new(&[SchemaColumn::new(type_code::U64, 0)], &[0])
    }

    /// Number of logical columns in this schema (PK + payload).
    #[inline]
    pub(crate) const fn num_columns(&self) -> usize {
        self.num_columns as usize
    }

    /// All PK column indices, in compound-key order. Length is the PK arity:
    /// 1 for a single-column PK, or the full sequence for a compound table PK
    /// (and the co-partition analyzers compare against this whole sequence).
    #[inline]
    pub(crate) fn pk_indices(&self) -> &[u32] {
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
    pub(crate) const fn pk_stride(&self) -> u8 {
        self.pk_stride
    }

    /// Byte width of the distribution prefix (the leading PK slice that
    /// `partition_for_pk` hashes); `pk_stride()` for the full-PK default. Prefer
    /// `partition_for_pk` over reading this and slicing by hand.
    #[inline]
    pub(crate) const fn dist_stride(&self) -> u8 {
        self.dist_stride
    }

    /// The single **table-key router**: maps a row's full OPK PK bytes to a
    /// partition by hashing only the leading distribution prefix
    /// (`key[..dist_stride()]`). Every write-side scatter, ingest/probe, and seek
    /// routes a base-table PK through here, so the "slice to the distribution
    /// prefix" contract — the load-bearing half of the prefix-distribution feature
    /// — lives in one place and cannot be forgotten by a new caller. For the
    /// full-PK default `dist_stride() == pk_stride()`, so this is byte-identical to
    /// hashing the whole PK. `key` is the full PK (`key.len() >= dist_stride()`).
    ///
    /// Not for **join-key** routing: the exchange relay scatters route an already
    /// reindexed `_join_pk` over a derived schema and call `partition_for_pk_bytes`
    /// directly (their key is the whole region, never a table prefix).
    #[inline]
    pub(crate) fn partition_for_pk(&self, key: &[u8]) -> usize {
        gnitz_wire::partition_for_pk_bytes(&key[..self.dist_stride() as usize])
    }

    /// Distribution prefix length `k` — leading PK columns rows are hashed by
    /// (`k == pk_count` for the full-PK default). Crate-visible so the ALTER …
    /// DROP NOT NULL descriptor rebuild (`hook_column_alter`) can carry the
    /// routing prefix across the swap — `SchemaDescriptor::eq` ignores it, so a
    /// rebuilt descriptor must re-apply it explicitly (§5).
    #[inline]
    pub(crate) const fn dist_prefix_len(&self) -> u8 {
        self.dist_prefix_len
    }

    /// True iff this relation is replicated — a full copy on every worker
    /// (single partition 0), with broadcast writes and single-source reads.
    /// Consulted by the write scatter (broadcast vs partition-scatter), the read
    /// gather (single-source vs union), the join co-partition analyzer (a
    /// replicated source, or a partitioned source whose join partner is
    /// replicated, skips its exchange), and the bootstrap trim exemption.
    #[inline]
    pub(crate) const fn replicated(&self) -> bool {
        self.replicated
    }

    /// True iff the PK is a single signed column (I8/I16/I32/I64). Its OPK
    /// encoding flips the sign bit of the leading byte, so the `extend_pk` /
    /// `set_pk_at` u128 fast paths — which write right-aligned big-endian bytes
    /// with no sign flip — are wrong for it. Such callers must use
    /// `extend_pk_bytes`. Only test-build callers need it, so it is gated to
    /// test builds to keep the production API minimal.
    #[cfg(test)]
    #[inline]
    pub(crate) const fn pk_is_signed_single_col(&self) -> bool {
        if self.pk_count != 1 {
            return false;
        }
        let tc = self.columns[self.pk_indices[0] as usize].type_code;
        is_signed_int(tc)
    }

    /// Number of non-PK ("payload") columns.
    #[inline]
    pub(crate) const fn num_payload_cols(&self) -> usize {
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
    pub(crate) fn payload_columns(&self) -> impl Iterator<Item = (usize, &SchemaColumn)> {
        (0..self.num_payload_cols()).map(move |pi| (pi, &self.columns[self.payload_to_ci[pi] as usize]))
    }

    /// Whether the schema carries a STRING/BLOB (German-string) column. Those
    /// can never be PK columns, so scanning the payload columns is exhaustive.
    /// Callers use this to decide whether a batch's blob region is live.
    #[inline]
    pub(crate) fn has_german_string(&self) -> bool {
        self.payload_columns()
            .any(|(_, col)| gnitz_wire::is_german_string(col.type_code))
    }

    /// Dense payload slot (batch payload region + null-bitmap bit position) for a
    /// payload column. `None` for a PK column — PK columns have no payload slot.
    /// The only `col_idx -> payload_index` function on `SchemaDescriptor`:
    /// because it returns `Option`, "this column is a PK and has no payload slot"
    /// must be handled, not poisoned with a sentinel. Reading a column whose
    /// index is not statically known to be payload goes through [`Self::locate`].
    #[inline]
    pub(crate) fn try_payload_idx(&self, col_idx: usize) -> Option<usize> {
        match self.payload_mapping[col_idx] {
            PAYLOAD_MAPPING_PK_SENTINEL => None,
            slot => Some(slot as usize),
        }
    }

    /// True iff column `ci` is a PK column.
    #[inline]
    pub(crate) fn is_pk_col(&self, ci: usize) -> bool {
        self.payload_mapping[ci] == PAYLOAD_MAPPING_PK_SENTINEL
    }

    /// Inverse of `payload_idx`: dense payload slot → logical column index.
    /// Caller must ensure `pi < num_payload_cols()`.
    #[inline]
    pub(crate) fn payload_col_idx(&self, pi: usize) -> usize {
        debug_assert!(pi < self.num_payload_cols(), "payload_col_idx: pi out of range");
        self.payload_to_ci[pi] as usize
    }

    /// True iff `cols` is a permutation of `pk_indices()` (same set,
    /// any order). Used by reduce to detect `GROUP BY pk` even when the
    /// SQL lists PK columns in an order that differs from the schema's
    /// pk-list order.
    pub(crate) fn group_cols_eq_pk(&self, cols: &[u32]) -> bool {
        let pk = self.pk_indices();
        cols.len() == pk.len() && pk.iter().all(|p| cols.contains(p))
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

    /// True iff `cols` is **exactly** this table's distribution prefix —
    /// `pk_indices()[..k]` in PK order, where `k = dist_prefix_len()`. The
    /// co-partition contract the exchange router enforces (`fill_worker_indices`
    /// routes by `partition_for_pk_bytes` over the leading `dist_stride` OPK bytes
    /// in schema order): a reindex/shard key equal to the distribution prefix
    /// means a derived operator co-partitions with this base table and its
    /// network exchange can be skipped. For the full-PK default (`k == |PK|`) this
    /// reduces to "shard key is exactly the source PK in order"; one component of
    /// a compound PK — or a permuted PK — never matches. The DAG/compiler
    /// co-partition analyzers carry shard columns as `i32`, so this takes
    /// `&[i32]`; PK indices are small and non-negative, so the compare is exact.
    ///
    /// **Exact `== k`, never a super-prefix (`>= k`)**, and load-bearing: a
    /// super-prefix gate would let the two sides of a join skip at *different*
    /// prefix widths, hashing equal join keys to different workers so the elided
    /// exchange silently drops matches. A side whose join-key length ≠ its own `k`
    /// instead exchanges and repartitions to the full key, reconverging with the
    /// other side. (`dist_prefix_len ≤ pk_count`, so `pk[..k]` is in range; the
    /// `cluster_by_super_prefix_join_safety` E2E test exercises this.)
    pub(crate) fn shard_cols_match_dist_key(&self, cols: &[i32]) -> bool {
        let k = self.dist_prefix_len() as usize;
        let pk = self.pk_indices();
        cols.len() == k && cols.iter().zip(&pk[..k]).all(|(&c, &p)| c == p as i32)
    }

    /// Byte offset of `col_idx` within the row's PK region. Walks
    /// `pk_columns()` in pk-list order; caller must ensure `col_idx` is
    /// a PK column.
    pub(crate) fn pk_byte_offset(&self, col_idx: usize) -> u8 {
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
    pub(crate) fn leading_key_size(&self, n: usize) -> usize {
        self.columns[..n].iter().map(|c| c.size() as usize).sum()
    }

    /// Resolve where column `col_idx`'s value lives. The canonical entry point
    /// for reading a column whose index is not statically a payload column.
    #[inline]
    pub(crate) fn locate(&self, col_idx: usize) -> ColumnLocator {
        // Release-active bound. An out-of-range `col_idx` otherwise reads a
        // zeroed padding slot in the fixed-capacity `columns`/`payload_mapping`
        // arrays, resolves to the PK arm, and dies in `pk_byte_offset`'s
        // `unreachable!()` with a message naming neither `locate` nor the bad
        // index. A `debug_assert` would let that ship in release, so this is a
        // hard `assert!`. It is a last-line guard against an internal bug,
        // distinct from
        // untrusted-index rejection (a client-supplied circuit naming an OOB
        // column). `locate` runs at extractor/program setup and, at worst, once
        // per group (`extract_group_key`) — never per row — so the check and the
        // PK arm's `pk_byte_offset` walk are both free.
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
/// `is_pk_col` are not restated: the trait derives both from `locate`, which is
/// what `SchemaDescriptor` does anyway.
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

impl Default for SchemaDescriptor {
    fn default() -> Self {
        Self::new(&[], &[])
    }
}

// ---------------------------------------------------------------------------
// Row-format utilities
// ---------------------------------------------------------------------------

/// Validate that a peer-supplied schema descriptor matches the expected one:
/// column count, PK column indices, per-column type codes and nullability.
/// Used at every trust boundary where rows are decoded against a descriptor
/// the sender chose (client INSERT frames, worker reply trains) — batch append
/// helpers do not validate shape, so an unguarded mismatch turns into
/// misinterpreted bytes handed onward.
pub(crate) fn validate_schema_match(wire: &SchemaDescriptor, expected: &SchemaDescriptor) -> Result<(), String> {
    if wire == expected {
        return Ok(());
    }
    // Only reached on a mismatch: the walk below exists to name it, not to
    // decide it. Keeping `==` as the verdict is what stops the two from drifting
    // — a field added to the comparison automatically tightens this validator.
    if wire.num_columns() != expected.num_columns() {
        return Err(format!(
            "Schema mismatch: expected {} columns, got {}",
            expected.num_columns(),
            wire.num_columns(),
        ));
    }
    if wire.pk_indices() != expected.pk_indices() {
        return Err(format!(
            "Schema mismatch: expected pk_indices={:?}, got {:?}",
            expected.pk_indices(),
            wire.pk_indices(),
        ));
    }
    for i in 0..wire.num_columns() {
        if wire.columns[i].type_code != expected.columns[i].type_code {
            return Err(format!(
                "Schema mismatch at column {}: expected type {}, got {}",
                i, expected.columns[i].type_code, wire.columns[i].type_code,
            ));
        }
        if wire.columns[i].nullable != expected.columns[i].nullable {
            return Err(format!(
                "Schema mismatch at column {}: expected nullable={}, got {}",
                i, expected.columns[i].nullable, wire.columns[i].nullable,
            ));
        }
    }
    Err("Schema mismatch: descriptors differ".to_string())
}

// ---------------------------------------------------------------------------
// Schema-shaping free functions
//
// Built purely from a `SchemaDescriptor` (no catalog or storage state); used
// by the catalog DDL/index paths and the runtime gather/preflight paths.
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
/// Bounds-checks every column, promotes it (rejecting STRING/BLOB/float), and
/// validates the index-schema PK limits — all **before** calling
/// `SchemaDescriptor::new`: that constructor is a `const fn` whose `assert!`s
/// fire in release and abort the master. An over-limit schema is reachable only
/// for a *composite* index (a single-column index — including every FK
/// auto-index — always fits, since `PK_LIST_MAX_COLS < MAX_PK_COLUMNS` reserves
/// the prefix slot), via a raw `gnitz-core` client or a crafted/over-range
/// persisted row replayed at boot, neither of which goes through the SQL
/// planner's pre-check. Validating here converts the abort into a clean ingest
/// `Err` for every path (defence in depth at the catalog trust boundary).
pub(crate) fn make_index_schema(source_cols: &[u32], source: &SchemaDescriptor) -> Result<SchemaDescriptor, String> {
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
    let over = || "Index: composite key exceeds the PK column limit".to_string();
    for &t in &promoted {
        b.push_pk(SchemaColumn::new(t, 0)).ok_or_else(over)?;
    }
    for &ci in src_pk {
        b.push_pk(SchemaColumn::new(source.columns[ci as usize].type_code, 0))
            .ok_or_else(over)?;
    }
    Ok(b.finish())
}

/// Build the schema for a `gather_family` result: the PK columns of `schema`
/// (in pk-list order, so the packed PK round-trips identically) followed by
/// the projected columns in `project` order as payload. `project` must list
/// only non-PK columns (PK members are resolved from the packed PK without a
/// gather); a projected PK column would be emitted twice.
///
/// `pub(crate)`: the master's gather drain builds the same descriptor as the
/// expected reply schema, so a projected reply with the wrong shape errors
/// instead of mis-decoding.
pub(crate) fn project_schema(schema: &SchemaDescriptor, project: &[u8]) -> SchemaDescriptor {
    let mut b = DerivedSchema::new();
    b.push_pk_of(schema)
        .expect("project_schema: source PK exceeds the PK limit");
    for &p in project {
        debug_assert!(
            !schema.is_pk_col(p as usize),
            "project_schema: projected column {p} is a PK column"
        );
        b.push(schema.columns[p as usize])
            .expect("project_schema: projection exceeds MAX_COLUMNS");
    }
    b.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // ── Distribution prefix (CLUSTER BY) ────────────────────────────────────

    /// 3-column compound PK `(U32, U64, U64)` + one payload, so the columns have
    /// distinct widths and a prefix stride is unambiguous.
    fn three_col_pk_schema(dist_k: usize) -> SchemaDescriptor {
        SchemaDescriptor::new_with_dist(
            &[
                SchemaColumn::new(type_code::U32, 0), // col 0: 4 bytes
                SchemaColumn::new(type_code::U64, 0), // col 1: 8 bytes
                SchemaColumn::new(type_code::U64, 0), // col 2: 8 bytes
                SchemaColumn::new(type_code::I64, 0), // payload
            ],
            &[0, 1, 2],
            dist_k,
        )
    }

    #[test]
    fn default_dist_is_full_pk() {
        // `new` (no clause) and `new_with_dist(.., 0)` and `new_with_dist(.., |PK|)`
        // all yield dist_stride == pk_stride and dist_prefix_len == pk_count.
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
            assert_eq!(s.dist_prefix_len(), s.pk_indices().len() as u8);
        }
    }

    #[test]
    fn dist_stride_sums_leading_prefix_columns() {
        // k=1 ⇒ just col 0 (U32 = 4 bytes).
        let s1 = three_col_pk_schema(1);
        assert_eq!(s1.dist_prefix_len(), 1);
        assert_eq!(s1.dist_stride(), 4);
        // k=2 ⇒ col 0 + col 1 (U32 + U64 = 12 bytes).
        let s2 = three_col_pk_schema(2);
        assert_eq!(s2.dist_prefix_len(), 2);
        assert_eq!(s2.dist_stride(), 12);
    }

    #[test]
    fn dist_prefix_clamps_out_of_range() {
        // A k past |PK| (only reachable from a corrupted catalog flag) clamps to
        // the full PK rather than overflowing the prefix sum.
        let s = three_col_pk_schema(99);
        assert_eq!(s.dist_prefix_len(), 3);
        assert_eq!(s.dist_stride(), s.pk_stride());
    }

    #[test]
    fn shard_cols_match_dist_key_is_exact_prefix() {
        let k1 = three_col_pk_schema(1); // CLUSTER BY col0
                                         // Exact prefix at k=1 matches; the full PK and a super-prefix do not.
        assert!(k1.shard_cols_match_dist_key(&[0]));
        assert!(!k1.shard_cols_match_dist_key(&[0, 1]), "super-prefix must NOT match");
        assert!(!k1.shard_cols_match_dist_key(&[0, 1, 2]));
        assert!(!k1.shard_cols_match_dist_key(&[1]), "non-leading column");
        assert!(!k1.shard_cols_match_dist_key(&[]));

        // Default (full-PK) schema: dist key is the whole PK, exactly.
        let full = three_col_pk_schema(0);
        assert!(full.shard_cols_match_dist_key(&[0, 1, 2]));
        assert!(
            !full.shard_cols_match_dist_key(&[0]),
            "a single component is not the full key"
        );
        assert!(!full.shard_cols_match_dist_key(&[0, 1]));

        // k=2 matches exactly [0,1], not [0] and not [0,1,2].
        let k2 = three_col_pk_schema(2);
        assert!(k2.shard_cols_match_dist_key(&[0, 1]));
        assert!(!k2.shard_cols_match_dist_key(&[0]));
        assert!(!k2.shard_cols_match_dist_key(&[0, 1, 2]));
    }

    fn two_col_schema(col1_nullable: u8) -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, col1_nullable),
            ],
            &[0],
        )
    }

    #[test]
    fn validate_schema_match_ok() {
        let sd = two_col_schema(0);
        assert!(validate_schema_match(&sd, &sd).is_ok());
    }

    #[test]
    fn validate_schema_match_rejects_column_count_mismatch() {
        let wire = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        assert!(validate_schema_match(&wire, &two_col_schema(0)).is_err());
    }

    #[test]
    fn validate_schema_match_rejects_pk_index_mismatch() {
        let wire = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[1],
        );
        assert!(validate_schema_match(&wire, &two_col_schema(0)).is_err());
    }

    #[test]
    fn validate_schema_match_rejects_type_code_mismatch() {
        let wire = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::F64, 0),
            ],
            &[0],
        );
        assert!(validate_schema_match(&wire, &two_col_schema(0)).is_err());
    }

    #[test]
    fn validate_schema_match_rejects_nullable_mismatch() {
        let wire = two_col_schema(0); // col1 not-nullable
        let expected = two_col_schema(1); // col1 nullable
        assert!(validate_schema_match(&wire, &expected).is_err());
    }

    #[test]
    fn test_schema_column_layout_and_is_signed() {
        // Repurposing the old `_pad` byte as `is_signed` must not grow the struct:
        // SchemaDescriptor is Copy and embedded by value in 20+ structs.
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

        // Trailing slot fill: SchemaColumn::EMPTY resolves via wire_stride(0)
        // → 8 (default arm). Locked in so future regressions in the trailing
        // representation are caught.
        assert_eq!(s.columns[3].type_code, 0);
        assert_eq!(s.columns[3].size(), 8);
        assert_eq!(s.columns[MAX_COLUMNS - 1].size(), 8);

        // payload_columns() walks non-PK indices in logical order.
        let payload: Vec<usize> = s.payload_columns().map(|(pi, _)| s.payload_col_idx(pi)).collect();
        assert_eq!(payload, vec![1, 2]);
        assert_eq!(s.try_payload_idx(1), Some(0));
        assert_eq!(s.try_payload_idx(2), Some(1));
        // The PK column has no payload slot.
        assert_eq!(s.try_payload_idx(0), None);

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

    #[test]
    fn test_pk_columns_single_pk() {
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U128, 0),
        ];
        let s = SchemaDescriptor::new(&cols, &[1]);
        let v: Vec<(usize, usize, u8)> = s
            .pk_columns()
            .enumerate()
            .map(|(ord, (ci, c))| (ord, ci, c.type_code))
            .collect();
        assert_eq!(v, vec![(0, 1, type_code::I64)]);
        assert_eq!(s.pk_indices()[0], 1);
    }

    #[test]
    fn test_pk_stride_matches_single_pk_size() {
        // Floats are excluded: F32/F64 are not PK-eligible (rejected in `new`).
        for tc in [
            type_code::U8,
            type_code::I8,
            type_code::U16,
            type_code::I16,
            type_code::U32,
            type_code::I32,
            type_code::U64,
            type_code::I64,
            type_code::U128,
            type_code::UUID,
        ] {
            let cols = [SchemaColumn::new(tc, 0)];
            let s = SchemaDescriptor::new(&cols, &[0]);
            assert_eq!(
                s.pk_stride(),
                s.columns[s.pk_indices()[0] as usize].size(),
                "pk_stride mismatch for type_code {tc}",
            );
        }
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

    #[test]
    fn test_pk_stride_compound() {
        // Synthetic compound schema: [U64, U32] with both columns as PK.
        // Sum = 8 + 4 = 12.
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
        ];
        let s = SchemaDescriptor::new(&cols, &[0, 1]);
        assert_eq!(s.pk_stride(), 12);
        let v: Vec<(usize, usize, u8)> = s
            .pk_columns()
            .enumerate()
            .map(|(ord, (ci, c))| (ord, ci, c.type_code))
            .collect();
        assert_eq!(v, vec![(0, 0, type_code::U64), (1, 1, type_code::U32)]);
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
    fn test_default_empty_schema() {
        let s = SchemaDescriptor::default();
        assert_eq!(s.pk_columns().count(), 0);
        assert_eq!(s.pk_stride(), 0);
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
