//! SQL type constants, schema descriptor types, and row-format utilities.
//!
//! These are shared across the storage, IPC, and query layers.

use rustc_hash::FxHashMap;

use crate::foundation::codec::{read_u32_le, read_u64_le};

pub(crate) use gnitz_wire::type_code;
pub(crate) use gnitz_wire::ReduceOutKey;
pub(crate) use gnitz_wire::TypeCode;
pub use gnitz_wire::MAX_COLUMNS;
pub(crate) use gnitz_wire::SHORT_STRING_THRESHOLD;
pub(crate) use gnitz_wire::{is_fixed_int, is_routable_int, is_signed_int};
pub use gnitz_wire::{MAX_PK_BYTES, MAX_PK_COLUMNS};

/// Order-preserving primary-key (OPK) primitives — encode/compare/route/pack
/// and the width-tagged `PkBuf`. Sits below both schema and storage; storage
/// re-exports each item so its call sites are unchanged.
pub(crate) mod key;

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
    pub const fn new(type_code: u8, nullable: u8) -> Self {
        let is_signed = is_signed_int(type_code) as u8;
        SchemaColumn {
            type_code,
            size: type_size(type_code),
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

/// Widest PK region that still fits in a packed `u128` word (16 bytes), the
/// boundary where a key stops fitting one `u128`. At or below it `get_pk`
/// returns the exact key as a `u128` (wider regions must read `get_pk_bytes`)
/// and the seek wire image splits into a low-16 word plus a `> 16` suffix
/// (`seek_opk_bytes`); above it a shard's PK region must be stored `Raw`
/// (a `Constant` region holds only 16 bytes) and is ordered via `compare_pk_bytes`.
pub(crate) const NARROW_PK_MAX_BYTES: usize = 16;

/// Reassemble the native seek image from the wire pair `(low, extra)` — the
/// inverse of `PkTuple::split_wire` — and OPK-encode it via [`key::opk_key`].
/// The shared seek-key encoder for the master partition router
/// (`fan_out_seek_async`) and the worker SEEK handler (`seek_family`) at every
/// PK width. The seek frame carries the key as native LE column bytes (it
/// bypasses `append_pk_region`, so the bytes are not yet OPK): the low ≤16 ride
/// in `low`, a wide PK's `16..stride` suffix in `extra` (empty for narrow PKs).
/// Errors if `extra` is shorter than the wide suffix `stride - 16`.
pub(crate) fn seek_opk_bytes(
    schema: &SchemaDescriptor,
    low: u128,
    extra: &[u8],
) -> Result<([u8; MAX_PK_BYTES], usize), String> {
    let stride = schema.pk_stride() as usize;
    if stride > MAX_PK_BYTES {
        return Err(format!("PK stride {stride} exceeds MAX_PK_BYTES {MAX_PK_BYTES}"));
    }
    let needed = stride.saturating_sub(NARROW_PK_MAX_BYTES);
    if extra.len() < needed {
        return Err(format!(
            "PK stride {stride} requires {needed} extra bytes, got {}",
            extra.len()
        ));
    }
    // Native image = `low`'s 16 LE bytes, then the wide suffix. The upper bound
    // is `16 + needed` (not `stride`): for a narrow PK `needed == 0` makes it the
    // empty copy `le[16..16]` rather than the inverted range `le[16..stride]`.
    let mut le = [0u8; MAX_PK_BYTES];
    le[..NARROW_PK_MAX_BYTES].copy_from_slice(&low.to_le_bytes());
    le[NARROW_PK_MAX_BYTES..NARROW_PK_MAX_BYTES + needed].copy_from_slice(&extra[..needed]);
    Ok(key::opk_key(schema, &le[..stride]))
}

/// Sentinel for any dense payload-index slot (e.g.
/// `SchemaDescriptor::payload_mapping[ci]`) that needs to express
/// "this slot refers to a PK column, not a payload column". Using
/// `u8::MAX` (not 0) keeps the sentinel unambiguous against a real
/// payload index of 0.
pub(crate) const PAYLOAD_MAPPING_PK_SENTINEL: u8 = u8::MAX;

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
pub struct SchemaDescriptor {
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
    /// `true` iff this is a **replicated** base table: every worker holds an
    /// identical full copy (single partition at index 0), writes broadcast, and
    /// reads single-source. Set only on base-table schemas decoded from
    /// `TABLE_TAB.flags` (see `gnitz_wire::table_flags_replicated`); every
    /// derived/intermediate schema (join/map/reduce/projection output, built via
    /// `new`/`new_with_dist`) is non-replicated, and a relation derived from
    /// replicated sources tracks its distribution at the circuit level, not here.
    /// Mutually exclusive with a non-default `dist_prefix_len` (DDL-enforced).
    replicated: bool,
    /// payload_mapping[ci] = dense payload index, or PAYLOAD_MAPPING_PK_SENTINEL:
    /// PK columns hold the sentinel, payload columns hold their dense payload
    /// slot. Lets call sites that need this byte read it directly via
    /// `payload_mapping_byte(ci)` instead of re-deriving it from a branch on
    /// `is_pk_col` + `payload_idx`.
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
    pub payload_cmp: PayloadCmpKind,
    pub columns: [SchemaColumn; MAX_COLUMNS],
}

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
    pub const fn new(cols: &[SchemaColumn], pk_indices: &[u32]) -> Self {
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
    pub const fn new_with_dist(cols: &[SchemaColumn], pk_indices: &[u32], dist_prefix_len: usize) -> Self {
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

        let mut columns = [SchemaColumn::new(0, 0); MAX_COLUMNS];
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
            // STRING and BLOB store a German-string struct whose heap_offset
            // points into the batch blob. The PK region is bulk-copied without
            // blob relocation, so these types would produce dangling pointers.
            assert!(
                cols[pk_indices[k] as usize].type_code != type_code::STRING
                    && cols[pk_indices[k] as usize].type_code != type_code::BLOB,
                "new: STRING and BLOB columns cannot be PK columns \
                 (PK region is bulk-copied without blob relocation)",
            );
            // `compare_pk_bytes` reads PK bytes with no null-bit handling; a
            // nullable PK would silently corrupt the merge comparison.
            assert!(
                cols[pk_indices[k] as usize].nullable == 0,
                "new: PK columns must be non-nullable",
            );
            // `compare_pk_bytes` and the order-preserving sort-key encoder have
            // no float arm (IEEE-754 breaks the byte-equal key contract), so
            // both panic on a float PK. Float PKs are already rejected at the
            // catalog DDL and wire layers; close the invariant here too so the
            // descriptor constructor is the single authority on PK eligibility.
            assert!(
                cols[pk_indices[k] as usize].type_code != type_code::F32
                    && cols[pk_indices[k] as usize].type_code != type_code::F64,
                "new: F32 and F64 columns cannot be PK columns \
                 (compare_pk_bytes has no float ordering)",
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
            // Distribution is a base-table property set via `with_replicated`
            // after decoding `TABLE_TAB.flags`; every constructor defaults it off.
            replicated: false,
            payload_mapping,
            payload_to_ci,
            payload_cmp,
            columns,
        }
    }

    /// Return a copy of this schema marked replicated (`true`) or partitioned
    /// (`false`). Used by the catalog DDL hook after decoding
    /// `gnitz_wire::table_flags_replicated`. A replicated table must carry the
    /// default full-PK distribution (`dist_prefix_len == pk_count`); the DDL
    /// layer enforces that, so this is a pure tag.
    #[inline]
    pub const fn with_replicated(mut self, replicated: bool) -> Self {
        self.replicated = replicated;
        self
    }

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

    /// Iterate over PK columns in pk-list order. Mirror of
    /// `payload_columns()`. Yields `(pk_ord, col_idx, &SchemaColumn)`.
    /// No non-test caller today; introduced preemptively so future
    /// compound-PK migration sites can drop in `for (ord, ci, col) in
    /// schema.pk_columns()` without redefining the iterator shape.
    #[inline]
    pub fn pk_columns(&self) -> impl Iterator<Item = (usize, usize, &SchemaColumn)> {
        self.pk_indices()
            .iter()
            .copied()
            .enumerate()
            .map(move |(ord, ci)| (ord, ci as usize, &self.columns[ci as usize]))
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
    /// `partition_for_pk` hashes); `pk_stride()` for the full-PK default. Prefer
    /// `partition_for_pk` over reading this and slicing by hand.
    #[inline]
    pub const fn dist_stride(&self) -> u8 {
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
    pub fn partition_for_pk(&self, key: &[u8]) -> usize {
        crate::schema::key::partition_for_pk_bytes(&key[..self.dist_stride() as usize])
    }

    /// Distribution prefix length `k` — leading PK columns rows are hashed by
    /// (`k == pk_count` for the full-PK default).
    #[inline]
    const fn dist_prefix_len(&self) -> u8 {
        self.dist_prefix_len
    }

    /// True iff this is a replicated base table — a full copy on every worker
    /// (single partition 0), with broadcast writes and single-source reads.
    /// Consulted by the write scatter (broadcast vs partition-scatter), the read
    /// gather (single-source vs union), the join co-partition analyzer (a
    /// replicated source, or a partitioned source whose join partner is
    /// replicated, skips its exchange), and the bootstrap trim exemption.
    #[inline]
    pub const fn replicated(&self) -> bool {
        self.replicated
    }

    /// True iff the PK is a single signed column (I8/I16/I32/I64). Its OPK
    /// encoding flips the sign bit of the leading byte, so the `extend_pk` /
    /// `set_pk_at` u128 fast paths — which write right-aligned big-endian bytes
    /// with no sign flip — are wrong for it. Such callers must use
    /// `extend_pk_bytes`. Only the `#[cfg(test)]` `append_row` guard needs this
    /// today, so it is gated to test builds to keep the production API minimal.
    #[cfg(test)]
    #[inline]
    pub(crate) const fn pk_is_signed_single_col(&self) -> bool {
        if self.pk_count != 1 {
            return false;
        }
        let tc = self.columns[self.pk_indices[0] as usize].type_code;
        is_signed_int(tc)
    }

    /// The single PK column index. Use only at boundaries that have not yet
    /// been generalized (format encoders, catalog serialization, SQL parser
    /// path, wire/client BatchAppender). Asserts pk_count == 1.
    ///
    /// Hard `assert!` (not `debug_assert!`): this is the release-active
    /// canary for the day a compound PK reaches a boundary that has not
    /// yet been generalised — a `debug_assert!` would compile out in
    /// release and let the silent truncation to the first PK column ship
    /// to production.
    #[inline]
    #[track_caller]
    pub(crate) const fn pk_index_single(&self) -> u32 {
        assert!(self.pk_count == 1, "compound PK not yet supported here");
        self.pk_indices[0]
    }

    /// Number of non-PK ("payload") columns.
    #[inline]
    pub const fn num_payload_cols(&self) -> usize {
        self.num_columns as usize - self.pk_count as usize
    }

    /// Iterate over the non-PK ("payload") columns.
    ///
    /// Yields `(payload_idx, col_idx, &SchemaColumn)` where:
    /// - `payload_idx` is the dense 0-based index used for batch payload
    ///   regions and null-bitmap bits;
    /// - `col_idx` is the original logical column index in `self.columns`.
    ///
    /// Walks a contiguous `0..num_payload` range with one byte load per
    /// element via `payload_to_ci` — no per-row predicate.
    #[inline]
    pub fn payload_columns(&self) -> impl Iterator<Item = (usize, usize, &SchemaColumn)> {
        let num_payload = self.num_payload_cols();
        (0..num_payload).map(move |pi| {
            let ci = self.payload_to_ci[pi] as usize;
            (pi, ci, &self.columns[ci])
        })
    }

    /// Whether the schema carries a STRING/BLOB (German-string) column. Those
    /// can never be PK columns, so scanning the payload columns is exhaustive.
    /// Callers use this to decide whether a batch's blob region is live.
    #[inline]
    pub(crate) fn has_german_string(&self) -> bool {
        self.payload_columns()
            .any(|(_, _, col)| gnitz_wire::is_german_string(col.type_code))
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
    pub fn is_pk_col(&self, ci: usize) -> bool {
        self.payload_mapping[ci] == PAYLOAD_MAPPING_PK_SENTINEL
    }

    /// Raw `payload_mapping[ci]` byte — equals `PAYLOAD_MAPPING_PK_SENTINEL`
    /// for PK columns, the dense payload index otherwise. Lets call sites
    /// that encode "pk → sentinel else payload_idx as u8" read the
    /// precomputed byte directly without a branch.
    #[inline]
    pub(crate) fn payload_mapping_byte(&self, ci: usize) -> u8 {
        self.payload_mapping[ci]
    }

    /// Inverse of `payload_idx`: dense payload slot → logical column index.
    /// Caller must ensure `pi < num_payload_cols()`.
    #[inline]
    pub fn payload_col_idx(&self, pi: usize) -> usize {
        debug_assert!(pi < self.num_payload_cols(), "payload_col_idx: pi out of range");
        self.payload_to_ci[pi] as usize
    }

    /// True iff `cols` is a permutation of `pk_indices()` (same set,
    /// any order). Used by reduce to detect `GROUP BY pk` even when the
    /// SQL lists PK columns in an order that differs from the schema's
    /// pk-list order.
    pub fn group_cols_eq_pk(&self, cols: &[u32]) -> bool {
        let pk = self.pk_indices();
        cols.len() == pk.len() && pk.iter().all(|p| cols.contains(p))
    }

    /// The output-key kind a reduce grouped by `cols` over this schema warrants
    /// — re-derived by the engine compiler only to *validate* the planner's
    /// shipped [`ReduceOutKey`], through the same shared [`ReduceOutKey::decide`]
    /// chain the planner decided with.
    pub fn reduce_out_key(&self, cols: &[u32]) -> ReduceOutKey {
        let single_natural = cols.len() == 1 && {
            let c = &self.columns[cols[0] as usize];
            c.nullable == 0 && TypeCode::from_validated_u8(c.type_code).is_natural_reduce_key()
        };
        ReduceOutKey::decide(self.group_cols_eq_pk(cols), single_natural)
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
    pub fn shard_cols_match_dist_key(&self, cols: &[i32]) -> bool {
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
        for (_, pk_ci, c) in self.pk_columns() {
            if pk_ci == col_idx {
                return off as u8;
            }
            off += c.size() as u16;
        }
        unreachable!("pk_byte_offset: col_idx is a pk column but not found in pk_columns()");
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
        // hard `assert!` in the `pk_index_single` canary style. It is a
        // last-line guard against an internal bug, distinct from
        // untrusted-index rejection (a client-supplied circuit naming an OOB
        // column). `locate` runs once per extractor at setup, not per row, so
        // the check is free.
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

/// The three per-row region reads a [`ColumnLocator`]/[`IndexKeySpec`] needs from
/// a physical batch, abstracted so schema (L1) does not name `storage::MemBatch`
/// (L2) — the up-edge that would re-form the `schema ↔ storage` cycle. The sole
/// implementor is `storage::MemBatch`; every call site monomorphizes to it, so
/// this is **static dispatch only** — never take `&dyn RowView` (it would add a
/// vtable to the per-row locator paths). The `&'b` returns are decoupled from
/// `&self` so `bytes()` can hand back a slice that outlives the row-view borrow.
pub(crate) trait RowView<'b> {
    /// The row's null-bitmap word (bit N = payload slot N is NULL).
    fn get_null_word(&self, row: usize) -> u64;
    /// The row's packed OPK PK-region bytes (`pk_stride` wide).
    fn get_pk_bytes(&self, row: usize) -> &'b [u8];
    /// `size` bytes of payload column `col` (native LE) in `row`.
    fn get_col_ptr(&self, row: usize, col: usize, size: usize) -> &'b [u8];
}

/// Where a logical column's value physically lives in a row, resolved once from
/// the schema. The only sanctioned way to read a column whose index is not
/// statically known to be a payload column: it cannot silently treat a PK
/// column as payload (the corruption `payload_idx`'s sentinel return invited).
/// A 4-byte `Copy` value (three `u8` fields + a 1-byte tag). The coordinates
/// match the schema's own widths — `pk_byte_offset` returns `u8` (PK stride ≤
/// `MAX_PK_BYTES` = 80), `payload_mapping` slots are `u8` (< `MAX_COLUMNS` = 65,
/// so ≤ 63 with at least one PK column), and every fixed-width column is ≤ 16
/// bytes — so `locate` stores them without widening and a `Vec<ColumnLocator>`
/// (group-key / emit columns) stays dense.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ColumnLocator {
    /// PK column: value is OPK-at-rest in the PK region at `byte_off`, width
    /// `size`, type `type_code`. PK columns are non-nullable.
    Pk { byte_off: u8, size: u8, type_code: u8 },
    /// Payload column: value is native-LE in dense payload slot `slot` (also its
    /// null-bitmap bit position), width `size`, type `type_code`.
    Payload { slot: u8, size: u8, type_code: u8 },
}

const _: () = assert!(
    std::mem::size_of::<ColumnLocator>() <= 8,
    "ColumnLocator must stay packed; a usize coordinate would balloon it to 24 bytes",
);

impl ColumnLocator {
    #[inline]
    pub(crate) fn size(&self) -> usize {
        match *self {
            ColumnLocator::Pk { size, .. } | ColumnLocator::Payload { size, .. } => size as usize,
        }
    }

    #[inline]
    pub(crate) fn type_code(&self) -> u8 {
        match *self {
            ColumnLocator::Pk { type_code, .. } | ColumnLocator::Payload { type_code, .. } => type_code,
        }
    }

    /// True iff this column is NULL in `row`. PK columns are never null.
    #[inline]
    pub(crate) fn is_null<'b>(&self, mb: &impl RowView<'b>, row: usize) -> bool {
        match *self {
            ColumnLocator::Pk { .. } => false,
            ColumnLocator::Payload { slot, .. } => (mb.get_null_word(row) >> slot) & 1 != 0,
        }
    }

    /// Raw at-rest bytes of the column in `row`: OPK/big-endian for a PK column,
    /// native little-endian for a payload column. For hashing, group keys, and
    /// verbatim copies. (Generalises the old `ColLoc::bytes`.) On a STRING/BLOB
    /// column these are the 16-byte German-string struct (a blob heap offset for
    /// long strings), not the content — content callers resolve through the blob
    /// arena. The returned slice borrows the batch's page memory (`'b`), not the
    /// `&self`/`&mb` reference, so it stays valid after the locator and the
    /// row-view borrow are dropped (matching [`RowView::get_pk_bytes`]/
    /// [`RowView::get_col_ptr`], which both return `&'b`).
    #[inline]
    pub(crate) fn bytes<'b>(&self, mb: &impl RowView<'b>, row: usize) -> &'b [u8] {
        match *self {
            ColumnLocator::Pk { byte_off, size, .. } => {
                let o = byte_off as usize;
                &mb.get_pk_bytes(row)[o..o + size as usize]
            }
            ColumnLocator::Payload { slot, size, .. } => mb.get_col_ptr(row, slot as usize, size as usize),
        }
    }

    /// Native little-endian value bytes of the column in `row`: a payload
    /// column verbatim, a PK column OPK-decoded into `scratch` (undoing the
    /// big-endian sign-flipped at-rest form). The value-reading counterpart to
    /// [`Self::bytes`] — every consumer that interprets a column's *value*
    /// (aggregation, order-encoding, exemplar copies) must read through here so
    /// a PK-source column can never be consumed in its at-rest byte order.
    #[inline]
    pub(crate) fn native_le_bytes<'a, 'b: 'a>(
        &self,
        mb: &impl RowView<'b>,
        row: usize,
        scratch: &'a mut [u8; 16],
    ) -> &'a [u8] {
        match *self {
            ColumnLocator::Pk { size, type_code, .. } => {
                *scratch = gnitz_wire::decode_pk_column_owned(self.bytes(mb, row), type_code);
                &scratch[..size as usize]
            }
            ColumnLocator::Payload { .. } => self.bytes(mb, row),
        }
    }

    /// Canonical native u128 key for the value in `row` (sign-aware; the form
    /// `has_pk` and the index seeks compare on). Callers must `is_null`-gate a
    /// nullable payload column first; a PK column is never null.
    #[inline]
    pub(crate) fn native_key<'b>(&self, mb: &impl RowView<'b>, row: usize) -> u128 {
        match *self {
            ColumnLocator::Pk {
                byte_off,
                size,
                type_code,
            } => pk_native_key(mb.get_pk_bytes(row), byte_off as usize, size as usize, type_code),
            ColumnLocator::Payload { slot, size, type_code } => payload_native_key(
                mb.get_col_ptr(row, slot as usize, size as usize),
                0,
                size as usize,
                type_code,
            ),
        }
    }

    /// Canonical sign-aware *routing* key for the value in `row` — the form
    /// `partition_for_pk_bytes` and the index routing cache compare on, and the
    /// routing counterpart to [`Self::native_key`]. A PK column widens its OPK
    /// bytes; a payload column OPK-encodes then widens, so equal logical values
    /// route to the same partition whether stored as a PK or a payload column.
    /// Callers must `is_null`-gate first. STRING/BLOB have no order-preserving
    /// routing image (this returns `payload_route_key`'s raw low-8-byte image for
    /// them); a caller routing by string content hashes it before reaching here.
    #[inline]
    pub(crate) fn route_key<'b>(&self, mb: &impl RowView<'b>, row: usize) -> u128 {
        match *self {
            ColumnLocator::Pk { byte_off, size, .. } => {
                pk_route_key(mb.get_pk_bytes(row), byte_off as usize, size as usize)
            }
            ColumnLocator::Payload { slot, size, type_code } => payload_route_key(
                mb.get_col_ptr(row, slot as usize, size as usize),
                0,
                size as usize,
                type_code,
            ),
        }
    }
}

impl SchemaDescriptor {
    /// Byte width of the leading `n` columns. For an index schema this is the
    /// OPK leading-key span width (`idx_key_size`) — the sum of every promoted
    /// column's width, never just `columns[0]` (a composite `UNIQUE (a, b)`
    /// span can exceed 16 bytes); the source-PK suffix begins there.
    #[inline]
    pub(crate) fn leading_key_size(&self, n: usize) -> usize {
        self.columns[..n].iter().map(|c| c.size() as usize).sum()
    }
}

/// Precomputed read/encode plan for one index's OPK leading-key span: per
/// indexed column, the owner-side read coordinate (PK-or-payload, resolved via
/// `locate` — a PK source column is sliced from the packed OPK PK region, a
/// payload column read from its dense slot) and the promoted index column it
/// is encoded at. Built once per circuit so the row paths do no catalog
/// reborrow, schema indexing, or allocation; `Copy`, so descriptors carrying
/// it stay allocation-free.
///
/// The span is the single definition of "what key do this row's indexed
/// columns map to", shared by every uniqueness-enforcement site (in-batch
/// validator, backfill dedup via `batch_project_index`, broadcast-skip filter,
/// insert-time check, pre-flight) — byte-equal ⟺ index-value equal at any
/// width, and byte-lexicographic order is the seek/merge order.
#[derive(Clone, Copy)]
pub(crate) struct IndexKeySpec {
    n: u8,
    locators: [ColumnLocator; gnitz_wire::PK_LIST_MAX_COLS],
    idx_cols: [SchemaColumn; gnitz_wire::PK_LIST_MAX_COLS],
}

impl IndexKeySpec {
    /// `cols` is the circuit's source column list (owner-schema indices);
    /// `idx_schema` supplies the promoted leading columns the span encodes at.
    pub(crate) fn new(cols: &[u32], owner: &SchemaDescriptor, idx_schema: &SchemaDescriptor) -> Self {
        debug_assert!(!cols.is_empty() && cols.len() <= gnitz_wire::PK_LIST_MAX_COLS);
        let mut locators = [ColumnLocator::Pk {
            byte_off: 0,
            size: 0,
            type_code: 0,
        }; gnitz_wire::PK_LIST_MAX_COLS];
        let mut idx_cols = [SchemaColumn::new(0, 0); gnitz_wire::PK_LIST_MAX_COLS];
        for (i, &c) in cols.iter().enumerate() {
            locators[i] = owner.locate(c as usize);
            idx_cols[i] = idx_schema.columns[i];
        }
        IndexKeySpec {
            n: cols.len() as u8,
            locators,
            idx_cols,
        }
    }

    /// The promoted index columns of the leading span, in index order.
    #[inline]
    pub(crate) fn idx_cols(&self) -> &[SchemaColumn] {
        &self.idx_cols[..self.n as usize]
    }

    /// Span width (`idx_key_size`); see `SchemaDescriptor::leading_key_size`.
    #[inline]
    pub(crate) fn key_size(&self) -> usize {
        self.idx_cols().iter().map(|c| c.size() as usize).sum()
    }

    /// Write one row's OPK leading-key span into `dst[..key_size()]`. Returns
    /// `false` (skip — the row is not indexed, `dst` partially written) when ANY
    /// indexed column is NULL: SQL NULL-distinctness, a row with a NULL in any
    /// indexed column never collides. The per-column encode is byte-identical
    /// to the seek-side [`Self::seek_prefix`], so the in-memory key, the
    /// projected index entry, and the seek prefix agree by construction.
    ///
    /// Each column encodes through `encode_pk_column_promoted`, sign-extending a
    /// signed source from its native width before OPK-encoding at the promoted
    /// index column: the span is order-preserving for every type (a signed source
    /// promotes to a signed `I64`/`I128` index column whose sign-flip puts
    /// negatives below non-negatives), and equality-correct (equal logical values
    /// pack byte-identically regardless of source/target width). A column whose
    /// source already matches the index type (`U128`/`UUID`, base unsigned ≤8B)
    /// reduces to `encode_pk_column`.
    pub(crate) fn write_span<'b>(&self, mb: &impl RowView<'b>, row: usize, dst: &mut [u8]) -> bool {
        let mut off = 0;
        for (loc, col) in self.locators[..self.n as usize].iter().zip(self.idx_cols()) {
            if loc.is_null(mb, row) {
                return false;
            }
            let src_w = loc.size(); // source column width
            let target_w = col.size() as usize; // promoted index column width
            let native = loc.native_key(mb, row); // zero-extended native LE in u128
                                                  // Sign-extends a signed source / zero-extends an unsigned source from
                                                  // `src_w`, then OPK-encodes at the promoted target. `src_w == target_w`
                                                  // (no promotion, e.g. U128) reduces to `encode_pk_column`.
            gnitz_wire::encode_pk_column_promoted(
                &native.to_le_bytes()[..src_w],
                loc.type_code(),
                col.type_code,
                &mut dst[off..off + target_w],
            );
            off += target_w;
        }
        true
    }

    /// `write_span` into a caller-reused `PkBuf` — no intermediate stack
    /// buffer, no `from_bytes` re-copy (this runs in the backfill scan and on
    /// every insert). Maintains `PkBuf`'s "tail past `len` is zero" invariant
    /// (zeroing only when this key is narrower than the previous one in the
    /// reused scratch — free in the common same-circuit loop), so callers may
    /// slice `out.bytes[..stride]` as the span zero-padded to any wider stride.
    /// A NULL-skipped row returns `false` with `out` unchanged in meaning.
    pub(crate) fn key_bytes<'b>(&self, mb: &impl RowView<'b>, row: usize, out: &mut key::PkBuf) -> bool {
        if !self.write_span(mb, row, &mut out.bytes) {
            return false;
        }
        let len = self.key_size();
        if (out.len as usize) > len {
            out.bytes[len..out.len as usize].fill(0);
        }
        out.len = len as u8;
        true
    }

    /// Seek-side counterpart of [`Self::write_span`]: OPK-encode native key
    /// values (zero-extended, as `pk_native_key`/`payload_native_key` produce
    /// them) into the leading-key prefix, returning the buffer and the filled
    /// prefix length. For a leading-prefix seek, build the spec over only the
    /// supplied columns. Each column makes the same `encode_pk_column_promoted`
    /// call as `write_span`, so the seek prefix matches the projected entries
    /// by construction. Bytes past the prefix stay zero (the source-PK suffix
    /// is not part of the leading-column prefix).
    pub(crate) fn seek_prefix(&self, natives: &[u128]) -> ([u8; MAX_PK_BYTES], usize) {
        debug_assert_eq!(
            natives.len(),
            self.n as usize,
            "seek_prefix: one native value per spec column"
        );
        let mut opk = [0u8; MAX_PK_BYTES];
        let mut off = 0;
        for (native, (loc, col)) in natives
            .iter()
            .zip(self.locators[..self.n as usize].iter().zip(self.idx_cols()))
        {
            let size = col.size() as usize;
            gnitz_wire::encode_pk_column_promoted(
                &native.to_le_bytes()[..loc.size()],
                loc.type_code(),
                col.type_code,
                &mut opk[off..off + size],
            );
            off += size;
        }
        (opk, off)
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

pub(crate) const fn type_size(tc: u8) -> u8 {
    gnitz_wire::wire_stride(tc) as u8
}

// ---------------------------------------------------------------------------
// Row-format utilities
// ---------------------------------------------------------------------------

#[inline(always)]
pub(crate) fn read_signed(bytes: &[u8], size: usize) -> i64 {
    debug_assert!(
        bytes.len() >= size,
        "read_signed: buffer too short ({} < {})",
        bytes.len(),
        size
    );
    match size {
        1 => bytes[0] as i8 as i64,
        2 => i16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        _ => unreachable!("read_signed: unexpected size {size}"),
    }
}

#[inline(always)]
pub(crate) fn read_unsigned(bytes: &[u8], size: usize) -> u64 {
    debug_assert!(
        bytes.len() >= size,
        "read_unsigned: buffer too short ({} < {})",
        bytes.len(),
        size
    );
    match size {
        1 => bytes[0] as u64,
        2 => u16::from_le_bytes(bytes[..2].try_into().unwrap()) as u64,
        4 => u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64,
        8 => u64::from_le_bytes(bytes[..8].try_into().unwrap()),
        _ => unreachable!("read_unsigned: unexpected size {size}"),
    }
}

// Two distinct key spaces derive a `u128` from a column. They coincide for
// unsigned types and differ for signed:
//
// * ROUTING (`*_route_key`): the canonical `widen_pk_be(OPK)` value — sign-
//   flipped for signed. Used by exchange/`extract_group_key`, matching
//   `partition_for_pk_bytes`, which is schema-less and *cannot* decode, so it
//   must hash the OPK bytes' widened value. Both sides of a distributed join
//   agree only in this space.
// * INDEX (`*_native_key`): the native value (signed integers keep their
//   two's-complement bits, zero-extended). Used by FK validation, unique-index
//   maintenance, `has_pk`, and `seek_by_index`, which all re-encode native →
//   OPK at the storage boundary (`Table::opk_key`, `batch_project_index`), so
//   they need the native value back, not the sign-flipped one.

/// ROUTING key for one PK column's OPK bytes (canonical / sign-flipped).
/// `col_size` is the addressed column's width (≤ 16); `offset` its byte offset
/// within the PK region (0 for a lone PK).
#[inline]
pub(crate) fn pk_route_key(pk_bytes: &[u8], offset: usize, col_size: usize) -> u128 {
    debug_assert!(
        pk_bytes.len() >= offset + col_size,
        "pk_route_key: buffer too short ({} < {})",
        pk_bytes.len(),
        offset + col_size,
    );
    gnitz_wire::widen_pk_be(&pk_bytes[offset..offset + col_size], col_size)
}

/// ROUTING key for one native little-endian payload column (canonical). Integer
/// columns are OPK-encoded (signed sign-flip) then widened, so a payload FK
/// column routes to the same partition as the same value stored as a PK column.
/// U128/UUID are unsigned (OPK == native). Float/String/Blob have no PK
/// counterpart; they keep a zero-extended low-8-byte key.
#[inline]
pub(crate) fn payload_route_key(col_data: &[u8], offset: usize, col_size: usize, type_code_val: u8) -> u128 {
    debug_assert!(
        col_data.len() >= offset + col_size,
        "payload_route_key: buffer too short ({} < {})",
        col_data.len(),
        offset + col_size,
    );
    let src = &col_data[offset..offset + col_size];
    match TypeCode::from_validated_u8(type_code_val) {
        TypeCode::U128 | TypeCode::UUID => u128::from_le_bytes(src.try_into().unwrap()),
        TypeCode::U8
        | TypeCode::I8
        | TypeCode::U16
        | TypeCode::I16
        | TypeCode::U32
        | TypeCode::I32
        | TypeCode::U64
        | TypeCode::I64
        | TypeCode::I128 => {
            let mut opk = [0u8; 16];
            gnitz_wire::encode_pk_column(src, type_code_val, &mut opk[..col_size]);
            gnitz_wire::widen_pk_be(&opk[..col_size], col_size)
        }
        TypeCode::F32 | TypeCode::F64 | TypeCode::String | TypeCode::Blob => {
            let mut bytes = [0u8; 8];
            let copy_len = col_size.min(8);
            bytes[..copy_len].copy_from_slice(&src[..copy_len]);
            u64::from_le_bytes(bytes) as u128
        }
    }
}

/// INDEX key for one PK column's OPK bytes: decode back to the native value
/// (signed bits preserved), zero-extended to `u128`. Feeds `has_pk` /
/// `seek_by_index`, which re-encode native → OPK to hit the OPK-stored index.
/// `offset + col_size` must lie within the OPK PK region (`pk_bytes`); a
/// mismatched `col_size` slices past the column and panics.
#[inline]
pub(crate) fn pk_native_key(pk_bytes: &[u8], offset: usize, col_size: usize, type_code_val: u8) -> u128 {
    debug_assert!(
        pk_bytes.len() >= offset + col_size,
        "pk_native_key: buffer too short ({} < {})",
        pk_bytes.len(),
        offset + col_size,
    );
    let mut le = [0u8; 16];
    gnitz_wire::decode_pk_column(&pk_bytes[offset..offset + col_size], type_code_val, &mut le[..col_size]);
    u128::from_le_bytes(le)
}

/// INDEX key for one native little-endian payload column: the native value,
/// zero-extended. U128/UUID read all 16 bytes; narrower types zero-extend the
/// low ≤8 bytes. Float/String/Blob keep the same zero-extended low-8-byte key.
#[inline]
pub(crate) fn payload_native_key(col_data: &[u8], offset: usize, col_size: usize, type_code_val: u8) -> u128 {
    debug_assert!(
        col_data.len() >= offset + col_size,
        "payload_native_key: buffer too short ({} < {})",
        col_data.len(),
        offset + col_size,
    );
    match TypeCode::from_validated_u8(type_code_val) {
        TypeCode::U128 | TypeCode::UUID | TypeCode::I128 => {
            u128::from_le_bytes(col_data[offset..offset + 16].try_into().unwrap())
        }
        _ => {
            let mut bytes = [0u8; 8];
            let copy_len = col_size.min(8);
            bytes[..copy_len].copy_from_slice(&col_data[offset..offset + copy_len]);
            u64::from_le_bytes(bytes) as u128
        }
    }
}

/// OPK-encode a native index-key value into the index's leading-column bytes,
/// for a prefix seek or a check-batch composite PK — the scalar sibling of
/// [`IndexKeySpec::seek_prefix`] for callers whose source column lives in
/// another table's schema (FK probes). `src_type` is the *source* column type
/// (the value in `native` is zero-extended): a signed source sign-extends from
/// its native width before OPK-encoding at the promoted `idx_key_type`,
/// byte-identical to the write-side `IndexKeySpec::write_span`. Bytes beyond
/// the leading column stay zero (the source-PK suffix is not part of the
/// leading-column prefix).
#[inline]
pub(crate) fn index_opk_prefix(native: u128, src_type: u8, idx_key_type: u8) -> [u8; MAX_PK_BYTES] {
    let mut opk = [0u8; MAX_PK_BYTES];
    let src_w = gnitz_wire::wire_stride(src_type);
    let idx_w = gnitz_wire::wire_stride(idx_key_type);
    gnitz_wire::encode_pk_column_promoted(
        &native.to_le_bytes()[..src_w],
        src_type,
        idx_key_type,
        &mut opk[..idx_w],
    );
    opk
}

/// Prepare the 16-byte German string output struct for a copy operation.
/// Fills length, prefix, and (for short strings) inline suffix.
/// For long strings dest[8..16] is left as zero — the caller must resolve
/// the blob data and write the new offset in.
/// Returns (dest_struct, is_long_string).
#[inline]
fn prep_german_string_copy(src: &[u8]) -> ([u8; 16], bool) {
    debug_assert!(
        src.len() >= 16,
        "prep_german_string_copy: src must be a 16-byte German string struct"
    );
    let length = u32::from_le_bytes(src[0..4].try_into().unwrap()) as usize;
    let mut dest = [0u8; 16];
    dest[0..4].copy_from_slice(&src[0..4]);
    dest[4..8].copy_from_slice(&src[4..8]);
    if length <= SHORT_STRING_THRESHOLD {
        let suffix_len = length.saturating_sub(4);
        if suffix_len > 0 {
            dest[8..8 + suffix_len].copy_from_slice(&src[8..8 + suffix_len]);
        }
        (dest, false)
    } else {
        (dest, true)
    }
}

/// Identity-keyed dedup cache for `relocate_german_string_vec`.
///
/// Key: `(src_blob.as_ptr() as usize, old_offset, length)`. The same source
/// span is copied at most once per merge — the cached value is the offset
/// inside the destination blob where the bytes were appended.
pub(crate) type BlobCache = FxHashMap<(usize, usize, usize), usize>;

/// Copy a 16-byte German string cell and (for long strings) migrate the
/// out-of-line payload from `src_blob` into `dst_blob`.
///
/// The returned 16-byte cell is ready to write into the output column buffer.
/// Short strings (≤ SHORT_STRING_THRESHOLD) are copied inline — `src_blob` is
/// unused for them.
///
/// When `cache` is `Some`, the appended blob data is deduplicated by
/// `(src_blob.as_ptr(), old_offset, length)` — i.e. the same source span is
/// only copied once per merge.
///
/// **Malformed-input fallback:** if the long-string header declares a region
/// `[old_offset, old_offset + length)` that overruns `src_blob.len()`, the
/// cell is rewritten to an empty string (length = 0) rather than triggering
/// an out-of-bounds read. This keeps trusted in-memory callers panic-free
/// even when fed corrupted data that slipped past validation.
pub(crate) fn relocate_german_string_vec(
    src_cell: &[u8],
    src_blob: &[u8],
    dst_blob: &mut Vec<u8>,
    cache: Option<&mut BlobCache>,
) -> [u8; 16] {
    let (mut dest, is_long) = prep_german_string_copy(src_cell);
    if !is_long {
        return dest;
    }
    let length = u32::from_le_bytes(src_cell[0..4].try_into().unwrap()) as usize;
    let old_offset = u64::from_le_bytes(src_cell[8..16].try_into().unwrap()) as usize;
    if old_offset.saturating_add(length) > src_blob.len() {
        // Malformed: emit an empty string. Zero the full inline header so no
        // stale prefix bytes remain. dest[8..16] is already 0 (prep_german_string_copy
        // zero-initialized the struct and left the long-string offset as 0).
        dest[0..8].copy_from_slice(&0u64.to_le_bytes());
        return dest;
    }
    let new_offset = dst_blob.len();
    let off = match cache {
        Some(cache) => {
            let key = (src_blob.as_ptr() as usize, old_offset, length);
            *cache.entry(key).or_insert_with(|| {
                dst_blob.extend_from_slice(&src_blob[old_offset..old_offset + length]);
                new_offset
            })
        }
        None => {
            dst_blob.extend_from_slice(&src_blob[old_offset..old_offset + length]);
            new_offset
        }
    };
    dest[8..16].copy_from_slice(&(off as u64).to_le_bytes());
    dest
}

pub(crate) use gnitz_wire::encode_german_string;
pub(crate) use gnitz_wire::try_decode_german_string;

/// Bytes `[heap_offset, heap_offset + length)` of a long German string's blob
/// payload, or `&[]` if the header overruns `blob`. Mirrors
/// `relocate_german_string_vec`'s overrun guard: corrupt data that slipped past
/// validation degrades to an empty payload rather than slicing OOB in release.
/// A corrupt long string then hashes identically (`checksum(&[])`) at every
/// call site, so routing stays deterministic.
#[inline]
pub(crate) fn long_string_bytes(blob: &[u8], heap_offset: usize, length: usize) -> &[u8] {
    if heap_offset.saturating_add(length) > blob.len() {
        &[]
    } else {
        &blob[heap_offset..heap_offset + length]
    }
}

/// Full logical content bytes of a German string struct `s` (16-byte layout:
/// `[0..4]` = length, then inline-or-heap content). Short strings
/// (len ≤ SHORT_STRING_THRESHOLD) store content inline at `[4..4+length]`;
/// long strings live in `blob` at the heap offset. Unlike `german_string_tail`,
/// this returns the complete content with no prefix skip — for hashing or
/// whole-value equality. A zero-length string yields `&[]`.
#[inline]
pub(crate) fn german_string_content<'a>(s: &'a [u8], blob: &'a [u8]) -> &'a [u8] {
    let length = read_u32_le(s, 0) as usize;
    if length == 0 {
        &[]
    } else if length <= SHORT_STRING_THRESHOLD {
        &s[4..4 + length]
    } else {
        let heap_offset = read_u64_le(s, 8) as usize;
        long_string_bytes(blob, heap_offset, length)
    }
}

/// Returns bytes [4..end] of a German string as a contiguous slice.
/// Short strings (len ≤ SHORT_STRING_THRESHOLD): inline at struct[8..4+end].
/// Long strings: full string is in blob at heap_offset; skip first 4 bytes
/// (they duplicate the prefix, already compared by the caller).
#[inline]
pub(crate) fn german_string_tail<'a>(s: &'a [u8], blob: &'a [u8], length: usize, end: usize) -> &'a [u8] {
    if length <= SHORT_STRING_THRESHOLD {
        debug_assert!(s.len() >= 4 + end, "german_string_tail: short string struct too small");
        &s[8..4 + end]
    } else {
        let heap_offset = read_u64_le(s, 8) as usize;
        let start = heap_offset.saturating_add(4);
        let limit = heap_offset.saturating_add(end);
        debug_assert!(
            limit <= blob.len(),
            "german_string_tail: long string [{start}..{limit}) overruns blob (len={})",
            blob.len(),
        );
        // `end ≥ min_len > 4` at the one call site (compare_german_strings),
        // so `start < limit`; if `limit ≤ blob.len()` the slice is in bounds.
        if limit > blob.len() {
            &[]
        } else {
            &blob[start..limit]
        }
    }
}

#[inline(always)]
pub(crate) fn compare_german_strings(a: &[u8], blob_a: &[u8], b: &[u8], blob_b: &[u8]) -> std::cmp::Ordering {
    let len_a = read_u32_le(a, 0) as usize;
    let len_b = read_u32_le(b, 0) as usize;
    let min_len = len_a.min(len_b);

    // Fixed 4-byte prefix comparison — one register compare, no runtime-length
    // memcmp. Valid because every cell zero-pads the prefix bytes beyond its
    // length (`encode_german_string` starts from a zeroed struct): the first
    // differing padded byte is either a real content difference or a longer
    // string's non-zero byte against the shorter's zero pad, and both order
    // exactly as the truncated-compare-then-length-tiebreak below would.
    let pfx_a = u32::from_be_bytes(a[4..8].try_into().unwrap());
    let pfx_b = u32::from_be_bytes(b[4..8].try_into().unwrap());
    if pfx_a != pfx_b {
        return pfx_a.cmp(&pfx_b);
    }
    if min_len <= 4 {
        return len_a.cmp(&len_b);
    }

    // Bulk suffix comparison — vectorised memcmp via [u8]::cmp.
    let tail_a = german_string_tail(a, blob_a, len_a, min_len);
    let tail_b = german_string_tail(b, blob_b, len_b, min_len);
    match tail_a.cmp(tail_b) {
        std::cmp::Ordering::Equal => len_a.cmp(&len_b),
        ord => ord,
    }
}

/// Validate that a peer-supplied schema descriptor matches the expected one:
/// column count, PK column indices, per-column type codes and nullability.
/// Used at every trust boundary where rows are decoded against a descriptor
/// the sender chose (client INSERT frames, worker reply trains) — batch append
/// helpers do not validate shape, so an unguarded mismatch turns into
/// misinterpreted bytes handed onward.
pub(crate) fn validate_schema_match(wire: &SchemaDescriptor, expected: &SchemaDescriptor) -> Result<(), String> {
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
    Ok(())
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
    let n = promoted.len();
    let arity = n + src_pk.len();
    let mut cols: Vec<SchemaColumn> = Vec::with_capacity(arity);
    let mut pk_indices: Vec<u32> = Vec::with_capacity(arity);
    for (i, &t) in promoted.iter().enumerate() {
        cols.push(SchemaColumn::new(t, 0));
        pk_indices.push(i as u32);
    }
    for (j, &ci) in src_pk.iter().enumerate() {
        cols.push(SchemaColumn::new(source.columns[ci as usize].type_code, 0));
        pk_indices.push((n + j) as u32);
    }
    Ok(SchemaDescriptor::new(&cols, &pk_indices))
}

/// Wire schema for the GET_INDICES descriptor list: `(packed_cols PK, is_unique)`.
/// The PK carries `pack_pk_cols(&col_indices)` — unique per circuit (circuits
/// dedup by column list), so a valid PK. The server ships this block on the data
/// path; the client decodes against the wire schema and reads columns by position.
pub(crate) fn index_meta_schema_desc() -> SchemaDescriptor {
    let u64c = SchemaColumn::new(type_code::U64, 0);
    SchemaDescriptor::new(&[u64c, u64c], &[0]) // [packed_cols (PK), is_unique]
}
pub(crate) const INDEX_META_COL_NAMES: [&[u8]; 2] = [b"cols", b"is_unique"];

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
    let mut cols: Vec<SchemaColumn> = Vec::with_capacity(schema.pk_indices().len() + project.len());
    let mut pk_idx: Vec<u32> = Vec::with_capacity(schema.pk_indices().len());
    for (_, _, col) in schema.pk_columns() {
        pk_idx.push(cols.len() as u32);
        cols.push(*col);
    }
    for &p in project {
        debug_assert!(
            !schema.is_pk_col(p as usize),
            "project_schema: projected column {p} is a PK column"
        );
        cols.push(schema.columns[p as usize]);
    }
    SchemaDescriptor::new(&cols, &pk_idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::pk_only_schema;

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

    // Claim 6: the malformed-long-string fallback must zero both the length
    // field (dest[0..4]) AND the prefix field (dest[4..8]).  Before the fix,
    // dest[4..8] was left containing the garbage bytes copied from the corrupt
    // source cell.
    #[test]
    fn test_malformed_long_string_fallback_clean_header() {
        let mut src_cell = [0u8; 16];
        // length = 20  (> SHORT_STRING_THRESHOLD = 12)
        src_cell[0..4].copy_from_slice(&20u32.to_le_bytes());
        // prefix = 0xDEADBEEF  (stale garbage that must be zeroed on fallback)
        src_cell[4..8].copy_from_slice(&0xDEAD_BEEF_u32.to_le_bytes());
        // heap_offset = 999  (well beyond src_blob)
        src_cell[8..16].copy_from_slice(&999u64.to_le_bytes());

        let src_blob = vec![0u8; 4]; // far too small
        let mut dst_blob: Vec<u8> = Vec::new();

        let result = relocate_german_string_vec(&src_cell, &src_blob, &mut dst_blob, None);

        assert_eq!(
            u32::from_le_bytes(result[0..4].try_into().unwrap()),
            0,
            "fallback must emit length=0",
        );
        assert_eq!(&result[4..8], &[0u8; 4], "fallback must zero the prefix field");
        assert_eq!(&result[8..16], &[0u8; 8], "fallback must leave blob offset zero");
        assert!(dst_blob.is_empty(), "fallback must not extend dst_blob");
    }

    #[test]
    fn test_long_string_bytes_overrun_returns_empty() {
        let blob = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        // In-bounds window returns the exact slice.
        assert_eq!(long_string_bytes(&blob, 2, 3), &[3u8, 4, 5]);
        // heap_offset + length past the end degrades to empty, no panic.
        assert_eq!(long_string_bytes(&blob, 6, 10), &[] as &[u8]);
        // heap_offset itself past the end.
        assert_eq!(long_string_bytes(&blob, 99, 1), &[] as &[u8]);
        // Saturating add: length near usize::MAX must not wrap to in-bounds.
        assert_eq!(long_string_bytes(&blob, 4, usize::MAX), &[] as &[u8]);
        // Exact fit is in bounds.
        assert_eq!(long_string_bytes(&blob, 0, 8), &blob[..]);
    }

    /// Pins the zero-padded-cell invariant the fixed 4-byte u32 prefix compare
    /// relies on: with `min_len < 4`, two cells differing only past `min_len`
    /// must order by length (the shorter's prefix pad bytes are zero, the
    /// longer's content byte is what makes the u32s differ) — exactly the
    /// truncated-compare-then-length-tiebreak result.
    #[test]
    fn test_compare_german_strings_short_prefix_zero_padding() {
        use std::cmp::Ordering;
        let mut blob = Vec::new();
        let cell = |s: &str, blob: &mut Vec<u8>| encode_german_string(s.as_bytes(), blob);
        let a = cell("ab", &mut blob); // min_len 2 < 4
        let b = cell("abc", &mut blob); // differs only at byte 2 (past min_len)
        assert_eq!(compare_german_strings(&a, &blob, &b, &blob), Ordering::Less);
        assert_eq!(compare_german_strings(&b, &blob, &a, &blob), Ordering::Greater);
        // Embedded NUL past min_len aliases the shorter cell's zero padding in
        // the u32 image; the length tiebreak must still order them.
        let c = cell("ab\0", &mut blob);
        assert_eq!(compare_german_strings(&a, &blob, &c, &blob), Ordering::Less);
        assert_eq!(compare_german_strings(&c, &blob, &a, &blob), Ordering::Greater);
        // And equal content stays Equal.
        let a2 = cell("ab", &mut blob);
        assert_eq!(compare_german_strings(&a, &blob, &a2, &blob), Ordering::Equal);
    }

    // The german_string_tail / compare_german_strings overrun fallback is
    // release-only: in debug the `debug_assert!` fires loudly (intended), so
    // these tests run only when debug assertions are off.
    #[cfg(not(debug_assertions))]
    #[test]
    fn test_german_string_tail_overrun_returns_empty() {
        // Long-string struct: length=20 (> SHORT_STRING_THRESHOLD), heap_offset
        // far beyond a tiny blob. Release path must return &[] not slice OOB.
        let mut s = [0u8; 16];
        s[0..4].copy_from_slice(&20u32.to_le_bytes());
        s[8..16].copy_from_slice(&999u64.to_le_bytes());
        let blob = vec![0u8; 4];
        // `end` = full length (the compare_german_strings call shape).
        assert_eq!(german_string_tail(&s, &blob, 20, 20), &[] as &[u8]);
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn test_compare_german_strings_corrupt_long_cell_no_panic() {
        // Two long-string cells with out-of-range heap offsets. Both tails
        // degrade to empty, so the comparison resolves on length/prefix
        // instead of panicking on an OOB blob slice.
        let mut a = [0u8; 16];
        a[0..4].copy_from_slice(&20u32.to_le_bytes());
        a[4..8].copy_from_slice(b"abcd");
        a[8..16].copy_from_slice(&500u64.to_le_bytes());
        let mut b = a;
        b[8..16].copy_from_slice(&900u64.to_le_bytes());
        let blob = vec![0u8; 4];
        // Equal length + equal prefix + empty tails ⇒ Equal, and crucially no panic.
        assert_eq!(compare_german_strings(&a, &blob, &b, &blob), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_relocate_german_string_vec_cache_hit_dedups() {
        // Long-string source: length=20, payload at offset 0 in src_blob.
        let payload: &[u8] = b"hello world test dat";
        assert_eq!(payload.len(), 20);
        let src_blob: Vec<u8> = payload.to_vec();

        let mut src_cell = [0u8; 16];
        src_cell[0..4].copy_from_slice(&20u32.to_le_bytes());
        src_cell[4..8].copy_from_slice(&payload[0..4]);
        src_cell[8..16].copy_from_slice(&0u64.to_le_bytes());

        let mut dst_blob: Vec<u8> = Vec::new();
        let mut cache: BlobCache = BlobCache::default();

        let r1 = relocate_german_string_vec(&src_cell, &src_blob, &mut dst_blob, Some(&mut cache));
        let after_first = dst_blob.len();
        assert_eq!(after_first, 20, "first call must append payload exactly once");

        let r2 = relocate_german_string_vec(&src_cell, &src_blob, &mut dst_blob, Some(&mut cache));
        assert_eq!(dst_blob.len(), 20, "cache hit must not append a second copy");
        assert_eq!(&r1[8..16], &r2[8..16], "both calls must return the same offset");
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
        assert_eq!(s.pk_index_single(), 0);
        assert_eq!(s.columns[0].type_code, type_code::U64);
        assert_eq!(s.columns[1].type_code, type_code::I64);
        assert_eq!(s.columns[2].type_code, type_code::STRING);

        // Trailing slot fill: SchemaColumn::new(0, 0) resolves via wire_stride(0)
        // → 8 (default arm). Locked in so future regressions in the trailing
        // representation are caught.
        assert_eq!(s.columns[3].type_code, 0);
        assert_eq!(s.columns[3].size(), 8);
        assert_eq!(s.columns[MAX_COLUMNS - 1].size(), 8);

        // payload_columns() walks non-PK indices in logical order.
        let payload: Vec<usize> = s.payload_columns().map(|(_, ci, _)| ci).collect();
        assert_eq!(payload, vec![1, 2]);
        assert_eq!(s.try_payload_idx(1), Some(0));
        assert_eq!(s.try_payload_idx(2), Some(1));
        // The PK column has no payload slot.
        assert_eq!(s.try_payload_idx(0), None);

        // Non-zero pk_index round-trips (use I64 col at index 1, not STRING).
        let s2 = SchemaDescriptor::new(&cols, &[1]);
        assert_eq!(s2.pk_index_single(), 1);

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
    fn test_locate_pk_and_payload_variants() {
        // Compound narrow PK (U32, U32) + an I64 payload: a PK column resolves
        // to the OPK byte offset within the packed region, a payload column to
        // its dense slot. Both carry the column's width and type.
        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        assert_eq!(
            s.locate(0),
            ColumnLocator::Pk {
                byte_off: 0,
                size: 4,
                type_code: type_code::U32
            },
        );
        assert_eq!(
            s.locate(1),
            ColumnLocator::Pk {
                byte_off: 4,
                size: 4,
                type_code: type_code::U32
            },
        );
        assert_eq!(
            s.locate(2),
            ColumnLocator::Payload {
                slot: 0,
                size: 8,
                type_code: type_code::I64
            },
        );
        // Accessors agree with the variant fields.
        assert_eq!(s.locate(2).size(), 8);
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
    fn test_num_payload_cols() {
        // 2-column schema → 1 payload column.
        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        assert_eq!(s.num_payload_cols(), 1);

        // pk_index not at column 0 → same answer (num_columns - pk_indices().len()).
        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[2],
        );
        assert_eq!(s.num_payload_cols(), 3);
    }

    #[test]
    fn test_num_columns() {
        let s = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        assert_eq!(s.num_columns(), 1);

        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        assert_eq!(s.num_columns(), 3);

        let s = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0); 8], &[0]);
        assert_eq!(s.num_columns(), 8);
    }

    #[test]
    fn test_pk_columns_single_pk() {
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U128, 0),
        ];
        let s = SchemaDescriptor::new(&cols, &[1]);
        let v: Vec<(usize, usize, u8)> = s.pk_columns().map(|(ord, ci, c)| (ord, ci, c.type_code)).collect();
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

    #[test]
    #[should_panic(expected = "STRING and BLOB columns cannot be PK columns")]
    fn test_string_pk_rejected() {
        let cols = [SchemaColumn::new(type_code::STRING, 0)];
        SchemaDescriptor::new(&cols, &[0]);
    }

    #[test]
    #[should_panic(expected = "STRING and BLOB columns cannot be PK columns")]
    fn test_blob_pk_rejected() {
        let cols = [SchemaColumn::new(type_code::BLOB, 0)];
        SchemaDescriptor::new(&cols, &[0]);
    }

    #[test]
    #[should_panic(expected = "F32 and F64 columns cannot be PK columns")]
    fn test_f32_pk_rejected() {
        let cols = [SchemaColumn::new(type_code::F32, 0)];
        SchemaDescriptor::new(&cols, &[0]);
    }

    #[test]
    #[should_panic(expected = "F32 and F64 columns cannot be PK columns")]
    fn test_f64_pk_rejected() {
        let cols = [SchemaColumn::new(type_code::F64, 0)];
        SchemaDescriptor::new(&cols, &[0]);
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
        let v: Vec<(usize, usize, u8)> = s.pk_columns().map(|(ord, ci, c)| (ord, ci, c.type_code)).collect();
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
        let collected: Vec<(usize, usize)> = s.pk_columns().map(|(ord, ci, _)| (ord, ci)).collect();
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

    #[test]
    #[should_panic(expected = "compound PK not yet supported here")]
    fn test_pk_index_single_panics_on_compound() {
        // Release-active canary the migration policy depends on.
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
        ];
        let s = SchemaDescriptor::new(&cols, &[0, 1]);
        let _ = s.pk_index_single();
    }

    // ── seek_opk_bytes: the width-universal seek-key encoder ─────────────────

    #[test]
    fn seek_opk_bytes_narrow_matches_opk_key() {
        // For every narrow stride (≤ 16) the wire pair degenerates to `(low, &[])`,
        // so `seek_opk_bytes` must be byte-identical to a direct `opk_key` of the
        // native value — both buffer and stride.
        let cases = [
            pk_only_schema(&[type_code::U8]),  // stride 1
            pk_only_schema(&[type_code::U32]), // stride 4
            pk_only_schema(&[type_code::U64]), // stride 8
            pk_only_schema(&[type_code::I64]), // stride 8, signed → OPK flips the sign bit
            // Compound (U32, U32) with a *permuted* PK list [1, 0]: stride 8,
            // exercises the multi-column pk-list walk in the encoder.
            SchemaDescriptor::new(
                &[
                    SchemaColumn::new(type_code::U32, 0),
                    SchemaColumn::new(type_code::U32, 0),
                ],
                &[1, 0],
            ),
        ];
        // Values spanning zero, small, mixed, and a sign-bit-set word (negative
        // for the I64 case) so the sign-flip and byte order are exercised. Both
        // encoders truncate to `stride`, so an over-wide value is a valid probe.
        for s in cases {
            for v in [0u128, 1, 0x0123_4567_89AB_CDEF, 0x8000_0000_0000_0000, u64::MAX as u128] {
                let (want_opk, want_stride) = key::opk_key(&s, &v.to_le_bytes());
                let (got_opk, got_stride) = seek_opk_bytes(&s, v, &[]).expect("narrow seek encodes");
                assert_eq!(got_stride, want_stride, "stride mismatch for {s:?} v={v:#x}");
                assert_eq!(
                    got_opk[..got_stride],
                    want_opk[..want_stride],
                    "OPK bytes mismatch for {s:?} v={v:#x}",
                );
            }
        }
    }

    #[test]
    fn seek_opk_bytes_wide_reproduces_hand_built_opk() {
        // (U64, U64, U64) = stride 24, wide. The wire pair carries the first 16
        // native bytes in `low` and the trailing U64 in `extra`, exactly as
        // `PkTuple::split_wire` packs them. All-unsigned ⇒ OPK is each column's
        // big-endian image, so the expected key is built by hand.
        let s = pk_only_schema(&[type_code::U64; 3]);
        assert_eq!(s.pk_stride(), 24);
        let (a, b, c): (u64, u64, u64) = (0x1122_3344_5566_7788, 0x99AA_BBCC_DDEE_FF00, 0x0102_0304_0506_0708);
        // Native LE image = [a_LE, b_LE, c_LE]; split_wire's `low` is the first 16
        // bytes (a in the low half, b in the high half), `extra` is c's 8 bytes.
        let low = (a as u128) | ((b as u128) << 64);
        let (opk, stride) = seek_opk_bytes(&s, low, &c.to_le_bytes()).expect("wide seek encodes");
        assert_eq!(stride, 24);
        let want: Vec<u8> = a
            .to_be_bytes()
            .into_iter()
            .chain(b.to_be_bytes())
            .chain(c.to_be_bytes())
            .collect();
        assert_eq!(&opk[..stride], want.as_slice());
    }

    #[test]
    fn seek_opk_bytes_missing_extra_errs_not_panics() {
        // A wide stride needs `stride - 16` extra bytes; too few must return Err,
        // never panic — the runtime guard the two dispatch sites rely on.
        let s = pk_only_schema(&[type_code::U64; 3]);
        assert!(seek_opk_bytes(&s, 0, &[]).is_err(), "stride 24 with no extra must Err");
        assert!(seek_opk_bytes(&s, 0, &[0u8; 7]).is_err(), "7 < 8 extra bytes must Err");
        assert!(
            seek_opk_bytes(&s, 0, &[0u8; 8]).is_ok(),
            "exactly 8 extra bytes is enough"
        );
    }

    #[test]
    fn seek_opk_bytes_four_u128_ceiling() {
        // The widest SQL-reachable PK is 4 columns (PK_LIST_MAX_COLS); 4×U128 =
        // stride 64 exercises the `le[16..16 + needed]` copy at its ceiling
        // (`needed == 48`). All-unsigned ⇒ OPK is each column's BE image.
        // Column 0 rides in `low`; columns 1..4 (48 bytes) in `extra`.
        let s = pk_only_schema(&[type_code::U128; 4]);
        assert_eq!(s.pk_stride(), 64);
        let vals: [u128; 4] = [
            0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10,
            0x1112_1314_1516_1718_191A_1B1C_1D1E_1F20,
            0x2122_2324_2526_2728_292A_2B2C_2D2E_2F30,
            0x3132_3334_3536_3738_393A_3B3C_3D3E_3F40,
        ];
        let extra: Vec<u8> = vals[1..].iter().flat_map(|v| v.to_le_bytes()).collect();
        let (opk, stride) = seek_opk_bytes(&s, vals[0], &extra).expect("4×U128 encodes");
        assert_eq!(stride, 64);
        let want: Vec<u8> = vals.iter().flat_map(|v| v.to_be_bytes()).collect();
        assert_eq!(&opk[..stride], want.as_slice());
    }

    #[test]
    fn read_unsigned_zero_extends() {
        // size 1: high-bit-set vs small — must match u8.cmp.
        assert_eq!(read_unsigned(&[0xFF], 1), 0xFF);
        assert_eq!(read_unsigned(&[0x01], 1), 0x01);
        assert!(read_unsigned(&[0xFF], 1) > read_unsigned(&[0x01], 1));

        // size 2: 0xFFFE > 0x0001 as unsigned (sign-extension would invert).
        assert_eq!(read_unsigned(&0xFFFEu16.to_le_bytes(), 2), 0xFFFE);
        assert_eq!(read_unsigned(&0x0001u16.to_le_bytes(), 2), 0x0001);
        assert!(read_unsigned(&0xFFFEu16.to_le_bytes(), 2) > read_unsigned(&0x0001u16.to_le_bytes(), 2),);

        // size 4.
        let big: u32 = 0xFFFF_FFFE;
        let small: u32 = 0x0000_0001;
        assert_eq!(read_unsigned(&big.to_le_bytes(), 4), big as u64);
        assert!(read_unsigned(&big.to_le_bytes(), 4) > read_unsigned(&small.to_le_bytes(), 4),);

        // size 8: full u64 round-trip.
        let v: u64 = 0xDEAD_BEEF_CAFE_BABE;
        assert_eq!(read_unsigned(&v.to_le_bytes(), 8), v);
    }
}
