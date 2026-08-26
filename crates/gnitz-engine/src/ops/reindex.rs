//! PK promotion for reindex: GROUP BY / equijoin / set-op key construction.
//!
//! `op_map`'s reindex paths (in `linear.rs`) and the exchange scatter share
//! these promoters so the reindexed `_join_pk` stored on the trace side and the
//! delta scatter on the producer side co-partition byte-for-byte at every key
//! arity and width. `ReindexPacker` / `german_string_promote_key` are per-row;
//! they stay `#[inline]` and monomorphic so producer and consumer keys agree.

use crate::foundation::xxh::{self, RowHasher};
use crate::schema::{ColumnLocator, DerivedSchema, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::{Batch, MemBatch};

use super::util::{hash_group_col, ieee_order_bits, ieee_order_bits_f32};

/// Set every row's PK to a hash of its full payload content. Identical row
/// content (including null pattern and string/blob bytes) yields an identical
/// 128-bit PK; any difference yields a distinct PK. This implements full-row
/// set membership for EXCEPT/INTERSECT/DISTINCT.
///
/// The canonical byte stream per row is, for each payload column in order: a
/// 1-byte null marker, then (if non-null) the column's content — fixed-width
/// columns by their raw little-endian bytes, STRING/BLOB by length-prefixed
/// content following the heap pointer for long strings. This is independent of
/// physical inline-vs-heap string layout, so equal logical rows hash equally.
///
/// Limitation: set membership is keyed on the 128-bit hash, so two logically
/// distinct rows that collide (~2^-64 birthday-bound) would be treated as the
/// same element and silently coalesce in DISTINCT/EXCEPT/INTERSECT. This is an
/// accepted tradeoff for the synthetic-PK set-op path, not a checked error.
pub(super) fn reindex_hash_row(out_schema: &SchemaDescriptor, output: &mut Batch, branch_id: u8) {
    let n = output.count;
    debug_assert!(
        out_schema.pk_stride() as usize <= gnitz_wire::NARROW_PK_MAX_BYTES,
        "reindex_hash_row: synthetic key stride exceeds NARROW_PK_MAX_BYTES"
    );
    // Hashing borrows the batch immutably and the write-back needs it mutably, so
    // the two cannot interleave per row. Buffering a chunk of keys on the stack
    // keeps both passes in cache and costs no allocation, whatever `n` is.
    const CHUNK: usize = 256;
    let mut keys = [0u128; CHUNK];
    let mut start = 0;
    while start < n {
        let end = (start + CHUNK).min(n);
        {
            let mb = output.as_mem_batch();
            // ~280-byte stack-allocated streaming hasher; `reset()` between rows
            // costs only a handful of word stores, and fixed-width columns are fed
            // straight from the column slot with no intermediate copy.
            let mut hasher = RowHasher::new();
            for row in start..end {
                hasher.reset();
                // Branch discriminator: distinguishes identical payloads arriving
                // on the left vs right side of a UNION ALL so they do not collide
                // to a single PK (which would collapse their +2 weight to +1).
                hasher.update(&[branch_id]);
                let null_word = mb.get_null_word(row);
                for (pi, col) in out_schema.payload_columns() {
                    let is_null = gnitz_wire::null_word_get(null_word, pi);
                    hasher.update(&[is_null as u8]);
                    if is_null {
                        continue;
                    }
                    if gnitz_wire::is_german_string(col.type_code) {
                        let sb = mb.get_col_ptr(row, pi, 16);
                        super::util::hash_german_string_content(&mut hasher, sb, mb.blob);
                    } else {
                        let cs = col.size() as usize;
                        hasher.update(mb.get_col_ptr(row, pi, cs));
                    }
                }
                keys[row - start] = hasher.digest128();
            }
        }
        for row in start..end {
            // Synthetic U128 (unsigned): OPK == big-endian, which `set_pk_at`
            // writes right-aligned into the stride (and debug-checks fits in it).
            output.set_pk_at(row, keys[row - start]);
        }
        start = end;
    }
}

/// Synthetic-PK / routing key for a German-string column's content, used by the
/// free `read_string` helper — which the packer's `String` arm calls to set a
/// reindexed row's `_join_pk`, and which the exchange scatter calls (through the
/// same packer) to route the raw delta. Both go through this one function, so a
/// string join key scatters to the worker that owns its reindexed `_join_pk`
/// partition; otherwise the row is stored on the wrong worker and the view scan
/// misses it. Empty content (including a NULL string, stored as a zeroed
/// German-string struct) hashes to 0.
#[inline]
pub(super) fn german_string_promote_key(struct_bytes: &[u8], blob: &[u8]) -> u128 {
    let content = gnitz_wire::german_string_content(struct_bytes, blob);
    if content.is_empty() {
        return 0; // NULL / empty-string sentinel
    }
    // A true 128-bit content hash: the trace-side reindex and the delta scatter
    // both reach this through the free `read_string` (the packer's `String` arm),
    // so they agree by construction. A 64-bit hash widened to 128 bits would carry only 2^64 of
    // entropy — a ~2^32-row birthday bound past which two distinct strings would
    // collide to one `_join_pk` and the join's OPK byte-compare would silently
    // equijoin them.
    xxh::checksum_128(content)
}

#[derive(Clone, Copy)]
enum PromoteKind {
    /// Any scalar source column. The resolved [`ColumnLocator`] already says
    /// which region holds the bytes — an at-rest OPK PK window (sign bit flipped
    /// for signed, so a promoted copy is the sign-aware synthetic key) or a
    /// native-LE payload cell — at what width and at what source type. Both
    /// widths and both signs share this arm: the encode differs only by region,
    /// which is what the locator discriminates.
    Col(ColumnLocator),
    /// STRING/BLOB payload: sign-agnostic XXH3 hash key. The only source that is
    /// not a scalar cell the OPK encoders can consume.
    String { pi: usize },
    /// Group-key presence bitmap (one leading `U8` slot): bit *i* is set iff
    /// packed column *i* is NULL. Reads the packer's `group` extras.
    Bitmap,
    /// Group-key float slot: the `ieee_order_bits` image, big-endian in a `U64`
    /// slot. Order-preserving where the raw bits are not, and matching the
    /// `total_cmp` order the group comparator uses.
    Float(ColumnLocator),
    /// Group-key overflow fold (one trailing `U128` slot): the 128-bit hash of
    /// every group column past the packed prefix.
    Fold,
}

/// Per-column classifier for "read a source column, project it to OPK PK
/// bytes". Runs once per column at `ReindexPacker::new`; the resulting
/// `PromoteKind` is stored on the `ColPromoter`, and the per-row work is the
/// read + OPK encode performed by `ColPromoter::write_into`.
fn classify_promote(schema: &SchemaDescriptor, col_idx: usize) -> PromoteKind {
    let loc = schema.locate(col_idx);
    match loc {
        // BLOB shares the 16-byte German-string struct layout with STRING, so it
        // takes the same hash path rather than a raw cell encode. Neither can be
        // a PK column, so only the payload arm needs the test.
        ColumnLocator::Payload { slot, type_code, .. } if gnitz_wire::is_german_string(type_code) => {
            PromoteKind::String { pi: slot as usize }
        }
        _ => PromoteKind::Col(loc),
    }
}

/// Native `u128` of a 16-byte payload cell. Test-only: the production `Wide` arm
/// goes through `encode_pk_column_promoted` like every other arm, so this
/// survives purely as the independent oracle the packer tests compare against.
#[cfg(test)]
#[inline]
fn read_wide(batch: &MemBatch, pi: usize, row: usize) -> u128 {
    let ptr = batch.get_col_ptr(row, pi, 16);
    u128::from_le_bytes(ptr[0..16].try_into().unwrap())
}

#[inline]
fn read_string(batch: &MemBatch, pi: usize, row: usize) -> u128 {
    let struct_bytes = batch.get_col_ptr(row, pi, 16);
    german_string_promote_key(struct_bytes, batch.blob)
}

/// One key column of a `ReindexPacker`: its output slot (`out_off`, `out_size`),
/// the **resolved** slot type `out_tc`, and the `PromoteKind` that decides which
/// region the source bytes come from.
///
/// `out_tc` is `resolve_reindex_type(src_tc, carried)` — the carried-or-derive
/// rule applied once, in `new`. Storing the *unresolved* carried tc (with `0` as
/// a self-derive sentinel) forced `write_into` to undo the sentinel again, in two
/// different spellings, on a per-row path; resolving it here removes both.
#[derive(Clone, Copy)]
struct ColPromoter {
    out_off: usize,
    out_size: usize,
    out_tc: u8,
    /// Group-key mode: the source column is nullable, so a NULL zeroes the slot
    /// and the presence bitmap carries the NULL-ness instead. A stale or
    /// arbitrary cell under a NULL would otherwise split one group in two.
    /// Never set on a join key — those are NULL-gated upstream.
    nullable: bool,
    kind: PromoteKind,
}

impl ColPromoter {
    const PLACEHOLDER: ColPromoter = ColPromoter {
        out_off: 0,
        out_size: 0,
        out_tc: 0,
        nullable: false,
        kind: PromoteKind::String { pi: 0 },
    };

    /// Write this column's `out_size` OPK bytes into `dst` (len == out_size).
    ///
    /// Every arm but the string hash is one call into `ColumnLocator`'s own OPK
    /// promotion — the same method `IndexKeySpec::write_span` uses, which is what
    /// holds an index key and a join key byte-identical for one logical value.
    #[inline]
    fn write_into(&self, dst: &mut [u8], batch: &MemBatch, row: usize, packer: &ReindexPacker) {
        if self.nullable
            && matches!(self.kind, PromoteKind::Col(loc) | PromoteKind::Float(loc) if loc.is_null(batch, row))
        {
            dst.fill(0);
            return;
        }
        match self.kind {
            // Order-preserving at `out_tc` whichever region the source sits in,
            // and the identity copy when unpromoted, so both join sides pack
            // equal values identically. A float self-derives to U128, so the
            // promotion also does the right-alignment the slot needs.
            PromoteKind::Col(loc) => loc.encode_opk_promoted(batch, row, self.out_tc, dst),
            // Synthetic XXH3 content hash (unsigned U128): OPK == big-endian.
            PromoteKind::String { pi } => dst.copy_from_slice(&read_string(batch, pi, row).to_be_bytes()),
            PromoteKind::Bitmap => {
                let g = packer
                    .group
                    .as_ref()
                    .expect("bitmap slot only exists in group-key mode");
                let null_word = batch.get_null_word(row);
                let mut bits = 0u8;
                for (i, loc) in g.packed.iter().enumerate() {
                    if let ColumnLocator::Payload { slot, .. } = *loc {
                        bits |= u8::from(gnitz_wire::null_word_get(null_word, slot as usize)) << i;
                    }
                }
                dst[0] = bits;
            }
            // The order-encoded image, big-endian: an ascending float is an
            // ascending unsigned slot, so the packed key stays OPK.
            PromoteKind::Float(loc) => {
                let mut scratch = [0u8; 16];
                let src = loc.native_le_bytes(batch, row, &mut scratch);
                let enc = if loc.type_code() == crate::schema::type_code::F32 {
                    ieee_order_bits_f32(u32::from_le_bytes(src[..4].try_into().unwrap()))
                } else {
                    ieee_order_bits(u64::from_le_bytes(src[..8].try_into().unwrap()))
                };
                dst.copy_from_slice(&enc.to_be_bytes());
            }
            PromoteKind::Fold => {
                let g = packer.group.as_ref().expect("fold slot only exists in group-key mode");
                let null_word = batch.get_null_word(row);
                let mut hasher = RowHasher::new();
                for &loc in &g.folded {
                    hash_group_col(&mut hasher, batch, row, null_word, loc);
                }
                dst.copy_from_slice(&hasher.digest128().to_be_bytes());
            }
        }
    }
}

/// Packs a reindex column list into a contiguous OPK PK region. The same packer
/// drives both `op_map` (which writes the synthetic `_join_pk` at emission) and
/// the exchange scatter (which routes the raw delta by the same key), so the
/// reindexed trace side and the delta scatter side co-partition byte-for-byte at
/// every key arity and width.
pub(crate) struct ReindexPacker {
    cols: [ColPromoter; crate::schema::MAX_PK_COLUMNS], // first `num_cols` valid
    num_cols: usize,
    pub(crate) out_stride: usize,
    /// Present iff this packer was built by [`ReindexPacker::new_group_key`] —
    /// the source columns the presence-bitmap and overflow-fold slots read.
    group: Option<GroupKeyExtras>,
}

/// The two group-key slots that read source columns other than their own: the
/// presence bitmap (over the packed prefix) and the overflow fold (over the
/// columns behind it).
struct GroupKeyExtras {
    packed: Vec<ColumnLocator>,
    folded: Vec<ColumnLocator>,
}

impl ReindexPacker {
    /// Build per-column promoters from the reindex column list (key order). Each
    /// column's slot width is `resolve_reindex_type(tc).wire_stride()` (= `cs`
    /// for Pk/Narrow ints, 16 for floats/Wide/String). Offsets are the running
    /// sum (tightly packed, no inter-column padding, matching
    /// `Schema::pk_stride()`).
    ///
    /// The **only** derivation of that layout — [`Self::output_schema`] reads
    /// these same promoters. Two derivations agreeing only on the total stride
    /// could disagree per slot, and the failure is silent: equal keys stop
    /// co-partitioning.
    ///
    /// `None` on arity over `MAX_PK_COLUMNS`, an out-of-range column, or a stride
    /// over `MAX_PK_BYTES`. The column check is not redundant: a `col_idx` in
    /// `[num_columns, MAX_COLUMNS)` is in-bounds of the fixed `columns` array, so
    /// without it a hand-assembled circuit packs bytes from a zeroed slot.
    pub(crate) fn new(schema: &SchemaDescriptor, reindex_cols: &[u32], target_tcs: &[u8]) -> Option<Self> {
        if reindex_cols.len() > crate::schema::MAX_PK_COLUMNS {
            return None;
        }
        let mut cols = [ColPromoter::PLACEHOLDER; crate::schema::MAX_PK_COLUMNS];
        let mut out_off = 0usize;
        for (i, &c) in reindex_cols.iter().enumerate() {
            let col_idx = c as usize;
            if col_idx >= schema.num_columns() {
                return None;
            }
            let kind = classify_promote(schema, col_idx); // carries source tc on Pk/Narrow
            let src_tc = schema.columns[col_idx].type_code;
            // Carried promotion target (`0` = self-derive); the slot type and width
            // follow `resolve_reindex_type` so the scatter packer and the trace-side
            // reindex Map derive identical widths.
            let carried = target_tcs.get(i).copied().unwrap_or(0);
            let out_tc = gnitz_wire::resolve_reindex_type(src_tc, carried);
            let out_size = gnitz_wire::wire_stride(out_tc);
            // The payload arm right-aligns the source's native bytes into
            // `out_size`; a negative pad (out_size < source width) is unreachable
            // but would corrupt the slot, so assert the invariant the encoders
            // rely on. (Internal type-system invariant, fully determined by
            // `resolve_reindex_type` — not input-driven, so debug-only is enough.)
            debug_assert!(
                out_size >= gnitz_wire::wire_stride(src_tc)
                    || !matches!(kind, PromoteKind::Col(ColumnLocator::Payload { .. }))
            );
            cols[i] = ColPromoter {
                out_off,
                out_size,
                out_tc,
                nullable: false,
                kind,
            };
            out_off += out_size;
        }
        if out_off > crate::schema::MAX_PK_BYTES {
            return None;
        }
        Some(ReindexPacker {
            cols,
            num_cols: reindex_cols.len(),
            out_stride: out_off,
            group: None,
        })
    }

    /// The reindex Map's output schema: one PK column per key slot at the type
    /// `new` resolved, then `in_schema.columns[payload_cols[i]]` at payload slot
    /// `i`. Reading `out_tc` off the promoters is what makes this schema's stride
    /// and `out_stride` equal by construction.
    ///
    /// `payload_cols` is the source columns the reindex program copies, so a join
    /// side whose program skips a dead column stops persisting it in the trace.
    ///
    /// `None` iff the result exceeds `MAX_COLUMNS`; the PK-side bounds are `new`'s.
    pub(crate) fn output_schema(&self, in_schema: &SchemaDescriptor, payload_cols: &[u32]) -> Option<SchemaDescriptor> {
        let mut b = DerivedSchema::new();
        for cp in &self.cols[..self.num_cols] {
            b.push_pk(SchemaColumn::new(cp.out_tc, 0))?; // PK region: nullable = 0
        }
        for &c in payload_cols {
            b.push(in_schema.columns[c as usize])?;
        }
        Some(b.finish())
    }

    /// Build the packer for a **group** key over `group_cols`, following
    /// [`gnitz_wire::group_key_layout`] — the one rule the AVI schema, the
    /// reindex output schema and this packer all read, so a group key's slots
    /// and its bytes cannot disagree.
    ///
    /// Unlike a join key this is total: every group set of every arity over
    /// every column type has a layout, because columns past the budget fold into
    /// one trailing hash slot. That totality is what lets the reduce index every
    /// group set instead of falling back to a per-epoch trace rescan.
    pub(crate) fn new_group_key(schema: &SchemaDescriptor, group_cols: &[u32]) -> Self {
        let descs: Vec<(u8, bool)> = group_cols
            .iter()
            .map(|&c| {
                let col = &schema.columns[c as usize];
                (col.type_code, col.nullable != 0)
            })
            .collect();
        let layout = gnitz_wire::group_key_layout(&descs);
        let packed: Vec<ColumnLocator> = group_cols[..layout.n_packed]
            .iter()
            .map(|&c| schema.locate(c as usize))
            .collect();
        let folded: Vec<ColumnLocator> = group_cols[layout.n_packed..]
            .iter()
            .map(|&c| schema.locate(c as usize))
            .collect();

        let mut cols = [ColPromoter::PLACEHOLDER; crate::schema::MAX_PK_COLUMNS];
        let mut out_off = 0usize;
        let mut slot = 0usize;
        let push = |cols: &mut [ColPromoter; crate::schema::MAX_PK_COLUMNS],
                    slot: &mut usize,
                    out_off: &mut usize,
                    out_tc: u8,
                    nullable: bool,
                    kind: PromoteKind| {
            let out_size = gnitz_wire::wire_stride(out_tc);
            cols[*slot] = ColPromoter {
                out_off: *out_off,
                out_size,
                out_tc,
                nullable,
                kind,
            };
            *slot += 1;
            *out_off += out_size;
        };
        if layout.has_bitmap {
            push(
                &mut cols,
                &mut slot,
                &mut out_off,
                crate::schema::type_code::U8,
                false,
                PromoteKind::Bitmap,
            );
        }
        for (i, &(tc, nullable)) in descs[..layout.n_packed].iter().enumerate() {
            let loc = packed[i];
            let kind = if TypeCode::from_validated_u8(tc).is_float() {
                PromoteKind::Float(loc)
            } else {
                classify_promote(schema, group_cols[i] as usize)
            };
            push(
                &mut cols,
                &mut slot,
                &mut out_off,
                gnitz_wire::group_key_slot_type(tc),
                nullable,
                kind,
            );
        }
        if layout.has_fold {
            push(
                &mut cols,
                &mut slot,
                &mut out_off,
                crate::schema::type_code::U128,
                false,
                PromoteKind::Fold,
            );
        }
        debug_assert_eq!(out_off, layout.stride());
        ReindexPacker {
            cols,
            num_cols: slot,
            out_stride: out_off,
            group: Some(GroupKeyExtras { packed, folded }),
        }
    }

    /// Pack the full `_join_pk` (out_stride OPK bytes) for `row` into `dst`.
    #[inline]
    pub(crate) fn pack_into(&self, dst: &mut [u8], batch: &MemBatch, row: usize) {
        for cp in &self.cols[..self.num_cols] {
            cp.write_into(&mut dst[cp.out_off..cp.out_off + cp.out_size], batch, row, self);
        }
    }

    /// Overwrite every row's PK in `output` with the packed reindex key. `output`
    /// already carries the reindex output schema (PK stride == `out_stride`).
    pub(super) fn promote_into(&self, batch: &MemBatch, output: &mut Batch) {
        debug_assert_eq!(output.pk_stride() as usize, self.out_stride);
        let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
        for row in 0..output.count {
            // Each column fully overwrites its slot (Narrow zeroes its own pad),
            // so reusing `buf` across rows needs no inter-row clear.
            self.pack_into(&mut buf[..self.out_stride], batch, row);
            output.set_pk_at_bytes(row, &buf[..self.out_stride]);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::Batch;
    use crate::test_support::{
        make_batch_i64pk, make_schema_i64pk_i64, make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string,
        opk_pk,
    };

    /// Worker count the co-partition pins route against. Any count works — the
    /// property is that producer and consumer agree — but the wide-arm formula
    /// pin needs a fixed one to recompute against.
    const NW: usize = 4;

    /// Arity-1 golden oracle (test-only). Wraps a single column classified via
    /// `classify_promote`; `promote`/`promote_into` derive the expected synthetic
    /// key independently of the production `ReindexPacker`'s row loop, so the
    /// byte-identity and copartition tests can cross-check the packer against it.
    struct PkPromoter {
        kind: PromoteKind,
    }

    impl PkPromoter {
        fn new(schema: &SchemaDescriptor, col_idx: usize) -> Self {
            PkPromoter {
                kind: classify_promote(schema, col_idx),
            }
        }

        /// The **OPK-widened** value (what `get_pk` returns after `promote_into`),
        /// via the same `pk_route_key`/`payload_route_key` helpers the routing
        /// surfaces use — sign-aware for signed columns, so it discriminates the
        /// OPK encoding (an unsigned-native oracle would pass on the broken code).
        #[inline]
        fn promote(&self, batch: &MemBatch, row: usize) -> u128 {
            match self.kind {
                PromoteKind::Col(ColumnLocator::Pk { byte_off, size, .. }) => {
                    gnitz_wire::pk_route_key(batch.get_pk_bytes(row), byte_off as usize, size as usize)
                }
                PromoteKind::Col(ColumnLocator::Payload { slot, size, type_code }) => {
                    let (pi, cs) = (slot as usize, size as usize);
                    match type_code {
                        crate::schema::type_code::U128 | crate::schema::type_code::UUID => read_wide(batch, pi, row),
                        _ => gnitz_wire::payload_route_key(batch.get_col_ptr(row, pi, cs), 0, cs, type_code),
                    }
                }
                PromoteKind::String { pi } => read_string(batch, pi, row),
                // The oracle covers arity-1 *join* keys; the group-key-only
                // slots have no single-column u128 image to compare against.
                PromoteKind::Bitmap | PromoteKind::Float(_) | PromoteKind::Fold => {
                    unreachable!("the arity-1 join-key oracle sees no group-key slot")
                }
            }
        }

        /// Promote every row of `output` from `batch`, hoisting the per-batch kind
        /// dispatch out of the row loop. Every arm emits sign-aware OPK bytes
        /// right-aligned into `output.pk_stride()`, so the synthetic reindex key is
        /// byte-identical to how `ColumnLocator::route_key`, `worker_for_pk_bytes`, and
        /// storage encode the same value. Production goes through `ReindexPacker`
        /// (which generalises this to N columns); the arity-1 byte-identity test
        /// asserts `ReindexPacker` matches this oracle.
        fn promote_into(&self, batch: &MemBatch, output: &mut Batch) {
            let stride = output.pk_stride() as usize;
            // The fixed `[0u8; 16]` scratch buffers below right-align into this
            // stride; a stride > 16 would underflow `16 - stride`. The compiler
            // types a narrow (≤8-byte) integer reindex key at its native width and
            // every other key as U128, so the synthetic key is always ≤ 16 bytes.
            debug_assert!(
                stride <= gnitz_wire::NARROW_PK_MAX_BYTES,
                "promote_into: synthetic key stride {stride} exceeds NARROW_PK_MAX_BYTES"
            );
            match self.kind {
                // Source PK column is already OPK at rest — copy it verbatim,
                // right-aligned with left zero-pad. widen_pk_be of the result
                // equals `route_key`'s `widen_pk_be(get_pk_bytes[col])`, so
                // routing agrees.
                PromoteKind::Col(ColumnLocator::Pk { byte_off, size, .. }) => {
                    let (off, cs) = (byte_off as usize, size as usize);
                    // Loop-invariant scratch: only `buf[16 - cs..]` is rewritten per
                    // row; the left zero-pad (read when stride > cs) is set once.
                    let mut buf = [0u8; 16];
                    for row in 0..output.count {
                        let src = &batch.get_pk_bytes(row)[off..off + cs];
                        buf[16 - cs..].copy_from_slice(src);
                        output.set_pk_at_bytes(row, &buf[16 - stride..]);
                    }
                }
                PromoteKind::Col(ColumnLocator::Payload { slot, size, type_code }) => {
                    let (pi, cs) = (slot as usize, size as usize);
                    match type_code {
                        // U128/UUID are unsigned: OPK == big-endian.
                        crate::schema::type_code::U128 | crate::schema::type_code::UUID => {
                            for row in 0..output.count {
                                let v = read_wide(batch, pi, row);
                                output.set_pk_at(row, v);
                            }
                        }
                        // Payload integer: OPK-encode the native value (sign-flipped
                        // for signed), matching `route_key`'s payload arm.
                        _ => {
                            // Loop-invariant scratch: `encode_pk_column` fully overwrites
                            // the `[16 - cs..]` slice each row; the zero prefix persists.
                            let mut opk = [0u8; 16];
                            for row in 0..output.count {
                                let native = batch.get_col_ptr(row, pi, cs);
                                gnitz_wire::encode_pk_column(native, type_code, &mut opk[16 - cs..]);
                                output.set_pk_at_bytes(row, &opk[16 - stride..]);
                            }
                        }
                    }
                }
                // Synthetic XXH3 hash key (unsigned U128): OPK == big-endian.
                PromoteKind::String { pi } => {
                    for row in 0..output.count {
                        let h = read_string(batch, pi, row);
                        output.set_pk_at(row, h);
                    }
                }
                // The oracle covers arity-1 *join* keys; the group-key-only
                // slots have no single-column image to compare against.
                PromoteKind::Bitmap | PromoteKind::Float(_) | PromoteKind::Fold => {
                    unreachable!("the arity-1 join-key oracle sees no group-key slot")
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // promote_col_to_pk tests
    // -----------------------------------------------------------------------

    fn make_schema_pk_u64_payload_u32() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U32, 0),
            ],
            &[0],
        )
    }

    fn make_schema_pk_u64_payload_uuid() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::UUID, 0),
            ],
            &[0],
        )
    }

    fn build_batch_u32_payload(schema: &SchemaDescriptor, rows: &[(u64, u32)]) -> Batch {
        let mut b = Batch::with_capacity(*schema, rows.len().max(1));
        for &(pk, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b
    }

    // -----------------------------------------------------------------------
    // PkPromoter — gap-coverage for STRING hashing and PK-column reindex
    // -----------------------------------------------------------------------

    #[test]
    fn test_pk_promoter_string_short_and_long_hash() {
        // Two rows: one short ("foo", inline) and one long string (> 12 bytes,
        // stored in blob). Both PromoteKind::String code paths execute, and
        // distinct strings hash to distinct PKs.
        let schema = make_schema_pk_u64_payload_string();
        let mut b = Batch::with_capacity(schema, 2);

        // Row 0: short string "foo" (3 bytes, inline).
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let gs0 = gnitz_wire::encode_german_string(b"foo", &mut b.blob);
        b.extend_col(0, &gs0);
        b.count += 1;

        // Row 1: long string (15 bytes > SHORT_STRING_THRESHOLD=12), heap-allocated.
        let long_str: &[u8] = b"hello-world-xyz";
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let gs1 = gnitz_wire::encode_german_string(long_str, &mut b.blob);
        b.extend_col(0, &gs1);
        b.count += 1;

        let mb = b.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1);

        let pk_short = promoter.promote(&mb, 0);
        let pk_long = promoter.promote(&mb, 1);

        // Hashed via xxh3_128 — exact value is deterministic but we don't pin it.
        // The load-bearing invariants are: (1) both produce a non-zero PK for
        // non-empty strings, (2) different strings hash to different PKs, (3) the
        // high 64 bits are populated (a real 128-bit hash fills the full image,
        // not a 64-bit hash widened to 128 bits).
        assert_ne!(pk_short, 0);
        assert_ne!(pk_long, 0);
        assert_ne!(pk_short, pk_long);
        assert_ne!(
            pk_short >> 64,
            0,
            "short string PK must populate high half via xxh3_128"
        );
        assert_ne!(pk_long >> 64, 0, "long string PK must populate high half via xxh3_128");
    }

    #[test]
    fn test_pk_promoter_empty_string_hashes_to_zero() {
        // PromoteKind::String early-returns 0 for length==0 — assert this is
        // the contract, not an accidental side-effect of xxh on empty input.
        let schema = make_schema_pk_u64_payload_string();
        let mut b = Batch::with_capacity(schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        // 16-byte German string struct, length=0.
        let gs = [0u8; 16];
        b.extend_col(0, &gs);
        b.count += 1;

        let mb = b.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1);
        assert_eq!(promoter.promote(&mb, 0), 0);
    }

    #[test]
    fn test_pk_promoter_pk_column_passthrough() {
        // PromoteKind::Pk: reindexing on the PK column itself reads the PK
        // value verbatim, no hashing, no type dispatch.
        let schema = make_schema_pk_u64_payload_u32();
        let batch = build_batch_u32_payload(&schema, &[(7, 100), (42, 200), (1234567890, 300)]);
        let mb = batch.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 0); // col_idx 0 == PK

        assert_eq!(promoter.promote(&mb, 0), 7u128);
        assert_eq!(promoter.promote(&mb, 1), 42u128);
        assert_eq!(promoter.promote(&mb, 2), 1234567890u128);
    }

    // -----------------------------------------------------------------------
    // PkPromoter::promote_into matches per-row promote across all variants (item 31)
    // -----------------------------------------------------------------------

    #[test]
    fn test_pk_promoter_promote_into_matches_per_row() {
        // Narrow (U32) variant.
        let schema = make_schema_pk_u64_payload_u32();
        let rows: Vec<(u64, u32)> = (0..1000).map(|i| (i, (i * 7 + 1) as u32)).collect();
        let batch = build_batch_u32_payload(&schema, &rows);
        let mb = batch.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1);

        let mut out = batch.clone_batch();
        promoter.promote_into(&mb, &mut out);
        for row in 0..batch.count {
            assert_eq!(out.get_pk(row), promoter.promote(&mb, row), "row {row} narrow");
        }

        // Pk variant (reindex on PK column itself).
        let promoter_pk = PkPromoter::new(&schema, 0);
        let mut out_pk = batch.clone_batch();
        promoter_pk.promote_into(&mb, &mut out_pk);
        for row in 0..batch.count {
            assert_eq!(out_pk.get_pk(row), promoter_pk.promote(&mb, row), "row {row} pk");
        }

        // Signed (I64) schema — the discriminating case. The oracle is sign-aware
        // (route_key helpers), so promote_into must produce OPK-encoded bytes for
        // both the PK-column (Pk) and payload-column (Narrow) arms.
        let s_schema = make_schema_i64pk_i64();
        let s_rows: Vec<(i64, i64, i64)> = (-500..500).map(|i| (i, 1, (i * 3 - 7))).collect();
        let s_batch = make_batch_i64pk(&s_schema, &s_rows);
        let s_mb = s_batch.as_mem_batch();

        let s_narrow = PkPromoter::new(&s_schema, 1);
        let mut s_out = s_batch.clone_batch();
        s_narrow.promote_into(&s_mb, &mut s_out);
        for row in 0..s_batch.count {
            assert_eq!(
                s_out.get_pk(row),
                s_narrow.promote(&s_mb, row),
                "row {row} signed narrow"
            );
        }

        let s_pk = PkPromoter::new(&s_schema, 0);
        let mut s_out_pk = s_batch.clone_batch();
        s_pk.promote_into(&s_mb, &mut s_out_pk);
        for row in 0..s_batch.count {
            assert_eq!(s_out_pk.get_pk(row), s_pk.promote(&s_mb, row), "row {row} signed pk");
        }
    }

    #[test]
    fn test_pk_promoter_signed_opk_encoding() {
        // Reindex on a signed I64 PAYLOAD column (Narrow arm). The synthetic key
        // must be the sign-aware OPK image (matching `route_key`'s payload
        // encoding), NOT the raw unsigned value — for -3 the OPK leading byte is
        // 0x7F, not 0xFF. This is the assertion the old unsigned-only test could
        // not make.
        let schema = make_schema_i64pk_i64();
        let batch = make_batch_i64pk(&schema, &[(1, 1, -3), (2, 1, 5), (3, 1, -1)]);
        let mb = batch.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1); // payload col → Narrow arm
                                                    // Output PK stride is 8 here (single I64 PK), so the synthetic key is the
                                                    // 8-byte OPK image directly (right-aligned into stride 8 == verbatim).
        let mut out = batch.clone_batch();
        promoter.promote_into(&mb, &mut out);

        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&(-3i64).to_le_bytes(), type_code::I64, &mut opk);
        assert_eq!(out.get_pk_bytes(0), &opk[..], "synthetic key must be sign-aware OPK");
        assert_eq!(
            opk[0], 0x7F,
            "OPK leading byte of -3:I64 is 0x7F (sign-flipped), not 0xFF"
        );

        for row in 0..batch.count {
            assert_eq!(out.get_pk(row), promoter.promote(&mb, row), "row {row} narrow signed");
        }
    }

    #[test]
    fn test_pk_promoter_signed_copartition_pk_vs_narrow() {
        // Co-partition contract: a value reindexed via the Pk arm (as a PK column)
        // and via the Narrow arm (as a payload column) must produce byte-identical
        // synthetic PK bytes, and widen_pk_be of those bytes must equal the routing
        // key for that value — including negatives. This is what makes the
        // reindexed (delta) side and the OPK-routed (trace) side co-partition.
        let schema = make_schema_i64pk_i64();
        // Each row carries the same logical value V in both the PK and the payload.
        // Listed in signed (OPK) ascending order so the consolidated batch the
        // builder certifies is genuinely (PK, payload)-sorted.
        let vals = [i64::MIN, -7i64, -1, 0, 3, i64::MAX];
        let rows: Vec<(i64, i64, i64)> = vals.iter().map(|&v| (v, 1, v)).collect();
        let batch = make_batch_i64pk(&schema, &rows);
        let mb = batch.as_mem_batch();

        let promoter_pk = PkPromoter::new(&schema, 0); // PK column → Pk arm
        let promoter_narrow = PkPromoter::new(&schema, 1); // payload column → Narrow arm
        let mut out_pk = batch.clone_batch();
        let mut out_narrow = batch.clone_batch();
        promoter_pk.promote_into(&mb, &mut out_pk);
        promoter_narrow.promote_into(&mb, &mut out_narrow);

        // `row` indexes out_pk, out_narrow, and vals together — enumerate over
        // any one would not cover the others.
        #[allow(clippy::needless_range_loop)]
        for row in 0..batch.count {
            assert_eq!(
                out_pk.get_pk_bytes(row),
                out_narrow.get_pk_bytes(row),
                "row {}: Pk-arm and Narrow-arm synthetic bytes must match for V={}",
                row,
                vals[row],
            );
            // widen_pk_be of the synthetic bytes equals the sign-aware routing key
            // (what `ColumnLocator::route_key` returns for this value).
            let expect = gnitz_wire::payload_route_key(&vals[row].to_le_bytes(), 0, 8, type_code::I64);
            assert_eq!(out_pk.get_pk(row), expect, "row {row}: routing key mismatch");
        }
    }

    // -----------------------------------------------------------------------
    // PkPromoter BLOB
    // -----------------------------------------------------------------------

    #[test]
    fn test_pk_promoter_blob_no_panic() {
        // Regression: PkPromoter mapped BLOB to Narrow { cs: 16 }, copying 16
        // bytes into an 8-byte buffer and panicking. BLOB shares the
        // German-string layout and must take the String hash path.
        let schema = make_schema_pk_u64_payload_blob();
        let mut b = Batch::with_capacity(schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        // Short inline BLOB "abc" in a 16-byte German-string struct.
        let gs = gnitz_wire::encode_german_string(b"abc", &mut b.blob);
        b.extend_col(0, &gs);
        b.count += 1;

        let mb = b.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1);
        let pk = promoter.promote(&mb, 0);
        assert_ne!(pk, 0, "non-empty BLOB must hash to a non-zero key");
    }

    // -----------------------------------------------------------------------
    // ReindexPacker::output_schema — the layout the per-row packer writes through
    // -----------------------------------------------------------------------

    #[test]
    fn packer_output_schema_pk_width_policy() {
        // (key column type, expected output PK type, expected pk_stride)
        let cases = [
            (type_code::U64, type_code::U64, 8u8),
            (type_code::I32, type_code::I32, 4),
            (type_code::U16, type_code::U16, 2),
            (type_code::STRING, type_code::U128, 16),
            (type_code::BLOB, type_code::U128, 16),
            (type_code::U128, type_code::U128, 16),
            (type_code::UUID, type_code::U128, 16),
            (type_code::F64, type_code::U128, 16),
        ];
        for (key_tc, want_tc, want_stride) in cases {
            // in_schema: [U64 PK, <key col>]; reindex on the payload col so the
            // PK-ineligible key types (STRING/BLOB/float) are exercisable as keys.
            let in_schema = SchemaDescriptor::new(
                &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(key_tc, 0)],
                &[0],
            );
            let node_schema = ReindexPacker::new(&in_schema, &[1], &[])
                .unwrap()
                .output_schema(&in_schema, &[0, 1])
                .unwrap();
            assert_eq!(node_schema.columns[0].type_code, want_tc, "key {key_tc} → PK type");
            assert_eq!(node_schema.pk_stride(), want_stride, "key {key_tc} → pk_stride");
        }
    }

    #[test]
    fn packer_output_schema_compound() {
        // in_schema: [U64 pk, I32, U128]; reindex on (col1 I32, col2 U128).
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U128, 0),
            ],
            &[0],
        );
        let out = ReindexPacker::new(&in_schema, &[1, 2], &[])
            .unwrap()
            .output_schema(&in_schema, &[0, 1, 2])
            .unwrap();
        assert_eq!(out.pk_indices(), &[0, 1], "2-slot compound PK");
        assert_eq!(out.columns[0].type_code, type_code::I32, "slot0 keeps I32 native width");
        assert_eq!(out.columns[1].type_code, type_code::U128, "slot1 U128");
        assert_eq!(out.pk_stride(), 4 + 16, "compound stride = Σ slot widths");
        // Input columns follow the synthetic PK slots.
        assert_eq!(out.num_columns(), 2 + 3);
        assert_eq!(out.columns[2].type_code, type_code::U64);
        assert_eq!(out.columns[3].type_code, type_code::I32);
        assert_eq!(out.columns[4].type_code, type_code::U128);
    }

    #[test]
    fn packer_output_schema_cross_width_promotes() {
        // in_schema: [U64 pk, I32, I64]; reindex on (col1 I32, col2 I64) with
        // slot 0 promoted to I64 (carried) and slot 1 self-deriving.
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let out = ReindexPacker::new(&in_schema, &[1, 2], &[type_code::I64, 0])
            .unwrap()
            .output_schema(&in_schema, &[0, 1, 2])
            .unwrap();
        assert_eq!(out.columns[0].type_code, type_code::I64, "slot0 carried T = I64");
        assert_eq!(out.columns[1].type_code, type_code::I64, "slot1 self-derives I64");
        assert_eq!(out.pk_stride(), 8 + 8, "both slots 8 bytes after promotion");
    }

    #[test]
    fn packer_output_schema_payload_prune() {
        // in_schema: [U64 pk, I32, U128, I16]; reindex on col1; keep payload {0, 3}.
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I16, 0),
            ],
            &[0],
        );
        let out = ReindexPacker::new(&in_schema, &[1], &[])
            .unwrap()
            .output_schema(&in_schema, &[0, 3])
            .unwrap();
        assert_eq!(out.pk_indices(), &[0], "single synthetic PK slot");
        assert_eq!(out.columns[0].type_code, type_code::I32, "PK slot = reindex col1 (I32)");
        // Only the two kept payload columns follow — not all four input columns.
        assert_eq!(out.num_columns(), 1 + 2, "1 PK + 2 kept payload");
        assert_eq!(out.columns[1].type_code, type_code::U64, "kept payload col 0");
        assert_eq!(out.columns[2].type_code, type_code::I16, "kept payload col 3");
    }

    // -----------------------------------------------------------------------
    // ReindexPacker — multi-column / compound reindex packing
    // -----------------------------------------------------------------------

    /// Build a zeroed batch with `schema` and `n` rows (PK region + payload all
    /// zero). `ReindexPacker`/`PkPromoter` `promote_into` only overwrite the PK,
    /// so the zeroed payload is irrelevant — the batch just needs `count == n`
    /// and a PK region of `n * pk_stride` bytes to receive the synthetic key.
    fn make_zeroed_batch(schema: &SchemaDescriptor, n: usize) -> Batch {
        let mut b = Batch::with_capacity(*schema, n.max(1));
        let pk_stride = schema.pk_stride() as usize;
        let zeros = [0u8; crate::schema::MAX_PK_BYTES];
        for _ in 0..n {
            b.extend_pk_bytes(&zeros[..pk_stride]);
            b.extend_weight(&0i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            for (pi, col) in schema.payload_columns() {
                b.extend_col(pi, &zeros[..col.size() as usize]);
            }
            b.count += 1;
        }
        b
    }

    #[test]
    fn test_reindex_packer_multi_column_bytes() {
        // Compound key spanning every promoter arm:
        //   col1 = second PK column (Pk arm, nonzero off=8, cs=8)
        //   col2 = signed I32 payload  (Narrow, sign-flip, out_size==cs==4)
        //   col3 = U128 payload        (Wide, out_size==16)
        //   col4 = F64 payload         (Narrow, out_size 16 > cs 8 → zero-pad)
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::F64, 0),
            ],
            &[0, 1],
        );
        let pk0: u64 = 0x0102_0304_0506_0708;
        let pk1: u64 = 0xA0B0_C0D0_E0F0_0102;
        let iv: i32 = -3;
        let uv: u128 = 0xdead_beef_cafe_1234_5678_9abc_def0_0001;
        let fv: f64 = 2.5;

        let mut b = Batch::with_capacity(schema, 1);
        b.extend_pk_opk(&schema, &[pk0 as u128, pk1 as u128]);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &iv.to_le_bytes()); // I32 payload (pi 0)
        b.extend_col(1, &uv.to_le_bytes()); // U128 payload (pi 1)
        b.extend_col(2, &fv.to_le_bytes()); // F64 payload (pi 2)
        b.count += 1;
        let mb = b.as_mem_batch();

        let packer = ReindexPacker::new(&schema, &[1, 2, 3, 4], &[]).unwrap();
        // out_stride = 8 (Pk U64) + 4 (I32) + 16 (U128) + 16 (F64→U128) = 44.
        assert_eq!(packer.out_stride, 8 + 4 + 16 + 16);

        let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut buf[..packer.out_stride], &mb, 0);

        // Expected: each column's OPK bytes concatenated at its offset.
        let mut want = Vec::new();
        want.extend_from_slice(&pk1.to_be_bytes()); // col1 Pk: BE(pk1) verbatim
        let mut i32_opk = [0u8; 4];
        gnitz_wire::encode_pk_column(&iv.to_le_bytes(), type_code::I32, &mut i32_opk);
        want.extend_from_slice(&i32_opk); // col2 Narrow: sign-aware OPK
        assert_eq!(i32_opk[0], 0x7F, "I32 -3 OPK leading byte is sign-flipped (0x7F)");
        want.extend_from_slice(&uv.to_be_bytes()); // col3 Wide: BE(u128)
        let mut f64_slot = [0u8; 16];
        f64_slot[8..].copy_from_slice(&fv.to_bits().to_be_bytes()); // high 8 zero-pad, low 8 = BE(bits)
        want.extend_from_slice(&f64_slot); // col4 Narrow float
        assert_eq!(&buf[..packer.out_stride], &want[..], "packed compound key bytes");
        // Float slot high pad is zeroed.
        assert_eq!(&buf[28..36], &[0u8; 8], "F64 slot high pad zeroed");
    }

    #[test]
    fn test_reindex_packer_arity1_byte_identity() {
        // For a single-column reindex of each kind, ReindexPacker's output PK
        // bytes must be byte-equal to the retained PkPromoter::promote_into oracle.
        // (in_schema, reindex col, out type code)
        let cases: &[(SchemaDescriptor, usize)] = &[
            // Pk arm (reindex on the U64 PK column itself).
            (make_schema_pk_u64_payload_u32(), 0),
            // Narrow int (U32 payload).
            (make_schema_pk_u64_payload_u32(), 1),
            // Wide (UUID payload).
            (make_schema_pk_u64_payload_uuid(), 1),
            // String (STRING payload).
            (make_schema_pk_u64_payload_string(), 1),
        ];
        for (schema, col) in cases {
            // Three rows with assorted values exercising the per-row read.
            let mut b = Batch::with_capacity(*schema, 3);
            for r in 0..3u64 {
                b.extend_pk((r + 1) as u128 * 11);
                b.extend_weight(&1i64.to_le_bytes());
                b.extend_null_bmp(&0u64.to_le_bytes());
                let col_tc = schema.columns[1].type_code;
                let cs = schema.columns[1].size() as usize;
                let mut slot = [0u8; 16];
                if gnitz_wire::is_german_string(col_tc) {
                    slot = gnitz_wire::encode_german_string(&[b'a' + r as u8, b'b', b'c'], &mut b.blob);
                } else {
                    slot[..cs.min(8)].copy_from_slice(&((r + 7) * 1000).to_le_bytes()[..cs.min(8)]);
                }
                b.extend_col(0, &slot[..cs]);
                b.count += 1;
            }
            let mb = b.as_mem_batch();

            let out_tc = gnitz_wire::reindex_output_type_code(schema.columns[*col].type_code);
            let out_schema = SchemaDescriptor::new(&[SchemaColumn::new(out_tc, 0)], &[0]);

            let packer = ReindexPacker::new(schema, &[*col as u32], &[]).unwrap();
            let mut packer_out = make_zeroed_batch(&out_schema, 3);
            packer.promote_into(&mb, &mut packer_out);

            let oracle = PkPromoter::new(schema, *col);
            let mut oracle_out = make_zeroed_batch(&out_schema, 3);
            oracle.promote_into(&mb, &mut oracle_out);

            for row in 0..3 {
                assert_eq!(
                    packer_out.get_pk_bytes(row),
                    oracle_out.get_pk_bytes(row),
                    "arity-1 col {col} row {row}: packer must byte-match the oracle",
                );
            }
        }
    }

    #[test]
    fn test_reindex_packer_narrow_float_arity1_zero_pad() {
        // The float arity-1 case (out_size 16 > cs 8) separately: the packed slot
        // is 8 zero bytes ++ BE(bits), and must byte-match the oracle.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::F64, 0),
            ],
            &[0],
        );
        let mut b = Batch::with_capacity(schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let fv: f64 = -7.25;
        b.extend_col(0, &fv.to_le_bytes());
        b.count += 1;
        let mb = b.as_mem_batch();

        let out_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U128, 0)], &[0]);
        let packer = ReindexPacker::new(&schema, &[1], &[]).unwrap();
        assert_eq!(packer.out_stride, 16);
        let mut packer_out = make_zeroed_batch(&out_schema, 1);
        packer.promote_into(&mb, &mut packer_out);

        let mut want = [0u8; 16];
        want[8..].copy_from_slice(&fv.to_bits().to_be_bytes());
        assert_eq!(
            packer_out.get_pk_bytes(0),
            &want[..],
            "float slot = zero-pad ++ BE(bits)"
        );

        let oracle = PkPromoter::new(&schema, 1);
        let mut oracle_out = make_zeroed_batch(&out_schema, 1);
        oracle.promote_into(&mb, &mut oracle_out);
        assert_eq!(packer_out.get_pk_bytes(0), oracle_out.get_pk_bytes(0));
    }

    #[test]
    fn test_reindex_packer_copartition_contract() {
        // The bytes the exchange scatter computes (pack_into into a scratch
        // buffer) must be byte-identical to the `_join_pk` stored by promote_into,
        // so the delta scatter and the reindexed trace land on the same partition.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        // Reindex on (col2 U64 payload, col1 I32 payload) — a 2-column non-PK key.
        let cols = [2u32, 1u32];
        let rows: &[(u64, i32, u64)] = &[
            (1, -5, 100),
            (2, 7, 100), // same col2 as row 0, different col1
            (3, -5, 200),
            (4, i32::MIN, 0),
        ];
        let mut b = Batch::with_capacity(schema, rows.len());
        for &(pk, c1, c2) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &c1.to_le_bytes()); // I32 payload (pi 0)
            b.extend_col(1, &c2.to_le_bytes()); // U64 payload (pi 1)
            b.count += 1;
        }
        let mb = b.as_mem_batch();

        let packer = ReindexPacker::new(&schema, &cols, &[]).unwrap();
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0), // col2 → U64
                SchemaColumn::new(type_code::I32, 0), // col1 → I32
            ],
            &[0, 1],
        );
        let mut out = make_zeroed_batch(&out_schema, rows.len());
        packer.promote_into(&mb, &mut out);

        for row in 0..rows.len() {
            let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
            packer.pack_into(&mut buf[..packer.out_stride], &mb, row);
            // Trace side (stored _join_pk) == scatter side (scratch buffer).
            assert_eq!(out.get_pk_bytes(row), &buf[..packer.out_stride], "row {row} key bytes");
            assert_eq!(
                gnitz_wire::worker_for_pk_bytes(out.get_pk_bytes(row), NW),
                gnitz_wire::worker_for_pk_bytes(&buf[..packer.out_stride], NW),
                "row {row} co-partition",
            );
        }
        // Rows 0 and 1 share col2 but differ in col1 → distinct keys.
        assert_ne!(out.get_pk_bytes(0), out.get_pk_bytes(1));
    }

    #[test]
    fn test_reindex_packer_copartition_contract_wide() {
        // WIDE-branch (key len > 16) co-partition pin. A 3×U64 reindex key is 24
        // OPK bytes, so `worker_for_pk_bytes` takes its `len > 16` arm
        // (multiply-shift over `xxh3_64(bytes)`), NOT the narrow
        // `mix(widen_pk_be(..))` arm the existing 12-byte contract test exercises.
        // The wide key is built by three INDEPENDENT code paths — the exchange
        // scatter (`pack_into` → scratch), the reindexed trace store
        // (`promote_into` → `get_pk_bytes`), and the storage/ingest OPK encoder
        // (`opk_pk` = `encode_order_preserving_pk`, which never touches
        // `ReindexPacker`) — so a fork in any single path's wide-key construction
        // breaks the byte-equality teeth, and a fork in `worker_for_pk_bytes`'s
        // wide arm breaks the formula-pin teeth.
        //
        // Source: narrow U64 PK + three U64 payload columns; reindex on the three
        // payloads (the `map_reindex` repartition-by-3-column-key shape).
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0), // PK (not part of the key)
                SchemaColumn::new(type_code::U64, 0), // c1 payload → key slot 0
                SchemaColumn::new(type_code::U64, 0), // c2 payload → key slot 1
                SchemaColumn::new(type_code::U64, 0), // c3 payload → key slot 2
            ],
            &[0],
        );
        // Non-trivial, high-entropy column values (so a forked hash seed/shift in
        // the wide arm lands on a different bucket with overwhelming probability).
        let key: [u64; 3] = [0x0102_0304_0506_0708, 0xA0B0_C0D0_E0F0_0102, 0xdead_beef_cafe_1234];
        let mut b = Batch::with_capacity(schema, 1);
        b.extend_pk(42u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &key[0].to_le_bytes()); // payload pi 0 (c1)
        b.extend_col(1, &key[1].to_le_bytes()); // payload pi 1 (c2)
        b.extend_col(2, &key[2].to_le_bytes()); // payload pi 2 (c3)
        b.count += 1;
        let mb = b.as_mem_batch();

        // Reindex on the three U64 payload columns → a 24-byte (3×U64) OPK key.
        let cols = [1u32, 2u32, 3u32];
        let packer = ReindexPacker::new(&schema, &cols, &[]).unwrap();
        assert_eq!(packer.out_stride, 24, "3×U64 reindex key must be 24 bytes (wide)");

        // The reindex output schema = natural 3×U64 PK (what `op_map` stamps and
        // what the trace store holds); identical layout to `wide_pk_3xu64_schema`
        // minus the trailing payload.
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1, 2],
        );
        assert!(out_schema.pk_stride() > 16, "test invariant: 24-byte key is wide");

        // PATH 1 — trace store: promote_into stamps the `_join_pk`; read it back.
        let mut out = make_zeroed_batch(&out_schema, 1);
        packer.promote_into(&mb, &mut out);
        let consumer = out.get_pk_bytes(0);

        // PATH 2 — exchange scatter: pack_into into a scratch buffer.
        let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut buf[..packer.out_stride], &mb, 0);
        let producer = &buf[..packer.out_stride];

        // PATH 3 — storage/ingest OPK encoder (no ReindexPacker involved at all).
        let oracle = opk_pk(&out_schema, &[key[0] as u128, key[1] as u128, key[2] as u128]);

        // (1) BYTE-EQUALITY teeth: all three independent builders agree, and the
        // key is genuinely wide (> 16 bytes).
        assert_eq!(consumer.len(), 24, "consumer key is the 24-byte wide region");
        assert!(consumer.len() > 16, "wide branch requires key len > 16");
        assert_eq!(producer, consumer, "scatter (pack_into) == trace store (_join_pk)");
        assert_eq!(consumer, oracle.as_slice(), "trace store == ingest OPK encoder");
        assert_eq!(producer, oracle.as_slice(), "scatter == ingest OPK encoder");

        // (2) CO-PARTITION teeth: producer and consumer route to the same worker
        // through the WIDE arm of worker_for_pk_bytes.
        let p_consumer = gnitz_wire::worker_for_pk_bytes(consumer, NW);
        let p_producer = gnitz_wire::worker_for_pk_bytes(producer, NW);
        let p_oracle = gnitz_wire::worker_for_pk_bytes(oracle.as_slice(), NW);
        assert_eq!(p_producer, p_consumer, "producer/consumer co-partition (wide)");
        assert_eq!(p_consumer, p_oracle, "trace store / ingest co-partition (wide)");

        // (3) WIDE-ARM FORMULA pin: the owner is the multiply-shift re-bucketing of
        // XXH3-64 over the OPK bytes, recomputed independently here. A forked hash
        // seed or a shifted bucket extraction in `worker_for_pk_bytes`'s `len > 16`
        // arm diverges from this reference even though it would still keep
        // producer == consumer (both call the same forked function). This is the
        // teeth that survives the "both paths share one worker_for_pk_bytes"
        // case called out for cluster C2.
        let expected = ((crate::foundation::xxh::checksum(consumer) as u128 * NW as u128) >> 64) as usize;
        assert_eq!(p_consumer, expected, "wide owner == ((xxh3_64(opk) * W) >> 64)");
        assert!(expected < NW, "the owner is a launched worker");
    }

    #[test]
    fn test_reindex_packer_null_key_determinism() {
        // A NULL value in a nullable (unsigned) reindex key column is canonically
        // zeroed at the source; the packer reads those zeros (ignoring the null
        // bitmap) and OPK-encodes them. Two distinct rows both NULL in the key
        // column must pack that slot identically (all-zero for an unsigned key).
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U32, 1), // nullable U32 key
            ],
            &[0],
        );
        let mut b = Batch::with_capacity(schema, 2);
        // Row 0 and row 1: distinct PK, both NULL in col1 (slot zeroed, null bit set).
        for pk in [10u128, 20u128] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            // null bit for payload col index 0 (col1) set.
            b.extend_null_bmp(&1u64.to_le_bytes());
            b.extend_col(0, &0u32.to_le_bytes());
            b.count += 1;
        }
        let mb = b.as_mem_batch();

        let packer = ReindexPacker::new(&schema, &[1], &[]).unwrap();
        assert_eq!(packer.out_stride, 4); // U32 key → 4-byte slot

        let mut buf0 = [0u8; crate::schema::MAX_PK_BYTES];
        let mut buf1 = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut buf0[..packer.out_stride], &mb, 0);
        packer.pack_into(&mut buf1[..packer.out_stride], &mb, 1);

        assert_eq!(&buf0[..4], &[0u8; 4], "NULL unsigned key slot is all-zero");
        assert_eq!(&buf0[..4], &buf1[..4], "two NULL-key rows pack identically");
    }

    /// A cross-width promoted reindex column (carried `T != 0`) packs each slot
    /// byte-identically to `encode_pk_column_promoted` for the same native value —
    /// for both a payload (`Narrow`) key and a PK-column (`Pk`) key, signed and
    /// unsigned, including a negative I32 → I64 that exercises sign-extension.
    #[test]
    fn test_reindex_packer_promoted_columns_match_encoder() {
        // schema: [I32 PK, I32 payload, U32 payload]. Reindex on the PK col (Pk
        // arm, promote I32→I64) and the U32 payload (Narrow arm, promote U32→U64).
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U32, 0),
            ],
            &[0],
        );
        let rows: &[(i32, i32, u32)] = &[(-5, -5, 7), (i32::MIN, 1, u32::MAX), (42, -1, 0)];
        let mut b = Batch::with_capacity(schema, rows.len());
        for &(pk, c1, c2) in rows {
            // PK is OPK at rest: encode the I32 PK value.
            let mut opk = [0u8; 4];
            gnitz_wire::encode_pk_column(&pk.to_le_bytes(), type_code::I32, &mut opk);
            b.extend_pk_bytes(&opk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &c1.to_le_bytes()); // I32 payload (pi 0)
            b.extend_col(1, &c2.to_le_bytes()); // U32 payload (pi 1)
            b.count += 1;
        }
        let mb = b.as_mem_batch();

        // Reindex (PK col0 → I64, payload col2 U32 → U64).
        let cols = [0u32, 2u32];
        let targets = [type_code::I64, type_code::U64];
        let packer = ReindexPacker::new(&schema, &cols, &targets).unwrap();
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1],
        );
        assert_eq!(packer.out_stride, 8 + 8);
        let mut out = make_zeroed_batch(&out_schema, rows.len());
        packer.promote_into(&mb, &mut out);

        for (row, &(pk, _c1, c2)) in rows.iter().enumerate() {
            // Slot 0: I32 PK value sign-extended to I64, OPK-encoded.
            let mut want0 = [0u8; 8];
            gnitz_wire::encode_pk_column_promoted(
                &(pk as i64).to_le_bytes(),
                type_code::I32,
                type_code::I64,
                &mut want0,
            );
            // Slot 1: U32 payload zero-extended to U64, OPK-encoded.
            let mut want1 = [0u8; 8];
            gnitz_wire::encode_pk_column_promoted(
                &(c2 as u64).to_le_bytes()[..4],
                type_code::U32,
                type_code::U64,
                &mut want1,
            );
            let got = out.get_pk_bytes(row);
            assert_eq!(&got[..8], &want0, "row {row} promoted Pk slot (I32→I64)");
            assert_eq!(&got[8..16], &want1, "row {row} promoted Narrow slot (U32→U64)");
        }
    }
}
