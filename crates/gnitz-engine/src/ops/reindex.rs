//! The synthetic PKs a reindex stamps: the equijoin `_join_pk`, the packed
//! `_group_pk`, and the set-op full-row content hash.
//!
//! `op_map`'s reindex paths and the exchange scatter share one `ReindexPacker`,
//! which is what makes the reindexed trace side and the delta scatter side
//! co-partition byte-for-byte at every key arity and width.

use crate::foundation::xxh::{self, RowHasher};
use crate::schema::{ColumnLocator, DerivedSchema, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::{Batch, MemBatch};

use super::util::{hash_fold, ieee_order_bits, ieee_order_bits_f32};

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

/// Synthetic-PK / routing key for a German-string column's content. Both the
/// reindex Map (setting a row's `_join_pk`) and the exchange scatter (routing the
/// raw delta) reach it through the packer's `String` arm, so a string join key
/// scatters to the worker that owns its own `_join_pk` partition. Empty content —
/// including a NULL string, a zeroed German-string struct — hashes to 0.
#[inline]
pub(super) fn german_string_promote_key(struct_bytes: &[u8], blob: &[u8]) -> u128 {
    let content = gnitz_wire::german_string_content(struct_bytes, blob);
    if content.is_empty() {
        return 0; // NULL / empty-string sentinel
    }
    // A true 128-bit content hash. A 64-bit hash widened to 128 bits would carry
    // only 2^64 of entropy — a ~2^32-row birthday bound past which two distinct
    // strings collide to one `_join_pk` and the join's OPK byte-compare silently
    // equijoins them.
    xxh::checksum_128(content)
}

#[derive(Clone, Copy)]
enum PromoteKind {
    /// Any scalar source column, at either width and either sign: the locator
    /// says which region holds the bytes, and the encode differs only by that.
    ///
    /// A float here packs raw IEEE bits — equality-correct, not order-preserving;
    /// [`Self::Float`] is the group key's spelling. `reject_float_key` blocks a
    /// float from every join / set-op / GROUP BY key, so neither is reachable.
    Col(ColumnLocator),
    /// STRING/BLOB payload: sign-agnostic XXH3 content-hash key. The only source
    /// that is not a scalar cell the OPK encoders can consume.
    String(ColumnLocator),
    /// Group-key presence bitmap (one leading `U8` slot): bit *i* is set iff
    /// packed column *i* is NULL. Written by `pack_into` after the slot loop,
    /// from the NULL tests that loop already performs.
    Bitmap,
    /// Group-key float slot: the `ieee_order_bits` image, big-endian in a `U64`
    /// slot. Order-preserving where the raw bits are not, and matching the
    /// `total_cmp` order the group comparator uses.
    Float(ColumnLocator),
    /// Group-key overflow fold (one trailing `U128` slot): the 128-bit hash of
    /// every group column past the packed prefix — [`ReindexPacker::folded`].
    Fold,
}

/// Per-column classifier for "read a source column, project it to OPK PK
/// bytes". Runs once per column at construction; the resulting `PromoteKind` is
/// stored on the `ColPromoter`, and the per-row work is the read + OPK encode
/// `ReindexPacker::pack_into` performs.
fn classify_promote(loc: ColumnLocator) -> PromoteKind {
    match loc {
        // BLOB shares the 16-byte German-string struct layout with STRING, so it
        // takes the same hash path rather than a raw cell encode. Neither can be
        // a PK column, so only the payload arm needs the test.
        ColumnLocator::Payload { type_code, .. } if gnitz_wire::is_german_string(type_code) => PromoteKind::String(loc),
        _ => PromoteKind::Col(loc),
    }
}

/// One key column of a `ReindexPacker`: the output PK column it packs into —
/// resolved once at construction, and its `size()` is the slot width, so the
/// packed bytes and [`ReindexPacker::output_schema`] read one value rather than
/// two — plus the `PromoteKind` that says where the source bytes come from.
#[derive(Clone, Copy)]
struct ColPromoter {
    out_col: SchemaColumn,
    /// Group-key mode: the **source** column is nullable, so a NULL zeroes the
    /// slot and the presence bitmap carries the NULL-ness instead. A stale or
    /// arbitrary cell under a NULL would otherwise split one group in two.
    /// Never set on a join key — those are NULL-gated upstream.
    nullable: bool,
    kind: PromoteKind,
}

impl ColPromoter {
    /// Unused slots of the fixed `cols` array, matching what `IndexKeySpec::new`
    /// fills its own with: the schema layer's designated padding column and a
    /// zeroed PK locator.
    const PLACEHOLDER: ColPromoter = ColPromoter {
        out_col: SchemaColumn::EMPTY,
        nullable: false,
        kind: PromoteKind::Col(ColumnLocator::Pk {
            byte_off: 0,
            size: 0,
            type_code: 0,
        }),
    };

    /// A slot packing into a `out_tc` output PK column. The one place the output
    /// column is spelled, because it is never nullable while `nullable` — the
    /// *source* column's — routinely is.
    fn new(out_tc: u8, nullable: bool, kind: PromoteKind) -> Self {
        ColPromoter {
            out_col: SchemaColumn::new(out_tc, 0),
            nullable,
            kind,
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
    /// Group columns past the packed prefix, hashed into the trailing fold slot.
    /// Empty for a join key and for a group key with no fold.
    folded: Vec<ColumnLocator>,
}

impl ReindexPacker {
    /// Build per-column promoters from the reindex column list (key order),
    /// tightly packed with no inter-column padding. The **only** derivation of
    /// that layout: [`Self::output_schema`] reads these same promoters, so the
    /// two cannot disagree per slot while still agreeing on the total stride —
    /// which would silently stop equal keys co-partitioning.
    ///
    /// `None` on arity over `MAX_PK_COLUMNS`, an out-of-range column, or a stride
    /// over `MAX_PK_BYTES` — a forged circuit is rejected, never panicked on.
    pub(crate) fn new(schema: &SchemaDescriptor, reindex_cols: &[u32], target_tcs: &[u8]) -> Option<Self> {
        if reindex_cols.len() > crate::schema::MAX_PK_COLUMNS {
            return None;
        }
        let mut cols = [ColPromoter::PLACEHOLDER; crate::schema::MAX_PK_COLUMNS];
        let mut stride = 0usize;
        for (i, &c) in reindex_cols.iter().enumerate() {
            // `locate`'s own out-of-range guard is a release-active panic, so the
            // rejection has to happen here — bound to the call, not merely ahead
            // of it.
            let loc = ((c as usize) < schema.num_columns()).then(|| schema.locate(c as usize))?;
            let kind = classify_promote(loc);
            // Carried promotion target (`0` = self-derive); the slot type and width
            // follow `resolve_reindex_type` so the scatter packer and the trace-side
            // reindex Map derive identical widths.
            let carried = target_tcs.get(i).copied().unwrap_or(0);
            let cp = ColPromoter::new(gnitz_wire::resolve_reindex_type(loc.type_code(), carried), false, kind);
            // A payload slot right-aligns its source, so one narrower than the
            // source would truncate it. `resolve_reindex_type` never derives that;
            // debug-only because it is a type-system invariant, not input.
            debug_assert!(
                cp.out_col.size() as usize >= gnitz_wire::wire_stride(loc.type_code())
                    || !matches!(kind, PromoteKind::Col(ColumnLocator::Payload { .. }))
            );
            stride += cp.out_col.size() as usize;
            cols[i] = cp;
        }
        if stride > crate::schema::MAX_PK_BYTES {
            return None;
        }
        Some(ReindexPacker {
            cols,
            num_cols: reindex_cols.len(),
            out_stride: stride,
            folded: Vec::new(),
        })
    }

    /// The reindex Map's output schema: the promoters' own `out_col` per key
    /// slot — so this schema's stride and `out_stride` are the same sum — then
    /// `in_schema.columns[payload_cols[i]]`. `payload_cols` is what the reindex
    /// program copies, so a join side skipping a dead column stops persisting it.
    ///
    /// `None` iff the result exceeds `MAX_COLUMNS`; the PK-side bounds are `new`'s.
    pub(crate) fn output_schema(&self, in_schema: &SchemaDescriptor, payload_cols: &[u32]) -> Option<SchemaDescriptor> {
        let mut b = DerivedSchema::new();
        for cp in &self.cols[..self.num_cols] {
            b.push_pk(cp.out_col)?;
        }
        for &c in payload_cols {
            b.push(in_schema.columns[c as usize])?;
        }
        Some(b.finish())
    }

    /// Build the packer for a **group** key over `group_cols`. Every slot type
    /// comes from [`gnitz_wire::group_key_layout`], which the AVI schema and the
    /// reindex output schema also read, so a group key's slots and its bytes
    /// cannot disagree.
    ///
    /// Unlike a join key this is total — columns past the budget fold into one
    /// trailing hash slot — which is what lets the reduce index every group set
    /// instead of rescanning the trace per epoch.
    pub(crate) fn new_group_key(schema: &SchemaDescriptor, group_cols: &[u32]) -> Self {
        let descs: Vec<(u8, bool)> = group_cols
            .iter()
            .map(|&c| {
                let col = &schema.columns[c as usize];
                (col.type_code, col.nullable != 0)
            })
            .collect();
        let layout = gnitz_wire::group_key_layout(&descs);
        let folded: Vec<ColumnLocator> = group_cols[layout.n_packed..]
            .iter()
            .map(|&c| schema.locate(c as usize))
            .collect();

        let mut cols = [ColPromoter::PLACEHOLDER; crate::schema::MAX_PK_COLUMNS];
        let mut slot = 0usize;
        if layout.has_bitmap {
            cols[slot] = ColPromoter::new(layout.slots[slot], false, PromoteKind::Bitmap);
            slot += 1;
        }
        for (i, &(tc, nullable)) in descs[..layout.n_packed].iter().enumerate() {
            let loc = schema.locate(group_cols[i] as usize);
            let kind = if TypeCode::from_validated_u8(tc).is_float() {
                PromoteKind::Float(loc)
            } else {
                classify_promote(loc)
            };
            cols[slot] = ColPromoter::new(layout.slots[slot], nullable, kind);
            slot += 1;
        }
        if layout.has_fold {
            cols[slot] = ColPromoter::new(layout.slots[slot], false, PromoteKind::Fold);
            slot += 1;
        }
        ReindexPacker {
            cols,
            num_cols: slot,
            out_stride: layout.stride(),
            folded,
        }
    }

    /// Pack the full reindex key (`out_stride` OPK bytes) for `row` into `dst`.
    ///
    /// One pass: the running slot offset, and — for a group key — the presence
    /// bitmap, whose bits are the NULL tests the packed slots already perform.
    #[inline]
    pub(crate) fn pack_into(&self, dst: &mut [u8], batch: &MemBatch, row: usize) {
        let null_word = batch.get_null_word(row);
        let mut off = 0usize;
        let mut null_bits = 0u8;
        for (i, cp) in self.cols[..self.num_cols].iter().enumerate() {
            let w = cp.out_col.size() as usize;
            let slot = &mut dst[off..off + w];
            off += w;
            match cp.kind {
                // A NULL packed column: zeroed slot, and its bit in the bitmap.
                // `nullable` implies the bitmap exists and is slot 0, so this
                // slot's packed index is `i - 1`.
                PromoteKind::Col(loc) | PromoteKind::String(loc) | PromoteKind::Float(loc)
                    if cp.nullable && loc.is_null_word(null_word) =>
                {
                    null_bits |= 1 << (i - 1);
                    slot.fill(0);
                }
                PromoteKind::Col(loc) => loc.encode_opk_promoted(batch, row, cp.out_col.type_code, slot),
                PromoteKind::String(loc) => {
                    let h = german_string_promote_key(loc.bytes(batch, row), batch.blob);
                    slot.copy_from_slice(&h.to_be_bytes());
                }
                PromoteKind::Bitmap => {}
                PromoteKind::Float(loc) => {
                    let mut scratch = [0u8; 16];
                    let src = loc.native_le_bytes(batch, row, &mut scratch);
                    let enc = if loc.type_code() == crate::schema::type_code::F32 {
                        ieee_order_bits_f32(u32::from_le_bytes(src[..4].try_into().unwrap()))
                    } else {
                        ieee_order_bits(u64::from_le_bytes(src[..8].try_into().unwrap()))
                    };
                    slot.copy_from_slice(&enc.to_be_bytes());
                }
                PromoteKind::Fold => {
                    slot.copy_from_slice(&hash_fold(&self.folded, batch, row, null_word).to_be_bytes());
                }
            }
        }
        // Unconditional per row, keeping `promote_into`'s "every slot is fully
        // overwritten" contract true.
        if matches!(self.cols[0].kind, PromoteKind::Bitmap) {
            dst[0] = null_bits;
        }
    }

    /// Overwrite every row's PK in `output` with the packed reindex key. `output`
    /// already carries the reindex output schema (PK stride == `out_stride`).
    pub(super) fn promote_into(&self, batch: &MemBatch, output: &mut Batch) {
        debug_assert_eq!(output.pk_stride() as usize, self.out_stride);
        let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
        for row in 0..output.count {
            // Every slot is fully overwritten each row (a promoted slot zeroes its
            // own pad), so reusing `buf` across rows needs no inter-row clear.
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
    use crate::test_support::{make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string, opk_pk};

    /// Worker count the co-partition pins route against. Any count works — the
    /// property is that producer and consumer agree — but the wide-arm formula
    /// pin needs a fixed one to recompute against.
    const NW: usize = 4;

    // -----------------------------------------------------------------------
    // german_string_promote_key — the content-hash arm's own contract
    // -----------------------------------------------------------------------

    #[test]
    fn test_german_string_promote_key_short_and_long() {
        // Two rows: one short ("foo", inline) and one long string (> 12 bytes,
        // stored in blob). Both German-string layouts execute, and distinct
        // strings hash to distinct PKs.
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
        let pk_short = german_string_promote_key(mb.get_col_ptr(0, 0, 16), mb.blob);
        let pk_long = german_string_promote_key(mb.get_col_ptr(1, 0, 16), mb.blob);

        // The digest is deterministic but not pinned. What matters: non-empty
        // content hashes non-zero, distinct content hashes distinctly, and the
        // high half is populated (a real 128-bit hash, not a widened 64-bit one).
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
    fn test_german_string_promote_key_empty_is_zero() {
        // The hash early-returns 0 for length==0 — assert this is the contract,
        // not an accidental side-effect of xxh on empty input.
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
        assert_eq!(german_string_promote_key(mb.get_col_ptr(0, 0, 16), mb.blob), 0);
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

    #[test]
    fn test_reindex_packer_multi_column_bytes() {
        // Compound key spanning every slot shape: a non-leading PK column (offset
        // 8), a sign-flipped I32 payload, a 16-byte U128 payload, and an F64
        // whose 8 source bytes zero-pad into a 16-byte slot.
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
        want.extend_from_slice(&i32_opk); // col2: sign-aware OPK
        assert_eq!(i32_opk[0], 0x7F, "I32 -3 OPK leading byte is sign-flipped (0x7F)");
        want.extend_from_slice(&uv.to_be_bytes()); // col3 Wide: BE(u128)
        let mut f64_slot = [0u8; 16];
        f64_slot[8..].copy_from_slice(&fv.to_bits().to_be_bytes()); // high 8 zero-pad, low 8 = BE(bits)
        want.extend_from_slice(&f64_slot); // col4: float, zero-padded
        assert_eq!(&buf[..packer.out_stride], &want[..], "packed compound key bytes");
        // Float slot high pad is zeroed.
        assert_eq!(&buf[28..36], &[0u8; 8], "F64 slot high pad zeroed");
    }

    #[test]
    fn test_reindex_packer_arity1_byte_identity() {
        // The content-hash arm: a STRING and a BLOB key both pack to the
        // big-endian image of `german_string_promote_key` over the column's
        // content. The integer and PK-placement arms are the proptest's; this is
        // the arm it cannot generate (`arb_pk_type` yields PK-eligible integers).
        for schema in [make_schema_pk_u64_payload_string(), make_schema_pk_u64_payload_blob()] {
            // Three rows with distinct content, one of them empty (the zero
            // sentinel), exercising the per-row read.
            let contents: [&[u8]; 3] = [b"abc", b"", b"hello-world-xyz"];
            let mut b = Batch::with_capacity(schema, 3);
            for (r, content) in contents.iter().enumerate() {
                b.extend_pk((r + 1) as u128 * 11);
                b.extend_weight(&1i64.to_le_bytes());
                b.extend_null_bmp(&0u64.to_le_bytes());
                let gs = gnitz_wire::encode_german_string(content, &mut b.blob);
                b.extend_col(0, &gs);
                b.count += 1;
            }
            let mb = b.as_mem_batch();

            let out_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U128, 0)], &[0]);
            let packer = ReindexPacker::new(&schema, &[1], &[]).unwrap();
            assert_eq!(packer.out_stride, 16, "a content-hash key is a 16-byte U128 slot");
            let mut out = Batch::zeroed(out_schema, 3);
            packer.promote_into(&mb, &mut out);

            for row in 0..3 {
                let want = german_string_promote_key(mb.get_col_ptr(row, 0, 16), mb.blob);
                assert_eq!(
                    out.get_pk_bytes(row),
                    &want.to_be_bytes()[..],
                    "{} row {row}: packed key is BE(content hash)",
                    schema.columns[1].type_code,
                );
            }
            // The empty-content row is the zero sentinel, and the two non-empty
            // rows do not collide with it or with each other.
            assert_eq!(out.get_pk_bytes(1), &[0u8; 16], "empty content hashes to zero");
            assert_ne!(out.get_pk_bytes(0), out.get_pk_bytes(2));
            assert_ne!(out.get_pk_bytes(0), out.get_pk_bytes(1));
        }
    }

    #[test]
    fn test_reindex_packer_float_arity1_zero_pad() {
        // A float key self-derives to a 16-byte slot from an 8-byte source: the
        // slot is 8 zero bytes ++ BE(bits).
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
        let mut packer_out = Batch::zeroed(out_schema, 1);
        packer.promote_into(&mb, &mut packer_out);

        let mut want = [0u8; 16];
        want[8..].copy_from_slice(&fv.to_bits().to_be_bytes());
        assert_eq!(
            packer_out.get_pk_bytes(0),
            &want[..],
            "float slot = zero-pad ++ BE(bits)"
        );
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
        let mut out = Batch::zeroed(out_schema, rows.len());
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
        // WIDE-branch (24-byte key) co-partition pin: three independent builders
        // must agree on the bytes — the scatter (`pack_into`), the trace store
        // (`promote_into`), and the ingest OPK encoder (`opk_pk`, which never
        // touches `ReindexPacker`) — and the formula pin below catches a fork in
        // the wide routing arm, which byte-equality alone cannot.
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
        let mut out = Batch::zeroed(out_schema, 1);
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

        // (3) WIDE-ARM FORMULA pin: the owner is the multiply-shift re-bucketing
        // of XXH3-64 over the OPK bytes, recomputed here. A forked seed or shift
        // would still keep producer == consumer — both call the same function —
        // so only an independent reference catches it.
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

    // -----------------------------------------------------------------------
    // ReindexPacker::new_group_key — the presence bitmap
    // -----------------------------------------------------------------------

    #[test]
    fn test_group_key_bitmap_bit_positions() {
        // Two packed group columns, only the second nullable: the bitmap must set
        // **bit 1**, not bit 0. Getting it wrong silently merges a NULL group with
        // a `0` group, and the end-to-end coverage groups on one nullable column,
        // where every wrong position still lands on bit 0.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0), // A: NOT NULL   → packed slot 0
                SchemaColumn::new(type_code::U32, 1), // B: nullable   → packed slot 1
            ],
            &[0],
        );
        // Row 0: B is NULL (payload slot 1 → null-word bit 1). Row 1: B == 0.
        let mut b = Batch::with_capacity(schema, 2);
        for (pk, null_word) in [(10u128, 1u64 << 1), (20u128, 0u64)] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&null_word.to_le_bytes());
            b.extend_col(0, &7i64.to_le_bytes()); // A, same in both rows
            b.extend_col(1, &0u32.to_le_bytes()); // B: the canonical zero under a NULL
            b.count += 1;
        }
        let mb = b.as_mem_batch();

        let packer = ReindexPacker::new_group_key(&schema, &[1, 2]);
        assert_eq!(packer.out_stride, 1 + 8 + 4, "bitmap ++ I64 slot ++ U32 slot");

        let mut null_row = [0u8; crate::schema::MAX_PK_BYTES];
        let mut zero_row = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut null_row[..packer.out_stride], &mb, 0);
        packer.pack_into(&mut zero_row[..packer.out_stride], &mb, 1);

        assert_eq!(null_row[0], 0b10, "NULL in packed column 1 sets bit 1, not bit 0");
        assert_eq!(zero_row[0], 0, "no NULL, no bits");
        // The NULL slot is zeroed and B == 0 encodes to zeros, so the bitmap byte
        // is the *only* thing separating a NULL group from a `0` group.
        assert_eq!(
            &null_row[1..packer.out_stride],
            &zero_row[1..packer.out_stride],
            "the two rows differ in nothing but the bitmap"
        );
        assert_ne!(
            &null_row[..packer.out_stride],
            &zero_row[..packer.out_stride],
            "a NULL group must not collide with a 0 group"
        );
    }

    // -----------------------------------------------------------------------
    // ReindexPacker::pack_into — property test over every PK-eligible type
    // -----------------------------------------------------------------------

    mod pack_proptest {
        use super::*;
        use crate::test_support::{arb_pk_type, pk_only_schema};
        use proptest::prelude::*;

        /// A legal carried promotion target: the widest slot of the source's own
        /// signedness. For the already-widest codes that is the self-derived type,
        /// so the two passes below are always legal, not always distinct.
        fn carried_target(tc: u8) -> u8 {
            match tc {
                type_code::U8 | type_code::U16 | type_code::U32 | type_code::U64 => type_code::U64,
                type_code::I8 | type_code::I16 | type_code::I32 | type_code::I64 => type_code::I64,
                type_code::I128 => type_code::I128,
                _ => type_code::U128, // U128, UUID
            }
        }

        /// `(column type codes, one native-LE value per column)`. 1..=MAX_PK_COLUMNS
        /// columns over every PK-eligible code, at every width combination — the
        /// surface the running-sum slot offsets live on.
        fn arb_key_case() -> impl Strategy<Value = (Vec<u8>, Vec<Vec<u8>>)> {
            prop::collection::vec(arb_pk_type(), 1..=crate::schema::MAX_PK_COLUMNS).prop_flat_map(|types| {
                let vals: Vec<_> = types
                    .iter()
                    .map(|&t| prop::collection::vec(any::<u8>(), gnitz_wire::wire_stride(t)))
                    .collect();
                (Just(types), vals)
            })
        }

        proptest! {
            /// At every arity and every PK-eligible type, self-derived and carried:
            /// each slot equals its own wire encoder's output at the offset the
            /// running sum put it, **and** the two placements agree with each
            /// other. The second does not follow from the first — PK and payload
            /// placement go through different primitives — and it is the
            /// co-partition contract this file exists for.
            #[test]
            fn reindex_pack_matches_wire_encoders((types, vals) in arb_key_case()) {
                let n = types.len();

                // Payload placement: [U64 PK, c0 .. cn-1], key = the payload columns.
                let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
                cols.extend(types.iter().map(|&tc| SchemaColumn::new(tc, 0)));
                let pay_schema = SchemaDescriptor::new(&cols, &[0]);
                let mut pb = Batch::with_capacity(pay_schema, 1);
                pb.extend_pk(1u128);
                pb.extend_weight(&1i64.to_le_bytes());
                pb.extend_null_bmp(&0u64.to_le_bytes());
                for (i, v) in vals.iter().enumerate() {
                    pb.extend_col(i, v);
                }
                pb.count += 1;
                let pay_mb = pb.as_mem_batch();

                // PK placement: the same columns, all of them PK columns, OPK at rest.
                let pk_schema = pk_only_schema(&types);
                let mut opk = Vec::new();
                for (i, v) in vals.iter().enumerate() {
                    let mut slot = vec![0u8; v.len()];
                    gnitz_wire::encode_pk_column(v, types[i], &mut slot);
                    opk.extend_from_slice(&slot);
                }
                let mut kb = Batch::with_capacity(pk_schema, 1);
                kb.extend_pk_bytes(&opk);
                kb.extend_weight(&1i64.to_le_bytes());
                kb.extend_null_bmp(&0u64.to_le_bytes());
                kb.count += 1;
                let pk_mb = kb.as_mem_batch();

                let pay_cols: Vec<u32> = (1..=n as u32).collect();
                let key_cols: Vec<u32> = (0..n as u32).collect();

                for carried in [false, true] {
                    let targets: Vec<u8> = types
                        .iter()
                        .map(|&tc| if carried { carried_target(tc) } else { 0 })
                        .collect();
                    let pay_packer = ReindexPacker::new(&pay_schema, &pay_cols, &targets).unwrap();
                    let pk_packer = ReindexPacker::new(&pk_schema, &key_cols, &targets).unwrap();
                    let stride = pay_packer.out_stride;
                    prop_assert_eq!(stride, pk_packer.out_stride);

                    let mut pay_buf = [0u8; crate::schema::MAX_PK_BYTES];
                    let mut pk_buf = [0u8; crate::schema::MAX_PK_BYTES];
                    pay_packer.pack_into(&mut pay_buf[..stride], &pay_mb, 0);
                    pk_packer.pack_into(&mut pk_buf[..stride], &pk_mb, 0);

                    // (1) Absolute.
                    let (mut off, mut src_off) = (0usize, 0usize);
                    for (i, &tc) in types.iter().enumerate() {
                        let out_tc = gnitz_wire::resolve_reindex_type(tc, targets[i]);
                        let w = gnitz_wire::wire_stride(out_tc);
                        let src_w = gnitz_wire::wire_stride(tc);

                        let mut want_pay = vec![0u8; w];
                        gnitz_wire::encode_pk_column_promoted(&vals[i], tc, out_tc, &mut want_pay);
                        prop_assert_eq!(&pay_buf[off..off + w], &want_pay[..], "payload slot {}", i);

                        let mut want_pk = vec![0u8; w];
                        gnitz_wire::promote_opk_column(&opk[src_off..src_off + src_w], tc, out_tc, &mut want_pk);
                        prop_assert_eq!(&pk_buf[off..off + w], &want_pk[..], "pk slot {}", i);

                        off += w;
                        src_off += src_w;
                    }
                    prop_assert_eq!(off, stride, "slot widths sum to out_stride");

                    // (2) Cross-placement.
                    prop_assert_eq!(&pay_buf[..stride], &pk_buf[..stride]);
                }
            }
        }
    }

    /// Release-only microbench for `pack_into` — once per equijoin delta row and
    /// per scatter row. Both packer shapes: a 3-column join key (offset sum, no
    /// null word) and a nullable 2-column group key (bitmap + null-word read).
    /// `cd crates && cargo test -p gnitz-engine --release reindex_pack_bench -- --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore]
    fn reindex_pack_bench() {
        use std::hint::black_box;
        use std::time::Instant;

        const N: usize = 1_000_000;
        const ITERS: usize = 20;

        // --- 3-column join key: [U64 PK, U64, U64, U64], reindex on (1, 2, 3).
        let join_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        let mut jb = Batch::with_capacity(join_schema, N);
        for i in 0..N as u64 {
            jb.extend_pk(i as u128);
            jb.extend_weight(&1i64.to_le_bytes());
            jb.extend_null_bmp(&0u64.to_le_bytes());
            jb.extend_col(0, &i.wrapping_mul(2_654_435_761).to_le_bytes());
            jb.extend_col(1, &i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
            jb.extend_col(2, &(!i).to_le_bytes());
            jb.count += 1;
        }
        let jmb = jb.as_mem_batch();
        let join_packer = ReindexPacker::new(&join_schema, &[1, 2, 3], &[]).unwrap();
        assert_eq!(join_packer.out_stride, 24);

        // --- Nullable 2-column group key: [U64 PK, I64, U32 NULL], group on (1, 2).
        let grp_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::U32, 1),
            ],
            &[0],
        );
        let mut gb = Batch::with_capacity(grp_schema, N);
        for i in 0..N as u64 {
            gb.extend_pk(i as u128);
            gb.extend_weight(&1i64.to_le_bytes());
            // Every 8th row is NULL in the nullable group column.
            gb.extend_null_bmp(&(u64::from(i % 8 == 0) << 1).to_le_bytes());
            gb.extend_col(0, &(i as i64).wrapping_mul(-7).to_le_bytes());
            gb.extend_col(1, &(i as u32).to_le_bytes());
            gb.count += 1;
        }
        let gmb = gb.as_mem_batch();
        let grp_packer = ReindexPacker::new_group_key(&grp_schema, &[1, 2]);

        for (name, packer, mb) in [("join3", &join_packer, &jmb), ("group2-nullable", &grp_packer, &gmb)] {
            let stride = packer.out_stride;
            let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
            // Warm up.
            packer.pack_into(&mut buf[..stride], mb, 0);

            let t = Instant::now();
            let mut acc = 0u64;
            for _ in 0..ITERS {
                for row in 0..N {
                    packer.pack_into(&mut buf[..stride], mb, row);
                    acc = acc.wrapping_add(black_box(buf[0]) as u64);
                }
            }
            let secs = t.elapsed().as_secs_f64();
            println!(
                "reindex_pack_bench[{name}]: {:.1} Mrows/s ({N} rows x {ITERS} iters in {secs:.3}s, stride {stride}, checksum {acc})",
                (N * ITERS) as f64 / secs / 1e6,
            );
        }
    }
}
