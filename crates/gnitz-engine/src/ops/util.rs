//! Shared helpers used by ≥2 sub-modules.

use crate::foundation::xxh::RowHasher;

use crate::schema::{ColumnLocator, SchemaDescriptor, TypeCode};
use crate::storage::ReadCursor;
use gnitz_expr::RowSource;

// ---------------------------------------------------------------------------
// Order-preserving aggregate-value codec (AVI keys)
// ---------------------------------------------------------------------------
//
// `encode_ordered` (index population) and `decode_ordered` (AVI scan in
// `agg::apply_agg_from_value_index`) are the two halves of one codec: encode
// maps a value to a u64 whose unsigned ordering matches the value's natural
// ordering, decode recovers the original bits. They must stay mutual inverses.

/// Width in bytes of the order-encoded aggregate value appended to every AVI
/// composite key (`group_key_bytes ++ av_encoded`). The codec always emits a
/// u64, so this is the trailing-segment length the index schema, the population
/// loop, and the lookup all agree on.
pub(crate) const AVI_AV_BYTES: usize = 8;

#[inline]
fn ieee_order_bits(raw_bits: u64) -> u64 {
    if raw_bits >> 63 != 0 {
        !raw_bits
    } else {
        raw_bits ^ (1u64 << 63)
    }
}

#[inline]
fn ieee_order_bits_reverse(encoded: u64) -> u64 {
    if encoded >> 63 != 0 {
        encoded ^ (1u64 << 63)
    } else {
        !encoded
    }
}

/// IEEE 754 order-preserving encoding for 32-bit floats, returning u64.
/// Checks the F32 sign bit (bit 31), not bit 63.
#[inline]
pub(super) fn ieee_order_bits_f32(raw_bits: u32) -> u64 {
    (if raw_bits >> 31 != 0 {
        !raw_bits
    } else {
        raw_bits ^ (1u32 << 31)
    }) as u64
}

/// Reverse of [`ieee_order_bits_f32`].
#[inline]
pub(super) fn ieee_order_bits_f32_reverse(encoded: u64) -> u32 {
    let e = encoded as u32;
    if e >> 31 != 0 {
        e ^ (1u32 << 31)
    } else {
        !e
    }
}

/// Order-preserving u64 encoding of a fixed-width aggregate value held in
/// `bytes` (native little-endian). A PK aggregate column's at-rest bytes are
/// the OPK window (big-endian, sign-flipped) — callers read them through
/// `ColumnLocator::native_le_bytes`, so a PK aggregate column encodes
/// identically to the same value in a payload column. `for_max` inverts
/// the order so the cursor's ascending walk yields the maximum first. Width is
/// ≤ 8 (F32 is 4); U128/UUID/String/Blob are excluded upstream by
/// `agg_value_idx_eligible`.
#[inline]
pub(super) fn encode_ordered(bytes: &[u8], col_type_code: TypeCode, for_max: bool) -> u64 {
    let val = match col_type_code {
        TypeCode::F32 => ieee_order_bits_f32(u32::from_le_bytes(bytes[..4].try_into().unwrap())),
        TypeCode::F64 => ieee_order_bits(u64::from_le_bytes(bytes[..8].try_into().unwrap())),
        TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 => gnitz_wire::read_unsigned_exact(bytes),
        TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => {
            (gnitz_wire::read_signed_exact(bytes) as u64).wrapping_add(1u64 << 63)
        }
        TypeCode::String | TypeCode::Blob | TypeCode::U128 | TypeCode::UUID | TypeCode::I128 => unreachable!(
            "AVI agg type {col_type_code:?} is not order-encodable (compile-rejected by \
             agg_value_idx_eligible)"
        ),
    };
    if for_max {
        !val
    } else {
        val
    }
}

/// Inverse of [`encode_ordered`] at `for_max == false`: recover the original
/// value's raw little-endian bits (IEEE bits for floats, two's-complement for
/// signed, the value itself for unsigned). A `for_max` key is un-inverted by its
/// reader before it gets here.
#[inline]
pub(super) fn decode_ordered(e: u64, col_type_code: TypeCode) -> u64 {
    match col_type_code {
        TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => (e as i64).wrapping_sub(1i64 << 63) as u64,
        TypeCode::F64 => ieee_order_bits_reverse(e),
        TypeCode::F32 => {
            // The accumulator and the reduce output column are F64; promote the
            // recovered F32 to F64 bits so the AVI seed matches the
            // step_from_batch path.
            let f32_bits = ieee_order_bits_f32_reverse(e);
            f64::to_bits(f32::from_bits(f32_bits) as f64)
        }
        TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 => e,
        TypeCode::String | TypeCode::Blob | TypeCode::U128 | TypeCode::UUID | TypeCode::I128 => unreachable!(
            "AVI agg type {col_type_code:?} is not order-encodable (compile-rejected by \
             agg_value_idx_eligible)"
        ),
    }
}

// ---------------------------------------------------------------------------
// Group key helpers (shared by reduce, exchange)
// ---------------------------------------------------------------------------

/// Feed a German-string column's content into `hasher` as a length-prefixed
/// byte run: a 4-byte LE length, then the content (following the heap pointer
/// for long strings). The length prefix keeps "ab"+"c" from aliasing "a"+"bc"
/// across adjacent columns. Shared by the group-key fold below and the set-op
/// row-identity hash (`reindex_hash_row`); the two MUST agree byte-for-byte so a
/// string key routes to — and dedups against — the partition it belongs to.
#[inline]
pub(super) fn hash_german_string_content(hasher: &mut RowHasher, struct_bytes: &[u8], blob: &[u8]) {
    let content = gnitz_wire::german_string_content(struct_bytes, blob);
    hasher.update(&(content.len() as u32).to_le_bytes());
    hasher.update(content);
}

/// The 128-bit group key of the **empty** group-column set — the constant `V₀` a
/// global (ungrouped) aggregate folds to. Batch-free: identical to the XXH3 empty
/// digest `extract_group_key(.., &[])` produces over any populated row (the
/// zero-column fold reads no row content), but without touching a batch, so it is
/// callable at emit time (no batch exists) and over an empty delta — where
/// `extract_group_key`'s unconditional `null_word()` read would index an empty
/// slice and panic (a safe slice index, so it panics in release too). The engine's
/// seam onto `V₀`: the owner-bake's `worker_for_key`, the seed's
/// `emit_global_ground` PK and `trace_out` probe all route through this, while the
/// runtime router and PK-stamp use the equal `extract_group_key` over a real row —
/// so every site agrees byte-for-byte with no embedded literal. `V₀` itself is
/// defined in `gnitz-wire`, so the SQL side's synthesized ground row shares the
/// constant rather than restating it.
#[inline]
pub(crate) fn global_group_key() -> u128 {
    gnitz_wire::global_group_key()
}

/// Whether the group key of `group_by_cols` can be emitted through the
/// canonical (order-preserving) fast path — `ColumnLocator::route_key` on the
/// single group column — rather than the XXH3 fold (multi-column, nullable, or
/// non-routable type). Two shapes qualify: a single PK (sub-)column, whose OPK
/// window widens directly; and a single non-nullable routable-int payload
/// column, which OPK-encodes then widens to the same image — so a value routes
/// identically whether it is the PK on one side of a join or a payload FK on
/// the other. `route_key` dispatches on the locator, so the two need no
/// separate arm here.
///
/// `op_reduce` upgrades its `trace_out` retraction probe to the monotone
/// `advance_to` exactly when this is true: a canonical key ascends in
/// group-visit order because the group comparator, the route-key encoding, and
/// the OPK truncation to the output stride agree on one byte order — the
/// agreement `op_reduce`'s debug tripwire and the monotone-probe tests pin.
#[inline]
pub(super) fn single_col_canonical_group_key(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> bool {
    if group_by_cols.len() != 1 {
        return false;
    }
    let c = group_by_cols[0] as usize;
    if schema.is_pk_col(c) {
        return true;
    }
    let col = &schema.columns[c];
    col.nullable == 0 && gnitz_wire::is_pk_eligible(col.type_code)
}

/// Hash one group column into the fold-path digest. The single per-column
/// body shared by the schema-walking [`extract_group_key`] and the baked
/// [`GroupKeyCols::key_row`] — a divergence would silently merge or split
/// groups in the non-linear REDUCE fallback (wrong MIN/MAX).
///
/// Reads the null bit unconditionally, like the sibling `compare_by_group_cols`:
/// a NOT NULL column never carries one, so masking it off would cost a per-row
/// AND to change nothing.
#[inline]
fn hash_group_col<R: RowSource>(hasher: &mut RowHasher, src: &R, row: usize, null_word: u64, loc: ColumnLocator) {
    match loc {
        ColumnLocator::Pk { .. } => {
            // PK columns are non-nullable; canonical OPK-derived route key, so
            // a PK sub-column hashes like the same value as a payload FK on a
            // join's other side.
            hasher.update(&[1u8]); // non-null marker
            hasher.update(&loc.route_key(src, row).to_le_bytes());
        }
        ColumnLocator::Payload { slot, size, type_code } => {
            if gnitz_wire::null_word_get(null_word, slot as usize) {
                hasher.update(&[0u8]); // null marker
                return;
            }
            hasher.update(&[1u8]); // non-null marker
            if gnitz_wire::is_german_string(type_code) {
                // STRING and BLOB both hash length-prefixed content via the shared
                // helper (matching reindex_hash_row). BLOB takes this path too: it
                // shares the 16-byte German-string struct, so hashing the struct
                // instead of the content would key on a heap pointer. `size` is
                // already 16 for both.
                hash_german_string_content(hasher, src.get_col_ptr(row, slot as usize, size as usize), src.blob());
            } else {
                // Canonical (sign-flipped/widened) value: a payload FK hashes like
                // the same value stored as a PK column — the same `route_key` the
                // Pk arm takes, which is exactly why the two agree. This covers
                // U128/UUID too: `payload_route_key`'s arm for them is
                // `u128::from_le_bytes(cell)`, so its `to_le_bytes()` *is* the
                // 16-byte cell.
                hasher.update(&loc.route_key(src, row).to_le_bytes());
            }
        }
    }
}

/// Extract the 128-bit group key of one row — the one group-key hash body,
/// generic over any [`RowSource`] row: a `MemBatch` row and a `ReadCursor`'s
/// current row hash byte-identically, so a trace row routes to the delta group
/// it belongs to.
#[inline]
pub(super) fn extract_group_key<R: RowSource>(
    src: &R,
    row: usize,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> u128 {
    // Canonical single-column fast path, dispatched by the shared
    // `single_col_canonical_group_key` classifier (see its doc). Nullable
    // columns (the hash loop handles NULL distinctly) and STRING/BLOB/F32/F64
    // fall through to the hash loop — a zero-extended content prefix is not a
    // valid routing key for them.
    if single_col_canonical_group_key(schema, group_by_cols) {
        return schema.locate(group_by_cols[0] as usize).route_key(src, row);
    }

    // Fold path: multi-column GROUP BY, a single STRING/BLOB column, or a single
    // nullable int/U128/UUID column. Stream the same canonical per-column
    // material into a true 128-bit Xxh3 digest, gaining full 128-bit entropy
    // (matching reindex_hash_row). Order-sensitivity comes from the stream
    // itself: every column emits exactly one null marker before its content, so
    // (a, b) ≠ (b, a) and (NULL, v) ≠ (v, NULL) without an explicit index byte,
    // and STRING content is length-prefixed so "ab"+"c" can't alias "a"+"bc".
    let null_word = src.get_null_word(row);
    // Function-local streaming hasher: `RowHasher::new()` only copies the
    // initial accumulator (the 256-byte buffer is `MaybeUninit`), so it is as
    // cheap as `reset()` — no thread-local or per-row reuse needed.
    let mut hasher = RowHasher::new();
    for &c_idx_u32 in group_by_cols {
        hash_group_col(&mut hasher, src, row, null_word, schema.locate(c_idx_u32 as usize));
    }
    hasher.digest128()
}

/// The baked form of [`extract_group_key`]: per-column locators resolved once at
/// plan-bake time, for per-row hot loops (the non-linear REDUCE fallback's
/// per-trace-row routing). Consumes the same bodies
/// (`ColumnLocator::route_key` / `hash_group_col`) as the schema-walking form,
/// so a baked key and an ad-hoc key are byte-identical by construction.
pub(super) struct GroupKeyCols {
    canonical: bool,
    cols: Vec<ColumnLocator>,
}

impl GroupKeyCols {
    pub(super) fn new(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> Self {
        GroupKeyCols {
            canonical: single_col_canonical_group_key(schema, group_by_cols),
            cols: group_by_cols.iter().map(|&c| schema.locate(c as usize)).collect(),
        }
    }

    /// The 128-bit group key of `row` — equals `extract_group_key` over the
    /// same (schema, group_by_cols) this was baked from.
    #[inline]
    pub(super) fn key_row<R: RowSource>(&self, src: &R, row: usize) -> u128 {
        if self.canonical {
            return self.cols[0].route_key(src, row);
        }
        let null_word = src.get_null_word(row);
        let mut hasher = RowHasher::new();
        for &loc in &self.cols {
            hash_group_col(&mut hasher, src, row, null_word, loc);
        }
        hasher.digest128()
    }

    /// [`Self::key_row`] over a `ReadCursor`'s current row.
    #[inline]
    pub(super) fn key_cursor(&self, cursor: &ReadCursor) -> u128 {
        let (src, row) = cursor.current_row_source();
        self.key_row(src, row)
    }
}

#[cfg(test)]
mod group_key_tests {
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::Batch;

    // The batch-free `global_group_key()` (used at emit time and over an empty
    // delta) must equal the runtime `extract_group_key(.., &[])` over a real,
    // arbitrarily-populated row — they are the SAME V₀, so routing, PK-stamp,
    // owner-bake and seed all agree byte-for-byte. Row content must not perturb
    // it (the empty-column fold reads nothing).
    #[test]
    fn global_group_key_matches_runtime_empty_cols() {
        use super::{extract_group_key, global_group_key};
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );
        let v0 = global_group_key();
        for (pk, payload, null) in [(1u128, 42i64, false), (999, -7, false), (0, 0, true)] {
            let mut b = Batch::with_capacity(schema, 1);
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&(null as u64).to_le_bytes());
            b.extend_col(0, &payload.to_le_bytes());
            b.count += 1;
            let mb = b.as_mem_batch();
            assert_eq!(
                extract_group_key(&mb, 0, &schema, &[]),
                v0,
                "extract_group_key over empty group cols must equal global_group_key regardless of row content",
            );
        }
    }

    // Co-partition invariant (bug #2 regression): a single narrow-int
    // routing/group column must yield the canonical OPK key — the value an
    // OPK PK column produces via `widen_pk_be` (native for unsigned, sign-
    // flipped for signed). Pre-OPK both PK and payload sides used the raw
    // native value; after the flip the PK side is sign-flipped, so a signed
    // payload FK must match it or a distributed join silently drops rows.
    #[test]
    fn extract_group_key_single_narrow_int_canonical_widen() {
        use super::extract_group_key;
        use crate::storage::Batch as B;

        // Group key for `le` (native LE bytes) stored as a payload column at
        // idx 1 (U64 PK + tested column), routed by col 1.
        let key_as_payload = |tc: u8, le: &[u8]| -> u128 {
            let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)], &[0]);
            let pi = schema.try_payload_idx(1).unwrap();
            let mut b = B::with_capacity(schema, 1);
            b.extend_pk(0u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(pi, le);
            b.count += 1;
            extract_group_key(&b.as_mem_batch(), 0, &schema, &[1])
        };

        // Signed I32: canonical key is the sign-flipped value (top bit toggled
        // on the native bits), NOT the old zero-extended native value.
        for (v, expected) in [
            (1i32, 0x8000_0001u128),
            (-1, 0x7FFF_FFFF),
            (2, 0x8000_0002),
            (100, 0x8000_0064),
            (i32::MIN, 0x0000_0000),
            (i32::MAX, 0xFFFF_FFFF),
        ] {
            assert_eq!(
                key_as_payload(type_code::I32, &v.to_le_bytes()),
                expected,
                "I32 v={v}: canonical sign-flipped key",
            );
        }

        // Unsigned U16: canonical key equals the native value (OPK == native).
        for v in [0u16, 1, 0xBEEF, u16::MAX] {
            assert_eq!(key_as_payload(type_code::U16, &v.to_le_bytes()), v as u128);
        }
    }
}
