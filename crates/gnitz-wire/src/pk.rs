//! Order-preserving primary-key (OPK) byte encoding.
//!
//! A PK region at rest holds **order-preserving big-endian** bytes: for every
//! pair of encoded keys `memcmp(a, b)` equals the typed lexicographic
//! comparison of the PK columns. Both the client write path (`gnitz-core`) and
//! the server read path (`gnitz-engine`) encode/decode through these functions,
//! so the primitive lives here in `gnitz-wire`, the crate both depend on.
//!
//! All PK columns are fixed-width integer scalars (floats/strings/blobs are
//! rejected at DDL), so the transform is a fixed-width bijection: unsigned types
//! map to big-endian, signed types map to big-endian with the sign bit flipped.

#[cfg(test)]
use crate::type_code;

/// Order-preserving big-endian encoding of one PK column.
///
/// `src` and `dst` are both exactly `col.size()` bytes (1/2/4/8/16). Native
/// little-endian input is byte-reversed to big-endian; signed types additionally
/// flip the sign bit so the signed range maps monotonically onto the unsigned
/// range. The result's unsigned lexicographic order equals the numeric order of
/// the source value.
///
/// The sign flip rides in the integer, not in `dst`: the sign bit *is* the top
/// bit of the big-endian image, so XOR-ing it before the store keeps the whole
/// transform one load, one `bswap` and one store — a trailing `dst[0] ^= 0x80`
/// would be a read-modify-write of bytes just written. [`decode_pk_column`] is
/// the exact mirror.
#[inline]
pub fn encode_pk_column(src: &[u8], tc: u8, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), src.len());
    let flip = crate::is_signed_int(tc);
    match dst.len() {
        16 => {
            let v = u128::from_le_bytes(src.try_into().unwrap()) ^ ((flip as u128) << 127);
            dst.copy_from_slice(&v.to_be_bytes());
        }
        8 => {
            let v = u64::from_le_bytes(src.try_into().unwrap()) ^ ((flip as u64) << 63);
            dst.copy_from_slice(&v.to_be_bytes());
        }
        4 => {
            let v = u32::from_le_bytes(src.try_into().unwrap()) ^ ((flip as u32) << 31);
            dst.copy_from_slice(&v.to_be_bytes());
        }
        2 => {
            let v = u16::from_le_bytes(src.try_into().unwrap()) ^ ((flip as u16) << 15);
            dst.copy_from_slice(&v.to_be_bytes());
        }
        1 => dst[0] = src[0] ^ ((flip as u8) << 7),
        other => unreachable!("PK column size must be 1/2/4/8/16, got {other}"),
    }
}

/// OPK-encode a whole PK tuple: [`encode_pk_column`] over `cols` — the PK
/// columns as `(width, type_code)` in **PK-list order** — tightly packed, no
/// inter-column padding (§6). `src` and `dst` are both the tuple's `pk_stride`
/// bytes.
///
/// The one packing walk, so a caller cannot pair the right per-column encoder
/// with the wrong column order: PK-list order is what makes the result's
/// unsigned byte comparison the typed PK order, and it is independent of column
/// order (`PRIMARY KEY (b, a)`).
#[inline]
pub fn encode_pk_tuple(cols: impl IntoIterator<Item = (usize, u8)>, src: &[u8], dst: &mut [u8]) {
    let mut off = 0;
    for (cs, tc) in cols {
        encode_pk_column(&src[off..off + cs], tc, &mut dst[off..off + cs]);
        off += cs;
    }
    debug_assert_eq!(off, dst.len(), "pk tuple width != sum of column widths");
}

/// Symmetric inverse of [`encode_pk_column`]: decode an OPK column back to
/// native little-endian bytes. `src` and `dst` are both `col.size()` bytes. The
/// big-endian image is read, its sign bit un-flipped for signed types, and the
/// native little-endian value stored — the mirror of the encoder, arm for arm,
/// and likewise never a read-modify-write of `dst`.
#[inline]
pub fn decode_pk_column(src: &[u8], tc: u8, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), src.len());
    let flip = crate::is_signed_int(tc);
    match src.len() {
        16 => {
            let v = u128::from_be_bytes(src.try_into().unwrap()) ^ ((flip as u128) << 127);
            dst.copy_from_slice(&v.to_le_bytes());
        }
        8 => {
            let v = u64::from_be_bytes(src.try_into().unwrap()) ^ ((flip as u64) << 63);
            dst.copy_from_slice(&v.to_le_bytes());
        }
        4 => {
            let v = u32::from_be_bytes(src.try_into().unwrap()) ^ ((flip as u32) << 31);
            dst.copy_from_slice(&v.to_le_bytes());
        }
        2 => {
            let v = u16::from_be_bytes(src.try_into().unwrap()) ^ ((flip as u16) << 15);
            dst.copy_from_slice(&v.to_le_bytes());
        }
        1 => dst[0] = src[0] ^ ((flip as u8) << 7),
        other => unreachable!("PK column size must be 1/2/4/8/16, got {other}"),
    }
}

/// [`decode_pk_column`] into an owned 16-byte buffer: the decoded native
/// little-endian value occupies the leading `src.len()` bytes (the column size,
/// which must be ≤ 16). For callers that want an owned scratch buffer rather
/// than threading one through; slice the result with `&buf[..src.len()]`.
///
/// Reached per row through `ColumnLocator::native_le_bytes` (SUM / MIN / MAX
/// accumulation, index-span writes, reindex promotion).
#[inline(always)]
pub fn decode_pk_column_owned(src: &[u8], tc: u8) -> [u8; 16] {
    // `src.len()` is a schema-derived column stride and never exceeds 16; the
    // `buf[..src.len()]` slice below already panics past 16, so document the
    // contract loudly (the promoted ColPromoter Pk arm newly leans on this).
    debug_assert!(src.len() <= 16, "decode_pk_column_owned: column stride > 16");
    let mut buf = [0u8; 16];
    decode_pk_column(src, tc, &mut buf[..src.len()]);
    buf
}

/// Widen a native-LE integer of type `src_tc` into the wider native-LE slot
/// `dst` (`dst.len() >= src.len()`), sign-extending a signed source and
/// zero-extending an unsigned one. This is the one definition of value-
/// preserving integer widening for identity-critical bytes: the cross-width
/// join key promotion ([`encode_pk_column_promoted`]) and the set-op payload
/// promotion (`copy_column`) both go through it, so equal numeric values widen
/// to byte-identical representations on every path.
#[inline]
pub fn widen_native_le(src: &[u8], src_tc: u8, dst: &mut [u8]) {
    let src_width = src.len();
    debug_assert!(dst.len() >= src_width);
    // Native LE: the sign bit is the high bit of the most-significant (last)
    // byte; the extension bytes are appended at the high LE indices.
    let is_neg = crate::is_signed_int(src_tc) && src_width > 0 && (src[src_width - 1] & 0x80) != 0;
    dst[..src_width].copy_from_slice(src);
    dst[src_width..].fill(if is_neg { 0xFF } else { 0x00 });
}

/// OPK-encode a native-LE value of type `src_tc` into a `target_tc` slot.
/// `dst.len() == wire_stride(target_tc) >= src.len()`. [`widen_native_le`]s the
/// value to the target width, then [`encode_pk_column`]s at `target_tc`. When
/// `src_tc == target_tc` this is exactly `encode_pk_column` (no widening) — the
/// no-widening fast path.
///
/// Both the trace-side reindex Map and the delta-scatter routing key go through
/// this single primitive, so equal numeric values from either side of a
/// cross-width join pack into byte-identical keys and co-partition.
#[inline]
pub fn encode_pk_column_promoted(src: &[u8], src_tc: u8, target_tc: u8, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), crate::wire_stride(target_tc));
    if src_tc == target_tc {
        encode_pk_column(src, src_tc, dst);
        return;
    }
    // `dst` is already the target width (asserted above), so read it from there
    // rather than re-deriving it through the `wire_stride` table — the identity
    // arm above is the common per-row case and must not pay for the slow one.
    // The fixed 16-byte `scratch` caps the in-scope target width; every promoted
    // `T` is a PK-eligible ≤16-byte scalar (the decode trust boundary validates
    // this), so this documents the contract a wider future type would have to
    // grow. A violation is a planner/compiler bug, never input — so it must fail
    // loudly rather than silently emit a wrong (mis-joining) key.
    let target_width = dst.len();
    debug_assert!((src.len()..=16).contains(&target_width));

    let mut scratch = [0u8; 16];
    widen_native_le(src, src_tc, &mut scratch[..target_width]);
    encode_pk_column(&scratch[..target_width], target_tc, dst);
}

/// BE value widener for an OPK region slice. Right-aligns (left-zero-pads) the
/// `stride` bytes into a `u128` and reads big-endian, recovering the native
/// value for UNSIGNED PKs (OPK == BE for unsigned). Signed PKs return the OPK
/// value (sign-flipped) — use [`decode_pk_column`] to recover the true integer.
///
/// Schema-free OPK byte primitive (sibling of [`encode_pk_column`]). A stride
/// `> 16` is a wide region and a caller bug. Opposite alignment from a left-
/// aligned sort-key packer; never conflate the two.
///
/// Specialized on the scalar widths, like the left-aligned sort-key packer it
/// mirrors: the general arm's `copy_from_slice` has a runtime length, so it
/// lowers to a zeroed 16-byte stack buffer plus a `memcpy` call, while a whole-
/// width arm is one load and one `bswap`. This is the bottom of every PK→u128
/// conversion — partition routing, XOR8 probes, the merge path. A compound PK
/// region of an unlisted total width (e.g. `(U32, U64)` = 12) falls to the
/// general arm, which is why it stays.
/// `widen_pk_be_matches_the_general_form` pins every stride against it.
#[inline(always)]
pub fn widen_pk_be(pk_bytes: &[u8], stride: usize) -> u128 {
    debug_assert!(
        stride <= NARROW_PK_MAX_BYTES,
        "widen_pk_be: wide PK region (stride {stride})"
    );
    match stride {
        16 => u128::from_be_bytes(pk_bytes[..16].try_into().unwrap()),
        8 => u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128,
        4 => u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128,
        2 => u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128,
        1 => pk_bytes[0] as u128,
        _ => {
            let mut buf = [0u8; 16];
            buf[16 - stride..].copy_from_slice(&pk_bytes[..stride]);
            u128::from_be_bytes(buf)
        }
    }
}

/// Decode one OPK PK column straight to `i64` — the exact inverse of
/// [`encode_pk_column`], fused with the widening [`FixedInt`] defines.
///
/// Spelled with byte-array literals rather than composed from `decode_pk_column`
/// and [`widen_pk_be`]: this runs per row in the evaluator's `LoadPk` opcode, and
/// at `-O0` (the profile the E2E suite runs) that composition costs an
/// out-of-line call plus a 16-byte stack materialization the fused form does not
/// need. `decode_opk_i64_matches_the_two_branches_it_replaces` pins it against
/// both. The assert carries a static message: an `#[inline(always)]` body
/// duplicates a formatted `Arguments` block into every call site.
#[inline(always)]
pub fn decode_opk_i64(opk: &[u8], fi: crate::FixedInt) -> i64 {
    use crate::FixedInt as F;
    debug_assert!(opk.len() == fi.width(), "decode_opk_i64: slice width != FixedInt width");
    match fi {
        F::U8 => opk[0] as i64,
        F::I8 => (opk[0] ^ 0x80) as i8 as i64,
        F::U16 => u16::from_be_bytes([opk[0], opk[1]]) as i64,
        F::I16 => u16::from_be_bytes([opk[0] ^ 0x80, opk[1]]) as i16 as i64,
        F::U32 => u32::from_be_bytes([opk[0], opk[1], opk[2], opk[3]]) as i64,
        F::I32 => u32::from_be_bytes([opk[0] ^ 0x80, opk[1], opk[2], opk[3]]) as i32 as i64,
        F::U64 => u64::from_be_bytes([opk[0], opk[1], opk[2], opk[3], opk[4], opk[5], opk[6], opk[7]]) as i64,
        F::I64 => u64::from_be_bytes([opk[0] ^ 0x80, opk[1], opk[2], opk[3], opk[4], opk[5], opk[6], opk[7]]) as i64,
    }
}

/// Re-encode an at-rest OPK column at a promoted index/join type — the OPK→OPK
/// sibling of [`encode_pk_column_promoted`] (native→OPK).
///
/// `src_tc == target_tc` is the **identity**: [`decode_pk_column`] and
/// [`encode_pk_column`] are documented mutual fixed-width bijections, so
/// decoding and re-encoding at the same type reproduces the input bytes. The
/// fast path copies them verbatim instead. That equality also implies
/// `dst.len() == src_opk.len()` (both are `wire_stride(tc)`), so the copy is
/// length-safe.
///
/// One home for the rule: the secondary-index leading-key span
/// (`IndexKeySpec::write_span`) and the reindex synthetic-key promotion
/// (`ColPromoter`) must produce byte-identical output for the same logical
/// value, and they did so only by spelling the same branch twice.
/// `#[inline(always)]`, not `#[inline]`: this is a per-row call on two hot paths
/// (`IndexKeySpec::write_span`'s PK arm, `ColPromoter::write_into`), and at
/// `opt-level=0` — the debug binary the E2E suite runs — LLVM runs only the
/// always-inline pass, so a plain hint would leave a real call frame around what
/// is otherwise a `copy_from_slice`. (The `debug_assert!` is deliberately not
/// `debug_assert_eq!`: the latter takes both lengths by reference and spills
/// them, for a message the following `copy_from_slice` panic already implies.)
#[inline(always)]
pub fn promote_opk_column(src_opk: &[u8], src_tc: u8, target_tc: u8, dst: &mut [u8]) {
    if src_tc == target_tc {
        debug_assert!(
            dst.len() == src_opk.len(),
            "promote_opk_column: identity width mismatch"
        );
        dst.copy_from_slice(src_opk);
        return;
    }
    // `decode_pk_column_owned` yields exactly the zero-filled native-LE image
    // the encoder wants.
    let native = decode_pk_column_owned(src_opk, src_tc);
    encode_pk_column_promoted(&native[..src_opk.len()], src_tc, target_tc, dst);
}

/// Widest PK region that still fits in a packed `u128` word, the boundary where
/// a key stops fitting one register. At or below it [`widen_pk_be`] recovers the
/// exact key as a `u128` (wider regions must be read as bytes) and
/// [`worker_for_pk_bytes`] routes through that value; above it a key is
/// ordered and hashed as raw bytes.
pub const NARROW_PK_MAX_BYTES: usize = 16;

/// Map a 64-bit hash onto `0..num_workers` — a multiply-shift, no divide and no
/// branch, uniform over the range at every count. The one spelling both routing
/// arms below use.
#[inline(always)]
fn bucket(h: u64, num_workers: usize) -> usize {
    debug_assert!(num_workers >= 1, "worker routing: num_workers must be >= 1");
    ((h as u128 * num_workers as u128) >> 64) as usize
}

/// Upper bound on a cluster's worker count. Every routing surface derives from
/// it: the SAL's per-worker slot arrays, and the relay's per-source scratch. It
/// lives beside the routing functions rather than in the engine's SAL module so
/// the operator layer can size an array by it without an up-edge into L7.
pub const MAX_WORKERS: usize = 64;

/// Which worker owns `key`.
///
/// Multiplicative hash: two Fibonacci multipliers XOR'd together, then
/// [`bucket`]. XXH3 is reserved for filters (xor8, bloom) where collision
/// quality matters.
///
/// The count is a parameter: every producer and consumer of a row must route it
/// against the same cluster shape, so it travels with the call rather than being
/// read from anywhere ambient.
#[inline(always)]
pub fn worker_for_key(pk: u128, num_workers: usize) -> usize {
    let lo = pk as u64;
    let hi = (pk >> 64) as u64;
    bucket(
        lo.wrapping_mul(0x9e3779b97f4a7c15_u64) ^ hi.wrapping_mul(0x6c62272e07bb0142_u64),
        num_workers,
    )
}

/// Route an OPK PK region (any width) to a worker. For a narrow region the OPK
/// bytes are big-endian, so [`widen_pk_be`] right-aligns them to recover the
/// native unsigned value (sign-flipped for signed) and the result is
/// `worker_for_key(widen_pk_be(bytes))` by construction. This is the invariant
/// the join router relies on: `ColumnLocator::route_key` (both PK and
/// OPK-encoded payload paths) also funnels through `widen_pk_be`, so the two
/// sides of a distributed join agree. A wide region takes the xxh3 of the OPK
/// bytes (uniformly distributed already) through the same multiply-shift.
#[inline]
pub fn worker_for_pk_bytes(bytes: &[u8], num_workers: usize) -> usize {
    if bytes.len() <= NARROW_PK_MAX_BYTES {
        worker_for_key(widen_pk_be(bytes, bytes.len()), num_workers)
    } else {
        bucket(crate::checksum(bytes), num_workers)
    }
}

// Two distinct key spaces derive a `u128` from a column. They coincide for
// unsigned types and differ for signed:
//
// * ROUTING (`*_route_key`): the canonical `widen_pk_be(OPK)` value — sign-
//   flipped for signed. Used by exchange/`extract_group_key`, matching
//   `worker_for_pk_bytes`, which is schema-less and *cannot* decode, so it
//   must hash the OPK bytes' widened value. Both sides of a distributed join
//   agree only in this space.
// * INDEX (`*_native_key`): the native value (signed integers keep their
//   two's-complement bits, zero-extended). Used by FK validation, unique-index
//   maintenance, `has_pk`, and `seek_by_index`, which all re-encode native →
//   OPK at the storage boundary (`Table::opk_key`, `batch_project_index`), so
//   they need the native value back, not the sign-flipped one.
//
// Each space has a PK-side and a payload-side reader because the two regions
// store the same logical value differently (OPK big-endian vs native LE); the
// pair agrees by construction, which is what lets a value route and probe the
// same whether it is a PK column or a payload FK.

/// The addressed column's bytes — `col_size` bytes at `offset` — for the four
/// key readers below. One bounds contract and one message for all four, instead
/// of the same `debug_assert!` re-worded per function.
#[inline(always)]
fn cell(data: &[u8], offset: usize, col_size: usize) -> &[u8] {
    debug_assert!(data.len() >= offset + col_size, "key column runs past its region");
    &data[offset..offset + col_size]
}

/// ROUTING key for one PK column's OPK bytes (canonical / sign-flipped).
/// `col_size` is the addressed column's width (≤ 16); `offset` its byte offset
/// within the PK region (0 for a lone PK).
#[inline]
pub fn pk_route_key(pk_bytes: &[u8], offset: usize, col_size: usize) -> u128 {
    widen_pk_be(cell(pk_bytes, offset, col_size), col_size)
}

/// The OPK↔native sign flip for a `col_size`-byte column of type `tc`: the top
/// bit of the column's own width for a signed type, zero otherwise.
///
/// This is the whole difference between the two key spaces. `encode_pk_column`
/// XORs exactly this bit before the byte swap, and the swap itself cancels once
/// both sides are read as integers — so a route key is a native key XOR this,
/// with no encode/decode round trip and no scratch buffer.
#[inline(always)]
fn opk_flip(tc: u8, col_size: usize) -> u128 {
    (crate::is_signed_int(tc) as u128) << (col_size * 8 - 1)
}

/// ROUTING key for one native little-endian payload column (canonical). Integer
/// columns carry the OPK sign flip, so a payload FK column routes to the same
/// partition as the same value stored as a PK column. U128/UUID are unsigned
/// (OPK == native). Float/String/Blob have no PK counterpart; they keep a
/// zero-extended low-8-byte key.
#[inline]
pub fn payload_route_key(col_data: &[u8], offset: usize, col_size: usize, type_code_val: u8) -> u128 {
    // Every schema this reaches passed a decode boundary that rejects an unknown
    // code, so classifying by predicate rather than by decoded enum keeps the
    // tripwire where the tests run without a per-row branch on the release path.
    debug_assert!(
        crate::is_valid_type_code(type_code_val),
        "payload_route_key: unknown type code"
    );
    let src = cell(col_data, offset, col_size);
    if crate::is_wide_int(type_code_val) {
        u128::from_le_bytes(src.try_into().unwrap()) ^ opk_flip(type_code_val, col_size)
    } else if crate::is_float(type_code_val) || crate::is_german_string(type_code_val) {
        crate::read_unsigned_exact(&src[..col_size.min(8)]) as u128
    } else {
        crate::read_unsigned_exact(src) as u128 ^ opk_flip(type_code_val, col_size)
    }
}

/// INDEX key for one PK column's OPK bytes: decode back to the native value
/// (signed bits preserved), zero-extended to `u128`. Feeds `has_pk` /
/// `seek_by_index`, which re-encode native → OPK to hit the OPK-stored index.
/// `offset + col_size` must lie within the OPK PK region (`pk_bytes`); a
/// mismatched `col_size` slices past the column and panics.
#[inline]
pub fn pk_native_key(pk_bytes: &[u8], offset: usize, col_size: usize, type_code_val: u8) -> u128 {
    widen_pk_be(cell(pk_bytes, offset, col_size), col_size) ^ opk_flip(type_code_val, col_size)
}

/// INDEX key for one native little-endian payload column: the native value,
/// zero-extended. U128/UUID read all 16 bytes; narrower types zero-extend the
/// low ≤8 bytes. Float/String/Blob keep the same zero-extended low-8-byte key.
#[inline]
pub fn payload_native_key(col_data: &[u8], offset: usize, col_size: usize, type_code_val: u8) -> u128 {
    debug_assert!(
        crate::is_valid_type_code(type_code_val),
        "payload_native_key: unknown type code"
    );
    let src = cell(col_data, offset, col_size);
    if crate::is_wide_int(type_code_val) {
        u128::from_le_bytes(src.try_into().unwrap())
    } else {
        crate::read_unsigned_exact(&src[..col_size.min(8)]) as u128
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the two key spaces exist for: a value stored as a PK column
    /// and the same value stored as a payload column must produce the same key
    /// in each space, or the two sides of a distributed join land on different
    /// workers (routing) or miss each other's index entries (native).
    #[test]
    fn pk_and_payload_keys_agree_on_one_logical_value() {
        use crate::type_code as tc;
        for &(t, sz) in &[(tc::I8, 1usize), (tc::I16, 2), (tc::I32, 4), (tc::I64, 8), (tc::U32, 4)] {
            for &v in &[i64::MIN, -257, -1, 0, 1, 255, i64::MAX] {
                let le = v.to_le_bytes();
                let native = &le[..sz];
                let mut opk = [0u8; 16];
                encode_pk_column(native, t, &mut opk[..sz]);
                assert_eq!(
                    pk_route_key(&opk[..sz], 0, sz),
                    payload_route_key(native, 0, sz, t),
                    "route keys diverge for tc={t} v={v}",
                );
                assert_eq!(
                    pk_native_key(&opk[..sz], 0, sz, t),
                    payload_native_key(native, 0, sz, t),
                    "native keys diverge for tc={t} v={v}",
                );
            }
        }
        // The native space zero-extends: a signed source keeps its
        // two's-complement bits in the low source-width bytes and is NOT
        // sign-extended to 128 bits (`has_pk` re-encodes from the source width).
        assert_eq!(payload_native_key(&(-1i32).to_le_bytes(), 0, 4, tc::I32), 0xFFFF_FFFF);
        assert_eq!(payload_native_key(&(-1i16).to_le_bytes(), 0, 2, tc::I16), 0xFFFF);
        assert_eq!(payload_native_key(&[0xFFu8], 0, 1, tc::I8), 0xFF);
    }

    /// The three key readers compute the OPK↔native flip arithmetically instead
    /// of round-tripping through `encode_pk_column` / `decode_pk_column`. Pin
    /// each against the primitive it replaced — `pk_route_key` (unchanged) is an
    /// independent oracle for the route space, but both *native* readers were
    /// rewritten, so their mutual cross-check alone would no longer catch a
    /// wrong flip. Swept over every PK-eligible type and the non-PK types the
    /// payload readers must pass through unflipped.
    #[test]
    fn key_readers_match_the_encode_decode_forms_they_replace() {
        use crate::type_code as tc;
        let cases: &[(u8, usize)] = &[
            (tc::U8, 1),
            (tc::I8, 1),
            (tc::U16, 2),
            (tc::I16, 2),
            (tc::U32, 4),
            (tc::I32, 4),
            (tc::U64, 8),
            (tc::I64, 8),
            (tc::U128, 16),
            (tc::UUID, 16),
            (tc::I128, 16),
            (tc::F32, 4),
            (tc::F64, 8),
            (tc::STRING, 16),
            (tc::BLOB, 16),
        ];
        for &(t, sz) in cases {
            for &v in &[0i128, 1, -1, 127, -128, 255, i64::MIN as i128, i64::MAX as i128] {
                let le = (v as u128).to_le_bytes();
                let native = &le[..sz];

                // payload_native_key: the old form decoded U128/UUID/I128 whole
                // and zero-extended everything else.
                let want_native = if crate::is_wide_int(t) {
                    u128::from_le_bytes(native.try_into().unwrap())
                } else {
                    crate::read_unsigned_exact(&native[..sz.min(8)]) as u128
                };
                assert_eq!(payload_native_key(native, 0, sz, t), want_native, "tc={t} v={v}");

                // payload_route_key: the old form OPK-encoded integers into a
                // right-aligned scratch buffer; floats and strings went unflipped.
                let want_route = if crate::is_float(t) || crate::is_german_string(t) {
                    crate::read_unsigned_exact(&native[..sz.min(8)]) as u128
                } else {
                    let mut opk = [0u8; 16];
                    encode_pk_column(native, t, &mut opk[16 - sz..]);
                    u128::from_be_bytes(opk)
                };
                assert_eq!(payload_route_key(native, 0, sz, t), want_route, "tc={t} v={v}");

                // pk_native_key: the old form ran `decode_pk_column` into a
                // zeroed 16-byte buffer. Only PK-eligible types reach it.
                if crate::is_pk_eligible(t) {
                    let mut opk = [0u8; 16];
                    encode_pk_column(native, t, &mut opk[..sz]);
                    let mut want = [0u8; 16];
                    decode_pk_column(&opk[..sz], t, &mut want[..sz]);
                    assert_eq!(
                        pk_native_key(&opk[..sz], 0, sz, t),
                        u128::from_le_bytes(want),
                        "tc={t} v={v}"
                    );
                }
            }
        }
    }

    /// `promote_opk_column`'s identity arm must equal the general
    /// decode-then-re-encode path it short-circuits, at every PK-eligible width.
    #[test]
    fn promote_opk_column_identity_matches_decode_encode() {
        for &(t, sz) in &[
            (crate::type_code::I8, 1usize),
            (crate::type_code::U8, 1),
            (crate::type_code::I16, 2),
            (crate::type_code::U16, 2),
            (crate::type_code::I32, 4),
            (crate::type_code::U32, 4),
            (crate::type_code::I64, 8),
            (crate::type_code::U64, 8),
        ] {
            for &v in &[0i128, 1, -1, i64::MIN as i128, i64::MAX as i128, u64::MAX as i128] {
                let le = (v as u128).to_le_bytes();
                let native = &le[..sz];
                let (mut opk, mut got) = ([0u8; 16], [0u8; 16]);
                encode_pk_column(native, t, &mut opk[..sz]);
                promote_opk_column(&opk[..sz], t, t, &mut got[..sz]);
                let decoded = decode_pk_column_owned(&opk[..sz], t);
                let mut want = [0u8; 16];
                encode_pk_column_promoted(&decoded[..sz], t, t, &mut want[..sz]);
                assert_eq!(got[..sz], want[..sz], "tc={t} v={v}");
                assert_eq!(got[..sz], opk[..sz], "identity must be the verbatim OPK bytes");
            }
        }
    }

    /// The width-specialized arms must agree with the general right-align form
    /// at every stride a PK region can have — including the compound widths
    /// (3, 5, 12, …) that only the general arm serves.
    #[test]
    fn widen_pk_be_matches_the_general_form() {
        let bytes: [u8; 16] = core::array::from_fn(|i| (i as u8).wrapping_mul(37).wrapping_add(1));
        for stride in 1..=16usize {
            let mut buf = [0u8; 16];
            buf[16 - stride..].copy_from_slice(&bytes[..stride]);
            assert_eq!(
                widen_pk_be(&bytes, stride),
                u128::from_be_bytes(buf),
                "stride {stride} diverges from the general form"
            );
        }
        // All-zero and all-ones edges at the specialized widths.
        for stride in [1usize, 2, 4, 8, 16] {
            assert_eq!(widen_pk_be(&[0u8; 16], stride), 0);
            assert_eq!(widen_pk_be(&[0xFFu8; 16], stride), u128::MAX >> (128 - stride * 8));
        }
    }

    fn roundtrip(tc: u8, le: &[u8]) {
        let mut opk = vec![0u8; le.len()];
        encode_pk_column(le, tc, &mut opk);
        let mut back = vec![0u8; le.len()];
        decode_pk_column(&opk, tc, &mut back);
        assert_eq!(back, le, "decode(encode(v)) != v for tc={tc} le={le:?}");
    }

    #[test]
    fn decode_pk_column_roundtrips_signed() {
        for &(tc, sz) in &[
            (type_code::I8, 1usize),
            (type_code::I16, 2),
            (type_code::I32, 4),
            (type_code::I64, 8),
        ] {
            for v in [i64::MIN >> (64 - sz * 8), -1, 0, 1, i64::MAX >> (64 - sz * 8)] {
                roundtrip(tc, &v.to_le_bytes()[..sz]);
            }
        }
    }

    #[test]
    fn decode_pk_column_roundtrips_unsigned() {
        for &(tc, sz) in &[
            (type_code::U8, 1usize),
            (type_code::U16, 2),
            (type_code::U32, 4),
            (type_code::U64, 8),
        ] {
            for v in [0u64, 1, 42, u64::MAX >> (64 - sz * 8)] {
                roundtrip(tc, &v.to_le_bytes()[..sz]);
            }
        }
        // U128 / UUID
        for v in [0u128, 1, 1u128 << 64, u128::MAX] {
            roundtrip(type_code::U128, &v.to_le_bytes());
            roundtrip(type_code::UUID, &v.to_le_bytes());
        }
    }

    /// `decode_opk_i64` is a third spelling of the OPK→native transform, so it
    /// is pinned against the two branches it replaces: the signed
    /// `decode_pk_column` + `read_signed` pair and the unsigned
    /// `widen_pk_be(..) as i64`. A wrong XOR arm is otherwise a silent wrong
    /// answer on every PK predicate.
    #[test]
    fn decode_opk_i64_matches_the_two_branches_it_replaces() {
        use crate::FixedInt as F;
        for &(fi, tc) in &[
            (F::U8, type_code::U8),
            (F::I8, type_code::I8),
            (F::U16, type_code::U16),
            (F::I16, type_code::I16),
            (F::U32, type_code::U32),
            (F::I32, type_code::I32),
            (F::U64, type_code::U64),
            (F::I64, type_code::I64),
        ] {
            let sz = fi.width();
            let (lo, hi) = fi.range();
            for v in [lo, -1, 0, 1, hi] {
                if v < lo || v > hi {
                    continue;
                }
                let le = (v as u128).to_le_bytes();
                let mut opk = [0u8; 8];
                encode_pk_column(&le[..sz], tc, &mut opk[..sz]);

                let want = if crate::is_signed_int(tc) {
                    let mut back = [0u8; 8];
                    decode_pk_column(&opk[..sz], tc, &mut back[..sz]);
                    crate::read_signed_exact(&back[..sz])
                } else {
                    widen_pk_be(&opk[..sz], sz) as i64
                };
                assert_eq!(
                    decode_opk_i64(&opk[..sz], fi),
                    want,
                    "decode_opk_i64 diverges for {fi:?} v={v}"
                );
            }
        }
        // The unsigned 64-bit edge: the i64 register holds the bit pattern, so
        // `u64::MAX` reads back as `-1`, exactly as `widen_pk_be(..) as i64` does.
        let mut opk = [0u8; 8];
        encode_pk_column(&u64::MAX.to_le_bytes(), type_code::U64, &mut opk);
        assert_eq!(decode_opk_i64(&opk, crate::FixedInt::U64), -1i64);
    }

    /// The largest worker count the SAL group format supports
    /// (`runtime::protocol::sal::MAX_WORKERS`). Restated here because
    /// `gnitz-wire` sits below the engine, and the router must be uniform over
    /// exactly the counts the engine can launch.
    const MAX_WORKERS: usize = 64;

    /// Peak worker load divided by the mean, over `keys` at `nw` workers.
    fn load_spread(keys: &[u128], nw: usize) -> f64 {
        let mut counts = vec![0usize; nw];
        for &k in keys {
            counts[worker_for_key(k, nw)] += 1;
        }
        let mean = keys.len() as f64 / nw as f64;
        counts.iter().map(|&c| c as f64 / mean).fold(0.0, f64::max)
    }

    /// The multiply-shift re-bucketing must spread every *structured* key set
    /// evenly at every launchable worker count — the shapes a real PK actually
    /// takes. These sets are deterministic and low-discrepancy under a
    /// golden-ratio multiplier, so 1.01× is achievable rather than merely
    /// plausible; the random arm below carries its own (looser) bound.
    #[test]
    fn router_spreads_structured_keys_evenly() {
        const N: u128 = 100_000;
        let sequential: Vec<u128> = (0..N).collect();
        // i64 spanning zero, in the canonical (sign-flipped) routing space.
        let signed: Vec<u128> = (0..N)
            .map(|i| (i as i64 - N as i64 / 2) as u64 as u128 ^ (1u128 << 63))
            .collect();
        let strided: Vec<u128> = (0..N).map(|i| i * 4096).collect();
        // Compound OPK: (hi, lo) packed as one u128, one half held constant.
        // `const_lo` is also the wide-key arm — it is the u128 shape that varies
        // the high word. A key varying *both* halves in lockstep is not covered
        // here: the mix XORs one multiply per half, so correlated halves
        // partially cancel and measure ~1.02–1.08 at high worker counts.
        // Genuinely independent 128-bit keys (UUIDs) are the random arm below.
        let const_hi: Vec<u128> = (0..N).map(|i| (7u128 << 64) | i).collect();
        let const_lo: Vec<u128> = (0..N).map(|i| (i << 64) | 7).collect();

        for (name, keys) in [
            ("sequential u64", &sequential),
            ("signed i64", &signed),
            ("strided", &strided),
            ("compound const-hi", &const_hi),
            ("compound const-lo", &const_lo),
        ] {
            for nw in 1..=MAX_WORKERS {
                let spread = load_spread(keys, nw);
                assert!(spread <= 1.01, "{name}: worker load {spread:.4}x the mean at nw={nw}",);
            }
        }
    }

    /// Independent 128-bit keys — the UUID shape, and the wide arm the
    /// structured test leaves to this one.
    ///
    /// Random keys carry their own sampling noise: at `nw = 64` and 100k keys,
    /// max/mean sits in 1.044–1.090 for a *correct* router, so the structured
    /// arm's 1.01× would be a flaky test here. 500k keys bring the same
    /// measurement into 1.026–1.037, which 1.05× bounds with room.
    #[test]
    fn router_spreads_random_keys_evenly() {
        const N: usize = 500_000;
        // SplitMix64-style stream over both halves — deterministic, no rand dep.
        let mut s: u64 = 0x243f_6a88_85a3_08d3;
        let mut next = || {
            s = s.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = s;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        };
        let keys: Vec<u128> = (0..N).map(|_| ((next() as u128) << 64) | next() as u128).collect();
        for nw in [1, 2, 3, 4, 8, 16, 32, MAX_WORKERS] {
            let spread = load_spread(&keys, nw);
            assert!(spread <= 1.05, "random keys: {spread:.4}x the mean at nw={nw}");
        }
    }

    /// `worker_for_pk_bytes` on a narrow OPK region is `worker_for_key` of the
    /// widened value, by construction — the invariant that makes a distributed
    /// join's two sides agree, since `ColumnLocator::route_key` funnels through
    /// `widen_pk_be` too.
    #[test]
    fn worker_for_pk_bytes_matches_widened_key() {
        for &(tc, sz) in &[
            (type_code::U8, 1usize),
            (type_code::I8, 1),
            (type_code::U16, 2),
            (type_code::I16, 2),
            (type_code::U32, 4),
            (type_code::I32, 4),
            (type_code::U64, 8),
            (type_code::I64, 8),
            (type_code::U128, 16),
        ] {
            for v in [0i128, 1, -1, 7, -7, 127, -128, 1000, -1000, i32::MAX as i128] {
                let le = (v as u128).to_le_bytes();
                let mut opk = [0u8; 16];
                encode_pk_column(&le[..sz], tc, &mut opk[..sz]);
                for nw in [1usize, 2, 3, 4, 7, 16, MAX_WORKERS] {
                    assert_eq!(
                        worker_for_pk_bytes(&opk[..sz], nw),
                        worker_for_key(widen_pk_be(&opk[..sz], sz), nw),
                        "tc={tc} v={v} nw={nw}",
                    );
                }
            }
        }
    }

    #[test]
    fn opk_order_equiv_signed_i64() {
        // -3 < -1 < 2 must hold byte-lexicographically after encoding.
        let mk = |v: i64| {
            let mut o = [0u8; 8];
            encode_pk_column(&v.to_le_bytes(), type_code::I64, &mut o);
            o
        };
        assert!(mk(-3) < mk(-1));
        assert!(mk(-1) < mk(2));
    }

    #[test]
    fn opk_order_equiv_unsigned_u64() {
        let mk = |v: u64| {
            let mut o = [0u8; 8];
            encode_pk_column(&v.to_le_bytes(), type_code::U64, &mut o);
            o
        };
        assert!(mk(1) < mk(256));
        assert!(mk(256) < mk(u64::MAX));
    }

    #[test]
    fn decode_pk_column_roundtrips_i128() {
        // The signed-128 join-key type: every value (including bit-127 negatives)
        // must survive encode→decode, and the 2^63/2^64 boundaries that
        // distinguish a U64 image from an I64 image round-trip too.
        for v in [
            i128::MIN,
            -1i128,
            0,
            1,
            i128::MAX,
            1i128 << 63,
            (1i128 << 63) - 1,
            1i128 << 64,
            (1i128 << 64) - 1,
        ] {
            roundtrip(type_code::I128, &v.to_le_bytes());
        }
    }

    #[test]
    fn opk_order_equiv_signed_i128() {
        // -3 < -1 < 2 < 2^64 must hold byte-lexicographically after I128 encoding
        // (the signed sign-flip puts negatives below non-negatives at 16-byte width).
        let mk = |v: i128| {
            let mut o = [0u8; 16];
            encode_pk_column(&v.to_le_bytes(), type_code::I128, &mut o);
            o
        };
        assert!(mk(-3) < mk(-1));
        assert!(mk(-1) < mk(2));
        assert!(mk(2) < mk(1i128 << 64));
    }

    /// `encode_pk_column_promoted` with `src_tc == target_tc` is exactly
    /// `encode_pk_column` — the no-widening fast path.
    #[test]
    fn promoted_identity_matches_encode_pk_column() {
        for &(tc, sz) in &[
            (type_code::I8, 1usize),
            (type_code::I16, 2),
            (type_code::I32, 4),
            (type_code::I64, 8),
            (type_code::U8, 1),
            (type_code::U16, 2),
            (type_code::U32, 4),
            (type_code::U64, 8),
            (type_code::U128, 16),
            (type_code::I128, 16),
        ] {
            for v in [0i128, 1, -1, 127, -128, i64::MIN as i128, i64::MAX as i128] {
                let le = v.to_le_bytes();
                let mut expect = vec![0u8; sz];
                encode_pk_column(&le[..sz], tc, &mut expect);
                let mut got = vec![0u8; sz];
                encode_pk_column_promoted(&le[..sz], tc, tc, &mut got);
                assert_eq!(got, expect, "no-widening fast path differs for tc={tc} v={v}");
            }
        }
    }

    // ── §7 co-partition property: both join sides pack equal values identically.

    /// OPK-encode `v` (held in i128, low `wire_stride(tc)` LE bytes are its image)
    /// as source type `tc` into a `target`-width slot, through the exact promoted
    /// encoder both join sides use.
    fn promote(v: i128, tc: u8, target: u8) -> [u8; 16] {
        let le = v.to_le_bytes();
        let mut out = [0u8; 16];
        encode_pk_column_promoted(
            &le[..crate::wire_stride(tc)],
            tc,
            target,
            &mut out[..crate::wire_stride(target)],
        );
        out
    }

    fn assert_copartition(v: i128, l: u8, r: u8, t: u8) {
        let tw = crate::wire_stride(t);
        let (bl, br) = (promote(v, l, t), promote(v, r, t));
        assert_eq!(&bl[..tw], &br[..tw], "byte-identity failed: v={v} L={l} R={r} T={t}");
        assert_eq!(
            widen_pk_be(&bl[..tw], tw),
            widen_pk_be(&br[..tw], tw),
            "widen_pk_be disagreement: v={v} T={t}"
        );
    }

    fn s_min(tc: u8) -> i128 {
        -(1i128 << (crate::wire_stride(tc) * 8 - 1))
    }
    fn s_max(tc: u8) -> i128 {
        (1i128 << (crate::wire_stride(tc) * 8 - 1)) - 1
    }
    fn u_max(tc: u8) -> i128 {
        (1i128 << (crate::wire_stride(tc) * 8)) - 1
    }
    fn narrower(l: u8, r: u8) -> u8 {
        if crate::wire_stride(l) <= crate::wire_stride(r) {
            l
        } else {
            r
        }
    }

    #[test]
    fn signed_ladder_copartitions() {
        use type_code::{I16, I32, I64, I8};
        for (l, r, t) in [
            (I8, I16, I16),
            (I8, I32, I32),
            (I8, I64, I64),
            (I16, I32, I32),
            (I16, I64, I64),
            (I32, I64, I64),
        ] {
            let n = narrower(l, r);
            for v in [0, 1, -1, s_min(n), s_max(n), s_min(n) + 1, s_max(n) - 1] {
                assert_copartition(v, l, r, t);
            }
        }
    }

    #[test]
    fn unsigned_ladder_copartitions() {
        use type_code::{U128, U16, U32, U64, U8, UUID};
        for (l, r, t) in [
            (U8, U16, U16),
            (U8, U32, U32),
            (U8, U64, U64),
            (U16, U32, U32),
            (U16, U64, U64),
            (U32, U64, U64),
            (U32, U128, U128),
            (U64, U128, U128),
            (U32, UUID, U128),
        ] {
            let n = narrower(l, r);
            for v in [0, 1, 127, u_max(n), u_max(n) - 1] {
                assert_copartition(v, l, r, t);
            }
        }
    }

    #[test]
    fn cross_sign_copartitions() {
        use type_code::{I128, I16, I32, I64, I8, U16, U32, U64, U8};
        // (unsigned ≤8B, signed, promoted T) — the full in-scope acceptance table.
        // The U64 rows exercise the new signed-128 target at 16-byte width.
        let cases = [
            (U8, I8, I16),
            (U8, I16, I16),
            (U8, I32, I32),
            (U8, I64, I64),
            (U16, I8, I32),
            (U16, I16, I32),
            (U16, I32, I32),
            (U16, I64, I64),
            (U32, I8, I64),
            (U32, I16, I64),
            (U32, I32, I64),
            (U32, I64, I64),
            (U64, I8, I128),
            (U64, I16, I128),
            (U64, I32, I128),
            (U64, I64, I128),
        ];
        for (u, s, t) in cases {
            // Equal logical values representable on BOTH sides (the overlap
            // [0, min(u_max(u), s_max(s))]) pack byte-identically into T, so equal
            // keys co-partition to the same worker and match in the join.
            let hi = u_max(u).min(s_max(s));
            for v in [0, 1, 127, hi - 1, hi] {
                assert_copartition(v, u, s, t);
            }
            // Injectivity: across a spread drawn from both sides — including the
            // native-byte aliasing trap (e.g. U8 255 and I8 -1 share all-0xFF
            // native bytes; U8 200 and I8 -56 share byte 0xC8) — two promoted
            // T-keys are byte-equal IFF the logical values are equal. No distinct
            // values ever collide; no equal values ever diverge.
            let tw = crate::wire_stride(t);
            let probes: &[(i128, u8)] = &[
                (0, u),
                (1, u),
                (127, u),
                (128, u),
                (200, u),
                (u_max(u) - 1, u),
                (u_max(u), u),
                (s_min(s), s),
                (-56, s),
                (-1, s),
                (0, s),
                (1, s),
                (127, s),
                (s_max(s), s),
            ];
            let mut seen: Vec<(i128, [u8; 16])> = Vec::new();
            for &(val, tc) in probes {
                let key = promote(val, tc, t);
                for &(pv, pk) in &seen {
                    assert_eq!(
                        pk[..tw] == key[..tw],
                        pv == val,
                        "cross-sign T-key equal IFF value equal failed: \
                         {val} vs {pv} (u={u} s={s} t={t})"
                    );
                }
                seen.push((val, key));
            }
        }
    }
}
