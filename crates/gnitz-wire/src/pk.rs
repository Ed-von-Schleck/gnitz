//! Order-preserving primary-key (OPK) byte encoding.
//!
//! A PK region at rest holds **order-preserving big-endian** bytes: for every
//! pair of encoded keys `memcmp(a, b)` equals the typed lexicographic
//! comparison of the PK columns. Both the client write path (`gnitz-core`) and
//! the server read path (`gnitz-server`) encode/decode through these functions,
//! so the primitive lives here in `gnitz-wire`, the crate both depend on.
//!
//! All PK columns are fixed-width integer scalars (floats/strings/blobs are
//! rejected at DDL), so the transform is a fixed-width bijection: unsigned types
//! map to big-endian, signed types map to big-endian with the sign bit flipped.

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
/// the exact mirror, `#[inline(always)]` for the reason stated there.
#[inline(always)]
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
/// inter-column padding. `src` and `dst` are both the tuple's `pk_stride`
/// bytes.
///
/// The one packing walk, so a caller cannot pair the right per-column encoder
/// with the wrong column order: PK-list order is what makes the result's
/// unsigned byte comparison the typed PK order, and it is independent of column
/// order (`PRIMARY KEY (b, a)`).
#[inline(always)]
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
///
/// Each arm stores through a `&mut [u8; W]`, never `copy_from_slice`: LLVM
/// tail-merges four runtime-length copies into one shared `memcpy` call, leaving
/// a caller's row loop an indirect branch and a `call memcpy`. In the type the
/// width propagates and the store is one instruction.
#[inline(always)]
pub fn decode_pk_column(src: &[u8], tc: u8, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), src.len());
    let flip = crate::is_signed_int(tc);
    macro_rules! decode {
        ($ty:ty, $sign_bit:expr) => {{
            const W: usize = std::mem::size_of::<$ty>();
            let v = <$ty>::from_be_bytes(src.try_into().unwrap()) ^ ((flip as $ty) << $sign_bit);
            let d: &mut [u8; W] = dst.try_into().unwrap();
            *d = v.to_le_bytes();
        }};
    }
    match src.len() {
        16 => decode!(u128, 127),
        8 => decode!(u64, 63),
        4 => decode!(u32, 31),
        2 => decode!(u16, 15),
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
/// `> 16` is a wide region and a caller bug. Right-aligned, unlike the engine's
/// left-aligning `pack_pk_be`: this one recovers a value, that one builds a sort
/// key. Never conflate them.
///
/// Specialized on the scalar widths, like the left-aligned sort-key packer it
/// mirrors: the general arm's `copy_from_slice` has a runtime length, so it
/// lowers to a zeroed 16-byte stack buffer plus a `memcpy` call, while a whole-
/// width arm is one load and one `bswap`. This is the bottom of every PK→u128
/// conversion — partition routing, shard PK-filter probes, the merge path. Compound widths
/// 9..=15 (e.g. `(U32, U64)` = 12) get two overlapping loads for the same reason;
/// only 3/5/6/7 still reach the buffer.
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
        // 9..=15: two overlapping big-endian loads instead of a runtime-length
        // `copy_from_slice`, which lowers to an out-of-line `memcpy` per key. With
        // `m = stride - 8`, the low `m` bytes of the tail load are exactly
        // `pk_bytes[8..stride]`, since `stride - m == 8`. This is the routing hash
        // under `worker_for_pk_bytes` — per row on the exchange scatter and the
        // bloom build — and a compound `(U32, U64)` PK lands here at 12.
        9..=15 => {
            let m = stride - 8;
            let hi = u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128;
            let tail = u64::from_be_bytes(pk_bytes[stride - 8..stride].try_into().unwrap());
            (hi << (8 * m)) | ((tail & ((1u64 << (8 * m)) - 1)) as u128)
        }
        // 3/5/6/7: too narrow for the overlapping-load trick (`stride - 8` underflows).
        _ => {
            let mut buf = [0u8; 16];
            buf[16 - stride..].copy_from_slice(&pk_bytes[..stride]);
            u128::from_be_bytes(buf)
        }
    }
}

/// Decode one OPK PK column straight to `i64` — the exact inverse of
/// [`encode_pk_column`], fused with the widening [`crate::types::FixedInt`] defines.
///
/// Spelled with byte-array literals rather than composed from `decode_pk_column`
/// and [`widen_pk_be`]: its production caller is `ColumnLocator::decode_i64`,
/// which the engine's reduce path runs per row, and
/// at `-O0` (the profile the E2E suite runs) that composition costs an
/// out-of-line call plus a 16-byte stack materialization the fused form does not
/// need. The assert carries a static message: an `#[inline(always)]` body
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
/// (`IndexKeySpec::write_span`'s PK arm, `ReindexPacker::pack_into`), and at
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
/// [`bucket`]. XXH3 is reserved for filters (the shard PK filter, bloom) where collision
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
//   flipped for signed. Used by the exchange's group-key fold, matching
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
#[path = "tests/pk.rs"]
mod tests;
