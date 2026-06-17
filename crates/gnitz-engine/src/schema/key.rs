//! Order-preserving primary-key (OPK) primitives — the §9 key cluster.
//!
//! These pure layout/key operations sit *below* both `schema` and `storage`:
//! they encode a PK region to its order-preserving big-endian image, compare two
//! such images with a raw `memcmp`, route a key to a partition, pack a narrow
//! region into a sort key, and carry a width-tagged PK byte buffer. None of them
//! reaches up into storage — the dependency runs `storage → schema::key`, the
//! legitimate downward direction. `storage` re-exports each one from the leaf
//! module that used to own it, so its call sites spell them unchanged.

use std::cmp::Ordering;

use gnitz_wire::encode_pk_column;

use crate::schema::{MAX_PK_BYTES, SchemaDescriptor};

// ---------------------------------------------------------------------------
// Column-aware PK byte-region comparator
// ---------------------------------------------------------------------------

/// Raw byte comparator for PK regions.
///
/// After the OPK-at-rest flip every PK region at rest holds order-preserving
/// big-endian bytes, so unsigned lexicographic byte comparison is numerically
/// identical to the typed comparison of the PK columns for any width. `a.cmp(b)`
/// compiles to an optimal `memcmp` (vectorised for long slices, a single
/// instruction for 8/16-byte keys). `a` and `b` are the OPK bytes produced by
/// `Batch::get_pk_bytes` / `MappedShard::get_pk_bytes`.
#[inline(always)]
pub fn compare_pk_bytes(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

// ---------------------------------------------------------------------------
// Order-preserving PK encoder
// ---------------------------------------------------------------------------

/// Encode a full PK region (`schema.pk_stride()` bytes, columns in pk-list
/// order) into an order-preserving big-endian key. `pk_bytes` and `out` are both
/// `pk_stride` bytes. Iterates `schema.pk_columns()` — the *same* iterator
/// `compare_pk_bytes` walks — so a non-identity `pk_indices` (e.g. `[1, 0]`)
/// encodes in pk-list order, matching the comparator.
///
/// The encoding is **injective**: `encode(a) == encode(b)` iff
/// `a == b` byte-for-byte, because each column's transform is a bijection on its
/// byte range. Consolidation grouping relies on this — an OPK equality test is
/// exactly a PK-byte equality test.
pub(crate) fn encode_order_preserving_pk(
    schema: &SchemaDescriptor,
    pk_bytes: &[u8],
    out: &mut [u8],
) {
    let mut off = 0usize;
    for (_ord, _ci, col) in schema.pk_columns() {
        let cs = col.size() as usize;
        encode_pk_column(&pk_bytes[off..off + cs], col.type_code, &mut out[off..off + cs]);
        off += cs;
    }
}

/// OPK-encode a **native** PK value `key` (handling signed/compound columns)
/// into a stack buffer, returning the buffer and its `pk_stride`. The narrow-PK
/// entry points encode once here and delegate to a byte-addressed sibling that
/// runs the universal OPK lookup. Wide PKs (`pk_stride > 16`) cannot fit a
/// `u128` and must take the byte path directly.
#[inline]
pub(crate) fn opk_key(
    schema: &SchemaDescriptor,
    key: u128,
) -> ([u8; crate::schema::MAX_PK_BYTES], usize) {
    let stride = schema.pk_stride() as usize;
    debug_assert!(stride <= 16, "opk_key: wide PK (stride {stride}); use the byte path");
    let mut opk = [0u8; crate::schema::MAX_PK_BYTES];
    encode_order_preserving_pk(schema, &key.to_le_bytes()[..stride], &mut opk[..stride]);
    (opk, stride)
}

// ---------------------------------------------------------------------------
// Hash routing
// ---------------------------------------------------------------------------

// Multiplicative hash: two Fibonacci multipliers XOR'd together.
// ~4 instructions vs ~20 for XXH3-64; distribution across 256 buckets
// is sufficient for worker routing. XXH3 is reserved for filters
// (xor8, bloom) where collision quality matters.
#[inline(always)]
fn mix(pk: u128) -> usize {
    let lo = pk as u64;
    let hi = (pk >> 64) as u64;
    let h = lo.wrapping_mul(0x9e3779b97f4a7c15_u64)
             ^ hi.wrapping_mul(0x6c62272e07bb0142_u64);
    (h >> 56) as usize
}

#[inline]
pub fn partition_for_key(pk: u128) -> usize {
    mix(pk)
}

/// Route an OPK PK region (any width) to a partition. For `len ≤ 16` the OPK
/// bytes are big-endian, so `widen_pk_be` right-aligns them to recover the
/// native unsigned value (sign-flipped for signed); `mix` of that equals
/// `partition_for_key(widen_pk_be(bytes))`. This is the invariant the join
/// router relies on: `extract_col_key` (both PK and OPK-encoded payload paths)
/// also funnels through `widen_pk_be`, so the two sides of a distributed join
/// agree. For wide regions (`len > 16`) it takes the top 8 bits of xxh3 of the
/// OPK bytes directly (uniformly distributed already).
#[inline]
pub fn partition_for_pk_bytes(bytes: &[u8]) -> usize {
    if bytes.len() <= 16 {
        mix(gnitz_wire::widen_pk_be(bytes, bytes.len()))
    } else {
        (crate::foundation::xxh::checksum(bytes) >> 56) as usize
    }
}

// ---------------------------------------------------------------------------
// Narrow-region PK key packing
// ---------------------------------------------------------------------------

/// BE sort-key packer over an OPK region. Left-aligns the bytes at the MSB end
/// of a `u128` and reads big-endian, so `pack_pk_be(a).cmp(&pack_pk_be(b))`
/// equals the lexicographic byte order of the OPK regions — exactly
/// `compare_pk_bytes`. Narrow (`len ≤ 16`) = the exact key; wide (`len > 16`) =
/// the order-preserving leading-16 prefix (authoritative whenever two prefixes
/// differ; a prefix collision needs a `compare_pk_bytes` tiebreak).
///
/// NOT a value accessor — for a U64 OPK value 1 (`[0,…,0,1]` at `[..8]`) this
/// packs as `1·2^64`, not 1. Sibling of `pack_pk_le`, opposite alignment from
/// `gnitz_wire::widen_pk_be` (right-aligned value recovery); never conflate them.
#[inline(always)]
pub(crate) fn pack_pk_be(pk_bytes: &[u8]) -> u128 {
    let take = pk_bytes.len().min(16);
    let mut buf = [0u8; 16];
    buf[..take].copy_from_slice(&pk_bytes[..take]);
    u128::from_be_bytes(buf)
}

/// Order-preserving sort key for a PK region. After the OPK-at-rest flip the PK
/// bytes are already order-preserving big-endian, so this is just `pack_pk_be`:
///   * `pk_stride ≤ 16`  — the *whole* PK, exact and authoritative.
///   * `pk_stride > 16`   — the order-preserving 16-byte *prefix*, needing a
///     `compare_pk_bytes` tiebreak on a prefix collision.
#[inline]
pub(crate) fn pk_sort_key(pk_bytes: &[u8]) -> u128 {
    pack_pk_be(pk_bytes)
}

// ---------------------------------------------------------------------------
// Width-tagged PK byte buffer
// ---------------------------------------------------------------------------

/// Width-tagged PK byte buffer. Plain value type — no generics, no
/// trait bounds. `len` mirrors the owning table's `pk_stride`, so a
/// manifest round-trip preserves the exact key width. Only
/// `bytes[..len]` is meaningful; the tail is always zero by
/// construction, which lets the single-PK fast path widen `bytes[..len]`
/// to a `u128` with no ambiguity.
#[derive(Clone, Copy)]
pub struct PkBuf {
    pub bytes: [u8; MAX_PK_BYTES],
    pub len: u8,
}

// Prints only the meaningful `bytes[..len]` span (the 80-byte tail is always
// zero by construction), so test assertion diffs over `PkBuf` keys are readable.
impl std::fmt::Debug for PkBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PkBuf({:02x?})", &self.bytes[..self.len as usize])
    }
}

// Manual Eq/Hash compare and hash only bytes[..len], so a HashSet<PkBuf>
// touches pk_stride bytes per key rather than the full 80-byte array.
impl PartialEq for PkBuf {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
            && self.bytes[..self.len as usize] == other.bytes[..other.len as usize]
    }
}
impl Eq for PkBuf {}

impl std::hash::Hash for PkBuf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bytes[..self.len as usize].hash(state);
    }
}

// Enables zero-allocation heterogeneous lookup: a raw &[u8] slice can be
// passed to HashSet<PkBuf>::contains / HashMap<PkBuf, _>::get without
// constructing a PkBuf. The Hash impl above hashes bytes[..len], matching
// <[u8] as Hash>, as the Borrow contract requires.
impl std::borrow::Borrow<[u8]> for PkBuf {
    fn borrow(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

// Byte-lexicographic (`memcmp`) order over `bytes[..len]` — identical to
// `compare_pk_bytes`, the canonical PK comparator, and consistent with the
// `Eq`/`Hash` impls above (which also read only `bytes[..len]`). This is the
// order `seek_first_positive_with_prefix` / `walk_to_positive_with_prefix`
// walk the index in, and the valid merge order for the unique pre-flight
// k-way merge whose keys are OPK leading-key spans of any width.
impl PartialOrd for PkBuf {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PkBuf {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_pk_bytes(self.pk_bytes(), other.pk_bytes())
    }
}

impl PkBuf {
    /// All-zero `bytes`, `len = stride`. The zero-row / placeholder /
    /// empty-shard form.
    pub fn empty(stride: u8) -> Self {
        debug_assert!(stride as usize <= MAX_PK_BYTES);
        PkBuf { bytes: [0u8; MAX_PK_BYTES], len: stride }
    }

    /// `len = slice.len()`, `bytes[..len]` copied from `slice`, tail
    /// zero. The only row constructor: `MappedShard::get_pk_bytes(row)`
    /// returns exactly `pk_stride` bytes, and manifest `parse` passes
    /// its on-disk `len`/payload slice.
    pub fn from_bytes(slice: &[u8]) -> Self {
        debug_assert!(slice.len() <= MAX_PK_BYTES);
        let mut bytes = [0u8; MAX_PK_BYTES];
        bytes[..slice.len()].copy_from_slice(slice);
        PkBuf { bytes, len: slice.len() as u8 }
    }

    /// `&self.bytes[..len]` — the OPK bytes of this bound. After the
    /// OPK-at-rest flip all PK comparison and range logic operates on these
    /// raw order-preserving bytes (`compare_pk_bytes` / `pack_pk_be`), so this
    /// is the single PK accessor.
    #[inline]
    pub fn pk_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    /// The key zero-padded to `width` bytes — sound because the tail past
    /// `len` is always zero by construction. Used where a narrower key (e.g.
    /// an index leading-key span) must be widened to a full PK stride whose
    /// suffix is zero.
    #[inline]
    pub fn padded(&self, width: usize) -> &[u8] {
        debug_assert!(self.len as usize <= width && width <= MAX_PK_BYTES);
        &self.bytes[..width]
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{read_signed, type_code, SchemaColumn, SchemaDescriptor};
    use crate::test_support::pk_only_schema;

    /// Independent typed reference comparator over native-LE PK bytes. This is
    /// the per-column column-walk that `compare_pk_bytes` used *before* the OPK
    /// flip; kept here as the test oracle for "OPK byte order == typed order".
    fn typed_cmp_pk_le(schema: &SchemaDescriptor, a: &[u8], b: &[u8]) -> Ordering {
        let mut off = 0usize;
        for (_ord, _ci, col) in schema.pk_columns() {
            let cs = col.size() as usize;
            let ord = match col.type_code {
                type_code::U128 | type_code::UUID => {
                    let va = u128::from_le_bytes(a[off..off + 16].try_into().unwrap());
                    let vb = u128::from_le_bytes(b[off..off + 16].try_into().unwrap());
                    va.cmp(&vb)
                }
                type_code::I128 => {
                    let va = i128::from_le_bytes(a[off..off + 16].try_into().unwrap());
                    let vb = i128::from_le_bytes(b[off..off + 16].try_into().unwrap());
                    va.cmp(&vb)
                }
                type_code::U64 | type_code::U32 | type_code::U16 | type_code::U8 => {
                    use crate::schema::read_unsigned;
                    read_unsigned(&a[off..], cs).cmp(&read_unsigned(&b[off..], cs))
                }
                _ => read_signed(&a[off..], cs).cmp(&read_signed(&b[off..], cs)),
            };
            if ord != Ordering::Equal { return ord; }
            off += cs;
        }
        Ordering::Equal
    }

    /// Compare two native-LE PK tuples the way storage now does: encode each to
    /// OPK, then `compare_pk_bytes` (a raw memcmp). Mirrors the read path.
    fn cmp_pk_le(schema: &SchemaDescriptor, a: &[u8], b: &[u8]) -> Ordering {
        let stride = schema.pk_stride() as usize;
        let mut opk_a = vec![0u8; stride];
        let mut opk_b = vec![0u8; stride];
        encode_order_preserving_pk(schema, a, &mut opk_a);
        encode_order_preserving_pk(schema, b, &mut opk_b);
        compare_pk_bytes(&opk_a, &opk_b)
    }

    /// The load-bearing OPK property: a raw memcmp of the order-preserving keys
    /// equals the typed lexicographic comparison of the PK columns.
    fn assert_opk_equivalence(schema: &SchemaDescriptor, a: &[u8], b: &[u8]) {
        assert_eq!(
            cmp_pk_le(schema, a, b),
            typed_cmp_pk_le(schema, a, b),
            "OPK order disagrees with typed comparison for a={a:?} b={b:?}",
        );
    }

    #[test]
    fn compare_pk_bytes_single_u64() {
        let s = pk_only_schema(&[type_code::U64]);
        let vals: [u64; 4] = [1, 2, 256, u64::MAX];
        for i in 0..vals.len() {
            for j in 0..vals.len() {
                let a = vals[i].to_le_bytes();
                let b = vals[j].to_le_bytes();
                assert_eq!(
                    cmp_pk_le(&s, &a, &b),
                    vals[i].cmp(&vals[j]),
                    "U64 mismatch at ({i}, {j})",
                );
                assert_opk_equivalence(&s, &a, &b);
            }
        }
        // Pin the LE-bytes vs lex-bytes regression: 1 < 256.
        assert_eq!(
            cmp_pk_le(&s, &1u64.to_le_bytes(), &256u64.to_le_bytes()),
            Ordering::Less,
        );
    }

    /// A single I128 PK column: the OPK memcmp order at rest must equal the typed
    /// SIGNED i128 order, across the sign boundary and the 2^63/2^64 width
    /// boundaries that distinguish a U64 image from an I64 image.
    #[test]
    fn compare_pk_bytes_single_i128() {
        let s = pk_only_schema(&[type_code::I128]);
        let vals: [i128; 7] = [
            i128::MIN, -1, 0, 1, 1i128 << 63, 1i128 << 64, i128::MAX,
        ];
        for i in 0..vals.len() {
            for j in 0..vals.len() {
                let a = vals[i].to_le_bytes();
                let b = vals[j].to_le_bytes();
                assert_eq!(
                    cmp_pk_le(&s, &a, &b),
                    vals[i].cmp(&vals[j]),
                    "I128 PK order mismatch at ({i}, {j})",
                );
                assert_opk_equivalence(&s, &a, &b);
            }
        }
    }

    #[test]
    fn compare_pk_bytes_single_u128() {
        let s = pk_only_schema(&[type_code::U128]);
        let lo = u64::MAX as u128;
        let hi = lo + 1;
        assert_eq!(
            cmp_pk_le(&s, &lo.to_le_bytes(), &hi.to_le_bytes()),
            Ordering::Less,
        );
        assert_eq!(
            cmp_pk_le(&s, &hi.to_le_bytes(), &lo.to_le_bytes()),
            Ordering::Greater,
        );
        assert_eq!(
            cmp_pk_le(&s, &lo.to_le_bytes(), &lo.to_le_bytes()),
            Ordering::Equal,
        );
        assert_opk_equivalence(&s, &lo.to_le_bytes(), &hi.to_le_bytes());
        assert_opk_equivalence(&s, &hi.to_le_bytes(), &lo.to_le_bytes());
    }

    #[test]
    fn compare_pk_bytes_single_uuid() {
        let s = pk_only_schema(&[type_code::UUID]);
        let low: u128 = 0x0000_0000_0000_0001;
        let high: u128 = 0x8000_0000_0000_0000_0000_0000_0000_0000;
        // High-bit-set sorts above small value (u128 LE numerical order).
        assert_eq!(
            cmp_pk_le(&s, &low.to_le_bytes(), &high.to_le_bytes()),
            Ordering::Less,
        );
        assert_opk_equivalence(&s, &low.to_le_bytes(), &high.to_le_bytes());
    }

    #[test]
    fn compare_pk_bytes_single_signed_i8() {
        let s = pk_only_schema(&[type_code::I8]);
        let vals: [i8; 5] = [i8::MIN, -1, 0, 1, i8::MAX];
        for i in 0..vals.len() {
            for j in 0..vals.len() {
                let a = [vals[i] as u8];
                let b = [vals[j] as u8];
                assert_eq!(
                    cmp_pk_le(&s, &a, &b),
                    vals[i].cmp(&vals[j]),
                    "I8 mismatch at ({i}, {j})",
                );
                assert_opk_equivalence(&s, &a, &b);
            }
        }
        // Sign-extension regression: -1 (0xFF) < 1 (0x01).
        assert_eq!(cmp_pk_le(&s, &[0xFF], &[0x01]), Ordering::Less);
    }

    #[test]
    fn compare_pk_bytes_single_signed_i16() {
        let s = pk_only_schema(&[type_code::I16]);
        let vals: [i16; 5] = [i16::MIN, -1, 0, 1, i16::MAX];
        for i in 0..vals.len() {
            for j in 0..vals.len() {
                let a = vals[i].to_le_bytes();
                let b = vals[j].to_le_bytes();
                assert_eq!(
                    cmp_pk_le(&s, &a, &b),
                    vals[i].cmp(&vals[j]),
                    "I16 mismatch at ({i}, {j})",
                );
                assert_opk_equivalence(&s, &a, &b);
            }
        }
    }

    #[test]
    fn compare_pk_bytes_single_signed_i32() {
        let s = pk_only_schema(&[type_code::I32]);
        let vals: [i32; 5] = [i32::MIN, -1, 0, 1, i32::MAX];
        for i in 0..vals.len() {
            for j in 0..vals.len() {
                let a = vals[i].to_le_bytes();
                let b = vals[j].to_le_bytes();
                assert_eq!(
                    cmp_pk_le(&s, &a, &b),
                    vals[i].cmp(&vals[j]),
                    "I32 mismatch at ({i}, {j})",
                );
                assert_opk_equivalence(&s, &a, &b);
            }
        }
    }

    #[test]
    fn compare_pk_bytes_single_signed_i64() {
        let s = pk_only_schema(&[type_code::I64]);
        let vals: [i64; 5] = [i64::MIN, -1, 0, 1, i64::MAX];
        for i in 0..vals.len() {
            for j in 0..vals.len() {
                let a = vals[i].to_le_bytes();
                let b = vals[j].to_le_bytes();
                assert_eq!(
                    cmp_pk_le(&s, &a, &b),
                    vals[i].cmp(&vals[j]),
                    "I64 mismatch at ({i}, {j})",
                );
                assert_opk_equivalence(&s, &a, &b);
            }
        }
    }

    #[test]
    fn compare_pk_bytes_single_unsigned_u8() {
        let s = pk_only_schema(&[type_code::U8]);
        // High-bit-set vs small: 0xFF > 0x01 numerically as unsigned.
        assert_eq!(cmp_pk_le(&s, &[0xFFu8], &[0x01u8]), Ordering::Greater);
        assert_eq!(cmp_pk_le(&s, &[0x00u8], &[0xFFu8]), Ordering::Less);
        assert_opk_equivalence(&s, &[0xFFu8], &[0x01u8]);
        assert_opk_equivalence(&s, &[0x00u8], &[0xFFu8]);
    }

    #[test]
    fn compare_pk_bytes_single_unsigned_u16() {
        let s = pk_only_schema(&[type_code::U16]);
        let lo: u16 = 0x0001;
        let hi: u16 = 0xFFFE;
        // Zero-extension: hi > lo. Sign-extension would invert.
        assert_eq!(
            cmp_pk_le(&s, &lo.to_le_bytes(), &hi.to_le_bytes()),
            Ordering::Less,
        );
        assert_opk_equivalence(&s, &lo.to_le_bytes(), &hi.to_le_bytes());
    }

    #[test]
    fn compare_pk_bytes_single_unsigned_u32() {
        let s = pk_only_schema(&[type_code::U32]);
        let lo: u32 = 1;
        let hi: u32 = 0xFFFF_FFFE;
        assert_eq!(
            cmp_pk_le(&s, &lo.to_le_bytes(), &hi.to_le_bytes()),
            Ordering::Less,
        );
        assert_opk_equivalence(&s, &lo.to_le_bytes(), &hi.to_le_bytes());
    }

    #[test]
    fn compare_pk_bytes_compound_u64_u64() {
        let s = pk_only_schema(&[type_code::U64, type_code::U64]);
        let mk = |a: u64, b: u64| {
            let mut v = Vec::with_capacity(16);
            v.extend_from_slice(&a.to_le_bytes());
            v.extend_from_slice(&b.to_le_bytes());
            v
        };
        let r0 = mk(1, 5);
        let r1 = mk(1, 9);
        let r2 = mk(2, 1);
        // Same first column, second column tiebreaks ascending.
        assert_eq!(cmp_pk_le(&s, &r0, &r1), Ordering::Less);
        // First column dominates (would be Greater under a u128 LE compare,
        // which would treat the second column as the high-order bits).
        assert_eq!(cmp_pk_le(&s, &r1, &r2), Ordering::Less);
        assert_opk_equivalence(&s, &r0, &r1);
        assert_opk_equivalence(&s, &r1, &r2);
        assert_opk_equivalence(&s, &r0, &r2);
    }

    #[test]
    fn compare_pk_bytes_compound_mixed() {
        let s = pk_only_schema(&[type_code::U64, type_code::I32]);
        let mk = |a: u64, b: i32| {
            let mut v = Vec::with_capacity(12);
            v.extend_from_slice(&a.to_le_bytes());
            v.extend_from_slice(&b.to_le_bytes());
            v
        };
        let neg = mk(1, -5);
        let zero = mk(1, 0);
        // Per-column dispatch picks read_signed for col 1 even though col 0
        // is unsigned: -5 < 0.
        assert_eq!(cmp_pk_le(&s, &neg, &zero), Ordering::Less);
        assert_opk_equivalence(&s, &neg, &zero);
    }

    #[test]
    fn compare_pk_bytes_pk_indices_order_not_schema_order() {
        // Schema [U64, U64] with pk_indices = [1, 0]: column 1 is the first
        // PK column. The byte layout follows pk_indices() order, so the
        // first 8 bytes correspond to column 1.
        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[1, 0],
        );
        // (col1=1, col0=5) vs (col1=2, col0=0): col1 dominates.
        let mut a = Vec::with_capacity(16);
        a.extend_from_slice(&1u64.to_le_bytes()); // col1
        a.extend_from_slice(&5u64.to_le_bytes()); // col0
        let mut b = Vec::with_capacity(16);
        b.extend_from_slice(&2u64.to_le_bytes()); // col1
        b.extend_from_slice(&0u64.to_le_bytes()); // col0
        assert_eq!(cmp_pk_le(&s, &a, &b), Ordering::Less);
        // Encoder iterates pk-list order [1,0], same as the comparator.
        assert_opk_equivalence(&s, &a, &b);
    }

    #[test]
    fn compare_pk_bytes_equal_returns_equal() {
        for tc in [
            type_code::U8, type_code::I8, type_code::U16, type_code::I16,
            type_code::U32, type_code::I32, type_code::U64, type_code::I64,
            type_code::U128, type_code::UUID,
        ] {
            let s = pk_only_schema(&[tc]);
            let stride = s.pk_stride() as usize;
            let buf = vec![0xABu8; stride];
            assert_eq!(
                cmp_pk_le(&s, &buf, &buf),
                Ordering::Equal,
                "equal-buffer mismatch for type_code {tc}",
            );
        }
        // Compound (U64, U64) equal buffers.
        let s = pk_only_schema(&[type_code::U64, type_code::U64]);
        let buf = vec![0x7Fu8; 16];
        assert_eq!(cmp_pk_le(&s, &buf, &buf), Ordering::Equal);
        assert_opk_equivalence(&s, &buf, &buf);
    }

    // -----------------------------------------------------------------------
    // OPK ↔ compare_pk_bytes property test
    // -----------------------------------------------------------------------

    mod opk_proptest {
        use super::*;
        use proptest::prelude::*;
        use crate::test_support::arb_pk_type;

        fn tc_size(tc: u8) -> usize {
            SchemaColumn::new(tc, 0).size() as usize
        }

        /// `(column type codes, pk_indices permutation, a_bytes, b_bytes)`.
        /// The permutation exercises non-identity `pk_indices` (e.g. `[1, 0]`),
        /// and 1..=4 columns spans both narrow (≤16) and wide (>16) strides.
        fn arb_pk_case() -> impl Strategy<Value = (Vec<u8>, Vec<u32>, Vec<u8>, Vec<u8>)> {
            prop::collection::vec(arb_pk_type(), 1..=4).prop_flat_map(|types| {
                let stride: usize = types.iter().map(|&t| tc_size(t)).sum();
                let n = types.len();
                (
                    Just(types),
                    Just((0..n as u32).collect::<Vec<u32>>()).prop_shuffle(),
                    prop::collection::vec(any::<u8>(), stride),
                    prop::collection::vec(any::<u8>(), stride),
                )
            })
        }

        proptest! {
            /// The order-preserving key agrees with `compare_pk_bytes` for every
            /// PK-eligible type, every 1..=4-column compound arrangement, and any
            /// `pk_indices` permutation — over random PK byte tuples.
            #[test]
            fn opk_matches_compare_pk_bytes((types, perm, a, b) in arb_pk_case()) {
                let cols: Vec<SchemaColumn> =
                    types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
                let s = SchemaDescriptor::new(&cols, &perm);
                assert_opk_equivalence(&s, &a, &b);
            }
        }
    }
}
