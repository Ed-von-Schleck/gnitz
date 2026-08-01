//! Order-preserving primary-key (OPK) primitives — the §9 key cluster.
//!
//! These pure layout/key operations sit *below* both `schema` and `storage`:
//! they encode a PK region — a whole one, a seek key reassembled from its wire
//! pair, or an index's leading-column span — to its order-preserving big-endian
//! image, compare two such images with a raw `memcmp`, route a key to a
//! partition, pack a narrow region into a sort key, and carry a width-tagged PK
//! byte buffer. None of them reaches up into storage — the dependency runs
//! `storage → schema::key`, the legitimate downward direction. This module is
//! the one import path: every caller, storage included, names
//! `crate::schema::key::X`, and every native→OPK encoder lives here so the
//! write, seek and route sides cannot spell the encoding differently.

use std::cmp::Ordering;

use crate::schema::{SchemaColumn, SchemaDescriptor, MAX_PK_BYTES};

/// Widest PK region that still fits in a packed `u128` word (16 bytes), the
/// boundary where a key stops fitting one `u128`. At or below it `get_pk`
/// returns the exact key as a `u128` (wider regions must read `get_pk_bytes`)
/// and the seek wire image splits into a low-16 word plus a wider suffix
/// ([`seek_opk_bytes`]); above it a shard's PK region must be stored `Raw`
/// (a `Constant` region holds only 16 bytes) and is ordered via
/// [`compare_pk_bytes`].
pub(crate) const NARROW_PK_MAX_BYTES: usize = 16;

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

/// Typed lexicographic OPK ordering of two **equal-length** PK regions — the
/// comparator the N-way merge and the read-cursor loser tree read through their
/// sources. Returns the same `Ordering` as `compare_pk_bytes` at every width,
/// settling the common case on the leading-16 `pack_pk_be` image. For `len ≤ 16`
/// that image is the *whole* PK and is injective, so a `pack_pk_be` tie is
/// already a byte-equal PK — the byte
/// compare is skipped (it would be a guaranteed-`Equal` `memcmp`). Only `len > 16`
/// can tie on the 16-byte prefix while differing later, so the full-byte
/// `compare_pk_bytes` tiebreak runs only there. No stride / width-class dispatch.
#[inline(always)]
pub(crate) fn compare_pk_ordering(a: &[u8], b: &[u8]) -> Ordering {
    debug_assert_eq!(a.len(), b.len(), "compare_pk_ordering on unequal PK widths");
    match pack_pk_be(a).cmp(&pack_pk_be(b)) {
        Ordering::Equal if a.len() > NARROW_PK_MAX_BYTES => compare_pk_bytes(a, b),
        ord => ord,
    }
}

/// OPK byte-equality of two **equal-length** PK regions (a full PK, or a shared
/// equi-prefix sliced to the same width on both sides) — the equality sibling of
/// [`compare_pk_ordering`]. `pk_bytes_eq(a, b) == (a == b)` at every width: for
/// `len ≤ 16` the executed path is the register `pack_pk_be` compare with no
/// `bcmp`/`memcmp` call (OPK encoding is a bijection, so the left-aligned `u128`
/// images are equal iff the keys are byte-equal); for `len > 16` it confirms a
/// leading-16 prefix tie with the full `compare_pk_bytes`. Operates under the same
/// equal-width contract as `compare_pk_ordering`. Use at every merge/group fold or
/// probe that tests "same PK".
#[inline(always)]
pub(crate) fn pk_bytes_eq(a: &[u8], b: &[u8]) -> bool {
    compare_pk_ordering(a, b) == Ordering::Equal
}

// ---------------------------------------------------------------------------
// Order-preserving PK encoder
// ---------------------------------------------------------------------------

/// The schema-typed face of [`gnitz_wire::encode_pk_tuple`]: encode a full PK
/// region (`schema.pk_stride()` bytes) into its order-preserving big-endian key.
/// `pk_bytes` and `out` are both `pk_stride` bytes. Feeds it
/// `schema.pk_columns()` — the *same* iterator `compare_pk_bytes` walks — so a
/// non-identity `pk_indices` (e.g. `[1, 0]`) encodes in pk-list order, matching
/// the comparator.
///
/// The encoding is **injective**: `encode(a) == encode(b)` iff
/// `a == b` byte-for-byte, because each column's transform is a bijection on its
/// byte range. Consolidation grouping relies on this — an OPK equality test is
/// exactly a PK-byte equality test.
pub(crate) fn encode_order_preserving_pk(schema: &SchemaDescriptor, pk_bytes: &[u8], out: &mut [u8]) {
    gnitz_wire::encode_pk_tuple(
        schema
            .pk_columns()
            .map(|(_ord, _ci, col)| (col.size() as usize, col.type_code)),
        pk_bytes,
        out,
    );
}

/// OPK-encode a PK from its **native LE** bytes, returning it as a `pk_stride`-
/// wide [`PkBuf`]. `native_le` must hold at least `pk_stride` bytes (in pk-list
/// column order); any trailing bytes are ignored. This is the single native→OPK
/// encoder for **every** PK width — a caller holding a narrow value passes
/// `&value.to_le_bytes()`; the seek path passes the reassembled wire image via
/// [`seek_opk_bytes`].
#[inline]
pub(crate) fn opk_key(schema: &SchemaDescriptor, native_le: &[u8]) -> PkBuf {
    let stride = schema.pk_stride() as usize;
    debug_assert!(
        native_le.len() >= stride,
        "opk_key: native_le ({}) shorter than pk_stride ({stride})",
        native_le.len(),
    );
    let mut out = PkBuf::zeroed(stride);
    encode_order_preserving_pk(schema, &native_le[..stride], &mut out.bytes[..stride]);
    out
}

/// Reassemble the native seek image from the wire pair `(low, extra)` — the
/// inverse of `PkTuple::split_wire` — and OPK-encode it via [`opk_key`]. The
/// shared seek-key encoder for the master partition router (`fan_out_seek`) and
/// the worker SEEK handler (`seek_family`) at every PK width. The seek frame
/// carries the key as native LE column bytes (it bypasses the client's
/// `build_pk_region`, so the bytes are not yet OPK): the low
/// [`NARROW_PK_MAX_BYTES`] ride in `low`, a wide PK's remaining suffix in
/// `extra` (empty for narrow PKs).
///
/// Errors if `extra` is shorter than that suffix.
pub(crate) fn seek_opk_bytes(schema: &SchemaDescriptor, low: u128, extra: &[u8]) -> Result<PkBuf, String> {
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
    Ok(opk_key(schema, &le[..stride]))
}

/// OPK-encode native key values into one leading-key span: column `i` reads
/// `natives[i]`'s low source-width bytes and encodes at its target column's
/// (possibly promoted) width, packed tightly in span order. The returned
/// [`PkBuf`] is exactly the span — bytes past it stay zero, so it is also the
/// minimum full key of its group and may be widened with [`PkBuf::padded`].
///
/// The one native→OPK leading-span encoder, shared by the index seek path
/// (`IndexKeySpec::seek_prefix`, whose sources promote to wider index columns)
/// and the base-table PK range path (whose source and target types are equal,
/// making the promotion the identity arm of `encode_pk_column_promoted`). Both
/// must agree byte-for-byte with the write side's `IndexKeySpec::write_span`,
/// which is why they encode through the same call rather than each spelling it.
pub(crate) fn encode_leading_opk(cols: impl IntoIterator<Item = (u8, SchemaColumn)>, natives: &[u128]) -> PkBuf {
    let mut out = PkBuf::zeroed(0);
    let mut off = 0usize;
    for ((src_tc, target), native) in cols.into_iter().zip(natives) {
        let w = target.size() as usize;
        gnitz_wire::encode_pk_column_promoted(
            &native.to_le_bytes()[..gnitz_wire::wire_stride(src_tc)],
            src_tc,
            target.type_code,
            &mut out.bytes[off..off + w],
        );
        off += w;
    }
    out.len = off as u8;
    out
}

/// OPK-encode a native index-key value into an index's leading key column, for a
/// prefix seek or a check-batch composite PK — the scalar sibling of
/// `IndexKeySpec::seek_prefix` for callers whose source column lives in another
/// table's schema (FK probes). `src_type` is the *source* column type (the value
/// in `native` is zero-extended): a signed source sign-extends from its native
/// width before OPK-encoding at the promoted `idx_key_type`, byte-identical to
/// the write-side `IndexKeySpec::write_span`.
///
/// The returned `PkBuf` is exactly the leading column; a caller matching against
/// a wider composite (the source-PK suffix is zero) widens it with
/// [`PkBuf::padded`].
#[inline]
pub(crate) fn index_opk_prefix(native: u128, src_type: u8, idx_key_type: u8) -> PkBuf {
    encode_leading_opk([(src_type, SchemaColumn::new(idx_key_type, 0))], &[native])
}

// ---------------------------------------------------------------------------
// Hash routing
// ---------------------------------------------------------------------------

/// Route a native PK value to a partition.
///
/// Multiplicative hash: two Fibonacci multipliers XOR'd together. ~4
/// instructions vs ~20 for XXH3-64; distribution across 256 buckets is
/// sufficient for worker routing. XXH3 is reserved for filters (xor8, bloom)
/// where collision quality matters.
#[inline(always)]
pub fn partition_for_key(pk: u128) -> usize {
    let lo = pk as u64;
    let hi = (pk >> 64) as u64;
    let h = lo.wrapping_mul(0x9e3779b97f4a7c15_u64) ^ hi.wrapping_mul(0x6c62272e07bb0142_u64);
    (h >> 56) as usize
}

/// Route an OPK PK region (any width) to a partition. For a narrow region the
/// OPK bytes are big-endian, so `widen_pk_be` right-aligns them to recover the
/// native unsigned value (sign-flipped for signed) and the result is
/// `partition_for_key(widen_pk_be(bytes))` by construction. This is the
/// invariant the join router relies on: `extract_col_key` (both PK and
/// OPK-encoded payload paths) also funnels through `widen_pk_be`, so the two
/// sides of a distributed join agree. For wide regions it takes the top 8 bits
/// of xxh3 of the OPK bytes directly (uniformly distributed already).
#[inline]
pub fn partition_for_pk_bytes(bytes: &[u8]) -> usize {
    if bytes.len() <= NARROW_PK_MAX_BYTES {
        partition_for_key(gnitz_wire::widen_pk_be(bytes, bytes.len()))
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
/// The `{2, 4, 8, ≥16}` arms load the dominant scalar and `U128`/wide-prefix
/// widths straight into a register, value-equal to the pad-and-copy (a narrow
/// value occupies the high bits, the low bits zero; `≥16` reads the
/// order-preserving leading 16 bytes) — pinned by
/// `pack_pk_be_specialization_matches_naive`. The wildcard arm covers only the
/// odd narrow widths (1/3/5/6/7/9..=15).
///
/// NOT a value accessor — for a U64 OPK value 1 (`[0,…,0,1]` at `[..8]`) this
/// packs as `1·2^64`, not 1. Sibling of `pack_pk_le`, opposite alignment from
/// `gnitz_wire::widen_pk_be` (right-aligned value recovery); never conflate them.
#[inline(always)]
pub(crate) fn pack_pk_be(pk_bytes: &[u8]) -> u128 {
    match pk_bytes.len() {
        8 => (u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128) << 64,
        len if len >= 16 => u128::from_be_bytes(pk_bytes[..16].try_into().unwrap()),
        4 => (u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128) << 96,
        2 => (u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128) << 112,
        // 1/3/5/6/7/9..=15: odd narrow widths — pad-and-copy (len < 16 here, so
        // the whole slice is copied and the old `len.min(16)` is unnecessary).
        len => {
            let mut buf = [0u8; 16];
            buf[..len].copy_from_slice(pk_bytes);
            u128::from_be_bytes(buf)
        }
    }
}

/// The OPK image of a **narrow unsigned** PK value: `pk`'s low `stride` bytes,
/// big-endian. OPK == big-endian for an unsigned key, so `widen_pk_be(bytes())`
/// recovers `pk`; a signed or compound key needs the per-column sign flip
/// (`encode_order_preserving_pk`) and must not come through here.
///
/// The one home for "right-align a `u128` into an OPK of width `stride`" — every
/// synthetic-key writer (the batch PK setters, the reduce group-key emitters)
/// builds one, so the width checks below cannot be skipped by hand-rolling
/// `&pk.to_be_bytes()[16 - stride..]`, which silently truncates a value that
/// overflows the stride. Zero-cost: a 16-byte stack value, no allocation.
pub(crate) struct NarrowPkOpk {
    be: [u8; 16],
    stride: usize,
}

impl NarrowPkOpk {
    #[inline(always)]
    pub(crate) fn new(pk: u128, stride: usize) -> Self {
        assert!(
            stride <= NARROW_PK_MAX_BYTES,
            "narrow PK required, got stride {stride}; use the raw-OPK-bytes setter"
        );
        debug_assert!(
            stride == 16 || (pk >> (stride * 8)) == 0,
            "narrow PK {pk} does not fit {stride} bytes",
        );
        NarrowPkOpk {
            be: pk.to_be_bytes(),
            stride,
        }
    }

    /// The `stride` order-preserving bytes — a full PK region for one row.
    #[inline(always)]
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.be[16 - self.stride..]
    }
}

/// A fixed-width order-preserving sort key built by left-aligning a row's
/// `pk_stride` OPK bytes big-endian. The OPK bytes are order-preserving, and every
/// row in one batch shares a single `pk_stride` (hence identical zero padding past
/// `opk.len()`), so for two equal-length OPK slices that fit in `size_of::<K>()`
/// bytes `K::from_opk(a).cmp(&K::from_opk(b)) == compare_pk_bytes(a, b)`. The key
/// is the *whole* PK image (not a 16-byte prefix), so a key sort is exact with no
/// PK-byte tiebreak. Used by the reduce sort to width-match the key to `pk_stride`
/// (`u64`/`u128`/`[u128; 2]` for strides ≤8/≤16/≤32; wider PKs byte-walk directly).
pub(crate) trait PkSortKey: Ord + Copy {
    fn from_opk(opk: &[u8]) -> Self;
}

impl PkSortKey for u64 {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u64 {
        // Dispatched only for strides ≤ 8; the `== 8` arm loads the dominant
        // U64/I64 key straight into a register (no memcpy). The narrower strides
        // left-align at the MSB end so a raw `u64` compare is the OPK byte order.
        if opk.len() == 8 {
            u64::from_be_bytes(opk.try_into().unwrap())
        } else {
            let mut x = [0u8; 8];
            x[..opk.len()].copy_from_slice(opk);
            u64::from_be_bytes(x)
        }
    }
}

impl PkSortKey for u128 {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u128 {
        // Identical left-align to `u64`, one width up; `pack_pk_be` is the canonical
        // left-align-to-`u128`, reused here. Dispatched only for strides ≤ 16.
        pack_pk_be(opk)
    }
}

impl PkSortKey for [u128; 2] {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> [u128; 2] {
        // Dispatched only for 17..=32-byte strides: hi = the full leading 16 bytes
        // (always a register load), lo = the trailing 1..=16 left-aligned. Array
        // `Ord` is lexicographic, so the low limb settles a leading-16-byte tie a
        // bare `u128` prefix would tie on.
        let hi = u128::from_be_bytes(opk[..16].try_into().unwrap());
        if opk.len() == 32 {
            [hi, u128::from_be_bytes(opk[16..32].try_into().unwrap())]
        } else {
            let mut lo = [0u8; 16];
            lo[..opk.len() - 16].copy_from_slice(&opk[16..]);
            [hi, u128::from_be_bytes(lo)]
        }
    }
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
        self.len == other.len && self.bytes[..self.len as usize] == other.bytes[..other.len as usize]
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
    /// All-zero key of the given width — the zero-row / placeholder /
    /// empty-shard form, and the mutable scratch every reused key buffer starts
    /// from (the fields are public; write the meaningful span in place).
    pub fn zeroed(len: usize) -> Self {
        debug_assert!(len <= MAX_PK_BYTES);
        PkBuf {
            bytes: [0u8; MAX_PK_BYTES],
            len: len as u8,
        }
    }

    /// `len = slice.len()`, `bytes[..len]` copied from `slice`, tail
    /// zero. The row constructor: `MappedShard::get_pk_bytes(row)`
    /// returns exactly `pk_stride` bytes, and manifest `parse` passes
    /// its on-disk `len`/payload slice.
    pub fn from_bytes(slice: &[u8]) -> Self {
        debug_assert!(slice.len() <= MAX_PK_BYTES);
        let mut bytes = [0u8; MAX_PK_BYTES];
        bytes[..slice.len()].copy_from_slice(slice);
        PkBuf {
            bytes,
            len: slice.len() as u8,
        }
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
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::test_support::pk_only_schema;
    use gnitz_wire::{read_signed, read_unsigned};

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
                    read_unsigned(&a[off..], cs).cmp(&read_unsigned(&b[off..], cs))
                }
                _ => read_signed(&a[off..], cs).cmp(&read_signed(&b[off..], cs)),
            };
            if ord != Ordering::Equal {
                return ord;
            }
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

    /// Sweep one single-column PK schema pairwise over `vals`: the OPK memcmp
    /// order at rest must equal `T`'s native typed order — an oracle fully
    /// independent of the in-file `typed_cmp_pk_le` — and must also agree with
    /// `typed_cmp_pk_le` over the same bytes. The `i == j` diagonal is the
    /// equal-buffer case.
    fn assert_single_col_order<T: Ord + Copy + std::fmt::Debug>(tc: u8, vals: &[T], le: impl Fn(T) -> Vec<u8>) {
        let s = pk_only_schema(&[tc]);
        for &a in vals {
            for &b in vals {
                let (ab, bb) = (le(a), le(b));
                assert_eq!(cmp_pk_le(&s, &ab, &bb), a.cmp(&b), "type_code {tc}: {a:?} vs {b:?}");
                assert_opk_equivalence(&s, &ab, &bb);
            }
        }
    }

    /// Boundary sweeps for every PK-eligible scalar type. The value sets carry
    /// the regressions that used to have a test each: sign-extension (`-1 < 1`),
    /// zero-extension (`0xFFFE > 1`), LE-vs-lex byte order (`1 < 256`), and the
    /// 2^63 / 2^64 width boundaries that separate a U64 image from an I64 one.
    #[test]
    fn opk_order_matches_native_order_per_type() {
        assert_single_col_order(type_code::U8, &[0u8, 1, 0x7F, 0x80, 0xFF], |v| vec![v]);
        assert_single_col_order(type_code::I8, &[i8::MIN, -1, 0, 1, i8::MAX], |v| vec![v as u8]);
        let le16 = |v: u16| v.to_le_bytes().to_vec();
        assert_single_col_order(type_code::U16, &[0u16, 1, 0x0100, 0x8000, 0xFFFE, u16::MAX], le16);
        assert_single_col_order(type_code::I16, &[i16::MIN, -1, 0, 1, i16::MAX], |v: i16| {
            v.to_le_bytes().to_vec()
        });
        let le32 = |v: u32| v.to_le_bytes().to_vec();
        assert_single_col_order(
            type_code::U32,
            &[0u32, 1, 256, 0x8000_0000, 0xFFFF_FFFE, u32::MAX],
            le32,
        );
        assert_single_col_order(type_code::I32, &[i32::MIN, -1, 0, 1, i32::MAX], |v: i32| {
            v.to_le_bytes().to_vec()
        });
        assert_single_col_order(type_code::U64, &[0u64, 1, 2, 256, 1 << 63, u64::MAX], |v: u64| {
            v.to_le_bytes().to_vec()
        });
        assert_single_col_order(type_code::I64, &[i64::MIN, -1, 0, 1, i64::MAX], |v: i64| {
            v.to_le_bytes().to_vec()
        });
        let le128 = |v: u128| v.to_le_bytes().to_vec();
        let wide = [0u128, 1, u64::MAX as u128, u64::MAX as u128 + 1, 1 << 127, u128::MAX];
        assert_single_col_order(type_code::U128, &wide, le128);
        assert_single_col_order(type_code::UUID, &wide, le128);
        assert_single_col_order(
            type_code::I128,
            &[i128::MIN, -1, 0, 1, 1i128 << 63, 1i128 << 64, i128::MAX],
            |v: i128| v.to_le_bytes().to_vec(),
        );
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
        // Equal compound buffers compare Equal at every column.
        assert_eq!(cmp_pk_le(&s, &r0, &r0), Ordering::Equal);
        assert_opk_equivalence(&s, &r0, &r0);
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

    /// The specialized `{8, 16}` register-load arms of `pack_pk_be` must be
    /// byte-value-identical to the generic pad-and-copy at every width — a
    /// changed value would silently corrupt every `pack_pk_be` consumer (the
    /// cached sort keys, the route/guard keys, the bloom probes).
    #[test]
    fn pack_pk_be_specialization_matches_naive() {
        fn naive(pk: &[u8]) -> u128 {
            let take = pk.len().min(16);
            let mut buf = [0u8; 16];
            buf[..take].copy_from_slice(&pk[..take]);
            u128::from_be_bytes(buf)
        }
        for width in [1usize, 2, 4, 8, 16, 24, 80] {
            for seed in 0u32..256 {
                let bytes: Vec<u8> = (0..width)
                    .map(|i| seed.wrapping_mul(31).wrapping_add(i as u32) as u8)
                    .collect();
                assert_eq!(pack_pk_be(&bytes), naive(&bytes), "width {width} seed {seed}");
            }
        }
    }

    #[test]
    fn pk_bytes_eq_matches_byte_equality_direct() {
        // Wide (> 16): fully-equal → true; equal leading-16 prefix, differing
        // suffix → false (the tiebreak arm, otherwise only covered two layers up).
        let mut a = [0u8; 24];
        let mut b = [0u8; 24];
        for i in 0..24 {
            a[i] = i as u8;
            b[i] = i as u8;
        }
        assert!(pk_bytes_eq(&a, &b));
        b[16] ^= 1;
        assert!(!pk_bytes_eq(&a, &b));
        assert_eq!(pk_bytes_eq(&a, &b), a[..] == b[..]);
        // Narrow (≤ 16): the register arm, equal and unequal.
        let x = [1u8; 8];
        let mut y = [1u8; 8];
        assert!(pk_bytes_eq(&x, &y));
        y[7] = 2;
        assert!(!pk_bytes_eq(&x, &y));
    }

    // -----------------------------------------------------------------------
    // OPK ↔ compare_pk_bytes property test
    // -----------------------------------------------------------------------

    mod opk_proptest {
        use super::*;
        use crate::test_support::arb_pk_type;
        use proptest::prelude::*;

        /// `(column type codes, pk_indices permutation, a_bytes, b_bytes)`.
        /// The permutation exercises non-identity `pk_indices` (e.g. `[1, 0]`),
        /// and 1..=4 columns spans both narrow (≤16) and wide (>16) strides.
        fn arb_pk_case() -> impl Strategy<Value = (Vec<u8>, Vec<u32>, Vec<u8>, Vec<u8>)> {
            prop::collection::vec(arb_pk_type(), 1..=4).prop_flat_map(|types| {
                let stride: usize = types.iter().map(|&t| gnitz_wire::wire_stride(t)).sum();
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

        proptest! {
            /// `compare_pk_ordering` agrees with the authoritative byte comparator
            /// at every PK width — narrow (register `pack_pk_be`) and wide
            /// (>16-byte prefix + `compare_pk_bytes` tiebreak) alike — so
            /// `== Ordering::Equal` is exactly byte equality, the property the
            /// N-way merge fold and the single-batch drain rely on for grouping.
            #[test]
            fn compare_pk_ordering_matches_byte_compare((types, perm, a, b) in arb_pk_case()) {
                let cols: Vec<SchemaColumn> =
                    types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
                let s = SchemaDescriptor::new(&cols, &perm);
                let stride = s.pk_stride() as usize;
                let (mut oa, mut ob) = (vec![0u8; stride], vec![0u8; stride]);
                encode_order_preserving_pk(&s, &a, &mut oa);
                encode_order_preserving_pk(&s, &b, &mut ob);
                prop_assert_eq!(compare_pk_ordering(&oa, &ob), compare_pk_bytes(&oa, &ob));
                prop_assert_eq!(
                    compare_pk_ordering(&oa, &ob) == std::cmp::Ordering::Equal,
                    oa == ob
                );
            }
        }

        proptest! {
            /// `pk_bytes_eq` is exactly byte-equality of the OPK regions at every
            /// PK width — the register-arm narrow case and the prefix-tiebreak wide
            /// case alike — the property every "same PK" merge/group fold relies on.
            #[test]
            fn pk_bytes_eq_matches_byte_equality((types, perm, a, b) in arb_pk_case()) {
                let cols: Vec<SchemaColumn> =
                    types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
                let s = SchemaDescriptor::new(&cols, &perm);
                let stride = s.pk_stride() as usize;
                let (mut oa, mut ob) = (vec![0u8; stride], vec![0u8; stride]);
                encode_order_preserving_pk(&s, &a, &mut oa);
                encode_order_preserving_pk(&s, &b, &mut ob);
                prop_assert_eq!(pk_bytes_eq(&oa, &ob), oa == ob);
                prop_assert!(pk_bytes_eq(&oa, &oa));
            }
        }
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
                let want = opk_key(&s, &v.to_le_bytes());
                let got = seek_opk_bytes(&s, v, &[]).expect("narrow seek encodes");
                assert_eq!(got, want, "narrow seek must match opk_key for {s:?} v={v:#x}");
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
        let opk = seek_opk_bytes(&s, low, &c.to_le_bytes()).expect("wide seek encodes");
        assert_eq!(opk.len, 24);
        let want: Vec<u8> = a
            .to_be_bytes()
            .into_iter()
            .chain(b.to_be_bytes())
            .chain(c.to_be_bytes())
            .collect();
        assert_eq!(opk.pk_bytes(), want.as_slice());
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
        let opk = seek_opk_bytes(&s, vals[0], &extra).expect("4×U128 encodes");
        assert_eq!(opk.len, 64);
        let want: Vec<u8> = vals.iter().flat_map(|v| v.to_be_bytes()).collect();
        assert_eq!(opk.pk_bytes(), want.as_slice());
    }

    #[test]
    fn pkbuf_eq_hash_compare_only_len_window() {
        use std::collections::HashSet;
        // Same meaningful bytes, different tail → equal and same hash.
        let mut a = PkBuf::from_bytes(&7u64.to_le_bytes());
        let b = PkBuf::from_bytes(&7u64.to_le_bytes());
        a.bytes[8] = 0xAB; // tail garbage past len; eq/hash must ignore it
        assert!(a == b);
        let mut set: HashSet<PkBuf> = HashSet::new();
        set.insert(b);
        assert!(set.contains(&a), "tail bytes must not affect membership");
    }

    #[test]
    fn pkbuf_borrow_heterogeneous_lookup() {
        use std::collections::HashSet;
        let mut set: HashSet<PkBuf> = HashSet::new();
        set.insert(PkBuf::from_bytes(&123u64.to_le_bytes()));
        // Raw &[u8] lookup via Borrow<[u8]> — no PkBuf construction.
        assert!(set.contains(&123u64.to_le_bytes()[..]));
        assert!(!set.contains(&124u64.to_le_bytes()[..]));
    }

    #[test]
    fn pkbuf_wide_differs_past_byte_16() {
        // Two 24-byte keys identical in the first 16 bytes but differing in the
        // last 8 must be distinct — the failure mode of any u128-truncating key.
        let mut x = Vec::new();
        x.extend_from_slice(&1u64.to_le_bytes());
        x.extend_from_slice(&2u64.to_le_bytes());
        x.extend_from_slice(&3u64.to_le_bytes());
        let mut y = x.clone();
        y[16..24].copy_from_slice(&999u64.to_le_bytes());
        let px = PkBuf::from_bytes(&x);
        let py = PkBuf::from_bytes(&y);
        assert!(px != py);
        let mut set = std::collections::HashSet::new();
        set.insert(px);
        assert!(!set.contains(&py));
    }
}
