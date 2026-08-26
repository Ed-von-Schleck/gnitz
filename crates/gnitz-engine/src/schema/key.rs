//! Order-preserving primary-key (OPK) primitives.
//!
//! These pure layout/key operations sit *below* both `schema` and `storage`:
//! they encode a PK region — a whole one, a seek key reassembled from its wire
//! pair, or an index's leading-column span — to its order-preserving big-endian
//! image, compare two such images with a raw `memcmp`, pack a narrow region
//! into a sort key, carry a width-tagged PK byte buffer, and derive the
//! half-open key range a `RangeDescriptor`'s cut pair denotes. None of them reaches up into storage — the dependency runs
//! `storage → schema::key`, the legitimate downward direction. This module is
//! the one import path: every caller, storage included, names
//! `crate::schema::key::X`, and every native→OPK encoder lives here so the
//! write, seek and route sides cannot spell the encoding differently.

use std::cmp::Ordering;

use gnitz_expr::RowSource;
use gnitz_wire::{Cut, RangeDescriptor, NARROW_PK_MAX_BYTES};

use crate::foundation::xxh;
use crate::schema::{ColumnLocator, SchemaColumn, SchemaDescriptor, MAX_PK_BYTES};

// ---------------------------------------------------------------------------
// Column-aware PK byte-region comparator
// ---------------------------------------------------------------------------

/// Raw byte comparator for PK regions.
///
/// After the OPK-at-rest flip every PK region at rest holds order-preserving
/// big-endian bytes, so unsigned lexicographic byte comparison is numerically
/// identical to the typed comparison of the PK columns for any width. `a.cmp(b)`
/// compiles to an optimal `memcmp`. `a` and `b` are the OPK bytes produced by
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

/// `min <= key <= max` over OPK bytes. The exact-match probes gate on this
/// before searching, so a key outside a block's bounds costs two `memcmp`s
/// instead of a `log n` walk over cold cache lines. The cursor's lower-bound
/// seeks do not: they need a landing position on a miss, not a verdict.
#[inline]
pub(crate) fn pk_in_range(min: &[u8], max: &[u8], key: &[u8]) -> bool {
    compare_pk_bytes(min, key) != Ordering::Greater && compare_pk_bytes(key, max) != Ordering::Greater
}

// ---------------------------------------------------------------------------
// Order-preserving PK encoder
// ---------------------------------------------------------------------------

/// OPK-encode a PK from its **native LE** bytes, returning it as a `pk_stride`-
/// wide [`PkBuf`]. `native_le` must hold at least `pk_stride` bytes (in pk-list
/// column order); any trailing bytes are ignored. This is the single native→OPK
/// encoder for **every** PK width — a caller holding a narrow value passes
/// `&value.to_le_bytes()`; the seek path passes the reassembled wire image via
/// [`seek_opk_bytes`].
///
/// The schema-typed face of [`gnitz_wire::encode_pk_tuple`], fed
/// `schema.pk_columns()` — the *same* iterator `compare_pk_bytes` walks — so a
/// non-identity `pk_indices` (e.g. `[1, 0]`) encodes in pk-list order, matching
/// the comparator.
///
/// The encoding is **injective**: `encode(a) == encode(b)` iff `a == b`
/// byte-for-byte, because each column's transform is a bijection on its byte
/// range. Consolidation grouping relies on this — an OPK equality test is
/// exactly a PK-byte equality test.
#[inline]
pub(crate) fn opk_key(schema: &SchemaDescriptor, native_le: &[u8]) -> PkBuf {
    let stride = schema.pk_stride() as usize;
    debug_assert!(
        native_le.len() >= stride,
        "opk_key: native_le ({}) shorter than pk_stride ({stride})",
        native_le.len(),
    );
    let mut out = PkBuf::zeroed(stride);
    gnitz_wire::encode_pk_tuple(
        schema.pk_columns().map(|(_, col)| (col.size() as usize, col.type_code)),
        &native_le[..stride],
        &mut out.bytes[..stride],
    );
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
pub fn seek_opk_bytes(schema: &SchemaDescriptor, low: u128, extra: &[u8]) -> Result<PkBuf, String> {
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
    out.set_len(off);
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
pub fn index_opk_prefix(native: u128, src_type: u8, idx_key_type: u8) -> PkBuf {
    encode_leading_opk([(src_type, SchemaColumn::new(idx_key_type, 0))], &[native])
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
/// `pack_pk_be_specialization_matches_naive`. Widths 9..=15 get two overlapping
/// loads for the same reason; only 1/3/5/6/7 reach the pad-and-copy arm.
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
        // 9..=15: two overlapping big-endian loads instead of a runtime-length
        // `copy_from_slice`, which lowers to an out-of-line `memcpy` per key. With
        // `m = len - 8`, the low `m` bytes of the tail load are exactly
        // `pk_bytes[8..len]`, since `len - m == 8`. Measured ~3x on this band —
        // the merge and AVI paths sit here (`GROUP BY <INT>` is 13,
        // `PRIMARY KEY (BIGINT, INT)` is 12).
        len @ 9..=15 => {
            let m = len - 8;
            let hi = u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128;
            let tail = u64::from_be_bytes(pk_bytes[len - 8..len].try_into().unwrap());
            let low = (tail & ((1u64 << (8 * m)) - 1)) as u128;
            (hi << 64) | (low << (64 - 8 * m))
        }
        // 1/3/5/6/7: too narrow for the overlapping-load trick (`len - 8` underflows).
        len => {
            let mut buf = [0u8; 16];
            buf[..len].copy_from_slice(pk_bytes);
            u128::from_be_bytes(buf)
        }
    }
}

/// The `stride` OPK bytes of a narrow PK value that is **already in OPK/route
/// space** — the widened image `widen_pk_be` produces, sign-flipped for a signed
/// key. Right-aligning it big-endian reproduces the key's OPK region at any
/// width. A *native* value must be encoded through [`opk_key`] instead, which
/// applies the per-column sign flip this does not.
///
/// The one home for "right-align a `u128` into an OPK of width `stride`" — every
/// synthetic-key writer (the batch PK setters, the reduce group-key emitters)
/// builds one, so the width checks below cannot be skipped by hand-rolling
/// `&pk.to_be_bytes()[16 - stride..]`, which silently truncates a value that
/// overflows the stride. Zero-cost: a stack value, no allocation.
pub(crate) struct NarrowPkOpk {
    be: [u8; 16],
    stride: usize,
}

impl NarrowPkOpk {
    #[inline(always)]
    pub(crate) fn new(pk: u128, stride: usize) -> Self {
        // Static message: `#[inline(always)]` puts this in every per-row caller,
        // and an `Arguments` value costs a stack slot even on a cold panic path.
        assert!(
            stride <= NARROW_PK_MAX_BYTES,
            "NarrowPkOpk::new: stride exceeds NARROW_PK_MAX_BYTES; use the raw-OPK-bytes setter"
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

/// Run `$body` with `$k` bound to the [`PkSortKey`] whose width matches `$stride`;
/// strides too wide to pack into a register key take `$wide`.
///
/// The one statement of the width taxonomy the impls below define. A stride→width
/// `match` spelled at a call site instead goes stale silently when an impl is
/// added: the new width keeps falling to `$wide` and merely gets slower, with
/// nothing to fail.
macro_rules! pk_width_dispatch {
    ($stride:expr, |$k:ident| $body:expr, $wide:expr $(,)?) => {
        match $stride {
            0..=8 => {
                type $k = u64;
                $body
            }
            9..=16 => {
                type $k = u128;
                $body
            }
            17..=32 => {
                type $k = [u128; 2];
                $body
            }
            _ => $wide,
        }
    };
}
pub(crate) use pk_width_dispatch;

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
// OPK span fingerprint
// ---------------------------------------------------------------------------

/// The 64-bit fingerprint of an OPK byte span — a row's whole PK region, or an
/// index's leading-key span. Every approximate-membership structure over OPK
/// keys derives its key here, so a probe key always equals the key inserted.
///
/// A narrow span right-aligns its big-endian bytes into a `u128`; a wider one
/// collapses to the xxh3 checksum of the whole span. Either way the full span
/// is hashed to 64 bits, keeping its entropy — this is deliberately not
/// `worker_for_pk_bytes`, which reduces the same two images to a worker index.
#[inline]
pub fn probe_key(opk: &[u8]) -> u64 {
    let fingerprint = if opk.len() > NARROW_PK_MAX_BYTES {
        xxh::checksum(opk) as u128
    } else {
        gnitz_wire::widen_pk_be(opk, opk.len())
    };
    xxh::checksum(&fingerprint.to_le_bytes())
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
    pub(crate) bytes: [u8; MAX_PK_BYTES],
    pub(crate) len: u8,
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
    /// from. The fields are crate-visible: in-crate callers write the meaningful
    /// span in place, outside go through `set_from` / `pk_bytes` / `padded`.
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

    /// In-place [`Self::from_bytes`]: overwrite this key with `src`. Reuses the
    /// buffer instead of re-zeroing and re-copying the whole `MAX_PK_BYTES`
    /// array, and keeps the zero tail (`set_len` clears whatever a wider
    /// previous key left behind).
    #[inline]
    pub fn set_from(&mut self, src: &[u8]) {
        debug_assert!(src.len() <= MAX_PK_BYTES);
        self.bytes[..src.len()].copy_from_slice(src);
        self.set_len(src.len());
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

    /// Owning form of [`Self::padded`]: the same key re-tagged as `width` bytes.
    /// The tail past `len` is already zero, so widening is a `len` bump with no
    /// copy — the alternative, `PkBuf::from_bytes(k.padded(width))`, re-zeroes
    /// and re-copies the whole `MAX_PK_BYTES` array to reach the same value.
    #[inline]
    pub fn widened(mut self, width: usize) -> Self {
        debug_assert!(self.len as usize <= width && width <= MAX_PK_BYTES);
        self.len = width as u8;
        self
    }

    /// Re-tag the meaningful span as `len` bytes, restoring the zero tail when
    /// this key is narrower than what the buffer previously held. The one
    /// writer of `len`, so "the tail past `len` is zero" — the invariant
    /// [`Self::padded`] and [`Self::widened`] rest on — holds by construction
    /// rather than by every caller remembering to re-zero.
    #[inline]
    pub(crate) fn set_len(&mut self, len: usize) {
        debug_assert!(len <= MAX_PK_BYTES);
        if (self.len as usize) > len {
            self.bytes[len..self.len as usize].fill(0);
        }
        self.len = len as u8;
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
pub struct IndexKeySpec {
    n: u8,
    /// Span width in bytes — the sum of the promoted column widths, precomputed
    /// so the per-row `key_bytes` path does no re-summation.
    key_size: u8,
    locators: [ColumnLocator; gnitz_wire::PK_LIST_MAX_COLS],
    idx_cols: [SchemaColumn; gnitz_wire::PK_LIST_MAX_COLS],
}

impl IndexKeySpec {
    /// `cols` is the circuit's source column list (owner-schema indices);
    /// `idx_schema` supplies the promoted leading columns the span encodes at.
    pub fn new(cols: &[u32], owner: &SchemaDescriptor, idx_schema: &SchemaDescriptor) -> Self {
        debug_assert!(!cols.is_empty() && cols.len() <= gnitz_wire::PK_LIST_MAX_COLS);
        let mut locators = [ColumnLocator::Pk {
            byte_off: 0,
            size: 0,
            type_code: 0,
        }; gnitz_wire::PK_LIST_MAX_COLS];
        let mut idx_cols = [SchemaColumn::EMPTY; gnitz_wire::PK_LIST_MAX_COLS];
        for (i, &c) in cols.iter().enumerate() {
            locators[i] = owner.locate(c as usize);
            idx_cols[i] = idx_schema.columns[i];
            // Spec-invariant, so checked once per circuit rather than per column
            // per row: `write_span` hands each source's bytes to the OPK encoder
            // *at `idx_cols[i].type_code`*, so the index column must be exactly
            // the source's promotion. `index_key_type` errors on STRING/BLOB, so
            // this also subsumes "a German-string source needs a content hash,
            // not a raw cell encode" — its 16-byte struct (a heap offset for a
            // long string) would otherwise encode as if it were an integer. Every
            // production caller builds `idx_schema` through `make_index_schema`,
            // which derives it from this very function.
            debug_assert_eq!(
                gnitz_wire::index_key_type(locators[i].type_code()).ok(),
                Some(idx_cols[i].type_code),
                "IndexKeySpec: index column {i} is not the source column's promotion",
            );
        }
        IndexKeySpec {
            n: cols.len() as u8,
            key_size: idx_schema.leading_key_size(cols.len()) as u8,
            locators,
            idx_cols,
        }
    }

    /// Span width (`idx_key_size`); see `SchemaDescriptor::leading_key_size`.
    #[inline]
    pub fn key_size(&self) -> usize {
        self.key_size as usize
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
    ///
    /// The source bytes come from the locator (`is_null` gates, `bytes` reads the
    /// OPK PK window or the native-LE payload cell); the only thing spelled per
    /// variant is *which encoder* consumes them — a PK source is already OPK, so
    /// it goes through `gnitz_wire::promote_opk_column` (OPK→OPK, identity when
    /// unpromoted), a payload source through `encode_pk_column_promoted`
    /// (native→OPK). Those are the same two primitives `ops::reindex`'s
    /// `ColPromoter::write_into` uses — the two must emit byte-identical keys for
    /// one logical value, so they share the encoders rather than each spelling the
    /// promotion.
    pub(crate) fn write_span(&self, mb: &impl RowSource, row: usize, dst: &mut [u8]) -> bool {
        debug_assert!(dst.len() >= self.key_size(), "write_span: dst shorter than the span");
        let mut off = 0;
        let n = self.n as usize;
        for (loc, col) in self.locators[..n].iter().zip(&self.idx_cols[..n]) {
            // PK columns are never null, so this is the payload-only NULL gate.
            if loc.is_null(mb, row) {
                return false;
            }
            let target_w = col.size() as usize; // promoted index column width
            let out = &mut dst[off..off + target_w];
            let src = loc.bytes(mb, row);
            match *loc {
                ColumnLocator::Pk { type_code, .. } => {
                    gnitz_wire::promote_opk_column(src, type_code, col.type_code, out)
                }
                ColumnLocator::Payload { type_code, .. } => {
                    gnitz_wire::encode_pk_column_promoted(src, type_code, col.type_code, out)
                }
            }
            off += target_w;
        }
        true
    }

    /// [`Self::write_span`] plus the source-PK OPK suffix: one row's full index
    /// entry key `[span ‖ src_pk]` in `dst[..key_size() + pk_stride]`. The single
    /// definition of "this row's index entry", shared by the write-side
    /// projection (`batch_project_index`) and the read-side entry-range filter
    /// (`row_in_index_range`), so the two agree byte-for-byte by construction.
    /// Returns `false` (row not indexed — NULL in an indexed column; `dst`
    /// partially written) exactly as `write_span` does. Full-arity specs only:
    /// a prefix spec would place the suffix over the uncovered columns' bytes.
    pub(crate) fn write_entry(&self, mb: &impl RowSource, row: usize, dst: &mut [u8]) -> bool {
        if !self.write_span(mb, row, dst) {
            return false;
        }
        let pk = mb.get_pk_bytes(row);
        dst[self.key_size()..self.key_size() + pk.len()].copy_from_slice(pk);
        true
    }

    /// Split a stored index entry back into `(span, source PK)` — the read-side
    /// inverse of [`Self::write_entry`], which put the PK at `key_size()`. The
    /// split is exact by layout (an index schema is the promoted indexed columns
    /// followed by the source PK columns, so its stride is `key_size() +
    /// src_pk_stride`), so nothing is decoded.
    pub fn split_entry<'a>(&self, entry: &'a [u8]) -> (&'a [u8], &'a [u8]) {
        debug_assert!(entry.len() > self.key_size(), "index entry shorter than its span");
        entry.split_at(self.key_size())
    }

    /// `write_span` into a caller-reused `PkBuf` — no intermediate stack
    /// buffer, no `from_bytes` re-copy (this runs in the backfill scan and on
    /// every insert). Maintains `PkBuf`'s "tail past `len` is zero" invariant
    /// (zeroing only when this key is narrower than the previous one in the
    /// reused scratch — free in the common same-circuit loop), so callers may
    /// slice `out.bytes[..stride]` as the span zero-padded to any wider stride.
    /// A NULL-skipped row returns `false` with `out` unchanged in meaning.
    pub fn key_bytes(&self, mb: &impl RowSource, row: usize, out: &mut PkBuf) -> bool {
        if !self.write_span(mb, row, &mut out.bytes) {
            return false;
        }
        out.set_len(self.key_size());
        true
    }

    /// Seek-side counterpart of [`Self::write_span`]: OPK-encode native key
    /// values (zero-extended, as `pk_native_key`/`payload_native_key` produce
    /// them) into the leading-key span, returned as a [`PkBuf`] of exactly
    /// the encoded span's width. Encodes through the shared
    /// [`encode_leading_opk`], the same per-column call `write_span` makes,
    /// so the seek prefix matches the projected entries by construction. Bytes
    /// past the span stay zero (the source-PK suffix is not part of it).
    ///
    /// A leading-prefix seek passes fewer values than the spec has columns and
    /// gets the corresponding prefix of the span — the spec's leading `k`
    /// columns are the same read/encode plan whether or not the trailing ones
    /// are supplied, so no sub-spec has to be derived to seek by a prefix.
    pub(crate) fn seek_prefix(&self, natives: &[u128]) -> PkBuf {
        let k = natives.len();
        debug_assert!(
            k >= 1 && k <= self.n as usize,
            "seek_prefix: one native value per leading spec column"
        );
        let cols = self.locators[..k]
            .iter()
            .zip(&self.idx_cols[..k])
            .map(|(loc, col)| (loc.type_code(), *col));
        encode_leading_opk(cols, natives)
    }
}

// ---------------------------------------------------------------------------
// Byte successor / predecessor and cut → key-range derivation
// ---------------------------------------------------------------------------

/// Fixed-width byte-string successor: `p + 1` with carry, in place. Returns
/// `false` when `p` is all-`0xFF` (or empty) — no successor exists at this width
/// (carry-out), which a caller reads as `+∞` (scan to the table end, or a
/// provably-empty start).
pub(crate) fn increment_key_in_place(p: &mut [u8]) -> bool {
    for b in p.iter_mut().rev() {
        *b = b.wrapping_add(1);
        if *b != 0 {
            return true;
        }
    }
    false
}

/// Fixed-width byte-string predecessor: `p - 1` with borrow, in place — the
/// mirror of [`increment_key_in_place`]. An all-zero `p` borrows out and wraps to
/// all-`0xFF`; the sole caller never passes one.
fn decrement_key_in_place(p: &mut [u8]) {
    for b in p.iter_mut().rev() {
        *b = b.wrapping_sub(1);
        if *b != 0xFF {
            return;
        }
    }
}

/// A cut in the OPK key space, named by an OPK group prefix: either that
/// group's own minimum key or the first key above every member of it. Zero-
/// padding the prefix to the key width is what makes it the group's minimum.
#[derive(Clone, Copy)]
pub(crate) struct KeyCut<'a> {
    group: &'a [u8],
    above: bool,
}

impl<'a> KeyCut<'a> {
    /// The group's own minimum key — below every member of it.
    pub(crate) fn min_of(group: &'a [u8]) -> Self {
        KeyCut { group, above: false }
    }

    /// The first key above every member of the group. A saturated group — and
    /// the zero-width group, which is the whole key space — has none.
    pub(crate) fn above(group: &'a [u8]) -> Self {
        KeyCut { group, above: true }
    }

    /// This cut as a `stride`-wide key; `None` when it lies above the whole key
    /// space. The successor's carry ripples into the equality prefix, landing
    /// exactly on the first key of the next equality group.
    fn key(&self, stride: usize) -> Option<PkBuf> {
        let mut k = PkBuf::from_bytes(self.group);
        let exists = !self.above || increment_key_in_place(&mut k.bytes[..self.group.len()]);
        exists.then(|| k.widened(stride))
    }
}

/// Half-open `[start, end)` OPK key range between two cuts over a `stride`-byte
/// key space.
///
/// `None` = provably empty: `start` lies above the whole key space, or
/// `start >= end`. `end == None` inside `Some` means the range runs to the table
/// end.
pub(crate) fn key_range_between_cuts(start: KeyCut, end: KeyCut, stride: usize) -> Option<(PkBuf, Option<PkBuf>)> {
    let start = start.key(stride)?;
    let end = end.key(stride);
    if end.as_ref().is_some_and(|e| start.pk_bytes() >= e.pk_bytes()) {
        return None;
    }
    Some((start, end))
}

/// Map `range`'s cut pair to its half-open `[start, end)` OPK key range via
/// [`key_range_between_cuts`]: `Before(v)` is [`KeyCut::min_of`] the group
/// `encode(v)` names, `After(v)` is [`KeyCut::above`] it. `encode` returns that
/// group as a `PkBuf` whose `len` is the prefix width.
///
/// SQL bound semantics (inclusivity, unboundedness, out-of-range saturation)
/// are resolved to cuts in the planner; none reach this layer.
pub(crate) fn range_keys_from_cuts(
    range: &RangeDescriptor,
    stride: usize,
    mut encode: impl FnMut(u128) -> PkBuf,
) -> Option<(PkBuf, Option<PkBuf>)> {
    fn cut(c: Cut, group: &PkBuf) -> KeyCut<'_> {
        match c {
            Cut::Before(_) => KeyCut::min_of(group.pk_bytes()),
            Cut::After(_) => KeyCut::above(group.pk_bytes()),
        }
    }
    let (s, e) = (encode(range.start.value()), encode(range.end.value()));
    key_range_between_cuts(cut(range.start, &s), cut(range.end, &e), stride)
}

/// [`range_keys_from_cuts`] for a range whose leading `range.eq_vals()` columns
/// are equality-pinned and whose next column is cut-bounded — the shape both the
/// base-PK and the secondary-index walks take. `encode_leading` OPK-encodes the
/// first `n_eq + 1` column values into their group prefix; `arity` is the key's
/// column count and `stride` its full byte width.
///
/// `Ok(None)` = provably empty. `Err` = the descriptor pins every column with no
/// range column left within `arity` — a trust-boundary rejection the `pub` seek
/// paths surface and a backfill bound merely degrades on. Guarding here also
/// keeps `prefix_len < stride` strict, so the pad always extends the group key.
/// `what` names the key space in that message.
pub(crate) fn eq_prefix_range_keys(
    range: &RangeDescriptor,
    arity: usize,
    stride: usize,
    what: &str,
    encode_leading: impl Fn(&[u128]) -> PkBuf,
) -> Result<Option<(PkBuf, Option<PkBuf>)>, String> {
    let eq_natives = range.eq_vals();
    let n_eq = eq_natives.len();
    // Written `n_eq >= arity`, never a `+ 1` that could overflow on an
    // adversarial length.
    if n_eq >= arity {
        return Err(format!("{what}: n_eq {n_eq} has no range column within arity {arity}"));
    }
    let mut natives = [0u128; gnitz_wire::PK_LIST_MAX_COLS];
    natives[..n_eq].copy_from_slice(eq_natives);
    Ok(range_keys_from_cuts(range, stride, |v| {
        natives[n_eq] = v;
        encode_leading(&natives[..=n_eq])
    }))
}

/// True when every key in a half-open `[start, end)` range from
/// [`range_keys_from_cuts`] shares its leading `prefix` bytes. Since OPK order IS
/// byte order, it is enough that the range's first and last keys agree there.
///
/// The last key is `end - 1`, undoing the `After` successor (and any carry ripple)
/// the cut derivation applied — `Some(end)` only ever comes back with
/// `start < end`, so the decrement cannot borrow out. `end == None` means the end
/// cut carried out and the range runs to the table end, whose last key is
/// all-`0xFF`.
pub(crate) fn range_shares_prefix(start: &PkBuf, end: Option<&PkBuf>, prefix: usize) -> bool {
    let last = match end {
        Some(e) => {
            let mut l = *e;
            decrement_key_in_place(&mut l.bytes[..l.len as usize]);
            l
        }
        None => PkBuf::from_bytes(&[0xFFu8; MAX_PK_BYTES][..start.len as usize]),
    };
    start.pk_bytes()[..prefix] == last.pk_bytes()[..prefix]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::test_support::pk_only_schema;
    use gnitz_wire::{read_signed_exact, read_unsigned_exact};

    /// Independent typed reference comparator over native-LE PK bytes. This is
    /// the per-column column-walk that `compare_pk_bytes` used *before* the OPK
    /// flip; kept here as the test oracle for "OPK byte order == typed order".
    fn typed_cmp_pk_le(schema: &SchemaDescriptor, a: &[u8], b: &[u8]) -> Ordering {
        let mut off = 0usize;
        for (_ord, col) in schema.pk_columns() {
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
                    read_unsigned_exact(&a[off..off + cs]).cmp(&read_unsigned_exact(&b[off..off + cs]))
                }
                _ => read_signed_exact(&a[off..off + cs]).cmp(&read_signed_exact(&b[off..off + cs])),
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
        compare_pk_bytes(opk_key(schema, a).pk_bytes(), opk_key(schema, b).pk_bytes())
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
        // Per-column dispatch picks read_signed_exact for col 1 even though col 0
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

    /// Every specialized arm of `pack_pk_be` — the `{2,4,8,≥16}` register loads
    /// and the `9..=15` overlapping pair — must be byte-value-identical to the
    /// generic pad-and-copy at every width. A changed value would silently
    /// corrupt every `pack_pk_be` consumer (the cached sort keys, the route/guard
    /// keys, the bloom probes). Sweeps `1..=16` so no arm boundary is untested.
    #[test]
    fn pack_pk_be_specialization_matches_naive() {
        fn naive(pk: &[u8]) -> u128 {
            let take = pk.len().min(16);
            let mut buf = [0u8; 16];
            buf[..take].copy_from_slice(&pk[..take]);
            u128::from_be_bytes(buf)
        }
        for width in (1usize..=16).chain([24, 80]) {
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
                let (oa, ob) = (opk_key(&s, &a), opk_key(&s, &b));
                let (oa, ob) = (oa.pk_bytes(), ob.pk_bytes());
                prop_assert_eq!(compare_pk_ordering(oa, ob), compare_pk_bytes(oa, ob));
                prop_assert_eq!(
                    compare_pk_ordering(oa, ob) == std::cmp::Ordering::Equal,
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
                let (oa, ob) = (opk_key(&s, &a), opk_key(&s, &b));
                let (oa, ob) = (oa.pk_bytes(), ob.pk_bytes());
                prop_assert_eq!(pk_bytes_eq(oa, ob), oa == ob);
                prop_assert!(pk_bytes_eq(oa, oa));
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

    // --- increment_key_in_place -------------------------------------------

    #[test]
    fn succ_increments_low_byte() {
        let mut k = [0x00, 0x00, 0x05];
        assert!(increment_key_in_place(&mut k));
        assert_eq!(k, [0x00, 0x00, 0x06]);
    }

    #[test]
    fn succ_ripples_carry() {
        let mut k = [0x00, 0x00, 0xFF];
        assert!(increment_key_in_place(&mut k));
        assert_eq!(k, [0x00, 0x01, 0x00]);
    }

    #[test]
    fn succ_carries_out_on_all_ff() {
        let mut k = [0xFF, 0xFF];
        assert!(!increment_key_in_place(&mut k));
        assert_eq!(k, [0x00, 0x00]);
    }

    #[test]
    fn succ_empty_carries_out() {
        let mut k: [u8; 0] = [];
        assert!(!increment_key_in_place(&mut k));
    }

    // --- range_shares_prefix ----------------------------------------------

    fn buf(bytes: &[u8]) -> PkBuf {
        PkBuf::from_bytes(bytes)
    }

    /// The last key is `end - 1`, so a range ending exactly at the next group's
    /// first key still shares the prefix — one past that does not.
    #[test]
    fn shares_prefix_stops_at_the_group_boundary() {
        let start = buf(&[0x07, 0x00]);
        assert!(
            range_shares_prefix(&start, Some(&buf(&[0x07, 0x01])), 1),
            "one key wide"
        );
        assert!(
            range_shares_prefix(&start, Some(&buf(&[0x08, 0x00])), 1),
            "the whole group"
        );
        assert!(
            !range_shares_prefix(&start, Some(&buf(&[0x08, 0x01])), 1),
            "one key past"
        );
        // A borrow chain out of the trailing byte still lands in the group.
        assert!(range_shares_prefix(&buf(&[0x07, 0x05]), Some(&buf(&[0x08, 0x00])), 1));
    }

    /// `end == None` runs to the table end (all-`0xFF`), which only the topmost
    /// group shares a prefix with — that is what confines a maximal-value point.
    #[test]
    fn shares_prefix_handles_an_unbounded_end() {
        assert!(range_shares_prefix(&buf(&[0xFF, 0xFF]), None, 1));
        assert!(!range_shares_prefix(&buf(&[0x07, 0x00]), None, 1));
        // A zero-width distribution prefix is shared by everything.
        assert!(range_shares_prefix(&buf(&[0x07, 0x00]), None, 0));
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
