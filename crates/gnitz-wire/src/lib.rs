//! Shared wire-protocol definitions for GnitzDB.
//!
//! Single source of truth for constants and codecs that both the client
//! (gnitz-core) and server (gnitz-server) must agree on.
//!
//! The crate is organized into topic modules, and most items are re-exported
//! flat at the crate root (`gnitz_wire::FOO`) so callers need not track which
//! module a symbol lives in. `control`, `schema_block`, `sys_rows`, `txn_frame`
//! and `wal` stay named modules and are referenced by path
//! (`gnitz_wire::wal::encode`). Only `wal::encode` and `schema_block::encode`
//! actually collide at the root; the other three are namespaced for consistency
//! with them rather than by necessity. `wal`'s *constants* (`WAL_*`,
//! `MAX_WIRE_REGIONS`, the `REG_*` region-convention indices) are flat-exported,
//! since they are referenced pervasively.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

/// Declare a **wire enum**: a closed set of values crossing the wire, with the
/// total encode and partial decode every such set needs.
///
/// `ALL`, `as_wire` and `from_wire` are all generated from the one variant list,
/// so a decode table cannot disagree with the discriminants it mirrors and a new
/// variant is covered without a second edit.
///
/// `TypeCode` stays hand-written: it carries extra per-variant data
/// (`wire_name`), and its `ALL` is a fixed-size array — the shape `gnitz-py`
/// builds its `TypeCode` IntEnum from — where this macro emits a slice. Moving
/// it would rewrite call sites in three other crates to buy nothing.
macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident: $repr:ident {
            $($(#[$vmeta:meta])* $variant:ident = $value:expr),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr($repr)]
        $vis enum $name {
            $($(#[$vmeta])* $variant = $value),+
        }

        impl $name {
            /// Every variant, in declaration order.
            pub const ALL: &'static [$name] = &[$($name::$variant),+];

            /// This variant's wire value.
            #[inline]
            pub const fn as_wire(self) -> $repr {
                self as $repr
            }

            /// The variant `v` names, or `None` for a value outside the set.
            ///
            /// Resolved against the discriminants themselves rather than a
            /// second table of arms, which is what makes a decode/declaration
            /// disagreement unrepresentable. These sets are a handful of
            /// variants wide; a scan is not a table lookup worth writing twice.
            #[inline]
            pub const fn from_wire(v: $repr) -> Option<Self> {
                let mut i = 0;
                while i < Self::ALL.len() {
                    if Self::ALL[i] as $repr == v {
                        return Some(Self::ALL[i]);
                    }
                    i += 1;
                }
                None
            }
        }
    };
}

mod catalog;
mod circuit;
mod codec;
mod error;
mod expr;
mod flags;
mod german_string;
mod handshake;
mod pk;
mod range;
mod read_spec;
mod rel_descriptor;
mod types;
mod uuid;

pub mod control;
pub mod schema_block;
pub mod sys_rows;
pub mod txn_frame;
pub mod wal;

pub use catalog::*;
pub use circuit::*;
pub use error::*;
pub use expr::*;
pub use flags::*;
pub use german_string::*;
pub use handshake::*;
pub use pk::*;
pub use range::*;
pub use read_spec::*;
pub use rel_descriptor::*;
pub use types::*;
pub use uuid::*;
// Flat-export `wal`'s constants (referenced everywhere) but not its framer
// functions (`encode`/`block_size`/… stay `gnitz_wire::wal::`-qualified).
pub use wal::{
    MAX_WIRE_REGIONS, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT, WAL_FORMAT_VERSION, WAL_HEADER_SIZE,
    WAL_OFF_CHECKSUM, WAL_OFF_COUNT, WAL_OFF_NUM_REGIONS, WAL_OFF_SIZE, WAL_OFF_TID, WAL_OFF_VERSION,
};

// ---------------------------------------------------------------------------
// Low-level byte primitives.
//
// Two calling conventions over two rules: the `*_le(buf, off)` offset form for
// region walks (no slice construction at the call site), and
// `read_{signed,unsigned}_exact(cell)` for typed cell reads, where the slice IS
// the column. A caller holding a tail sub-slices it itself — at `opt-level=0`
// the `bytes[..size]` bound is an out-of-line `Range::index` call, so the
// comparators over already-exact windows must not pay it.
//
// **Inlining policy for this crate's per-row primitives:** they carry
// `#[inline(always)]`, not `#[inline]`. The E2E suite runs the debug binary, and
// at `opt-level=0` LLVM runs only the always-inline pass — a plain hint leaves a
// real call frame around a body that is often a single load or shift. Individual
// items below do not restate this.
// ---------------------------------------------------------------------------

/// Align `n` up to an 8-byte boundary.
pub const fn align8(n: usize) -> usize {
    (n + 7) & !7
}

/// XXH3-64 over `b` — the WAL body checksum. The one hash both ends compute;
/// the engine's `foundation::xxh::checksum` re-exports this.
#[inline]
pub fn checksum(b: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(b)
}

/// `V₀` — the group key of the ungrouped (global) aggregate: the XXH3-128
/// digest over no group columns at all. The single definition both ends share,
/// so the engine's emitted ground row and the client's synthesized one carry the
/// same key with no literal embedded on either side.
#[inline]
pub fn global_group_key() -> u128 {
    xxhash_rust::xxh3::xxh3_128(b"")
}

#[inline]
pub(crate) fn read_u16_le(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(buf[off..off + 2].try_into().unwrap())
}

#[inline]
pub(crate) fn write_u16_le(buf: &mut [u8], off: usize, val: u16) {
    buf[off..off + 2].copy_from_slice(&val.to_le_bytes());
}

#[inline]
pub fn read_u32_le(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline(always)]
pub fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

#[inline]
pub(crate) fn read_u128_le(buf: &[u8], off: usize) -> u128 {
    u128::from_le_bytes(buf[off..off + 16].try_into().unwrap())
}

#[inline]
pub fn read_i64_le(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

/// Padding-free little-endian scalars a region may be reinterpreted as. A sealed
/// bound rather than `T: Copy`, which also admits padded types whose padding
/// bytes are never initialized — reading those as `u8` is UB, and a safe fn must
/// not carry an unenforced precondition.
pub trait LeScalar: Copy {}
macro_rules! le_scalar {
    ($($t:ty),*) => { $(impl LeScalar for $t {})* };
}
le_scalar!(u8, i8, u16, i16, u32, i32, u64, i64, u128, i128);

/// Reinterpret a `&[T]` of LE scalars as the region bytes it already is. On the
/// little-endian target the native layout *is* the wire/shard layout, so this is
/// the zero-copy way to hand a typed column (weights, null words, `u128` cells)
/// to the byte-oriented region APIs.
/// `#[inline(always)]`: `gnitz-wire` has no dev opt-level override, and the
/// per-morsel callers in gnitz-expr are themselves `#[inline(always)]` for
/// opt-0 consumers, where only the always-inline pass runs.
#[inline(always)]
pub fn as_le_bytes<T: LeScalar>(v: &[T]) -> &[u8] {
    // SAFETY: `size_of_val(v)` initialized bytes borrowed from `v`, consumed as
    // opaque bytes and never as typed values.
    unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<u8>(), std::mem::size_of_val(v)) }
}

/// Read a whole 1/2/4/8-byte little-endian **signed** cell, sign-extended to
/// i64 — the native-LE payload/decoded-PK read. `bytes.len()` IS the column
/// width. Sibling of [`read_unsigned_exact`]; the two are the one pair every
/// fixed-int value read goes through.
#[inline(always)]
pub fn read_signed_exact(bytes: &[u8]) -> i64 {
    match bytes.len() {
        1 => bytes[0] as i8 as i64,
        2 => i16::from_le_bytes(bytes.try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes.try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes.try_into().unwrap()),
        // Static message on purpose: this body is `#[inline(always)]` and lands
        // at every per-row read site, and a formatted one duplicates its
        // `Arguments` block into each.
        _ => unreachable!("read_signed_exact: unexpected column width"),
    }
}

/// Read a whole 1/2/4/8-byte little-endian **unsigned** cell, zero-extended to
/// u64. See [`read_signed_exact`].
#[inline(always)]
pub fn read_unsigned_exact(bytes: &[u8]) -> u64 {
    match bytes.len() {
        1 => bytes[0] as u64,
        2 => u16::from_le_bytes(bytes.try_into().unwrap()) as u64,
        4 => u32::from_le_bytes(bytes.try_into().unwrap()) as u64,
        8 => u64::from_le_bytes(bytes.try_into().unwrap()),
        _ => unreachable!("read_unsigned_exact: unexpected column width"),
    }
}

/// True iff payload null-bit `pi` is set in `word` (the column is NULL).
///
/// The null word packs one bit per *payload* column: bit `pi` is the `pi`-th
/// non-PK column in schema order (the region convention, whose `REG_NULL_BMP`
/// index this crate already owns). These two accessors are the **one** read/write
/// convention for the bitmap, shared by the client (`gnitz-core`), the evaluator
/// (`gnitz-expr`) and the engine — it was previously spelled out in each.
#[inline(always)]
pub fn null_word_get(word: u64, pi: usize) -> bool {
    (word >> pi) & 1 == 1
}

/// Set (`is_null == true`) or clear (`is_null == false`) payload null-bit `pi`
/// in `word`. See [`null_word_get`].
#[inline(always)]
pub fn null_word_set(word: &mut u64, pi: usize, is_null: bool) {
    if is_null {
        *word |= 1u64 << pi;
    } else {
        *word &= !(1u64 << pi);
    }
}

/// Null word with the low `npc` payload bits set — "all `npc` payload columns
/// are null". `npc` reaches the row-major cap of 64 only when a schema has
/// exactly 64 payload columns; `1u64 << 64` would panic in debug builds, so
/// that boundary returns all-ones directly.
#[inline]
pub fn all_payload_null_mask(npc: usize) -> u64 {
    if npc < 64 {
        (1u64 << npc) - 1
    } else {
        u64::MAX
    }
}

/// Concatenate two rows' null words for an output row laid out as
/// `[left payload..., right payload...]`. The right bits shift up by `left_npc`.
/// `left_npc` reaches 64 only when the right side has no payload columns, in
/// which case `right` is 0 and the dropped shift is a no-op.
#[inline]
pub fn merge_null_words(left: u64, right: u64, left_npc: usize) -> u64 {
    if left_npc < 64 {
        left | (right << left_npc)
    } else {
        left
    }
}

// `write_u32_le` has only in-crate callers (the WAL framer, the control-block
// encoder, the handshake), so it stays crate-internal rather than widening the
// public surface with a dead export.
#[inline]
pub(crate) fn write_u32_le(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline(always)]
pub fn write_u64_le(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
}

#[inline]
pub(crate) fn write_u128_le(buf: &mut [u8], off: usize, val: u128) {
    buf[off..off + 16].copy_from_slice(&val.to_le_bytes());
}

#[cfg(test)]
#[path = "tests/lib.rs"]
mod tests;
