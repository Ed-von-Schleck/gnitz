//! Shared wire-protocol definitions for GnitzDB.
//!
//! Single source of truth for what the client (gnitz-core) and the server
//! (gnitz-server) must agree on: the constants and codecs, the typed forms of
//! the wire payloads (`OpNode`, `MapKind`, `IndexBound`, `ReadSpec`), and the
//! semantic rules both sides compute with (`agg_output_type`,
//! `raw_output_nullable`, `TypeCode::join_key_common_type`,
//! `ReduceOutKey::for_group_cols`, `worker_for_key`). It is the lowest crate
//! every consumer of a rule links, so a rule that lands anywhere else can drift.
//!
//! So this is the shared **model**, not only a byte format: a rule both sides
//! compute belongs here even when it crosses no wire, as `ReduceOutKey` does.
//!
//! The crate is organized into topic modules under one export rule: a private
//! module is glob-re-exported flat at the crate root (`gnitz_wire::FOO`), so
//! callers need not track which module a symbol lives in; a module whose item
//! names read relative to the module is public and referenced by path
//! (`gnitz_wire::wal::WalBlock`). No item is reachable at two paths.
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
#[macro_export]
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
            /// disagreement unrepresentable. A scan is not a table lookup worth
            /// writing twice: over a dense discriminant run LLVM folds this to a
            /// branchless range check with no `.rodata` table at all.
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
mod deframe;
mod flags;
mod german_string;
mod handshake;
mod pk;
mod range;
mod read_spec;
mod region;
mod rel_descriptor;
mod types;
mod uuid;
mod xxh;

pub mod control;
pub mod decimal;
pub mod schema_block;
pub mod sys_rows;
pub mod txn_frame;
pub mod wal;

pub use catalog::*;
pub use circuit::*;
// The cursor itself, for the payloads other crates encode through it.
pub use codec::{Reader, Writer};
pub use deframe::*;
pub use flags::*;
pub use german_string::*;
pub use handshake::*;
pub use pk::*;
pub use range::*;
pub use read_spec::*;
pub use region::*;
pub use rel_descriptor::*;
pub use types::*;
pub use uuid::*;
pub use xxh::*;

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

#[inline(always)]
pub fn read_u32_le(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline(always)]
pub fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

#[inline(always)]
pub fn read_i64_le(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

mod sealed {
    pub trait Sealed {}
}

/// Padding-free little-endian scalars whose every byte pattern is a valid
/// value — the types a region may be reinterpreted as. Sealed: a padded type's
/// padding bytes are never initialized, reading them as `u8` is UB, and a safe
/// fn must not carry an unenforced precondition.
pub trait LeScalar: Copy + sealed::Sealed {}
macro_rules! le_scalar {
    ($($t:ty),*) => { $(impl sealed::Sealed for $t {} impl LeScalar for $t {})* };
}
le_scalar!(u32, u64, i64);

/// Reinterpret a `&[T]` of LE scalars as the region bytes it already is. On the
/// little-endian target the native layout *is* the wire/shard layout, so this is
/// the zero-copy way to hand a typed column (weights, null words) to the
/// byte-oriented region APIs.
#[inline(always)]
pub fn as_le_bytes<T: LeScalar>(v: &[T]) -> &[u8] {
    // SAFETY: `size_of_val(v)` initialized bytes borrowed from `v`, consumed as
    // opaque bytes and never as typed values.
    unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<u8>(), std::mem::size_of_val(v)) }
}

/// [`as_le_bytes`] for writing: every byte pattern is a valid `LeScalar`.
#[inline(always)]
pub fn as_le_bytes_mut<T: LeScalar>(v: &mut [T]) -> &mut [u8] {
    // SAFETY: `size_of_val(v)` initialized bytes exclusively borrowed from `v`; `LeScalar` admits
    // only padding-free integers, so any written value is a valid `T`.
    unsafe { std::slice::from_raw_parts_mut(v.as_mut_ptr().cast::<u8>(), std::mem::size_of_val(v)) }
}

/// Read a whole 1/2/4/8-byte little-endian **signed** cell, sign-extended to
/// i64 — the native-LE payload/decoded-PK read. `bytes.len()` IS the column
/// width. Sibling of [`read_unsigned_exact`].
#[inline(always)]
pub fn read_signed_exact(bytes: &[u8]) -> i64 {
    match bytes.len() {
        1 => bytes[0] as i8 as i64,
        2 => i16::from_le_bytes(bytes.try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes.try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes.try_into().unwrap()),
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

/// A `u64` with its low `n` bits set, total at every `n`: `1u64 << 64` is
/// undefined, so `n >= 64` answers all-ones directly. The one place that guard
/// lives, whatever the bits mean — a caller whose `n` is provably below 64 needs
/// no guard and says so where it proves it.
#[inline(always)]
pub const fn low_bits_mask(n: usize) -> u64 {
    if n < 64 {
        (1u64 << n) - 1
    } else {
        u64::MAX
    }
}

/// Yields the set bit positions of a mask, lowest first.
pub struct BitIter(pub u64);

impl Iterator for BitIter {
    type Item = usize;

    #[inline(always)]
    fn next(&mut self) -> Option<usize> {
        if self.0 == 0 {
            return None;
        }
        let i = self.0.trailing_zeros() as usize;
        self.0 &= self.0 - 1;
        Some(i)
    }
}

#[inline(always)]
pub fn write_u32_le(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline(always)]
pub fn write_u64_le(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
}

#[cfg(test)]
#[path = "tests/lib.rs"]
mod tests;
