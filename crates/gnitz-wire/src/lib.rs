//! Shared wire-protocol definitions for GnitzDB.
//!
//! Single source of truth for what the client (gnitz-core) and the engine
//! (gnitz-zset, gnitz-store and gnitz-server) must agree on: the constants and codecs, the
//! typed forms of the wire payloads, and the semantic rules both sides compute
//! with. It is the lowest crate every consumer of a rule links, so a rule that
//! lands anywhere else can drift.
//!
//! So this is the shared **model**, not only a byte format: a rule both sides
//! compute belongs here even when it crosses no wire, as `ReduceOutKey` does.
//!
//! The crate is organized into topic modules under one export rule: a private
//! module is glob-re-exported flat at the crate root (`gnitz_wire::FOO`), so
//! callers need not track which module a symbol lives in; a module whose item
//! names read relative to the module is public and referenced by path
//! (`gnitz_wire::wal::parse_block`). No item is reachable at two paths.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.
//!
//! Per-row primitives are `#[inline(always)]`: the E2E suite runs the
//! `opt-level=0` binary, where a plain `#[inline]` still leaves a call.

#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

/// `$body` once per [`FixedInt`] variant, with `$c` that variant as a `const`,
/// so a width or codec call on it is a literal inside the row loop.
#[macro_export]
macro_rules! for_each_fixed_int {
    ($fi:expr, |$c:ident| $body:block) => {
        match $fi {
            $crate::FixedInt::U8 => {
                const $c: $crate::FixedInt = $crate::FixedInt::U8;
                $body
            }
            $crate::FixedInt::I8 => {
                const $c: $crate::FixedInt = $crate::FixedInt::I8;
                $body
            }
            $crate::FixedInt::U16 => {
                const $c: $crate::FixedInt = $crate::FixedInt::U16;
                $body
            }
            $crate::FixedInt::I16 => {
                const $c: $crate::FixedInt = $crate::FixedInt::I16;
                $body
            }
            $crate::FixedInt::U32 => {
                const $c: $crate::FixedInt = $crate::FixedInt::U32;
                $body
            }
            $crate::FixedInt::I32 => {
                const $c: $crate::FixedInt = $crate::FixedInt::I32;
                $body
            }
            $crate::FixedInt::U64 => {
                const $c: $crate::FixedInt = $crate::FixedInt::U64;
                $body
            }
            $crate::FixedInt::I64 => {
                const $c: $crate::FixedInt = $crate::FixedInt::I64;
                $body
            }
        }
    };
}

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

        impl $crate::Wire for $name {
            fn write(&self, w: &mut $crate::Writer) {
                w.$repr(self.as_wire());
            }
            fn read(r: &mut $crate::Reader) -> Result<Self, String> {
                let v = r.$repr()?;
                Self::from_wire(v).ok_or_else(|| format!(concat!("unknown ", stringify!($name), " {}"), v))
            }
        }
    };
}

mod bytes;
mod catalog;
mod circuit;
mod codec;
mod column;
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

pub use bytes::*;
pub use catalog::*;
pub use circuit::*;
pub use codec::*;
pub use column::*;
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
