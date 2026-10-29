//! Shared wire-protocol definitions for GnitzDB.
//!
//! Single source of truth for what the client (gnitz-core) and the engine
//! (gnitz-zset, gnitz-store and gnitz-server) must agree on: the constants and codecs, the
//! typed forms of the wire payloads, and the semantic rules both sides compute
//! with. It is the lowest crate every consumer of a rule links, so a rule that
//! lands anywhere else can drift.
//!
//! So this is the shared **model**, not only a byte format: a rule both sides
//! compute belongs here even when it crosses no wire, as
//! `AggFunc::output_type` does.
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

/// Declare a **wire enum**: a closed set of values crossing the wire, with the
/// total encode and partial decode every such set needs.
///
/// `ALL`, `as_wire`, `from_wire` and `variant_name` (which `Debug` prints) are
/// all generated from the one variant list, so a decode table cannot disagree
/// with the discriminants it mirrors and a new variant is covered without a
/// second edit.
#[macro_export]
macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident: $repr:ident {
            $($(#[$vmeta:meta])* $variant:ident = $value:expr),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq)]
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

            /// This variant's name, as declared.
            pub const fn variant_name(self) -> &'static str {
                match self {
                    $($name::$variant => stringify!($variant)),+
                }
            }
        }

        impl ::core::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(self.variant_name())
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

/// The first value outside a wire enum, for a test forging one — not a
/// literal, which the next variant added would quietly turn into a valid value.
#[cfg(test)]
fn outside<V: From<u8> + Copy, T>(from_wire: impl Fn(V) -> Option<T>) -> V {
    (0..=u8::MAX).map(V::from).find(|&v| from_wire(v).is_none()).unwrap()
}

/// The most workers a cluster runs.
pub const MAX_WORKERS: usize = 64;

mod bytes;
mod catalog;
mod circuit;
mod codec;
mod column;
mod control;
mod decimal;
mod deframe;
mod german_string;
mod handshake;
mod heap;
mod ident;
mod operator;
mod pk;
mod pk_list;
mod props;
mod range;
mod read_spec;
mod region;
mod rel_descriptor;
mod temporal;
mod types;
mod uuid;
mod xxh;

#[cfg(feature = "field-map")]
pub mod fields;
pub mod schema_block;
pub mod snapshot;
pub mod txn_frame;
pub mod wal;

pub use bytes::*;
pub use catalog::*;
pub use circuit::*;
pub use codec::*;
pub use column::*;
pub use control::*;
pub use decimal::*;
pub use deframe::*;
pub use german_string::*;
pub use handshake::*;
pub use heap::*;
pub use ident::*;
pub use operator::*;
pub use pk::*;
pub use pk_list::*;
pub use props::*;
pub use range::*;
pub use read_spec::*;
pub use region::*;
pub use rel_descriptor::*;
pub use temporal::*;
pub use types::*;
pub use uuid::*;
pub use xxh::*;
