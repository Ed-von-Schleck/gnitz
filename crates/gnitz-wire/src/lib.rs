//! Shared wire-protocol definitions for GnitzDB.
//!
//! Single source of truth for constants and codecs that both the client
//! (gnitz-core) and server (gnitz-engine) must agree on.
//!
//! The crate is organized into topic modules, but every item is re-exported
//! flat at the crate root (`gnitz_wire::FOO`) so callers need not track which
//! module a symbol lives in. `control` and `type_code` are the two exceptions:
//! they remain named modules because callers reference them by path.

#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

mod catalog;
mod circuit;
mod expr;
mod flags;
mod german_string;
mod handshake;
mod pk;
mod range;
mod types;
mod uuid;
mod wal;

pub mod control;

pub use catalog::*;
pub use circuit::*;
pub use expr::*;
pub use flags::*;
pub use german_string::*;
pub use handshake::*;
pub use pk::*;
pub use range::*;
pub use types::*;
pub use uuid::*;
pub use wal::*;

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Align `n` up to an 8-byte boundary.
pub const fn align8(n: usize) -> usize {
    (n + 7) & !7
}
