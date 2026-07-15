//! Shared wire-protocol definitions for GnitzDB.
//!
//! Single source of truth for constants and codecs that both the client
//! (gnitz-core) and server (gnitz-engine) must agree on.
//!
//! The crate is organized into topic modules, but every item is re-exported
//! flat at the crate root (`gnitz_wire::FOO`) so callers need not track which
//! module a symbol lives in. `control`, `type_code`, and `wal` are the
//! exceptions: they remain named modules because callers reference their
//! functions by path (`gnitz_wire::wal::encode`) — the generic names would
//! collide at the crate root. `wal`'s *constants* (`WAL_*`, `MAX_WIRE_REGIONS`)
//! are still flat-exported, since they are referenced pervasively.

#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

mod catalog;
mod circuit;
mod error;
mod expr;
mod flags;
mod german_string;
mod handshake;
mod pk;
mod range;
mod types;
mod uuid;

pub mod control;
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
pub use types::*;
pub use uuid::*;
// Flat-export `wal`'s constants (referenced everywhere) but not its framer
// functions (`encode`/`block_size`/… stay `gnitz_wire::wal::`-qualified).
pub use wal::{
    IPC_CONTROL_TID, MAX_WIRE_REGIONS, WAL_FORMAT_VERSION, WAL_HEADER_SIZE, WAL_OFF_CHECKSUM, WAL_OFF_COUNT,
    WAL_OFF_NUM_REGIONS, WAL_OFF_SIZE, WAL_OFF_TID, WAL_OFF_VERSION,
};

// ---------------------------------------------------------------------------
// Low-level byte primitives — the wire codec owns them; the engine re-exports
// (`foundation::codec`) rather than redefining, same as `align8`.
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

#[inline]
pub fn read_u32_le(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline]
pub fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

// `write_u32_le` is used only by the WAL framer within this crate (the engine
// re-exports the other three LE helpers but never this one), so it stays
// crate-internal rather than widening the public surface with a dead export.
#[inline]
pub(crate) fn write_u32_le(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline]
pub fn write_u64_le(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
}
