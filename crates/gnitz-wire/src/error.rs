//! Error type for the low-level WAL-block codec (`wal`).
//!
//! The codec is the one framer both the client (`gnitz-core`) and the engine
//! (`gnitz-engine`) run, so its error type lives here rather than in either
//! side's private error enum. The `Display` wording matches the engine's
//! `StorageError` (whose framer variants this replaces) so log lines and
//! substring asserts read identically across the split.

use std::fmt;

/// A malformed WAL block, as diagnosed by [`crate::wal::validate_and_parse`] /
/// [`crate::wal::encode`]. Coarse by design: every caller either `expect`s the
/// encode (a too-small output buffer is a caller bug) or maps a decode failure
/// into its own string-based protocol/storage error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalError {
    /// Buffer is shorter than the header / a directory entry / a region extent
    /// requires, or the declared `total_size` overruns the input.
    Truncated,
    /// Block format version is not the one this build speaks.
    InvalidVersion,
    /// XXH3 body checksum did not match the stored value.
    ChecksumMismatch,
    /// Output buffer was too small to hold the encoded block.
    BufferTooSmall,
    /// Region count exceeds the directory cap, or a directory entry failed
    /// validation.
    InvalidShard,
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            WalError::Truncated => "truncated",
            WalError::InvalidVersion => "invalid version",
            WalError::ChecksumMismatch => "checksum mismatch",
            WalError::BufferTooSmall => "buffer too small",
            WalError::InvalidShard => "invalid shard layout",
        };
        f.write_str(s)
    }
}
