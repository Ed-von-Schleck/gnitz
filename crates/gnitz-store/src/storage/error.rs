//! Domain-specific error type for the storage subsystem.
//!
//! Replaces the historical `Result<_, i32>` / negative-i32 sentinel pattern
//! that several modules (wal, manifest, shard_file, shard_reader, run_set,
//! table, shard_index, compact) used to share —
//! sometimes with overlapping `-1`/`-2`/`-3` meanings.
//!
//! The mapping is intentionally coarse: the engine treats almost all
//! storage failures as fatal (it `unwrap`s or `let _ =`s them), so the value
//! of the type is in being *unambiguous and grep-able*, not in carrying rich
//! context.  Add variants when a caller actually needs to discriminate.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    /// Underlying libc I/O error (open, fstat, mmap, pwrite, fdatasync,
    /// rename, unlink, …), carrying the `errno` so a fatal report can separate
    /// a full disk from an exhausted fd table. `0` when the failure was
    /// synthesized rather than reported by a syscall.
    Io(i32),
    /// File or buffer is shorter than the on-disk header / payload requires.
    Truncated,
    /// File magic number didn't match (wrong file type or corruption).
    InvalidMagic,
    /// On-disk format version is not supported by this build.
    InvalidVersion,
    /// xxh3 checksum did not match the stored value.
    ChecksumMismatch,
    /// Output buffer was too small to hold the encoded form.
    BufferTooSmall,
    /// Shard directory entry / encoding byte / region offset failed validation.
    InvalidShard,
    /// CString conversion failed (path contained an interior NUL).
    InvalidPath,
}

impl From<std::io::Error> for StorageError {
    /// The one place an `io::Error` becomes a `StorageError`, so no call site
    /// has to remember to keep the errno.
    fn from(e: std::io::Error) -> Self {
        StorageError::Io(e.raw_os_error().unwrap_or(0))
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Not `from_raw_os_error(0)`: that renders "Success".
            StorageError::Io(0) => f.write_str("io error"),
            StorageError::Io(e) => write!(f, "io error: {}", std::io::Error::from_raw_os_error(*e)),
            StorageError::Truncated => f.write_str("truncated"),
            StorageError::InvalidMagic => f.write_str("invalid magic"),
            StorageError::InvalidVersion => f.write_str("invalid version"),
            StorageError::ChecksumMismatch => f.write_str("checksum mismatch"),
            StorageError::BufferTooSmall => f.write_str("buffer too small"),
            StorageError::InvalidShard => f.write_str("invalid shard layout"),
            StorageError::InvalidPath => f.write_str("invalid path"),
        }
    }
}
