//! Domain-specific error type for the storage subsystem.
//!
//! Replaces the historical `Result<_, i32>` / negative-i32 sentinel pattern
//! that several modules (wal, manifest, shard_file, shard_reader, memtable,
//! table, partitioned_table, shard_index, compact) used to share —
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
    /// rename, unlink, …).  `errno` is *not* captured today — callers that
    /// need it should convert at the syscall site.
    Io,
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
    /// MemTable hit its `max_bytes` ceiling — caller should flush and retry.
    Capacity,
    /// Shard directory entry / encoding byte / region offset failed validation.
    InvalidShard,
    /// CString conversion failed (path contained an interior NUL).
    InvalidPath,
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            StorageError::Io => "io error",
            StorageError::Truncated => "truncated",
            StorageError::InvalidMagic => "invalid magic",
            StorageError::InvalidVersion => "invalid version",
            StorageError::ChecksumMismatch => "checksum mismatch",
            StorageError::BufferTooSmall => "buffer too small",
            StorageError::Capacity => "capacity exceeded",
            StorageError::InvalidShard => "invalid shard layout",
            StorageError::InvalidPath => "invalid path",
        };
        f.write_str(s)
    }
}

impl std::error::Error for StorageError {}
