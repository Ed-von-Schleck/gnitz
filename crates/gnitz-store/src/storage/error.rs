//! `StorageError` is the storage rung's failure: an errno, or a named on-disk
//! image check.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    /// Underlying libc I/O error, carrying the `errno` so a fatal report can
    /// separate a full disk from an exhausted fd table. `0` when the failure was
    /// synthesized rather than reported by a syscall.
    Io(i32),
    /// An on-disk image failed a check; the reason names which one.
    Corrupt(&'static str),
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
            StorageError::Corrupt(reason) => write!(f, "corrupt: {reason}"),
        }
    }
}
