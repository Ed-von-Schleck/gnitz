//! Domain-specific error type for the storage subsystem.
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
    /// An on-disk image failed a check; the reason names which one.
    Corrupt(&'static str),
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
            StorageError::Corrupt(reason) => write!(f, "corrupt: {reason}"),
            StorageError::InvalidPath => f.write_str("invalid path"),
        }
    }
}

/// Why a store verb did not do what it was asked. The three variants differ in
/// what the caller may do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// Nothing was applied and no state diverged: the message is the whole
    /// answer to whoever asked, whether the fault is the frame's, the caller's
    /// own state, or an internal one (a replay that fails, a walk that meets a
    /// skeleton row with no hydrator).
    Rejected(String),
    /// The store failed to read or write. Committed data may not have reached it,
    /// which leaves this process diverged from whatever durable log carried it.
    Storage { context: String, err: StorageError },
    /// A delta cursor below the round this worker has already dropped. The text
    /// is the refusal as the reader phrases it; the variant is what a wire reply
    /// branches on.
    DeltaExpired(String),
}

impl StoreError {
    pub fn rejected(message: impl Into<String>) -> Self {
        StoreError::Rejected(message.into())
    }

    pub fn storage(context: impl Into<String>, err: StorageError) -> Self {
        StoreError::Storage { context: context.into(), err }
    }

    /// Prefix the refusal with what the caller was doing. A `DeltaExpired`
    /// message is the callee's whole answer, so it passes through.
    pub fn in_context(self, what: &str) -> Self {
        match self {
            StoreError::Storage { context, err } => StoreError::Storage {
                context: format!("{what}: {context}"),
                err,
            },
            StoreError::Rejected(m) => StoreError::Rejected(format!("{what}: {m}")),
            other @ StoreError::DeltaExpired(_) => other,
        }
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Rejected(m) | StoreError::DeltaExpired(m) => f.write_str(m),
            StoreError::Storage { context, err } => write!(f, "{context}: {err}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// The server's error plumbing is string-typed; this is what keeps `?` working
/// at every one of its call sites.
impl From<StoreError> for String {
    fn from(e: StoreError) -> String {
        e.to_string()
    }
}
