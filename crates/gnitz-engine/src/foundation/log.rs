//! Logging module for gnitz-engine.
//!
//! Log format: `secs.millis tag LEVEL msg` on stderr (fd 2).
//! Log levels:
//!   0 = QUIET  (error/warn only)
//!   1 = NORMAL (+ info)
//!   2 = DEBUG  (+ debug)

use std::sync::atomic::{AtomicU32, Ordering};

static LEVEL: AtomicU32 = AtomicU32::new(0);

/// Process tag packed as [b0, b1, b2, len] in native byte order.
static TAG: AtomicU32 = AtomicU32::new(0);

pub const QUIET: u32 = 0;
pub const NORMAL: u32 = 1;
pub const DEBUG: u32 = 2;

/// Set the log level and process tag.
pub fn init(level: u32, tag: &[u8]) {
    LEVEL.store(level.min(DEBUG), Ordering::Relaxed);
    let len = tag.len().min(3);
    let mut bytes = [0u8; 4];
    bytes[..len].copy_from_slice(&tag[..len]);
    bytes[3] = len as u8;
    TAG.store(u32::from_ne_bytes(bytes), Ordering::Relaxed);
}

#[inline(always)]
pub fn is_debug() -> bool {
    LEVEL.load(Ordering::Relaxed) >= DEBUG
}

#[inline(always)]
pub fn is_info() -> bool {
    LEVEL.load(Ordering::Relaxed) >= NORMAL
}

/// Format and write a log line to stderr. Called by macros, not directly.
///
/// Formats straight into a fixed stack buffer — no heap allocation, so
/// `gnitz_fatal_abort!` can log with a broken SAL mmap — and has no panic
/// paths. An over-long message is truncated; the trailing `\n` is always the
/// final byte, so even a truncated line terminates inside the one `write(2)`.
#[cold]
pub fn _emit(level_tag: &str, args: core::fmt::Arguments<'_>) {
    let mut buf = [0u8; 512];
    let len = format_line(&mut buf, level_tag, args);
    unsafe {
        libc::write(2, buf.as_ptr() as *const libc::c_void, len);
    }
}

/// Truncating `fmt::Write` over a fixed buffer: keeps what fits in
/// `buf[..limit]`, silently discards the rest, and never returns `Err`
/// (a propagated `fmt::Error` would panic inside `write!`).
struct TruncatingWriter<'a> {
    buf: &'a mut [u8],
    limit: usize,
    pos: usize,
}

impl core::fmt::Write for TruncatingWriter<'_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let n = s.len().min(self.limit - self.pos);
        self.buf[self.pos..self.pos + n].copy_from_slice(&s.as_bytes()[..n]);
        self.pos += n;
        Ok(())
    }
}

/// Assemble `secs.millis tag LEVEL msg\n` into `buf`, truncating an over-long
/// message so the trailing `\n` is always the final byte. Returns the line
/// length in bytes.
fn format_line(buf: &mut [u8; 512], level_tag: &str, args: core::fmt::Arguments<'_>) -> usize {
    use core::fmt::Write;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();

    let packed = TAG.load(Ordering::Relaxed);
    let tag_bytes = u32::to_ne_bytes(packed);
    let tag_len = (tag_bytes[3] as usize).min(3);
    let tag = std::str::from_utf8(&tag_bytes[..tag_len]).unwrap_or("");

    let limit = buf.len() - 1; // reserve the final byte for '\n'
    let mut w = TruncatingWriter { buf, limit, pos: 0 };
    let _ = write!(w, "{}.{:03} {} {} ", now.as_secs(), now.subsec_millis(), tag, level_tag);
    let _ = w.write_fmt(args);
    let pos = w.pos;
    buf[pos] = b'\n';
    pos + 1
}

/// Log at ERROR level (always emits).
#[macro_export]
macro_rules! gnitz_error {
    ($($arg:tt)*) => {
        $crate::foundation::log::_emit("ERROR", format_args!($($arg)*));
    };
}

/// Log at WARN level (always emits).
#[macro_export]
macro_rules! gnitz_warn {
    ($($arg:tt)*) => {
        $crate::foundation::log::_emit("WARN", format_args!($($arg)*));
    };
}

/// Log at INFO level (emits when level >= NORMAL).
#[macro_export]
macro_rules! gnitz_info {
    ($($arg:tt)*) => {
        if $crate::foundation::log::is_info() {
            $crate::foundation::log::_emit("INFO", format_args!($($arg)*));
        }
    };
}

/// Log at DEBUG level (emits when level >= DEBUG).
#[macro_export]
macro_rules! gnitz_debug {
    ($($arg:tt)*) => {
        if $crate::foundation::log::is_debug() {
            $crate::foundation::log::_emit("DEBUG", format_args!($($arg)*));
        }
    };
}

/// Emit a FATAL log line and immediately terminate the process with exit
/// code 134 (= 128 + SIGABRT). Uses `libc::_exit` to skip atexit handlers,
/// TLS destructors, and stdio flush — none of which are safe to run with a
/// broken SAL mmap or in-flight io_uring SQEs. Matches the "aborted" signal
/// convention that systemd/monit expect.
#[macro_export]
macro_rules! gnitz_fatal_abort {
    ($($arg:tt)*) => {{
        $crate::foundation::log::_emit("FATAL", format_args!($($arg)*));
        unsafe { ::libc::_exit(134) };
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_and_level_checks() {
        init(QUIET, b"T");
        assert!(!is_info());
        assert!(!is_debug());

        init(NORMAL, b"T");
        assert!(is_info());
        assert!(!is_debug());

        init(DEBUG, b"T");
        assert!(is_info());
        assert!(is_debug());

        // Reset to quiet for other tests
        init(QUIET, b"");
    }

    #[test]
    fn test_tag_packing() {
        init(DEBUG, b"W2");
        let packed = TAG.load(Ordering::Relaxed);
        let bytes = u32::to_ne_bytes(packed);
        assert_eq!(bytes[0], b'W');
        assert_eq!(bytes[1], b'2');
        assert_eq!(bytes[3], 2); // length

        init(DEBUG, b"M");
        let packed = TAG.load(Ordering::Relaxed);
        let bytes = u32::to_ne_bytes(packed);
        assert_eq!(bytes[0], b'M');
        assert_eq!(bytes[3], 1);

        init(QUIET, b"");
    }

    #[test]
    fn test_format_line_shape() {
        // `secs.millis tag LEVEL msg\n` — millis zero-padded to 3 digits.
        init(NORMAL, b"W2");
        let mut buf = [0u8; 512];
        let len = format_line(&mut buf, "INFO", format_args!("hello {}", 42));
        let line = std::str::from_utf8(&buf[..len]).unwrap();
        assert!(line.ends_with(" W2 INFO hello 42\n"), "got: {line:?}");
        let (secs, rest) = line.split_once('.').unwrap();
        assert!(secs.parse::<u64>().is_ok(), "secs field: {secs:?}");
        assert_eq!(rest.split(' ').next().unwrap().len(), 3, "millis must be 3 digits");
        init(QUIET, b"");
    }

    #[test]
    fn test_format_line_truncates_with_trailing_newline() {
        // An oversized message fills the buffer exactly; the final byte is
        // still the terminating '\n' inside the single write.
        init(QUIET, b"M");
        let long = "x".repeat(4096);
        let mut buf = [0u8; 512];
        let len = format_line(&mut buf, "ERROR", format_args!("{long}"));
        assert_eq!(len, 512, "truncated line must fill the buffer");
        assert_eq!(buf[511], b'\n', "trailing newline must be the final byte");
        assert!(std::str::from_utf8(&buf[..len]).unwrap().contains("ERROR xxx"));
        init(QUIET, b"");
    }
}
