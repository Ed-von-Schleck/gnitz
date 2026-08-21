//! Logging module for gnitz-engine.
//!
//! Log format: `secs.millis tag LEVEL msg` on stderr (fd 2).
//! Log levels:
//!   0 = QUIET  (error/warn only)
//!   1 = NORMAL (+ info)
//!   2 = DEBUG  (+ debug)

use std::sync::atomic::{AtomicU32, Ordering};

/// Line buffer size. A line is assembled on the stack and written whole, so an
/// over-long message is truncated rather than split across two `write(2)`s.
const LINE_MAX: usize = 512;

static LEVEL: AtomicU32 = AtomicU32::new(0);

/// Process tag packed as [b0, b1, b2, len] in native byte order.
static TAG: AtomicU32 = AtomicU32::new(0);

pub const QUIET: u32 = 0;
pub const NORMAL: u32 = 1;
pub const DEBUG: u32 = 2;

/// Set the log level and process tag. A tag is at most three bytes: `MAX_WORKERS`
/// is 64 and `main.rs` validates the count into `1..=64`, so the longest one a
/// worker forms is `"W63"` — exactly what fits. The clamp below is what keeps a
/// release build from panicking rather than a supported way to pass more.
pub fn init(level: u32, tag: &[u8]) {
    debug_assert!(tag.len() <= 3);
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
    let mut buf = [0u8; LINE_MAX];
    let len = format_line(&mut buf, level_tag, args);
    unsafe {
        libc::write(2, buf.as_ptr() as *const libc::c_void, len);
    }
}

/// Terminate the process with exit code 134 (= 128 + SIGABRT) without running
/// atexit handlers, TLS destructors or a stdio flush. Called by
/// [`gnitz_fatal_abort!`]; it lives here so the macro expands to no unqualified
/// `libc` path and a consumer needs no `libc` of its own.
///
/// Diverges, like the `_exit` it wraps: `gnitz_fatal_abort!` is used in
/// value positions (a match arm whose siblings yield values), which a `()`
/// return would break.
pub fn _abort_134() -> ! {
    unsafe { libc::_exit(134) }
}

/// Truncating `fmt::Write` over a fixed buffer: keeps what fits, silently
/// discards the rest, and never returns `Err` (a propagated `fmt::Error` would
/// panic inside `write!`).
struct TruncatingWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl core::fmt::Write for TruncatingWriter<'_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let n = s.len().min(self.buf.len() - self.pos);
        self.buf[self.pos..self.pos + n].copy_from_slice(&s.as_bytes()[..n]);
        self.pos += n;
        Ok(())
    }
}

/// Assemble `secs.millis tag LEVEL msg\n` into `buf`, truncating an over-long
/// message so the trailing `\n` is always the final byte. Returns the line
/// length in bytes.
fn format_line(buf: &mut [u8; LINE_MAX], level_tag: &str, args: core::fmt::Arguments<'_>) -> usize {
    use core::fmt::Write;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();

    let packed = TAG.load(Ordering::Relaxed);
    let tag_bytes = u32::to_ne_bytes(packed);
    let tag_len = (tag_bytes[3] as usize).min(3);
    let tag = std::str::from_utf8(&tag_bytes[..tag_len]).unwrap_or("");

    // The writer gets all but the final byte, which is reserved for '\n'.
    let mut w = TruncatingWriter {
        buf: &mut buf[..LINE_MAX - 1],
        pos: 0,
    };
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

/// Emit a FATAL log line and immediately terminate **the calling process** with
/// exit code 134 (= 128 + SIGABRT), through [`_abort_134`].
///
/// The termination is unconditional and uninterceptable: `_exit` runs no atexit
/// handler, no TLS destructor, no `Drop` and no stdio flush, `catch_unwind` does
/// not see it, and no wrapper can intercept it. None of those are safe to run
/// with a broken SAL mmap or in-flight io_uring SQEs, which is why the engine
/// uses it — but a caller that cannot afford to lose its process must not reach
/// any path that can invoke this. The exit code matches the "aborted"
/// convention systemd/monit expect.
#[macro_export]
macro_rules! gnitz_fatal_abort {
    ($($arg:tt)*) => {{
        $crate::foundation::log::_emit("FATAL", format_args!($($arg)*));
        $crate::foundation::log::_abort_134()
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The logger state (`LEVEL`/`TAG`) is process-global; these tests each
    /// `init` it, so they must not interleave across the parallel test
    /// threads.
    static LOG_STATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_init_and_level_checks() {
        let _g = LOG_STATE.lock().unwrap();
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
    fn test_format_line_shape() {
        // `secs.millis tag LEVEL msg\n` — millis zero-padded to 3 digits. Both
        // real tag widths: the master's `"M"` and a worker's longest, `"W63"`.
        let _g = LOG_STATE.lock().unwrap();
        for tag in ["M", "W63"] {
            init(NORMAL, tag.as_bytes());
            let mut buf = [0u8; LINE_MAX];
            let len = format_line(&mut buf, "INFO", format_args!("hello {}", 42));
            let line = std::str::from_utf8(&buf[..len]).unwrap();
            assert!(line.ends_with(&format!(" {tag} INFO hello 42\n")), "got: {line:?}");
            let (secs, rest) = line.split_once('.').unwrap();
            assert!(secs.parse::<u64>().is_ok(), "secs field: {secs:?}");
            assert_eq!(rest.split(' ').next().unwrap().len(), 3, "millis must be 3 digits");
        }
        init(QUIET, b"");
    }

    #[test]
    fn test_format_line_truncates_with_trailing_newline() {
        // An oversized message fills the buffer exactly; the final byte is
        // still the terminating '\n' inside the single write.
        let _g = LOG_STATE.lock().unwrap();
        init(QUIET, b"M");
        let long = "x".repeat(4096);
        let mut buf = [0u8; LINE_MAX];
        let len = format_line(&mut buf, "ERROR", format_args!("{long}"));
        assert_eq!(len, LINE_MAX, "truncated line must fill the buffer");
        assert_eq!(buf[LINE_MAX - 1], b'\n', "trailing newline must be the final byte");
        assert!(std::str::from_utf8(&buf[..len]).unwrap().contains("ERROR xxx"));
        init(QUIET, b"");
    }
}
