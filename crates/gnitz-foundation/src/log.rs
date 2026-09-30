//! Process-wide logging.
//!
//! Log format: `secs.millis tag LEVEL msg` on stderr (fd 2); an untagged
//! process omits the tag.

use std::sync::atomic::{AtomicU32, Ordering};

/// Line buffer size. A line is assembled on the stack and written whole, so an
/// over-long message is truncated rather than split across two `write(2)`s.
const LINE_MAX: usize = 512;

/// Which macros emit: ERROR, WARN and NOTE always do.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Level {
    /// ERROR/WARN/NOTE only.
    Quiet,
    /// + INFO.
    Normal,
    /// + DEBUG.
    Debug,
}

/// The process a line came from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tag {
    Master,
    Worker(u32),
}

static LEVEL: AtomicU32 = AtomicU32::new(Level::Quiet as u32);

/// `0` untagged, `1` the master, `w + 2` worker `w`.
static TAG: AtomicU32 = AtomicU32::new(0);

/// Set the log level and process tag.
pub fn init(level: Level, tag: Tag) {
    LEVEL.store(level as u32, Ordering::Relaxed);
    set_tag(tag);
}

/// Re-tag this process, leaving the level alone — what a forked child that
/// inherits the level needs.
pub fn set_tag(tag: Tag) {
    let packed = match tag {
        Tag::Master => 1,
        Tag::Worker(w) => w + 2,
    };
    TAG.store(packed, Ordering::Relaxed);
}

fn tag() -> Option<Tag> {
    match TAG.load(Ordering::Relaxed) {
        0 => None,
        1 => Some(Tag::Master),
        n => Some(Tag::Worker(n - 2)),
    }
}

#[inline(always)]
pub fn is_debug() -> bool {
    LEVEL.load(Ordering::Relaxed) >= Level::Debug as u32
}

#[inline(always)]
pub fn is_info() -> bool {
    LEVEL.load(Ordering::Relaxed) >= Level::Normal as u32
}

/// Format and write a log line to stderr. Called by macros, not directly.
///
/// Formats straight into a fixed stack buffer — no heap allocation, so a
/// fail-stop path can log with a broken SAL mmap — and has no panic paths. An
/// over-long message is truncated; the trailing `\n` is always the final byte,
/// so even a truncated line terminates inside the one `write(2)`.
#[cold]
pub fn _emit(level_tag: &str, args: core::fmt::Arguments<'_>) {
    let mut buf = [0u8; LINE_MAX];
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let len = format_line(&mut buf, now, tag(), level_tag, args);
    unsafe {
        libc::write(2, buf.as_ptr() as *const libc::c_void, len);
    }
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

/// Assemble `secs.millis tag LEVEL msg\n` into `buf`, `now` being the time since
/// the epoch, truncating an over-long message so the trailing `\n` is always the
/// final byte. Returns the line length in bytes.
fn format_line(
    buf: &mut [u8; LINE_MAX],
    now: std::time::Duration,
    tag: Option<Tag>,
    level_tag: &str,
    args: core::fmt::Arguments<'_>,
) -> usize {
    use core::fmt::Write;

    // The writer gets all but the final byte, which is reserved for '\n'.
    let mut w = TruncatingWriter { buf: &mut buf[..LINE_MAX - 1], pos: 0 };
    let _ = write!(w, "{}.{:03} ", now.as_secs(), now.subsec_millis());
    let _ = match tag {
        None => Ok(()),
        Some(Tag::Master) => w.write_str("M "),
        Some(Tag::Worker(n)) => write!(w, "W{n} "),
    };
    let _ = write!(w, "{level_tag} ");
    let _ = w.write_fmt(args);
    let pos = w.pos;
    buf[pos] = b'\n';
    pos + 1
}

/// Log at ERROR level (always emits).
#[macro_export]
macro_rules! gnitz_error {
    ($($arg:tt)*) => {
        $crate::log::_emit("ERROR", format_args!($($arg)*));
    };
}

/// Log at WARN level (always emits).
#[macro_export]
macro_rules! gnitz_warn {
    ($($arg:tt)*) => {
        $crate::log::_emit("WARN", format_args!($($arg)*));
    };
}

/// Log at NOTE level (always emits). For a line that must appear whatever the
/// level and is not a fault — boot progress, chiefly, where `gnitz_info!` is
/// silent at the default QUIET.
#[macro_export]
macro_rules! gnitz_note {
    ($($arg:tt)*) => {
        $crate::log::_emit("NOTE", format_args!($($arg)*));
    };
}

/// Log at INFO level (emits when level >= NORMAL).
#[macro_export]
macro_rules! gnitz_info {
    ($($arg:tt)*) => {
        if $crate::log::is_info() {
            $crate::log::_emit("INFO", format_args!($($arg)*));
        }
    };
}

/// Log at DEBUG level (emits when level >= DEBUG).
#[macro_export]
macro_rules! gnitz_debug {
    ($($arg:tt)*) => {
        if $crate::log::is_debug() {
            $crate::log::_emit("DEBUG", format_args!($($arg)*));
        }
    };
}

#[cfg(test)]
#[path = "tests/log.rs"]
mod tests;
