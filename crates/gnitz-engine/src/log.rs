//! Logging module for gnitz-engine.
//!
//! Log format: `secs.millis tag LEVEL msg` on stderr (fd 2).
//! Log level is set once at startup via FFI. Levels:
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

/// Set the log level and process tag. Called from FFI or tests.
pub fn init(level: u32, tag: &[u8]) {
    LEVEL.store(level.min(DEBUG), Ordering::Relaxed);
    let len = tag.len().min(3);
    let mut bytes = [0u8; 4];
    for i in 0..len {
        bytes[i] = tag[i];
    }
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
#[cold]
pub fn _emit(level_tag: &str, msg: &str) {
    let mut tv = libc::timeval {
        tv_sec: 0,
        tv_usec: 0,
    };
    unsafe {
        libc::gettimeofday(&mut tv, core::ptr::null_mut());
    }
    let secs = tv.tv_sec as u64;
    let millis = (tv.tv_usec / 1000) as u32;

    let packed = TAG.load(Ordering::Relaxed);
    let tag_bytes = u32::to_ne_bytes(packed);
    let tag_len = tag_bytes[3] as usize;

    let mut buf = [0u8; 512];
    let mut pos = 0;

    pos += write_u64(&mut buf[pos..], secs);
    buf[pos] = b'.';
    pos += 1;
    pos += write_millis(&mut buf[pos..], millis);
    buf[pos] = b' ';
    pos += 1;

    let n = tag_len.min(buf.len() - pos - 1);
    buf[pos..pos + n].copy_from_slice(&tag_bytes[..n]);
    pos += n;
    buf[pos] = b' ';
    pos += 1;

    let lb = level_tag.as_bytes();
    let n = lb.len().min(buf.len() - pos - 1);
    buf[pos..pos + n].copy_from_slice(&lb[..n]);
    pos += n;
    buf[pos] = b' ';
    pos += 1;

    let mb = msg.as_bytes();
    let n = mb.len().min(buf.len() - pos - 1);
    buf[pos..pos + n].copy_from_slice(&mb[..n]);
    pos += n;
    buf[pos] = b'\n';
    pos += 1;

    unsafe {
        libc::write(2, buf.as_ptr() as *const libc::c_void, pos);
    }
}

fn write_u64(buf: &mut [u8], val: u64) -> usize {
    if val == 0 {
        buf[0] = b'0';
        return 1;
    }
    let mut tmp = [0u8; 20];
    let mut n = val;
    let mut i = 0;
    while n > 0 {
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        i += 1;
    }
    for j in 0..i {
        buf[j] = tmp[i - 1 - j];
    }
    i
}

fn write_millis(buf: &mut [u8], val: u32) -> usize {
    buf[0] = b'0' + ((val / 100) % 10) as u8;
    buf[1] = b'0' + ((val / 10) % 10) as u8;
    buf[2] = b'0' + (val % 10) as u8;
    3
}

/// Log at ERROR level (always emits).
#[macro_export]
macro_rules! gnitz_error {
    ($($arg:tt)*) => {
        $crate::log::_emit("ERROR", &format!($($arg)*));
    };
}

/// Log at WARN level (always emits).
#[macro_export]
macro_rules! gnitz_warn {
    ($($arg:tt)*) => {
        $crate::log::_emit("WARN", &format!($($arg)*));
    };
}

/// Log at INFO level (emits when level >= NORMAL).
#[macro_export]
macro_rules! gnitz_info {
    ($($arg:tt)*) => {
        if $crate::log::is_info() {
            $crate::log::_emit("INFO", &format!($($arg)*));
        }
    };
}

/// Log at DEBUG level (emits when level >= DEBUG).
#[macro_export]
macro_rules! gnitz_debug {
    ($($arg:tt)*) => {
        if $crate::log::is_debug() {
            $crate::log::_emit("DEBUG", &format!($($arg)*));
        }
    };
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
    fn test_write_u64_formatting() {
        let mut buf = [0u8; 20];
        let n = write_u64(&mut buf, 1711712345);
        assert_eq!(&buf[..n], b"1711712345");

        let n = write_u64(&mut buf, 0);
        assert_eq!(&buf[..n], b"0");
    }

    #[test]
    fn test_write_millis_formatting() {
        let mut buf = [0u8; 3];
        write_millis(&mut buf, 42);
        assert_eq!(&buf, b"042");

        write_millis(&mut buf, 7);
        assert_eq!(&buf, b"007");

        write_millis(&mut buf, 999);
        assert_eq!(&buf, b"999");
    }
}
