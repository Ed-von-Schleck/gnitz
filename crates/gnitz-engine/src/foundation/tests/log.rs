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
