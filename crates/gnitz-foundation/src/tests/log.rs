use super::*;
use std::time::Duration;

/// The one test that touches the process-global level and tag.
#[test]
fn init_sets_the_level_and_the_tag() {
    for (level, info, debug) in [
        (Level::Quiet, false, false),
        (Level::Normal, true, false),
        (Level::Debug, true, true),
    ] {
        init(level, Tag::Master);
        assert_eq!(
            (enabled(Level::Normal), enabled(Level::Debug)),
            (info, debug),
            "{level:?}"
        );
    }
    assert_eq!(tag(), Some(Tag::Master));
    set_tag(Tag::Worker(63));
    assert_eq!(tag(), Some(Tag::Worker(63)));
    assert!(enabled(Level::Debug), "re-tagging leaves the level alone");
}

/// Each macro is one expression, so a match arm takes it without a block.
#[test]
fn the_macros_are_expressions() {
    let _never_called = |n: u32| match n {
        0 => gnitz_error!("{n}"),
        1 => gnitz_warn!("{n}"),
        2 => gnitz_note!("{n}"),
        3 => gnitz_info!("{n}"),
        _ => gnitz_debug!("{n}"),
    };
}

fn line(now: Duration, tag: Option<Tag>, level_tag: &str, args: core::fmt::Arguments<'_>) -> String {
    let mut buf = [0u8; LINE_MAX];
    let len = format_line(&mut buf, now, tag, level_tag, args);
    String::from_utf8(buf[..len].to_vec()).unwrap()
}

#[test]
fn format_line_shape() {
    let now = Duration::from_millis(5_007);
    assert_eq!(
        line(now, Some(Tag::Master), "INFO", format_args!("hi {}", 42)),
        "5.007 M INFO hi 42\n"
    );
    assert_eq!(
        line(now, Some(Tag::Worker(63)), "WARN", format_args!("x")),
        "5.007 W63 WARN x\n"
    );
    assert_eq!(line(now, None, "NOTE", format_args!("x")), "5.007 NOTE x\n");
}

/// An oversized message fills the buffer exactly; the final byte is still the
/// terminating '\n' inside the single write.
#[test]
fn format_line_truncates_with_trailing_newline() {
    let long = "x".repeat(4096);
    let got = line(Duration::ZERO, Some(Tag::Master), "ERROR", format_args!("{long}"));
    assert_eq!(got.len(), LINE_MAX);
    assert!(got.starts_with("0.000 M ERROR xxx"));
    assert!(got.ends_with("x\n"));
}
