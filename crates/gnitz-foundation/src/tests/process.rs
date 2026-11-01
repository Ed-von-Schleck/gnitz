use super::*;
use std::fs;
use std::process::Stdio;

fn spawn_logging(script: &str, log: &Path) -> Child {
    Command::new("sh")
        .args(["-c", script])
        .stdout(Stdio::null())
        .stderr(File::create(log).unwrap())
        .spawn()
        .unwrap()
}

#[test]
fn a_line_logged_before_the_exit_is_found() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("log");
    let mut child = spawn_logging("echo ready >&2", &log);
    child.wait().unwrap();
    let found = await_log(&mut child, &log, 0, Duration::from_secs(10), 10, |l| {
        l.contains("ready").then_some(())
    });
    assert!(found.is_ok(), "{found:?}");
}

#[test]
fn an_exit_without_the_line_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("log");
    let mut child = spawn_logging("echo booting >&2; exit 3", &log);
    let found = await_log(&mut child, &log, 0, Duration::from_secs(10), 10, |l| {
        l.contains("ready").then_some(())
    });
    let why = found.expect_err("the child exited");
    assert!(why.starts_with("exited (") && why.ends_with("booting"), "{why}");
}

#[test]
fn log_tail_keeps_the_last_lines() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("log");
    fs::write(&p, "a\nb\nc\n").unwrap();
    assert_eq!(log_tail(&p, 2), "b\nc");
    assert_eq!(log_tail(&dir.path().join("missing"), 2), "");
}
