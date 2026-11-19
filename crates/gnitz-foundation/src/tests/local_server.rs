use super::*;

/// An executable at `dir/name` running `body` under `sh`.
///
/// A child process writes it: a write descriptor held here would be inherited by
/// a child another test thread forks meanwhile, and the kernel refuses to exec a
/// file still open for writing (ETXTBSY).
fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    let src = dir.join(format!("{name}.src"));
    fs::write(&src, format!("#!/bin/sh\n{body}\n")).unwrap();
    let status = std::process::Command::new("install")
        .arg("-m755")
        .arg(&src)
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success(), "install {}", path.display());
    path
}

/// A stand-in server: it writes its arguments and the SAL size it is given to
/// `args`, logs the ready line, and runs until it is signalled.
fn standin(dir: &Path) -> PathBuf {
    let args = dir.join("args");
    script(
        dir,
        "standin",
        &format!(
            "echo \"$* $GNITZ_SAL_BYTES\" > {}\necho \"0.000 M NOTE {READY_LINE}\" >&2\nexec sleep 600",
            args.display()
        ),
    )
}

#[test]
fn a_throwaway_server_runs_in_a_scratch_directory_it_takes_with_it() {
    let dir = ScratchDir::new("gnitz-test-").unwrap();
    let mut server = LocalServer::start(&standin(dir.path()), None, 2).unwrap();
    let scratch = server.socket().parent().unwrap().to_path_buf();
    assert_eq!(server.data_dir(), scratch.join("data"));
    assert!(server.running());
    let args = fs::read_to_string(dir.path().join("args")).unwrap();
    let want = format!(
        "--workers=2 {} {} {}\n",
        scratch.join("data").display(),
        server.socket().display(),
        env::var("GNITZ_SAL_BYTES").as_deref().unwrap_or(THROWAWAY_SAL_BYTES)
    );
    assert_eq!(args, want);
    server.stop().unwrap();
    assert!(!server.running());
    assert!(!scratch.exists(), "{} outlives its server", scratch.display());
}

#[test]
fn a_kept_server_keeps_its_data_directory() {
    let dir = ScratchDir::new("gnitz-test-").unwrap();
    let data = dir.path().join("db");
    let server = LocalServer::start(&standin(dir.path()), Some(&data), 1).unwrap();
    assert_eq!(server.data_dir(), data);
    let args = fs::read_to_string(dir.path().join("args")).unwrap();
    assert!(args.starts_with(&format!("--workers=1 {} ", data.display())), "{args}");
    drop(server);
    assert!(dir.path().exists());
}

#[test]
fn a_server_that_exits_before_it_serves_is_reported_with_its_log() {
    let dir = ScratchDir::new("gnitz-test-").unwrap();
    let bin = script(dir.path(), "failing", "echo 'the data directory is locked' >&2\nexit 3");
    let err = LocalServer::start(&bin, None, 1).err().unwrap();
    assert!(err.contains("exited"), "{err}");
    assert!(err.contains("the data directory is locked"), "{err}");
}

#[test]
fn a_server_that_exits_while_it_serves_is_reported_when_stopped() {
    let dir = ScratchDir::new("gnitz-test-").unwrap();
    let bin = script(
        dir.path(),
        "crashing",
        &format!("echo \"{READY_LINE}\" >&2\necho 'worker 0 crashed' >&2"),
    );
    let mut server = LocalServer::start(&bin, None, 1).unwrap();
    while server.running() {
        std::thread::sleep(POLL_INTERVAL);
    }
    let err = server.stop().unwrap_err();
    assert!(err.contains("worker 0 crashed"), "{err}");
    assert_eq!(server.stop(), Ok(()));
}

#[test]
fn only_an_executable_file_is_a_server() {
    let dir = ScratchDir::new("gnitz-test-").unwrap();
    let plain = dir.path().join("plain");
    fs::write(&plain, "").unwrap();
    assert!(!is_executable(&plain));
    assert!(!is_executable(dir.path()));
    assert!(!is_executable(&dir.path().join("missing")));
    assert!(is_executable(&script(dir.path(), "exe", "true")));
}
