use super::*;

/// A cgroup tree `root/a/b` with the given `memory.max` at each level, `None`
/// writing no file.
fn tree(levels: [Option<&str>; 3]) -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    let dirs = [root.path().to_owned(), root.path().join("a"), root.path().join("a/b")];
    std::fs::create_dir_all(&dirs[2]).unwrap();
    for (dir, max) in dirs.iter().zip(levels) {
        if let Some(max) = max {
            std::fs::write(dir.join("memory.max"), format!("{max}\n")).unwrap();
        }
    }
    root
}

#[test]
fn the_tightest_limit_on_the_path_wins() {
    for (levels, want) in [
        ([None, Some("4096"), Some("8192")], Some(4096)),
        ([None, Some("8192"), Some("4096")], Some(4096)),
        ([Some("1024"), Some("max"), Some("4096")], Some(1024)),
        ([None, Some("max"), Some("max")], None),
        ([None, None, None], None),
    ] {
        let root = tree(levels);
        assert_eq!(
            tightest_on_path("0::/a/b\n", root.path(), "memory.max", memory_max),
            want,
            "{levels:?}"
        );
    }
}

#[test]
fn only_the_process_cgroup_path_counts() {
    let root = tree([None, None, Some("4096")]);
    let tightest = |proc_cgroup| tightest_on_path(proc_cgroup, root.path(), "memory.max", memory_max);
    assert_eq!(tightest("0::/a\n"), None, "a limit below the process's cgroup");
    assert_eq!(tightest("0::/\n"), None, "the root cgroup");
    assert_eq!(tightest("4:memory:/a/b\n1:cpu:/a/b\n"), None, "a v1 host");
}

#[test]
fn a_cpu_quota_reads_in_whole_cpus() {
    for (cpu_max_file, want) in [
        ("150000 100000", Some(1)),
        ("50000 10000", Some(5)),
        ("50000 100000", Some(0)),
        ("max 100000", None),
        ("100000", None),
        ("100000 0", None),
    ] {
        assert_eq!(cpu_max(cpu_max_file), want, "{cpu_max_file:?}");
    }
}

#[test]
fn a_refused_setup_names_its_cause() {
    for (errno, disabled, names) in [
        (libc::EPERM, Some("2"), "io_uring_disabled=2"),
        (libc::EACCES, Some("1"), "io_uring_group"),
        (libc::ENOSYS, Some("0"), "seccomp profile that also allows"),
        (libc::ENOSYS, None, "no io_uring"),
        (libc::ENOSYS, None, "an emulator"),
        (libc::EPERM, Some("0"), "Docker's default profile"),
        (libc::EPERM, None, "seccomp profile that also allows"),
    ] {
        let why = refusal(Some(errno), disabled).unwrap();
        assert!(why.contains(names), "{errno} under {disabled:?}: {why}");
    }
    assert_eq!(refusal(Some(libc::ENOMEM), Some("0")), None, "no refusal");
}
