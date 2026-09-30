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
        assert_eq!(tightest_memory_max("0::/a/b\n", root.path()), want, "{levels:?}");
    }
}

#[test]
fn only_the_process_cgroup_path_counts() {
    let root = tree([None, None, Some("4096")]);
    assert_eq!(
        tightest_memory_max("0::/a\n", root.path()),
        None,
        "a limit below the process's cgroup"
    );
    assert_eq!(tightest_memory_max("0::/\n", root.path()), None, "the root cgroup");
    assert_eq!(
        tightest_memory_max("4:memory:/a/b\n1:cpu:/a/b\n", root.path()),
        None,
        "a v1 host"
    );
}
