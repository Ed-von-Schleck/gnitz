use super::*;

#[test]
fn a_backup_name_is_one_plain_visible_path_component() {
    for name in ["nightly", "2026-10-16_01.full", "a", &"x".repeat(MAX_NAME_LEN)] {
        assert_eq!(entry_name(name.as_bytes()).unwrap(), name);
    }
    for name in [
        "",
        ".",
        "..",
        ".hidden",
        "a/b",
        "../up",
        "sp ace",
        "ü",
        &"x".repeat(MAX_NAME_LEN + 1),
    ] {
        assert!(entry_name(name.as_bytes()).is_err(), "{name:?}");
    }
    assert!(entry_name(&[0xff]).is_err());
}

#[test]
fn rename_noreplace_moves_a_directory_and_refuses_to_replace_one() {
    let root = tempfile::tempdir().unwrap();
    let path = |name: &str| root.path().join(name).to_str().unwrap().to_string();
    std::fs::create_dir(path("from")).unwrap();
    std::fs::create_dir(path("taken")).unwrap();
    let refused = rename_noreplace(&path("from"), &path("taken")).unwrap_err();
    assert_eq!(refused.kind(), std::io::ErrorKind::AlreadyExists);
    rename_noreplace(&path("from"), &path("to")).unwrap();
    assert!(!std::path::Path::new(&path("from")).exists());
    assert!(std::path::Path::new(&path("to")).is_dir());
}
