use super::*;
use crate::relation::{lock_data_dir, reset_data_dir};
use std::path::Path;

const OURS: u64 = 0x1111;
const THEIRS: u64 = 0x2222;

fn dir() -> (tempfile::TempDir, String) {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("data").to_str().unwrap().to_string();
    (tmp, base)
}

/// Every file under `base` but the lock, which `lock_data_dir` creates first,
/// with its bytes.
fn snapshot(base: &str) -> Vec<(String, Vec<u8>)> {
    fn walk(p: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for e in fs::read_dir(p).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(&p, out);
            } else if !p.ends_with("LOCK") {
                out.push((p.display().to_string(), fs::read(&p).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(Path::new(base), &mut out);
    out.sort();
    out
}

fn write_relation(base: &str) {
    let rel = format!("{}/1/w0of1", relations_dir(base));
    fs::create_dir_all(&rel).unwrap();
    fs::write(format!("{rel}/manifest.bin"), b"rows").unwrap();
}

fn stamped_by(base: &str, format: u64, release: &str) {
    fs::create_dir_all(base).unwrap();
    fs::write(stamp_path(base), encode(format, release)).unwrap();
}

#[test]
fn a_new_directory_is_stamped_before_its_relations() {
    let (_tmp, base) = dir();
    let (_lock, found) = lock_data_dir(&base, OURS).unwrap();
    assert_eq!(found, None);
    let stamp = read_stamp(&base).unwrap().expect("stamped");
    assert_eq!((stamp.format, stamp.release.as_str()), (OURS, RELEASE));
    assert!(Path::new(&relations_dir(&base)).is_dir());
}

#[test]
fn a_directory_of_this_format_reopens_and_takes_this_release() {
    let (_tmp, base) = dir();
    stamped_by(&base, OURS, "0.0.1");
    write_relation(&base);
    let (_lock, found) = lock_data_dir(&base, OURS).unwrap();
    assert_eq!(found, None);
    assert_eq!(read_stamp(&base).unwrap().unwrap().release, RELEASE);
}

#[test]
fn a_directory_of_another_format_is_left_as_it_was() {
    let (_tmp, base) = dir();
    stamped_by(&base, THEIRS, "0.0.1");
    write_relation(&base);
    let before = snapshot(&base);
    let (_lock, found) = lock_data_dir(&base, OURS).unwrap();
    assert_eq!(found, Some(Foreign::Stamped { release: "0.0.1".into() }));
    assert_eq!(snapshot(&base), before);
}

#[test]
fn relations_without_a_stamp_are_another_format() {
    let (_tmp, base) = dir();
    write_relation(&base);
    let before = snapshot(&base);
    let (_lock, found) = lock_data_dir(&base, OURS).unwrap();
    assert_eq!(found, Some(Foreign::Unstamped));
    assert!(read_stamp(&base).unwrap().is_none());
    assert_eq!(snapshot(&base), before);
}

/// A boot killed after creating an empty relations directory, before anything
/// went into it, left a directory that opens as new.
#[test]
fn an_empty_unstamped_directory_is_new() {
    let (_tmp, base) = dir();
    fs::create_dir_all(relations_dir(&base)).unwrap();
    let (_lock, found) = lock_data_dir(&base, OURS).unwrap();
    assert_eq!(found, None);
    assert_eq!(read_stamp(&base).unwrap().unwrap().format, OURS);
}

#[test]
fn a_damaged_stamp_refuses_the_open() {
    let (_tmp, base) = dir();
    fs::create_dir_all(&base).unwrap();
    fs::write(stamp_path(&base), "not a stamp\n").unwrap();
    let err = lock_data_dir(&base, OURS).err().expect("refused");
    assert!(err.contains("is damaged"), "{err}");
}

/// A stamp from a later build may carry lines this one does not know.
#[test]
fn a_stamp_line_this_build_does_not_know_is_skipped() {
    let text = format!("{STAMP_HEADER}\nwritten-by-later 7\nformat {OURS:016x}\nrelease 9.9.9\n");
    let stamp = decode(&text).expect("decodes");
    assert_eq!((stamp.format, stamp.release.as_str()), (OURS, "9.9.9"));
}

#[test]
fn reset_erases_the_relations_and_stamps_this_format() {
    let (_tmp, base) = dir();
    stamped_by(&base, THEIRS, "0.0.1");
    write_relation(&base);
    let (lock, _) = lock_data_dir(&base, OURS).unwrap();
    reset_data_dir(&lock, &base, OURS).unwrap();
    assert_eq!(read_stamp(&base).unwrap().unwrap().format, OURS);
    assert!(!holds_relations(&base).unwrap());
    assert!(Path::new(&relations_dir(&base)).is_dir());
}
