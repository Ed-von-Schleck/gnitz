use super::*;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
#[test]
fn test_write_all_fd_roundtrip() {
    // Guards the happy path of the partial-write loop (and that the
    // ret==0 guard does not break a normal full write).
    let mut f = tempfile::tempfile().unwrap();
    let data = b"hello write_all_fd partial-write loop";
    write_all_fd(f.as_raw_fd(), data).unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap();
    assert_eq!(&buf, data);
}

#[test]
fn test_open_tmpfile_is_anonymous_and_round_trips() {
    // O_TMPFILE yields an inode with ZERO directory links: nothing to leak,
    // reclaimed on close. The data written round-trips (fd is O_RDWR).
    let dir = tempfile::tempdir().expect("tempdir");
    let dir_path = dir.path().to_str().expect("utf8 dir");
    let owned = open_tmpfile(dir_path).expect("open_tmpfile");
    let fd = owned.as_raw_fd();

    let data = b"external-sort spill run bytes";
    write_all_fd(fd, data).expect("write_all_fd");

    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::fstat(fd, &mut st) }, 0, "fstat");
    assert_eq!(st.st_nlink, 0, "O_TMPFILE inode must have no directory entry");
    assert_eq!(st.st_size as usize, data.len(), "written size");

    let mapped = Mmap::from_fd(fd, data.len(), Advice::Sequential).expect("map the anonymous file back");
    assert_eq!(mapped.as_slice(), data, "round-trip through the anonymous file");
}

/// The file reaches `size` before the mapping exists, so the store lands on
/// a real page instead of raising SIGBUS.
#[test]
fn test_map_file_reserved() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let fd = tmp.as_file().as_raw_fd();
    let ptr = map_file_reserved(fd, 8192).unwrap();
    assert_eq!(fd_size(fd).unwrap(), 8192);
    unsafe {
        *ptr = 42;
        assert_eq!(*ptr, 42);
        libc::munmap(ptr as *mut libc::c_void, 8192);
    }
}
