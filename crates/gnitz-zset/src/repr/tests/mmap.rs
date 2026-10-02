use super::*;

#[test]
fn mmap_maps_the_whole_file_and_refuses_an_empty_one() {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    let err = Mmap::from_file(tmp.as_file()).err().unwrap();
    assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
    std::io::Write::write_all(&mut tmp, b"abc").unwrap();
    assert_eq!(Mmap::from_file(tmp.as_file()).unwrap().as_slice(), b"abc");
}
