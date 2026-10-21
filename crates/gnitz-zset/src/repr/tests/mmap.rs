use super::*;

#[test]
fn mmap_maps_the_whole_file() {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    std::io::Write::write_all(&mut tmp, b"abc").unwrap();
    assert_eq!(Mmap::from_file(tmp.as_file(), 3).unwrap().as_slice(), b"abc");
}
