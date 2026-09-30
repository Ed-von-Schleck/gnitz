use super::*;

#[test]
fn check_hello_accepts_its_own_and_classifies_the_rest() {
    assert_eq!(&HELLO[..4], b"GNTZ");
    assert_eq!(check_hello(&HELLO), Ok(()));

    let mut forged = HELLO;
    forged[0] ^= 1;
    assert_eq!(check_hello(&forged), Err(HelloError::Malformed));
    assert_eq!(check_hello(&HELLO[..7]), Err(HelloError::Malformed));
    let mut long = HELLO.to_vec();
    long.push(0);
    assert_eq!(check_hello(&long), Err(HelloError::Malformed));

    // The version is the little-endian word at offset 4.
    let mut other = HELLO;
    other[4] ^= 1;
    assert_eq!(
        check_hello(&other),
        Err(HelloError::Version { peer: WAL_FORMAT_VERSION ^ 1 })
    );
}
