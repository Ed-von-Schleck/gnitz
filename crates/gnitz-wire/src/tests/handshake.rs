use super::*;

#[test]
fn a_hello_is_checked_at_its_prefix_and_classified() {
    assert_eq!(&HELLO[..4], b"GNTZ");
    assert_eq!(read_client_hello(&HELLO), Ok(&[][..]));

    let mut forged = client_hello(b"t");
    forged[0] ^= 1;
    assert_eq!(read_client_hello(&forged), Err(HelloError::Malformed));
    assert_eq!(read_client_hello(&HELLO[..7]), Err(HelloError::Malformed));
}

#[test]
fn a_client_hello_round_trips_its_token_and_refuses_a_longer_one() {
    for token in [&b""[..], b"gnitz_00", &[7u8; MAX_TOKEN_BYTES]] {
        let hello = client_hello(token);
        assert!(hello.len() <= HANDSHAKE_FRAME_MAX);
        assert_eq!(read_client_hello(&hello), Ok(token));
    }
    let long = [&HELLO[..], &[0u8; MAX_TOKEN_BYTES + 1]].concat();
    assert_eq!(read_client_hello(&long), Err(HelloError::Malformed));
}

#[test]
fn a_server_hello_carries_each_verdict() {
    for &status in AuthStatus::ALL {
        assert_eq!(read_server_hello(&server_hello(status)), Ok(status));
    }
    let mut unknown = server_hello(AuthStatus::Accepted);
    unknown[HELLO.len()] = 3;
    assert_eq!(read_server_hello(&unknown), Err(HelloError::Malformed));
    assert_eq!(read_server_hello(&HELLO), Err(HelloError::Malformed));
    let long = [&server_hello(AuthStatus::Accepted)[..], b"x"].concat();
    assert_eq!(read_server_hello(&long), Err(HelloError::Malformed));
}

#[test]
fn a_refusing_hello_names_its_release_to_a_client_of_another_version() {
    let refusal = refusal_hello();
    assert_eq!(&refusal[..HELLO.len()], &HELLO);
    assert_eq!(&refusal[HELLO.len()..], RELEASE.as_bytes());
    // From this build's own version the refusal carries no verdict.
    assert_eq!(read_server_hello(&refusal), Err(HelloError::Malformed));

    // The version is the little-endian word at offset 4, read before what follows it.
    let mut other = refusal.clone();
    other[4] ^= 1;
    let peer = WIRE_VERSION ^ 1;
    let want = HelloError::Version { peer, release: Some(RELEASE.to_string()) };
    assert_eq!(read_server_hello(&other), Err(want));

    // What no release name is — a verdict byte from another version among
    // them — reads as a server naming none.
    for rest in [&b""[..], b"\x01", b"0.1 8", &[b'9'; MAX_RELEASE_BYTES + 1], b"\xff\xfe"] {
        let hello = [&other[..HELLO.len()], rest].concat();
        assert_eq!(
            read_server_hello(&hello),
            Err(HelloError::Version { peer, release: None }),
            "{rest:?}"
        );
    }
    // A client's HELLO carries its token where a refusal carries a release.
    let mut client = client_hello(b"gnitz_0.1.8");
    client[4] ^= 1;
    assert_eq!(
        read_client_hello(&client),
        Err(HelloError::Version { peer, release: None })
    );
}
