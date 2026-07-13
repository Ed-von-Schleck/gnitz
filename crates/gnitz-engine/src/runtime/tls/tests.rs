//! Unit tests for the TLS plaintext deframing path (`feed_decrypted` over
//! the reused `io::RecvState`) and the RecvBuf-RAII inbound accounting.
//!
//! No sockets: a rustls client+server pair is handshaken by shuttling
//! ciphertext through memory, then client-written plaintext frames are fed
//! into the server session and drained through `feed_decrypted`.

use super::*;

/// Handshake an in-memory client/server pair (dev-cert server config). The
/// client verifies for real against the minted dev certificate's public
/// PEM — no skip-verifier.
fn handshaken_pair() -> (rustls::ClientConnection, rustls::ServerConnection) {
    use rustls::pki_types::pem::PemObject;
    let (server_cfg, dev_pem) = config::server_crypto(None, None).unwrap();
    let pem = dev_pem.expect("dev mint returns the public PEM");
    let mut roots = rustls::RootCertStore::empty();
    let certs = rustls::pki_types::CertificateDer::pem_slice_iter(pem.as_bytes());
    roots.add_parsable_certificates(certs.filter_map(Result::ok));
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut client_cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_cfg.alpn_protocols = vec![gnitz_wire::ALPN_GNITZ.to_vec()];
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut client = rustls::ClientConnection::new(Arc::new(client_cfg), server_name).unwrap();
    let mut server = rustls::ServerConnection::new(server_cfg).unwrap();

    while client.is_handshaking() || server.is_handshaking() {
        let mut c2s = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut c2s).unwrap();
        }
        let mut s: &[u8] = &c2s;
        while !s.is_empty() {
            server.read_tls(&mut s).unwrap();
            server.process_new_packets().unwrap();
        }
        let mut s2c = Vec::new();
        while server.wants_write() {
            server.write_tls(&mut s2c).unwrap();
        }
        let mut c: &[u8] = &s2c;
        while !c.is_empty() {
            client.read_tls(&mut c).unwrap();
            client.process_new_packets().unwrap();
        }
    }
    (client, server)
}

fn test_conn(sess: rustls::ServerConnection, max_payload_len: usize) -> TlsConn {
    TlsConn {
        sess,
        recv_state: io::RecvState::new(),
        max_payload_len,
        inbound: VecDeque::new(),
        recv_waker: None,
        recv_closed: false,
        closed: false,
        pump_exited: false,
    }
}

/// Client-side: buffer `frames` (each as [len:u32 LE][payload]) as
/// plaintext and return the resulting ciphertext. Writes are interleaved
/// with `write_tls` flushes because rustls bounds its plaintext buffer
/// (64 KiB default) — the same discipline the client transport uses.
fn encrypt_frames(client: &mut rustls::ClientConnection, frames: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    let write_plaintext = |client: &mut rustls::ClientConnection, mut data: &[u8], out: &mut Vec<u8>| {
        while !data.is_empty() {
            let n = client.writer().write(data).unwrap();
            data = &data[n..];
            if n == 0 || client.wants_write() {
                while client.wants_write() {
                    client.write_tls(out).unwrap();
                }
            }
        }
    };
    for f in frames {
        let prefix = (f.len() as u32).to_le_bytes();
        write_plaintext(client, &prefix, &mut out);
        write_plaintext(client, f, &mut out);
    }
    while client.wants_write() {
        client.write_tls(&mut out).unwrap();
    }
    out
}

/// Feed `cipher` into the server session (whole-slice) and drain the
/// plaintext through `feed_decrypted`.
fn ingest(conn: &mut TlsConn, reactor: &Reactor, mut cipher: &[u8]) -> Result<(), ()> {
    while !cipher.is_empty() {
        let n = conn.sess.read_tls(&mut cipher).unwrap();
        assert!(n > 0);
        conn.sess.process_new_packets().map_err(|_| ())?;
        conn.feed_decrypted(reactor)?;
    }
    Ok(())
}

fn frame_payloads(conn: &mut TlsConn) -> Vec<Vec<u8>> {
    conn.inbound.drain(..).map(|b| b.as_slice().to_vec()).collect()
}

#[test]
fn pipelined_frames_deframe_in_order() {
    let (mut client, server) = handshaken_pair();
    let reactor = Reactor::new(16).unwrap();
    let mut conn = test_conn(server, 1 << 20);

    let f1 = vec![0xAAu8; 10];
    let f2 = vec![0xBBu8; 100_000]; // spans multiple 16 KiB records
    let f3 = b"tail".to_vec();
    let cipher = encrypt_frames(&mut client, &[&f1, &f2, &f3]);
    ingest(&mut conn, &reactor, &cipher).unwrap();

    assert_eq!(frame_payloads(&mut conn), vec![f1, f2, f3]);
}

#[test]
fn split_ciphertext_delivery_reassembles() {
    let (mut client, server) = handshaken_pair();
    let reactor = Reactor::new(16).unwrap();
    let mut conn = test_conn(server, 1 << 20);

    let payload: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
    let cipher = encrypt_frames(&mut client, &[&payload]);
    // Deliver in awkward chunks (mid-record splits included).
    for chunk in cipher.chunks(1_313) {
        ingest(&mut conn, &reactor, chunk).unwrap();
    }
    assert_eq!(frame_payloads(&mut conn), vec![payload]);
}

#[test]
fn oversize_frame_rejected_at_header() {
    let (mut client, server) = handshaken_pair();
    let reactor = Reactor::new(16).unwrap();
    // Pre-HELLO cap: 8 bytes. A 9-byte frame must be refused at the header.
    let mut conn = test_conn(server, io::HELLO_PRE_HANDSHAKE_LEN);

    let cipher = encrypt_frames(&mut client, &[&[0u8; 9]]);
    assert!(ingest(&mut conn, &reactor, &cipher).is_err());
    assert!(conn.inbound.is_empty());
}

#[test]
fn zero_len_sentinel_rejected() {
    let (mut client, server) = handshaken_pair();
    let reactor = Reactor::new(16).unwrap();
    let mut conn = test_conn(server, 1 << 20);

    // A hand-built zero-length header is the close sentinel.
    client.writer().write_all(&0u32.to_le_bytes()).unwrap();
    let mut cipher = Vec::new();
    while client.wants_write() {
        client.write_tls(&mut cipher).unwrap();
    }
    assert!(ingest(&mut conn, &reactor, &cipher).is_err());
}

#[test]
fn header_time_charge_refuses_undelivered_oversize_frame() {
    // The slow-drip OOM stays closed: a declared 1 MiB frame whose payload
    // never arrives is charged (and here refused) at HeaderDone, before any
    // payload byte.
    let (mut client, server) = handshaken_pair();
    let reactor = Reactor::new(16).unwrap();
    reactor.set_inbound_cap(64 * 1024);
    let mut conn = test_conn(server, 16 << 20);

    // Send ONLY the 4-byte header declaring 1 MiB; no payload follows.
    client.writer().write_all(&(1u32 << 20).to_le_bytes()).unwrap();
    let mut cipher = Vec::new();
    while client.wants_write() {
        client.write_tls(&mut cipher).unwrap();
    }
    let before = reactor.total_inbound_bytes();
    assert!(
        ingest(&mut conn, &reactor, &cipher).is_err(),
        "cap breach must refuse the frame at header-parse time"
    );
    assert_eq!(
        reactor.total_inbound_bytes(),
        before,
        "a refused allocation must charge nothing"
    );
}

#[test]
fn inbound_cap_trips_after_many_small_frames() {
    let (mut client, server) = handshaken_pair();
    let reactor = Reactor::new(16).unwrap();
    // frame_weight floors at 64 B/frame → a 4 KiB cap trips within ~64
    // one-byte frames.
    reactor.set_inbound_cap(4 * 1024);
    let mut conn = test_conn(server, 1 << 20);

    let mut tripped = false;
    for _ in 0..200 {
        let cipher = encrypt_frames(&mut client, &[&[0x7Fu8; 1]]);
        if ingest(&mut conn, &reactor, &cipher).is_err() {
            tripped = true;
            break;
        }
    }
    assert!(tripped, "queued unconsumed frames must trip the inbound cap");
}

#[test]
fn dirty_drop_reclaims_inbound_counter() {
    // Queue several completed frames (charging the counter), then drop the
    // TlsConn without popping them: each RecvBuf's own Drop must refund.
    let (mut client, server) = handshaken_pair();
    let reactor = Reactor::new(16).unwrap();
    let base = reactor.total_inbound_bytes();
    let mut conn = test_conn(server, 1 << 20);

    let frames: Vec<Vec<u8>> = (0..5).map(|i| vec![i as u8; 4_000]).collect();
    let refs: Vec<&[u8]> = frames.iter().map(|f| f.as_slice()).collect();
    let cipher = encrypt_frames(&mut client, &refs);
    ingest(&mut conn, &reactor, &cipher).unwrap();
    assert_eq!(conn.inbound.len(), 5);
    assert!(
        reactor.total_inbound_bytes() > base,
        "queued frames must be charged to the global counter"
    );

    drop(conn);
    assert_eq!(
        reactor.total_inbound_bytes(),
        base,
        "dropping the TlsConn with unread frames must refund the counter in full"
    );
}

/// mTLS handshake: a server built with a client-cert verifier and a client
/// presenting a CA-signed leaf must complete the handshake, and the server
/// must observe the client's cert chain. This is the in-memory analogue of
/// the required-mTLS `--tls-client-ca` path.
#[test]
fn mtls_handshake_presents_client_cert() {
    use rustls::pki_types::pem::PemObject;
    use std::io::Write;

    // Client CA + a leaf signed by it (rcgen 0.14 Issuer API). Default
    // validity (1975–4096) never expires; an empty key-usage set emits no KU
    // extension, so webpki imposes no CA key-usage constraint.
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(vec!["gnitz-client-ca".to_string()]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_pem = ca_cert.pem();

    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf_params = rcgen::CertificateParams::new(vec!["gnitz-client".to_string()]).unwrap();
    let issuer = rcgen::Issuer::new(ca_params, ca_key);
    let leaf_cert = leaf_params.signed_by(&leaf_key, &issuer).unwrap();

    // Server: required mTLS against the client CA (fed as a PEM file path).
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("client_ca.pem");
    std::fs::File::create(&ca_path)
        .unwrap()
        .write_all(ca_pem.as_bytes())
        .unwrap();
    let (server_cfg, dev_pem) = config::server_crypto(None, Some(ca_path.to_str().unwrap())).unwrap();
    let dev_pem = dev_pem.expect("dev server cert minted");

    // Client: verify the dev server cert AND present the CA-signed leaf.
    let mut roots = rustls::RootCertStore::empty();
    roots.add_parsable_certificates(
        rustls::pki_types::CertificateDer::pem_slice_iter(dev_pem.as_bytes()).filter_map(Result::ok),
    );
    let leaf_chain = vec![leaf_cert.der().clone()];
    let leaf_key_der = rustls::pki_types::PrivateKeyDer::from_pem_slice(leaf_key.serialize_pem().as_bytes()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut client_cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(leaf_chain, leaf_key_der)
        .unwrap();
    client_cfg.alpn_protocols = vec![gnitz_wire::ALPN_GNITZ.to_vec()];
    // 0-RTT lock (client side): early data stays off (rustls default).
    assert!(!client_cfg.enable_early_data, "client 0-RTT early data must be off");

    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut client = rustls::ClientConnection::new(Arc::new(client_cfg), server_name).unwrap();
    let mut server = rustls::ServerConnection::new(server_cfg).unwrap();

    while client.is_handshaking() || server.is_handshaking() {
        let mut c2s = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut c2s).unwrap();
        }
        let mut s: &[u8] = &c2s;
        while !s.is_empty() {
            server.read_tls(&mut s).unwrap();
            server.process_new_packets().unwrap();
        }
        let mut s2c = Vec::new();
        while server.wants_write() {
            server.write_tls(&mut s2c).unwrap();
        }
        let mut c: &[u8] = &s2c;
        while !c.is_empty() {
            client.read_tls(&mut c).unwrap();
            client.process_new_packets().unwrap();
        }
    }
    assert!(
        !client.is_handshaking() && !server.is_handshaking(),
        "mTLS handshake must complete on both sides"
    );
    assert!(
        server.peer_certificates().is_some(),
        "server must observe the authenticated client cert chain"
    );
}
