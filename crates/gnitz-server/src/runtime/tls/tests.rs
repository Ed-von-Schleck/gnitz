//! Unit tests for what is specific to the TLS ingress path: driving the
//! shared `io::RecvQueue` from `rustls::Reader::read`, across record
//! boundaries and awkward socket-chunk splits. The policy the queue owns —
//! the per-frame ceiling, the inbound charge, the zero-length sentinel — is
//! covered once, on the fd path, in `reactor::tests`.
//!
//! No sockets: a rustls client+server pair is handshaken by shuttling
//! ciphertext through memory, then client-written plaintext frames are fed
//! into the server session and drained through `ingest_cipher`.

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

/// A session whose inbound budget is wide enough never to trip: the cap is
/// the fd path's to test.
fn test_conn(sess: rustls::ServerConnection, max_payload_len: usize) -> TlsConn {
    let mut conn = TlsConn::new(sess, Rc::new(io::InboundBudget::new(usize::MAX)));
    conn.q.set_max_payload_len(max_payload_len);
    conn
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

fn frame_payloads(conn: &TlsConn) -> Vec<Vec<u8>> {
    conn.q.queued_payloads()
}

#[test]
fn pipelined_frames_deframe_in_order() {
    let (mut client, server) = handshaken_pair();
    let mut conn = test_conn(server, 1 << 20);

    let f1 = vec![0xAAu8; 10];
    let f2 = vec![0xBBu8; 100_000]; // spans multiple 16 KiB records
    let f3 = b"tail".to_vec();
    let cipher = encrypt_frames(&mut client, &[&f1, &f2, &f3]);
    conn.ingest_cipher(&cipher, -1).unwrap();

    assert_eq!(frame_payloads(&conn), vec![f1, f2, f3]);
}

#[test]
fn split_ciphertext_delivery_reassembles() {
    let (mut client, server) = handshaken_pair();
    let mut conn = test_conn(server, 1 << 20);

    let payload: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
    let cipher = encrypt_frames(&mut client, &[&payload]);
    // Deliver in awkward chunks (mid-record splits included): a chunk that
    // ends mid-record must leave the session waiting, not close it.
    for chunk in cipher.chunks(1_313) {
        conn.ingest_cipher(chunk, -1).unwrap();
    }
    assert_eq!(frame_payloads(&conn), vec![payload]);
}
