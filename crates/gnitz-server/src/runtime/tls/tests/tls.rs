//! Unit tests for what is specific to the TLS ingress path: driving the
//! connection's `RecvQueue` from rustls's `fill_buf`, across record
//! boundaries and awkward socket-chunk splits. The policy the queue owns —
//! the per-frame ceiling, the inbound charge, the zero-length prefix — is
//! covered once, on the fd path, in `reactor::tests`.
//!
//! The ingress tests use no sockets: a rustls client+server pair is handshaken
//! by shuttling ciphertext through memory, then client-written plaintext frames
//! are fed into the server session and drained through `ingest_cipher`. The
//! teardown tests run a `TlsShared` over a socketpair.

use std::os::fd::{AsRawFd, OwnedFd};

use super::*;
use crate::runtime::reactor::{egress_pair, poll_until, read_nonblocking, Budget, Limits};

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
    let mut client_cfg = rustls::ClientConfig::builder()
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

/// The queue a session deframes into, whose inbound budget is wide enough never
/// to trip: the cap is the fd path's to test.
fn test_queue() -> RecvQueue {
    RecvQueue::new(Budget::new(usize::MAX))
}

/// Client-side: buffer `frames` (each as [len:u32 LE][payload]) as plaintext and
/// return the ciphertext.
fn encrypt_frames(client: &mut rustls::ClientConnection, frames: &[&[u8]]) -> Vec<u8> {
    client.set_buffer_limit(None);
    for f in frames {
        client.writer().write_all(&(f.len() as u32).to_le_bytes()).unwrap();
        client.writer().write_all(f).unwrap();
    }
    let mut out = Vec::new();
    while client.wants_write() {
        client.write_tls(&mut out).unwrap();
    }
    out
}

/// Every frame the queue holds, in order.
fn frame_payloads(q: &mut RecvQueue) -> Vec<Vec<u8>> {
    std::iter::from_fn(|| q.try_recv())
        .map(|b| b.as_slice().to_vec())
        .collect()
}

#[test]
fn pipelined_frames_deframe_in_order() {
    let (mut client, mut server) = handshaken_pair();
    let mut q = test_queue();

    let f1 = vec![0xAAu8; 10];
    let f2 = vec![0xBBu8; 100_000]; // spans multiple 16 KiB records
    let f3 = b"tail".to_vec();
    let cipher = encrypt_frames(&mut client, &[&f1, &f2, &f3]);
    ingest_cipher(&mut server, &cipher, &mut q).unwrap();

    assert_eq!(frame_payloads(&mut q), vec![f1, f2, f3]);
}

#[test]
fn split_ciphertext_delivery_reassembles() {
    let (mut client, mut server) = handshaken_pair();
    let mut q = test_queue();

    let payload: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
    let cipher = encrypt_frames(&mut client, &[&payload]);
    // Deliver in awkward chunks (mid-record splits included): a chunk that
    // ends mid-record must leave the session waiting, not close it.
    for chunk in cipher.chunks(1_313) {
        ingest_cipher(&mut server, chunk, &mut q).unwrap();
    }
    assert_eq!(frame_payloads(&mut q), vec![payload]);
}

/// A `TlsShared` over one end of a socketpair; the other end is the client.
fn tls_over_socketpair() -> (Rc<Reactor>, Rc<ClientConn>, OwnedFd) {
    let (r, sender, receiver) = egress_pair(Limits::TEST, None);
    let conn = r.client_conn(sender).expect("under the cap");
    let (cfg, _) = config::server_crypto(None, None).unwrap();
    TlsShared::start(Rc::clone(&r), Rc::clone(&conn), cfg);
    (r, conn, receiver)
}

/// Tick until the client end reads EOF, returning every byte before it; `None`
/// if the EOF never comes.
fn read_until_eof(r: &Reactor, fd: &OwnedFd) -> Option<Vec<u8>> {
    let mut seen = Vec::new();
    let eof = poll_until(r, 10_000, || {
        while let Some(bytes) = read_nonblocking(fd, 4096) {
            if bytes.is_empty() {
                return true;
            }
            seen.extend_from_slice(&bytes);
        }
        false
    });
    eof.then_some(seen)
}

/// TLS record content type of an alert.
const ALERT: u8 = 0x15;

/// Bytes that are not TLS end the session with a fatal alert the client reads
/// before the EOF.
#[test]
fn a_protocol_error_reaches_the_client_as_an_alert() {
    let (r, _conn, client) = tls_over_socketpair();
    let n = unsafe {
        let req = b"GET / HTTP/1.1\r\n\r\n";
        libc::write(client.as_raw_fd(), req.as_ptr() as *const libc::c_void, req.len())
    };
    assert!(n > 0, "write");

    let seen = read_until_eof(&r, &client).expect("the session ends");
    assert_eq!(seen.first(), Some(&ALERT), "an alert precedes the EOF, got {seen:?}");
}

/// A local close ships the close_notify, then shuts the socket down.
#[test]
fn a_tls_close_ships_close_notify_before_the_shutdown() {
    let (r, conn, client) = tls_over_socketpair();
    conn.retire();

    let seen = read_until_eof(&r, &client).expect("the flusher shuts the socket down");
    assert_eq!(
        seen.first(),
        Some(&ALERT),
        "a close_notify precedes the EOF, got {seen:?}"
    );
}
