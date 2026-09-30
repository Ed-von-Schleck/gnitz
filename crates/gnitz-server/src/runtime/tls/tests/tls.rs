//! Unit tests for what is specific to TLS: the config `--tls-*` resolves to,
//! and driving the connection's `RecvQueue` from rustls's `fill_buf` across
//! record boundaries and awkward socket-chunk splits. The policy the queue
//! owns — the per-frame ceiling, the inbound charge, the zero-length prefix —
//! is covered once, on the fd path, in `reactor::tests`.
//!
//! Handshakes shuttle ciphertext between a rustls client and server in memory,
//! the client verifying for real against the certificate it is told to trust.
//! The teardown tests run a `TlsShared` over a socketpair.

use std::io::Write as _;
use std::os::unix::net::UnixStream;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};

use super::*;
use crate::runtime::reactor::{egress_pair, framed, poll_until, read_nonblocking, Budget, Limits};

const LOOPBACK: &str = "127.0.0.1:0";

/// A client trusting only `pem`'s certificate, offering gnitz's ALPN.
fn client_trusting(pem: &str) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(pem.as_bytes()).unwrap())
        .unwrap();
    let mut cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![gnitz_wire::ALPN_GNITZ.to_vec()];
    cfg
}

/// Every TLS byte `conn` has queued.
fn ciphertext<D>(conn: &mut rustls::ConnectionCommon<D>) -> Vec<u8> {
    let mut out = Vec::new();
    while conn.wants_write() {
        conn.write_tls(&mut out).unwrap();
    }
    out
}

/// Handshake a `client` for "localhost" against a session under `server`; the
/// server's refusal, if it refuses.
fn handshake(
    server: &TlsConfig,
    client: rustls::ClientConfig,
) -> Result<(rustls::ClientConnection, rustls::ServerConnection), rustls::Error> {
    let name = ServerName::try_from("localhost").unwrap();
    let mut client = rustls::ClientConnection::new(Arc::new(client), name).unwrap();
    let mut server = rustls::ServerConnection::new(Arc::clone(&server.cfg)).unwrap();
    while client.is_handshaking() || server.is_handshaking() {
        let c2s = ciphertext(&mut client);
        let mut c2s = &c2s[..];
        while !c2s.is_empty() {
            server.read_tls(&mut c2s).unwrap();
            server.process_new_packets()?;
        }
        let s2c = ciphertext(&mut server);
        let mut s2c = &s2c[..];
        while !s2c.is_empty() {
            client.read_tls(&mut s2c).unwrap();
            client.process_new_packets().unwrap();
        }
    }
    Ok((client, server))
}

/// A dev listener's config and the public PEM of its minted certificate.
fn dev_config() -> (TlsConfig, String) {
    let mut dev = TlsArgs::on(LOOPBACK).resolve().expect("loopback is turnkey");
    let pem = dev.dev_pem.take().expect("no operator identity mints one");
    (dev, pem)
}

/// Write `pem` to `dir/name`, returning the path.
fn pem_file(dir: &tempfile::TempDir, name: &str, pem: &str) -> String {
    let path = dir.path().join(name);
    std::fs::write(&path, pem).unwrap();
    path.to_str().unwrap().to_string()
}

/// With no operator identity the listener serves a minted certificate a client
/// trusting its public PEM verifies, negotiating gnitz's ALPN, with neither
/// resumption tickets nor replayable 0-RTT data.
#[test]
fn a_dev_listener_serves_its_minted_certificate() {
    let (dev, pem) = dev_config();
    assert!(!pem.contains("PRIVATE KEY"), "the dev private key is never exported");
    assert_eq!((dev.cfg.send_tls13_tickets, dev.cfg.max_early_data_size), (0, 0));
    let (client, _) = handshake(&dev, client_trusting(&pem)).unwrap();
    assert_eq!(client.alpn_protocol(), Some(gnitz_wire::ALPN_GNITZ));
}

#[test]
fn an_operator_identity_is_served_in_place_of_a_minted_one() {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert_key = (
        pem_file(&dir, "cert.pem", &ck.cert.pem()),
        pem_file(&dir, "key.pem", &ck.signing_key.serialize_pem()),
    );
    let tls = TlsArgs {
        cert_key: Some(cert_key),
        ..TlsArgs::on(LOOPBACK)
    }
    .resolve()
    .unwrap();
    assert!(tls.dev_pem.is_none(), "nothing is minted");
    handshake(&tls, client_trusting(&ck.cert.pem())).expect("the client verifies the operator's certificate");
}

/// A client CA is what admits a public bind without the escape hatch, and it
/// makes a client certificate mandatory.
#[test]
fn a_client_ca_admits_a_public_bind_and_requires_a_client_certificate() {
    let ca = rcgen::generate_simple_self_signed(vec!["client-ca".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let tls = TlsArgs {
        client_ca: Some(pem_file(&dir, "ca.pem", &ca.cert.pem())),
        ..TlsArgs::on("0.0.0.0:0")
    }
    .resolve()
    .expect("mTLS admits a public bind");
    let pem = tls.dev_pem.as_deref().expect("the server certificate is still minted");
    assert_eq!(
        handshake(&tls, client_trusting(pem)).err(),
        Some(rustls::Error::NoCertificatesPresented)
    );
}

#[test]
fn a_public_bind_without_client_authentication_needs_the_escape_hatch() {
    for public in ["0.0.0.0:0", "[::]:0", "[::ffff:127.0.0.1]:0"] {
        let Err(e) = TlsArgs::on(public).resolve() else {
            panic!("{public} is refused")
        };
        assert!(e.contains("--allow-unauthenticated"), "{e}");
    }
    let hatch = TlsArgs {
        allow_unauthenticated: true,
        ..TlsArgs::on("0.0.0.0:0")
    };
    assert!(hatch.resolve().is_ok(), "the escape hatch admits it");
}

/// Every unusable PEM file is refused, and the error names it.
#[test]
fn an_unusable_pem_file_is_refused_naming_it() {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert = pem_file(&dir, "cert.pem", &ck.cert.pem());
    let malformed = pem_file(
        &dir,
        "malformed.pem",
        &format!(
            "{}-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n",
            ck.cert.pem()
        ),
    );
    let empty = pem_file(&dir, "empty.pem", "");
    let (no_cert, no_key, no_ca) = ("/nonexistent/cert.pem", "/nonexistent/key.pem", "/nonexistent/ca.pem");
    for (cert_key, client_ca, named) in [
        (Some((no_cert, no_key)), None, no_cert),
        (Some((&cert[..], no_key)), None, no_key),
        (None, Some(no_ca), no_ca),
        (None, Some(&malformed[..]), &malformed[..]),
        (None, Some(&empty[..]), &empty[..]),
    ] {
        let args = TlsArgs {
            cert_key: cert_key.map(|(c, k)| (c.to_string(), k.to_string())),
            client_ca: client_ca.map(str::to_string),
            ..TlsArgs::on(LOOPBACK)
        };
        let e = args.resolve().err().unwrap_or_else(|| panic!("{named} is refused"));
        assert!(e.contains(named), "{e}");
    }
}

/// Pipelined frames spanning several records deframe in order under any
/// ciphertext split, and a close_notify behind them ends the recv side only once
/// every frame ahead of it is queued.
#[test]
fn frames_deframe_in_order_under_any_split_until_close_notify() {
    let (dev, pem) = dev_config();
    let frames = [
        vec![0xAAu8; 10],
        (0..100_000).map(|i| (i % 251) as u8).collect(), // spans several 16 KiB records
        b"tail".to_vec(),
    ];
    for chunk in [usize::MAX, 1_313] {
        let (mut client, mut server) = handshake(&dev, client_trusting(&pem)).unwrap();
        client.set_buffer_limit(None);
        for f in &frames {
            client.writer().write_all(&framed(f)).unwrap();
        }
        client.send_close_notify();
        let mut q = RecvQueue::new(Budget::new(usize::MAX));

        let ends: Vec<_> = ciphertext(&mut client)
            .chunks(chunk)
            .map(|c| ingest_cipher(&mut server, c, &mut q))
            .collect();
        let (last, rest) = ends.split_last().unwrap();
        assert!(
            rest.iter().all(Result::is_ok),
            "a chunk ending mid-record waits: {ends:?}"
        );
        assert!(matches!(last, Err(RecvEnd::PeerClosed)), "{last:?}");
        let got: Vec<_> = std::iter::from_fn(|| q.try_recv())
            .map(|b| b.as_slice().to_vec())
            .collect();
        assert_eq!(got, frames);
    }
}

/// A `TlsShared` over one end of a socketpair; the other end is the client.
fn tls_over_socketpair() -> (Rc<Reactor>, Rc<ClientConn>, UnixStream) {
    let (r, conn, receiver) = egress_pair(Limits::TEST, None);
    TlsShared::start(Rc::clone(&r), Rc::clone(&conn), dev_config().0.cfg);
    (r, conn, receiver)
}

/// Tick until the client end reads EOF, returning every byte before it; `None`
/// if the EOF never comes.
fn read_until_eof(r: &Reactor, fd: &UnixStream) -> Option<Vec<u8>> {
    let mut seen = Vec::new();
    let eof = poll_until(r, || {
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

/// An alert record before the handshake: content type, legacy version, length,
/// then the level, fatal (2) or warning (1).
const ALERT_HEADER: [u8; 5] = [0x15, 3, 3, 0, 2];

/// Bytes that are not TLS end the session with one fatal alert the client reads
/// before the EOF, and no close_notify behind it.
#[test]
fn a_protocol_error_reaches_the_client_as_one_fatal_alert() {
    let (r, _conn, client) = tls_over_socketpair();
    (&client).write_all(b"GET / HTTP/1.1\r\n\r\n").expect("write");

    let seen = read_until_eof(&r, &client).expect("the session ends");
    assert!(
        seen.len() == 7 && seen[..5] == ALERT_HEADER && seen[5] == 2,
        "one fatal alert, then EOF: {seen:?}"
    );
}

/// A local close ships the close_notify, then shuts the socket down.
#[test]
fn a_tls_close_ships_close_notify_before_the_shutdown() {
    let (r, conn, client) = tls_over_socketpair();
    conn.retire();

    let seen = read_until_eof(&r, &client).expect("the flusher shuts the socket down");
    assert_eq!(
        seen,
        [&ALERT_HEADER[..], &[1, 0]].concat(),
        "one close_notify, then EOF"
    );
}
