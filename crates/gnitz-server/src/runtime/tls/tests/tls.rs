//! Unit tests for what is specific to TLS: what a listener serves, the minted
//! identity kept in the data directory, the reload, and driving the
//! connection's `RecvQueue` from rustls's `fill_buf` across record boundaries
//! and awkward socket-chunk splits. The policy the queue owns — the per-frame
//! ceiling, the inbound charge, the zero-length prefix — is covered once, on
//! the fd path, in `reactor::tests`; what `TlsArgs::resolve` refuses is
//! covered from argv, beside the binary's argument parser.
//!
//! Handshakes shuttle ciphertext between a rustls client and server in memory,
//! the client verifying for real against the certificate it is told to trust.
//! The session tests run a `TlsShared` over a socketpair.

use std::io::Read as _;
use std::os::unix::net::UnixStream;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};

use super::config::{TLS_CERT_FILE, TLS_KEY_FILE};
use super::*;
use crate::runtime::reactor::{egress_pair, poll_until, read_nonblocking, Budget};
use gnitz_wire::framed;

const LOOPBACK: &str = "127.0.0.1:0";

/// A client dialling `name`, trusting only `pem`'s certificate, offering gnitz's ALPN.
fn client_trusting(pem: &str, name: &str) -> rustls::ClientConnection {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(pem.as_bytes()).unwrap())
        .unwrap();
    let mut cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![gnitz_wire::ALPN_GNITZ.to_vec()];
    let name = ServerName::try_from(name.to_string()).unwrap();
    rustls::ClientConnection::new(Arc::new(cfg), name).unwrap()
}

/// Every TLS byte `conn` has queued.
fn ciphertext<D>(conn: &mut rustls::ConnectionCommon<D>) -> Vec<u8> {
    let mut out = Vec::new();
    while conn.wants_write() {
        conn.write_tls(&mut out).unwrap();
    }
    out
}

/// Feed `bytes` of ciphertext through `conn`; its refusal, if it refuses.
fn feed<D>(conn: &mut rustls::ConnectionCommon<D>, mut bytes: &[u8]) -> Result<(), rustls::Error> {
    while !bytes.is_empty() {
        conn.read_tls(&mut bytes).unwrap();
        conn.process_new_packets()?;
    }
    Ok(())
}

/// Handshake a [`client_trusting`] `pem` and dialling `name` against a session
/// under `served`; the server's refusal, if it refuses.
fn handshake(
    served: &Served,
    pem: &str,
    name: &str,
) -> Result<(rustls::ClientConnection, rustls::ServerConnection), rustls::Error> {
    let mut client = client_trusting(pem, name);
    let mut server = rustls::ServerConnection::new(Arc::clone(&served.cfg)).unwrap();
    while client.is_handshaking() || server.is_handshaking() {
        feed(&mut server, &ciphertext(&mut client))?;
        feed(&mut client, &ciphertext(&mut server)).unwrap();
    }
    Ok((client, server))
}

/// The listener `args` opens in `dir`, once [`TlsArgs::resolve`] admits them.
fn open_in(args: TlsArgs, dir: &tempfile::TempDir) -> Result<TlsListener, String> {
    TlsListener::open(args.resolve().unwrap(), dir.path().to_str().unwrap())
}

/// What `args` serves from a fresh data directory, the PEM minted there, and the directory.
fn load_minted(args: TlsArgs) -> (Rc<Served>, String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let served = open_in(args, &dir).unwrap().current();
    let pem = std::fs::read_to_string(dir.path().join(TLS_CERT_FILE)).expect("no operator identity mints one");
    (served, pem, dir)
}

/// `dir/name`, as the flags spell a path.
fn path_in(dir: &tempfile::TempDir, name: &str) -> String {
    dir.path().join(name).to_str().unwrap().to_string()
}

/// The minted certificate is verified under gnitz's ALPN, with neither
/// resumption tickets nor 0-RTT, and is served again by the next open until
/// its certificate goes.
#[test]
fn a_minted_identity_is_served_and_kept() {
    use std::os::unix::fs::PermissionsExt as _;
    let (dev, pem, dir) = load_minted(TlsArgs::on(LOOPBACK));
    assert!(!pem.contains("PRIVATE KEY"), "the certificate file holds no key");
    let key_mode = std::fs::metadata(dir.path().join(TLS_KEY_FILE))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(key_mode & 0o777, 0o600);
    assert_eq!((dev.cfg.send_tls13_tickets, dev.cfg.max_early_data_size), (0, 0));
    let (client, _) = handshake(&dev, &pem, "localhost").unwrap();
    assert_eq!(client.alpn_protocol(), Some(gnitz_wire::ALPN_GNITZ));
    assert_eq!(dev.fingerprint.len(), 32 * 3 - 1, "{}", dev.fingerprint);

    let again = open_in(TlsArgs::on(LOOPBACK), &dir).unwrap().current();
    assert_eq!(again.fingerprint, dev.fingerprint, "a kept identity is served again");

    // A key without its certificate, what a crash mid-mint leaves, mints a new identity.
    std::fs::remove_file(dir.path().join(TLS_CERT_FILE)).unwrap();
    let minted = open_in(TlsArgs::on(LOOPBACK), &dir).unwrap().current();
    assert_ne!(minted.fingerprint, dev.fingerprint);
}

#[test]
fn tls_names_reach_the_minted_certificate() {
    let named = |names: &[&str]| TlsArgs {
        cert: CertSource::Minted(
            names
                .iter()
                .map(|n| ServerName::try_from(n.to_string()).unwrap())
                .collect(),
        ),
        allow_unauthenticated: true,
        ..TlsArgs::on("192.0.2.7:0")
    };
    let (served, pem, dir) = load_minted(named(&["db.internal", "192.0.2.7"]));
    for name in ["db.internal", "192.0.2.7", "localhost", "127.0.0.1", "::1"] {
        handshake(&served, &pem, name).unwrap_or_else(|e| panic!("{name}: {e}"));
    }

    let e = open_in(named(&["db.internal", "other.internal"]), &dir)
        .err()
        .expect("a name the kept certificate lacks");
    assert!(e.contains("other.internal") && e.contains(TLS_CERT_FILE), "{e}");
}

/// Every client authentication admits a public bind and is named by the
/// listener's log line; a client CA makes a client certificate mandatory.
#[test]
fn a_public_bind_is_served_under_the_authentication_it_names() {
    let dir = tempfile::tempdir().unwrap();
    let ca = path_in(&dir, "ca.pem");
    let client_ca = rcgen::generate_simple_self_signed(vec!["client-ca".to_string()]).unwrap();
    std::fs::write(&ca, client_ca.cert.pem()).unwrap();
    let tokens = path_in(&dir, "tokens");
    let token = mint_token(&tokens, "app").unwrap();
    for (client_ca, auth_tokens, by) in [
        (None, None, "nothing"),
        (Some(&ca), None, "a client certificate"),
        (None, Some(&tokens), "one of 1 access tokens"),
        (
            Some(&ca),
            Some(&tokens),
            "a client certificate and one of 1 access tokens",
        ),
    ] {
        let args = TlsArgs {
            client_ca: client_ca.cloned(),
            auth_tokens: auth_tokens.cloned(),
            allow_unauthenticated: client_ca.is_none() && auth_tokens.is_none(),
            ..TlsArgs::on("0.0.0.0:0")
        };
        let listener = open_in(args, &dir).unwrap();
        let line = listener.describe();
        assert!(line.ends_with(&format!("authenticated by {by}")), "{line}");
        let served = listener.current();
        let held = served.tokens.as_ref().map(|t| t.check(token.as_bytes()));
        assert_eq!(held, auth_tokens.map(|_| Some("app")), "{by}");
        let pem = std::fs::read_to_string(dir.path().join(TLS_CERT_FILE)).unwrap();
        let refused = handshake(&served, &pem, "localhost").err();
        assert_eq!(
            refused,
            client_ca.map(|_| rustls::Error::NoCertificatesPresented),
            "{by}"
        );
    }
}

/// An operator identity is served in place of a minted one, and a reload
/// replaces it unless the reload fails.
#[test]
fn a_reload_replaces_what_is_served_unless_it_fails() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = (path_in(&dir, "cert.pem"), path_in(&dir, "key.pem"));
    let write_identity = || {
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        std::fs::write(&cert, ck.cert.pem()).unwrap();
        std::fs::write(&key, ck.signing_key.serialize_pem()).unwrap();
        ck.cert.pem()
    };
    let first = write_identity();
    let args = TlsArgs {
        cert: CertSource::Operator { cert: cert.clone(), key: key.clone() },
        ..TlsArgs::on(LOOPBACK)
    };
    let listener = open_in(args, &dir).unwrap();
    assert!(!dir.path().join(TLS_CERT_FILE).exists(), "nothing is minted");
    handshake(&listener.current(), &first, "localhost").expect("the operator's certificate is served");

    let renewed = write_identity();
    listener.reload().unwrap();
    handshake(&listener.current(), &renewed, "localhost").expect("the renewed certificate is served");

    std::fs::write(&key, "").unwrap();
    let e = listener.reload().unwrap_err();
    assert!(e.contains(&key), "{e}");
    handshake(&listener.current(), &renewed, "localhost").expect("a failed reload keeps the last good setup");
}

/// Pipelined frames spanning several records deframe in order under any
/// ciphertext split, and a close_notify behind them ends the recv side only once
/// every frame ahead of it is queued.
#[test]
fn frames_deframe_in_order_under_any_split_until_close_notify() {
    let (dev, pem, _dir) = load_minted(TlsArgs::on(LOOPBACK));
    let frames = [
        vec![0xAAu8; 10],
        (0..100_000).map(|i| (i % 251) as u8).collect(), // spans several 16 KiB records
        b"tail".to_vec(),
    ];
    for chunk in [usize::MAX, 1_313] {
        let (mut client, mut server) = handshake(&dev, &pem, "localhost").unwrap();
        client.set_buffer_limit(None);
        for f in &frames {
            client.writer().write_all(&framed(f)).unwrap();
        }
        client.send_close_notify();
        let mut q = RecvQueue::new(Budget::new(usize::MAX));
        q.admit_full_frames();

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

/// A `TlsShared` over one end of a socketpair, the other end, which is the
/// client's, and the PEM the client trusts.
fn tls_over_socketpair() -> (Reactor, Rc<TlsShared>, UnixStream, String) {
    let (r, conn, receiver) = egress_pair();
    let (served, pem, _dir) = load_minted(TlsArgs::on(LOOPBACK));
    let tls = TlsShared::start(r.clone(), conn, Arc::clone(&served.cfg));
    (r, tls, receiver, pem)
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

/// An unencrypted alert record's header: content type, legacy version, length 2.
/// The level, fatal (2) or warning (1), and the description follow it.
const ALERT_HEADER: [u8; 5] = [0x15, 3, 3, 0, 2];

/// Bytes that are not TLS end the session with one fatal alert the client reads
/// before the EOF, and no close_notify behind it.
#[test]
fn a_protocol_error_reaches_the_client_as_one_fatal_alert() {
    let (r, _tls, client, _) = tls_over_socketpair();
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
    let (r, tls, client, _) = tls_over_socketpair();
    tls.conn.retire();

    let seen = read_until_eof(&r, &client).expect("the flusher shuts the socket down");
    assert_eq!(
        seen,
        [&ALERT_HEADER[..], &[1, 0]].concat(),
        "one close_notify, then EOF"
    );
}

/// One round between `client` and its end of the socket: what it queued goes
/// out, what the server sent comes in, and its plaintext is appended to `got`.
fn exchange(fd: &UnixStream, client: &mut rustls::ClientConnection, got: &mut Vec<u8>) {
    while client.wants_write() {
        client.write_tls(&mut &*fd).unwrap();
    }
    while let Some(bytes) = read_nonblocking(fd, 64 * 1024) {
        assert!(!bytes.is_empty(), "the server hung up");
        let mut bytes = &bytes[..];
        while !bytes.is_empty() {
            client.read_tls(&mut bytes).unwrap();
            client.process_new_packets().unwrap();
            let _ = client.reader().read_to_end(got);
        }
    }
}

/// The recv filter and the flusher drive a handshake, and a body far past
/// rustls's outgoing buffer reaches the client whole and in order.
#[test]
fn a_send_past_the_outgoing_buffer_reaches_the_client_whole() {
    let (r, tls, fd, pem) = tls_over_socketpair();
    let mut client = client_trusting(&pem, "localhost");
    let mut got = Vec::new();
    let handshaken = poll_until(&r, || {
        exchange(&fd, &mut client, &mut got);
        !client.is_handshaking() && !tls.state.borrow().is_handshaking()
    });
    assert!(handshaken, "the handshake completes");

    let body: Vec<u8> = (0..1 << 20).map(|i| (i % 251) as u8).collect();
    let (sender, sent) = (Rc::clone(&tls), body.clone());
    r.spawn(async move { sender.send(SendBody::Pooled(PooledBuf(sent))).await.expect("sent") });
    let arrived = poll_until(&r, || {
        exchange(&fd, &mut client, &mut got);
        got.len() >= body.len()
    });
    assert!(arrived, "{} of {} bytes arrive", got.len(), body.len());
    assert!(got == body, "the body arrives as it was sent");
}
