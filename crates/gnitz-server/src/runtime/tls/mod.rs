//! TLS 1.3 server transport: the codec a TLS connection holds as its
//! [`Session`]. It does no I/O: the reactor lands the ciphertext it decrypts and
//! sends the records it seals.

mod config;
mod tokens;

pub(crate) use config::{CertSource, ResolvedTls, Served, TlsArgs, TlsListener};
pub(crate) use tokens::mint as mint_token;

use std::cell::RefCell;
use std::io::{BufRead, ErrorKind, Write};
use std::rc::Rc;

use gnitz_zset::repr::PooledBuf;

use crate::runtime::reactor::{RecvEnd, Session};

/// The most plaintext one TLS record carries.
const TLS_MAX_FRAGMENT: usize = 16 * 1024;
/// A TLS 1.3 record's bytes beyond its plaintext: header, inner content type,
/// AEAD tag.
const TLS_RECORD_OVERHEAD: usize = 5 + 1 + 16;

impl Served {
    /// The session of a connection served under this.
    pub(crate) fn session(&self) -> Rc<dyn Session> {
        let mut sess = rustls::ServerConnection::new(self.cfg.clone())
            .expect("a session fails to build only on a max_fragment_size no `Served` sets");
        // The reactor bounds each seal, and every seal drains what it buffered.
        sess.set_buffer_limit(None);
        Rc::new(RefCell::new(sess))
    }
}

/// One TLS connection's codec.
impl Session for RefCell<rustls::ServerConnection> {
    /// A peer's close_notify ends the recv side once the plaintext ahead of it
    /// is handed over.
    fn ingest(&self, mut cipher: &[u8], plain: &mut dyn FnMut(&[u8]) -> Result<(), RecvEnd>) -> Result<(), RecvEnd> {
        let mut sess = self.borrow_mut();
        while !cipher.is_empty() {
            // rustls takes nothing once a handshake message has outgrown its
            // buffer, which no drain below frees.
            if !matches!(sess.read_tls(&mut cipher), Ok(1..)) {
                return Err(RecvEnd::Oversize);
            }
            sess.process_new_packets()
                .map_err(|e| RecvEnd::Session(e.to_string()))?;
            let mut reader = sess.reader();
            loop {
                let n = match reader.fill_buf() {
                    Ok([]) => return Err(RecvEnd::PeerClosed),
                    Ok(chunk) => {
                        plain(chunk)?;
                        chunk.len()
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(_) => return Err(RecvEnd::Protocol),
                };
                reader.consume(n);
            }
        }
        Ok(())
    }

    fn owes(&self) -> bool {
        self.borrow().wants_write()
    }

    fn seal(&self, plain: &[u8]) -> PooledBuf {
        let mut sess = self.borrow_mut();
        sess.writer()
            .write_all(plain)
            .expect("rustls takes every byte under no buffer limit");
        let records = plain.len().div_ceil(TLS_MAX_FRAGMENT);
        let mut out = PooledBuf::with_capacity(plain.len() + records * TLS_RECORD_OVERHEAD);
        while sess.wants_write() {
            let _ = sess.write_tls(&mut out.0);
        }
        out
    }

    fn close(&self) -> PooledBuf {
        self.borrow_mut().send_close_notify();
        self.seal(&[])
    }
}

#[cfg(test)]
#[path = "tests/tls.rs"]
mod tests;
