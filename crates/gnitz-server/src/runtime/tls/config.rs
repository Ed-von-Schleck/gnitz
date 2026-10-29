//! The TLS flags, and what a TLS listener's connections are served under, read
//! again on every reload.

use std::cell::RefCell;
use std::io::Write as _;
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt as _;
use std::rc::Rc;
use std::sync::Arc;

use gnitz_foundation::posix_io::fsync_parent;
use gnitz_wire::ALPN_GNITZ;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::danger::ClientCertVerifier;

use super::tokens::{sha256, Tokens};

/// The `--tls-*` and `--auth-tokens` flags.
pub(crate) struct TlsArgs {
    pub listen: SocketAddr,
    pub cert: CertSource,
    pub client_ca: Option<String>,
    /// `--auth-tokens`: the token file every client's HELLO is judged against.
    pub auth_tokens: Option<String>,
    pub allow_unauthenticated: bool,
}

/// Where the served certificate comes from.
#[derive(Debug, PartialEq)]
pub(crate) enum CertSource {
    /// `--tls-cert` and `--tls-key`: the operator's chain and key files.
    Operator { cert: String, key: String },
    /// The identity minted in the data directory, covering [`LOOPBACK_NAMES`]
    /// and these `--tls-name`s.
    Minted(Vec<ServerName<'static>>),
}

/// The names every minted certificate covers.
const LOOPBACK_NAMES: [&str; 3] = ["localhost", "127.0.0.1", "::1"];

/// `<data_dir>/<name>` files of the minted identity: the certificate a client
/// pins with `ca=`, and its key.
pub(super) const TLS_CERT_FILE: &str = "tls_cert.pem";
pub(super) const TLS_KEY_FILE: &str = "tls_key.pem";

/// What one load of a [`TlsArgs`] serves connections under.
pub(crate) struct Served {
    pub(crate) cfg: Arc<rustls::ServerConfig>,
    /// `None`: no token is asked for.
    pub(crate) tokens: Option<Tokens>,
    /// The served certificate's SHA-256, as `openssl x509 -fingerprint -sha256` prints it.
    pub(super) fingerprint: String,
}

/// [`TlsArgs`] the bind policy admits, every file of which loaded once.
pub(crate) struct ResolvedTls(TlsArgs);

#[cfg(test)]
impl ResolvedTls {
    /// `args`, past no bind policy and no load.
    pub(crate) fn unchecked(args: TlsArgs) -> Self {
        ResolvedTls(args)
    }
}

impl TlsArgs {
    /// Apply the bind policy, and load every file the flags name so an unusable
    /// one is refused as argv.
    pub(crate) fn resolve(self) -> Result<ResolvedTls, String> {
        let listen = self.listen;
        let authenticated = self.client_ca.is_some() || self.auth_tokens.is_some();
        if !listen.ip().is_loopback() && !authenticated {
            if !self.allow_unauthenticated {
                return Err(format!(
                    "refusing to bind a non-loopback TLS listener {listen} without client authentication; \
                     pass --auth-tokens=FILE, --tls-client-ca=PEM or --allow-unauthenticated",
                ));
            }
            gnitz_warn!(
                "TLS listener on NON-LOOPBACK address {listen} with --allow-unauthenticated and NO client \
                 authentication: anyone who can reach this port gets full DDL/DML/scan access. Prefer \
                 --auth-tokens=FILE.",
            );
        }
        // A minted certificate names loopback and `--tls-name`. A wildcard bind
        // is still reachable through loopback; a specific address is not.
        if self.cert == CertSource::Minted(Vec::new()) && !listen.ip().is_loopback() {
            if !listen.ip().is_unspecified() {
                return Err(format!(
                    "refusing to bind the TLS listener to {listen} without a certificate naming it: pass \
                     --tls-name with the name or address clients dial, or --tls-cert and --tls-key",
                ));
            }
            gnitz_warn!(
                "TLS listener on {listen} serves a minted certificate for {} only: a client dialling any other \
                 address cannot verify it. Pass --tls-name, or --tls-cert and --tls-key.",
                LOOPBACK_NAMES.join(", "),
            );
        }
        // Every file the flags name is loaded once now, so one that does not
        // load is refused as argv. A minted identity is the data directory's.
        match &self.cert {
            CertSource::Operator { cert, key } => drop(self.serve(operator_identity(cert, key)?)?),
            CertSource::Minted(_) => {
                self.client_ca.as_deref().map(client_verifier).transpose()?;
                self.auth_tokens.as_deref().map(Tokens::load).transpose()?;
            }
        }
        Ok(ResolvedTls(self))
    }

    /// Read every file the flags name, and the identity minted in `data_dir`
    /// unless an operator's is named, into what connections are served under.
    fn load(&self, data_dir: &str) -> Result<Served, String> {
        let identity = match &self.cert {
            CertSource::Operator { cert, key } => operator_identity(cert, key)?,
            CertSource::Minted(names) => minted_identity(data_dir, names)?,
        };
        self.serve(identity)
    }

    /// What connections are served under with `identity`.
    fn serve(&self, (chain, key): Identity) -> Result<Served, String> {
        let fingerprint = sha256(&chain[0]).map(|b| format!("{b:02X}")).join(":");
        let builder = rustls::ServerConfig::builder();
        let auth = match &self.client_ca {
            Some(path) => builder.with_client_cert_verifier(client_verifier(path)?),
            None => builder.with_no_client_auth(),
        };
        let mut cfg = auth
            .with_single_cert(chain, key)
            .map_err(|e| format!("tls cert/key rejected: {e}"))?;
        cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
        // Connections are long-lived: resumption buys nothing.
        cfg.send_tls13_tickets = 0;
        // Early data is replayable.
        cfg.max_early_data_size = 0;
        Ok(Served {
            cfg: Arc::new(cfg),
            tokens: self.auth_tokens.as_deref().map(Tokens::load).transpose()?,
            fingerprint,
        })
    }
}

/// A TLS listener's flags, and what its next connection is served under.
pub(crate) struct TlsListener {
    args: TlsArgs,
    data_dir: String,
    served: RefCell<Rc<Served>>,
}

impl TlsListener {
    /// The first load of `args`.
    pub(crate) fn open(ResolvedTls(args): ResolvedTls, data_dir: &str) -> Result<TlsListener, String> {
        let served = args.load(data_dir)?;
        Ok(TlsListener {
            args,
            data_dir: data_dir.to_string(),
            served: RefCell::new(Rc::new(served)),
        })
    }

    pub(crate) fn listen(&self) -> SocketAddr {
        self.args.listen
    }

    pub(crate) fn current(&self) -> Rc<Served> {
        Rc::clone(&self.served.borrow())
    }

    /// Load the flags' files again; a failed load keeps what is served.
    pub(crate) fn reload(&self) -> Result<(), String> {
        let served = self.args.load(&self.data_dir)?;
        *self.served.borrow_mut() = Rc::new(served);
        Ok(())
    }

    /// One line for the log: the certificate served and what authenticates a client.
    pub(crate) fn describe(&self) -> String {
        let served = self.current();
        let auth = match (&served.tokens, self.args.client_ca.is_some()) {
            (Some(t), true) => format!("a client certificate and one of {} access tokens", t.len()),
            (Some(t), false) => format!("one of {} access tokens", t.len()),
            (None, true) => "a client certificate".to_string(),
            (None, false) => "nothing".to_string(),
        };
        format!(
            "certificate SHA-256 {}; a client is authenticated by {auth}",
            served.fingerprint
        )
    }
}

type Identity = (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>);

/// The operator's certificate chain and key, as `--tls-cert`/`--tls-key` name them.
fn operator_identity(cert_path: &str, key_path: &str) -> Result<Identity, String> {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .and_then(|it| it.collect())
        .map_err(|e| format!("tls cert {cert_path:?}: {e}"))?;
    if chain.is_empty() {
        return Err(format!("tls cert {cert_path:?}: no certificates"));
    }
    let key = PrivateKeyDer::from_pem_file(key_path).map_err(|e| format!("tls key {key_path:?}: {e}"))?;
    Ok((chain, key))
}

/// The client certificate verifier `--tls-client-ca` names: a client
/// certificate chaining to it is mandatory.
fn client_verifier(path: &str) -> Result<Arc<dyn ClientCertVerifier>, String> {
    let verifier = || -> Result<_, Box<dyn std::error::Error>> {
        let mut roots = rustls::RootCertStore::empty();
        for ca in CertificateDer::pem_file_iter(path)? {
            roots.add(ca?)?;
        }
        Ok(rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?)
    };
    verifier().map_err(|e| format!("tls client-ca {path:?}: {e}"))
}

/// The self-signed identity kept in `data_dir`, minted for [`LOOPBACK_NAMES`]
/// and `names` when absent. Clients pin it, so a kept one is never replaced.
fn minted_identity(data_dir: &str, names: &[ServerName<'static>]) -> Result<Identity, String> {
    let cert_path = format!("{data_dir}/{TLS_CERT_FILE}");
    let key_path = format!("{data_dir}/{TLS_KEY_FILE}");
    let mut all: Vec<ServerName<'static>> = LOOPBACK_NAMES
        .map(|n| ServerName::try_from(n).expect("a loopback name"))
        .to_vec();
    for name in names {
        if !all.contains(name) {
            all.push(name.clone());
        }
    }
    if !(std::path::Path::new(&cert_path).exists() && std::path::Path::new(&key_path).exists()) {
        let sans: Vec<String> = all.iter().map(|n| n.to_str().into_owned()).collect();
        let ck = rcgen::generate_simple_self_signed(sans.clone())
            .map_err(|e| format!("tls certificate mint failed: {e}"))?;
        // The certificate last: a crash leaves no certificate, which mints anew.
        write_file(&key_path, ck.signing_key.serialize_pem().as_bytes(), 0o600)?;
        write_file(&cert_path, ck.cert.pem().as_bytes(), 0o644)?;
        gnitz_note!(
            "TLS: minted a self-signed certificate for {}; a client trusts it with ca={cert_path}",
            sans.join(", ")
        );
    }
    let identity = operator_identity(&cert_path, &key_path)?;
    let leaf = rustls::server::ParsedCertificate::try_from(&identity.0[0])
        .map_err(|e| format!("tls cert {cert_path:?}: {e}"))?;
    for name in &all {
        if rustls::client::verify_server_name(&leaf, name).is_err() {
            return Err(format!(
                "the kept certificate {cert_path} does not cover {:?}; remove {TLS_CERT_FILE} and \
                 {TLS_KEY_FILE} from the data directory to mint one for every --tls-name, and give every \
                 client the new {TLS_CERT_FILE}",
                name.to_str()
            ));
        }
    }
    Ok(identity)
}

/// Replace `path` with `bytes`, permission bits `mode`, whole or not at all, and
/// durably once it returns.
pub(super) fn write_file(path: &str, bytes: &[u8], mode: u32) -> Result<(), String> {
    let tmp = format!("{path}.tmp");
    let _ = std::fs::remove_file(&tmp);
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(&tmp)
        .and_then(|mut f| f.write_all(bytes).and_then(|()| f.sync_all()))
        .and_then(|()| std::fs::rename(&tmp, path))
        .and_then(|()| fsync_parent(path))
        .map_err(|e| format!("failed to write {path}: {e}"))
}
