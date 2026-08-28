//! The operator-facing half of TLS: what `--tls-listen` asks for, the bind
//! refusal that guards it, and the listener the executor accepts on. Its policy
//! — an unauthenticated public bind is impossible by accident — is the other
//! half of the rule `config` states, where the CA becomes an mTLS verifier.

use std::cell::Cell;
use std::rc::Rc;

use super::ConnCountGuard;
use crate::runtime::posix;

/// TLS listener request from the CLI: the address to bind, optional operator
/// cert/key PEM paths (a self-signed dev cert is minted when absent), the
/// optional client-auth CA (enables required mTLS), the
/// `--allow-unauthenticated` escape hatch, and the global connection cap.
pub(crate) struct TlsCli {
    pub listen: std::net::SocketAddr,
    pub cert_key: Option<(String, String)>,
    pub client_ca: Option<String>,
    pub allow_unauthenticated: bool,
    pub max_conns: u32,
}

/// TLS listener runtime inputs, threaded into `ServerExecutor::run` (hence
/// `pub(crate)`): the bound listen fd, the rustls config, and both halves of
/// the "live TLS sessions ≤ `max_conns`" invariant. `Rc<Cell<..>>` also makes
/// this `!Send`, so the single-reactor-thread assumption the count rests on is
/// checked rather than assumed.
pub(crate) struct TlsListener {
    pub fd: i32,
    pub cfg: std::sync::Arc<rustls::ServerConfig>,
    pub max_conns: u32,
    live: Rc<Cell<u32>>,
}

impl TlsListener {
    /// Admit one session, or `None` at the cap. Test and increment are one
    /// call, so there is no window between them and no other way to obtain a
    /// [`ConnCountGuard`]. The guard's `Drop` decrements.
    pub(crate) fn admit(&self) -> Option<ConnCountGuard> {
        if self.live.get() >= self.max_conns {
            return None;
        }
        Some(ConnCountGuard::new(Rc::clone(&self.live)))
    }
}

/// Build the rustls server config (minting + persisting the public dev cert
/// when no PEM pair is given), bind the TCP listener, and publish the bound
/// address to `<data_dir>/tls_endpoint` (atomically: tmp + rename, so
/// existence implies complete content).
///
/// **Bind refusal:** a non-loopback bind with neither `--tls-client-ca` nor
/// `--allow-unauthenticated` aborts boot — an unauthenticated public bind is
/// impossible by accident. `--allow-unauthenticated` is the single, loud
/// escape hatch; the CA enables required mTLS.
pub(crate) fn setup_tls_listener(data_dir: &str, cli: &TlsCli) -> Result<TlsListener, String> {
    // `is_loopback()` is the conservative test: `0.0.0.0`/`::` (bind-all),
    // IPv4-mapped `::ffff:127.0.0.1`, and any specific LAN/link-local IP are
    // all non-loopback → refused unless a CA or the escape hatch is present.
    if !cli.listen.ip().is_loopback() && cli.client_ca.is_none() && !cli.allow_unauthenticated {
        return Err(format!(
            "refusing to bind a non-loopback TLS listener {} without client authentication; \
             pass --tls-client-ca=PEM or --allow-unauthenticated",
            cli.listen,
        ));
    }

    let cert_key = cli.cert_key.as_ref().map(|(c, k)| (c.as_str(), k.as_str()));
    let (config, dev_pem) = super::config::server_crypto(cert_key, cli.client_ca.as_deref())?;
    if let Some(pem) = dev_pem {
        let path = format!("{data_dir}/tls_dev_cert.pem");
        std::fs::write(&path, pem).map_err(|e| format!("failed to write {path}: {e}"))?;
        gnitz_info!(
            "TLS: minted a self-signed dev certificate (identity is ephemeral, regenerated every boot); \
             public PEM at {path}"
        );
    }
    let listen_fd =
        posix::tcp_bind(&cli.listen).map_err(|e| format!("failed to bind TLS listener {}: {e}", cli.listen))?;
    let bound = posix::tcp_local_addr(listen_fd).unwrap_or(cli.listen);
    let endpoint_path = format!("{data_dir}/tls_endpoint");
    let tmp_path = format!("{endpoint_path}.tmp");
    std::fs::write(&tmp_path, format!("{bound}\n")).map_err(|e| format!("failed to write {tmp_path}: {e}"))?;
    std::fs::rename(&tmp_path, &endpoint_path).map_err(|e| format!("failed to publish {endpoint_path}: {e}"))?;
    gnitz_info!("Listening on tls://{}", bound);
    // A deliberately-unauthenticated non-loopback bind (escape hatch, no CA)
    // stays loud. With a CA the listener is authenticated — no warning.
    if !bound.ip().is_loopback() && cli.client_ca.is_none() {
        gnitz_warn!(
            "TLS listener bound to NON-LOOPBACK address {} with --allow-unauthenticated and NO client \
             authentication: anyone who can reach this port gets full DDL/DML/scan access. Prefer \
             --tls-client-ca=PEM (required mTLS). Note: even a loopback bind trusts every local UID.",
            bound,
        );
    }
    Ok(TlsListener {
        fd: listen_fd,
        cfg: config,
        max_conns: cli.max_conns,
        live: Rc::new(Cell::new(0)),
    })
}
