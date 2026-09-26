//! The operator-facing half of TLS: what `--tls-listen` asks for, the bind
//! refusal that guards it, and the listener the executor accepts on. Its policy
//! — an unauthenticated public bind is impossible by accident — is the other
//! half of the rule `config` states, where the CA becomes an mTLS verifier.

use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::rc::Rc;

use crate::runtime::reactor::{Budget, Charge};

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

/// TLS listener runtime inputs: the rustls config and this listener's admission
/// policy.
pub(crate) struct TlsListener {
    pub cfg: std::sync::Arc<rustls::ServerConfig>,
    /// How long the TLS handshake and HELLO may take after accept
    /// (`GNITZ_TLS_HELLO_TIMEOUT_MS`). The default outlasts the client's own
    /// `CONNECT_TIMEOUT`, so only a client that has given up is reaped.
    pub hello_timeout: std::time::Duration,
    /// Live sessions under the connection cap; each holds one [`Charge`] of it.
    live: Rc<Budget>,
}

impl TlsListener {
    pub(crate) fn max_conns(&self) -> usize {
        self.live.cap()
    }

    /// Admit one session, or `None` at the cap.
    pub(crate) fn admit(&self) -> Option<Charge> {
        self.live.charge(1)
    }
}

/// Build the rustls server config (minting + persisting the public dev cert
/// when no PEM pair is given), bind the TCP listener, and publish the bound
/// address to `<data_dir>/tls_endpoint`. Both files go out through the storage
/// layer's publisher, so each exists only with its complete content.
///
/// **Bind refusal:** a non-loopback bind with neither `--tls-client-ca` nor
/// `--allow-unauthenticated` aborts boot — an unauthenticated public bind is
/// impossible by accident. `--allow-unauthenticated` is the single, loud
/// escape hatch; the CA enables required mTLS.
pub(crate) fn setup_tls_listener(data_dir: &str, cli: &TlsCli) -> Result<(TcpListener, TlsListener), String> {
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
        gnitz_store::storage::publish_file_sync(data_dir, "tls_dev_cert.pem", pem.as_bytes())
            .map_err(|e| format!("failed to publish {path}: {e}"))?;
        gnitz_info!(
            "TLS: minted a self-signed dev certificate (identity is ephemeral, regenerated every boot); \
             public PEM at {path}"
        );
    }
    let listener =
        TcpListener::bind(cli.listen).map_err(|e| format!("failed to bind TLS listener {}: {e}", cli.listen))?;
    // std hardcodes backlog 128 for TCP; a second `listen` rewrites
    // `sk_max_ack_backlog` in place, and a negative backlog means
    // `net.core.somaxconn` — what std itself passes for AF_UNIX on Linux, and
    // what the AF_UNIX listener therefore already gets.
    if unsafe { libc::listen(listener.as_raw_fd(), -1) } < 0 {
        return Err(format!(
            "failed to widen the TLS listener backlog: {}",
            std::io::Error::last_os_error()
        ));
    }
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("failed to set the TLS listener non-blocking: {e}"))?;
    // Propagated, not defaulted to `cli.listen`: with `--tls-listen …:0` the
    // requested address is a literal `:0`, and publishing that would hand every
    // client an unconnectable endpoint.
    let bound = listener
        .local_addr()
        .map_err(|e| format!("failed to read the bound TLS address: {e}"))?;
    gnitz_store::storage::publish_file_sync(data_dir, "tls_endpoint", format!("{bound}\n").as_bytes())
        .map_err(|e| format!("failed to publish {data_dir}/tls_endpoint: {e}"))?;
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
    let tl = TlsListener {
        cfg: config,
        hello_timeout: std::time::Duration::from_millis(gnitz_foundation::env::env_num(
            "GNITZ_TLS_HELLO_TIMEOUT_MS",
            (gnitz_wire::CONNECT_TIMEOUT * 3 / 2).as_millis() as u64,
        )),
        live: Budget::new(cli.max_conns as usize),
    };
    Ok((listener, tl))
}
