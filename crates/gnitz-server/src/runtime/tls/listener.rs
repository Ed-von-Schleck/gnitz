//! The operator-facing half of TLS: what `--tls-listen` asks for, the bind
//! refusal that guards it, and the listener the executor accepts on. Its policy
//! — an unauthenticated public bind is impossible by accident — is the other
//! half of the rule `config` states, where the CA becomes an mTLS verifier.

use std::cell::Cell;
use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::rc::Rc;

use super::ConnCountGuard;

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
/// `pub(crate)`): the bound listener, the rustls config, and this listener's
/// admission policy — how many peers it admits, and how long an admitted one may
/// stay unauthenticated.
pub(crate) struct TlsListener {
    /// Owned rather than a raw fd, so the descriptor is closed on every path
    /// out of [`setup_tls_listener`] — the bound address is published across
    /// two fallible steps after the bind.
    listener: TcpListener,
    pub cfg: std::sync::Arc<rustls::ServerConfig>,
    pub max_conns: u32,
    /// How long an admitted peer may stay unauthenticated
    /// (`GNITZ_TLS_HELLO_TIMEOUT_MS`, default 15 000 ms): HELLO must arrive within
    /// this of accept. Set above the client's own connect deadline, so a client is
    /// reaped only once it has given up itself.
    pub pre_auth_window: std::time::Duration,
    /// Co-owned with every live [`ConnCountGuard`], which is what decrements it.
    live: Rc<Cell<u32>>,
}

impl TlsListener {
    /// The listen fd, for the reactor's multishot accept and the accept loop's
    /// which-listener test.
    pub(crate) fn fd(&self) -> i32 {
        self.listener.as_raw_fd()
    }

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
/// address to `<data_dir>/tls_endpoint`. Both files go out through the storage
/// layer's publisher, so each exists only with its complete content.
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
        gnitz_store::storage::publish_file_sync(data_dir, "tls_dev_cert.pem", &[pem.as_bytes()])
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
    gnitz_store::storage::publish_file_sync(data_dir, "tls_endpoint", &[format!("{bound}\n").as_bytes()])
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
    Ok(TlsListener {
        listener,
        cfg: config,
        max_conns: cli.max_conns,
        pre_auth_window: std::time::Duration::from_millis(gnitz_foundation::env::env_num(
            "GNITZ_TLS_HELLO_TIMEOUT_MS",
            15_000,
        )),
        live: Rc::new(Cell::new(0)),
    })
}
