//! The client listeners: the AF_UNIX socket every server has and the TCP socket
//! `--tls-listen` adds, and the files that describe the TCP one to its clients.

use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;

use crate::runtime::tls::TlsConfig;

/// `<data_dir>/<name>` files a TLS listener is described by: the bound `IP:PORT`,
/// and the public PEM of a minted dev certificate.
pub(crate) const TLS_ENDPOINT_FILE: &str = "tls_endpoint";
pub(crate) const TLS_DEV_CERT_FILE: &str = "tls_dev_cert.pem";

/// A bound, listening, non-blocking socket, and the rustls config its connections are
/// served under (`None`: the AF_UNIX socket, served in plaintext).
pub(crate) struct ClientListener {
    pub fd: OwnedFd,
    pub tls: Option<Arc<rustls::ServerConfig>>,
}

/// Remove an earlier boot's TLS files. Called with the data-directory lock held,
/// so the files it removes belong to no live server; from here until
/// [`bind_listeners`] writes them, their absence is the truth.
pub(crate) fn clear_published(data_dir: &str) -> Result<(), String> {
    for name in [TLS_ENDPOINT_FILE, TLS_DEV_CERT_FILE] {
        let path = format!("{data_dir}/{name}");
        match std::fs::remove_file(&path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(format!("failed to remove {path}: {e}"));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Bind every listener, then publish the TLS files. Called after the fork, so no
/// worker inherits a listening fd. The TCP socket binds first, so a boot that fails
/// the TCP bind never exposed the AF_UNIX socket. The files are written only once
/// both binds succeeded, the endpoint last.
pub(crate) fn bind_listeners(
    data_dir: &str,
    socket_path: &str,
    tls: Option<TlsConfig>,
) -> Result<Vec<ClientListener>, String> {
    let tcp = tls
        .map(|t| bind_tcp(t.listen).map(|(fd, bound)| (fd, bound, t)))
        .transpose()?;
    let mut listeners = vec![ClientListener { fd: bind_unix(socket_path)?, tls: None }];
    if let Some((fd, bound, t)) = tcp {
        if let Some(pem) = t.dev_pem {
            let path = format!("{data_dir}/{TLS_DEV_CERT_FILE}");
            std::fs::write(&path, pem).map_err(|e| format!("failed to publish {path}: {e}"))?;
            gnitz_info!(
                "TLS: minted a self-signed dev certificate (identity is ephemeral, regenerated every boot); \
                 public PEM at {path}"
            );
        }
        let path = format!("{data_dir}/{TLS_ENDPOINT_FILE}");
        std::fs::write(&path, format!("{bound}\n")).map_err(|e| format!("failed to publish {path}: {e}"))?;
        gnitz_info!("Listening on tls://{}", bound);
        listeners.push(ClientListener { fd, tls: Some(t.cfg) });
    }
    Ok(listeners)
}

/// Bind the TCP listener at `addr`, returning it with the address it bound.
fn bind_tcp(addr: SocketAddr) -> Result<(OwnedFd, SocketAddr), String> {
    use gnitz_foundation::posix_io::set_sockopt_int;

    let listener = TcpListener::bind(addr).map_err(|e| format!("failed to bind TLS listener {addr}: {e}"))?;
    let fd = listener.as_raw_fd();
    // Both are inherited by every accepted socket. Keepalive lets the kernel
    // reap a half-open peer that would otherwise park its recv forever.
    set_sockopt_int(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, 1);
    set_sockopt_int(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1);
    // A negative backlog re-listens at `net.core.somaxconn`.
    if unsafe { libc::listen(fd, -1) } < 0 {
        return Err(format!(
            "failed to widen the TLS listener backlog: {}",
            std::io::Error::last_os_error()
        ));
    }
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("failed to set the TLS listener non-blocking: {e}"))?;
    // Not `addr`, whose port may be 0.
    let bound = listener
        .local_addr()
        .map_err(|e| format!("failed to read the bound TLS address: {e}"))?;
    Ok((OwnedFd::from(listener), bound))
}

/// Bind the AF_UNIX listener at `path`, replacing only a stale socket: a
/// regular file there, or a socket something answers on, refuses the boot.
fn bind_unix(path: &str) -> Result<OwnedFd, String> {
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::net::{UnixListener, UnixStream};

    let listener = match UnixListener::bind(path) {
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            let is_socket = std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_socket());
            if !is_socket {
                return Err(format!("{path} exists and is not a socket"));
            }
            if UnixStream::connect(path).is_ok() {
                return Err(format!("{path} is served by a running server"));
            }
            // Left by a server that is gone.
            std::fs::remove_file(path).map_err(|e| format!("failed to remove stale socket {path}: {e}"))?;
            UnixListener::bind(path)
        }
        r => r,
    }
    .and_then(|l| {
        l.set_nonblocking(true)?;
        Ok(OwnedFd::from(l))
    })
    .map_err(|e| format!("failed to create server socket {path}: {e}"))?;
    gnitz_info!("Listening on {}", path);
    Ok(listener)
}

#[cfg(test)]
#[path = "tests/listen.rs"]
mod tests;
