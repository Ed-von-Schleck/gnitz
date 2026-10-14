//! The client listeners: the AF_UNIX socket every server has and the TCP socket
//! `--tls-listen` adds, and the file that tells clients where the TCP one bound.

use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;

use crate::runtime::tls::TlsListener;

/// `<data_dir>/<name>` file holding the TLS listener's bound `IP:PORT`.
const TLS_ENDPOINT_FILE: &str = "tls_endpoint";

/// A bound, listening socket, and what its connections are served under
/// (`None`: the AF_UNIX socket, served in plaintext and asked for no token).
pub(crate) struct ClientListener {
    pub fd: OwnedFd,
    pub tls: Option<Rc<TlsListener>>,
}

/// Remove an earlier boot's endpoint file. Called with the data-directory lock
/// held, so the file belongs to no live server; from here until
/// [`bind_listeners`] writes it, its absence is the truth.
pub(crate) fn clear_published(data_dir: &str) -> Result<(), String> {
    let path = format!("{data_dir}/{TLS_ENDPOINT_FILE}");
    match std::fs::remove_file(&path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(format!("failed to remove {path}: {e}")),
        _ => Ok(()),
    }
}

/// Bind every listener, the AF_UNIX socket last, then publish the endpoint.
/// Called after the fork, so no worker inherits a listening fd.
pub(crate) fn bind_listeners(
    data_dir: &str,
    socket_path: &str,
    tls: Option<TlsListener>,
) -> Result<Vec<ClientListener>, String> {
    let tcp = tls
        .map(|tls| bind_tcp(tls.listen()).map(|(fd, bound)| (fd, bound, tls)))
        .transpose()?;
    let mut listeners = vec![ClientListener { fd: bind_unix(socket_path)?, tls: None }];
    if let Some((fd, bound, tls)) = tcp {
        let path = format!("{data_dir}/{TLS_ENDPOINT_FILE}");
        std::fs::write(&path, format!("{bound}\n")).map_err(|e| format!("failed to publish {path}: {e}"))?;
        gnitz_note!("Listening on tls://{bound}: {}", tls.describe());
        listeners.push(ClientListener { fd, tls: Some(Rc::new(tls)) });
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
    for (level, opt) in [
        (libc::IPPROTO_TCP, libc::TCP_NODELAY),
        (libc::SOL_SOCKET, libc::SO_KEEPALIVE),
    ] {
        set_sockopt_int(fd, level, opt, 1).map_err(|e| format!("failed to configure the TLS listener: {e}"))?;
    }
    // A negative backlog re-listens at `net.core.somaxconn`.
    if unsafe { libc::listen(fd, -1) } < 0 {
        return Err(format!(
            "failed to widen the TLS listener backlog: {}",
            std::io::Error::last_os_error()
        ));
    }
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
    .map(OwnedFd::from)
    .map_err(|e| format!("failed to create server socket {path}: {e}"))?;
    gnitz_info!("Listening on {}", path);
    Ok(listener)
}

#[cfg(test)]
#[path = "tests/listen.rs"]
mod tests;
