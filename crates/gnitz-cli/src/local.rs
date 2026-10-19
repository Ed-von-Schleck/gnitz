//! The server a session given no target runs: a `gnitz-server` installed
//! beside the shell or on `PATH`, held for the session and stopped when it ends.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use gnitz_core::local_server::{self, LocalServer};

/// A [`LocalServer`] held for the session: stopped when the session ends, and
/// also when one of the signals it was held under ends the process, which runs
/// no destructor. So the server is gone, its throwaway data with it, by the
/// time the shell's exit is seen.
pub struct Held {
    server: Arc<Mutex<Option<LocalServer>>>,
    socket: PathBuf,
}

impl Held {
    /// Start the session's server, on `data` where it is given; an interactive
    /// session says where its data lives.
    pub fn start(data: Option<&Path>, interactive: bool) -> Result<Held, String> {
        let installed = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(Path::to_path_buf));
        let bin = local_server::find_server(installed.as_deref()).map_err(|e| not_installed(&e))?;
        let server = LocalServer::start(&bin, data, 1)?;
        if interactive {
            match data {
                Some(dir) => eprintln!("Started {} on {} for this session.", bin.display(), dir.display()),
                None => eprintln!(
                    "Started {} for this session; its data is deleted when the shell exits \
                     (`gnitz --data DIR` keeps it).",
                    bin.display()
                ),
            }
        }
        // An interactive session's Ctrl-C cancels a statement; a script's ends it.
        let signals: &[libc::c_int] = match interactive {
            true => &[libc::SIGTERM, libc::SIGHUP],
            false => &[libc::SIGTERM, libc::SIGHUP, libc::SIGINT],
        };
        let socket = server.socket().to_path_buf();
        let server = Arc::new(Mutex::new(Some(server)));
        let mut caught =
            signal_hook::iterator::Signals::new(signals).map_err(|e| format!("cannot handle signals: {e}"))?;
        let held = Arc::clone(&server);
        std::thread::spawn(move || {
            if let Some(signal) = caught.forever().next() {
                drop(held.lock().unwrap_or_else(|e| e.into_inner()).take());
                std::process::exit(128 + signal);
            }
        });
        Ok(Held { server, socket })
    }

    /// The Unix socket the server listens on.
    pub fn socket(&self) -> &Path {
        &self.socket
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        drop(self.server.lock().unwrap_or_else(|e| e.into_inner()).take());
    }
}

/// `why` no server could be found, with the ways on.
fn not_installed(why: &str) -> String {
    let elsewhere = match cfg!(target_os = "linux") {
        true => "",
        false => "; it runs on Linux, so elsewhere run it in a container and connect to it over TLS",
    };
    format!(
        "no server given, and {why}: connect to a server with `gnitz <socket path | tls://host:port>` or \
         GNITZ_TARGET, or install {} beside gnitz or on PATH{elsewhere}",
        local_server::SERVER
    )
}
