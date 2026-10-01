//! GnitzDB's server binary: the DBSP layer — the circuit compiler, the bytecode
//! VM, epoch execution and the system-table catalog — plus the process model,
//! wire protocol and reactor that drive it.
//!
//! Two crates sit beneath it. `gnitz-zset` is the kernel: the schema, the
//! columnar batch and its cursor, and every operator a circuit dispatches to.
//! `gnitz-store` keeps Z-sets: the LSM, the relation registry and the `ReadSpec`
//! executor. That split is what makes "a client links no compiler, no VM and no
//! catalog" a fact of the crate graph rather than a convention: a host holding a
//! mirrored view links those two, and nothing in this crate is linkable at all.
//!
//! The three module roots below are the layer ladder: `runtime` over `catalog`
//! over `query` over everything `gnitz-store` and `gnitz-zset` publish. The submodules under
//! each root are private; what a root re-exports is what it publishes to the
//! rungs above it, scoped `pub(in crate::<root>)` when it publishes to none.
//! There is no crate-root re-export façade: a type's rung is part of what its
//! path says.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

// Crate-wide scope for `gnitz_warn!` and its siblings; a plain `use` would
// reach this module only.
#[macro_use]
extern crate gnitz_foundation;

mod catalog;
mod query;
mod runtime;

use gnitz_foundation::log::Level;
use std::env;
use std::process;

const HELP_TEXT: &str = "\
gnitz-server — GnitzDB database server

Usage:
  gnitz-server [OPTIONS] <data_dir> <socket_path>

Arguments:
  <data_dir>      Path to the database data directory (created if absent)
  <socket_path>   Path for the Unix domain socket to listen on

Options:
  --workers=N          Number of worker processes (default: 1)
  --log-level=LEVEL    Set log verbosity: quiet, normal, verbose (default: quiet)
  --tls-listen=IP:PORT Additionally listen for TLS 1.3 clients on this TCP
                       address (port 0 = ephemeral). The bound address is
                       written to <data_dir>/tls_endpoint. A NON-LOOPBACK bind
                       REQUIRES --tls-client-ca or --allow-unauthenticated —
                       boot aborts otherwise. A loopback bind is turnkey.
  --tls-cert=PEM       Server certificate chain (requires --tls-key and
                       --tls-listen). Without cert+key a self-signed dev
                       certificate for localhost/127.0.0.1/::1 is minted and
                       its public PEM written to <data_dir>/tls_dev_cert.pem.
  --tls-key=PEM        Server private key (see --tls-cert)
  --tls-client-ca=PEM  Enable REQUIRED mTLS: clients must present an X.509
                       certificate chaining to this CA (chain) to complete the
                       handshake. Use a DEDICATED client-auth CA — any leaf the
                       CA signs authenticates (a leaf with no extended-key-usage
                       extension authenticates too). No CRL/OCSP: revoke by rotating the CA,
                       which invalidates all clients at once.
  --allow-unauthenticated
                       Escape hatch: permit a non-loopback bind with NO client
                       authentication — anyone who can reach the port gets full
                       DDL/DML/scan access. Prefer --tls-client-ca. Note: even a
                       loopback bind trusts every local UID (like the always-on
                       AF_UNIX socket), gated only by network reachability and
                       filesystem permissions.
  --help, -h           Show this help message and exit

Environment:
  GNITZ_LOG_LEVEL          Same as --log-level; CLI flag takes precedence
  GNITZ_CHECKPOINT_BYTES   SAL checkpoint threshold in bytes (default: 75% of SAL size)
  GNITZ_CPU_AFFINITY       Pin the master and each worker to CPUs (default: on; 0 disables).
                           Set to 0 when servers share a host without per-server cpusets.
  GNITZ_MAX_CONNS          Cap on open client connections, every transport together
                           (default: 256). A connection accepted past it is closed at once.
  GNITZ_HELLO_TIMEOUT_MS   How long a connection may take from accept to its HELLO, TLS
                           handshake included, before it is closed (default: longer
                           than a client waits for its own connect).
";

fn parse_level(s: &str) -> Result<Level, String> {
    match s {
        "quiet" => Ok(Level::Quiet),
        "normal" => Ok(Level::Normal),
        "verbose" | "debug" => Ok(Level::Debug),
        _ => Err(format!("invalid log level {s:?} (expected quiet, normal or verbose)")),
    }
}

/// Parse `--workers=N` into `1..=MAX_WORKERS`.
fn parse_workers(val: &str) -> Result<u32, String> {
    const MAX: u32 = runtime::MAX_WORKERS as u32;
    val.parse()
        .ok()
        .filter(|n| (1..=MAX).contains(n))
        .ok_or_else(|| format!("--workers must be between 1 and {MAX} (got {val:?})"))
}

/// What argv asks for: each flag parsed, and the TLS flags checked against each
/// other. `tls` is `None` when no TLS listener was asked for.
struct Args {
    data_dir: String,
    socket_path: String,
    workers: u32,
    level: Level,
    tls: Option<runtime::TlsArgs>,
}

/// Parse argv (program name excluded), with `GNITZ_LOG_LEVEL` as `env_level`.
fn parse_args(args: &[String], env_level: Option<&str>) -> Result<Args, String> {
    let mut level = env_level.map(parse_level).transpose()?.unwrap_or(Level::Quiet);
    let mut workers = 1;
    let (mut tls_listen, mut cert, mut key, mut client_ca) = (None, None, None, None);
    let mut allow_unauthenticated = false;
    let mut positional: Vec<&String> = Vec::new();
    for arg in args {
        if let Some(val) = arg.strip_prefix("--log-level=") {
            level = parse_level(val)?;
        } else if let Some(val) = arg.strip_prefix("--workers=") {
            workers = parse_workers(val)?;
        } else if let Some(val) = arg.strip_prefix("--tls-listen=") {
            let addr = val
                .parse()
                .map_err(|_| format!("invalid --tls-listen address {val:?} (expected IP:PORT)"))?;
            tls_listen = Some(addr);
        } else if let Some(val) = arg.strip_prefix("--tls-cert=") {
            cert = Some(val.to_string());
        } else if let Some(val) = arg.strip_prefix("--tls-key=") {
            key = Some(val.to_string());
        } else if let Some(val) = arg.strip_prefix("--tls-client-ca=") {
            client_ca = Some(val.to_string());
        } else if arg == "--allow-unauthenticated" {
            allow_unauthenticated = true;
        } else if arg.starts_with('-') {
            return Err(format!("unknown option {arg:?}"));
        } else {
            positional.push(arg);
        }
    }
    let [data_dir, socket_path] = positional[..] else {
        return Err(format!("expected <data_dir> <socket_path>, got {positional:?}"));
    };
    let tls = match tls_listen {
        Some(listen) => Some(runtime::TlsArgs {
            listen,
            cert_key: match (cert, key) {
                (None, None) => None,
                (Some(c), Some(k)) => Some((c, k)),
                _ => return Err("--tls-cert and --tls-key must be given together".to_string()),
            },
            client_ca,
            allow_unauthenticated,
        }),
        None if cert.is_some() || key.is_some() || client_ca.is_some() || allow_unauthenticated => {
            return Err(
                "--tls-cert, --tls-key, --tls-client-ca and --allow-unauthenticated require --tls-listen".to_string(),
            );
        }
        None => None,
    };
    Ok(Args {
        data_dir: data_dir.clone(),
        socket_path: socket_path.clone(),
        workers,
        level,
        tls,
    })
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print!("{HELP_TEXT}");
        process::exit(0);
    }
    let env_level = env::var("GNITZ_LOG_LEVEL").ok();
    let args = parse_args(&args, env_level.as_deref()).unwrap_or_else(|e| {
        eprintln!("Error: {e}");
        eprintln!("Try 'gnitz-server --help' for usage information");
        process::exit(1);
    });
    gnitz_foundation::log::init(args.level, gnitz_foundation::log::Tag::Master);
    process::exit(runtime::server_main(
        &args.data_dir,
        &args.socket_path,
        args.workers,
        args.tls,
    ));
}

#[cfg(test)]
mod test_support;

/// Tests no single module owns: the rung guard over `catalog/`, `query/` and
/// `runtime/`.
#[cfg(test)]
#[path = "tests/rungs.rs"]
mod rung_tests;

#[cfg(test)]
#[path = "tests/main.rs"]
mod tests;
