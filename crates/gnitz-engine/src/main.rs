#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

#[macro_use]
mod foundation;
mod catalog;
mod expr;
mod ops;
mod query;
mod runtime;
mod schema;
mod storage;

#[cfg(test)]
mod test_rng;
#[cfg(test)]
mod test_support;

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
  --workers=N          Number of worker processes (default: 1, single-process)
  --log-level=LEVEL    Set log verbosity: quiet, normal, verbose (default: quiet)
  --tls-listen=IP:PORT Additionally listen for TLS 1.3 clients on this TCP
                       address (port 0 = ephemeral). The bound address is
                       written to <data_dir>/tls_endpoint.
                       WARNING: the TLS listener is fully UNAUTHENTICATED —
                       anyone who can connect gets full DDL/DML/scan access.
                       TLS provides confidentiality and server authentication
                       only; bind loopback unless the network is trusted.
  --tls-cert=PEM       Server certificate chain (requires --tls-key and
                       --tls-listen). Without cert+key a self-signed dev
                       certificate for localhost/127.0.0.1/::1 is minted and
                       its public PEM written to <data_dir>/tls_dev_cert.pem.
  --tls-key=PEM        Server private key (see --tls-cert)
  --help, -h           Show this help message and exit

Environment:
  GNITZ_LOG_LEVEL          Same as --log-level; CLI flag takes precedence
  GNITZ_CHECKPOINT_BYTES   SAL checkpoint threshold in bytes (default: 75% of SAL size)
";

fn parse_level(s: &str) -> u32 {
    match s.to_ascii_lowercase().as_str() {
        "quiet" | "0" => foundation::log::QUIET,
        "normal" | "1" => foundation::log::NORMAL,
        "verbose" | "debug" | "2" => foundation::log::DEBUG,
        _ => foundation::log::QUIET,
    }
}

/// Parse and validate `--workers=N` against the hard `MAX_WORKERS` limit.
/// Values above it cannot work — `worker_for_partition` divides by
/// `256 / num_workers` (zero when `num_workers > 256`, panicking) and the SAL
/// write path rejects groups wider than `MAX_WORKERS` — so reject them at the
/// boundary with a clear message instead of crashing later.
fn parse_workers(val: &str) -> Result<u32, String> {
    const MAX: u32 = runtime::MAX_WORKERS as u32;
    match val.parse::<u32>() {
        Ok(n) if (1..=MAX).contains(&n) => Ok(n),
        Ok(n) => Err(format!("--workers must be between 1 and {MAX} (got {n})")),
        Err(_) => Err("invalid --workers value".to_string()),
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut level = foundation::log::QUIET;
    if let Ok(env_level) = env::var("GNITZ_LOG_LEVEL") {
        level = parse_level(&env_level);
    }

    let mut data_dir = String::new();
    let mut socket_path = String::new();
    let mut num_workers: u32 = 1;
    let mut tls_listen: Option<std::net::SocketAddr> = None;
    let mut tls_cert: Option<String> = None;
    let mut tls_key: Option<String> = None;
    let mut pos = 0;

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--help" || arg == "-h" {
            eprint!("{HELP_TEXT}");
            process::exit(0);
        } else if let Some(val) = arg.strip_prefix("--log-level=") {
            level = parse_level(val);
        } else if let Some(val) = arg.strip_prefix("--workers=") {
            match parse_workers(val) {
                Ok(n) => num_workers = n,
                Err(e) => {
                    eprintln!("Error: {e}");
                    process::exit(1);
                }
            }
        } else if let Some(val) = arg.strip_prefix("--tls-listen=") {
            match val.parse::<std::net::SocketAddr>() {
                Ok(a) => tls_listen = Some(a),
                Err(_) => {
                    eprintln!("Error: invalid --tls-listen address {val:?} (expected IP:PORT)");
                    process::exit(1);
                }
            }
        } else if let Some(val) = arg.strip_prefix("--tls-cert=") {
            tls_cert = Some(val.to_string());
        } else if let Some(val) = arg.strip_prefix("--tls-key=") {
            tls_key = Some(val.to_string());
        } else if pos == 0 {
            data_dir = arg.clone();
            pos += 1;
        } else if pos == 1 {
            socket_path = arg.clone();
            pos += 1;
        }
        i += 1;
    }

    if pos < 2 {
        eprintln!("Error: missing required arguments");
        eprintln!("Try 'gnitz-server --help' for usage information");
        process::exit(1);
    }

    let tls_cli = match (tls_listen, tls_cert, tls_key) {
        (None, None, None) => None,
        (None, _, _) => {
            eprintln!("Error: --tls-cert/--tls-key require --tls-listen");
            process::exit(1);
        }
        (Some(listen), None, None) => Some(runtime::TlsCli { listen, cert_key: None }),
        (Some(listen), Some(cert), Some(key)) => Some(runtime::TlsCli {
            listen,
            cert_key: Some((cert, key)),
        }),
        (Some(_), _, _) => {
            eprintln!("Error: --tls-cert and --tls-key must be given together");
            process::exit(1);
        }
    };

    foundation::log::init(level, b"M");
    let rc = runtime::server_main(&data_dir, &socket_path, num_workers, level, tls_cli);
    process::exit(rc);
}

#[cfg(test)]
mod tests {
    use super::parse_workers;
    use super::runtime::MAX_WORKERS;

    #[test]
    fn parse_workers_accepts_valid_range() {
        assert_eq!(parse_workers("1"), Ok(1));
        assert_eq!(parse_workers(&MAX_WORKERS.to_string()), Ok(MAX_WORKERS as u32));
    }

    #[test]
    fn parse_workers_rejects_zero() {
        assert!(parse_workers("0").is_err());
    }

    #[test]
    fn parse_workers_rejects_above_max() {
        // Regression: values > MAX_WORKERS reached worker_for_partition and
        // divided by `256 / num_workers == 0` (panic for num_workers > 256).
        assert!(parse_workers(&(MAX_WORKERS + 1).to_string()).is_err());
        assert!(parse_workers("100000").is_err());
    }

    #[test]
    fn parse_workers_rejects_nonnumeric() {
        assert!(parse_workers("abc").is_err());
        assert!(parse_workers("").is_err());
    }
}
