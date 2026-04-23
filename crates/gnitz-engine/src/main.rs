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
  --workers=N        Number of worker processes (default: 1, single-process)
  --log-level=LEVEL  Set log verbosity: quiet, normal, verbose (default: quiet)
  --help, -h         Show this help message and exit

Environment:
  GNITZ_LOG_LEVEL          Same as --log-level; CLI flag takes precedence
  GNITZ_CHECKPOINT_BYTES   SAL checkpoint threshold in bytes (default: 75% of SAL size)
";

fn parse_level(s: &str) -> u32 {
    match s.to_ascii_lowercase().as_str() {
        "quiet" | "0" => gnitz_engine::log::QUIET,
        "normal" | "1" => gnitz_engine::log::NORMAL,
        "verbose" | "debug" | "2" => gnitz_engine::log::DEBUG,
        _ => gnitz_engine::log::QUIET,
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut level = gnitz_engine::log::QUIET;
    if let Ok(env_level) = env::var("GNITZ_LOG_LEVEL") {
        level = parse_level(&env_level);
    }

    let mut data_dir = String::new();
    let mut socket_path = String::new();
    let mut num_workers: u32 = 1;
    let mut pos = 0;

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--help" || arg == "-h" {
            eprint!("{}", HELP_TEXT);
            process::exit(0);
        } else if let Some(val) = arg.strip_prefix("--log-level=") {
            level = parse_level(val);
        } else if let Some(val) = arg.strip_prefix("--workers=") {
            match val.parse::<u32>() {
                Ok(n) if n >= 1 => num_workers = n,
                _ => {
                    eprintln!("Error: invalid --workers value");
                    process::exit(1);
                }
            }
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

    gnitz_engine::log::init(level, b"M");
    let rc = gnitz_engine::runtime::server_main(&data_dir, &socket_path, num_workers, level);
    process::exit(rc);
}
