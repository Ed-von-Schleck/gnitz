//! `gnitz-fuzz`: the session fuzzer's command line.
//!
//! ```text
//! gnitz-fuzz run <input>                      a session, aborting on a finding: the AFL++ target
//! gnitz-fuzz replay <input>                   a session with every frame traced: triage
//! gnitz-fuzz import <recordings> <dir>        <dir>/seeds from recordings, and every
//!                                             target's starting inputs in <dir>/starts
//! ```
//!
//! `run` and `replay` read the seeds from `GNITZ_FUZZ_SEEDS` and boot the
//! server `GNITZ_SERVER_BIN` names.

use std::path::Path;
use std::process::ExitCode;

use gnitz_fuzz::input::Input;
use gnitz_fuzz::seed::{self, Seeds, SEEDS_ENV};
use gnitz_fuzz::session;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args[..] {
        ["run", input] => {
            // Every failure is a crash to the engine, a panic of this driver's
            // own included: an exit code is not one.
            let default = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                default(info);
                std::process::abort();
            }));
            if let Err(finding) = session_of(input, false) {
                eprintln!("finding: {finding}");
                std::process::abort();
            }
            ExitCode::SUCCESS
        }
        ["replay", input] => match session_of(input, true) {
            Ok(()) => {
                eprintln!("no finding");
                ExitCode::SUCCESS
            }
            Err(finding) => {
                eprintln!("finding: {finding}");
                ExitCode::FAILURE
            }
        },
        ["import", recordings, dir] => match import(Path::new(recordings), Path::new(dir)) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("import: {e}");
                ExitCode::FAILURE
            }
        },
        _ => {
            eprintln!("usage: gnitz-fuzz run <input> | replay <input> | import <recordings> <dir>");
            ExitCode::from(64)
        }
    }
}

/// The session the input file `path` names.
fn session_of(path: &str, trace: bool) -> Result<(), String> {
    let seeds = gnitz_foundation::env::env_path(SEEDS_ENV).ok_or(format!("{SEEDS_ENV} names no seed directory"))?;
    let seeds = Seeds::open(&seeds).map_err(|e| format!("{}: {e}", seeds.display()))?;
    let input = Input::decode(&std::fs::read(path).map_err(|e| format!("{path}: {e}"))?);
    let (seed, file) = seeds.pick(input.seed)?;
    if trace {
        eprintln!(
            "seed {} under {:?}, {} edits: {:?}",
            file.display(),
            input.boot,
            input.edits.len(),
            input.edits
        );
    }
    session::run(&input, &seed, trace)
}

/// Import `recordings` into `dir/seeds`, and write every target's starting
/// inputs into `dir/starts`.
fn import(recordings: &Path, dir: &Path) -> Result<(), String> {
    let seeds = dir.join("seeds");
    let imported = seed::import(recordings, &seeds).map_err(|e| e.to_string())?;
    let all = Seeds::open(&seeds).map_err(|e| e.to_string())?;
    seed::write_starts(&all, &dir.join("starts")).map_err(|e| e.to_string())?;
    eprintln!("{imported:?}; {} seeds in all", all.len());
    Ok(())
}
