use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use clap::{CommandFactory, FromArgMatches, Parser};
use gnitz_cli::{Expanded, Flow, Format, Options, Settings, Shell};

mod local;

/// The interactive SQL shell for gnitz.
///
/// With no -c or -f, reads statements from the terminal, or from stdin when it
/// is not one.
#[derive(Parser)]
#[command(name = "gnitz", version)]
struct Cli {
    /// The server: its Unix socket path, or a tls:// target. Without one,
    /// GNITZ_TARGET names it; with neither, the shell runs a gnitz-server
    /// installed beside it or on PATH for the session.
    target: Option<String>,

    /// Run the session's own server, whatever GNITZ_TARGET names, and keep
    /// its data in DIR, where the next session given DIR finds it, instead of
    /// deleting it when the shell exits.
    #[arg(long, value_name = "DIR", conflicts_with = "target")]
    data: Option<PathBuf>,

    /// The schema unqualified names resolve in.
    #[arg(short, long, default_value = gnitz_wire::PUBLIC_SCHEMA, value_parser = schema_name)]
    schema: String,

    /// Run COMMAND (SQL or a meta-command) and exit. Repeatable.
    #[arg(short, long = "command", value_name = "COMMAND")]
    command: Vec<String>,

    /// Run the commands in FILE and exit. Repeatable; `-` is stdin.
    #[arg(short, long = "file", value_name = "FILE")]
    file: Vec<String>,

    /// The output format.
    #[arg(short = 'F', long, value_enum, default_value_t = Format::Aligned)]
    format: Format,

    /// Print one field per line.
    #[arg(short = 'x', long)]
    expanded: bool,

    /// Run every command of a script, even after one fails.
    #[arg(long)]
    continue_on_error: bool,

    /// vi key bindings instead of emacs ones.
    #[arg(long)]
    vi: bool,

    /// Never style output.
    #[arg(long)]
    no_color: bool,
}

/// `-s`'s value, read as SQL reads a schema name.
fn schema_name(text: &str) -> Result<String, String> {
    gnitz_sql::parse_schema_name(text).ok_or_else(|| format!("\"{text}\" is not a schema name"))
}

/// One input of a script run, in command-line order.
enum Input<'a> {
    Command(&'a str),
    File(&'a str),
}

fn main() -> ExitCode {
    let matches = Cli::command().get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());
    let mut inputs: Vec<(usize, Input<'_>)> = matches
        .indices_of("command")
        .into_iter()
        .flatten()
        .zip(cli.command.iter().map(|c| Input::Command(c)))
        .chain(
            matches
                .indices_of("file")
                .into_iter()
                .flatten()
                .zip(cli.file.iter().map(|f| Input::File(f))),
        )
        .collect();
    inputs.sort_by_key(|&(at, _)| at);
    let scripted = !inputs.is_empty();
    let interactive = !scripted && std::io::stdin().is_terminal();
    let color = interactive
        && !cli.no_color
        && std::io::stdout().is_terminal()
        && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty());

    // In a session Ctrl-C cancels the wait in progress; a script dies by it.
    let interrupt = interactive.then(|| {
        let flag = Arc::new(AtomicBool::new(false));
        if let Err(e) = signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&flag)) {
            eprintln!("gnitz: cannot handle SIGINT: {e}");
        }
        flag
    });

    // An empty GNITZ_TARGET is none, and --data asks for a server of the
    // session's own whatever the environment names.
    let given = match (cli.target.clone(), &cli.data) {
        (Some(target), _) => Some(target),
        (None, Some(_)) => None,
        (None, None) => match std::env::var("GNITZ_TARGET") {
            Ok(target) => (!target.is_empty()).then_some(target),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                eprintln!("gnitz: GNITZ_TARGET is not UTF-8");
                return ExitCode::from(2);
            }
        },
    };
    // Declared ahead of the shell, so the shell's connection closes before the
    // server it dials is stopped.
    let (target, _local) = match given {
        Some(target) => (target, None),
        None => match local::Held::start(cli.data.as_deref(), interactive) {
            Ok(held) => (held.socket().to_string_lossy().into_owned(), Some(held)),
            Err(e) => {
                eprintln!("gnitz: {e}");
                return ExitCode::from(2);
            }
        },
    };

    let settings = Settings {
        format: cli.format,
        expanded: if cli.expanded { Expanded::On } else { Expanded::Off },
        ..Settings::default()
    };
    let options = Options {
        target: target.clone(),
        schema: cli.schema.clone(),
        settings,
        color,
        interactive,
        stop_on_error: !cli.continue_on_error,
        interrupt,
    };
    let mut shell = match Shell::connect(options) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("gnitz: could not connect to {target}: {e}");
            return ExitCode::from(2);
        }
    };

    if interactive {
        return gnitz_cli::repl(&mut shell, cli.vi);
    }
    if !scripted {
        inputs.push((0, Input::File("-")));
    }
    for (_, input) in inputs {
        let run = match input {
            Input::Command(c) => shell.run_script(c),
            Input::File(f) => shell.run_file(f),
        };
        match run {
            Ok(Flow::Quit) => break,
            Err(_) if !cli.continue_on_error => break,
            Ok(Flow::Continue) | Err(_) => {}
        }
    }
    match shell.failed() {
        true => ExitCode::FAILURE,
        false => ExitCode::SUCCESS,
    }
}
