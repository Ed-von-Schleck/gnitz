use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use clap::Parser;
use gnitz_cli::editor::{self, ShellPrompt};
use gnitz_cli::render::{Expanded, Format, Settings};
use gnitz_cli::shell::{Flow, Options, Shell};
use gnitz_core::RelName;
use reedline::{EditCommand, Signal};

mod local;

/// The interactive SQL shell for gnitz.
///
/// With no -c or -f, reads statements from the terminal, or from stdin when it
/// is not one.
#[derive(Parser)]
#[command(name = "gnitz", version)]
struct Cli {
    /// The server: its Unix socket path, or a tls:// target. Without one, the
    /// shell runs a gnitz-server installed beside it or on PATH for the
    /// session.
    #[arg(env = "GNITZ_TARGET")]
    target: Option<String>,

    /// With no target: keep the session's data in DIR, where the next session
    /// given DIR finds it, instead of deleting it when the shell exits.
    #[arg(long, value_name = "DIR", conflicts_with = "target")]
    data: Option<PathBuf>,

    /// The schema unqualified names resolve in.
    #[arg(short, long, default_value = gnitz_wire::PUBLIC_SCHEMA, value_parser = RelName::schema_key)]
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

fn main() -> ExitCode {
    let cli = Cli::parse();
    let scripted = !cli.command.is_empty() || !cli.file.is_empty();
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

    // Declared ahead of the shell, so the shell's connection closes before the
    // server it dials is stopped.
    let (target, _local) = match cli.target.clone() {
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
        color,
        ..Settings::default()
    };
    let options = Options {
        target: target.clone(),
        schema: cli.schema.clone(),
        settings,
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
        return repl(&mut shell, cli.vi);
    }

    // `-c` and `-f` in order, or stdin when there are none.
    let stdin = ["-".to_string()];
    let files = match scripted {
        true => &cli.file[..],
        false => &stdin[..],
    };
    let inputs = cli
        .command
        .iter()
        .map(|c| (true, c))
        .chain(files.iter().map(|f| (false, f)));
    let mut ok = true;
    for (is_command, input) in inputs {
        let run = match is_command {
            true => shell.run_script(input),
            false => shell.run_file(input),
        };
        match run {
            Ok(Flow::Quit) => break,
            Ok(Flow::Continue) => {}
            Err(_) => {
                ok = false;
                if !cli.continue_on_error {
                    break;
                }
            }
        }
    }
    match ok {
        true => ExitCode::SUCCESS,
        false => ExitCode::FAILURE,
    }
}

fn repl(shell: &mut Shell, vi: bool) -> ExitCode {
    let Ok(buffer) = shell.scratch().map(|dir| dir.join("buffer.sql")) else {
        return ExitCode::FAILURE;
    };
    let mut line_editor = editor::build(vi, shell.settings.color, buffer);
    println!(
        "gnitz {} — connected to {}. Type \\? for help, \\q to quit.",
        gnitz_wire::RELEASE,
        shell.target()
    );
    loop {
        // Completion offers what the catalog holds now, whichever session
        // changed it; a failed read keeps the previous completer.
        if let Some(ctx) = shell.completion() {
            line_editor = line_editor.with_completer(Box::new(ctx));
        }
        if let Some(text) = shell.prefill.take() {
            line_editor.run_edit_commands(&[EditCommand::InsertString(text)]);
        }
        let prompt = ShellPrompt {
            schema: shell.schema().to_string(),
            in_transaction: shell.in_transaction(),
        };
        match line_editor.read_line(&prompt) {
            Ok(Signal::Success(buffer)) => {
                if let Ok(Flow::Quit) = shell.run_script(&buffer) {
                    break;
                }
            }
            Ok(Signal::CtrlD) => break,
            Ok(_) => continue,
            Err(e) => {
                eprintln!("gnitz: terminal error: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}
