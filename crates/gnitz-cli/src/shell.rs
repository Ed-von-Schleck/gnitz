//! The shell: one connection, the settings, and running what is typed or read
//! — statements through the SQL front end, meta-commands through [`crate::meta`].

use std::fmt::Display;
use std::fs::File;
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use gnitz_core::{BlockingHost, Catalog, ClientError, GnitzClient, block_on};
use gnitz_foundation::posix_io::ScratchDir;
use gnitz_sql::{GnitzSqlError, SqlResult};
use nu_ansi_term::{Color, Style};

use crate::lex::{self, Piece, Splitter};
use crate::render::{Target, Weights, paint, render};
use crate::settings::{Format, Settings};
use crate::value::Rows;

/// Whether the shell goes on after a piece of input.
#[derive(Debug, PartialEq, Eq)]
pub enum Flow {
    Continue,
    Quit,
}

/// A command failed; its error has been reported.
#[derive(Debug)]
pub struct Failed;

/// Where results go.
enum Sink {
    Stdout,
    File(File),
}

/// The session's options: where it is connected, how it reads names and
/// prints results, and how it runs input. `\c`, `\use` and `\pset` change
/// them as it goes.
pub struct Options {
    pub target: String,
    /// The schema unqualified names resolve in.
    pub schema: String,
    pub settings: Settings,
    /// Whether the session styles what it prints to the terminal.
    pub color: bool,
    pub interactive: bool,
    /// Whether a failed statement ends a script.
    pub stop_on_error: bool,
    /// Readable to cancel the wait in progress, which reads it empty; `None`
    /// where nothing cancels one.
    pub interrupt: Option<UnixStream>,
}

pub struct Shell {
    pub(crate) client: GnitzClient,
    pub(crate) options: Options,
    sink: Sink,
    /// Whether a command has failed, for the exit status of a script.
    failed: bool,
    /// The last statement run, which `\e` opens.
    pub(crate) last_statement: String,
    /// Text for the next prompt's buffer, from `\e`.
    pub(crate) prefill: Option<String>,
    /// The line editor's session's files, `\e`'s among them; `None` outside
    /// one. Removed when the shell is dropped.
    pub(crate) scratch: Option<ScratchDir>,
}

/// A client of `target` whose waits give up once `interrupt` is readable.
fn dial(target: &str, interrupt: Option<&UnixStream>) -> Result<GnitzClient, ClientError> {
    let host = match interrupt {
        Some(cancel) => BlockingHost::cancelled_by(cancel.try_clone()?)?,
        None => BlockingHost::default(),
    };
    GnitzClient::connect_on(target, Box::new(host))
}

impl Shell {
    pub fn connect(options: Options) -> Result<Shell, ClientError> {
        Ok(Shell {
            client: dial(&options.target, options.interrupt.as_ref())?,
            options,
            sink: Sink::Stdout,
            failed: false,
            last_statement: String::new(),
            prefill: None,
            scratch: None,
        })
    }

    /// Whether any command of the session has failed.
    pub fn failed(&self) -> bool {
        self.failed
    }

    /// Clear an interrupt that arrived before the wait it would cancel.
    pub(crate) fn clear_interrupt(&self) {
        if let Some(cancel) = &self.options.interrupt {
            gnitz_core::drain_cancel(cancel);
        }
    }

    /// The catalog as it stands now, for a command that answers from it.
    pub(crate) fn catalog(&mut self) -> Result<Catalog, Failed> {
        block_on(self.client.catalog()).or_else(|e| self.fail(e))
    }

    /// Dial `target` in place of the current connection, whose open
    /// transaction, if any, is lost.
    pub(crate) fn reconnect(&mut self, target: &str) -> Result<(), Failed> {
        let client = dial(target, self.options.interrupt.as_ref())
            .or_else(|e| self.fail(format!("could not connect to {target}: {e}")))?;
        if self.client.txn_active() {
            self.note("the open transaction was lost");
        }
        self.client = client;
        self.options.target = target.to_string();
        Ok(())
    }

    // ── Running input ─────────────────────────────────────────────────────

    pub fn run_script(&mut self, text: &str) -> Result<Flow, Failed> {
        self.run_input(text.lines().map(|l| Ok(l.to_string())))
    }

    /// Run the commands in the file at `path`; `-` is stdin.
    pub fn run_file(&mut self, path: &str) -> Result<Flow, Failed> {
        if path == "-" {
            return self.run_input(stdin_lines());
        }
        let file = File::open(path).or_else(|e| self.fail(format!("{path}: {e}")))?;
        self.run_input(BufReader::new(file).lines())
    }

    /// Run a whole script, each piece as soon as it is read: every statement
    /// and meta-command in it, and SQL left without a closing `;` at its end.
    /// `Err` when a failure stopped it.
    fn run_input(&mut self, lines: impl Iterator<Item = io::Result<String>>) -> Result<Flow, Failed> {
        let mut splitter = Splitter::default();
        for line in lines.map(Some).chain([None]) {
            let pieces = match line {
                Some(Ok(line)) => splitter.push_line(&line),
                Some(Err(e)) => return self.fail(format!("reading input: {e}")),
                None => splitter.finish(),
            };
            for piece in pieces {
                match self.run_piece(piece) {
                    Ok(Flow::Quit) => return Ok(Flow::Quit),
                    Err(Failed) if self.options.stop_on_error => return Err(Failed),
                    Ok(Flow::Continue) | Err(Failed) => {}
                }
            }
        }
        Ok(Flow::Continue)
    }

    fn run_piece(&mut self, piece: Piece) -> Result<Flow, Failed> {
        self.clear_interrupt();
        let r = match piece {
            Piece::Meta(line) => self.meta(&line),
            Piece::Sql(stmt) if lex::is_exit_word(&stmt) => Ok(Flow::Quit),
            Piece::Sql(stmt) => self.run_statement(&stmt).map(|()| Flow::Continue),
        };
        if r.is_err() && self.client.is_closed() {
            let target = self.options.target.clone();
            if self.reconnect(&target).is_ok() {
                self.note("reconnected");
            }
        }
        r
    }

    fn run_statement(&mut self, stmt: &str) -> Result<(), Failed> {
        self.last_statement = format!("{stmt};");
        let started = Instant::now();
        let result = block_on(gnitz_sql::execute(&mut self.client, &self.options.schema, stmt));
        let elapsed = started.elapsed();
        let outcome = match result {
            Ok(results) => {
                for r in results {
                    self.show(r);
                }
                Ok(())
            }
            Err(e) => {
                let r = self.fail(&e);
                if let GnitzSqlError::Client(ClientError::Interrupted(_)) = e {
                    self.note("the statement may still complete on the server");
                }
                r
            }
        };
        if self.options.settings.timing {
            self.status(&format_elapsed(elapsed));
        }
        outcome
    }

    fn show(&mut self, result: SqlResult) {
        match result {
            SqlResult::Rows(reply) => self.print_rows(&Rows::decode(&reply)),
            SqlResult::Ddl { tag } => self.status(tag),
            SqlResult::RowsAffected { tag, count } => self.status(&format!("{tag} {count}")),
            SqlResult::TransactionStarted => self.status("BEGIN"),
            SqlResult::TransactionCommitted { .. } => self.status("COMMIT"),
            SqlResult::TransactionRolledBack => self.status("ROLLBACK"),
        }
    }

    /// Rows, as SQL reads a bag where every weight is positive, else with
    /// their weights shown. Rows too long or wide for the terminal go to the
    /// pager where one is enabled; otherwise an aligned table wraps its cells.
    pub(crate) fn print_rows(&mut self, rows: &Rows) {
        let weights = match rows.is_bag() {
            true => Weights::Repeat,
            false => Weights::Column,
        };
        let settings = &self.options.settings;
        // The terminal an aligned result of a session may page on; elsewhere a
        // table wraps its cells to fit from the start.
        let pager = self.options.interactive && settings.pager && settings.format == Format::Aligned;
        let paging = self.terminal_size().filter(|_| pager);
        let target = self.target_for(paging.is_none());
        let text = render(rows, weights, settings, target);
        if fits(&text, paging) {
            self.write_result(&text);
        } else if !paged(&text) {
            let wrapped = render(rows, weights, &self.options.settings, Target { wrap: true, ..target });
            self.write_result(&wrapped);
        }
    }

    // ── Output ─────────────────────────────────────────────────────────────

    /// The terminal's columns and rows, where results go to one.
    fn terminal_size(&self) -> Option<(u16, u16)> {
        match self.sink {
            Sink::Stdout => console::Term::stdout().size_checked().map(|(rows, cols)| (cols, rows)),
            Sink::File(_) => None,
        }
    }

    /// Where results go now: the terminal's width and the session's styles
    /// where that is the terminal, else plain text. An aligned table wraps
    /// its cells to the width when `wrap`.
    pub(crate) fn target_for(&self, wrap: bool) -> Target {
        let width = self.terminal_size().map(|(w, _)| w);
        Target {
            width,
            wrap,
            color: self.options.color && width.is_some(),
            header: true,
        }
    }

    /// Write result text where results go.
    pub(crate) fn write_result(&mut self, text: &str) {
        // A reader gone from stdout, or a full disk, ends no session.
        let _ = match &mut self.sink {
            Sink::Stdout => {
                let mut out = io::stdout().lock();
                out.write_all(text.as_bytes()).and_then(|()| out.flush())
            }
            Sink::File(f) => f.write_all(text.as_bytes()),
        };
    }

    /// Send results to the file at `path`, created afresh, or to stdout where
    /// `path` is empty.
    pub(crate) fn output_to(&mut self, path: &str) -> Result<(), Failed> {
        self.sink = match path {
            "" => Sink::Stdout,
            path => Sink::File(File::create(path).or_else(|e| self.fail(format!("{path}: {e}")))?),
        };
        Ok(())
    }

    /// A command's status line: its tag, a timing, a setting changed. On the
    /// terminal in a session; on stderr from a script, so stdout carries only
    /// results.
    pub(crate) fn status(&self, msg: &str) {
        if msg.is_empty() {
            return;
        }
        // A reader gone from stdout ends no session.
        let _ = match self.options.interactive {
            true => writeln!(
                io::stdout(),
                "{}",
                paint(self.options.color, Style::new().dimmed(), msg)
            ),
            false => writeln!(io::stderr(), "{msg}"),
        };
    }

    /// A hint after an error.
    pub(crate) fn note(&self, msg: &str) {
        let style = Style::new().fg(Color::Yellow);
        eprintln!("{}", paint(self.stderr_color(), style, &format!("NOTE: {msg}")));
    }

    /// Report an error; the command it ended has failed.
    pub(crate) fn fail<T>(&mut self, msg: impl Display) -> Result<T, Failed> {
        self.failed = true;
        let style = Style::new().fg(Color::Red).bold();
        eprintln!("{} {msg}", paint(self.stderr_color(), style, "ERROR:"));
        Err(Failed)
    }

    /// Whether what goes to stderr is styled: in a styled session, where
    /// stderr is a terminal too.
    fn stderr_color(&self) -> bool {
        self.options.color && io::stderr().is_terminal()
    }
}

/// Stdin's lines, each read under its own lock, so a `\i -` in a script read
/// from stdin reads on from where the script stopped.
fn stdin_lines() -> impl Iterator<Item = io::Result<String>> {
    std::iter::from_fn(|| {
        let mut line = String::new();
        match io::stdin().read_line(&mut line) {
            Ok(0) => None,
            Ok(_) => {
                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') {
                        line.pop();
                    }
                }
                Some(Ok(line))
            }
            Err(e) => Some(Err(e)),
        }
    })
}

/// Whether `text` fits a terminal of `size` (columns, rows) without the
/// pager; any text fits where there is none.
fn fits(text: &str, size: Option<(u16, u16)>) -> bool {
    let Some((w, h)) = size else {
        return true;
    };
    text.lines().count() < usize::from(h).saturating_sub(1)
        && text.lines().all(|l| console::measure_text_width(l) <= usize::from(w))
}

/// The command the first of `vars` set to more than whitespace names, else
/// `default`.
pub(crate) fn command_from_env(vars: &[&str], default: &str) -> String {
    vars.iter()
        .find_map(|v| std::env::var(v).ok().filter(|c| !c.trim().is_empty()))
        .unwrap_or_else(|| default.to_string())
}

/// Show `text` in `$PAGER`, else `less`; whether it started.
fn paged(text: &str) -> bool {
    let Ok(mut child) = Command::new("/bin/sh")
        .arg("-c")
        .arg(command_from_env(&["PAGER"], "less -SRFX"))
        .stdin(Stdio::piped())
        .spawn()
    else {
        return false;
    };
    if let Some(mut stdin) = child.stdin.take() {
        // A pager quit before reading everything closes the pipe; that is
        // the user being done, not an error.
        let _ = stdin.write_all(text.as_bytes());
    }
    let _ = child.wait();
    true
}

fn format_elapsed(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    match ms < 1000.0 {
        true => format!("Time: {ms:.3} ms"),
        false => format!("Time: {ms:.3} ms ({:.3} s)", d.as_secs_f64()),
    }
}

#[cfg(test)]
#[path = "tests/shell.rs"]
mod tests;
