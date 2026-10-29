//! The shell: one connection, the settings, and running what is typed or read
//! — statements through the SQL front end, meta-commands through [`crate::meta`].

use std::cell::Cell;
use std::fmt::Display;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gnitz_core::{block_on, BlockingHost, Catalog, ClientError, GnitzClient};
use gnitz_sql::{GnitzSqlError, SqlResult};
use nu_ansi_term::{Color, Style};

use crate::complete::Context;
use crate::lex::{self, Piece, Splitter};
use crate::render::{paint, render, Target, Weights};
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

/// How the shell was started.
pub struct Options {
    pub target: String,
    pub schema: String,
    pub settings: Settings,
    /// Whether the session styles what it prints to the terminal.
    pub color: bool,
    pub interactive: bool,
    /// Whether a failed statement ends a script.
    pub stop_on_error: bool,
    /// Set by SIGINT; a blocking wait checks it and gives up.
    pub interrupt: Option<Arc<AtomicBool>>,
}

pub struct Shell {
    pub(crate) client: GnitzClient,
    target: String,
    /// The schema unqualified names resolve in.
    pub(crate) schema: String,
    pub(crate) settings: Settings,
    pub(crate) color: bool,
    pub(crate) interactive: bool,
    stop_on_error: bool,
    interrupt: Option<Arc<AtomicBool>>,
    sink: Sink,
    /// Whether a command has failed, for the exit status of a script.
    failed: Cell<bool>,
    /// The last statement run, which `\e` opens.
    pub(crate) last_statement: String,
    /// Text for the next prompt's buffer, from `\e`.
    pub(crate) prefill: Option<String>,
    /// The REPL's directory for the editors' buffers, removed with the shell;
    /// `None` outside one.
    pub(crate) scratch: Option<tempfile::TempDir>,
}

/// A blocking host whose waits give up once `interrupt` is set.
fn host(interrupt: Option<Arc<AtomicBool>>) -> BlockingHost {
    match interrupt {
        None => BlockingHost::default(),
        Some(flag) => BlockingHost::with_hook(Box::new(move || match flag.swap(false, Ordering::SeqCst) {
            true => Err("cancelled by Ctrl-C".into()),
            false => Ok(()),
        })),
    }
}

fn connect(target: &str, interrupt: Option<Arc<AtomicBool>>) -> Result<GnitzClient, ClientError> {
    GnitzClient::connect_on(target, Box::new(host(interrupt)))
}

impl Shell {
    pub fn connect(options: Options) -> Result<Shell, ClientError> {
        let client = connect(&options.target, options.interrupt.clone())?;
        Ok(Shell {
            client,
            target: options.target,
            schema: options.schema,
            settings: options.settings,
            color: options.color,
            interactive: options.interactive,
            stop_on_error: options.stop_on_error,
            interrupt: options.interrupt,
            sink: Sink::Stdout,
            failed: Cell::new(false),
            last_statement: String::new(),
            prefill: None,
            scratch: None,
        })
    }

    pub(crate) fn target(&self) -> &str {
        &self.target
    }

    /// Whether any command of the session has failed.
    pub fn failed(&self) -> bool {
        self.failed.get()
    }

    /// Clear an interrupt that arrived before the wait it would cancel.
    pub(crate) fn clear_interrupt(&self) {
        if let Some(flag) = &self.interrupt {
            flag.store(false, Ordering::SeqCst);
        }
    }

    pub(crate) fn interrupted(&self) -> bool {
        self.interrupt.as_ref().is_some_and(|f| f.load(Ordering::SeqCst))
    }

    /// The catalog as it stands now, for a command that answers from it.
    pub fn catalog(&mut self) -> Result<Catalog, Failed> {
        block_on(self.client.catalog()).or_else(|e| self.fail(e))
    }

    /// What completion reads now: the catalog as it stands, and the schema.
    /// `None` when the catalog cannot be read; completion is a convenience,
    /// never a reason to stop.
    pub(crate) fn completion(&mut self) -> Option<Context> {
        block_on(self.client.catalog())
            .ok()
            .map(|catalog| Context { catalog, schema: self.schema.clone() })
    }

    /// Dial `target` in place of the current connection, whose open
    /// transaction, if any, is lost.
    pub(crate) fn reconnect(&mut self, target: &str) -> Result<(), Failed> {
        let client = connect(target, self.interrupt.clone())
            .or_else(|e| self.fail(format!("could not connect to {target}: {e}")))?;
        if self.client.txn_active() {
            self.note("the open transaction was lost");
        }
        self.client = client;
        self.target = target.to_string();
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
    fn run_input(&mut self, mut lines: impl Iterator<Item = io::Result<String>>) -> Result<Flow, Failed> {
        let mut splitter = Splitter::default();
        let mut more = true;
        while more {
            let pieces = match lines.next() {
                Some(Ok(line)) => splitter.push_line(&line),
                Some(Err(e)) => return self.fail(format!("reading input: {e}")),
                None => {
                    more = false;
                    splitter.finish()
                }
            };
            for piece in pieces {
                match self.run_piece(piece) {
                    Ok(Flow::Quit) => return Ok(Flow::Quit),
                    Err(Failed) if self.stop_on_error => return Err(Failed),
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
            let target = self.target.clone();
            if self.reconnect(&target).is_ok() {
                self.note("reconnected");
            }
        }
        r
    }

    fn run_statement(&mut self, stmt: &str) -> Result<(), Failed> {
        self.last_statement = format!("{stmt};");
        let started = Instant::now();
        let result = block_on(gnitz_sql::execute(&mut self.client, &self.schema, stmt));
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
        if self.settings.timing {
            self.status(&format_elapsed(elapsed));
        }
        outcome
    }

    fn show(&mut self, result: SqlResult) {
        match result {
            SqlResult::Rows(reply) => {
                let rows = Rows::decode(&reply);
                self.print_rows(&rows);
            }
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
        // The terminal an aligned result of a session may page on; elsewhere a
        // table wraps its cells to fit from the start.
        let pager = self.interactive && self.settings.pager && self.settings.format == Format::Aligned;
        let paging = self.terminal_size().filter(|_| pager);
        let target = self.target_for(paging.is_none());
        let text = render(rows, weights, &self.settings, target);
        if fits(&text, paging) {
            self.write_result(&text);
        } else if !paged(&text) {
            let wrapped = render(rows, weights, &self.settings, Target { wrap: true, ..target });
            self.write_result(&wrapped);
        }
    }

    // ── Output ─────────────────────────────────────────────────────────────

    /// The terminal's columns and rows, where results go to one.
    pub(crate) fn terminal_size(&self) -> Option<(u16, u16)> {
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
            color: self.color && width.is_some(),
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

    pub fn output_to_file(&mut self, path: &str) -> Result<(), Failed> {
        match File::create(path) {
            Ok(f) => {
                self.sink = Sink::File(f);
                Ok(())
            }
            Err(e) => self.fail(format!("{path}: {e}")),
        }
    }

    pub fn output_to_stdout(&mut self) {
        self.sink = Sink::Stdout;
    }

    /// A command's status line: its tag, a timing, a setting changed. On the
    /// terminal in a session; on stderr from a script, so stdout carries only
    /// results.
    pub(crate) fn status(&self, msg: &str) {
        if msg.is_empty() {
            return;
        }
        // A reader gone from stdout ends no session.
        let _ = match self.interactive {
            true => writeln!(io::stdout(), "{}", paint(self.color, Style::new().dimmed(), msg)),
            false => writeln!(io::stderr(), "{msg}"),
        };
    }

    /// A hint after an error.
    pub(crate) fn note(&self, msg: &str) {
        eprintln!(
            "{}",
            paint(self.color, Style::new().fg(Color::Yellow), &format!("NOTE: {msg}"))
        );
    }

    /// Report an error; the command it ended has failed.
    pub(crate) fn fail<T>(&self, msg: impl Display) -> Result<T, Failed> {
        self.failed.set(true);
        eprintln!(
            "{} {msg}",
            paint(self.color, Style::new().fg(Color::Red).bold(), "ERROR:")
        );
        Err(Failed)
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
    let lines = text.lines().count();
    let widest = text.lines().map(console::measure_text_width).max().unwrap_or(0);
    lines < usize::from(h).saturating_sub(1) && widest <= usize::from(w)
}

/// Show `text` in `$PAGER`, else `less`; whether it started.
fn paged(text: &str) -> bool {
    let pager = std::env::var("PAGER")
        .ok()
        .filter(|p| !p.trim().is_empty())
        .unwrap_or_else(|| "less -SRFX".to_string());
    let Ok(mut child) = Command::new("/bin/sh")
        .arg("-c")
        .arg(&pager)
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
