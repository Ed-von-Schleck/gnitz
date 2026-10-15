//! The shell: one connection, the settings, and running what is typed or read
//! — statements through the SQL front end, meta-commands through [`crate::meta`].

use std::fmt::Display;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, IsTerminal, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gnitz_core::{block_on, BlockingHost, Catalog, ClientError, GnitzClient};
use gnitz_sql::{GnitzSqlError, SqlResult};
use nu_ansi_term::{Color, Style};

use crate::complete::Context;
use crate::lex::{self, Piece, Splitter};
use crate::render::{render, Format, Settings, Weights};
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
    File(BufWriter<File>),
}

/// How the shell was started.
pub struct Options {
    pub target: String,
    pub schema: String,
    pub settings: Settings,
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
    pub settings: Settings,
    pub interactive: bool,
    stop_on_error: bool,
    interrupt: Option<Arc<AtomicBool>>,
    sink: Sink,
    /// The last statement run, which `\e` opens.
    pub(crate) last_statement: String,
    /// Text for the next prompt's buffer, from `\e`.
    pub prefill: Option<String>,
    /// Where the editors' buffers are written, made on first use and removed
    /// with the shell.
    scratch: Option<tempfile::TempDir>,
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
    block_on(GnitzClient::connect_with(target, Box::new(host(interrupt))))
}

/// Whether `e` ended the connection, so a statement after it needs a new one.
fn is_disconnect(e: &GnitzSqlError) -> bool {
    matches!(
        e,
        GnitzSqlError::Client(ClientError::Closed | ClientError::ConnectionLost(_))
    )
}

impl Shell {
    pub fn connect(options: Options) -> Result<Shell, ClientError> {
        let client = connect(&options.target, options.interrupt.clone())?;
        Ok(Shell {
            client,
            target: options.target,
            schema: options.schema,
            settings: options.settings,
            interactive: options.interactive,
            stop_on_error: options.stop_on_error,
            interrupt: options.interrupt,
            sink: Sink::Stdout,
            last_statement: String::new(),
            prefill: None,
            scratch: None,
        })
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn in_transaction(&self) -> bool {
        self.client.txn_active()
    }

    /// Clear an interrupt that arrived before the wait it would cancel.
    pub fn clear_interrupt(&self) {
        if let Some(flag) = &self.interrupt {
            flag.store(false, Ordering::SeqCst);
        }
    }

    pub fn interrupted(&self) -> bool {
        self.interrupt.as_ref().is_some_and(|f| f.load(Ordering::SeqCst))
    }

    /// The catalog as it stands now, for a command that answers from it.
    pub fn catalog(&mut self) -> Result<Catalog, Failed> {
        block_on(self.client.catalog()).or_else(|e| self.fail(e))
    }

    /// The private directory the editors' buffers go in.
    pub fn scratch(&mut self) -> Result<&Path, Failed> {
        if self.scratch.is_none() {
            let dir = tempfile::Builder::new()
                .prefix("gnitz-")
                .tempdir()
                .or_else(|e| self.fail(format!("cannot make a scratch directory: {e}")))?;
            self.scratch = Some(dir);
        }
        Ok(self.scratch.as_ref().expect("made above").path())
    }

    /// What completion reads now: the catalog as it stands, and the schema.
    /// `None` when the catalog cannot be read; completion is a convenience,
    /// never a reason to stop.
    pub fn completion(&mut self) -> Option<Context> {
        block_on(self.client.catalog())
            .ok()
            .map(|catalog| Context { catalog, schema: self.schema.clone() })
    }

    pub fn reconnect(&mut self, target: &str) -> Result<(), Failed> {
        match connect(target, self.interrupt.clone()) {
            Ok(client) => {
                self.client = client;
                self.target = target.to_string();
                Ok(())
            }
            Err(e) => self.fail(format!("could not connect to {target}: {e}")),
        }
    }

    // ── Running input ─────────────────────────────────────────────────────

    pub fn run_script(&mut self, text: &str) -> Result<Flow, Failed> {
        self.run_input(text.as_bytes())
    }

    /// Run the commands in the file at `path`; `-` is stdin.
    pub fn run_file(&mut self, path: &str) -> Result<Flow, Failed> {
        if path == "-" {
            return self.run_input(std::io::stdin().lock());
        }
        let file = File::open(path).or_else(|e| self.fail(format!("{path}: {e}")))?;
        self.run_input(BufReader::new(file))
    }

    /// Run a whole script, each piece as soon as it is read: every statement
    /// and meta-command in it, and SQL left without a closing `;` at its end.
    pub fn run_input(&mut self, input: impl BufRead) -> Result<Flow, Failed> {
        let mut splitter = Splitter::default();
        let mut failed = false;
        let mut lines = input.lines();
        loop {
            let piece = match lines.next() {
                Some(Ok(line)) => match splitter.push_line(&line) {
                    Some(piece) => piece,
                    None => continue,
                },
                Some(Err(e)) => return self.fail(format!("reading input: {e}")),
                None => match splitter.finish() {
                    Some(piece) => piece,
                    None => break,
                },
            };
            let r = match piece {
                Piece::Meta(line) => self.meta(&line),
                Piece::Sql(sql) => self.run_sql(&sql),
            };
            match r {
                Ok(Flow::Quit) => return Ok(Flow::Quit),
                Ok(Flow::Continue) => {}
                Err(Failed) if self.stop_on_error => return Err(Failed),
                Err(Failed) => failed = true,
            }
        }
        match failed {
            true => Err(Failed),
            false => Ok(Flow::Continue),
        }
    }

    /// Run each statement of `sql` in turn; the first that fails ends it.
    pub fn run_sql(&mut self, sql: &str) -> Result<Flow, Failed> {
        for range in lex::statements(sql) {
            let stmt = &sql[range];
            if lex::is_exit_word(stmt) {
                return Ok(Flow::Quit);
            }
            self.run_statement(stmt)?;
        }
        Ok(Flow::Continue)
    }

    fn run_statement(&mut self, stmt: &str) -> Result<(), Failed> {
        self.last_statement = format!("{stmt};");
        self.clear_interrupt();
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
                if is_disconnect(&e) {
                    let target = self.target.clone();
                    if self.reconnect(&target).is_ok() {
                        self.note("reconnected; an open transaction was lost");
                    }
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
    pub fn print_rows(&mut self, rows: &Rows) {
        let weights = match rows.is_bag() {
            true => Weights::Repeat,
            false => Weights::Column,
        };
        let (width, height) = self.terminal_size();
        let pageable = self.pageable() && self.settings.format == Format::Aligned;
        let text = render(rows, weights, &self.settings, width, !pageable);
        if !pageable || self.fits(&text, width, height) {
            self.write_result(&text);
        } else if !self.paged(&text) {
            let wrapped = render(rows, weights, &self.settings, width, true);
            self.write_result(&wrapped);
        }
    }

    // ── Output ─────────────────────────────────────────────────────────────

    fn to_terminal(&self) -> bool {
        matches!(self.sink, Sink::Stdout) && std::io::stdout().is_terminal()
    }

    /// The terminal's columns and rows, where results go to one.
    pub fn terminal_size(&self) -> (Option<u16>, Option<u16>) {
        if !self.to_terminal() {
            return (None, None);
        }
        match crossterm::terminal::size() {
            Ok((w, h)) => (Some(w), Some(h)),
            Err(_) => (None, None),
        }
    }

    fn fits(&self, text: &str, width: Option<u16>, height: Option<u16>) -> bool {
        let (Some(w), Some(h)) = (width, height) else {
            return true;
        };
        let lines = text.lines().count();
        let widest = text.lines().map(console::measure_text_width).max().unwrap_or(0);
        lines < usize::from(h).saturating_sub(1) && widest <= usize::from(w)
    }

    fn pageable(&self) -> bool {
        self.interactive && self.settings.pager && self.to_terminal()
    }

    /// Show `text` in the pager; whether it started.
    fn paged(&mut self, text: &str) -> bool {
        let pager = std::env::var("PAGER").unwrap_or_else(|_| "less -SRFX".to_string());
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

    /// Write result text where results go.
    pub fn write_result(&mut self, text: &str) {
        match &mut self.sink {
            Sink::Stdout => {
                let mut out = std::io::stdout().lock();
                let _ = out.write_all(text.as_bytes());
                let _ = out.flush();
            }
            Sink::File(f) => {
                let _ = f.write_all(text.as_bytes());
                let _ = f.flush();
            }
        }
    }

    pub fn output_to_file(&mut self, path: &str) -> Result<(), Failed> {
        match File::create(path) {
            Ok(f) => {
                self.sink = Sink::File(BufWriter::new(f));
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
    pub fn status(&mut self, msg: &str) {
        if msg.is_empty() {
            return;
        }
        match self.interactive {
            true => println!("{}", self.settings.paint(Style::new().dimmed(), msg)),
            false => eprintln!("{msg}"),
        }
    }

    /// A hint after an error.
    pub fn note(&self, msg: &str) {
        eprintln!(
            "{}",
            self.settings
                .paint(Style::new().fg(Color::Yellow), &format!("NOTE: {msg}"))
        );
    }

    /// Report an error; the command it ended has failed.
    pub fn fail<T>(&self, msg: impl Display) -> Result<T, Failed> {
        eprintln!(
            "{} {msg}",
            self.settings.paint(Style::new().fg(Color::Red).bold(), "ERROR:")
        );
        Err(Failed)
    }
}

pub fn format_elapsed(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    match ms < 1000.0 {
        true => format!("Time: {ms:.3} ms"),
        false => format!("Time: {ms:.3} ms ({:.3} s)", d.as_secs_f64()),
    }
}
