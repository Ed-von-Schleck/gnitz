//! `\watch`: a view's delta feed, live. The subscription's first answer is the
//! view whole; every later one is the deltas a push brought, printed weights
//! and all, until Ctrl-C.

use std::time::Duration;

use gnitz_core::{block_on, ClientError, ScanReply, Subscription};
use gnitz_sql::GnitzSqlError;
use gnitz_wire::{WireFault, WireStatus};
use nu_ansi_term::{Color, Style};

use crate::render::{Expanded, Format, Weights};
use crate::shell::{Failed, Shell};
use crate::value::Rows;

/// How long one sync is held at the server while the view does not change.
/// A Ctrl-C that lands just before a sync starts does not wake it, so this
/// bounds how long such a Ctrl-C takes to stop the watch.
const HOLD: Duration = Duration::from_millis(500);

/// The `SELECT` a `\watch` argument stands for: a bare relation name reads
/// the view whole.
pub fn watch_query(arg: &str) -> String {
    let arg = arg.trim().trim_end_matches(';').trim();
    let is_name = !arg.is_empty() && arg.chars().all(|c| c.is_alphanumeric() || matches!(c, '_' | '.' | '"'));
    match is_name {
        true => format!("SELECT * FROM {arg}"),
        false => arg.to_string(),
    }
}

/// A refusal a watch recovers from by reading the view whole again: the feed
/// no longer continues the cursor, or (`StaleCatalog`) the view resolves
/// differently and the `SELECT` is planned again.
fn restart_status(e: &ClientError) -> Option<WireStatus> {
    match e {
        ClientError::Refused(WireFault {
            status: s @ (WireStatus::DeltaExpired | WireStatus::StaleCatalog),
            ..
        }) => Some(*s),
        _ => None,
    }
}

fn local_time() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs()) as libc::time_t;
    // SAFETY: `tm` is plain data `localtime_r` fills in; both pointers are to
    // locals that outlive the call.
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    let ok = unsafe { !libc::localtime_r(&now, &mut tm).is_null() };
    match ok {
        true => format!("{:02}:{:02}:{:02}", tm.tm_hour, tm.tm_min, tm.tm_sec),
        false => String::new(),
    }
}

impl Shell {
    fn plan_watch(&mut self, sql: &str) -> Result<Subscription, GnitzSqlError> {
        let schema = self.schema();
        block_on(gnitz_sql::plan_subscription(self.client(), &schema, sql))
    }

    /// Subscribe to `plan` from scratch: its id, and the view whole.
    fn subscribe(&mut self, plan: &Subscription) -> Result<(u64, ScanReply), ClientError> {
        let (id, rows, _cursor) = block_on(self.client().subscribe(&*plan.upstream, None, &plan.schema, &plan.spec))?;
        Ok((id, rows))
    }

    /// A line naming what follows. Among the results only where they are for
    /// reading: a CSV or JSON stream stays parseable.
    fn watch_banner(&mut self, label: &str, rows: &Rows) {
        let plus: i64 = rows.rows.iter().map(|&(_, w)| w.max(0)).sum();
        let minus: i64 = rows.rows.iter().map(|&(_, w)| (-w).max(0)).sum();
        let line = format!("── {} {label}  +{plus} −{minus}", local_time());
        if matches!(self.settings.format, Format::Csv | Format::Json) {
            self.status(&line);
            return;
        }
        let line = self.settings.paint(Style::new().fg(Color::Cyan), &line);
        self.write_result(&format!("{line}\n"));
    }

    /// `first` is whether this is the watch's first batch: a CSV stream has
    /// one header.
    fn watch_print(&mut self, label: &str, reply: &ScanReply, first: bool) {
        let rows = Rows::decode(reply);
        self.watch_banner(label, &rows);
        if rows.rows.is_empty() && !first {
            return;
        }
        let mut settings = self.settings.clone();
        settings.footer = false;
        if settings.expanded == Expanded::Auto {
            settings.expanded = Expanded::Off;
        }
        let (width, _) = self.terminal_size();
        let text = crate::render::render(&rows, Weights::Column, &settings, width, true);
        let text = match (settings.format, first) {
            (Format::Csv, false) => text.split_once('\n').map_or("", |(_, rest)| rest),
            _ => &text,
        };
        self.write_result(text);
    }

    /// Follow the delta feed of the view `arg` names, or of the `SELECT` over
    /// one it is, until interrupted.
    pub fn watch(&mut self, arg: &str) -> Result<(), Failed> {
        let sql = watch_query(arg);
        self.clear_interrupt();
        let mut plan = match self.plan_watch(&sql) {
            Ok(p) => p,
            Err(e) => return self.fail(e),
        };
        let (mut id, first) = match self.subscribe(&plan) {
            Ok(s) => s,
            Err(e) => return self.fail(e),
        };
        self.status("Watching; Ctrl-C stops.");
        self.watch_print("snapshot", &first, true);
        let outcome = loop {
            if self.interrupted() {
                break Ok(());
            }
            let synced = match block_on(self.client().sync(HOLD)) {
                Ok(s) => s,
                Err(ClientError::Interrupted(_)) => {
                    // Past the `^C` the terminal echoed.
                    self.write_result("\n");
                    break Ok(());
                }
                Err(e) => break self.fail(e),
            };
            let mut restart = None;
            let mut ended = None;
            for pushed in synced.pushed.into_iter().filter(|p| p.sub == id) {
                match pushed.result {
                    Ok((reply, _cursor)) => {
                        if !reply.batch.is_empty() {
                            self.watch_print("delta", &reply, false);
                        }
                    }
                    Err(e) => match restart_status(&e) {
                        Some(status) => restart = Some((status, e)),
                        None => ended = Some(e),
                    },
                }
            }
            if let Some(e) = ended {
                break self.fail(e);
            }
            if let Some((status, cause)) = restart {
                self.client().unsubscribe(id);
                self.note(&format!("{cause}; reading the view whole again"));
                if status == WireStatus::StaleCatalog {
                    plan = match self.plan_watch(&sql) {
                        Ok(p) => p,
                        Err(e) => break self.fail(e),
                    };
                }
                match self.subscribe(&plan) {
                    Ok((new_id, whole)) => {
                        id = new_id;
                        self.watch_print("reseeded", &whole, false);
                    }
                    Err(e) => break self.fail(e),
                }
            }
        };
        self.client().unsubscribe(id);
        self.clear_interrupt();
        outcome
    }
}

#[cfg(test)]
#[path = "tests/watch.rs"]
mod tests;
