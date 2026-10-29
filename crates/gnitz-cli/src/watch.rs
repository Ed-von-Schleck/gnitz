//! `\watch`: a view's delta feed, live. The subscription's first answer is the
//! view whole; every later one is the deltas a push brought, printed weights
//! and all, until Ctrl-C. A CSV stream carries its header with the view whole:
//! the snapshot, and each reseed.

use std::time::Duration;

use gnitz_core::{block_on, ClientError, ScanReply, SubId, Subscription};
use gnitz_sql::GnitzSqlError;
use gnitz_wire::WireStatus;
use nu_ansi_term::{Color, Style};

use crate::render::{render, Target, Weights};
use crate::settings::{Expanded, Format, Settings};
use crate::shell::{Failed, Shell};
use crate::value::Rows;

/// How long one sync is held at the server while the view does not change.
/// A Ctrl-C that lands just before a sync starts does not wake it, so this
/// bounds how long such a Ctrl-C takes to stop the watch.
const HOLD: Duration = Duration::from_millis(500);

/// The `SELECT` a `\watch` argument stands for: a relation name alone, as
/// SQL reads one in `schema`, reads the view whole.
pub(crate) fn watch_query(arg: &str, schema: &str) -> String {
    let arg = arg.trim().trim_end_matches(';').trim();
    match gnitz_sql::parse_relation_name(arg, schema).is_some() {
        true => format!("SELECT * FROM {arg}"),
        false => arg.to_string(),
    }
}

/// A refusal a watch recovers from by reading the view whole again: the feed
/// no longer continues the cursor, or (`StaleCatalog`) the view resolves
/// differently and the `SELECT` is planned again.
fn restart_status(e: &ClientError) -> Option<WireStatus> {
    e.status()
        .filter(|s| matches!(s, WireStatus::DeltaExpired | WireStatus::StaleCatalog))
}

impl Shell {
    fn plan_watch(&mut self, sql: &str) -> Result<Subscription, GnitzSqlError> {
        block_on(gnitz_sql::plan_subscription(&mut self.client, &self.schema, sql))
    }

    /// Subscribe to `plan` from scratch: its id, and the view whole.
    fn subscribe(&mut self, plan: &Subscription) -> Result<(SubId, ScanReply), ClientError> {
        let (id, rows, _cursor) = block_on(self.client.subscribe(&*plan.upstream, None, &plan.schema, &plan.spec))?;
        Ok((id, rows))
    }

    /// A line naming what follows. Among the results only where they are for
    /// reading: a CSV or JSON stream stays parseable.
    fn watch_banner(&mut self, label: &str, rows: &Rows, target: Target) {
        let plus: i64 = rows.rows.iter().map(|&(_, w)| w.max(0)).sum();
        let minus: i64 = rows.rows.iter().map(|&(_, w)| (-w).max(0)).sum();
        let line = format!(
            "── {} {label}  +{plus} −{minus}",
            chrono::Local::now().format("%H:%M:%S")
        );
        if matches!(self.settings.format, Format::Csv | Format::Json) {
            self.status(&line);
            return;
        }
        let line = format!("{}\n", target.paint(Style::new().fg(Color::Cyan), &line));
        self.write_result(&line);
    }

    /// Print one answer of the feed. `whole` is whether it is the view whole —
    /// the snapshot, or a reseed — which a CSV stream heads with its header:
    /// a reseed's columns are those of the `SELECT` as planned again.
    fn watch_print(&mut self, label: &str, reply: &ScanReply, whole: bool) {
        let rows = Rows::decode(reply);
        let target = Target { header: whole, ..self.target_for(true) };
        self.watch_banner(label, &rows, target);
        let settings = Settings {
            footer: false,
            expanded: match self.settings.expanded {
                Expanded::Auto => Expanded::Off,
                e => e,
            },
            ..self.settings.clone()
        };
        let text = render(&rows, Weights::Column, &settings, target);
        self.write_result(&text);
    }

    /// The end of a watch a wait ended with `e`: a Ctrl-C ends it as asked.
    fn watch_ended(&mut self, e: ClientError) -> Result<(), Failed> {
        match e {
            ClientError::Interrupted(_) => {
                // Past the `^C` the terminal echoed.
                self.write_result("\n");
                Ok(())
            }
            e => self.fail(e),
        }
    }

    /// Follow the delta feed of the view `arg` names, or of the `SELECT` over
    /// one it is, until interrupted.
    pub(crate) fn watch(&mut self, arg: &str) -> Result<(), Failed> {
        let sql = watch_query(arg, &self.schema);
        let mut plan = self.plan_watch(&sql).or_else(|e| self.fail(e))?;
        let (mut id, first) = self.subscribe(&plan).or_else(|e| self.fail(e))?;
        self.status("Watching; Ctrl-C stops.");
        self.watch_print("snapshot", &first, true);
        let outcome = loop {
            if self.interrupted() {
                break Ok(());
            }
            let synced = match block_on(self.client.sync(HOLD)) {
                Ok(s) => s,
                Err(e) => break self.watch_ended(e),
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
                self.client.unsubscribe(id);
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
                        self.watch_print("reseeded", &whole, true);
                    }
                    Err(e) => break self.watch_ended(e),
                }
            }
        };
        self.client.unsubscribe(id);
        self.clear_interrupt();
        outcome
    }
}

#[cfg(test)]
#[path = "tests/watch.rs"]
mod tests;
