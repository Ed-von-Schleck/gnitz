//! `\watch`: a view's delta feed, live. The subscription's first answer is the
//! view whole; every later one is the deltas a push brought, printed weights
//! and all, until Ctrl-C. A CSV stream carries its header with the view whole:
//! the snapshot, and each reseed.

use std::time::Duration;

use gnitz_core::{ClientError, ScanReply, Subscription, block_on};
use gnitz_sql::GnitzSqlError;
use gnitz_wire::WireStatus;
use gnitz_wire::multi_frame::SubId;
use nu_ansi_term::{Color, Style};

use crate::render::{Target, Weights, paint, render};
use crate::settings::{Expanded, Format, Settings};
use crate::shell::{Failed, Shell};
use crate::value::Rows;

/// How long one sync is held at the server while the view does not change. A
/// Ctrl-C wakes the wait itself, so this bounds no response time.
const HOLD: Duration = Duration::from_secs(60);

/// The `SELECT` a `\watch` argument stands for: a relation name alone, as
/// SQL reads one in `schema`, reads the view whole.
pub(crate) fn watch_query(arg: &str, schema: &str) -> String {
    let arg = arg.trim().trim_end_matches(';').trim();
    match gnitz_sql::parse_relation_name(arg, schema).is_some() {
        true => format!("SELECT * FROM {arg}"),
        false => arg.to_string(),
    }
}

impl Shell {
    fn plan_watch(&mut self, sql: &str) -> Result<Subscription, GnitzSqlError> {
        block_on(gnitz_sql::plan_subscription(
            &mut self.client,
            &self.options.schema,
            sql,
        ))
    }

    /// A line naming what follows. Among the results only where they are for
    /// reading: a CSV or JSON stream stays parseable.
    fn watch_banner(&mut self, label: &str, rows: &Rows, target: Target) {
        let plus: i64 = rows.weights.iter().map(|&w| w.max(0)).sum();
        let minus: i64 = rows.weights.iter().map(|&w| (-w).max(0)).sum();
        let line = format!(
            "── {} {label}  +{plus} −{minus}",
            chrono::Local::now().format("%H:%M:%S")
        );
        if matches!(self.options.settings.format, Format::Csv | Format::Json) {
            self.status(&line);
            return;
        }
        let line = format!("{}\n", paint(target.color, Style::new().fg(Color::Cyan), &line));
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
            expanded: match self.options.settings.expanded {
                Expanded::Auto => Expanded::Off,
                e => e,
            },
            ..self.options.settings.clone()
        };
        let text = render(&rows, Weights::Column, &settings, target);
        self.write_result(&text);
    }

    /// Follow the delta feed of the view `arg` names, or of the `SELECT` over
    /// one it is, until interrupted.
    pub(crate) fn watch(&mut self, arg: &str) -> Result<(), Failed> {
        let sql = watch_query(arg, &self.options.schema);
        let mut plan = self.plan_watch(&sql).or_else(|e| self.fail(e))?;
        let mut label = "snapshot";
        loop {
            let cause = match block_on(self.client.subscribe(&*plan.upstream, None, &plan.schema, &plan.spec)) {
                Ok((id, whole, _cursor)) => {
                    if label == "snapshot" {
                        self.status("Watching; Ctrl-C stops.");
                    }
                    self.watch_print(label, &whole, true);
                    label = "reseeded";
                    let cause = self.follow(id);
                    self.client.unsubscribe(id);
                    cause
                }
                Err(e) => e,
            };
            // The feed no longer continues the cursor, or the view resolves
            // differently and the `SELECT` is planned again: either way the
            // view is read whole again.
            let replan = match cause.status() {
                Some(WireStatus::StaleCatalog) => true,
                Some(WireStatus::DeltaExpired) => false,
                // A Ctrl-C ends the watch as asked, past the `^C` the
                // terminal echoed.
                _ if matches!(cause, ClientError::Interrupted(_)) => {
                    eprintln!();
                    return Ok(());
                }
                _ => return self.fail(cause),
            };
            self.note(&format!("{cause}; reading the view whole again"));
            if replan {
                plan = self.plan_watch(&sql).or_else(|e| self.fail(e))?;
            }
        }
    }

    /// Print subscription `id`'s deltas as they come: the error that ends
    /// them.
    fn follow(&mut self, id: SubId) -> ClientError {
        loop {
            let synced = match block_on(self.client.sync(HOLD)) {
                Ok(s) => s,
                Err(e) => return e,
            };
            // A sync has one entry per subscription.
            let Some(pushed) = synced.pushed.into_iter().find(|p| p.sub == id) else {
                continue;
            };
            match pushed.result {
                Ok((reply, _cursor)) if !reply.batch.is_empty() => self.watch_print("delta", &reply, false),
                Ok(_) => {}
                Err(e) => return e,
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/watch.rs"]
mod tests;
