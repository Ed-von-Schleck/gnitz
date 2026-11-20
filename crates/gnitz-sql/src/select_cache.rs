//! The scripts of one `SELECT` a thread runs more than once, each kept parsed
//! and its rows plan with it. A plan is a function of its query, the session's
//! schema and what the catalog answers for each name the planner probes, so a
//! plan made from answers a later run's probes repeat — the same descriptor, by
//! identity, or the same absence — is the plan that run would make. Each kept
//! answer holds its descriptor, so no other one can take its address.
//!
//! A script is kept from its second run, so a script run once — one whose
//! literals change every run — evicts none that runs again.

use crate::catalog::Catalog;
use crate::dml::{ReadPlan, plan_read};
use crate::error::GnitzSqlError;
use crate::source::{StatementText, split_statements};
use gnitz_core::{RelDescriptor, RelName};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
use sqlparser::ast::{Query, Statement};
use sqlparser::parser::ParserError;
use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, PoisonError};

/// Scripts a thread keeps; the next one past this many takes the place of one
/// picked at random. A random pick keeps most of a working set a little larger
/// than this hitting, where evicting the oldest misses on every run of a cycle
/// through one more script than this.
const CAPACITY: usize = 64;
const _: () = assert!(CAPACITY.is_power_of_two());

/// Scripts a thread remembers having run once; the next one past this many
/// forgets them all.
const SEEN_CAPACITY: usize = 1024;

/// The longest script kept: past it the script is parsed again on every run.
const MAX_SCRIPT_LEN: usize = 4096;

#[derive(Default)]
struct Selects {
    kept: FxHashMap<Box<str>, Arc<CachedSelect>>,
    /// Hashes of the scripts run once. A collision only keeps a script a run
    /// early.
    seen: FxHashSet<u64>,
    /// Scripts evicted from `kept`, the seed of the next pick.
    evicted: u64,
}

thread_local! {
    static SELECTS: RefCell<Selects> = RefCell::default();
}

/// A script of one `SELECT`, parsed, and its last rows plan.
pub(crate) struct CachedSelect {
    query: Query,
    plan: Mutex<Option<KeptPlan>>,
}

struct KeptPlan {
    schema_name: Box<str>,
    /// Each name the planner probed, in order, and what the catalog answered.
    answers: Vec<(RelName, Option<Arc<RelDescriptor>>)>,
    plan: ReadPlan,
}

/// A script as [`parse`] answers it.
pub(crate) enum Script<'a> {
    /// A script of one `SELECT`, kept.
    Select(Arc<CachedSelect>),
    /// Any other script, or one `SELECT` run for the first time.
    Statements(Vec<(Statement, StatementText<'a>)>),
}

/// `sql` split into its statements, or the kept `SELECT` it is: taken from this
/// thread's cache, or kept from this run on when it ran before.
pub(crate) fn parse(sql: &str) -> Result<Script<'_>, ParserError> {
    if let Some(select) = lookup(sql) {
        return Ok(Script::Select(select));
    }
    let mut stmts = split_statements(sql)?;
    if let [(Statement::Query(_), _)] = &stmts[..]
        && ran_before(sql)
        && let Some((Statement::Query(query), _)) = stmts.pop()
    {
        return Ok(Script::Select(insert(sql, *query)));
    }
    Ok(Script::Statements(stmts))
}

/// The parsed `SELECT` of `sql`, if this thread keeps it.
fn lookup(sql: &str) -> Option<Arc<CachedSelect>> {
    SELECTS.with(|s| s.borrow().kept.get(sql).cloned())
}

/// Note a run of `sql`, a script of one `SELECT`: whether it ran before, and
/// is to be kept.
fn ran_before(sql: &str) -> bool {
    if sql.len() > MAX_SCRIPT_LEN {
        return false;
    }
    let mut h = FxHasher::default();
    sql.hash(&mut h);
    SELECTS.with(|s| {
        let seen = &mut s.borrow_mut().seen;
        if seen.len() >= SEEN_CAPACITY {
            seen.clear();
        }
        !seen.insert(h.finish())
    })
}

/// Keep `query`, parsed from `sql`, a script of that one `SELECT`.
fn insert(sql: &str, query: Query) -> Arc<CachedSelect> {
    let select = Arc::new(CachedSelect { query, plan: Mutex::new(None) });
    SELECTS.with(|s| {
        let s = &mut *s.borrow_mut();
        if s.kept.len() >= CAPACITY {
            // Fibonacci hashing of the count onto CAPACITY's bits: consecutive
            // picks land far apart in the map's iteration order.
            let bits = CAPACITY.trailing_zeros();
            let pick = (s.evicted.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - bits)) as usize;
            s.evicted += 1;
            let victim = s
                .kept
                .keys()
                .nth(pick)
                .cloned()
                .expect("a full cache holds CAPACITY scripts");
            s.kept.remove(&victim);
        }
        s.kept.insert(sql.into(), Arc::clone(&select));
    });
    select
}

impl CachedSelect {
    /// [`plan_read`] of the kept query against `cat`: the kept plan when `cat`
    /// repeats every answer it was made from, else a new one, which is kept
    /// when it is a rows read.
    pub(crate) fn plan(&self, cat: &dyn Catalog) -> Result<ReadPlan, GnitzSqlError> {
        if let Some(plan) = self.repeated(cat)? {
            return Ok(plan);
        }
        let recording = Recording { cat, answers: RefCell::new(Vec::new()) };
        let plan = plan_read(&self.query, &recording)?;
        if let Some(copy) = plan.rows_copy() {
            *self.plan.lock().unwrap_or_else(PoisonError::into_inner) = Some(KeptPlan {
                schema_name: cat.schema_name().into(),
                answers: recording.answers.into_inner(),
                plan: copy,
            });
        }
        Ok(plan)
    }

    /// A copy of the kept plan, if `cat` answers each name it was made from as
    /// it was answered then. Probes in the planner's order, so a run that
    /// diverges has probed what the planner would have before diverging.
    fn repeated(&self, cat: &dyn Catalog) -> Result<Option<ReadPlan>, GnitzSqlError> {
        let kept = self.plan.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(kept) = kept.as_ref() else { return Ok(None) };
        if *kept.schema_name != *cat.schema_name() {
            return Ok(None);
        }
        for (name, answer) in &kept.answers {
            let same = match (cat.probe(name)?, answer) {
                (Some(now), Some(then)) => Arc::ptr_eq(&now, then),
                (now, then) => now.is_none() && then.is_none(),
            };
            if !same {
                return Ok(None);
            }
        }
        Ok(kept.plan.rows_copy())
    }
}

/// A catalog that notes each name probed through it and the answer.
struct Recording<'a> {
    cat: &'a dyn Catalog,
    answers: RefCell<Vec<(RelName, Option<Arc<RelDescriptor>>)>>,
}

impl Catalog for Recording<'_> {
    fn schema_name(&self) -> &str {
        self.cat.schema_name()
    }

    fn probe(&self, name: &RelName) -> Result<Option<Arc<RelDescriptor>>, GnitzSqlError> {
        let answer = self.cat.probe(name)?;
        let mut answers = self.answers.borrow_mut();
        if !answers.iter().any(|(n, _)| n == name) {
            answers.push((name.clone(), answer.clone()));
        }
        Ok(answer)
    }
}

#[cfg(test)]
#[path = "tests/select_cache.rs"]
mod tests;
