//! Debug-only fault-injection seams — the one place a `GNITZ_INJECT_*` variable
//! is read and the one place release builds fold it away.
//!
//! A seam is a `static` next to the code it perturbs, of the kind its variable
//! takes: a [`Switch`] is on or off, a [`Count`] carries a positive number, a
//! [`Stage`] names one of the injection points it lists. Every accessor is
//! statically false/`None` in a release build, so the guarded branch is dead
//! code the optimizer drops: no call site needs its own `#[cfg]`, and a seam
//! cannot leak into a release binary. The variable is read once per process —
//! it cannot change mid-run, and several seams sit on per-push or per-tick
//! paths.
//!
//! A value `env`'s flag rule reads as off disarms a seam of any kind. Any other
//! value its kind does not read panics at the seam's first use, as an `env`
//! override's does: a misspelt setting never leaves a seam silently inert.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::env::{BOOLEAN, POSITIVE, flag, parse, positive, raw};

/// Whether this build keeps its seams. Decided in *this* crate's compilation
/// unit, so a per-package `debug-assertions` override on `gnitz-foundation`
/// arms or disarms every seam, wherever it is declared.
pub const ARMED: bool = cfg!(debug_assertions);

/// A seam's variable as its kind reads it, `None` while disarmed.
struct Var<T> {
    name: &'static str,
    value: OnceLock<Option<T>>,
}

impl<T: Copy> Var<T> {
    const fn new(name: &'static str) -> Self {
        Var { name, value: OnceLock::new() }
    }

    /// The setting as `read` reads the variable's raw value, read once per
    /// process; always `None` in a build without seams.
    fn get(&self, read: impl FnOnce(&str, Option<&str>) -> Option<T>) -> Option<T> {
        if !ARMED {
            return None;
        }
        *self.value.get_or_init(|| read(self.name, raw(self.name).as_deref()))
    }
}

/// As [`parse`], except that a value `env`'s flag rule reads as off counts as
/// unset.
fn arm<T>(name: &str, raw: Option<&str>, rule: impl FnOnce(&str) -> Option<T>, what: &str) -> Option<T> {
    parse(name, raw.filter(|v| flag(v) != Some(false)), rule, what)
}

/// A seam that is on or off.
pub struct Switch {
    var: Var<bool>,
    spent: AtomicBool,
}

impl Switch {
    pub const fn new(var: &'static str) -> Self {
        Switch {
            var: Var::new(var),
            spent: AtomicBool::new(false),
        }
    }

    fn read(name: &str, raw: Option<&str>) -> Option<bool> {
        arm(name, raw, flag, BOOLEAN)
    }

    /// True while the seam is on.
    pub fn armed(&self) -> bool {
        self.var.get(Self::read).is_some()
    }

    /// Consume the seam's one-shot latch: true on the first call while armed,
    /// false ever after, so the rest of the run behaves normally.
    pub fn take_once(&self) -> bool {
        self.armed() && !self.spent.swap(true, Ordering::Relaxed)
    }
}

/// A seam carrying a positive count (milliseconds, rows, bytes — whatever the
/// caller's unit is).
pub struct Count(Var<u64>);

impl Count {
    pub const fn new(var: &'static str) -> Self {
        Count(Var::new(var))
    }

    fn read(name: &str, raw: Option<&str>) -> Option<u64> {
        arm(name, raw, positive, POSITIVE)
    }

    /// The count; `None` while the seam is disarmed.
    pub fn count(&self) -> Option<u64> {
        self.0.get(Self::read)
    }
}

/// A seam that picks one of several injection points.
pub struct Stage {
    var: Var<&'static str>,
    stages: &'static [&'static str],
}

impl Stage {
    pub const fn new(var: &'static str, stages: &'static [&'static str]) -> Self {
        Stage { var: Var::new(var), stages }
    }

    /// The stage of `stages` that `raw` names; `None` when off.
    fn read(&self, name: &str, raw: Option<&str>) -> Option<&'static str> {
        let rule = |v: &str| self.stages.iter().copied().find(|s| *s == v);
        arm(name, raw, rule, &format!("one of {}", self.stages.join("|")))
    }

    /// True while the seam names `stage`, one of those it lists.
    pub fn at(&self, stage: &str) -> bool {
        debug_assert!(
            self.stages.contains(&stage),
            "{stage:?} is no stage of {}",
            self.var.name
        );
        self.var.get(|name, raw| self.read(name, raw)) == Some(stage)
    }
}

// The seams read as disarmed without debug assertions, so their tests hold in
// debug only.
#[cfg(all(test, debug_assertions))]
#[path = "tests/fault.rs"]
mod tests;
