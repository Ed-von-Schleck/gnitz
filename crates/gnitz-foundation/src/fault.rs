//! Debug-only fault-injection seams — the one place a `GNITZ_INJECT_*` variable
//! is read and the one place release builds fold it away.
//!
//! A seam is a `static Seam` next to the code it perturbs. Every accessor is
//! statically false/`None` in a release build, so the guarded branch is dead
//! code the optimizer drops: no call site needs its own `#[cfg]`, and a seam
//! cannot leak into a release binary. The variable is read once per process —
//! it cannot change mid-run, and several seams sit on per-push or per-tick
//! paths where a fresh `std::env::var` would allocate every call.
//!
//! Which build decides is *this* crate's: the `cfg!(debug_assertions)` below is
//! evaluated in `gnitz-foundation`'s compilation unit, so a per-package
//! `debug-assertions` override on this crate arms or disarms every seam,
//! wherever it is declared.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

pub struct Seam {
    var: &'static str,
    setting: OnceLock<Option<String>>,
    spent: AtomicBool,
}

impl Seam {
    pub const fn new(var: &'static str) -> Self {
        Self {
            var,
            setting: OnceLock::new(),
            spent: AtomicBool::new(false),
        }
    }

    /// The seam's setting, read once per process; always `None` in release.
    fn setting(&self) -> Option<&str> {
        if cfg!(debug_assertions) {
            self.setting.get_or_init(|| std::env::var(self.var).ok()).as_deref()
        } else {
            None
        }
    }

    /// True while the seam is set to anything `env`'s flag rule does not read as
    /// off, so a seam carrying a stage or a count is armed too.
    pub fn armed(&self) -> bool {
        self.setting().is_some_and(|v| crate::env::flag(v) != Some(false))
    }

    /// True while the seam names this stage — for seams that pick one of several
    /// injection points.
    pub fn at(&self, stage: &str) -> bool {
        self.setting() == Some(stage)
    }

    /// The seam's setting as a positive count (milliseconds, rows, bytes —
    /// whatever the caller's unit is), under `env`'s rule: a zero or
    /// unparseable value reads as unset.
    pub fn count(&self) -> Option<u64> {
        self.setting().and_then(crate::env::positive)
    }

    /// Consume the seam's one-shot latch: true on the first call while armed,
    /// false ever after, so the rest of the run behaves normally.
    pub fn take_once(&self) -> bool {
        self.armed() && !self.spent.swap(true, Ordering::Relaxed)
    }
}

// The seams read `None` in release, so their tests hold in debug only.
#[cfg(all(test, debug_assertions))]
#[path = "tests/fault.rs"]
mod tests;
