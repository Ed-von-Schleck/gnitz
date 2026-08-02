//! Debug-only fault-injection seams — the one place a `GNITZ_INJECT_*` variable
//! is read and the one place release builds fold it away.
//!
//! A seam is a `static Seam` next to the code it perturbs. Every accessor is
//! statically false/`None` in a release build, so the guarded branch is dead
//! code the optimizer drops: no call site needs its own `#[cfg]`, and a seam
//! cannot leak into a release binary. The variable is read once per process —
//! it cannot change mid-run, and several seams sit on per-push or per-relay
//! paths where a fresh `std::env::var` would allocate every call.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

pub(crate) struct Seam {
    var: &'static str,
    setting: OnceLock<Option<String>>,
    spent: AtomicBool,
}

impl Seam {
    pub(crate) const fn new(var: &'static str) -> Self {
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

    /// True while the seam is set to any value.
    pub(crate) fn armed(&self) -> bool {
        self.setting().is_some()
    }

    /// True while the seam names this stage — for seams that pick one of several
    /// injection points.
    pub(crate) fn at(&self, stage: &str) -> bool {
        self.setting() == Some(stage)
    }

    /// The seam's setting as a name the caller matches against its own state
    /// (a table name, a pipeline stage).
    pub(crate) fn names(&self) -> Option<&str> {
        self.setting()
    }

    /// The seam's setting as a positive count (milliseconds, rows, bytes —
    /// whatever the caller's unit is). A zero or unparseable value reads as
    /// unset, matching `env`'s rule that an override never zeroes a knob.
    pub(crate) fn count(&self) -> Option<u64> {
        self.setting()?.parse::<u64>().ok().filter(|&n| n > 0)
    }

    /// Consume the seam's one-shot latch: true on the first call while armed,
    /// false ever after, so the rest of the run behaves normally.
    pub(crate) fn take_once(&self) -> bool {
        self.armed() && !self.spent.swap(true, Ordering::Relaxed)
    }
}
