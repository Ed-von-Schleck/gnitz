//! Debug-only fault-injection seams — the one place a `GNITZ_INJECT_*` variable
//! is read and the one place release builds fold it away.
//!
//! A seam is a `static Seam` next to the code it perturbs. Every accessor is
//! statically false/`None` in a release build, so the guarded branch is dead
//! code the optimizer drops: no call site needs its own `#[cfg]`, and a seam
//! cannot leak into a release binary. The variable is read once per process —
//! it cannot change mid-run, and several seams sit on per-push or per-relay
//! paths where a fresh `std::env::var` would allocate every call.
//!
//! Which build decides is *this* crate's: the `cfg!(debug_assertions)` below is
//! evaluated in `gnitz-store`'s compilation unit, while most `Seam` declarations
//! live in `gnitz-server` and `gnitz-engine`. The three profiles agree in this
//! workspace, so a debug server arms its seams — but a profile that optimized
//! only the store would disarm every one of them, with no diagnostic.

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

    /// True while the seam is set to any value.
    pub fn armed(&self) -> bool {
        self.setting().is_some()
    }

    /// True while the seam names this stage — for seams that pick one of several
    /// injection points.
    pub fn at(&self, stage: &str) -> bool {
        self.setting() == Some(stage)
    }

    /// The seam's setting as a name the caller matches against its own state
    /// (a table name, a pipeline stage).
    pub fn names(&self) -> Option<&str> {
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
    pub fn take_once(&self) -> bool {
        self.armed() && !self.spent.swap(true, Ordering::Relaxed)
    }
}
