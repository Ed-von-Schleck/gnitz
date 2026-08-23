//! A maintained local copy of a view, answering reads in the host's own process.
//!
//! The copy is a real engine store — an LSM that spills to disk, not a resident
//! Z-set — fed by the view's delta feed
//! (`CREATE VIEW … WITH (delta = '<size>')`). Reads run the engine's own
//! `ReadSpec` executor locally and are finished by the SQL layer exactly as a
//! remote reply is, so a mirrored `SELECT` costs no round trip after
//! registration.
//!
//! What can be mirrored is a **view**, and only a view: `delta_bytes` is a
//! `VIEW_TAB` column, so a base table cannot carry a feed and cannot be
//! mirrored.
//!
//! # Freshness
//!
//! A mirrored read answers **at the mirror's cursor round**. A read against the
//! server drains pending ticks first, so it answers "what is current"; a delta
//! read drives no tick and a local read never polls, so the copy holds the
//! rounds the server had already emitted when the host last called
//! [`Mirror::poll`]. Two consequences, neither with a workaround inside the
//! mirror: it is not read-your-own-writes, and two mirrored views can sit at
//! different rounds, so a read spanning both is not a consistent cut. A relation
//! the mirror does not hold is delegated upstream and keeps every guarantee a
//! server read has.
//!
//! # Threads and processes
//!
//! The handle is pinned to one thread — the engine's stores are `!Send` and the
//! compiler enforces it — and there may be **one live handle per process**,
//! because the engine publishes its resume generation into a process-global.
//! Across processes the engine's own `flock` on the data directory is what
//! refuses a second writer.
//!
//! # The read ceiling a mirror inherits
//!
//! A reply carrying STRING or BLOB values goes out as one frame, so the server
//! refuses one past its frame cap. That ceiling is the server's and applies to
//! every client — an unprojected `SELECT` of the same view fails the same way,
//! which is why a wide text relation is served by projecting the read.
//!
//! A bootstrap cannot take that way out: it reads the view whole, so it has no
//! projection, predicate or `LIMIT` to narrow, and the feed's one recovery is to
//! bootstrap again, which fails identically. So a view whose STRING/BLOB rows on
//! one worker exceed the cap cannot be mirrored, and [`Mirror::mirror_view`]
//! reports the server's refusal. What it leaves behind is a registration with no
//! copy beneath it, and [`Mirror::mirrors`] answers `false` for one: a read of it
//! is delegated upstream, where it is refused for the same reason, rather than
//! answered off a copy that never arrived.
//!
//! # Cost
//!
//! One thread reads what W workers read in parallel. Total work is the same, so
//! a mirror wins outright on point and small bounded reads and the margin
//! narrows as the walk grows: a full scan of a large view at high W is a loss.
//! Narrowing the bound is the lever.

mod apply;
mod cursors;
mod error;
mod handle;
mod reads;
mod register;

pub use error::MirrorError;
pub use handle::Mirror;
