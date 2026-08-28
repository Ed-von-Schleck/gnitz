//! The store behind a maintained local copy of a view — `gnitz-core`'s
//! [`MirrorStore`](gnitz_core::MirrorStore), implemented once.
//!
//! The copy is a real engine store — an LSM that spills to disk, not a resident
//! Z-set — fed by the view's delta feed
//! (`CREATE VIEW … WITH (delta = '<size>')`). Reads run the engine's own
//! `ReadSpec` executor locally and are finished by the SQL layer exactly as a
//! remote reply is, so a mirrored `SELECT` costs no round trip after
//! registration.
//!
//! A host opens one and hands it to a client:
//!
//! ```ignore
//! client.attach_mirror(gnitz_mirror::Mirror::open(dir)?)?;
//! client.mirror_view("public", "recent")?;
//! ```
//!
//! Everything a *host* contracts for — freshness, what can be mirrored, the read
//! ceiling, the cost — is stated in `gnitz_core::mirror`, because that is where a
//! host meets it. What lives here is the store: the local catalog, the copies,
//! their feed positions, and the durability around them.
//!
//! The store is `Send` and `!Sync`, which is what lets it live inside a
//! `Send + Sync` [`gnitz_core::GnitzClient`]; the argument is beside the
//! `unsafe impl` in `handle.rs`.
//!
//! There is **one store per data directory**. The engine's own `flock` refuses a
//! second one, in this process or any other: a second [`Mirror::open`] takes a
//! fresh file description, which `flock` treats as a conflict.

mod apply;
mod cursors;
mod handle;
mod reads;
mod register;

pub use handle::Mirror;
