//! SAL (shared append-only log): master→worker broadcast channel.
//!
//! Owns the layout of `wal.sal` — the anchor page and the ring of groups
//! behind it — and every reader and writer of it. A group's kind byte, its
//! request arguments and its payload head are this module's alone: callers name
//! a [`Read`] or an [`Apply`], a boot reads each tail member's [`Ingest`], and
//! every group is written through [`SalExcl`] or a [`SalScope`], whose methods
//! are the shapes a group takes.
//!
//! One file per concern, each importing only from the ones listed before it.
//!
//! - `format` — the bytes: the publication prefix, anchor and format words,
//!   the group header and its codec, the directory, the payload codec, the
//!   kind ordinals and the scalars of a request the header carries. The
//!   header's field offsets and flag bits are private to it.
//! - `request` — what a group asks of a worker, typed: [`Read`], [`Apply`], and
//!   the one mapping between a request and those scalars and its blob.
//! - `log` — [`SalLog`], the mapping of the file: a verified group read at an
//!   offset, and the walk over consecutive groups.
//! - `reader` — [`SalReader`], a worker's live drain: the groups addressed to
//!   it, each decoded to an [`Inbound`].
//! - `tail` — [`CommittedTail`], what a boot replays: every zone closed before
//!   the recovery walk's first defect, each member as the [`Ingest`] it carries.
//! - `group` — one group as the writer encodes it: a request plus what varies
//!   by worker; [`Routed`] is what a push hands in.
//! - `durable` — how far the log is durable, who waits on it, and the driver of
//!   the fdatasyncs.
//! - `writer` — [`SalWriter`], the master's end: layout and publication, the
//!   hold ([`SalExcl`]) and the publication scope with its zone ([`SalScope`]),
//!   LSNs, and when a checkpoint is due. Its child `fixtures` is the test
//!   fixture of the whole module.

mod durable;
mod format;
mod group;
mod log;
mod reader;
mod request;
mod tail;
mod writer;

pub(crate) use group::Routed;
pub(crate) use reader::{Inbound, ReplyRoute, SalReader};
pub(crate) use request::{Apply, Ingest, KeptRead, Read};
pub(crate) use tail::CommittedTail;
pub(crate) use writer::{SalExcl, SalWriter};
#[cfg(test)]
pub(crate) use writer::{SalFull, fixtures};
