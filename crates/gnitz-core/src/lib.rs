//! The gnitz client core: the connection to a server and the wire protocol it
//! speaks, the client-side schema and batch, the DDL and transaction verbs,
//! and the mirror state machine.
//!
//! A module's unit tests sit in the `tests/` directory beside it; tests no single
//! module owns — the client against a real server, gated on the `integration`
//! feature — live in `suites/`.

mod batch;
mod catalog;
mod client;
mod connection;
mod ddl;
mod error;
mod host;
pub mod local_server;
mod mirror;
mod pushed;
pub mod recording;
mod rel_name;
mod schema;
mod snapshot;
#[cfg(all(test, feature = "integration"))]
mod suites;
#[cfg(test)]
mod test_support;
mod transport;
mod txn;

pub use batch::{BatchAppender, PayloadColumn, PkColumn, RowMark, ZSetBatch};
pub use catalog::{Catalog, CatalogRelation, IndexRow, RelProps};
pub use client::{GnitzClient, env_target};
pub use connection::{
    DeltaCursor, MAX_IN_FLIGHT, MAX_QUEUED_BYTES, RelDescriptor, ScanReply, SubId, Target, encode_ddl_txn,
};
pub use ddl::{ColumnChange, FkTarget, InlineUniqueIndex, PlannedView, TableDef, ViewBundle, segment_id};
pub use error::{ClientError, ProtocolError, not_found};
pub use host::{BlockingHost, BoxFut, Host, Job, Op, Pending, Promise, Sent, block_on, drain_cancel, promise, serve};
pub use mirror::{MirrorStore, Planner, PollOutcome, PollResult, Subscription};
pub use pushed::{Delivery, Pushed, Synced};
pub use rel_name::RelName;
pub use schema::{Schema, sys_schema};
pub use snapshot::DEFAULT_PAGE_ROWS;
pub use transport::{ClientTransport, Interest};
pub use txn::PushBatch;
