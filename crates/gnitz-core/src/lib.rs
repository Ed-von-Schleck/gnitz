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

pub use batch::{BatchAppender, BatchMark, PayloadColumn, PkColumn, ZSetBatch};
pub use catalog::{Catalog, CatalogRelation, IndexRow, RelProps};
pub use client::{env_target, GnitzClient};
pub use connection::{
    encode_ddl_txn, DeltaCursor, RelDescriptor, ScanReply, SubId, Target, MAX_IN_FLIGHT, MAX_QUEUED_BYTES,
};
pub use ddl::{segment_id, ColumnChange, FkTarget, InlineUniqueIndex, PlannedView, TableDef, ViewBundle};
pub use error::{not_found, ClientError, ProtocolError};
pub use host::{block_on, drain_cancel, promise, serve, BlockingHost, BoxFut, Host, Job, Op, Pending, Promise, Sent};
pub use mirror::{MirrorStore, Planner, PollOutcome, PollResult, Subscription};
pub use pushed::{Delivery, Pushed, Synced};
pub use rel_name::RelName;
pub use schema::{sys_schema, Schema};
pub use snapshot::DEFAULT_PAGE_ROWS;
pub use transport::{ClientTransport, Interest};
pub use txn::PushBatch;
