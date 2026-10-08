//! Z-set algebra: the functions of one Z-set, and the key encoders they share.
//!
//!   - `linear`      — filter, union (Theorem 3.3: no state added)
//!   - `map`         — `MapPlan`, the columnar driver behind every map and projection
//!   - `exchange`    — the scatter plan of a worker exchange
//!   - `aggregate`   — the aggregates, a reduce's row shape, and the ad-hoc fold
//!   - `group_key`   — a group as the output PK a reduce over it stamps
//!   - `reindex`     — the key composers a reindex Map and an exchange scatter share, and a
//!     secondary index's entries
//!   - `order_image` — the byte images an ordered index keys by
//!   - `sink`        — the sink of an ad-hoc read: forward, top-k or fold
//!
//! Nothing here reads a trace: each is a function of the one batch it is handed.
//! The linear ones are their own incremental form, so the read executor in
//! `gnitz-store` and the circuit VM in `gnitz-server` call the same kernels; the
//! ad-hoc fold and the top-k sink are one-shot folds only a read runs. The
//! operators that do read a trace are the `stream` rung above, which builds on
//! the aggregate, group-key and image machinery here.
//!
//! A `pub use` below is named by another crate; a `pub(crate) use` is what
//! `stream` reaches. The submodules stay private.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod aggregate;
mod exchange;
mod group_key;
mod linear;
mod map;
mod order_image;
mod reindex;
mod route;
mod sink;

pub use exchange::ScatterPlan;
pub use linear::{null_extend_output_schema, op_filter, op_union, union_nullability_merge};
pub use map::MapPlan;
pub use reindex::{append_spans, index_entries};
pub use route::{ground_owner, Placement, Slot};
pub use sink::SinkPlan;

pub(crate) use aggregate::{emit_reduce_row, Agg, AggValues, RangeGroups, ReduceShape};
pub(crate) use group_key::{ground_pk, GroupOrdinals, GroupOutKey};
pub(crate) use order_image::{image_slot_col, ImageCol, IMAGE_COL};
pub(crate) use reindex::ReindexPacker;
