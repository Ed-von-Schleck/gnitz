//! Z-set algebra: the functions of one Z-set, and the key encoders they share.
//!
//!   - `linear`      — filter, union (Theorem 3.3: no state added)
//!   - `map`         — `MapPlan`, the columnar driver behind every map and projection
//!   - `exchange`    — the scatter plan and the gather of a worker exchange
//!   - `aggregate`   — the accumulators, a reduce's row shape, and the ad-hoc fold
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
mod sink;

pub use exchange::{op_exchange_gather, op_worker_filter, ScatterPlan};
pub use linear::{null_extend_output_schema, op_filter, op_union, union_nullability_merge};
pub use map::MapPlan;
pub use reindex::index_entries;
pub use sink::SinkPlan;

pub(crate) use aggregate::{emit_reduce_row, Accumulator, ExtremeSpec, ReduceShape};
pub(crate) use group_key::{ground_pk, GroupOutKey};
pub(crate) use order_image::{
    append_image, has_fixed_image, image_slot_col, int16_image, scalar_image, write_image_slot, ImageKind, WideKind,
    IMAGE_COL,
};
pub(crate) use reindex::ReindexPacker;
