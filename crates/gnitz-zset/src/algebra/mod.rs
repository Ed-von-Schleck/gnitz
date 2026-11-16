//! Z-set algebra: the functions of one Z-set, and the key encoders they share.
//!
//!   - `linear`      — filter and union (Theorem 3.3: no state added), and the output schemas of a union and a null-extend
//!   - `map`         — `MapPlan`, the columnar driver behind every map and projection
//!   - `exchange`    — the scatter plan of a worker exchange
//!   - `aggregate`   — the aggregates, a reduce's row shape, and the ad-hoc fold
//!   - `group_key`   — the key a row is grouped or routed by, the output PK a reduce stamps, and a batch's groups numbered
//!   - `key_pack`    — the packed keys of a join, a group and an index span, and the row digest
//!   - `index_entry` — a secondary index's key span, its entries, and their spans alone
//!   - `order_image` — the byte images an ordered index keys by
//!   - `route`       — which worker owns a key, and where a relation's rows live
//!   - `sink`        — the sink of an ad-hoc read: map, then forward, a prefix cut, a top-k or a fold
//!
//! Nothing here reads a trace: each is a function of the one batch it is handed.
//! The linear ones are their own incremental form, so the read executor in
//! `gnitz-store` and the circuit VM in `gnitz-server` call the same kernels; the
//! ad-hoc fold and the top-k sink are one-shot folds only a read runs. The
//! operators that do read a trace are the `stream` rung above, which builds on
//! the aggregate, group-key, key-packing, image and map machinery here.
//!
//! A `pub use` below is named by another crate; a `pub(crate) use` is what
//! `stream` reaches. The submodules stay private.

mod aggregate;
mod exchange;
mod group_key;
mod index_entry;
mod key_pack;
mod linear;
mod map;
mod order_image;
mod route;
mod sink;

pub use exchange::ScatterPlan;
pub use index_entry::{KeySpec, extend_spans, index_entries};
pub use linear::{null_extend_output_schema, op_filter, op_union, union_nullability_merge};
pub use map::MapPlan;
pub use route::{Placement, Slot, ground_owner};
pub use sink::SinkPlan;

pub(crate) use aggregate::{Agg, AggValues, RangeGroups, ReduceShape, resolve_aggs};
pub(crate) use group_key::{GroupOrdinals, GroupOutKey, GroupScratch, OutPk, ground_pk};
pub(crate) use key_pack::KeyPacker;
pub(crate) use map::copy_columns;
pub(crate) use order_image::{IMAGE_COL, ImageCol, image_slot_col};

/// `v` as `n` zeros: freshly allocated zeroed where it lacks the room, else
/// overwritten in place.
pub(crate) fn zeroed<T: Copy + Default>(v: &mut Vec<T>, n: usize) {
    match v.capacity() < n {
        true => *v = vec![T::default(); n],
        false => {
            v.clear();
            v.resize(n, T::default());
        }
    }
}
