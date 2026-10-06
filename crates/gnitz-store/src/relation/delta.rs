//! The delta store's key layout: a fed view's rows, stamped with the round that
//! recorded them.

use gnitz_wire::TypeCode;
use gnitz_zset::schema::{key_prefixed_schema, SchemaColumn, SchemaDescriptor};

/// The delta store's stamp column: the `_tick` round number, leading the delta
/// schema's PK. Named so its width is read off the column rather than written as
/// a literal at each offset.
const DELTA_TICK_COL: SchemaColumn = SchemaColumn::new(TypeCode::U64, false);
// A `u64`'s big-endian image is this column's OPK image only at this width.
const _: () = assert!(DELTA_TICK_COL.size() as usize == 8);

/// The prefix every delta key recorded in `round` starts with, ahead of the
/// view row's own key.
pub(crate) fn delta_round_prefix(round: u64) -> [u8; 8] {
    round.to_be_bytes()
}

/// The round a delta key was recorded in.
pub(crate) fn delta_round(delta_key: &[u8]) -> u64 {
    let tick = &delta_key[..DELTA_TICK_COL.size() as usize];
    u64::from_be_bytes(tick.try_into().expect("a delta key leads with its round"))
}

/// The delta store's schema for a fed view: its key is a `_tick` U64 column, then
/// the view's PK, and its payload space is the view's. The view's columns keep
/// their numbers, so a program compiled over the view runs on the store's rows.
/// Derived rather than persisted, as an index schema is. `None` for a view with
/// no column to spare for the stamp.
pub(crate) fn make_delta_schema(view: &SchemaDescriptor) -> Option<SchemaDescriptor> {
    key_prefixed_schema(DELTA_TICK_COL, view)
}

// The stamp is one U64 column ahead of a view PK of at most `PK_LIST_MAX_COLS`
// columns.
const _: () = assert!(
    gnitz_wire::PK_LIST_MAX_COLS < gnitz_wire::MAX_PK_COLUMNS,
    "a _tick stamp on the widest view PK no longer fits the PK limits"
);

#[cfg(test)]
#[path = "tests/delta.rs"]
mod tests;
