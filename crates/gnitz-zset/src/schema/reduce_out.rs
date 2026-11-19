//! How a reduce keys its output and lays out the columns ahead of its
//! aggregates. For a `Reduce` node both are derived, never transmitted: the
//! planner and the engine call [`reduce_out_layout`] over the reduce's input
//! schema. The fold sink's reply is keyed the same way.

use super::{SchemaColumn, SchemaDescriptor};
use gnitz_wire::TypeCode;

/// How a reduce keys its output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReduceOutKey {
    /// The group columns are the output PK: the source's whole PK list, or one
    /// non-nullable PK-eligible column.
    Natural,
    /// A leading hidden `_group_pk` U128, a NULL-distinct key of the group
    /// columns, which ride as payload. Every other group set, the empty
    /// (global) one included.
    SyntheticFold,
}

/// One slot of a reduce's output layout, ahead of the aggregate columns. See
/// [`reduce_out_layout`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReduceOutSlot {
    /// The synthetic fold key, [`Self::SYNTHETIC_KEY_COL`]: slot 0 of the fold
    /// arm, and its whole PK.
    SyntheticKey,
    /// The source column it names, in the output's PK region.
    Key(u32),
    /// The source column it names, carried into the payload — a column of the
    /// output's row the key region does not spell.
    Carried(u32),
}

impl ReduceOutSlot {
    /// The column of [`Self::SyntheticKey`]: the hidden `_group_pk`.
    pub const SYNTHETIC_KEY_COL: SchemaColumn = SchemaColumn::new(TypeCode::U128, false);
}

/// The output-key kind a reduce grouped by `group` over `schema` warrants.
pub(crate) fn reduce_out_key(schema: &SchemaDescriptor, group: &[u32]) -> ReduceOutKey {
    let natural = group == schema.pk_cols()
        || matches!(*group, [c] if {
            let col = schema.columns()[c as usize];
            // The PK region carries no null bitmap.
            !col.nullable && col.type_code.is_pk_eligible()
        });
    if natural {
        ReduceOutKey::Natural
    } else {
        ReduceOutKey::SyntheticFold
    }
}

/// A reduce's output layout ahead of its aggregates, grouped by `group` over
/// `schema`: the key region (or the synthetic key), then each `row` column the
/// key region does not spell.
pub fn reduce_out_layout(
    schema: &SchemaDescriptor,
    group: &[u32],
    row: impl IntoIterator<Item = u32>,
) -> Vec<ReduceOutSlot> {
    let (mut slots, spelled): (Vec<ReduceOutSlot>, &[u32]) = match reduce_out_key(schema, group) {
        ReduceOutKey::SyntheticFold => (vec![ReduceOutSlot::SyntheticKey], &[]),
        ReduceOutKey::Natural => (group.iter().map(|&c| ReduceOutSlot::Key(c)).collect(), group),
    };
    slots.extend(
        row.into_iter()
            .filter(|c| !spelled.contains(c))
            .map(ReduceOutSlot::Carried),
    );
    slots
}

#[cfg(test)]
#[path = "tests/reduce_out.rs"]
mod tests;
