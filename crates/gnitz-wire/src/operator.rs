//! The parameters an operator carries on both wire paths, a circuit node and a
//! `ReadSpec` sink: the aggregate functions and the typing rules both sides read,
//! a grouped aggregation, the computed projection, and the ORDER BY key. A layout
//! here is stored inside a `CIRCUIT_TAB` cell, so changing it bumps
//! `CIRCUIT_VERSION`, which moves `WIRE_VERSION` through `SYS_SCHEMA_DIGEST`.

use crate::codec::{Reader, Wire, Writer};
use crate::{ScalarKind, TypeCode, MAX_COLUMNS};

wire_enum! {
    /// Aggregate function discriminant. The values are durable catalog state and
    /// a wire value, both inside an [`AggDescriptor`].
    pub enum AggFunc: u8 {
        Count = 1,
        Sum = 2,
        Min = 3,
        Max = 4,
        CountNonNull = 5,
    }
}

impl AggFunc {
    /// True iff `Agg(A + B) == Agg(A) + Agg(B)`: a delta folds with no history
    /// replay, and the value over no rows is `0`.
    pub const fn is_linear(self) -> bool {
        match self {
            AggFunc::Count | AggFunc::CountNonNull | AggFunc::Sum => true,
            AggFunc::Min | AggFunc::Max => false,
        }
    }

    /// The op a reduce folds this aggregate's own output with: linear partials add,
    /// extremes compete.
    pub const fn merge_op(self) -> AggFunc {
        if self.is_linear() {
            AggFunc::Sum
        } else {
            self
        }
    }

    /// True iff a reduce's raw output column for this aggregate can hold NULL.
    pub const fn raw_output_nullable(self, src_nullable: bool, ungrouped: bool) -> bool {
        !self.is_linear() && (src_nullable || ungrouped)
    }

    /// Output type code of this aggregate over a source column of type `src_tc`,
    /// or `None` when that type does not admit it — the one rule both the
    /// planner's declared schema and the engine's emitted batch read.
    ///
    /// A sum is typed as its 8-byte register image, so SUM over U64 is U64: the
    /// wrapping i64 accumulator's bits are the true sum mod 2^64.
    pub const fn output_type(self, src_tc: TypeCode) -> Option<TypeCode> {
        match self {
            AggFunc::Count | AggFunc::CountNonNull => Some(TypeCode::I64),
            // Adding two calendar values, or two truth values, is meaningless.
            AggFunc::Sum => {
                if ScalarKind::from_type_code(src_tc).is_some()
                    && !src_tc.is_temporal()
                    && !matches!(src_tc, TypeCode::Bool)
                {
                    Some(src_tc.register_image())
                } else {
                    None
                }
            }
            AggFunc::Min | AggFunc::Max => Some(src_tc),
        }
    }
}

/// One aggregate of a reduce: which function, over which column of the reduce's
/// input. Carries no column *type* — that is `schema.columns[col_idx]`, which
/// every consumer already holds. Travels inside a [`GroupAggs`] and an
/// [`crate::OpNode::Window`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AggDescriptor {
    pub col_idx: u32,
    pub agg_op: AggFunc,
}

impl AggDescriptor {
    /// COUNT(*), which reads no column: column 0 is a placeholder.
    pub const COUNT_STAR: AggDescriptor = AggDescriptor { col_idx: 0, agg_op: AggFunc::Count };
}

/// The function, then its column.
impl Wire for AggDescriptor {
    fn write(&self, w: &mut Writer) {
        w.put(&self.agg_op).u32(self.col_idx);
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        Ok(AggDescriptor { agg_op: r.get()?, col_idx: r.u32()? })
    }
}

/// A grouped aggregation over an input: the columns it groups by, then the
/// aggregates of each group. The one carrier of both a `Reduce` node and a
/// `ReadSpec` fold sink, so the reduce and the ad-hoc fold consume the same
/// parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupAggs {
    pub group_cols: Vec<u32>,
    pub aggs: Vec<AggDescriptor>,
}

/// The group columns, then the aggregates.
impl Wire for GroupAggs {
    fn write(&self, w: &mut Writer) {
        w.list(&self.group_cols).list(&self.aggs);
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        Ok(GroupAggs {
            group_cols: col_list(r)?,
            aggs: r.list("aggregate list", MAX_COLUMNS)?,
        })
    }
}

/// A compiled expression program and the payload slots it writes, as
/// `(type_code, nullable)` in output order; the PK region is inherited verbatim.
/// The declarations travel because a computed slot has no source column the
/// engine could derive its type from; a pure column copy declares its slots too.
/// One type on both wire paths — a circuit's `Map` and a `ReadSpec` fold's
/// pre-map are the same device.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComputeMap {
    pub program: Vec<u8>,
    pub out_cols: Vec<(TypeCode, bool)>,
}

/// The declared payload slots, then the program.
impl Wire for ComputeMap {
    fn write(&self, w: &mut Writer) {
        w.list(&self.out_cols).bytes32(&self.program);
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        let out_cols = r.list("compute map", MAX_COLUMNS)?;
        Ok(ComputeMap { program: r.bytes32()?.to_vec(), out_cols })
    }
}

/// ORDER BY keys apply in sequence; a spec carries at most this many.
pub const MAX_ORDER_KEYS: usize = 16;

/// One ORDER BY key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderKey {
    /// A full reply-schema column index (not a dense payload index).
    pub col: u16,
    pub desc: bool,
    pub nulls_first: bool,
}

const ORDER_DESC: u8 = 1 << 0;
const ORDER_NULLS_FIRST: u8 = 1 << 1;

/// The column, then a flag byte; an unknown flag bit is a refusal.
impl Wire for OrderKey {
    fn write(&self, w: &mut Writer) {
        let mut flags = 0u8;
        if self.desc {
            flags |= ORDER_DESC;
        }
        if self.nulls_first {
            flags |= ORDER_NULLS_FIRST;
        }
        w.u16(self.col).u8(flags);
    }

    fn read(r: &mut Reader) -> Result<Self, String> {
        let col = r.u16()?;
        let flags = r.flags(ORDER_DESC | ORDER_NULLS_FIRST)?;
        Ok(OrderKey {
            col,
            desc: flags & ORDER_DESC != 0,
            nulls_first: flags & ORDER_NULLS_FIRST != 0,
        })
    }
}

/// A column-index list, capped at [`MAX_COLUMNS`].
pub(crate) fn col_list(r: &mut Reader) -> Result<Vec<u32>, String> {
    r.list("column list", MAX_COLUMNS)
}

/// An ORDER BY key list, capped at [`MAX_ORDER_KEYS`].
pub(crate) fn order_keys(r: &mut Reader) -> Result<Vec<OrderKey>, String> {
    r.list("order keys", MAX_ORDER_KEYS)
}

#[cfg(test)]
#[path = "tests/operator.rs"]
mod tests;
