use serde::{Serialize, Deserialize};

/// DBSP circuit graph in canonical, hashable form. Embedded in
/// `ViewDef.circuit` so migrations are self-contained — the server
/// applies sys_circuit_* batches directly from this without re-parsing
/// SQL.
///
/// Shape mirrors `gnitz-core::types::CircuitGraph` (the imperative
/// client-side type) with three additions: `output_col_defs` so the
/// AST is self-describing for the view's schema, serde derives for
/// canonical encoding, and `Hash` derive for `DesiredState`
/// consistency.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CircuitGraph {
    pub view_id:           u64,
    pub primary_source_id: u64,
    pub nodes:         Vec<(u64, u64)>,           // (node_id, opcode)
    pub edges:         Vec<(u64, u64, u64, u64)>, // (edge_id, src, dst, port)
    pub sources:       Vec<(u64, u64)>,           // (node_id, table_id)
    pub params:        Vec<(u64, u64, u64)>,      // (node_id, slot, value)
    pub group_cols:    Vec<(u64, u64)>,           // (node_id, col_idx)
    pub const_strings: Vec<(u64, u64, String)>,   // (node_id, slot, string)
    pub output_col_defs: Vec<(String, u8)>,       // (name, type_code)
    pub dependencies:    Vec<u64>,                // source table_ids
}
