use std::hash::{Hash, Hasher};
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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

// view_id is a client-side allocation artefact from alloc_table_id(); the
// server ignores it and allocates its own vid in build_create_view_batches.
// Two compilations of the same view SQL yield different view_ids, so we
// exclude it from PartialEq/Hash to avoid spurious "modified" diffs.
impl PartialEq for CircuitGraph {
    fn eq(&self, other: &Self) -> bool {
        self.primary_source_id == other.primary_source_id
            && self.nodes         == other.nodes
            && self.edges         == other.edges
            && self.sources       == other.sources
            && self.params        == other.params
            && self.group_cols    == other.group_cols
            && self.const_strings == other.const_strings
            && self.output_col_defs == other.output_col_defs
            && self.dependencies  == other.dependencies
    }
}
impl Eq for CircuitGraph {}

impl Hash for CircuitGraph {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.primary_source_id.hash(state);
        self.nodes.hash(state);
        self.edges.hash(state);
        self.sources.hash(state);
        self.params.hash(state);
        self.group_cols.hash(state);
        self.const_strings.hash(state);
        self.output_col_defs.hash(state);
        self.dependencies.hash(state);
    }
}
