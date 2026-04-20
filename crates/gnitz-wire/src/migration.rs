//! Declarative migration AST + canonical serialisation + content
//! hash + diff / topo-sort helpers.
//!
//! Lives in `gnitz-wire` so both the client-side parser in
//! `gnitz-sql` and the server-side apply in `gnitz-engine` can share
//! the same types. The SQL parser lives in `gnitz-sql` because it
//! depends on `sqlparser`; this crate stays lean.
//!
//! Rules:
//! - Object references in the AST are by **name**, not `table_id`.
//!   IDs are allocation artefacts and would break content addressing
//!   across migrations.
//! - Field order inside every struct must not change without a
//!   `format_version` bump. Bincode serialisation depends on field
//!   declaration order; silent AST drift produces silent hash drift.
//! - Vec-typed fields (`tables`, `views`, `indices`) are sorted by
//!   fully-qualified name before serialisation so insertion order
//!   does not affect the hash.
//! - Column order inside `TableDef.columns` is part of the table's
//!   identity and is NOT re-sorted.

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

use crate::circuit::CircuitGraph;

// ---------------------------------------------------------------------------
// AST types (canonical form)
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef {
    pub name:        String,
    pub type_code:   u8,
    pub is_nullable: bool,
    pub fk_schema:   String,
    pub fk_table:    String,
    pub fk_col_idx:  u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableDef {
    pub schema:     String,
    pub name:       String,
    pub columns:    Vec<ColumnDef>,
    pub pk_col_idx: u32,
    pub unique_pk:  bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewDef {
    pub schema:  String,
    pub name:    String,
    pub sql:     String,
    pub circuit: CircuitGraph,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexDef {
    pub schema:       String,
    pub name:         String,
    pub owner_schema: String,
    pub owner_name:   String,
    pub source_col:   String,
    pub is_unique:    bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewDep {
    pub view_schema: String,
    pub view_name:   String,
    pub dep_schema:  String,
    pub dep_name:    String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct DesiredState {
    pub tables:  Vec<TableDef>,
    pub views:   Vec<ViewDef>,
    pub indices: Vec<IndexDef>,
}

// ---------------------------------------------------------------------------
// Diff
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct Diff {
    pub created_tables:  Vec<TableDef>,
    pub created_views:   Vec<ViewDef>,
    pub created_indices: Vec<IndexDef>,
    pub modified_tables:  Vec<(TableDef, TableDef)>,
    pub modified_views:   Vec<(ViewDef, ViewDef)>,
    pub modified_indices: Vec<(IndexDef, IndexDef)>,
    pub dropped_tables:  Vec<TableDef>,
    pub dropped_views:   Vec<ViewDef>,
    pub dropped_indices: Vec<IndexDef>,
}

impl Diff {
    pub fn has_modifications(&self) -> bool {
        !self.modified_tables.is_empty()
            || !self.modified_views.is_empty()
            || !self.modified_indices.is_empty()
    }

    pub fn first_modification_description(&self) -> Option<String> {
        if let Some((_, t)) = self.modified_tables.first() {
            return Some(format!("table {}.{}", t.schema, t.name));
        }
        if let Some((_, v)) = self.modified_views.first() {
            return Some(format!("view {}.{}", v.schema, v.name));
        }
        if let Some((_, i)) = self.modified_indices.first() {
            return Some(format!("index {}.{}", i.schema, i.name));
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Canonicalisation + hash
// ---------------------------------------------------------------------------

pub fn canonicalize(state: &DesiredState) -> Vec<u8> {
    let mut s = state.clone();
    s.tables.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    s.views.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    s.indices.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    let cfg = bincode::config::legacy();
    bincode::serde::encode_to_vec(&s, cfg)
        .expect("bincode encode never fails for in-memory owned DesiredState")
}

pub fn decanonicalize(bytes: &[u8]) -> Result<DesiredState, String> {
    let cfg = bincode::config::legacy();
    let (state, consumed) = bincode::serde::decode_from_slice::<DesiredState, _>(bytes, cfg)
        .map_err(|e| format!("canonical decode error: {}", e))?;
    if consumed != bytes.len() {
        return Err(format!(
            "canonical decode: {} trailing bytes", bytes.len() - consumed
        ));
    }
    Ok(state)
}

/// Compute the content-addressed hash for a migration commit.
///
/// Returns `xxh3_128(parent || len(canonical) || canonical || ...)`.
/// xxh3_128 over a length-prefixed framing — collision resistance
/// isn't required (migrations are authored by trusted operators),
/// drift detection is.
pub fn compute_migration_hash(
    parent_hash: u128,
    canonical: &[u8],
    author: &str,
    message: &str,
) -> u128 {
    let cap = 16 + 8 + canonical.len() + 8 + author.len() + 8 + message.len();
    let mut buf = Vec::with_capacity(cap);
    buf.extend_from_slice(&parent_hash.to_le_bytes());
    buf.extend_from_slice(&(canonical.len() as u64).to_le_bytes());
    buf.extend_from_slice(canonical);
    buf.extend_from_slice(&(author.len() as u64).to_le_bytes());
    buf.extend_from_slice(author.as_bytes());
    buf.extend_from_slice(&(message.len() as u64).to_le_bytes());
    buf.extend_from_slice(message.as_bytes());
    xxhash_rust::xxh3::xxh3_128(&buf)
}

// ---------------------------------------------------------------------------
// Diff
// ---------------------------------------------------------------------------

pub fn diff_by_name(parent: &DesiredState, new: &DesiredState) -> Diff {
    let mut d = Diff::default();

    let parent_t: HashMap<(String, String), &TableDef> = parent.tables.iter()
        .map(|t| ((t.schema.clone(), t.name.clone()), t)).collect();
    let new_t: HashMap<(String, String), &TableDef> = new.tables.iter()
        .map(|t| ((t.schema.clone(), t.name.clone()), t)).collect();
    for (k, nt) in &new_t {
        match parent_t.get(k) {
            None => d.created_tables.push((*nt).clone()),
            Some(pt) if *pt != *nt => d.modified_tables.push(((*pt).clone(), (*nt).clone())),
            _ => {}
        }
    }
    for (k, pt) in &parent_t {
        if !new_t.contains_key(k) { d.dropped_tables.push((*pt).clone()); }
    }

    let parent_v: HashMap<(String, String), &ViewDef> = parent.views.iter()
        .map(|v| ((v.schema.clone(), v.name.clone()), v)).collect();
    let new_v: HashMap<(String, String), &ViewDef> = new.views.iter()
        .map(|v| ((v.schema.clone(), v.name.clone()), v)).collect();
    for (k, nv) in &new_v {
        match parent_v.get(k) {
            None => d.created_views.push((*nv).clone()),
            Some(pv) if *pv != *nv => d.modified_views.push(((*pv).clone(), (*nv).clone())),
            _ => {}
        }
    }
    for (k, pv) in &parent_v {
        if !new_v.contains_key(k) { d.dropped_views.push((*pv).clone()); }
    }

    let parent_i: HashMap<(String, String), &IndexDef> = parent.indices.iter()
        .map(|i| ((i.schema.clone(), i.name.clone()), i)).collect();
    let new_i: HashMap<(String, String), &IndexDef> = new.indices.iter()
        .map(|i| ((i.schema.clone(), i.name.clone()), i)).collect();
    for (k, ni) in &new_i {
        match parent_i.get(k) {
            None => d.created_indices.push((*ni).clone()),
            Some(pi) if *pi != *ni => d.modified_indices.push(((*pi).clone(), (*ni).clone())),
            _ => {}
        }
    }
    for (k, pi) in &parent_i {
        if !new_i.contains_key(k) { d.dropped_indices.push((*pi).clone()); }
    }

    d.created_tables.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    d.created_views.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    d.created_indices.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    d.dropped_tables.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    d.dropped_views.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    d.dropped_indices.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    d.modified_tables.sort_by(|a, b| (&a.1.schema, &a.1.name).cmp(&(&b.1.schema, &b.1.name)));
    d.modified_views.sort_by(|a, b| (&a.1.schema, &a.1.name).cmp(&(&b.1.schema, &b.1.name)));
    d.modified_indices.sort_by(|a, b| (&a.1.schema, &a.1.name).cmp(&(&b.1.schema, &b.1.name)));

    d
}

// ---------------------------------------------------------------------------
// Cascade validation
// ---------------------------------------------------------------------------

pub fn validate_drop_closure(
    dropped_qualified: &HashSet<(String, String)>,
    deps: &[ViewDep],
) -> Result<(), String> {
    for d in deps {
        let dep_key = (d.dep_schema.clone(), d.dep_name.clone());
        if !dropped_qualified.contains(&dep_key) { continue; }
        let view_key = (d.view_schema.clone(), d.view_name.clone());
        if !dropped_qualified.contains(&view_key) {
            return Err(format!(
                "cannot drop {}.{}: still referenced by view {}.{}",
                dep_key.0, dep_key.1, view_key.0, view_key.1,
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Hex transport helpers — bincode output may contain arbitrary bytes
// (any 0x00..=0xFF). The wire protocol's STRING column holds UTF-8,
// so we hex-encode canonical bytes for transport and hex-decode
// server-side. The migration hash is computed over RAW bytes on both
// ends, not over the hex string — so both sides agree.
// ---------------------------------------------------------------------------

pub fn to_hex(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(LUT[(b >> 4) as usize] as char);
        s.push(LUT[(b & 0x0f) as usize] as char);
    }
    s
}

pub fn from_hex(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("hex string has odd length".into());
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for i in (0..bytes.len()).step_by(2) {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(c: u8) -> Result<u8, String> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(format!("invalid hex char: 0x{:02x}", c)),
    }
}

// ---------------------------------------------------------------------------
// Topo sort
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TopoKind { Table, View, Index }

#[derive(Clone, Debug)]
struct TopoNode {
    kind: TopoKind,
    idx: usize,
    key: (String, String),
    deps: Vec<(String, String)>,
}

pub fn topo_sort_diff(diff: &mut Diff) -> Result<(), String> {
    let created_order = topo_order_created(diff)?;
    let mut new_tables = Vec::with_capacity(diff.created_tables.len());
    let mut new_views = Vec::with_capacity(diff.created_views.len());
    let mut new_indices = Vec::with_capacity(diff.created_indices.len());
    for (kind, i) in &created_order {
        match kind {
            TopoKind::Table => new_tables.push(diff.created_tables[*i].clone()),
            TopoKind::View  => new_views.push(diff.created_views[*i].clone()),
            TopoKind::Index => new_indices.push(diff.created_indices[*i].clone()),
        }
    }
    diff.created_tables = new_tables;
    diff.created_views = new_views;
    diff.created_indices = new_indices;

    let dropped_order = topo_order_dropped(diff)?;
    let mut new_tables = Vec::with_capacity(diff.dropped_tables.len());
    let mut new_views = Vec::with_capacity(diff.dropped_views.len());
    let mut new_indices = Vec::with_capacity(diff.dropped_indices.len());
    for (kind, i) in &dropped_order {
        match kind {
            TopoKind::Table => new_tables.push(diff.dropped_tables[*i].clone()),
            TopoKind::View  => new_views.push(diff.dropped_views[*i].clone()),
            TopoKind::Index => new_indices.push(diff.dropped_indices[*i].clone()),
        }
    }
    diff.dropped_tables = new_tables;
    diff.dropped_views = new_views;
    diff.dropped_indices = new_indices;

    Ok(())
}

fn build_nodes_from_sets(
    tables: &[TableDef], views: &[ViewDef], indices: &[IndexDef],
) -> (Vec<TopoNode>, HashMap<(String, String), usize>) {
    let mut nodes: Vec<TopoNode> = Vec::new();
    let mut key_to_id: HashMap<(String, String), usize> = HashMap::new();

    for (i, t) in tables.iter().enumerate() {
        let key = (t.schema.clone(), t.name.clone());
        let mut deps: Vec<(String, String)> = Vec::new();
        for c in &t.columns {
            if !c.fk_table.is_empty() {
                deps.push((c.fk_schema.clone(), c.fk_table.clone()));
            }
        }
        let id = nodes.len();
        key_to_id.insert(key.clone(), id);
        nodes.push(TopoNode { kind: TopoKind::Table, idx: i, key, deps });
    }
    let table_needles: Vec<((String, String), String, String)> = key_to_id.keys()
        .map(|k| (
            k.clone(),
            format!("{}.{}", k.0.to_ascii_lowercase(), k.1.to_ascii_lowercase()),
            k.1.to_ascii_lowercase(),
        ))
        .collect();
    let view_names_lower: Vec<String> = views.iter()
        .map(|v| v.name.to_ascii_lowercase()).collect();
    for (i, v) in views.iter().enumerate() {
        let key = (v.schema.clone(), v.name.clone());
        let sql_lower = v.sql.to_ascii_lowercase();
        let mut deps: Vec<(String, String)> = Vec::new();
        for (k, needle, bare) in &table_needles {
            if sql_lower.contains(needle) || sql_lower.contains(bare) {
                deps.push(k.clone());
            }
        }
        for (j, other) in views.iter().enumerate() {
            if i == j { continue; }
            if sql_lower.contains(&view_names_lower[j]) {
                deps.push((other.schema.clone(), other.name.clone()));
            }
        }
        let id = nodes.len();
        key_to_id.insert(key.clone(), id);
        nodes.push(TopoNode { kind: TopoKind::View, idx: i, key, deps });
    }
    for (i, idx) in indices.iter().enumerate() {
        let key = (idx.schema.clone(), idx.name.clone());
        let deps = vec![(idx.owner_schema.clone(), idx.owner_name.clone())];
        let id = nodes.len();
        key_to_id.insert(key.clone(), id);
        nodes.push(TopoNode { kind: TopoKind::Index, idx: i, key, deps });
    }
    (nodes, key_to_id)
}

fn topo_order_created(diff: &Diff) -> Result<Vec<(TopoKind, usize)>, String> {
    let (nodes, key_to_id) = build_nodes_from_sets(
        &diff.created_tables, &diff.created_views, &diff.created_indices,
    );
    kahn_sort(&nodes, &key_to_id)
}

fn topo_order_dropped(diff: &Diff) -> Result<Vec<(TopoKind, usize)>, String> {
    let (nodes, key_to_id) = build_nodes_from_sets(
        &diff.dropped_tables, &diff.dropped_views, &diff.dropped_indices,
    );
    let mut order = kahn_sort(&nodes, &key_to_id)?;
    order.reverse();
    Ok(order)
}

fn kahn_sort(
    nodes: &[TopoNode], key_to_id: &HashMap<(String, String), usize>,
) -> Result<Vec<(TopoKind, usize)>, String> {
    use std::collections::VecDeque;
    let n = nodes.len();
    let mut indegree = vec![0usize; n];
    let mut fwd: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (id, node) in nodes.iter().enumerate() {
        for dep in &node.deps {
            if let Some(&src) = key_to_id.get(dep) {
                fwd[src].push(id);
                indegree[id] += 1;
            }
        }
    }
    let mut ready: VecDeque<usize> = (0..n).filter(|&i| indegree[i] == 0).collect();
    let mut out = Vec::with_capacity(n);
    while let Some(id) = ready.pop_front() {
        out.push((nodes[id].kind, nodes[id].idx));
        for &next in &fwd[id] {
            indegree[next] -= 1;
            if indegree[next] == 0 { ready.push_back(next); }
        }
    }
    if out.len() < n {
        let offender = nodes.iter().zip(indegree.iter())
            .find(|(_, &ind)| ind > 0)
            .map(|(n, _)| format!("{}.{}", n.key.0, n.key.1))
            .unwrap_or_else(|| "<unknown>".into());
        return Err(format!("cycle detected involving {}", offender));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_deterministic() {
        let s = DesiredState::default();
        let c = canonicalize(&s);
        let a = compute_migration_hash(0, &c, "a", "m");
        let b = compute_migration_hash(0, &c, "a", "m");
        assert_eq!(a, b);
    }

    #[test]
    fn canonical_roundtrip() {
        let s = DesiredState {
            tables: vec![TableDef {
                schema: "public".into(), name: "t".into(),
                columns: vec![ColumnDef {
                    name: "id".into(), type_code: 8, is_nullable: false,
                    fk_schema: String::new(), fk_table: String::new(), fk_col_idx: 0,
                }],
                pk_col_idx: 0, unique_pk: true,
            }],
            views: vec![], indices: vec![],
        };
        let bytes = canonicalize(&s);
        let decoded = decanonicalize(&bytes).unwrap();
        assert_eq!(s, decoded);
    }
}
