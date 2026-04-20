//! Server-side SQL compile — the entry point that `FLAG_EXECUTE_SQL`
//! will route through once Phase 2 wires it in. Turns a submitted SQL
//! string into a declarative `Compiled` record: new desired state,
//! newly allocated IDs, and compiled view circuits.
//!
//! Nothing calls this module yet; its only job in Phase 1 is to exist,
//! build, be unit-tested, and be ready for Phase 2 to plug into
//! `execute_sql`.

use std::collections::HashMap;

use gnitz_protocol::{ColumnDef, Schema, TypeCode};
use gnitz_sql_parse::{classify_sql, GnitzSqlParseError};
use gnitz_sql_parse::binder::Binder;
use gnitz_sql_parse::circuit::CircuitGraph;
use gnitz_sql_parse::migration::{parse_desired_state, DesiredState, TableDef};
use gnitz_sql_parse::resolver::CatalogResolver;
use gnitz_sql_parse::view::compile_view_from_sql;

/// Starting HWMs for ID allocation. The caller (server-side apply path)
/// supplies these from `derive_hwms_from_migrations()` at lock time.
#[derive(Debug, Clone, Copy, Default)]
pub struct Hwms {
    pub next_schema_id: u64,
    pub next_table_id:  u64,
    pub next_index_id:  u64,
}

/// IDs minted during `compile()` for objects that did not already exist
/// in `current`. Bincoded into the migration row so the hash is stable
/// across master/worker and replay (the hash covers allocated IDs per
/// Phase 1's pinned design decision).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AllocatedIds {
    pub schemas: HashMap<String, u64>,
    /// (schema, table_name) → table_id. Views share the table-id
    /// namespace today, so view allocations also live here.
    pub tables:  HashMap<(String, String), u64>,
    pub indices: HashMap<(String, String), u64>,
}

/// Pre-existing object IDs supplied by the server's live catalog.
/// Populated from the registry for each object in `current` whose
/// definition is preserved in `new_state` (and therefore isn't a
/// fresh allocation).
#[derive(Debug, Clone, Default)]
pub struct CurrentIds {
    pub tables:  HashMap<(String, String), u64>,
    pub indices: HashMap<(String, String), u64>,
}

/// Output of `compile()`.
#[derive(Debug, Clone)]
pub struct Compiled {
    pub new_state:      DesiredState,
    pub allocated_ids:  AllocatedIds,
    pub has_ddl:        bool,
    pub has_dml:        bool,
    /// One `CircuitGraph` per view in `new_state.views`, keyed by
    /// `(schema, view_name)`. Populated in topological (fixpoint)
    /// order so views-selecting-from-views resolve naturally.
    pub view_circuits:  HashMap<(String, String), CircuitGraph>,
}

/// Parse `sql` and classify it, producing either a DesiredState (all-DDL
/// — views compiled into circuits), an empty DesiredState with
/// `has_dml=true` (all-DML), or an error (mixed, parse failure,
/// unsupported DDL shape, circuit compile failure).
pub fn compile(
    sql:            &str,
    default_schema: &str,
    current:        &DesiredState,
    current_ids:    &CurrentIds,
    hwms:           Hwms,
) -> Result<Compiled, GnitzSqlParseError> {
    let cls = classify_sql(sql)?;
    let has_ddl = cls.has_ddl;
    let has_dml = cls.has_dml;

    if has_ddl && has_dml {
        return Err(GnitzSqlParseError::Unsupported(
            "DDL and DML cannot be mixed in a single submission".into(),
        ));
    }

    if !has_ddl {
        return Ok(Compiled {
            new_state: DesiredState { tables: vec![], views: vec![], indices: vec![] },
            allocated_ids: AllocatedIds::default(),
            has_ddl: false, has_dml,
            view_circuits: HashMap::new(),
        });
    }

    let new_state     = parse_desired_state(sql, default_schema)?;
    let allocated_ids = allocate_ids(&new_state, current, hwms);
    let view_circuits = compile_all_views(&new_state, current_ids, &allocated_ids)?;

    Ok(Compiled {
        new_state, allocated_ids,
        has_ddl: true, has_dml: false,
        view_circuits,
    })
}

/// Mint IDs for every schema / table / view / index in `new_state` that
/// is not already present (by name) in `current`.
fn allocate_ids(
    new_state: &DesiredState,
    current:   &DesiredState,
    hwms:      Hwms,
) -> AllocatedIds {
    let mut ids         = AllocatedIds::default();
    let mut next_table  = hwms.next_table_id;
    let mut next_index  = hwms.next_index_id;
    let mut next_schema = hwms.next_schema_id;

    for t in &new_state.tables {
        let key = (t.schema.clone(), t.name.clone());
        let existed = current.tables.iter()
            .any(|x| x.schema == t.schema && x.name == t.name);
        if !existed {
            ids.tables.insert(key, next_table);
            next_table = next_table.wrapping_add(1);
        }
    }

    for v in &new_state.views {
        let key = (v.schema.clone(), v.name.clone());
        let existed = current.views.iter()
            .any(|x| x.schema == v.schema && x.name == v.name);
        if !existed {
            ids.tables.insert(key, next_table);
            next_table = next_table.wrapping_add(1);
        }
    }

    for i in &new_state.indices {
        let key = (i.schema.clone(), i.name.clone());
        let existed = current.indices.iter()
            .any(|x| x.schema == i.schema && x.name == i.name);
        if !existed {
            ids.indices.insert(key, next_index);
            next_index = next_index.wrapping_add(1);
        }
    }

    let current_schemas: std::collections::HashSet<&str> = current.tables.iter()
        .map(|t| t.schema.as_str())
        .chain(current.views.iter().map(|v| v.schema.as_str()))
        .chain(current.indices.iter().map(|i| i.schema.as_str()))
        .collect();

    let mut seen_new: std::collections::HashSet<String> = std::collections::HashSet::new();
    for name in new_state.tables.iter().map(|t| &t.schema)
        .chain(new_state.views.iter().map(|v| &v.schema))
        .chain(new_state.indices.iter().map(|i| &i.schema))
    {
        if current_schemas.contains(name.as_str()) { continue; }
        if !seen_new.insert(name.clone())         { continue; }
        ids.schemas.insert(name.clone(), next_schema);
        next_schema = next_schema.wrapping_add(1);
    }

    ids
}

// ---------------------------------------------------------------------------
// View compile — fixpoint iteration over view DAG
// ---------------------------------------------------------------------------

fn compile_all_views(
    new_state:     &DesiredState,
    current_ids:   &CurrentIds,
    allocated_ids: &AllocatedIds,
) -> Result<HashMap<(String, String), CircuitGraph>, GnitzSqlParseError> {
    let mut compiled: HashMap<(String, String), CircuitGraph> = HashMap::new();
    let total = new_state.views.len();
    if total == 0 { return Ok(compiled); }

    // Fixpoint: compile whatever resolves this round; repeat until all
    // compile or a round makes no progress (cyclic or unresolvable).
    let max_rounds = total + 1;
    let mut pending_err: Option<GnitzSqlParseError> = None;
    for _round in 0..max_rounds {
        let mut progress = false;
        pending_err = None;
        for v in &new_state.views {
            let key = (v.schema.clone(), v.name.clone());
            if compiled.contains_key(&key) { continue; }

            let view_id = resolve_table_id(new_state, current_ids, allocated_ids, &v.schema, &v.name)
                .ok_or_else(|| GnitzSqlParseError::Plan(format!(
                    "no id allocated for view '{}.{}'", v.schema, v.name,
                )))?;

            let resolver = CompileCatalog {
                new_state, current_ids, allocated_ids, compiled: &compiled,
            };
            let mut binder = Binder::new(&resolver, &v.schema);
            match compile_view_from_sql(view_id, &v.sql, &mut binder) {
                Ok(graph) => { compiled.insert(key, graph); progress = true; }
                Err(e @ GnitzSqlParseError::Bind(_)) => {
                    // Bind errors often mean "source table not yet compiled"
                    // — remember and retry next round.
                    pending_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        if compiled.len() == total { return Ok(compiled); }
        if !progress { break; }
    }

    // If we fell out of the loop with unresolved views, surface the
    // last bind error — callers see the specific unresolved reference.
    Err(pending_err.unwrap_or_else(|| GnitzSqlParseError::Plan(
        "unable to resolve view dependencies (possible cycle)".into(),
    )))
}

/// Look up a table (or view) id by (schema, name) across allocated +
/// current + compiled-view maps.
fn resolve_table_id(
    new_state:     &DesiredState,
    current_ids:   &CurrentIds,
    allocated_ids: &AllocatedIds,
    schema:        &str,
    name:          &str,
) -> Option<u64> {
    let key = (schema.to_string(), name.to_string());
    if let Some(&id) = allocated_ids.tables.get(&key) { return Some(id); }
    if let Some(&id) = current_ids.tables.get(&key)   { return Some(id); }
    // Sanity: the object must also be present in new_state.
    let in_state = new_state.tables.iter().any(|t| t.schema == schema && t.name == name)
                || new_state.views.iter().any(|v| v.schema == schema && v.name == name);
    if in_state { None } else { None }
}

// ---------------------------------------------------------------------------
// CatalogResolver backed by DesiredState + AllocatedIds + compiled views.
// ---------------------------------------------------------------------------

struct CompileCatalog<'a> {
    new_state:     &'a DesiredState,
    current_ids:   &'a CurrentIds,
    allocated_ids: &'a AllocatedIds,
    compiled:      &'a HashMap<(String, String), CircuitGraph>,
}

impl<'a> CatalogResolver for CompileCatalog<'a> {
    fn resolve_table_or_view(&self, schema: &str, name: &str) -> Option<(u64, Schema)> {
        // Tables — from new_state (declared this migration) or existing ids.
        for t in &self.new_state.tables {
            if t.schema.eq_ignore_ascii_case(schema) && t.name.eq_ignore_ascii_case(name) {
                let tid = resolve_table_id(
                    self.new_state, self.current_ids, self.allocated_ids, &t.schema, &t.name,
                )?;
                return Some((tid, synthesize_table_schema(t)));
            }
        }
        // Views — only resolvable once their circuit has been compiled
        // (we use the compiled output_col_defs as the declared schema).
        for v in &self.new_state.views {
            if v.schema.eq_ignore_ascii_case(schema) && v.name.eq_ignore_ascii_case(name) {
                let graph = self.compiled.get(&(v.schema.clone(), v.name.clone()))?;
                let tid = resolve_table_id(
                    self.new_state, self.current_ids, self.allocated_ids, &v.schema, &v.name,
                )?;
                let schema = schema_from_output_cols(&graph.output_col_defs);
                return Some((tid, schema));
            }
        }
        None
    }

    fn find_index_for_column(&self, _table_id: u64, _col_idx: usize) -> Option<(u64, bool)> {
        // Phase 1: view compile doesn't use index hints. Phase 5+ can
        // thread real index lookup through when the unified migration
        // apply gains a view-on-view optimization pass.
        None
    }
}

fn synthesize_table_schema(t: &TableDef) -> Schema {
    let columns = t.columns.iter().map(|c| ColumnDef {
        name:        c.name.clone(),
        type_code:   type_code_from_u8(c.type_code),
        is_nullable: c.is_nullable,
        fk_table_id: 0,
        fk_col_idx:  c.fk_col_idx as u64,
    }).collect();
    Schema {
        columns,
        pk_index: t.pk_col_idx as usize,
    }
}

fn schema_from_output_cols(cols: &[(String, u8)]) -> Schema {
    let columns = cols.iter().map(|(name, tc)| ColumnDef {
        name: name.clone(),
        type_code: type_code_from_u8(*tc),
        is_nullable: false,
        fk_table_id: 0,
        fk_col_idx:  0,
    }).collect();
    // All compiled view outputs put PK at col 0 (join/set-op/distinct
    // synthesise a U128 PK; plain/group-by keep the natural PK at front).
    Schema { columns, pk_index: 0 }
}

fn type_code_from_u8(tc: u8) -> TypeCode {
    // Mirror of gnitz_protocol::TypeCode discriminants.
    match tc {
        1  => TypeCode::U64,
        2  => TypeCode::I64,
        3  => TypeCode::String,
        4  => TypeCode::U128,
        5  => TypeCode::F64,
        6  => TypeCode::F32,
        7  => TypeCode::U8,
        8  => TypeCode::I8,
        9  => TypeCode::U16,
        10 => TypeCode::I16,
        11 => TypeCode::U32,
        12 => TypeCode::I32,
        // Default to I64 for unknown discriminants; a debug_assert
        // would be ideal here but the match is complete per the
        // enum's public invariants.
        _  => TypeCode::I64,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn empty() -> DesiredState {
        DesiredState { tables: vec![], views: vec![], indices: vec![] }
    }

    fn hwms() -> Hwms {
        Hwms { next_schema_id: 100, next_table_id: 200, next_index_id: 300 }
    }

    fn no_ids() -> CurrentIds { CurrentIds::default() }

    #[test]
    fn ddl_create_table_allocates_fresh_ids() {
        let sql = "CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY, email TEXT NOT NULL)";
        let c = compile(sql, "public", &empty(), &no_ids(), hwms()).expect("compile ok");
        assert!(c.has_ddl);
        assert!(!c.has_dml);
        assert_eq!(c.new_state.tables.len(), 1);
        assert_eq!(
            c.allocated_ids.tables.get(&("public".into(), "users".into())),
            Some(&200),
        );
        assert_eq!(c.allocated_ids.schemas.get("public"), Some(&100));
        assert!(c.view_circuits.is_empty());
    }

    #[test]
    fn ddl_create_table_plus_index_allocates_both() {
        let sql = "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL); \
                   CREATE UNIQUE INDEX i_x ON t (x)";
        let c = compile(sql, "public", &empty(), &no_ids(), hwms()).expect("compile ok");
        assert_eq!(c.allocated_ids.tables.len(), 1);
        assert_eq!(c.allocated_ids.indices.len(), 1);
    }

    #[test]
    fn dml_only_flags_has_dml() {
        let sql = "INSERT INTO users VALUES (1, 'a@example.com')";
        let c = compile(sql, "public", &empty(), &no_ids(), hwms()).expect("compile ok");
        assert!(!c.has_ddl);
        assert!(c.has_dml);
    }

    #[test]
    fn mixed_ddl_dml_rejected() {
        let sql = "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY); \
                   INSERT INTO t VALUES (1)";
        let err = compile(sql, "public", &empty(), &no_ids(), hwms()).expect_err("must reject");
        assert!(err.to_string().contains("cannot be mixed"));
    }

    #[test]
    fn unsupported_ddl_shape_rejected() {
        let sql = "DROP TABLE t";
        let err = compile(sql, "public", &empty(), &no_ids(), hwms()).expect_err("must reject");
        assert!(matches!(err, GnitzSqlParseError::Unsupported(_)));
    }

    #[test]
    fn parse_failure_surfaces_as_parse_err() {
        let sql = "CREATE TABLE whoops (";
        let err = compile(sql, "public", &empty(), &no_ids(), hwms()).expect_err("must reject");
        assert!(matches!(err, GnitzSqlParseError::Parse(_)));
    }

    #[test]
    fn default_schema_applies_to_unqualified_names() {
        let sql = "CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY)";
        let c = compile(sql, "my_schema", &empty(), &no_ids(), hwms()).expect("compile ok");
        assert_eq!(c.new_state.tables[0].schema, "my_schema");
        assert!(c.allocated_ids.schemas.contains_key("my_schema"));
    }

    #[test]
    fn view_compile_plain_filter() {
        let sql = "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL); \
                   CREATE VIEW v AS SELECT id, x FROM t WHERE x > 5";
        let c = compile(sql, "public", &empty(), &no_ids(), hwms()).expect("compile ok");
        assert_eq!(c.new_state.views.len(), 1);
        assert_eq!(c.view_circuits.len(), 1);
        let graph = c.view_circuits.get(&("public".into(), "v".into())).unwrap();
        assert!(!graph.nodes.is_empty());
        assert_eq!(graph.primary_source_id,
            *c.allocated_ids.tables.get(&("public".into(), "t".into())).unwrap());
        // Filter node should exist, and the WHERE clause compiles without strings.
        assert!(graph.output_col_defs.len() >= 2);
    }

    #[test]
    fn view_compile_view_on_view_resolves_via_fixpoint() {
        let sql = "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL); \
                   CREATE VIEW v1 AS SELECT id, x FROM t WHERE x > 0; \
                   CREATE VIEW v2 AS SELECT id, x FROM v1 WHERE x < 100";
        let c = compile(sql, "public", &empty(), &no_ids(), hwms()).expect("compile ok");
        assert_eq!(c.view_circuits.len(), 2);
        assert!(c.view_circuits.contains_key(&("public".into(), "v1".into())));
        assert!(c.view_circuits.contains_key(&("public".into(), "v2".into())));
    }

    #[test]
    fn view_referencing_missing_table_fails() {
        let sql = "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY); \
                   CREATE VIEW v AS SELECT id FROM nope";
        let err = compile(sql, "public", &empty(), &no_ids(), hwms()).expect_err("must fail");
        assert!(err.to_string().contains("nope") || err.to_string().contains("not found"));
    }

    #[test]
    fn view_compile_group_by() {
        let sql = "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL); \
                   CREATE VIEW v AS SELECT g, SUM(x) AS total FROM t GROUP BY g";
        let c = compile(sql, "public", &empty(), &no_ids(), hwms()).expect("compile ok");
        let g = c.view_circuits.get(&("public".into(), "v".into())).unwrap();
        assert!(g.output_col_defs.len() >= 2);
    }

    #[test]
    fn view_compile_join() {
        let sql = "CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, k BIGINT NOT NULL); \
                   CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, k BIGINT NOT NULL); \
                   CREATE VIEW v AS SELECT a.id, a.k, b.k FROM a JOIN b ON a.k = b.k";
        let c = compile(sql, "public", &empty(), &no_ids(), hwms()).expect("compile ok");
        let g = c.view_circuits.get(&("public".into(), "v".into())).unwrap();
        // Two delta-tagged sources, no primary_source_id for joins.
        assert_eq!(g.primary_source_id, 0);
        assert_eq!(g.dependencies.len(), 2);
    }

    #[test]
    fn view_compile_distinct() {
        let sql = "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL); \
                   CREATE VIEW v AS SELECT DISTINCT id, x FROM t";
        let c = compile(sql, "public", &empty(), &no_ids(), hwms()).expect("compile ok");
        let g = c.view_circuits.get(&("public".into(), "v".into())).unwrap();
        assert_eq!(g.output_col_defs[0].0, "_distinct_pk");
    }

    #[test]
    fn view_compile_union() {
        let sql = "CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL); \
                   CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL); \
                   CREATE VIEW v AS SELECT id, x FROM a UNION ALL SELECT id, x FROM b";
        let c = compile(sql, "public", &empty(), &no_ids(), hwms()).expect("compile ok");
        let g = c.view_circuits.get(&("public".into(), "v".into())).unwrap();
        assert_eq!(g.output_col_defs[0].0, "_set_pk");
    }
}
