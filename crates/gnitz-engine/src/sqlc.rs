//! Server-side SQL compile — the entry point that `FLAG_EXECUTE_SQL`
//! will route through once Phase 2 wires it in. Turns a submitted SQL
//! string into a declarative `Compiled` record: new desired state,
//! newly allocated IDs, and (future: compiled view circuits).
//!
//! Phase 1 scope: DDL classification, DesiredState parse, ID allocation
//! for tables + indices. Does NOT yet compile views into `CircuitGraph`s
//! — the view-circuit port (from `gnitz-core::CircuitBuilder` and the
//! `gnitz-sql::planner` view arms) is the remaining Phase 1 work, tracked
//! separately. Until then, `compile` returns an empty `view_circuits`
//! map and any migration that creates a view will fail hash-compatibility
//! checks once the compiled-circuit field is part of the hash (Phase 2).
//!
//! Nothing calls this module yet; its only job in Phase 1 is to exist,
//! build, and have unit tests.

use std::collections::HashMap;

use gnitz_sql_parse::{classify_sql, GnitzSqlParseError};
use gnitz_sql_parse::migration::{parse_desired_state, DesiredState};

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

/// Output of `compile()`.
#[derive(Debug, Clone)]
pub struct Compiled {
    pub new_state:     DesiredState,
    pub allocated_ids: AllocatedIds,
    pub has_ddl:       bool,
    pub has_dml:       bool,
    // TODO(Phase-1-chunk-3): populate view_circuits via the ported
    // server-side circuit builder + view compile arms.
    // pub view_circuits: HashMap<(String, String), CircuitGraph>,
}

/// Parse `sql` and classify it, producing either a DesiredState (all-DDL),
/// an empty DesiredState with `has_dml=true` (all-DML), or an error
/// (mixed, parse failure, unsupported DDL shape).
///
/// `default_schema` is the schema name used for unqualified object
/// references. `current` is the parent migration's resolved state, used
/// to compute which tables/indices are new (so we only allocate IDs for
/// genuine additions — rebuilding the same object with the same
/// definition across migrations keeps its id).
pub fn compile(
    sql:            &str,
    default_schema: &str,
    current:        &DesiredState,
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
            new_state: DesiredState {
                tables: vec![], views: vec![], indices: vec![],
            },
            allocated_ids: AllocatedIds::default(),
            has_ddl: false,
            has_dml,
        });
    }

    // DDL. Hand off to the shared desired-state parser (which also
    // rejects DROP / ALTER / anything that isn't CREATE).
    let new_state     = parse_desired_state(sql, default_schema)?;
    let allocated_ids = allocate_ids(&new_state, current, hwms);

    Ok(Compiled {
        new_state,
        allocated_ids,
        has_ddl: true,
        has_dml: false,
    })
}

/// Mint IDs for every schema / table / index in `new_state` that is not
/// already present (by name) in `current`. Views are allocated from the
/// table-id sequence — Phase 1B chunk 3 will wire this once the view
/// parse path produces `ViewDef`s keyed by (schema, name) the same way
/// tables are.
fn allocate_ids(
    new_state: &DesiredState,
    current:   &DesiredState,
    hwms:      Hwms,
) -> AllocatedIds {
    let mut ids        = AllocatedIds::default();
    let mut next_table = hwms.next_table_id;
    let mut next_index = hwms.next_index_id;
    let mut next_schema = hwms.next_schema_id;

    // Tables — allocate for new names.
    for t in &new_state.tables {
        let key = (t.schema.clone(), t.name.clone());
        let existed = current.tables.iter()
            .any(|x| x.schema == t.schema && x.name == t.name);
        if !existed {
            ids.tables.insert(key, next_table);
            next_table = next_table.wrapping_add(1);
        }
    }

    // Views — share the table-id namespace.
    for v in &new_state.views {
        let key = (v.schema.clone(), v.name.clone());
        let existed = current.views.iter()
            .any(|x| x.schema == v.schema && x.name == v.name);
        if !existed {
            ids.tables.insert(key, next_table);
            next_table = next_table.wrapping_add(1);
        }
    }

    // Indices.
    for i in &new_state.indices {
        let key = (i.schema.clone(), i.name.clone());
        let existed = current.indices.iter()
            .any(|x| x.schema == i.schema && x.name == i.name);
        if !existed {
            ids.indices.insert(key, next_index);
            next_index = next_index.wrapping_add(1);
        }
    }

    // Schemas — any schema name referenced by a new object that doesn't
    // already appear in `current` gets a fresh id. (Schemas are not
    // first-class in DesiredState today; derived from object schema
    // names.)
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

    #[test]
    fn ddl_create_table_allocates_fresh_ids() {
        let sql = "CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY, email TEXT NOT NULL)";
        let c = compile(sql, "public", &empty(), hwms()).expect("compile ok");
        assert!(c.has_ddl);
        assert!(!c.has_dml);
        assert_eq!(c.new_state.tables.len(), 1);
        assert_eq!(
            c.allocated_ids.tables.get(&("public".into(), "users".into())),
            Some(&200),
        );
        assert_eq!(c.allocated_ids.schemas.get("public"), Some(&100));
    }

    #[test]
    fn ddl_existing_table_keeps_id_no_new_allocation() {
        let sql = "CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY)";
        let current = parse_desired_state(sql, "public").unwrap();
        let c = compile(sql, "public", &current, hwms()).expect("compile ok");
        // Still DDL (it's a CREATE) but nothing new to allocate.
        assert!(c.has_ddl);
        assert!(c.allocated_ids.tables.is_empty(),
            "expected no new table allocations, got {:?}", c.allocated_ids.tables);
    }

    #[test]
    fn ddl_create_table_plus_index_allocates_both() {
        let sql = "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL); \
                   CREATE UNIQUE INDEX i_x ON t (x)";
        let c = compile(sql, "public", &empty(), hwms()).expect("compile ok");
        assert!(c.has_ddl);
        assert_eq!(c.allocated_ids.tables.len(), 1);
        assert_eq!(c.allocated_ids.indices.len(), 1);
        assert_eq!(
            c.allocated_ids.indices.get(&("public".into(), "i_x".into())),
            Some(&300),
        );
    }

    #[test]
    fn dml_only_flags_has_dml() {
        let sql = "INSERT INTO users VALUES (1, 'a@example.com')";
        let c = compile(sql, "public", &empty(), hwms()).expect("compile ok");
        assert!(!c.has_ddl);
        assert!(c.has_dml);
        assert!(c.allocated_ids.tables.is_empty());
        assert!(c.new_state.tables.is_empty());
    }

    #[test]
    fn select_counts_as_dml() {
        let sql = "SELECT * FROM users";
        let c = compile(sql, "public", &empty(), hwms()).expect("compile ok");
        assert!(!c.has_ddl);
        assert!(c.has_dml);
    }

    #[test]
    fn mixed_ddl_dml_rejected() {
        let sql = "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY); \
                   INSERT INTO t VALUES (1)";
        let err = compile(sql, "public", &empty(), hwms()).expect_err("must reject");
        assert!(err.to_string().contains("cannot be mixed"),
            "unexpected err: {}", err);
    }

    #[test]
    fn parse_failure_surfaces_as_parse_err() {
        let sql = "CREATE TABLE whoops (";
        let err = compile(sql, "public", &empty(), hwms()).expect_err("must reject");
        assert!(matches!(err, GnitzSqlParseError::Parse(_)),
            "expected Parse err, got {:?}", err);
    }

    #[test]
    fn unsupported_ddl_shape_rejected() {
        // DROP is not yet supported by parse_desired_state; must surface
        // as an Unsupported error rather than silently skipping.
        let sql = "DROP TABLE t";
        let err = compile(sql, "public", &empty(), hwms()).expect_err("must reject");
        assert!(matches!(err, GnitzSqlParseError::Unsupported(_)),
            "expected Unsupported err, got {:?}", err);
    }

    #[test]
    fn schema_allocation_only_for_new_schemas() {
        // First migration introduces "app" schema.
        let sql1 = "CREATE TABLE app.users (id BIGINT UNSIGNED PRIMARY KEY)";
        let c1 = compile(sql1, "public", &empty(), hwms()).expect("compile ok");
        assert_eq!(c1.allocated_ids.schemas.get("app"), Some(&100));

        // Second migration reuses "app" — should NOT re-allocate.
        let current = parse_desired_state(sql1, "public").unwrap();
        let sql2 = "CREATE TABLE app.users (id BIGINT UNSIGNED PRIMARY KEY); \
                    CREATE TABLE app.orders (id BIGINT UNSIGNED PRIMARY KEY)";
        let c2 = compile(sql2, "public", &current, hwms()).expect("compile ok");
        assert!(!c2.allocated_ids.schemas.contains_key("app"),
            "schema 'app' already existed; must not re-allocate");
        assert!(c2.allocated_ids.tables.contains_key(&("app".into(), "orders".into())));
    }

    #[test]
    fn default_schema_applies_to_unqualified_names() {
        let sql = "CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY)";
        let c = compile(sql, "my_schema", &empty(), hwms()).expect("compile ok");
        assert_eq!(c.new_state.tables[0].schema, "my_schema");
        assert!(c.allocated_ids.schemas.contains_key("my_schema"));
    }
}
