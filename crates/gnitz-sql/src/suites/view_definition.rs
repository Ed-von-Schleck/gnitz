//! A view's recorded definition: what `CREATE VIEW` keeps of its body, and the
//! body written back out under the names the catalog holds later.
//!
//! The contract is behavioural: a view's body written out after its sources and
//! their columns were renamed plans, over the renamed catalog, exactly the chain
//! the original text planned over the original one.

use std::sync::Arc;

use gnitz_core::{Catalog, CatalogRelation, RelDescriptor, RelProps};
use gnitz_wire::{CatalogColumn, ColumnDef, PkColList, RelClass, TableProps, ViewProps};

use crate::catalog::Catalog as _;

use super::*;
use crate::create_view_statement;
use crate::ddl::render::view_body;
use crate::ddl::view_def::ViewDefinition;

/// The base relations the bodies below read.
const SOURCES: &[&str] = &["t", "u", "a", "b", "n", "c", "r"];

/// `base()` with every source and every source column renamed, ids kept.
fn renamed() -> TestCatalog {
    let out = TestCatalog::default();
    for &name in SOURCES {
        out.insert(&in_sn(&format!("{name}_r")), renamed_desc(&source(name)));
    }
    out
}

fn source(name: &str) -> Arc<RelDescriptor> {
    base().probe(&in_sn(name)).unwrap()
}

fn renamed_desc(d: &RelDescriptor) -> Arc<RelDescriptor> {
    let cols = d
        .schema
        .columns()
        .iter()
        .map(|c| ColumnDef {
            name: format!("{}_r", c.name).into(),
            ..c.clone()
        })
        .collect();
    rel(d.tid, d.class, cols, d.schema.pk_cols().to_vec(), d.indexes.clone())
}

/// The client catalog holding `cat`'s sources under the names it gives them.
fn client_catalog(cat: &TestCatalog, names: &[String]) -> Catalog {
    let relations = names
        .iter()
        .map(|name| {
            let d = cat.probe(&in_sn(name)).unwrap();
            CatalogRelation {
                id: d.tid,
                schema: SN.to_string(),
                name: name.clone(),
                props: RelProps::Table(TableProps::default()),
                columns: d
                    .schema
                    .columns()
                    .iter()
                    .map(|def| CatalogColumn { def: def.clone(), fk: None })
                    .collect(),
                pk: PkColList::from_slice(d.schema.pk_cols()),
                indexes: Vec::new(),
                reads: Vec::new(),
                definition: Vec::new(),
            }
        })
        .collect();
    Catalog { schemas: vec![SN.to_string()], relations }
}

fn definition(chain: &ViewBundle) -> ViewDefinition {
    ViewDefinition::decode(&chain.definition).unwrap()
}

/// One chain member as committed: its encoded circuit, its key and its
/// PK-repeat flag.
type Member = (Vec<u8>, Vec<u32>, bool);

/// What a chain commits that a reader can tell apart: every member, and the
/// user-named view's visible columns.
fn shape(chain: &ViewBundle) -> (Vec<Member>, Vec<String>) {
    let members = all_views(chain)
        .map(|v| (v.circuit.encode(), v.schema.pk_cols().to_vec(), v.pk_repeats))
        .collect();
    let names = final_view(chain)
        .schema
        .visible_columns()
        .map(|(_, c)| c.name.to_string())
        .collect();
    (members, names)
}

const BODIES: &[&str] = &[
    "SELECT id, v FROM t WHERE g > 3",
    "SELECT * FROM t",
    "SELECT x.id, x.v + 1 AS w FROM t AS x WHERE x.g = 2",
    "SELECT x.id FROM t x",
    "SELECT d.p, d.q FROM t AS d(i, p, q)",
    "SELECT g, SUM(v) AS s, COUNT(*) AS n FROM t GROUP BY g",
    "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY g HAVING COUNT(*) > 1",
    "SELECT DISTINCT g FROM t",
    "SELECT a.id, b.w FROM a JOIN b ON a.k = b.k",
    "SELECT a.id, b.w FROM a LEFT JOIN b ON a.k = b.k",
    "SELECT x.id, y.w FROM a AS x FULL JOIN b AS y ON x.k = y.k",
    "SELECT n.id, b.w FROM n LEFT JOIN b ON n.k = b.k",
    "SELECT id FROM t EXCEPT SELECT id FROM u",
    "SELECT id, g FROM t INTERSECT SELECT id, g FROM u",
    "SELECT id, g FROM t UNION ALL SELECT id, g FROM u",
    "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.g = t.g)",
    "SELECT id FROM t WHERE g IN (SELECT g FROM u)",
    "SELECT id FROM t AS o WHERE NOT EXISTS (SELECT 1 FROM u AS i WHERE i.g = o.g)",
    "WITH q AS (SELECT g, SUM(v) AS s FROM t GROUP BY g) SELECT q.g, q.s, r.v FROM q JOIN r ON q.g = r.id",
    "WITH t AS (SELECT id, g FROM u) SELECT t.id FROM t",
    "SELECT id, g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn FROM t QUALIFY rn <= 3",
    "SELECT id, g, SUM(v) OVER (PARTITION BY g ORDER BY id) AS run FROM t",
    "SELECT a, b, v FROM c WHERE a > 1",
    "SELECT c.a, SUM(c.v) AS s FROM s.c GROUP BY c.a",
    "SELECT t.g, COUNT(*) AS n FROM t JOIN u ON t.g = u.g GROUP BY t.g",
    "SELECT id, v FROM t ORDER BY v DESC LIMIT 10",
    "SELECT a.id, s.tot FROM a JOIN (SELECT k, SUM(w) AS tot FROM b GROUP BY k) AS s ON a.k = s.k",
    "SELECT id, CASE WHEN v > 0 THEN 'p' ELSE 'n' END AS sign FROM t",
    "SELECT t.id, u.id AS uid FROM t, u WHERE t.id = u.id",
    "(SELECT id FROM t) UNION ALL (SELECT id FROM u)",
];

#[test]
fn a_body_written_back_over_renamed_sources_plans_the_original_chain() {
    let (orig, ren) = (base(), renamed());
    let names: Vec<String> = SOURCES.iter().map(|n| format!("{n}_r")).collect();
    let catalog = client_catalog(&ren, &names);
    let mut checked = 0;
    for body in BODIES {
        let before = view(&orig, body);
        let written = view_body(&catalog, &definition(&before)).unwrap();
        let after = plan(&ren, &format!("CREATE VIEW v AS {written}"))
            .unwrap_or_else(|e| panic!("`{body}` written back as `{written}`: {e:?}"));
        assert!(
            shape(&before) == shape(&after),
            "`{body}` written back as `{written}` plans another chain"
        );
        checked += 1;
    }
    assert_eq!(checked, BODIES.len());
}

#[test]
fn a_body_written_back_unrenamed_is_its_text_with_qualified_names() {
    let cat = base();
    let names: Vec<String> = SOURCES.iter().map(|n| n.to_string()).collect();
    let catalog = client_catalog(&cat, &names);
    let cases = [
        ("SELECT id FROM t", "SELECT id FROM s.t"),
        ("SELECT x.id FROM t AS x", "SELECT x.id FROM s.t AS x"),
        (
            "SELECT id FROM s.t WHERE g IN (SELECT g FROM u)",
            "SELECT id FROM s.t WHERE g IN (SELECT g FROM s.u)",
        ),
        (
            "WITH t AS (SELECT id FROM u) SELECT id FROM t",
            "WITH t AS (SELECT id FROM s.u) SELECT id FROM t",
        ),
    ];
    for (body, written) in cases {
        let chain = view(&cat, body);
        assert_eq!(view_body(&catalog, &definition(&chain)).unwrap(), written, "{body}");
    }
}

/// A source column renamed in case alone is renamed: the body reads it under
/// its old name, which the written-back body restores with a column alias list.
#[test]
fn a_body_written_back_over_a_recased_column_restores_its_old_name() {
    let cat = base();
    let names: Vec<String> = SOURCES.iter().map(|n| n.to_string()).collect();
    let mut catalog = client_catalog(&cat, &names);
    let t = catalog.relations.iter_mut().find(|r| r.name == "t").unwrap();
    let old: Vec<String> = t.columns.iter().map(|c| c.def.name.to_string()).collect();
    let v = t.columns.iter_mut().find(|c| &*c.def.name == "v").unwrap();
    v.def.name = "V".into();
    let chain = view(&cat, "SELECT id, v FROM t");
    assert_eq!(
        view_body(&catalog, &definition(&chain)).unwrap(),
        format!("SELECT id, v FROM s.t AS t({})", old.join(", "))
    );
}

#[test]
fn a_definition_keeps_the_body_verbatim_and_names_each_source_once() {
    let sql =
        "CREATE VIEW v WITH (delta = '1 MB') AS\n  -- kept\n  SELECT T.id FROM \"t\" T WHERE g IN (SELECT g FROM u)";
    let chain = plan(&base(), sql).unwrap();
    let def = definition(&chain);
    assert_eq!(
        def.query,
        "-- kept\n  SELECT T.id FROM \"t\" T WHERE g IN (SELECT g FROM u)"
    );
    let named: Vec<(&str, u64)> = def
        .refs
        .iter()
        .map(|r| (&def.query[r.name.start as usize..r.name.end as usize], r.tid))
        .collect();
    assert_eq!(named, [("\"t\"", source("t").tid), ("u", source("u").tid)]);
    let t = &def.refs[0];
    assert_eq!(&def.query[t.name.end as usize..t.alias_end.unwrap() as usize], " T");
    assert_eq!(t.alias, "T");
    assert_eq!(def.refs[1].alias_end, None);
    assert_eq!(def.body_columns, ["id"]);
}

#[test]
fn a_view_column_list_survives_only_where_the_body_names_differ() {
    let cat = base();
    let names: Vec<String> = SOURCES.iter().map(|n| n.to_string()).collect();
    let mut catalog = client_catalog(&cat, &names);
    let renaming = plan(
        &cat,
        "CREATE VIEW v (k, total) AS SELECT g, SUM(v) AS s FROM t GROUP BY g",
    )
    .unwrap();
    let plain = plan(&cat, "CREATE VIEW w AS SELECT g FROM t").unwrap();
    // A name differing in case alone is a different name: the catalog keeps it
    // as written.
    let recased = plan(&cat, "CREATE VIEW x (G) AS SELECT g FROM t").unwrap();
    for (name, chain) in [("v", &renaming), ("w", &plain), ("x", &recased)] {
        let pv = final_view(chain);
        catalog.relations.push(CatalogRelation {
            id: 100 + catalog.relations.len() as u64,
            schema: SN.to_string(),
            name: name.to_string(),
            props: RelProps::View(ViewProps::default()),
            columns: pv
                .schema
                .columns()
                .iter()
                .map(|def| CatalogColumn { def: def.clone(), fk: None })
                .collect(),
            pk: PkColList::from_slice(pv.schema.pk_cols()),
            indexes: Vec::new(),
            reads: Vec::new(),
            definition: chain.definition.clone(),
        });
    }
    let statement = |name: &str| {
        let rel = catalog.relations.iter().find(|r| r.name == name).unwrap();
        create_view_statement(&catalog, rel).unwrap()
    };
    assert_eq!(
        statement("v"),
        "CREATE VIEW s.v (k, total) AS\nSELECT g, SUM(v) AS s FROM s.t GROUP BY g"
    );
    assert_eq!(statement("w"), "CREATE VIEW s.w AS\nSELECT g FROM s.t");
    assert_eq!(statement("x"), "CREATE VIEW s.x (G) AS\nSELECT g FROM s.t");
}

#[test]
fn a_view_over_a_view_records_the_view_it_reads() {
    let cat = base();
    let chain = view(&cat, "SELECT tv.id FROM tv JOIN t ON tv.id = t.id");
    let tids: Vec<u64> = definition(&chain).refs.iter().map(|r| r.tid).collect();
    let tv = cat.probe(&in_sn("tv")).unwrap().tid;
    assert_eq!(tids, [tv, source("t").tid]);
    assert_eq!(cat.probe(&in_sn("tv")).unwrap().class, RelClass::View);
}
