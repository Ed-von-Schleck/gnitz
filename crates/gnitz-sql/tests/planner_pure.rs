//! The planner as a pure function of `(statement, CatalogSnapshot)`.
//!
//! Every test here builds its catalog by hand and runs the real planning entry
//! points — no `ServerHandle`, no socket, no `GnitzClient`. That is the property
//! under test: compiling a `CREATE VIEW` to a `Circuit` and a `SELECT` to a
//! `ReadSpec` reaches nothing. It is also the one gnitz-sql test file not gated
//! on the `integration` feature, so a bare `cargo test` runs it.

use std::sync::Arc;

use gnitz_core::{
    is_segment_id, CatalogSnapshot, ColumnDef, IndexMeta, PkColList, RelClass, RelDescriptor, Schema, TypeCode,
};
use gnitz_sql::sqlparser::ast::Statement;
use gnitz_sql::sqlparser::dialect::GenericDialect;
use gnitz_sql::sqlparser::parser::Parser;
use gnitz_sql::{plan_read, plan_view, GnitzSqlError, ReadKind};

const SN: &str = "s";

/// `Err` unwrapped without demanding `Debug` of the success value — a `ReadPlan`
/// is deliberately opaque.
fn err_of<T>(r: Result<T, GnitzSqlError>) -> GnitzSqlError {
    match r {
        Err(e) => e,
        Ok(_) => panic!("expected a rejection"),
    }
}

fn parse(sql: &str) -> Statement {
    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"))
        .into_iter()
        .next()
        .expect("one statement")
}

fn col(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, false)
}

/// `t(id BIGINT PK, g BIGINT, v BIGINT)`, optionally with an index on `v`.
fn relation(tid: u64, class: RelClass, indexed: bool) -> Arc<RelDescriptor> {
    Arc::new(RelDescriptor {
        tid,
        class,
        replicated: false,
        delta: false,
        schema: Arc::new(Schema {
            columns: vec![
                col("id", TypeCode::I64),
                col("g", TypeCode::I64),
                col("v", TypeCode::I64),
            ],
            pk_cols: vec![0],
        }),
        indexes: Arc::new(if indexed {
            vec![IndexMeta {
                cols: PkColList::from_slice(&[2]),
                is_unique: false,
            }]
        } else {
            Vec::new()
        }),
    })
}

/// `t` (indexed on `v`) and `u` — the two relations every body below names.
fn catalog() -> CatalogSnapshot {
    let mut cat = CatalogSnapshot::default();
    cat.insert(SN, "t", Some(relation(16, RelClass::Table, true)));
    cat.insert(SN, "u", Some(relation(17, RelClass::Table, false)));
    cat
}

/// Plan `sql` the way `dispatch` does: run the pass, resolve each name it
/// reports missing out of `known`, and re-run. Returns the plan and the names
/// resolved, in the order the planner asked for them.
fn resolving<T>(
    known: &CatalogSnapshot,
    mut plan: impl FnMut(&CatalogSnapshot) -> Result<T, GnitzSqlError>,
) -> (Result<T, GnitzSqlError>, Vec<String>) {
    let mut cat = CatalogSnapshot::default();
    let mut asked = Vec::new();
    loop {
        match plan(&cat) {
            Err(GnitzSqlError::CatalogMiss(name)) => {
                assert!(
                    cat.get(SN, &name).is_none(),
                    "the loop must not be re-asked for '{name}'"
                );
                cat.insert(SN, &name, known.get(SN, &name).flatten());
                asked.push(name);
            }
            other => return (other, asked),
        }
    }
}

// ── CREATE VIEW ──────────────────────────────────────────────────────────────

/// A grouped body that computes on the way *into* its reduce — a composite GROUP
/// BY key or an aggregate over an expression — stays one segment. The `Project`
/// bind inserts for it is a map inside the reduce's own circuit, not a second
/// materialized relation: cutting it would cost a full second copy of the source,
/// maintained and checkpointed forever, for one arithmetic expression.
#[test]
fn a_computed_group_key_or_aggregate_argument_adds_no_segment() {
    let cat = catalog();
    for body in [
        "SELECT g, SUM(v) AS s FROM t GROUP BY g",
        "SELECT g, SUM(v * 2) AS s FROM t GROUP BY g",
        "SELECT g + v AS k, COUNT(*) AS n FROM t GROUP BY g + v",
        "SELECT g + v AS k, SUM(g * v) AS s FROM t GROUP BY g + v HAVING SUM(g * v) > 3",
        "SELECT g, SUM(v * 2) AS s FROM t WHERE v > 5 GROUP BY g",
    ] {
        let stmt = parse(&format!("CREATE VIEW vw AS {body}"));
        let views = plan_view(&stmt, &cat, SN).unwrap_or_else(|e| panic!("`{body}`: {e:?}"));
        assert_eq!(views.views.len(), 1, "`{body}`: one segment, no hidden materialization");
    }
}

/// Every view shape compiles to a bundle with no server anywhere: the
/// user-named view at chain-local slot 0 and last, distinct slots throughout,
/// and every id in every circuit still symbolic (a real relation id appears only
/// as a `ScanDelta` source).
#[test]
fn every_view_shape_compiles_with_no_server() {
    let cat = catalog();
    for body in [
        "SELECT id, v FROM t",
        "SELECT id, v * 2 AS d FROM t WHERE v > 5",
        "SELECT t.id, u.v FROM t JOIN u ON t.g = u.g",
        "SELECT t.id, u.v FROM t LEFT JOIN u ON t.g = u.g",
        "SELECT g, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY g",
        "SELECT g, SUM(v) AS s FROM t GROUP BY g HAVING SUM(v) > 10",
        "SELECT DISTINCT g FROM t",
        "SELECT g FROM t UNION SELECT g FROM u",
        "SELECT g FROM t EXCEPT SELECT g FROM u",
        "SELECT g FROM t INTERSECT ALL SELECT g FROM u",
        "WITH c AS (SELECT id, v FROM t WHERE v > 1) SELECT id FROM c",
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.g = t.g)",
        "SELECT id FROM t WHERE g IN (SELECT g FROM u)",
        "SELECT id, (SELECT MAX(v) FROM u WHERE u.g = t.g) AS m FROM t",
        "SELECT d.id FROM (SELECT id, v FROM t WHERE v > 2) d",
    ] {
        let stmt = parse(&format!("CREATE VIEW vw AS {body}"));
        let views = plan_view(&stmt, &cat, SN).unwrap_or_else(|e| panic!("`{body}`: {e:?}"));

        assert_eq!(views.name, "vw", "`{body}`: the bundle carries one user-facing name");
        let mut slots: Vec<u32> = views.views.iter().map(|pv| pv.seg).collect();
        slots.sort_unstable();
        let distinct = slots.windows(2).all(|w| w[0] != w[1]);
        assert!(distinct, "`{body}`: chain-local slots must be distinct: {slots:?}");
        assert_eq!(
            views.views.last().map(|pv| pv.seg),
            Some(0),
            "`{body}`: the user-named view is the chain's slot 0 and last"
        );
        for pv in &views.views {
            for dep in pv.circuit.dependencies() {
                assert!(
                    dep == 16 || dep == 17 || is_segment_id(dep),
                    "`{body}`: source {dep} is neither a named relation nor a segment"
                );
            }
        }
    }
}

/// `ALTER VIEW … AS` plans through the same entry point, and a body naming the
/// view it retargets is rejected — the symbolic ids never collide with the real
/// id the snapshot holds for it.
#[test]
fn alter_view_plans_and_rejects_self_reference() {
    let mut cat = catalog();
    cat.insert(SN, "vw", Some(relation(18, RelClass::View, false)));

    let ok = parse("ALTER VIEW vw AS SELECT id, v FROM t");
    let views = plan_view(&ok, &cat, SN).expect("a retarget over another relation plans");
    assert_eq!(views.name, "vw");

    let looping = parse("ALTER VIEW vw AS SELECT id, v FROM vw");
    assert!(matches!(
        plan_view(&looping, &cat, SN),
        Err(GnitzSqlError::Unsupported(_))
    ));
}

/// A view body naming N relations, planned by the resolve loop one name at a
/// time, emits byte-identical `CircuitRows` to the same body planned in one pass
/// against a pre-populated snapshot: the re-runs produce no drift in ids,
/// chain-local slots or node order.
#[test]
fn re_running_a_pass_emits_the_same_circuit() {
    let cat = catalog();
    for body in [
        "SELECT t.id, u.v FROM t JOIN u ON t.g = u.g",
        "WITH c AS (SELECT id, v FROM t WHERE v > 1) SELECT c.id FROM c JOIN u ON c.id = u.id",
        "SELECT g FROM t EXCEPT SELECT g FROM u",
    ] {
        let stmt = parse(&format!("CREATE VIEW vw AS {body}"));
        let (looped, asked) = resolving(&cat, |c| plan_view(&stmt, c, SN));
        let looped = looped.unwrap_or_else(|e| panic!("`{body}`: {e:?}"));
        assert_eq!(asked.len(), 2, "`{body}`: one resolve per named relation");

        let single = plan_view(&stmt, &cat, SN).unwrap();
        assert_eq!(looped.name, single.name, "`{body}`: view name");
        assert_eq!(looped.views.len(), single.views.len(), "`{body}`: segment count");
        for (a, b) in looped.views.into_iter().zip(single.views) {
            assert_eq!(a.seg, b.seg, "`{body}`: chain-local slot");
            assert_eq!(a.circuit, b.circuit, "`{body}`: circuit");
        }
    }
}

// ── SELECT ───────────────────────────────────────────────────────────────────

/// Every ad-hoc read shape plans to the sink it belongs on, and each case
/// reports the surface a caller reads it through.
#[test]
fn every_read_shape_plans_with_no_server() {
    let cat = catalog();
    for (sql, kind, dispatches) in [
        ("SELECT * FROM t", ReadKind::PlainScan, true),
        ("SELECT id FROM t", ReadKind::Rows, true),
        ("SELECT * FROM t WHERE id = 3", ReadKind::Rows, true),
        ("SELECT * FROM t WHERE v = 3", ReadKind::Rows, true),
        ("SELECT id FROM t ORDER BY v LIMIT 5 OFFSET 2", ReadKind::Rows, true),
        ("SELECT id FROM t LIMIT 0", ReadKind::Rows, false),
        ("SELECT COUNT(*) FROM t", ReadKind::Fold, true),
        ("SELECT g, SUM(v) AS s FROM t GROUP BY g", ReadKind::Fold, true),
        ("SELECT DISTINCT g FROM t", ReadKind::Fold, true),
        ("SELECT COUNT(*) FROM t LIMIT 0", ReadKind::Fold, false),
        ("WITH c AS (SELECT * FROM t) SELECT id FROM c", ReadKind::Rows, true),
    ] {
        let plan = plan_read(&parse(sql), &cat, SN).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        assert_eq!(plan.kind(), kind, "`{sql}`");
        assert_eq!(plan.target_id(), 16, "`{sql}`: the relation it reads");
        assert_eq!(plan.dispatches(), dispatches, "`{sql}`");
        match kind {
            ReadKind::PlainScan => {
                assert!(plan.reply_schema().is_none(), "`{sql}`: a plain scan projects nothing");
                assert!(plan.encoded_spec().is_none(), "`{sql}`: a plain scan ships no spec");
            }
            _ => {
                assert!(plan.reply_schema().is_some(), "`{sql}`");
                assert!(
                    !plan.encoded_spec().expect("a spec").is_empty(),
                    "`{sql}`: the spec is what ships"
                );
            }
        }
    }
}

/// The `EXPLAIN` of a query plans to the identical read — same sink, same
/// encoded spec — which is what makes describing a query describe the one the
/// `SELECT` would run.
#[test]
fn explain_plans_the_query_it_describes() {
    let cat = catalog();
    for sql in [
        "SELECT id FROM t WHERE v = 3 ORDER BY v LIMIT 4",
        "SELECT g, COUNT(*) FROM t GROUP BY g",
    ] {
        let direct = plan_read(&parse(sql), &cat, SN).unwrap();
        let described = plan_read(&parse(&format!("EXPLAIN {sql}")), &cat, SN).unwrap();
        assert_eq!(direct.kind(), described.kind(), "`{sql}`");
        assert_eq!(direct.encoded_spec(), described.encoded_spec(), "`{sql}`");
    }
}

/// An aggregate `SELECT` whose ORDER BY names a column the output does not carry
/// is rejected at plan time — before any request could be issued — and the
/// `EXPLAIN` of it is rejected identically, where describing it once meant
/// describing a plan the query cannot run.
#[test]
fn an_aggregate_order_by_on_a_missing_column_rejects_at_plan_time() {
    let cat = catalog();
    let sql = "SELECT g, COUNT(*) AS n FROM t GROUP BY g ORDER BY nope";
    let direct = err_of(plan_read(&parse(sql), &cat, SN));
    let described = err_of(plan_read(&parse(&format!("EXPLAIN {sql}")), &cat, SN));
    assert!(matches!(direct, GnitzSqlError::Bind(_)), "got {direct:?}");
    assert_eq!(format!("{direct}"), format!("{described}"), "both tails reject alike");
}

// ── The resolve loop ─────────────────────────────────────────────────────────

/// A pass over a snapshot missing one name reports it, and the same pass over
/// the snapshot with that name inserted succeeds — the loop's progress step.
#[test]
fn a_missing_name_is_reported_and_inserting_it_makes_progress() {
    let stmt = parse("SELECT id FROM t WHERE v = 1");
    let empty = CatalogSnapshot::default();
    match err_of(plan_read(&stmt, &empty, SN)) {
        GnitzSqlError::CatalogMiss(name) => assert_eq!(name, "t"),
        other => panic!("expected a catalog miss naming `t`, got {other:?}"),
    }
    assert!(plan_read(&stmt, &catalog(), SN).is_ok());
}

/// A name the snapshot holds as a **recorded absence** is the ordinary
/// not-found error, not a miss — otherwise the loop would re-ask for it forever.
#[test]
fn a_recorded_absence_is_not_found_not_a_miss() {
    let mut cat = CatalogSnapshot::default();
    cat.insert(SN, "t", None);
    let err = err_of(plan_read(&parse("SELECT id FROM t"), &cat, SN));
    assert!(!matches!(err, GnitzSqlError::CatalogMiss(_)), "got {err:?}");
    assert!(format!("{err}").contains("not found"), "got {err}");
}

/// The loop resolves one name per distinct relation the planner asks for, and
/// none for a name only the AST mentions: a CTE alias is answered from the
/// binder's own cache, and a query rejected before its first resolve costs
/// nothing.
#[test]
fn the_loop_resolves_only_what_the_planner_asks_for() {
    let cat = catalog();
    for (sql, want) in [
        ("SELECT id FROM t", vec!["t"]),
        ("SELECT * FROM t", vec!["t"]),
        // The CTE resolves the real `t` once; the alias never reaches the catalog.
        ("WITH t2 AS (SELECT * FROM t) SELECT id FROM t2", vec!["t"]),
        // A shadowing alias resolves its source once, and no more.
        ("WITH t AS (SELECT * FROM t) SELECT id FROM t", vec!["t"]),
    ] {
        let stmt = parse(sql);
        let (plan, asked) = resolving(&cat, |c| plan_read(&stmt, c, SN).map(|_| ()));
        assert!(plan.is_ok(), "`{sql}`: {:?}", plan.err());
        assert_eq!(asked, want, "`{sql}`");
    }

    // A derivation is rejected from the AST alone, before any name is resolved.
    let stmt = parse("SELECT t.id FROM t JOIN u ON t.g = u.g");
    let (plan, asked) = resolving(&cat, |c| plan_read(&stmt, c, SN).map(|_| ()));
    assert!(matches!(plan, Err(GnitzSqlError::Unsupported(_))));
    assert!(asked.is_empty(), "a derivation costs no resolve, asked: {asked:?}");

    // So is a reserved-prefix name: the funnel guard runs before the snapshot is
    // probed at all.
    let stmt = parse("SELECT id FROM _seg4096");
    let (plan, asked) = resolving(&cat, |c| plan_read(&stmt, c, SN).map(|_| ()));
    assert!(matches!(plan, Err(GnitzSqlError::Plan(_))), "got {plan:?}");
    assert!(asked.is_empty(), "a reserved-prefix name costs no resolve");
}

/// No statement shape lets a `CatalogMiss` or an `Internal` out of the loop —
/// the shapes that reach a relation name only from a deep position, where a pass
/// that forgot to report would surface as an invariant break instead.
#[test]
fn no_shape_leaks_a_control_signal_out_of_the_loop() {
    let cat = catalog();
    let bodies = [
        // A CTE body, and a CTE whose alias shadows a real relation.
        "WITH c AS (SELECT id, v FROM t) SELECT c.id FROM c JOIN u ON c.id = u.id",
        "WITH u AS (SELECT id, g, v FROM t) SELECT id FROM u",
        // A derived table in a view body.
        "SELECT d.id FROM (SELECT id, v FROM t WHERE v > 2) d",
        // EXISTS / IN / a scalar subquery.
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.g = t.g)",
        "SELECT id FROM t WHERE g IN (SELECT g FROM u)",
        "SELECT id, (SELECT MAX(v) FROM u WHERE u.g = t.g) AS m FROM t",
        // A set-op side and a nested join.
        "SELECT g FROM t UNION ALL SELECT g FROM u",
        "SELECT t.id FROM t JOIN u ON t.g = u.g JOIN t AS t2 ON t2.id = t.id",
        // Names that do not exist at all, in the same deep positions.
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM nope WHERE nope.g = t.g)",
        "SELECT d.id FROM (SELECT id FROM nope) d",
        "WITH c AS (SELECT id FROM nope) SELECT id FROM c",
    ];
    for body in bodies {
        for sql in [
            format!("CREATE VIEW vw AS {body}"),
            format!("SELECT * FROM ({body}) x"),
            format!("EXPLAIN {body}"),
            body.to_string(),
        ] {
            let Ok(stmt) = Parser::parse_sql(&GenericDialect {}, &sql).map(|mut v| v.remove(0)) else {
                continue;
            };
            let (out, _) = resolving(&cat, |c| match &stmt {
                Statement::CreateView(_) => plan_view(&stmt, c, SN).map(|_| ()),
                _ => plan_read(&stmt, c, SN).map(|_| ()),
            });
            if let Err(e @ (GnitzSqlError::CatalogMiss(_) | GnitzSqlError::Internal(_))) = out {
                panic!("`{sql}` leaked a control signal: {e:?}");
            }
        }
    }
}
