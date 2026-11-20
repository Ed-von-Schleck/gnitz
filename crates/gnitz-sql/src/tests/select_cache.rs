use super::*;
use crate::test_support::{catalog, col, in_sn, parse_query, table};
use gnitz_wire::TypeCode;

fn cached(sql: &str) -> Arc<CachedSelect> {
    insert(sql, parse_query(sql))
}

fn u(name: &str) -> gnitz_wire::ColumnDef {
    col(name, TypeCode::U64)
}

#[test]
fn a_rows_plan_is_reused_while_its_descriptors_stand() {
    let cat = catalog(vec![("t", table(1, vec![u("pk"), u("a")], vec![0]))]);
    let select = cached("SELECT * FROM t");
    let first = select.plan(&cat).unwrap();
    assert!(select.repeated(&cat).unwrap().is_some());
    let again = select.plan(&cat).unwrap();
    assert_eq!(again.spec(), first.spec());
    assert_eq!(again.reply_schema(), first.reply_schema());
}

#[test]
fn a_relation_that_resolves_anew_is_planned_anew() {
    let cat = catalog(vec![("t", table(1, vec![u("pk"), u("a")], vec![0]))]);
    let select = cached("SELECT * FROM t");
    select.plan(&cat).unwrap();

    // An equal descriptor that is another one is still a new answer.
    cat.insert(&in_sn("t"), table(1, vec![u("pk"), u("a")], vec![0]));
    assert!(select.repeated(&cat).unwrap().is_none());

    cat.insert(&in_sn("t"), table(2, vec![u("pk"), u("a"), u("b")], vec![0]));
    let plan = select.plan(&cat).unwrap();
    let names: Vec<_> = plan.reply_schema().columns().iter().map(|c| &*c.name).collect();
    assert_eq!(names, ["pk", "a", "b"]);
    assert!(select.repeated(&cat).unwrap().is_some());
}

#[test]
fn a_fold_or_a_failed_plan_is_not_kept() {
    let cat = catalog(vec![("t", table(1, vec![u("pk"), u("a")], vec![0]))]);
    let fold = cached("SELECT a, COUNT(*) AS n FROM t GROUP BY a");
    fold.plan(&cat).unwrap();
    assert!(fold.plan.lock().unwrap().is_none());

    let missing = cached("SELECT * FROM nowhere");
    assert!(missing.plan(&cat).is_err());
    assert!(missing.plan.lock().unwrap().is_none());
}

#[test]
fn a_script_is_kept_from_its_second_run() {
    let kept = |sql| matches!(parse(sql).unwrap(), Script::Select(_));
    let sql = "SELECT * FROM t WHERE pk = 4242424242";
    assert!(!kept(sql));
    assert!(kept(sql) && lookup(sql).is_some());
    assert!(kept(sql));
    let script = "SELECT * FROM t; SELECT * FROM t";
    assert!(!kept(script) && !kept(script));
    assert!(!ran_before(&"x".repeat(MAX_SCRIPT_LEN + 1)));
}

#[test]
fn a_cycle_through_one_more_script_than_kept_mostly_hits() {
    let scripts: Vec<String> = (0..=CAPACITY)
        .map(|i| format!("SELECT * FROM t WHERE pk = {i}"))
        .collect();
    let parsed: Vec<Query> = scripts.iter().map(|sql| parse_query(sql)).collect();
    let mut hits = 0;
    let rounds = 20;
    for round in 0..rounds + 2 {
        for (sql, query) in scripts.iter().zip(&parsed) {
            if lookup(sql).is_some() {
                hits += usize::from(round >= 2);
            } else if ran_before(sql) {
                insert(sql, query.clone());
            }
        }
        assert!(SELECTS.with(|s| s.borrow().kept.len()) <= CAPACITY);
    }
    let runs = rounds * scripts.len();
    assert!(hits * 4 >= runs * 3, "{hits} of {runs} runs hit");
}
