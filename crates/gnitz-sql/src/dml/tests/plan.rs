use super::*;
use crate::expr_lower::compile_wire_conjuncts;
use crate::test_support::{bind_where, col, ix, pk_schema, schema, two_col};
use gnitz_wire::TypeCode;

/// The first candidate whose residual compiles serves the WHERE and ships exactly
/// the conjuncts it leaves; with no candidate the whole WHERE ships under no bound.
/// `Some(k)` names the candidate, by rank, whose bound the plan must carry.
#[test]
fn the_first_servable_candidate_bounds_the_read_and_ships_its_residual() {
    let by_pk = pk_schema(TypeCode::U64); // (id pk, v), v indexed
    let narrow = schema(["id", "v", "w"].map(|n| col(n, TypeCode::U64)).to_vec(), &[0]);
    // No register holds `val`, so a residual naming it does not compile.
    let wide = two_col(TypeCode::U128);
    for (s, sql, served_by, residual) in [
        (&by_pk, "", None, ""),
        (&by_pk, "id IN (7, 9) AND v > 5", Some(0), "v > 5"),
        // No conjunct any bound consumes.
        (&by_pk, "id + v = 7", None, "id + v = 7"),
        (&narrow, "v = 5 AND w = 9", Some(0), "w = 9"),
        // The PK walk would leave `val = 7` residual, so the index walk serves.
        (&wide, "pk > 5 AND val = 7", Some(1), "pk > 5"),
    ] {
        let conjuncts = |sql: &str| match sql {
            "" => Vec::new(),
            sql => bind_where(sql, s),
        };
        let (indexes, bound_conjuncts) = ([ix(&[1])], conjuncts(sql));
        let (bound, predicate) = bound_and_predicate(s, &bound_conjuncts, &indexes).expect(sql);
        let want = match served_by {
            Some(k) => candidates(&bound_conjuncts, s, &indexes).swap_remove(k).bound,
            None => ReadBound::None,
        };
        assert_eq!(bound, want, "{sql}");
        assert_eq!(
            predicate,
            compile_wire_conjuncts(&conjuncts(residual), &s.columns).unwrap(),
            "{sql}: the residual is what ships as a predicate"
        );
    }
}

/// When no candidate's residual compiles, the error is the best candidate's: the
/// conjunct that candidate could not consume, not one a later candidate left.
#[test]
fn an_unsupported_residual_reports_the_best_candidates_blocker() {
    let schema = schema(
        vec![
            col("id", TypeCode::U64),
            col("big", TypeCode::U128),
            col("name", TypeCode::String),
        ],
        &[0],
    );
    let indexes = [ix(&[1])];
    let conjuncts = bind_where("id > 5 AND big = 7 AND name + 1 > 0", &schema);
    let blockers: Vec<String> = candidates(&conjuncts, &schema, &indexes)
        .iter()
        .map(|c| {
            compile_wire_conjuncts(residual(&conjuncts, &c.consumed).iter().copied(), &schema.columns)
                .expect_err("every residual keeps `name + 1`")
                .to_string()
        })
        .collect();
    assert!(blockers.len() == 2 && blockers[0] != blockers[1], "{blockers:?}");
    let err = bound_and_predicate(&schema, &conjuncts, &indexes).expect_err("no residual compiles");
    assert_eq!(err.to_string(), blockers[0]);
}
