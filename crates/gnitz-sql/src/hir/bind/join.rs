//! FROM-join binding: one join step folded onto the accumulated left input.

use super::super::{ColId, HirCol, JoinType, RelExpr};
use super::{resolve_table_factor, BindCx, JoinScope, ScopeLeaf, SubPolicy};
use crate::bind::{bind_conjuncts, find_unique_column, unsupported_subquery};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp};
use sqlparser::ast::{JoinConstraint, JoinOperator, TableFactor};
use std::rc::Rc;

/// What supplies one join step's key columns, and the step's type — orthogonal in
/// the grammar, so `NATURAL LEFT JOIN` is `(JoinConstraint::Natural, JoinType::Left)`.
pub(super) fn join_keys_and_type(join: &sqlparser::ast::Join) -> Result<(&JoinConstraint, JoinType), GnitzSqlError> {
    let sqlparser::ast::Join {
        relation: _, // resolved by the caller as the step's right input
        // Inert: ClickHouse's `GLOBAL` asks for evaluation against the whole
        // right relation, which a DBSP bilinear join already does.
        global: _,
        join_operator,
    } = join;
    Ok(match join_operator {
        JoinOperator::Inner(c) | JoinOperator::Join(c) => (c, JoinType::Inner),
        JoinOperator::LeftOuter(c) | JoinOperator::Left(c) => (c, JoinType::Left),
        JoinOperator::RightOuter(c) | JoinOperator::Right(c) => (c, JoinType::Right),
        JoinOperator::FullOuter(c) => (c, JoinType::Full),
        JoinOperator::CrossJoin(c) => (c, JoinType::Inner),
        _ => {
            return Err(GnitzSqlError::Rejected(
                "JOIN: only INNER / LEFT / RIGHT / FULL JOIN, with ON / USING / \
                 NATURAL, are supported"
                    .into(),
            ))
        }
    })
}

/// Fold one join step onto the accumulated left input: resolve the right
/// relation, derive the step's ON conjuncts from its constraint, merge away the
/// duplicate copy of each `USING` / `NATURAL` column, and widen the scope by the
/// step's null semantics.
pub(super) fn fold_join_step(
    cx: &mut BindCx<'_>,
    scope: &mut JoinScope,
    left: Rc<RelExpr>,
    relation: &TableFactor,
    (keys, kind): (&JoinConstraint, JoinType),
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let (right_src, ralias, rcols) = resolve_table_factor(cx, relation)?;
    // Under one alias twice, `alias.col` would name a column of either relation.
    if scope.relations.iter().any(|(a, _)| a.eq_ignore_ascii_case(&ralias)) {
        return Err(GnitzSqlError::Rejected(format!(
            "relation alias '{ralias}' is used by two relations of one FROM; rename one"
        )));
    }

    // Before `scope.push` below: a `USING`/`NATURAL` name pairs against the left
    // side alone, where an `ON` (bound after the push) sees both sides.
    let (pairs, merge_clause) = match keys {
        JoinConstraint::Using(cols) => {
            let mut names = Vec::with_capacity(cols.len());
            for c in cols {
                let name = crate::ast_util::single_part_ident(c)
                    .ok_or_else(|| GnitzSqlError::Rejected("JOIN USING: column must be a simple identifier".into()))?;
                names.push(name.to_string());
            }
            crate::rules::reject_duplicate_names(names.iter().map(String::as_str), "JOIN USING")?;
            (merge_pairs(scope, &rcols, &names, "USING")?, "USING")
        }
        JoinConstraint::Natural => {
            // SQL's rule: no shared name makes the step keyless, i.e. a CROSS
            // JOIN. The keyless guard decides it like any other keyless step.
            (
                merge_pairs(scope, &rcols, &scope.shared_names(&rcols), "NATURAL")?,
                "NATURAL",
            )
        }
        JoinConstraint::On(_) | JoinConstraint::None => (Vec::new(), ""),
    };

    scope.push(&ralias, rcols);

    // Every other form states its keys as `pairs`, empty for a keyless step — so a
    // step with no constraint of its own needs no arm, and the WHERE may key it.
    let on = match keys {
        JoinConstraint::On(e) => {
            let leaf = ScopeLeaf {
                scope,
                sub: SubPolicy::Reject(unsupported_subquery),
            };
            bind_conjuncts(e, &leaf).map_err(|e| e.in_clause("JOIN ON"))?
        }
        _ => pairs
            .iter()
            .map(|&(l, r)| BExpr::bin(BExpr::ColRef(l), BinOp::Eq, BExpr::ColRef(r)))
            .collect(),
    };

    if kind == JoinType::Full && !pairs.is_empty() {
        return Err(GnitzSqlError::Rejected(format!(
            "FULL JOIN … {merge_clause} is not supported: the merged column would be \
             COALESCE(left, right), which the join scope cannot name — the merge \
             retargets the shared name to one side's column, and a column reference \
             cannot name an expression. Write the equality as `ON …` and project the \
             two columns you want."
        )));
    }

    // The merged column's value is the PRESERVED side's copy, so the other side's
    // stops answering an unqualified reference and leaves `*`. `alias.col` still
    // reaches it — the two are distinct columns of the join output, and only the
    // one name they share was ambiguous.
    for &(l, r) in &pairs {
        scope.merge_away(if kind == JoinType::Right { l } else { r });
    }

    let out = RelExpr::join(left, right_src, kind, on)?;
    // Reflect this step's null-widening back into the scope so a later ON /
    // the WHERE / the projection resolve against the widened nullability.
    scope.widen_step(kind);
    Ok(out)
}

/// The `(left col, right col)` pair for each merged column name: the left copy
/// resolved against the scope as it stands, the right copy by name within the
/// incoming relation. `clause` names the surface for the error.
fn merge_pairs(
    scope: &JoinScope,
    rcols: &[HirCol],
    names: &[String],
    clause: &str,
) -> Result<Vec<(ColId, ColId)>, GnitzSqlError> {
    let mut pairs = Vec::with_capacity(names.len());
    for name in names {
        // `?` first: a name the left side carries twice is ambiguous, and the
        // scope's own wording says so — reporting it as "not found" would send
        // the reader looking for a column that is right there, twice.
        let l = scope.find_unqualified(name)?.ok_or_else(|| {
            GnitzSqlError::Rejected(format!(
                "JOIN {clause}: column '{name}' not found on the left of the join"
            ))
        })?;
        let idx = find_unique_column(rcols.iter().map(|c| &c.def), name)?.ok_or_else(|| {
            GnitzSqlError::Rejected(format!(
                "JOIN {clause}: column '{name}' not found on the right of the join"
            ))
        })?;
        pairs.push((l, rcols[idx].id));
    }
    Ok(pairs)
}
