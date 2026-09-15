//! The decorrelated emitters of the join shell: a `Join{Semi|Anti|Mark}` over an
//! equi, band or pure-range correlation, each composed by [`exists_branches`].

use super::super::{JoinClass, JoinType};
use super::join::Branch;
use super::joincore::{equi_prologue, range_prologue};
use super::JoinSide;
use crate::error::GnitzSqlError;
use gnitz_core::{Circuit, NodeId};

/// One half of the outer input split by match existence; the other half is
/// `all − it`.
#[derive(Clone, Copy)]
enum Split {
    Matched(NodeId),
    Unmatched(NodeId),
}

/// The branches a decorrelated kind emits from one half of its outer input:
///
/// ```text
/// Semi = matched      Anti = unmatched      Mark = (matched @1, unmatched @0)
/// ```
///
/// The half `split` does not carry is `all − known`, built only when read.
fn exists_branches(cb: &mut Circuit, kind: JoinType, split: Split, all: NodeId) -> Vec<Branch> {
    let (Split::Matched(known) | Split::Unmatched(known)) = split;
    let complement = |cb: &mut Circuit| cb.difference(all, known);
    match (kind, split) {
        (JoinType::Semi, Split::Matched(half)) | (JoinType::Anti, Split::Unmatched(half)) => vec![(half, None)],
        (JoinType::Semi | JoinType::Anti, _) => vec![(complement(cb), None)],
        (JoinType::Mark(_), Split::Matched(matched)) => vec![(matched, Some(1)), (complement(cb), Some(0))],
        (JoinType::Mark(_), Split::Unmatched(unmatched)) => vec![(complement(cb), Some(1)), (unmatched, Some(0))],
        _ => unreachable!("exists_branches receives a decorrelated kind"),
    }
}

/// Equi correlation: a semi-join of A against B's key set, keyed by `_join_pk`.
pub(super) fn equi(
    cb: &mut Circuit,
    inputs: [NodeId; 2],
    class: &JoinClass,
    kind: JoinType,
    sides: &[JoinSide; 2],
    b_unique: bool,
) -> Result<Vec<Branch>, GnitzSqlError> {
    let terms = equi_prologue(cb, class, sides, inputs, kind, b_unique)?;
    let matched = terms.merged(cb);
    Ok(exists_branches(cb, kind, Split::Matched(matched), terms.p_all(true)))
}

/// Band correlation: ν over A, keyed on A's source PK behind the output exchange.
pub(super) fn band(
    cb: &mut Circuit,
    inputs: [NodeId; 2],
    class: &JoinClass,
    kind: JoinType,
    sides: &[JoinSide; 2],
    b_unique: bool,
) -> Result<Vec<Branch>, GnitzSqlError> {
    let pro = range_prologue(cb, class, sides, inputs, kind)?;
    let merged = pro.merged(cb);
    let (all, nu) = pro.nu(cb, merged, true, b_unique);
    Ok(exists_branches(cb, kind, Split::Unmatched(nu), all))
}

/// Pure-range correlation: the one-row threshold `m = MAX/MIN(b.range)` decides
/// existence over A's owned slice, already on the worker owning its output key.
pub(super) fn pure_range(
    cb: &mut Circuit,
    inputs: [NodeId; 2],
    class: &JoinClass,
    kind: JoinType,
    sides: &[JoinSide; 2],
) -> Result<Vec<Branch>, GnitzSqlError> {
    let pro = range_prologue(cb, class, sides, inputs, kind)?;
    let (owned, matched) = pro.threshold(cb);
    Ok(exists_branches(cb, kind, Split::Matched(matched), owned))
}
