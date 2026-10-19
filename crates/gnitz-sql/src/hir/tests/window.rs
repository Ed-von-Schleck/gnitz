use super::*;
use crate::hir::bind::tests::bound;

/// The `(limit, offset)` of the top-N `sql`'s body cuts, if it cuts one.
fn cut(sql: &str) -> Option<(u64, u64)> {
    fn find(rel: &RelExpr) -> Option<(u64, u64)> {
        match rel {
            RelExpr::TopN { limit, offset, .. } => Some((*limit, *offset)),
            RelExpr::Project { input, .. } | RelExpr::Filter { input, .. } => find(input),
            _ => None,
        }
    }
    find(&bound(sql))
}

/// A QUALIFY conjunct bounding a ROW_NUMBER with an order cuts a top-N only as
/// `rn <= n`, `rn < n` or `rn = n`, either way round, keeping at least one slot,
/// and only while nothing else reads `rn`.
#[test]
fn a_qualify_bound_on_an_unread_row_number_cuts_its_slots() {
    let rn = "ROW_NUMBER() OVER (ORDER BY a)";
    for (qualify, want) in [
        ("rn <= 3", Some((3, 0))),
        ("3 >= rn", Some((3, 0))),
        ("rn < 3", Some((2, 0))),
        ("3 > rn", Some((2, 0))),
        ("rn = 1", Some((1, 0))),
        ("rn = 3", Some((1, 2))),
        ("3 = rn", Some((1, 2))),
        ("rn < 1", None),
        ("rn <= 0", None),
        ("rn = 0", None),
        ("rn = -2", None),
        ("rn > 3", None),
        ("rn >= 3", None),
        // A second conjunct reading `rn` keeps it a window; one that does not
        // stays a filter above the cut.
        ("rn <= 3 AND rn > 1", None),
        ("rn <= 3 AND a > 1", Some((3, 0))),
        ("RANK() OVER (ORDER BY a) <= 3", None),
        // The order is extended by the input's key: partitioned by that key, a
        // ROW_NUMBER has no order left to cut by.
        ("ROW_NUMBER() OVER (PARTITION BY a) <= 1", Some((1, 0))),
        ("ROW_NUMBER() OVER (PARTITION BY id) <= 1", None),
    ] {
        let sql = format!("SELECT id FROM t QUALIFY {}", qualify.replace("rn", rn));
        assert_eq!(cut(&sql), want, "{sql}");
    }
    // A selected `rn` is read elsewhere.
    assert_eq!(cut(&format!("SELECT id, {rn} AS rn FROM t QUALIFY rn <= 3")), None);
}
