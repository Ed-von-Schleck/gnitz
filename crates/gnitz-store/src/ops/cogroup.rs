//! Sorted-stream co-group merge skeletons.
//!
//! Two skeletons over one operation: **walk a sorted delta batch against a sorted
//! [`ReadCursor`], grouped by equal PK**, handing each `(key, delta-group, cursor)`
//! to the operator, which makes the Z-set decision — the equi delta-trace join
//! ([`cogroup_intersection`]) and distinct ([`cogroup_left`]). The skip step
//! galloping-seeks from the live position: K ascending probes over an N-row source
//! cost `O(K · log gap)`, not `O(K · log N)` or a linear `O(K + N)` scan.
//!
//! Equal key is the whole contract — which is why neither the range join's
//! ordered-span walk nor `Batch::merged_consolidated` is a third skeleton here.

use std::cmp::Ordering;
use std::ops::Range;

use crate::schema::key::compare_pk_ordering;
use crate::storage::{Batch, ReadCursor};

/// Intersection co-group: emit only at keys present on **both** sides. Both
/// pointers galloping-skip to catch up, so the cost is bounded by the smaller
/// side's matches plus galloping skips — optimal whichever side is larger,
/// replacing any size selector. Inner join, inner DD join.
///
/// **Self-positioning.** The first act is `m.advance_to(delta[0])`, so a shared
/// match cursor (a trace register reused by several ops in one epoch) is reset
/// to the co-group start from any prior position — `advance_to` is
/// backward-capable. This subsumes the old `rewind` + `seek_bytes(delta[0])`
/// the merge-walk paid; `cogroup_left`'s ascending gate positions the same way.
///
/// **Callback contract.** `on_match` reads its match group through
/// `ReadCursor::for_each_pk_group_row` and must walk the *whole* group to emit
/// correctly. Loop
/// *progress* is robust either way: the skeleton re-establishes the match
/// position with `advance_to` at the next delta key (an under-walk forfeits rows
/// the callback should have read; an over-walk merely sends the following
/// `advance_to` down its bounded-backward branch, still correct).
#[inline]
pub(crate) fn cogroup_intersection(
    delta: &Batch,
    m: &mut ReadCursor,
    mut on_match: impl FnMut(&[u8], Range<usize>, &mut ReadCursor),
) {
    let n = delta.count;
    if n == 0 {
        return;
    }
    m.advance_to(delta.get_pk_bytes(0));
    let mut i = 0;
    while i < n && m.valid {
        let dk = delta.get_pk_bytes(i);
        match compare_pk_ordering(dk, m.current_pk_bytes()) {
            Ordering::Less => i = delta.advance_to(m.current_pk_bytes(), i), // skip delta
            Ordering::Greater => m.advance_to(dk),                           // skip match side
            Ordering::Equal => {
                let j = delta.pk_group_end(i); // delta group
                on_match(dk, i..j, m); // walks match group
                i = j;
            }
        }
    }
}

/// Left co-group: visit **every** delta group (the match side galloping-skips to
/// it), because the operator emits per delta key whether or not the match group
/// exists. Used by distinct.
///
/// **Callback contract.** Same as [`cogroup_intersection`]: `on_group` walks the
/// forward-only `m` over the match group (possibly empty) and must walk the
/// whole group; loop progress is robust to under/over-walk.
#[inline]
pub(crate) fn cogroup_left(
    delta: &Batch,
    m: &mut ReadCursor,
    mut on_group: impl FnMut(&[u8], Range<usize>, &mut ReadCursor),
) {
    let n = delta.count;
    let mut i = 0;
    while i < n {
        let dk = delta.get_pk_bytes(i);
        let j = delta.pk_group_end(i);
        // Ascending, so an absent group costs a comparison; the first key
        // positions the cursor, which is what self-positions the whole walk.
        m.seek_pk_group_ascending(dk);
        on_group(dk, i..j, m);
        i = j;
    }
}

#[cfg(test)]
#[path = "tests/cogroup.rs"]
mod tests;
