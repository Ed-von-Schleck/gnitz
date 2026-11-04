//! [`GroupProbe`]: a cursor's runs walked forward one PK at a time, each PK
//! resolved to its first live row without the merge tournament.

use std::cmp::Ordering;
use std::ops::Range;

use smallvec::SmallVec;

use super::{CursorRow, ReadCursor, Run};
use crate::repr::batch::Batch;
use crate::repr::scatter::{gather_into, with_picks};
use crate::schema::{compare_pk_ordering, with_payload_cmp, PayloadOrder, SchemaDescriptor};
use gnitz_wire::RowSource;

/// A [`ReadCursor`] turned into a forward probe of ascending PKs. Each
/// [`Self::find`] gallops every run to the PK on its own, and folds the PK's
/// rows across runs only where two or more runs hold it — unless one of the
/// cursor's [newest-first heap runs](ReadCursor::mark_heap_newest_first) does
/// and the probe is over a store of one row per PK, where that run decides it.
pub(crate) struct GroupProbe<'c> {
    cursor: &'c mut ReadCursor,
    /// The row the cursor stood on when the probe began, until a key reaches
    /// its PK: the cursor's walk is already past it, and every row below it.
    pending: Option<CursorRow>,
    /// The leading sources of which the first holding a PK decides it: the
    /// cursor's newest-first heap runs where every PK nets 0 or one row at +1,
    /// none otherwise.
    deciding: usize,
}

impl ReadCursor {
    /// A [`GroupProbe`] from where this cursor stands, over a store where
    /// every PK nets 0 or one row at +1 iff `one_row_per_pk`. The probe moves
    /// the runs' positions under the merge, so it leaves the cursor on no row:
    /// only a seek positions it again.
    pub(crate) fn group_probe(&mut self, one_row_per_pk: bool) -> GroupProbe<'_> {
        // A skeleton run folds a PK group regardless of payload; a probe folds by element.
        debug_assert!(!self.order.coarsen, "group probe over a skeleton store");
        // An invalid cursor's runs are spent: every key finds none.
        let pending = self.valid.then_some(self.current);
        self.valid = false;
        let deciding = match one_row_per_pk {
            true => self.heap_newest_first as usize,
            false => 0,
        };
        GroupProbe { cursor: self, pending, deciding }
    }
}

impl GroupProbe<'_> {
    /// The source and row of a handle [`Self::find`] returned.
    #[inline(always)]
    pub(crate) fn row_at(&self, at: CursorRow) -> (&Run, usize) {
        self.cursor.row_at(at)
    }

    /// Append to `batch`, of the cursor's layout, the rows of handles
    /// [`Self::find`] returned, in the order given, at weight +1.
    pub(crate) fn gather_into(&self, rows: &[CursorRow], batch: &mut Batch) {
        let ReadCursor { sources, schema, .. } = &*self.cursor;
        debug_assert!(batch.schema().same_region_types(schema));
        with_picks(|picks| {
            picks.extend(rows.iter().map(|at| at.pick(1)));
            batch.write_rows(picks.len(), |w| gather_into(sources, schema, picks, w));
        });
    }

    /// Copy the row of a handle [`Self::find`] returned into `batch` at `weight`.
    #[inline]
    pub(crate) fn copy_row_into(&self, at: CursorRow, batch: &mut Batch, weight: i64) {
        let (src, row) = self.row_at(at);
        batch.append_row_from_source(weight, src, row, None);
    }

    /// The first live row of PK `key`, in (PK, payload) order, or `None` for a
    /// PK with none — where [`ReadCursor::seek_pk_group_ascending`] would land.
    /// Keys must strictly ascend across calls.
    #[inline(always)]
    pub(crate) fn find(&mut self, key: &[u8]) -> Option<CursorRow> {
        if let Some(at) = self.pending {
            let (src, row) = self.cursor.row_at(at);
            match vs_key(src, row, key) {
                Ordering::Greater => return None,
                Ordering::Equal => {
                    self.pending = None;
                    return Some(at);
                }
                Ordering::Less => self.pending = None,
            }
        }
        let ReadCursor { sources, states, schema, .. } = &mut *self.cursor;
        debug_assert_eq!(key.len(), schema.pk_stride(), "a probe key is a whole PK");
        // Each run to its first row at or above the key; a run already there
        // costs one comparison.
        let (mut holders, mut found, mut newest) = (0, CursorRow { src: 0, row: 0 }, None);
        for (i, (src, st)) in sources.iter().zip(states.iter_mut()).enumerate() {
            if st.start >= st.end {
                continue;
            }
            let mut ord = vs_key(src, st.start, key);
            if ord.is_lt() {
                st.start = src.seek(key, Some(st.start)).min(st.end);
                if st.start == st.end {
                    continue;
                }
                ord = vs_key(src, st.start, key);
            }
            if ord.is_eq() {
                // The runs after it can stay below the PK: a later key gallops them.
                if i < self.deciding {
                    newest = Some(i);
                    break;
                }
                holders += 1;
                found = CursorRow { src: i as u32, row: st.start as u32 };
            }
        }
        if let Some(i) = newest {
            let folded = cfg!(debug_assertions).then(|| fold_every_run(sources, states, schema, key));
            let live = newest_decides(&sources[i], &mut states[i], i, key);
            debug_assert!(
                folded.is_none_or(|f| f == live.map(|at| (at.src, at.row))),
                "the newest heap run holding a PK answers what folding every run does",
            );
            return live;
        }
        match holders {
            0 => None,
            // A run is folded within, so its PK's first row is live.
            1 => {
                // The row is the PK's, below every later key.
                states[found.src as usize].start += 1;
                Some(found)
            }
            _ => with_payload_cmp!(schema, |p| fold_pk(sources, states, schema, key, p)),
        }
    }
}

/// The live row of PK `key` in `run`, source `i`, the newest of a store's runs
/// holding the PK, where every PK nets 0 or one row at +1; `state` stands on the
/// PK's first row.
///
/// Every older run is older than this one, and each run is the consolidated
/// sum of a contiguous stretch of the store's ingests, so what this stretch
/// left is the PK's state now, and its rows of the PK are that state less the
/// one it started from: one row at +1 and one at -1 at most. A positive row is
/// the live one, and a retraction alone means the stretch deleted the PK.
#[inline(always)]
fn newest_decides(run: &Run, state: &mut Range<usize>, i: usize, key: &[u8]) -> Option<CursorRow> {
    let mut row = state.start;
    if run.get_weight(row) < 0 {
        row += 1;
        if row == state.end || vs_key(run, row, key).is_ne() {
            state.start = row;
            return None;
        }
    }
    debug_assert!(run.get_weight(row) > 0, "a run holds one row of a PK at either sign");
    // The row is the PK's, below every later key.
    state.start = row + 1;
    Some(CursorRow { src: i as u32, row: row as u32 })
}

/// [`fold_pk`]'s answer at PK `key` over every run, each galloped to the PK
/// from where it stands; the runs' positions do not move.
fn fold_every_run(
    sources: &[Run],
    states: &[Range<usize>],
    schema: &SchemaDescriptor,
    key: &[u8],
) -> Option<(u32, u32)> {
    let mut states = states.to_vec();
    for (src, st) in sources.iter().zip(states.iter_mut()) {
        if st.start < st.end && vs_key(src, st.start, key).is_lt() {
            st.start = src.seek(key, Some(st.start)).min(st.end);
        }
    }
    let at = with_payload_cmp!(schema, |p| fold_pk(sources, &mut states, schema, key, p));
    at.map(|at| (at.src, at.row))
}

/// The first element, in payload order, of PK `key`, among those whose weights
/// summed across the runs are not zero, each run positioned at or above the
/// PK; each run's position moves past the PK.
#[inline(never)]
fn fold_pk<P: PayloadOrder>(
    sources: &[Run],
    states: &mut [Range<usize>],
    schema: &SchemaDescriptor,
    key: &[u8],
    payload: P,
) -> Option<CursorRow> {
    let cmp = |a: CursorRow, b: CursorRow| {
        let (sa, sb) = (&sources[a.src as usize], &sources[b.src as usize]);
        payload.compare(schema, sa, a.row as usize, sb, b.row as usize)
    };
    // Every element of the PK with its summed weight.
    let mut elements: SmallVec<[(CursorRow, i64); 8]> = SmallVec::new();
    for (src, (run, state)) in sources.iter().zip(states.iter_mut()).enumerate() {
        let src = src as u32;
        let mut row = state.start;
        while row < state.end && vs_key(run, row, key).is_eq() {
            let at = CursorRow { src, row: row as u32 };
            let w = run.get_weight(row);
            match elements.iter_mut().find(|(e, _)| cmp(*e, at).is_eq()) {
                Some((_, sum)) => *sum += w,
                None => elements.push((at, w)),
            }
            row += 1;
        }
        state.start = row;
    }
    let mut live = elements.iter().filter(|&&(_, w)| w != 0).map(|&(e, _)| e);
    let first = live.next()?;
    Some(live.fold(first, |m, e| if cmp(e, m).is_lt() { e } else { m }))
}

/// Row `row` of `src`'s PK against `key`.
#[inline(always)]
fn vs_key(src: &Run, row: usize, key: &[u8]) -> Ordering {
    compare_pk_ordering(src.get_pk_bytes(row), key)
}
