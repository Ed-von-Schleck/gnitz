//! Read I/O on relation families. [`RelationRegistry::open_bound`] opens every
//! source a bound can name as a [`SourceCursor`];
//! [`RelationRegistry::gather_bytes`] is the batched FK parent probe.

use crate::relation::{Relation, RelationKind, RelationRegistry};
use crate::schema::key::{sort_indices, KeySpec};
use crate::schema::{project_schema, ColumnLocator};
use crate::schema::{ColumnTable, SchemaFacts};
use crate::storage::{Batch, PkSetGather, ReadCursor, SkeletonKeys};
use gnitz_wire::{KeyRange, PkKeys, ReadBound};

const INDEX_SCAN_RATIO: usize = 16;

impl RelationRegistry {
    /// The FK parent probe: every live row of `keys` at weight 1, projected to the payload
    /// column `ref_col`.
    pub fn gather_bytes(&self, id: u64, keys: PkKeys, ref_col: u8) -> Result<Batch, String> {
        let entry = self.relation_or_err(id)?;
        let schema = entry.schema();
        let out_schema = project_schema(&schema, &[ref_col as u32]).expect("a one-column projection fits MAX_COLUMNS");
        // A PK `ref_col` would be dropped by `project_schema`, leaving the reply payload-less.
        let loc = schema.locate(ref_col as usize);
        assert!(
            matches!(loc, ColumnLocator::Payload { .. }),
            "FK projection excludes PK columns"
        );
        let n = keys.len();
        let mut gather = entry.gather(keys, None);
        let mut out = Batch::with_capacity(&out_schema, n);
        gather.for_each_live_row(usize::MAX, |c| {
            let (src, row) = c.current_row_source();
            let mut null_word = 0;
            out.begin_row(c.current_pk_bytes(), 1);
            out.append_cells_from(0, std::slice::from_ref(&loc), src, row, &mut null_word);
            out.commit_row(null_word);
        });
        Ok(out)
    }

    /// Open `bound`'s source over `id`, without walking it, and the part of `bound`
    /// the source does not apply.
    pub fn open_bound(&self, id: u64, bound: ReadBound) -> Result<(SourceCursor, ReadBound), String> {
        let entry = self.relation_or_err(id)?;
        // A stream holds no rows, but a backfill over one still feeds an empty
        // epoch: that is what mints a global aggregate's ground row.
        if entry.kind() == RelationKind::Stream {
            let empty = crate::storage::empty_cursor(entry.schema());
            return Ok((SourceCursor::Full(Box::new(empty)), ReadBound::None));
        }
        let cursor = match bound {
            ReadBound::None => SourceCursor::Full(Box::new(entry.cursor())),
            ReadBound::PkSet(keys) => {
                let schema = entry.schema();
                if keys.stride() != schema.pk_stride() {
                    return Err(format!(
                        "open_bound: PkSet key stride {} != pk_stride {} (table {id})",
                        keys.stride(),
                        schema.pk_stride()
                    ));
                }
                // A key this worker holds no row for copies nothing.
                SourceCursor::PkSet(Box::new(entry.gather(keys, None)))
            }
            ReadBound::Range(r) => return open_range(entry, r),
        };
        Ok((cursor, ReadBound::None))
    }
}

/// The walk `r` names over `entry`, and the part of it the cursor leaves unapplied.
fn open_range(entry: &Relation, r: KeyRange) -> Result<(SourceCursor, ReadBound), String> {
    let cols = entry.bound_cols(r.cols(), "open_bound")?;
    let schema = entry.schema();
    if r.walks_pk(schema.pk_cols()) {
        let cursor = entry.store().held().range_cursor(schema.pk_range_keys(&r));
        return Ok((SourceCursor::Full(Box::new(cursor)), ReadBound::None));
    }
    if let Some(ic) = entry.index_on(cols.as_slice()) {
        let idx = ic.cursor_over(&r);
        if idx.estimated_length() <= entry.store().held().estimated_rows() / INDEX_SCAN_RATIO {
            let walk = BoundedIndexCursor {
                idx,
                src: PkSetGather::new(entry.cursor(), PkKeys::from_sorted(schema.pk_stride(), Vec::new())),
                spec: ic.key_spec(),
                pks: Vec::new(),
                order: Vec::new(),
            };
            return Ok((SourceCursor::Bounded(Box::new(walk)), ReadBound::None));
        }
    }
    Ok((SourceCursor::Full(Box::new(entry.cursor())), ReadBound::Range(r)))
}

/// A chunked source of `Batch`es over one relation, in every shape a bound can take.
pub enum SourceCursor {
    Full(Box<ReadCursor>),
    Bounded(Box<BoundedIndexCursor>),
    /// `pk IN (…)` gather over a listed key set.
    PkSet(Box<PkSetGather>),
}

impl SourceCursor {
    /// The next non-empty source chunk, or `None` once the source is exhausted.
    /// `PkSet` exceeds `max_rows` rather than split a PK group.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        let mut skeletons = SkeletonKeys::default();
        let chunk = self.drain_live_chunk(max_rows, &mut skeletons);
        skeletons.assert_none();
        chunk
    }

    /// [`Self::drain_chunk`] with skeleton rows split out; `Some` and empty means every
    /// row of the chunk was a skeleton.
    pub(super) fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        match self {
            SourceCursor::Full(c) => c.drain_live_chunk(max_rows, skeletons),
            SourceCursor::Bounded(c) => c.drain_live_chunk(max_rows, skeletons),
            SourceCursor::PkSet(g) => g.drain_live_chunk(max_rows, skeletons),
        }
    }
}

/// A walk of one secondary-index key range, gathering each entry's row from the base
/// table it indexes, over one snapshot of each. The owner is a base table, so a source
/// PK has one live row and at most one live index entry, at weight 1.
pub struct BoundedIndexCursor {
    /// Positioned on the index range and clamped at its end.
    idx: ReadCursor,
    src: PkSetGather,
    spec: KeySpec,
    /// A refill's source PKs in index order, flat at `src`'s `pk_stride`.
    pks: Vec<u8>,
    order: Vec<u32>,
}

impl BoundedIndexCursor {
    fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        assert!(
            max_rows > 0,
            "BoundedIndexCursor::drain_live_chunk: max_rows must be positive"
        );
        loop {
            if let Some(chunk) = self.src.drain_live_chunk(max_rows, skeletons) {
                return Some(chunk);
            }
            if !self.refill(max_rows) {
                return None;
            }
        }
    }

    /// Reload the gather with the source PKs of the next `max_rows` index entries;
    /// `false` once the index range is spent.
    fn refill(&mut self, max_rows: usize) -> bool {
        let stride = self.src.schema().pk_stride();
        self.pks.clear();
        let mut taken = 0;
        while taken < max_rows && self.idx.valid {
            debug_assert!(self.idx.current_weight > 0, "an index entry at a non-positive weight");
            self.pks
                .extend_from_slice(self.spec.split_entry(self.idx.current_pk_bytes()).1);
            self.idx.advance();
            taken += 1;
        }
        if taken == 0 {
            return false;
        }
        sort_indices(&self.pks, stride, &mut self.order);
        let mut keys = Vec::with_capacity(self.pks.len());
        for &i in &self.order {
            keys.extend_from_slice(&self.pks[i as usize * stride..][..stride]);
        }
        self.src.reload(PkKeys::from_sorted(stride, keys));
        true
    }
}

#[cfg(test)]
#[path = "tests/store_io.rs"]
mod tests;
