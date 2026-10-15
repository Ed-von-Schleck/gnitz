//! Read I/O on relation families. [`RelationRegistry::open_bound`] opens every
//! source a bound can name as a [`SourceCursor`], choosing between the store and
//! an index over it;
//! [`RelationRegistry::probe`] answers a HasPk probe.

use crate::relation::{Cut, Relation, RelationKind, RelationRegistry};
use gnitz_wire::{KeyRange, PkKeys, Probe, ReadBound};
use gnitz_zset::repr::{empty_cursor, Batch, BoundedIndexCursor, SourceCursor};

/// A range walks an index only while the walk's estimated entries are at most
/// one in this many of the store's rows; past that it scans the store under
/// the range.
const INDEX_SCAN_RATIO: usize = 16;

impl RelationRegistry {
    /// Answer `probe` at `keys` over the rows this process holds of `id`.
    pub fn probe(&self, id: u64, probe: Probe, keys: PkKeys) -> Result<Batch, String> {
        let relation = self.relation_or_err(id)?;
        let stride = keys.stride();
        let keyed_as = |expected: usize| match expected == stride {
            true => Ok(()),
            false => Err(format!(
                "probe: key stride {stride} != the probed store's {expected} (table {id})"
            )),
        };
        match probe {
            Probe::Pk => {
                keyed_as(relation.schema().pk_stride())?;
                let mut echo = Batch::empty_with_schema(&relation.probe_reply_schema(probe)?);
                let table = relation.table();
                keys.iter()
                    .filter(|k| table.has_pk_bytes(k))
                    .for_each(|key| echo.push_key_row(key, 1));
                Ok(echo)
            }
            Probe::PkColumn(col) => {
                keyed_as(relation.schema().pk_stride())?;
                let table = relation.table();
                let live: Vec<u8> = keys
                    .iter()
                    .filter(|k| table.has_pk_bytes(k))
                    .flatten()
                    .copied()
                    .collect();
                let live = PkKeys::from_sorted(stride, live);
                relation.gather(live, Cut::Now).project_live(&[col])
            }
            Probe::Index(cols, cap) => {
                let index = relation
                    .index_on(cols.as_slice())
                    .ok_or_else(|| format!("No index on columns {:?} for table {}", cols.as_slice(), id))?;
                // The keys are the probed spans themselves.
                keyed_as(index.key_spec().key_size())?;
                let mut entries = Batch::empty_with_schema(&relation.probe_reply_schema(probe)?);
                let cap = usize::try_from(cap.get()).unwrap_or(usize::MAX);
                index
                    .gather(keys)
                    .for_each_positive_capped(cap, |c| entries.push_key_row(c.current_pk_bytes(), 1));
                Ok(entries)
            }
        }
    }

    /// Open `bound`'s source over `id`, without walking it, and the part of `bound`
    /// the source does not apply.
    pub fn open_bound(&self, id: u64, bound: ReadBound, cut: Cut) -> Result<(SourceCursor, Option<KeyRange>), String> {
        let entry = self.relation_or_err(id)?;
        // A stream holds no rows, but a backfill over one still feeds an empty
        // epoch: that keeps every worker's exchange rounds in step.
        if entry.kind() == RelationKind::Stream {
            let empty = empty_cursor(entry.schema());
            return Ok((SourceCursor::Full(Box::new(empty)), None));
        }
        let cursor = match bound {
            ReadBound::None => SourceCursor::Full(Box::new(entry.table().open_cursor(cut))),
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
                SourceCursor::PkSet(Box::new(entry.gather(keys, cut)))
            }
            ReadBound::Range(r) => return open_range(entry, r, cut),
        };
        Ok((cursor, None))
    }
}

/// The walk `r` names over `entry`, and the part of it the cursor leaves unapplied.
fn open_range(entry: &Relation, r: KeyRange, cut: Cut) -> Result<(SourceCursor, Option<KeyRange>), String> {
    let schema = entry.schema();
    let table = entry.table();
    if r.walks_pk(schema.pk_cols()) {
        let cursor = table.range_cursor(
            gnitz_zset::schema::KeySpec::for_pk(&schema).range_keys(schema.pk_stride(), &r),
            cut,
        );
        return Ok((SourceCursor::Full(Box::new(cursor)), None));
    }
    // An index holds a pending row's entry from its ingest on, so it is walked
    // only where the store's two cuts read the same rows.
    let index = entry
        .index_on(r.cols().as_slice())
        .filter(|_| cut == Cut::Now || !entry.has_pending());
    if let Some(ic) = index {
        let idx = ic.cursor_over(&r);
        if idx.estimated_length() <= table.estimated_rows() / INDEX_SCAN_RATIO {
            let walk = BoundedIndexCursor::new(idx, table.open_cursor(cut), ic.key_spec());
            return Ok((SourceCursor::Bounded(Box::new(walk)), None));
        }
    }
    Ok((SourceCursor::Full(Box::new(table.open_cursor(cut))), Some(r)))
}

#[cfg(test)]
#[path = "tests/store_io.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/store_io.rs"]
mod bench;
