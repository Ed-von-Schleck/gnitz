//! Snapshot reads and backups, as a client drives them.
//!
//! A snapshot pins every base table and system family as one point of the log
//! holds them, so a reader can walk any number of relations, larger than it can
//! hold, and see one cut of all of them while writes go on. A snapshot is its
//! connection's and ends with it.

use std::sync::Arc;

use gnitz_wire::sys_rows::SeqTabRow;

use crate::catalog::CATALOG_FAMILIES;
use crate::connection::SnapshotPage;
use crate::{sys_schema, Catalog, ClientError, GnitzClient, Pending, Request, Schema, ZSetBatch};

/// The rows a page asks each worker for when a caller names no other size.
pub const DEFAULT_PAGE_ROWS: u64 = 16_384;

impl GnitzClient {
    /// Pin every base table and system family; the snapshot's id.
    pub async fn snapshot_open(&mut self) -> Result<u64, ClientError> {
        self.ack(Request::SnapshotOpen).await
    }

    /// Release snapshot `id`.
    pub async fn snapshot_close(&mut self, id: u64) -> Result<(), ClientError> {
        self.ack(Request::SnapshotClose(id)).await.map(drop)
    }

    /// Copy the server's data directory, at a checkpoint, into the entry `name`
    /// of the backup directory it was started with.
    pub async fn backup(&mut self, name: &str) -> Result<(), ClientError> {
        self.ack(Request::Backup(name)).await.map(drop)
    }

    /// One page of relation `tid` as snapshot `id` holds it: each worker's
    /// first `limit` rows above `after`, in `schema`'s layout.
    pub fn snapshot_page(
        &mut self,
        id: u64,
        tid: u64,
        after: Option<&[u8]>,
        limit: u64,
        schema: &Arc<Schema>,
    ) -> Pending<'_, SnapshotPage> {
        let sent = self.session.submit_snapshot_page(id, tid, after, limit, schema);
        self.wait(sent)
    }

    /// Every row of relation `tid` as snapshot `id` holds it, handed to `each` a
    /// batch at a time, no row twice. A batch is up to `page_rows` rows of each
    /// worker; its rows are in no order across batches or workers.
    pub async fn snapshot_read(
        &mut self,
        id: u64,
        tid: u64,
        schema: &Arc<Schema>,
        page_rows: u64,
        mut each: impl FnMut(ZSetBatch) -> Result<(), ClientError>,
    ) -> Result<(), ClientError> {
        let mut after: Option<Vec<u8>> = None;
        loop {
            let SnapshotPage { mut batch, worker_ends } =
                self.snapshot_page(id, tid, after.as_deref(), page_rows, schema).await?;
            let segments = segments(&worker_ends);
            // A worker that filled its page may hold more past its last row, so
            // only what every such worker has passed is read whole.
            let frontier = segments
                .iter()
                .filter(|&&(start, end)| (end - start) as u64 == page_rows)
                .map(|&(_, end)| batch.pks.get_bytes(end - 1))
                .min()
                .map(<[u8]>::to_vec);
            let Some(frontier) = frontier else {
                return each(batch);
            };
            let kept: Vec<(usize, usize)> = segments
                .iter()
                .map(|&(start, end)| {
                    let below = (start..end)
                        .take_while(|&i| batch.pks.get_bytes(i) <= &frontier[..])
                        .count();
                    (start, start + below)
                })
                .collect();
            batch.retain_ranges(&kept);
            each(batch)?;
            after = Some(frontier);
        }
    }

    /// Every row of relation `tid` as snapshot `id` holds it, in one batch.
    pub async fn snapshot_rows(&mut self, id: u64, tid: u64, schema: &Arc<Schema>) -> Result<ZSetBatch, ClientError> {
        let mut all = ZSetBatch::new(schema);
        self.snapshot_read(id, tid, schema, DEFAULT_PAGE_ROWS, |batch| {
            all.extend_from_owned(batch);
            Ok(())
        })
        .await?;
        Ok(all)
    }

    /// The catalog as snapshot `id` holds it.
    pub async fn snapshot_catalog(&mut self, id: u64) -> Result<Catalog, ClientError> {
        let mut families = Vec::with_capacity(CATALOG_FAMILIES.len());
        for family in CATALOG_FAMILIES {
            families.push(self.snapshot_rows(id, family, sys_schema(family)).await?);
        }
        let families: [ZSetBatch; CATALOG_FAMILIES.len()] = families.try_into().expect("one batch per family");
        Ok(Catalog::decode(&families)?)
    }

    /// Each SERIAL table's id and the last id its sequence has handed out, as
    /// snapshot `id` holds them.
    pub async fn snapshot_serials(&mut self, id: u64) -> Result<Vec<(u64, u64)>, ClientError> {
        let rows = self
            .snapshot_rows(id, gnitz_wire::SEQ_TAB, sys_schema(gnitz_wire::SEQ_TAB))
            .await?;
        Ok(rows
            .live_rows()
            .map(|i| {
                let Ok(r) = SeqTabRow::read(&rows, i);
                (r.seq_id, r.next_val)
            })
            .filter(|&(seq_id, _)| seq_id >= gnitz_wire::FIRST_USER_TABLE_ID)
            .collect())
    }
}

/// Each train's rows, `(start, end)`, in a page whose trains ended at
/// `worker_ends`.
fn segments(worker_ends: &[usize]) -> Vec<(usize, usize)> {
    std::iter::once(0)
        .chain(worker_ends.iter().copied())
        .zip(worker_ends.iter().copied())
        .collect()
}

#[cfg(test)]
#[path = "tests/snapshot.rs"]
mod tests;
