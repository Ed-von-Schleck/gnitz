//! Snapshot reads and backups, as a client drives them.
//!
//! A snapshot pins every base table and system family as one point of the log
//! holds them, so a reader can walk any number of relations, larger than it can
//! hold, and see one cut of all of them while writes go on. A snapshot is its
//! connection's and ends with it.

use std::collections::HashMap;
use std::sync::Arc;

use gnitz_wire::snapshot::PageRequest;
use gnitz_wire::sys_rows::SeqTabRow;

use crate::catalog::CATALOG_FAMILIES;
use crate::connection::SnapshotPage;
use crate::{sys_schema, Catalog, ClientError, GnitzClient, Pending, Request, Schema, ZSetBatch};

/// The rows a page holds when a caller names no other size.
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

    /// One page of relation `tid` as snapshot `id` holds it: up to `limit`
    /// rows, each worker's share of them after its key in `after` — from its
    /// first row with `after` empty — in `schema`'s layout.
    pub fn snapshot_page(
        &mut self,
        id: u64,
        tid: u64,
        after: &[Option<Vec<u8>>],
        limit: u64,
        schema: &Arc<Schema>,
    ) -> Pending<'_, SnapshotPage> {
        let sent = self.session.submit_snapshot_page(id, tid, after, limit, schema);
        self.wait(sent)
    }

    /// Every row of relation `tid` as snapshot `id` holds it, handed to `each` a
    /// batch at a time, no row twice: up to `page_rows` rows a batch, in no order
    /// across batches or workers.
    pub async fn snapshot_read<E: From<ClientError>>(
        &mut self,
        id: u64,
        tid: u64,
        schema: &Arc<Schema>,
        page_rows: u64,
        mut each: impl FnMut(ZSetBatch) -> Result<(), E>,
    ) -> Result<(), E> {
        // Each worker's last key handed out; empty until the first page names the workers.
        let mut after: Vec<Option<Vec<u8>>> = Vec::new();
        loop {
            let SnapshotPage { batch, worker_ends } = self.snapshot_page(id, tid, &after, page_rows, schema).await?;
            let segments = segments(&worker_ends);
            if after.is_empty() {
                after = vec![None; segments.len()];
            }
            if segments.len() != after.len() {
                return Err(ClientError::from(format!(
                    "a page of {} workers after one of {}",
                    segments.len(),
                    after.len()
                ))
                .into());
            }
            let share = PageRequest::rows_per_store(page_rows, segments.len());
            // A worker that filled its share may hold more past its last row.
            let mut more = false;
            for (at, &(start, end)) in after.iter_mut().zip(&segments) {
                if end > start {
                    *at = Some(batch.pks.get_bytes(end - 1).to_vec());
                }
                more |= (end - start) as u64 == share;
            }
            each(batch)?;
            if !more {
                return Ok(());
            }
        }
    }

    /// Every row of relation `tid` as snapshot `id` holds it, in one batch.
    async fn snapshot_rows(&mut self, id: u64, tid: u64, schema: &Arc<Schema>) -> Result<ZSetBatch, ClientError> {
        let mut all = ZSetBatch::new(schema);
        self.snapshot_read(id, tid, schema, DEFAULT_PAGE_ROWS, |batch| {
            all.extend_from_owned(batch);
            Ok::<_, ClientError>(())
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
    pub async fn snapshot_serials(&mut self, id: u64) -> Result<HashMap<u64, u64>, ClientError> {
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
