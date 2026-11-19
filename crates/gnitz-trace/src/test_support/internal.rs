//! The test helpers only this crate uses.

use std::path::Path;
use std::rc::Rc;

use crate::runs::{MappedShard, ReadCursor, Run, ShardWriteOpts, from_runs, from_runs_in_band, write_shard};
use gnitz_wire::PkBuf;
use gnitz_zset::repr::{Batch, Codebook};
use gnitz_zset::schema::{SchemaDescriptor, group_band};

/// An operator's trace in a test: one consolidated run per ingest, read through
/// one cursor — what a store's memtable holds between folds.
pub(crate) struct TestTrace {
    schema: SchemaDescriptor,
    runs: Vec<Rc<Batch>>,
}

impl TestTrace {
    pub(crate) fn new(schema: SchemaDescriptor) -> Self {
        TestTrace { schema, runs: Vec::new() }
    }

    /// `rows` dealt round-robin into `runs` runs: an element can cancel across
    /// runs.
    pub(crate) fn dealt(rows: &Batch, runs: usize) -> Self {
        let mut trace = TestTrace::new(*rows.schema());
        for k in 0..runs {
            let rows_k: Vec<(usize, usize)> = (k..rows.len()).step_by(runs).map(|r| (r, r + 1)).collect();
            trace.ingest(Batch::from_ranges(rows, &rows_k, 0));
        }
        trace
    }

    /// Add `batch` as one more run.
    pub(crate) fn ingest(&mut self, mut batch: Batch) {
        batch.set_schema(&self.schema);
        let run = batch.into_consolidated();
        if !run.is_empty() {
            self.runs.push(Rc::new(run));
        }
    }

    /// The runs, oldest first.
    pub(crate) fn runs(&self) -> &[Rc<Batch>] {
        &self.runs
    }

    /// A cursor over every run, newest first, as a store's is.
    pub(crate) fn cursor(&self) -> ReadCursor {
        from_runs(self.runs.iter().rev().cloned().map(Run::Mem), self.schema)
    }

    /// A cursor over the rows whose PK begins with a key in `[first, last]` and
    /// over no other: an operator that reads outside the keys it opened its
    /// history at finds nothing there. Its runs are newest first.
    pub(crate) fn cursor_within(&self, first: &[u8], last: &[u8]) -> ReadCursor {
        let (start, end) =
            group_band(first, last, self.schema.pk_stride()).expect("`first <= last`, so the band holds a key");
        let runs = self.runs.iter().rev().cloned().map(Run::Mem);
        let end = end.as_ref().map(PkBuf::pk_bytes);
        from_runs_in_band(runs, self.schema, start.pk_bytes(), end)
    }
}

/// Write `batch` to `path` as a shard whose heap is coded under `codebook`,
/// and map it.
pub(crate) fn map_coded_shard(path: &Path, batch: &Batch, codebook: &Codebook) -> Rc<MappedShard> {
    let path = path.to_str().unwrap();
    let opts = ShardWriteOpts {
        codebook: Some(codebook),
        ..Default::default()
    };
    write_shard(batch, path, opts).unwrap();
    let shard = MappedShard::open(path, batch.schema()).unwrap();
    assert!(
        shard.coded_heap().is_some(),
        "premise: the heap codes to less than three quarters"
    );
    Rc::new(shard)
}

/// `batch` written to `path` and mapped back, as a store maps the shards it
/// writes.
pub(crate) fn map_shard(path: &Path, batch: &Batch) -> Rc<MappedShard> {
    let path = path.to_str().unwrap();
    write_shard(batch, path, ShardWriteOpts::default()).unwrap();
    Rc::new(MappedShard::open(path, batch.schema()).unwrap())
}
