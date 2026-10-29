//! Transactions as a client runs them. One transaction per client:
//! [`GnitzClient::txn_begin`] opens the buffer every write path then routes
//! into, [`GnitzClient::txn_commit`] ships it as one atomic frame, and
//! [`GnitzClient::txn_rollback`] discards it. All state-machine errors are
//! raised here; the SQL dispatch arms and the Python context manager only
//! translate them.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use gnitz_expr::{LogicalProgram, RowFilter};
use gnitz_wire::txn_frame::BLIND;
use gnitz_wire::{ComputeMap, PkBuf, PkKeys, ReadBound, ReadSink, ReadSpec, WireConflictMode, WireFault, WireStatus};

use crate::client::GnitzClient;
use crate::connection::PushFamily;
use crate::connection::{RelDescriptor, Request, ScanReply, Target};
use crate::error::ClientError;
use crate::{Schema, ZSetBatch};

/// The keys-only read of `schema`: a reply of its PK columns alone, and a rows sink
/// whose zero-instruction map fills no payload slot.
pub fn key_reply(schema: &Schema) -> (Arc<Schema>, ReadSink) {
    let pk: Vec<u32> = (0..schema.pk_cols().len() as u32).collect();
    let reply = Schema::from_parts(schema.hidden_key_columns().collect(), &pk)
        .expect("a schema's own PK columns are an admissible schema");
    let program = LogicalProgram::copy_cols(&[]).to_blob_bytes();
    let map = ComputeMap { program, out_cols: Vec::new() };
    (Arc::new(reply), ReadSink { map: Some(map), ..ReadSink::all_rows() })
}

/// Autocommit attempts [`GnitzClient::read_modify_write`] makes before surfacing
/// the conflict for the caller's own retry. Each attempt re-reads; there is no
/// backoff.
pub const RMW_MAX_ATTEMPTS: usize = 4;

impl GnitzClient {
    /// True while a transaction is open.
    pub fn txn_active(&self) -> bool {
        self.txn.is_some()
    }

    /// Open a transaction; errors if one is already open. Each family it buffers
    /// carries the basis of the read it was built from, which COMMIT checks.
    pub fn txn_begin(&mut self) -> Result<(), ClientError> {
        if self.txn.is_some() {
            return Err(ClientError::from("transaction already open".to_string()));
        }
        self.txn = Some(TxnBuffer::default());
        Ok(())
    }

    /// Discard the open transaction (ROLLBACK): drop the buffer, sending
    /// nothing. Errors if no transaction is open.
    pub fn txn_rollback(&mut self) -> Result<(), ClientError> {
        self.txn.take().map(|_| ()).ok_or_else(no_transaction)
    }

    /// Commit the open transaction atomically and return its durable zone LSN, `0`
    /// when it wrote nothing. Errors if no transaction is open; the transaction is
    /// closed even when the commit fails.
    ///
    /// A `StaleCatalog` refusal is reported as `TxnConflict`: a relation the
    /// transaction wrote has been altered since this client resolved it — which
    /// may have been before BEGIN, a write buffered under a kept descriptor
    /// sending no request of its own. The recovery is a conflict's: nothing was
    /// written, and the transaction run again resolves the relations it wrote
    /// afresh.
    pub async fn txn_commit(&mut self) -> Result<u64, ClientError> {
        let buf = self.txn.take().ok_or_else(no_transaction)?;
        self.push_txn(&buf).await.map_err(|e| match e {
            ClientError::Refused(WireFault { status: WireStatus::StaleCatalog, text }) => {
                self.kept.retain(|_, rel| !buf.families_of.contains_key(&rel.tid));
                ClientError::Refused(WireFault {
                    status: WireStatus::TxnConflict,
                    text: format!("transaction conflict: {text}; retry"),
                })
            }
            e => e,
        })
    }

    /// Ship `buf` as one `PUSH_TXN` frame, whose families land together under one
    /// zone LSN or not at all; an empty buffer sends nothing.
    async fn push_txn(&mut self, buf: &TxnBuffer) -> Result<u64, ClientError> {
        if buf.families.is_empty() {
            return Ok(0);
        }
        self.ack(Request::PushTxn { families: &buf.families }).await
    }

    /// Read `target`'s rows under `bound` and `predicate` — only their keys when
    /// `keys` — as the open transaction sees them, hand them to `build`, and write
    /// what it returns on the condition that `target` was not written after the
    /// read. Returns the written row count.
    ///
    /// Inside a transaction COMMIT checks the condition. In autocommit the statement
    /// is a transaction of its own, and a conflict re-reads and rebuilds, up to
    /// [`RMW_MAX_ATTEMPTS`] times.
    pub async fn read_modify_write<E: From<ClientError>>(
        &mut self,
        target: &RelDescriptor,
        bound: ReadBound,
        predicate: Vec<u8>,
        keys: bool,
        mut build: impl FnMut(ZSetBatch) -> Result<ZSetBatch, E>,
    ) -> Result<usize, E> {
        let (tid, schema) = (target.tid, &target.schema);
        let (reply, sink) = match keys {
            true => key_reply(schema),
            false => (Arc::clone(schema), ReadSink::all_rows()),
        };
        let spec = ReadSpec { bound, predicate, sink };
        let mut attempt = 0;
        loop {
            attempt += 1;
            let rest = match (self.txn.as_mut(), &spec.bound) {
                (Some(txn), ReadBound::PkSet(set)) => txn.unwritten(tid, set),
                _ => None,
            };
            let (batch, basis) = match rest {
                // Every key is the transaction's own: nothing is read, so the write
                // depends on no committed state.
                Some(rest) if rest.is_empty() => (ZSetBatch::new(&reply), BLIND),
                rest => {
                    let narrowed = rest.map(|rest| ReadSpec {
                        bound: ReadBound::PkSet(rest),
                        predicate: spec.predicate.clone(),
                        sink: spec.sink.clone(),
                    });
                    let ScanReply { batch, lsn, .. } = self
                        .scan_spec(target, narrowed.as_ref().unwrap_or(&spec), &reply)
                        .await?;
                    (batch, lsn.expect("a server read carries its watermark"))
                }
            };
            let mut own = TxnBuffer::default();
            let txn = self.txn.as_mut().unwrap_or(&mut own);
            let batch = build(txn.overlay(tid, schema, &spec, keys, batch)?)?;
            let count = batch.len();
            txn.push(target, schema, batch, WireConflictMode::Update, basis)?;
            let pushed = match self.txn {
                Some(_) => Ok(0),
                None => self.push_txn(&own).await,
            };
            match pushed {
                Err(e) if e.status() == Some(WireStatus::TxnConflict) && attempt < RMW_MAX_ATTEMPTS => {}
                r => {
                    r?;
                    return Ok(count);
                }
            }
        }
    }
}

fn no_transaction() -> ClientError {
    ClientError::from("no transaction open".to_string())
}

/// A transaction's buffered writes, which [`GnitzClient::push_txn`] ships as one
/// `PUSH_TXN` frame; dropping it sends nothing. Per tid, the families are the
/// maximal same-mode runs of the writes in call order, the order the engine
/// validates and applies them in.
#[derive(Default)]
pub(crate) struct TxnBuffer {
    /// Creation order; per tid, maximal same-mode runs of the caller's op
    /// sequence.
    families: Vec<PushFamily>,
    /// Per family, how many of its batch's rows are already folded into
    /// `last_op_of`. A batch only ever extends, so this is a watermark.
    indexed: Vec<usize>,

    /// tid → its family indices in creation order. Only the last one can still
    /// grow (a matching-mode append extends it), which is what makes indexing a
    /// tid's families in this order the same as indexing in append order.
    families_of: HashMap<u64, Vec<usize>>,
    /// tid → PK → `(family index, row index)` of the LAST op buffered on that
    /// PK. Row indices are stable: `push` only ever extends a family batch or
    /// pushes a new one. Weight-0 rows are not indexed — they are inert, exactly
    /// as the engine's fold treats them.
    last_op_of: HashMap<u64, HashMap<PkBuf, (usize, usize)>>,
}

impl TxnBuffer {
    /// The buffer's one write entry point: append `batch` to `tid`'s current run,
    /// or open a new family when the mode differs (or `tid` has no family yet).
    /// Empty batches contribute nothing and open no family.
    /// `basis`: the watermark of the read `batch` was built from, or `BLIND`. A
    /// family keeps the oldest basis of the batches it holds.
    ///
    /// `Error` mode rejects the whole transaction if any of these rows' PKs
    /// already exist, checked cumulatively in frame order against committed state
    /// and earlier families. A delete is a batch of `-1` rows in `Update` mode, so
    /// "delete k; insert k" emits an Update family `[D(k)]` then an Error family
    /// `[I(k)]`, in that order.
    ///
    /// Refused when `batch` is not in the layout `tid` already holds.
    pub(crate) fn push<'b>(
        &mut self,
        target: impl Into<Target>,
        schema: &Arc<Schema>,
        batch: impl Into<Cow<'b, ZSetBatch>>,
        mode: WireConflictMode,
        basis: u64,
    ) -> Result<(), ClientError> {
        let (target, batch) = (target.into(), batch.into());
        let tid = target.tid;
        batch
            .layout_matches(schema)
            .map_err(|e| ClientError::from(format!("relation {tid}: the batch is not in its schema's layout: {e}")))?;
        if batch.is_empty() {
            return Ok(());
        }
        // A batch in this very schema is in the layout the first family holds.
        self.check_layout(tid, |held| match Arc::ptr_eq(&held.schema, schema) {
            true => Ok(()),
            false => batch.layout_matches(&held.schema),
        })?;
        // Copied out, so no borrow of `families_of` spans the `families` read.
        let last = self.families_of.get(&tid).and_then(|v| v.last().copied());
        match last.filter(|&i| self.families[i].mode == mode) {
            Some(i) => {
                let f = &mut self.families[i];
                f.batch.extend_from(&batch);
                f.basis = f.basis.min(basis);
                if f.target.token == 0 {
                    f.target = target;
                }
            }
            None => {
                self.families_of.entry(tid).or_default().push(self.families.len());
                self.families.push(PushFamily {
                    target,
                    schema: Arc::clone(schema),
                    batch: batch.into_owned(),
                    mode,
                    basis,
                });
                self.indexed.push(0);
            }
        }
        Ok(())
    }

    /// `matches` over `tid`'s first family; every later one shares its layout.
    fn check_layout(
        &self,
        tid: u64,
        matches: impl FnOnce(&PushFamily) -> Result<(), String>,
    ) -> Result<(), ClientError> {
        let Some(&first) = self.families_of.get(&tid).and_then(|v| v.first()) else {
            return Ok(());
        };
        matches(&self.families[first])
            .map_err(|e| ClientError::from(format!("relation {tid} changed layout during this transaction: {e}")))
    }

    /// Fold every row `tid` has buffered since the last catch-up into
    /// `last_op_of`. Each row is folded at most once, so the whole index costs
    /// O(rows buffered) across the transaction however often it is read — and
    /// nothing at all for a transaction that never reads.
    fn index_tid(&mut self, tid: u64) {
        let Some(own) = self.families_of.get(&tid) else {
            return;
        };
        let index = self.last_op_of.entry(tid).or_default();
        for &fam in own {
            let batch = &self.families[fam].batch;
            for row in std::mem::replace(&mut self.indexed[fam], batch.len())..batch.len() {
                if batch.weights[row] != 0 {
                    index.insert(PkBuf::from_bytes(batch.pks.get_bytes(row)), (fam, row));
                }
            }
        }
    }

    /// The keys of `set` this transaction has not written — what a read of
    /// `set` still has to ask the server. `None` when it wrote none of them.
    fn unwritten(&mut self, tid: u64, set: &PkKeys) -> Option<PkKeys> {
        self.index_tid(tid);
        let index = self.last_op_of.get(&tid)?;
        if !set.iter().any(|k| index.contains_key(k)) {
            return None;
        }
        let mut rest = set.clone();
        rest.retain(|k| !index.contains_key(k));
        Some(rest)
    }

    /// `committed`, a server read of `tid` under `spec`, as this transaction sees
    /// it: less every PK the transaction wrote, plus its live rows (last op per PK,
    /// weight > 0) that `spec` keeps, each at weight 1. With `keys`, `committed` is in the
    /// [`key_reply`] layout, and so are the added rows.
    fn overlay(
        &mut self,
        tid: u64,
        schema: &Schema,
        spec: &ReadSpec,
        keys: bool,
        committed: ZSetBatch,
    ) -> Result<ZSetBatch, ClientError> {
        self.check_layout(tid, |held| held.batch.layout_matches(schema))?;
        self.index_tid(tid);
        let Some(index) = self.last_op_of.get(&tid) else {
            return Ok(committed);
        };
        let keep: Vec<u32> = (0..committed.len())
            .filter(|&i| !index.contains_key(committed.pks.get_bytes(i)))
            .map(|i| i as u32)
            .collect();
        // `gather` compacts the string arena the dropped rows carried.
        let mut out = committed.gather(&keep);

        let mut live = ZSetBatch::new(schema);
        let families = &self.families;
        let take = |&(fam, row): &(usize, usize)| {
            let b = &families[fam].batch;
            if b.weights[row] > 0 {
                live.copy_row_at(b, row, 1);
            }
        };
        match &spec.bound {
            ReadBound::PkSet(set) => set.iter().filter_map(|k| index.get(k)).for_each(take),
            _ => index.values().for_each(take),
        }
        if live.is_empty() {
            return Ok(out);
        }
        let mut ranges = Vec::new();
        RowFilter::for_read(&spec.predicate, spec.bound.range(), schema.layout())
            .map_err(|e| ClientError::from(e.to_string()))?
            .ranges(&live, &mut ranges);
        if keys {
            // The predicate has read the payload; the key reply carries none.
            live.payload.clear();
            live.blob.clear();
            live.retain_ranges(&ranges);
            live.nulls.fill(0);
        } else {
            let kept: Vec<u32> = ranges.iter().flat_map(|&(s, e)| s as u32..e as u32).collect();
            live = live.gather(&kept);
        }
        out.extend_from_owned(live);
        Ok(out)
    }
}

#[cfg(test)]
#[path = "tests/txn.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/txn.rs"]
mod bench;
