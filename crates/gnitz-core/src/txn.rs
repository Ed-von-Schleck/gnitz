//! Transactions as a client runs them. One transaction per client:
//! [`GnitzClient::txn_begin`] opens the buffer every write path then routes
//! into, [`GnitzClient::txn_commit`] ships it as one atomic frame, and
//! [`GnitzClient::txn_rollback`] discards it. Each refuses a call out of turn:
//! a BEGIN inside a transaction, a COMMIT or ROLLBACK outside one.

use std::collections::HashMap;
use std::sync::Arc;

use gnitz_expr::{LogicalProgram, RowFilter, leading_pk};
use gnitz_wire::Target;
use gnitz_wire::multi_frame::BLIND;
use gnitz_wire::{PkBuf, PkKeys, ReadBound, ReadSink, ReadSpec, WireConflictMode, WireFault, WireStatus};
use gnitz_zset::repr::{MemBatch, Pick, gather_rows};

use crate::client::GnitzClient;
use crate::connection::{PushFamily, RelDescriptor, Request, ScanReply};
use crate::error::ClientError;
use crate::{Schema, ZSetBatch};

/// The keys-only read of `schema`: a reply of its PK columns alone, and a rows sink
/// whose zero-instruction map fills no payload slot.
pub(crate) fn key_reply(schema: &Schema) -> (Arc<Schema>, ReadSink) {
    let reply = Schema::from_parts(
        schema.hidden_key_columns().collect(),
        leading_pk(schema.pk_cols().len()),
    )
    .expect("a schema's own PK columns are an admissible schema");
    let map = LogicalProgram::copy_cols(&[]).to_map(Vec::new());
    (Arc::new(reply), ReadSink { map: Some(map), ..ReadSink::all_rows() })
}

impl GnitzClient {
    /// True while a transaction is open.
    pub fn txn_active(&self) -> bool {
        self.txn.is_some()
    }

    /// Open a transaction; errors if one is already open. Each family it buffers
    /// carries the basis of the read it was built from, which COMMIT checks.
    pub fn txn_begin(&mut self) -> Result<(), ClientError> {
        if self.txn.is_some() {
            return Err(ClientError::from("transaction already open"));
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
    /// closed even when the commit fails. The buffer ships as one `PUSH_TXN`
    /// frame, whose families land together under one zone LSN or not at all; an
    /// empty buffer sends nothing.
    ///
    /// A `StaleCatalog` refusal is reported as `TxnConflict`: a relation the
    /// transaction wrote has been altered since this client resolved it — which
    /// may have been before BEGIN, a write buffered under a kept descriptor
    /// sending no request of its own. The recovery is a conflict's: nothing was
    /// written, and the transaction run again resolves the relations it wrote
    /// afresh.
    pub async fn txn_commit(&mut self) -> Result<u64, ClientError> {
        let buf = self.txn.take().ok_or_else(no_transaction)?;
        if buf.families.is_empty() {
            return Ok(0);
        }
        self.ack(Request::PushTxn(&buf.families)).await.map_err(|e| match e {
            ClientError::Refused(WireFault { status: WireStatus::StaleCatalog, text }) => {
                self.kept.retain(|_, rel| !buf.tids.contains_key(&rel.tid));
                ClientError::Refused(WireFault {
                    status: WireStatus::TxnConflict,
                    text: format!("transaction conflict: {text}; retry"),
                })
            }
            e => e,
        })
    }

    /// Read `target`'s rows under `bound` and `predicate` — only their keys when
    /// `keys` — as the open transaction sees them, hand them to `build`, and write
    /// what it returns on the condition that `target` was not written after the
    /// read. Returns the written row count.
    ///
    /// Inside a transaction COMMIT checks the condition. In autocommit the statement
    /// is a transaction of its own, committed here; a conflict is refused
    /// `TxnConflict` with nothing written, and is the caller's to run again.
    pub async fn read_modify_write<E: From<ClientError>>(
        &mut self,
        target: &RelDescriptor,
        bound: ReadBound,
        predicate: Vec<u8>,
        keys: bool,
        build: impl FnOnce(ZSetBatch) -> Result<ZSetBatch, E>,
    ) -> Result<usize, E> {
        let (tid, schema) = (target.tid, &target.schema);
        let (reply, sink) = match keys {
            true => key_reply(schema),
            false => (Arc::clone(schema), ReadSink::all_rows()),
        };
        let spec = ReadSpec { bound, predicate, sink };
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
                    .scan_served(target, narrowed.as_ref().unwrap_or(&spec), &reply)
                    .await?;
                (batch, lsn.expect("a server read carries its watermark"))
            }
        };
        let mut own = TxnBuffer::default();
        let txn = self.txn.as_mut().unwrap_or(&mut own);
        let batch = build(txn.overlay(tid, schema, &spec, keys, batch)?)?;
        let count = batch.len();
        txn.push(target, batch, WireConflictMode::Update, basis)?;
        // Autocommit: the statement is a transaction of its own.
        if self.txn.is_none() && !own.families.is_empty() {
            self.ack(Request::PushTxn(&own.families)).await?;
        }
        Ok(count)
    }
}

fn no_transaction() -> ClientError {
    ClientError::from("no transaction open")
}

/// A transaction's buffered writes, which [`GnitzClient::txn_commit`] ships as one
/// `PUSH_TXN` frame; dropping it sends nothing. Per tid, the families are the
/// maximal same-mode runs of the writes in call order, the order the engine
/// validates and applies them in.
#[derive(Default)]
pub(crate) struct TxnBuffer {
    /// Creation order; per tid, maximal same-mode runs of the caller's op
    /// sequence. Only a tid's last family can still grow (a matching-mode
    /// append extends it), which is what makes indexing a tid's families in
    /// creation order the same as indexing in append order.
    families: Vec<PushFamily>,
    /// Every tid with a family.
    tids: HashMap<u64, TidWrites>,
}

/// One tid's families and the read index over them.
#[derive(Default)]
struct TidWrites {
    /// Its families' indices in `families`, in creation order.
    fams: Vec<usize>,
    /// Rows already folded into `last_op`: those of every family before
    /// `fams[at]`, and the first `rows` of `fams[at]`. Only the last family
    /// grows, so this is a watermark.
    at: usize,
    rows: usize,
    /// PK → `(position in fams, row index)` of the LAST op buffered on that PK.
    /// Row indices are stable: `push` only ever extends a family batch or
    /// pushes a new one. Weight-0 rows are not indexed — they are inert,
    /// exactly as the engine's fold treats them.
    last_op: HashMap<PkBuf, (usize, usize)>,
}

impl TidWrites {
    /// The read index, once every row buffered since the last catch-up is
    /// folded into it. Each row is folded at most once, so the whole index
    /// costs O(rows buffered) across the transaction however often it is read —
    /// and nothing at all for a transaction that never reads.
    fn index(&mut self, families: &[PushFamily]) -> &HashMap<PkBuf, (usize, usize)> {
        let TidWrites { fams, at, rows, last_op } = self;
        let from = *at;
        for (p, &fam) in fams.iter().enumerate().skip(from) {
            let batch = families[fam].batch.view();
            let start = if p == from { *rows } else { 0 };
            for row in start..batch.len() {
                if batch.get_weight(row) != 0 {
                    last_op.insert(PkBuf::from_bytes(batch.get_pk_bytes(row)), (p, row));
                }
            }
            (*at, *rows) = (p, batch.len());
        }
        last_op
    }
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
    /// Refused when `batch` is not in the layout `tid` already holds. A batch shared
    /// with its caller is buffered without a copy, and copied only when the family it
    /// opened is extended while the caller still holds it.
    pub(crate) fn push(
        &mut self,
        target: impl Into<Target>,
        batch: impl Into<Arc<ZSetBatch>>,
        mode: WireConflictMode,
        basis: u64,
    ) -> Result<(), ClientError> {
        let (target, batch) = (target.into(), batch.into());
        let tid = target.tid;
        if batch.is_empty() {
            return Ok(());
        }
        self.check_layout(tid, batch.schema())?;
        let last = self.tids.get(&tid).and_then(|t| t.fams.last().copied());
        match last.filter(|&i| self.families[i].mode == mode) {
            Some(i) => {
                let f = &mut self.families[i];
                // Copies the batch first while its pusher still shares it, so a
                // shared batch is never written; row indices are kept either way.
                Arc::make_mut(&mut f.batch).extend_from(&batch);
                f.basis = f.basis.min(basis);
                if f.target.token == 0 {
                    f.target = target;
                }
            }
            None => {
                self.tids.entry(tid).or_default().fams.push(self.families.len());
                self.families.push(PushFamily { target, batch, mode, basis });
            }
        }
        Ok(())
    }

    /// `schema` has the layout of `tid`'s first family, which every later one shares.
    fn check_layout(&self, tid: u64, schema: &Schema) -> Result<(), ClientError> {
        let Some(t) = self.tids.get(&tid) else {
            return Ok(());
        };
        match self.families[t.fams[0]]
            .batch
            .schema()
            .layout()
            .same_region_types(schema.layout())
        {
            true => Ok(()),
            false => Err(ClientError::from(format!(
                "relation {tid} changed layout during this transaction"
            ))),
        }
    }

    /// The keys of `set` this transaction has not written — what a read of
    /// `set` still has to ask the server. `None` when it wrote none of them.
    fn unwritten(&mut self, tid: u64, set: &PkKeys) -> Option<PkKeys> {
        let index = self.tids.get_mut(&tid)?.index(&self.families);
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
        schema: &Arc<Schema>,
        spec: &ReadSpec,
        keys: bool,
        committed: ZSetBatch,
    ) -> Result<ZSetBatch, ClientError> {
        self.check_layout(tid, schema)?;
        let families = &self.families;
        let Some(t) = self.tids.get_mut(&tid) else {
            return Ok(committed);
        };
        // This tid's families alone: a gather's sources all share its layout.
        let views: Vec<MemBatch<'_>> = t.fams.iter().map(|&f| families[f].batch.view()).collect();
        let index = t.index(families);
        let held = committed.view();
        let keep: Vec<u32> = (0..held.len())
            .filter(|&i| !index.contains_key(held.get_pk_bytes(i)))
            .map(|i| i as u32)
            .collect();
        // `gather` compacts the string arena the dropped rows carried.
        let mut out = committed.gather(&keep);

        // The families' live rows, gathered column by column in one pass.
        let mut picks: Vec<Pick> = Vec::new();
        let take = |&(fam, row): &(usize, usize)| {
            if views[fam].get_weight(row) > 0 {
                picks.push((fam as u32, row as u32, 1));
            }
        };
        match &spec.bound {
            ReadBound::PkSet(set) => set.iter().filter_map(|k| index.get(k)).for_each(take),
            _ => index.values().for_each(take),
        }
        let mut live = ZSetBatch::of(schema, gather_rows(&views, schema.layout(), &picks, 0));
        if live.is_empty() {
            return Ok(out);
        }
        let mut filter = RowFilter::for_read(&spec.predicate, spec.bound.range(), schema.layout())
            .map_err(|e| ClientError::from(e.to_string()))?;
        let ranges = filter.ranges(&live.view());
        match keys {
            // The predicate has read the payload; the key reply carries none.
            true => {
                let (v, stride) = (live.view(), schema.layout().pk_stride());
                let kept: Vec<u8> = ranges
                    .iter()
                    .flat_map(|&(s, e)| &v.pk()[s * stride..e * stride])
                    .copied()
                    .collect();
                let added = ZSetBatch::key_rows(out.schema(), &kept, 1);
                out.extend_from_owned(added);
            }
            false => {
                live.retain_ranges(ranges);
                out.extend_from_owned(live);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
#[path = "tests/txn.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/txn.rs"]
mod bench;
