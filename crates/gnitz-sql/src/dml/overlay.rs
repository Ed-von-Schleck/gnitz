//! The transaction read-your-own-writes overlay.
//!
//! An open transaction's writes are buffered, not sent, so a DML statement must
//! resolve its rows against the **effective** state: the transaction's own
//! buffered ops layered over the committed store. `TxnBuffer` indexes each
//! buffered row by PK as it arrives (`last_op`/`last_ops`), so the fold here is
//! a read of that index — the last op per PK, `Present`/`Deleted` by weight sign
//! (mirroring the engine's own per-table fold).
//!
//! - [`resolve_where_matches`] — the committed rows a `ReadSpec` returns, less every
//!   PK the transaction wrote, plus the transaction's own rows it matches (UPDATE,
//!   DELETE).
//! - [`KeyProbe`] — the rows a batch's keys already hold, and a verdict per key (the
//!   ON CONFLICT paths).
//!
//! The map is **empty in autocommit**: `HashMap::new()` does not allocate.

use gnitz_expr::SchemaFacts;
use std::sync::Arc;

use crate::codec::project_schema::key_reply;
use crate::error::GnitzSqlError;
use gnitz_core::{GnitzClient, PkBuf, PkColumn, Schema, ZSetBatch};
use gnitz_expr::RowFilter;
use gnitz_wire::{PkKeys, ReadBound, ReadSink, ReadSpec};
use std::collections::{HashMap, HashSet};

/// A PK's net effect within the transaction so far. `Present` borrows the
/// buffered row in place (its batch + row index); `Deleted` is a tombstone.
pub(super) enum Buffered<'a> {
    Present(&'a ZSetBatch, usize),
    Deleted,
}

/// PK → net effect, borrowing the buffer's rows. Empty in autocommit.
pub(super) type Net<'a> = HashMap<PkBuf, Buffered<'a>>;

/// One incoming row's verdict against the effective state.
pub(crate) enum Conflict {
    /// An earlier row of the same batch already claimed this PK.
    Repeat,
    /// Nothing holds this PK: absent from the store, or buffered as deleted.
    Fresh,
    /// A row holds this PK.
    Existing,
}

/// The rows `spec` matches in the transaction's effective state: the committed reply
/// under `reply_schema` without the PKs the transaction has written, and the
/// transaction's own rows under `schema` that `spec`'s bound and predicate keep.
pub(crate) fn resolve_where_matches(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Arc<Schema>,
    spec: &ReadSpec,
    reply_schema: &Arc<Schema>,
) -> Result<(ZSetBatch, ZSetBatch), GnitzSqlError> {
    // A DML target is a base table, which is never mirrored.
    let committed = client.scan_spec(tid, spec, reply_schema)?;
    let net = buffered_net(client, tid, &spec.bound);
    if net.is_empty() {
        return Ok((committed, ZSetBatch::new(schema)));
    }
    // A PK the transaction has written is decided by its buffered version.
    let keep: Vec<(usize, i64)> = (0..committed.len())
        .filter(|&i| !net.contains_key(committed.pks.get_bytes(i)))
        .map(|i| (i, committed.weights[i]))
        .collect();
    let present = present_rows(&net, schema);
    let mut ranges = Vec::new();
    RowFilter::for_read(&spec.predicate, &spec.bound, schema.as_ref())?.ranges(&present, &mut ranges);
    // `gather` compacts the string arena these rows carry.
    let matched: Vec<(usize, i64)> = ranges
        .iter()
        .flat_map(|&(s, e)| s..e)
        .map(|i| (i, present.weights[i]))
        .collect();
    Ok((committed.gather(&keep), present.gather(&matched)))
}

/// A read of the rows a batch's keys already hold: one `PkSet` gather of them.
pub(crate) struct KeyProbe<'a> {
    pks: &'a PkColumn,
    spec: ReadSpec,
    reply: Arc<Schema>,
}

impl<'a> KeyProbe<'a> {
    /// The keys' rows, whole.
    pub(crate) fn rows(schema: &Arc<Schema>, pks: &'a PkColumn) -> Self {
        KeyProbe::new(schema, pks, ReadSink::all_rows(), Arc::clone(schema))
    }

    /// The keys' rows as keys alone, for their verdicts.
    pub(crate) fn keys(schema: &Schema, pks: &'a PkColumn) -> Result<Self, GnitzSqlError> {
        let (reply, map) = key_reply(schema)?;
        let sink = ReadSink { map: Some(map), ..ReadSink::all_rows() };
        Ok(KeyProbe::new(schema, pks, sink, Arc::new(reply)))
    }

    fn new(schema: &Schema, pks: &'a PkColumn, sink: ReadSink, reply: Arc<Schema>) -> Self {
        let keys = PkKeys::from_keys(schema.pk_stride(), (0..pks.len()).map(|i| pks.get_bytes(i)));
        let spec = ReadSpec {
            bound: ReadBound::PkSet(keys),
            predicate: Vec::new(),
            sink,
        };
        KeyProbe { pks, spec, reply }
    }

    /// The rows that exist — the committed ones in this probe's reply, the transaction's
    /// own under `schema` — and one [`Conflict`] per key. The transaction's last buffered
    /// op on a key wins.
    pub(crate) fn resolve(
        &self,
        client: &mut GnitzClient,
        tid: u64,
        schema: &Arc<Schema>,
    ) -> Result<(ZSetBatch, ZSetBatch, Vec<Conflict>), GnitzSqlError> {
        let (committed, buffered) = resolve_where_matches(client, tid, schema, &self.spec, &self.reply)?;
        let held: HashSet<&[u8]> = (0..committed.len())
            .map(|r| committed.pks.get_bytes(r))
            .chain((0..buffered.len()).map(|r| buffered.pks.get_bytes(r)))
            .collect();
        let mut seen: HashSet<&[u8]> = HashSet::with_capacity(self.pks.len());
        let verdicts = (0..self.pks.len())
            .map(|i| {
                let key = self.pks.get_bytes(i);
                match (seen.insert(key), held.contains(key)) {
                    (false, _) => Conflict::Repeat,
                    (true, true) => Conflict::Existing,
                    (true, false) => Conflict::Fresh,
                }
            })
            .collect();
        Ok((committed, buffered, verdicts))
    }
}

/// The transaction's net effect on `tid`: over a `PkSet`'s keys, else over every PK it
/// touched. **Empty in autocommit**, and in any transaction that has not written `tid`.
fn buffered_net<'a>(client: &'a mut GnitzClient, tid: u64, bound: &ReadBound) -> Net<'a> {
    let Some(buf) = client.txn_reads(tid) else {
        return Net::new();
    };
    match bound {
        ReadBound::PkSet(keys) => keys
            .iter()
            .filter_map(|k| buf.last_op(k).map(|(b, r)| (PkBuf::from_bytes(k), op_of(b, r))))
            .collect(),
        _ => buf.last_ops().map(|(pk, b, r)| (pk, op_of(b, r))).collect(),
    }
}

/// The transaction's own live rows: every `Present` op in `net`, materialized
/// under `schema` as one batch.
fn present_rows(net: &Net, schema: &Schema) -> ZSetBatch {
    let mut out = ZSetBatch::with_capacity(schema, net.len());
    for op in net.values() {
        if let Buffered::Present(batch, row) = op {
            out.copy_row_at(batch, *row, batch.weights[*row]);
        }
    }
    out
}

/// One buffered row's net effect, borrowing it in place: the weight sign decides.
fn op_of(batch: &ZSetBatch, row: usize) -> Buffered<'_> {
    if batch.weights[row] < 0 {
        Buffered::Deleted
    } else {
        Buffered::Present(batch, row)
    }
}

#[cfg(test)]
#[path = "tests/overlay.rs"]
mod tests;
