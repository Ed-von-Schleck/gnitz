//! The transaction read-your-own-writes overlay.
//!
//! An open transaction's writes are buffered, not sent, so a DML statement must
//! resolve its rows against the **effective** state: the transaction's own
//! buffered ops layered over the committed store. `TxnBuffer` indexes each
//! buffered row by PK as it arrives (`last_op`/`last_ops`), so the fold here is
//! a read of that index — the last op per PK, `Present`/`Deleted` by weight sign
//! (mirroring the engine's own per-table fold).
//!
//! - [`resolve_where_matches`] — the one overlay read: the committed reply of an
//!   access plan, less every PK the transaction wrote, plus the transaction's own
//!   rows the plan matches (UPDATE, DELETE, and through [`effective_rows`] the ON
//!   CONFLICT paths).
//! - [`effective_rows`] — a PK column's existing rows and per-row verdicts.
//! - [`buffered_net`] — the net map over the keys a plan restricts the buffer to.
//!   It borrows each buffered row **in place** (no copy).
//! - [`present_rows`] — that map's live rows, materialized once as a batch, before
//!   the caller's next `&mut client`.
//!
//! The map is **empty in autocommit**: `HashMap::new()` does not allocate.

use std::sync::Arc;

use crate::dml::plan::{fetch_bound, AccessPlan, BufferedKeys};
use crate::error::GnitzSqlError;
use crate::exec::residual::matching_indices;
use gnitz_core::{GnitzClient, PkBuf, PkColumn, Schema, ZSetBatch};
use gnitz_wire::{PkKeys, ReadBound, ReadSink};
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
    /// Row `usize` of the returned batch holds it.
    Existing(usize),
}

/// The rows `plan` matches in the transaction's effective state: the committed
/// reply under `reply_schema` without the PKs the transaction has written, and the
/// transaction's own matching rows under `schema`. Each part's arena holds only
/// its own rows' strings.
///
/// Committed reply rows are final — the server applied the whole WHERE. Only the
/// buffered rows, which no server-side walk ever saw, are re-filtered here.
pub(crate) fn resolve_where_matches(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Arc<Schema>,
    plan: &AccessPlan<'_>,
    sink: &ReadSink,
    reply_schema: &Arc<Schema>,
) -> Result<(ZSetBatch, ZSetBatch), GnitzSqlError> {
    let committed = fetch_bound(client, tid, &plan.access, sink, reply_schema)?;
    let (keys, preds) = plan.buffered_scope(schema);
    let net = buffered_net(client, tid, keys);
    if net.is_empty() {
        return Ok((committed, ZSetBatch::new(schema)));
    }
    // A PK the transaction has written is decided by its buffered version,
    // whatever the committed row said.
    let keep: Vec<(usize, i64)> = (0..committed.len())
        .filter(|&i| !net.contains_key(committed.pks.get_bytes(i)))
        .map(|i| (i, committed.weights[i]))
        .collect();
    let present = present_rows(&net, schema);
    let matched: Vec<(usize, i64)> = matching_indices(preds, &present, schema)?
        .into_iter()
        .map(|i| (i, present.weights[i]))
        .collect();
    Ok((committed.gather(&keep), present.gather(&matched)))
}

/// One [`Conflict`] per row of `pks`, plus the rows that exist as one owned batch.
/// The transaction's last buffered op on a key wins; every other key comes from
/// one `PkSet` gather.
pub(crate) fn effective_rows(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Arc<Schema>,
    pks: &PkColumn,
) -> Result<(ZSetBatch, Vec<Conflict>), GnitzSqlError> {
    let keys = PkKeys::from_keys(schema.pk_stride(), (0..pks.len()).map(|i| pks.get_bytes(i)));
    // No WHERE behind it, so the gather is the whole selection.
    let plan = AccessPlan::new(ReadBound::PkSet(keys), &[], Vec::new(), schema)?;
    let (mut rows, buffered) = resolve_where_matches(client, tid, schema, &plan, &ReadSink::all_rows(), schema)?;
    rows.extend_from_owned(buffered);
    let verdicts = {
        let at: HashMap<&[u8], usize> = (0..rows.len()).map(|r| (rows.pks.get_bytes(r), r)).collect();
        let mut seen: HashSet<&[u8]> = HashSet::with_capacity(pks.len());
        (0..pks.len())
            .map(|i| {
                let key = pks.get_bytes(i);
                match (seen.insert(key), at.get(key)) {
                    (false, _) => Conflict::Repeat,
                    (true, Some(&row)) => Conflict::Existing(row),
                    (true, None) => Conflict::Fresh,
                }
            })
            .collect()
    };
    Ok((rows, verdicts))
}

/// The transaction's net effect on `tid` over `keys`: an exact key set restricts
/// it — its residual does not constrain the PK, so an unrelated buffered row must
/// not enter the candidates — else it is every PK the transaction touched.
///
/// **Empty in autocommit**, and in any transaction that has not written `tid`.
pub(crate) fn buffered_net<'a>(client: &'a mut GnitzClient, tid: u64, keys: BufferedKeys<'_>) -> Net<'a> {
    let Some(buf) = client.txn_reads(tid) else {
        return Net::new();
    };
    match keys {
        BufferedKeys::Keys(keys) => keys
            .iter()
            .filter_map(|k| buf.last_op(k).map(|(b, r)| (PkBuf::from_bytes(k), op_of(b, r))))
            .collect(),
        BufferedKeys::Point(pk) => buf
            .last_op(pk.pk_bytes())
            .map(|(b, r)| (pk, op_of(b, r)))
            .into_iter()
            .collect(),
        BufferedKeys::All => buf.last_ops().map(|(pk, b, r)| (pk, op_of(b, r))).collect(),
    }
}

/// The transaction's own live rows: every `Present` op in `net`, materialized
/// under `schema` as one batch.
pub(crate) fn present_rows(net: &Net, schema: &Schema) -> ZSetBatch {
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
