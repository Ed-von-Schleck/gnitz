//! The unit of the write path: one table's rows of a write, and the certified
//! client block they may lie in.

use std::ops::Range;
use std::rc::Rc;

use crate::runtime::reactor::RecvBuf;
use gnitz_store::relation::PkRule;
use gnitz_wire::WireConflictMode;
use gnitz_zset::repr::{Batch, MemBatch};
use gnitz_zset::schema::SchemaDescriptor;

/// One decoded, shape-validated transaction family: the target `tid`, its
/// conflict `mode`, its rows, and the PK rule its workers hold them to. The
/// executor decodes into these, the validator borrows them and extends them
/// with the families a delete cascades to, and the committer takes them by
/// value.
pub(super) struct TxnFamily {
    pub(super) tid: u64,
    pub(super) mode: WireConflictMode,
    pub(super) rows: FamilyRows,
    /// [`PkRule::Enforce`] until the write's validation proves the family
    /// fresh: see [`Checks::validate_txn`](super::validate::Checks::validate_txn).
    pub(super) pk_rule: PkRule,
}

impl TxnFamily {
    /// A family held to [`PkRule::Enforce`].
    pub(super) fn new(tid: u64, mode: WireConflictMode, rows: FamilyRows) -> Self {
        TxnFamily {
            tid,
            mode,
            rows,
            pk_rule: PkRule::Enforce,
        }
    }
}

/// The rows of a [`TxnFamily`].
pub(super) enum FamilyRows {
    /// A client's block, read where it lies in the request frame that carried
    /// it. The frame, and its charge to the inbound budget, live as long as
    /// the rows.
    Frame(Rc<RecvBuf>, PushedBlock),
    /// Rows the master built: a merged run of pushes, or a cascade's
    /// retractions.
    Built(Batch),
}

impl FamilyRows {
    /// The rows, read where they lie.
    pub(super) fn view(&self) -> MemBatch<'_> {
        match self {
            FamilyRows::Frame(frame, block) => block.view(frame.as_slice()),
            FamilyRows::Built(batch) => batch.as_mem_batch(),
        }
    }
}

/// A client's data block that [`MemBatch::certify_foreign`] accepted, at
/// `range` of the bytes it certified, under the schema it was certified under.
pub(super) struct PushedBlock {
    range: Range<usize>,
    schema: SchemaDescriptor,
}

impl PushedBlock {
    /// Certify the block at `range` of `data` under `schema`.
    pub(super) fn certify(
        data: &mut [u8],
        range: Range<usize>,
        schema: SchemaDescriptor,
    ) -> Result<Self, &'static str> {
        MemBatch::certify_foreign(&mut data[range.clone()], &schema)?;
        Ok(PushedBlock { range, schema })
    }

    /// The block's rows, read from `data`: the bytes it was certified in.
    pub(super) fn view<'a>(&'a self, data: &'a [u8]) -> MemBatch<'a> {
        MemBatch::of_wal_block(&data[self.range.clone()], &self.schema).expect("a certified block parses")
    }
}
