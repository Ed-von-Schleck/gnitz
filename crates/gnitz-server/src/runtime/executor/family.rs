//! The unit of the write path: one table's rows of a write.

use std::rc::Rc;

use crate::runtime::reactor::RecvBuf;
use gnitz_store::relation::PkRule;
use gnitz_wire::WireConflictMode;
use gnitz_zset::repr::{Batch, CertifiedBlock, MemBatch};
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
    /// A client's block, certified where it lies in the request frame that
    /// carried it, under the schema it was certified under. The frame, and its
    /// charge to the inbound budget, live as long as the rows.
    Frame(Rc<RecvBuf>, CertifiedBlock, SchemaDescriptor),
    /// Rows the master built: a merged run of pushes, or a cascade's
    /// retractions.
    Built(Batch),
}

impl FamilyRows {
    /// The rows, read where they lie.
    pub(super) fn view(&self) -> MemBatch<'_> {
        match self {
            FamilyRows::Frame(frame, block, schema) => block.view(frame.as_slice(), schema),
            FamilyRows::Built(batch) => batch.as_mem_batch(),
        }
    }
}
