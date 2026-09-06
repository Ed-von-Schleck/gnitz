use std::rc::Rc;

use crate::catalog::CatalogEngine;
use gnitz_store::schema::key::PkBuf;
use gnitz_store::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, BatchBuilder};
use gnitz_wire::type_code;

use super::MasterDispatcher;
use crate::runtime::sal::SalWriter;
use crate::runtime::test_support::SharedRegion;
use crate::runtime::w2m::W2mReceiver;

/// The OPK leading-key span of an unsigned integer value at `width` bytes —
/// unsigned OPK is plain big-endian, so this is exactly what `key_bytes`
/// produces for an index column promoted to that width.
pub(super) fn span_uint(v: u128, width: usize) -> PkBuf {
    PkBuf::from_bytes(&v.to_be_bytes()[16 - width..])
}

/// PK U64 at index 0, one **nullable** payload U64 at index 1.
pub(super) fn two_col_schema() -> SchemaDescriptor {
    crate::test_support::u64_pk_schema(SchemaColumn::new(type_code::U64, 1))
}

/// Rows are `(pk, weight, payload)`; `None` writes a NULL payload cell.
pub(super) fn make_row_batch(schema: SchemaDescriptor, rows: &[(u128, i64, Option<i64>)]) -> Batch {
    let mut bb = BatchBuilder::new(schema);
    for &(pk, weight, val) in rows {
        bb.begin_row(pk, weight);
        for _ in 0..schema.num_payload_cols() {
            match val {
                Some(v) => bb.put_int(v as u128),
                None => bb.put_null(),
            }
        }
        bb.end_row();
    }
    bb.finish()
}

/// An inert dispatcher for the paths that never reach a live cluster: one empty
/// W2M ring per worker, so the wait loops always reach their no-progress arm,
/// and `-1` eventfds, which nothing parks on. Its mappings are leaked, so
/// nothing here has a lifetime a caller could get wrong. `catalog` may be null
/// only where the path under test never calls `cat()`.
pub(super) fn test_dispatcher(worker_pids: Vec<i32>, catalog: *mut CatalogEngine) -> MasterDispatcher {
    const RING_CAP: usize = 64 * 1024;
    const SAL_SIZE: usize = 4096;
    let nw = worker_pids.len();
    let rings = (0..nw)
        .map(|_| unsafe { crate::runtime::w2m::fixtures::test_ring(RING_CAP) }.leak())
        .collect();
    // A real SAL page: `rewind`/`checkpoint_reset` store through the base
    // pointer, so it must not be null.
    let sal = SharedRegion::new(SAL_SIZE).leak();
    MasterDispatcher::new(
        worker_pids,
        catalog,
        0,
        SalWriter::new(sal, -1, SAL_SIZE, nw),
        Rc::new(W2mReceiver::new(rings)),
        vec![-1; nw],
    )
}
