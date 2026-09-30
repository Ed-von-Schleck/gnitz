use std::rc::Rc;

use crate::catalog::CatalogEngine;
use gnitz_store::schema::key::PkBuf;
use gnitz_store::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, BatchBuilder};
use gnitz_wire::TypeCode;

use super::MasterDispatcher;
use crate::runtime::sal::fixtures::test_writer;
use crate::runtime::w2m::{W2mReceiver, W2mWriter};

/// The OPK leading-key span of an unsigned integer value at `width` bytes —
/// unsigned OPK is plain big-endian, so this is exactly what `key_bytes`
/// produces for an index column promoted to that width.
pub(super) fn span_uint(v: u128, width: usize) -> PkBuf {
    PkBuf::from_bytes(&v.to_be_bytes()[16 - width..])
}

/// PK U64 at index 0, one **nullable** payload U64 at index 1.
pub(super) fn two_col_schema() -> SchemaDescriptor {
    crate::test_support::u64_pk_schema(SchemaColumn::new(TypeCode::U64, true))
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

/// A dispatcher over empty W2M rings nothing parks on. `catalog` may be
/// null where the path under test never calls `cat()`.
pub(super) fn test_dispatcher(worker_pids: Vec<i32>, catalog: *mut CatalogEngine) -> MasterDispatcher {
    inert_dispatcher(worker_pids, catalog).0
}

/// [`test_dispatcher`] with each worker ring's writer, for a test that answers in
/// a worker's place.
pub(super) fn test_dispatcher_with_writers(worker_pids: Vec<i32>) -> (MasterDispatcher, Vec<W2mWriter>) {
    inert_dispatcher(worker_pids, std::ptr::null_mut())
}

fn inert_dispatcher(worker_pids: Vec<i32>, catalog: *mut CatalogEngine) -> (MasterDispatcher, Vec<W2mWriter>) {
    let rings: Vec<*mut u8> = worker_pids
        .iter()
        .map(|_| crate::runtime::w2m::fixtures::test_ring(64 * 1024))
        .collect();
    let writers = rings.iter().map(|&p| W2mWriter::new(p)).collect();
    let sal = test_writer(1 << 20, &rings);
    let reactor = crate::runtime::test_support::make_reactor_over(W2mReceiver::new(rings));
    let disp = MasterDispatcher::new(worker_pids, catalog, 0, sal, Rc::new(reactor));
    (disp, writers)
}
