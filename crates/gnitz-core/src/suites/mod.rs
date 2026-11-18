//! The client against a real `gnitz-server`, one module per surface.

mod base_replay;
mod ddl_txn;
mod key_reply;
mod read_modify_write;
mod scan_spec_guard;
mod snapshot;
mod spine;
mod transport;
mod view_chain;

use std::os::fd::{AsFd, AsRawFd};
use std::sync::Arc;

use crate::connection::{Request, Session};
use crate::test_support::{drive, park, rel};
use crate::{
    BatchAppender, ClientError, GnitzClient, Interest, PlannedView, RelName, Schema, Sent, TableDef, ViewBundle,
    ZSetBatch, block_on,
};
use gnitz_test_harness::{ServerHandle, unique_schema};
use gnitz_wire::{
    Circuit, ColumnDef, ReadBound, ReadSpec, TableProps, TypeCode, ViewProps, WireConflictMode, read_i64_le,
};

/// Non-nullable `columns`, the first one the PK.
fn schema_of(columns: &[(&str, TypeCode)]) -> Schema {
    Schema::from_parts(
        columns
            .iter()
            .map(|&(name, tc)| ColumnDef::new(name, tc, false))
            .collect(),
        &[0],
    )
    .unwrap()
}

/// `schema` created as table `t` in a fresh schema: `(schema name, tid, schema)`.
fn create_table(client: &mut GnitzClient, schema: Schema) -> (String, u64, Arc<Schema>) {
    let sn = unique_schema("t");
    block_on(client.create_schema(&sn, false)).unwrap();
    let tid = block_on(client.create_table(&TableDef::new(rel(&sn, "t"), schema.clone()))).unwrap();
    (sn, tid, Arc::new(schema))
}

/// One identity segment reading `source_id`.
fn segment(source_id: u64, schema: &Arc<Schema>) -> PlannedView {
    let mut circuit = Circuit::default();
    circuit.input_delta(source_id, ReadBound::None);
    PlannedView {
        circuit,
        schema: Arc::clone(schema),
        pk_repeats: false,
    }
}

/// `segments` then `view`, created as `name` with default options and an empty
/// definition, which the engine stores verbatim and no reader here renders.
fn bundle(name: RelName, segments: Vec<PlannedView>, view: PlannedView) -> ViewBundle {
    ViewBundle {
        name,
        props: Some(ViewProps::default()),
        replacing: None,
        segments,
        view,
        definition: Vec::new(),
    }
}

/// `pks` at weight 1, payload slot `j` of each holding `pk * 3 + j`; every
/// payload column of `schema` is an I64.
fn rows(schema: &Schema, pks: impl IntoIterator<Item = u64>) -> ZSetBatch {
    let mut batch = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut batch);
    for pk in pks {
        let row = app.add_row(pk as u128, 1);
        for j in 0..schema.columns().len() - 1 {
            row.int_val(pk as i64 * 3 + j as i64);
        }
    }
    batch
}

/// Every row of `tid`, decoded under `schema`.
fn scan_all(client: &mut GnitzClient, tid: u64, schema: &Arc<Schema>) -> ZSetBatch {
    block_on(client.scan_served(tid, &ReadSpec::all_rows(ReadBound::None), schema))
        .unwrap()
        .batch
}

/// `(pk, payload cells, weight)` of every row of `batch`, sorted; every payload
/// column is an I64.
fn weighted_rows(batch: &ZSetBatch) -> Vec<(u64, Vec<i64>, i64)> {
    let mut out: Vec<_> = (0..batch.len())
        .map(|i| {
            let cells = (0..batch.payload_count())
                .map(|pi| read_i64_le(batch.col(pi), i * 8))
                .collect();
            (batch.pk(i) as u64, cells, batch.weight(i))
        })
        .collect();
    out.sort();
    out
}

/// `batch` pushed into `tid` as an upsert.
fn push_req<'a>(tid: u64, schema: &'a Schema, batch: &'a ZSetBatch) -> Request<'a> {
    Request::Push {
        target: tid.into(),
        schema,
        batch,
        mode: WireConflictMode::Update,
    }
}

/// The reply to a request submitted ahead of one already answered.
fn arrived<T>(mut sent: Sent<T>) -> Result<T, ClientError> {
    sent.try_take().expect("replies arrive in request order")
}

/// Write everything `s` has queued, parking on writability alone so no reply is
/// read; false once the session has ended.
fn write_out(s: &mut Session) -> bool {
    loop {
        s.step(Interest::WRITE);
        if s.is_closed() {
            return false;
        }
        if !s.interest().write {
            return true;
        }
        park(s, Interest::WRITE);
    }
}
