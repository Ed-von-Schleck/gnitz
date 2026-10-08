#![cfg(feature = "integration")]

//! The client against a real `gnitz-server`, one module per surface.

mod base_replay;
mod ddl_txn;
mod key_reply;
mod read_modify_write;
mod scan_spec_guard;
mod spine;
mod transport;
mod view_chain;

use gnitz_core::block_on;
use std::sync::Arc;

use gnitz_core::{
    BatchAppender, ClientError, GnitzClient, Interest, RelName, Request, Schema, Sent, Session, ZSetBatch,
};
use gnitz_test_harness::{unique_schema, ServerHandle};
use gnitz_wire::{read_i64_le, ColumnDef, ReadBound, ReadSpec, TableProps, TypeCode, WireConflictMode};

/// `schema.name`.
fn rel(schema: &str, name: &str) -> RelName {
    RelName::new(schema, name).unwrap()
}

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
    block_on(client.create_schema(&sn)).unwrap();
    let tid = block_on(client.create_table(&rel(&sn, "t"), &schema, &[], TableProps::default(), &[])).unwrap();
    (sn, tid, Arc::new(schema))
}

/// `pks` at weight 1, payload slot `j` of each holding `pk * 3 + j`; every
/// payload column of `schema` is an I64.
fn rows(schema: &Schema, pks: impl IntoIterator<Item = u64>) -> ZSetBatch {
    let mut batch = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut batch);
    for pk in pks {
        let row = app.add_row(pk as u128, 1);
        for j in 0..schema.columns().len() - 1 {
            row.i64_val(pk as i64 * 3 + j as i64);
        }
    }
    batch
}

/// Every row of `tid`, decoded under `schema`.
fn scan_all(client: &mut GnitzClient, tid: u64, schema: &Arc<Schema>) -> ZSetBatch {
    block_on(client.scan_spec(tid, &ReadSpec::all_rows(ReadBound::None), schema))
        .unwrap()
        .batch
}

/// `(pk, payload cells, weight)` of every row of `batch`, sorted; every payload
/// column is an I64.
fn weighted_rows(batch: &ZSetBatch) -> Vec<(u64, Vec<i64>, i64)> {
    let mut out: Vec<_> = (0..batch.len())
        .map(|i| {
            let cells = batch.payload.iter().map(|p| read_i64_le(&p.bytes, i * 8)).collect();
            (batch.pks.get(i) as u64, cells, batch.weights[i])
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

/// Park until `s`'s fd is ready for `want` (at most 30 s) and say what is.
fn park(s: &Session, want: Interest) -> Interest {
    assert!(!want.is_empty(), "slots pending but nothing to wait for");
    let mut pfd = libc::pollfd {
        fd: s.as_raw_fd(),
        events: want.poll_events(),
        revents: 0,
    };
    // SAFETY: one valid pollfd.
    let rc = unsafe { libc::poll(&mut pfd, 1, 30_000) };
    assert!(rc > 0, "poll: {}", std::io::Error::last_os_error());
    Interest::from_revents(pfd.revents)
}

/// The driver: step with what `poll` reported, parking on `interest()`, until
/// `last` is answered. Also reports whether both interests were ever armed at
/// once.
fn drive<T>(s: &mut Session, mut last: Sent<T>) -> (Result<T, ClientError>, bool) {
    let mut ready = Interest::WRITE;
    let mut both_armed = false;
    loop {
        s.step(ready);
        if let Some(reply) = last.try_take() {
            return (reply, both_armed);
        }
        let interest = s.interest();
        both_armed |= interest.read && interest.write;
        ready = park(s, interest);
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
