use super::*;
use crate::test_support::{rel, wide_rows, wide_schema};
use crate::{TableDef, block_on};
use gnitz_test_harness::{ServerHandle, unique_schema};
use gnitz_wire::{ReadBound, WireConflictMode};
use std::hint::black_box;

/// Client-thread instructions, user mode, per one-key read against a server
/// and the reply's drop, by column count: the request's encode, the socket
/// round trip's reads and writes, the reply's decode into a batch, and its free.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn point_read_bench() {
    const ROUNDS: u64 = 2000;
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let counter = gnitz_foundation::perf::Counter::instructions();
    for ncols in [4usize, 16, 64] {
        let schema = wide_schema(ncols, false);
        let sn = unique_schema("t");
        block_on(client.create_schema(&sn, false)).unwrap();
        let tid = block_on(client.create_table(&TableDef::new(rel(&sn, "t"), (*schema).clone()))).unwrap();
        block_on(client.push(tid, wide_rows(&schema, 100, 0), WireConflictMode::Update)).unwrap();
        let spec = ReadSpec::all_rows(ReadBound::PkSet(schema.pk_keys([[5]])));
        let mut read = || block_on(client.scan_served(tid, &spec, &schema)).unwrap();
        assert_eq!(read().batch.len(), 1);
        let total: u64 = (0..ROUNDS).map(|_| counter.measure(|| drop(black_box(read()))).1).sum();
        println!("point_read_bench {ncols:>2} cols: {} instr", total / ROUNDS);
    }
}

/// Client-thread instructions, user mode, per whole-table read of a 64-column table and the
/// reply's drop: against one worker, whose reply is one block in one frame larger than a
/// socket read, and against four, whose reply is a block per worker.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn scan_reply_bench() {
    const ROUNDS: u64 = 200;
    const ROWS: usize = 2000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    let schema = wide_schema(64, false);
    for workers in [1, 4] {
        let srv = ServerHandle::start_n(workers);
        let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
        let sn = unique_schema("t");
        block_on(client.create_schema(&sn, false)).unwrap();
        let tid = block_on(client.create_table(&TableDef::new(rel(&sn, "t"), (*schema).clone()))).unwrap();
        block_on(client.push(tid, wide_rows(&schema, ROWS, 0), WireConflictMode::Update)).unwrap();
        let spec = ReadSpec::all_rows(ReadBound::None);
        let mut read = || block_on(client.scan_served(tid, &spec, &schema)).unwrap();
        assert_eq!(read().batch.len(), ROWS);
        let total: u64 = (0..ROUNDS).map(|_| counter.measure(|| drop(black_box(read()))).1).sum();
        println!(
            "scan_reply_bench {workers} worker(s), {ROWS} rows of 64 cols: {} instr",
            total / ROUNDS
        );
    }
}
