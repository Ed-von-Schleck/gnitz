#![cfg(feature = "integration")]

//! A base table's SAL replay applies every committed group, whatever LSNs its
//! shards carry.

use gnitz_core::{BatchAppender, ColumnDef, GnitzClient, Schema, TableProps, TypeCode, ZSetBatch};
use gnitz_test_harness::{unique_schema, ServerHandle};

const WORKERS: usize = 2;

/// Keys the engine's own router places on worker 1 of [`WORKERS`].
fn keys_on_worker_one(n: usize) -> Vec<u64> {
    (0u64..)
        .filter(|k| {
            let mut opk = [0u8; 8];
            gnitz_wire::encode_pk_column(&k.to_le_bytes(), gnitz_wire::type_code::U64, &mut opk);
            gnitz_wire::worker_for_pk_bytes(&opk, WORKERS) == 1
        })
        .take(n)
        .collect()
}

fn push_one(client: &mut GnitzClient, tid: u64, schema: &Schema, k: u64) {
    let mut batch = ZSetBatch::new(schema);
    BatchAppender::new(&mut batch, schema)
        .add_row(k as u128, 1)
        .i64_val(k as i64);
    client.push(tid, schema, &batch).unwrap();
}

#[test]
fn acked_pushes_survive_a_worker_counter_ahead_of_the_zone_seed() {
    let mut srv = ServerHandle::start_n(WORKERS);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("base_replay");
    client.create_schema(&sn).unwrap();
    let cols = vec![
        ColumnDef::new("pk", TypeCode::U64, false),
        ColumnDef::new("v", TypeCode::I64, false),
    ];
    let tid = client
        .create_table(&sn, "t", &cols, &[0], TableProps::default(), &[])
        .unwrap();
    let schema = Schema { columns: cols, pk_cols: vec![0] };
    let keys = keys_on_worker_one(420);

    // Worker 1's shards will carry LSNs far above the master's zone numbering.
    for &k in &keys[..400] {
        push_one(&mut client, tid, &schema, k);
    }
    drop(client);
    // Replay flushes them into worker 1's shards.
    srv.restart();
    // An empty tail: the next zones number from the system tables alone.
    srv.restart();

    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    for &k in &keys[400..] {
        push_one(&mut client, tid, &schema, k);
    }
    drop(client);
    srv.restart();

    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let gnitz_core::ScanReply { schema: got_schema, batch, .. } = client.scan(tid).unwrap();
    let mut got: Vec<(u64, i64)> = (0..batch.len())
        .map(|i| (batch.pks.get(&got_schema, i) as u64, batch.weights[i]))
        .collect();
    got.sort_unstable();
    let mut want: Vec<(u64, i64)> = keys.iter().map(|&k| (k, 1)).collect();
    want.sort_unstable();
    assert_eq!(got, want, "every ACKed push must survive at weight 1");
}
