//! A base table's SAL replay applies every committed group, whatever LSNs its
//! shards carry.

use super::*;
use gnitz_core::block_on;
use gnitz_core::PkColumn;

const WORKERS: usize = 2;

#[test]
fn acked_pushes_survive_a_worker_counter_ahead_of_the_zone_seed() {
    let mut srv = ServerHandle::start_n(WORKERS);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (_, tid, schema) = create_table(&mut client, schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]));
    // Keys the engine's own router places on worker 1, one push each.
    let keys: Vec<u64> = (0u64..)
        .filter(|&k| {
            let opk = PkColumn::from_natives(&schema, [k as u128]);
            let pk = opk.get_bytes(0);
            gnitz_zset::schema::Placement::Keyed { dist_stride: pk.len() as u8 }.owner(pk, WORKERS) == Some(1)
        })
        .take(420)
        .collect();
    let push_each = |client: &mut GnitzClient, keys: &[u64]| {
        for &k in keys {
            block_on(client.push(tid, &schema, rows(&schema, [k]), WireConflictMode::Update)).unwrap();
        }
    };

    // Worker 1's shards will carry LSNs far above the master's zone numbering.
    push_each(&mut client, &keys[..400]);
    drop(client);
    // Replay flushes them into worker 1's shards.
    srv.restart();
    // An empty tail: the next zones number from the system tables alone.
    srv.restart();

    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    push_each(&mut client, &keys[400..]);
    drop(client);
    srv.restart();

    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    assert_eq!(
        weighted_rows(&scan_all(&mut client, tid, &schema)),
        weighted_rows(&rows(&schema, keys)),
        "every ACKed push must survive at weight 1"
    );
}
