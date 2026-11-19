//! [`key_reply`](crate::txn::key_reply): a `ReadSpec` rows sink whose reply is **nothing but
//! the primary key**, a PK-only reply schema plus a zero-instruction projection
//! program.
//!
//! What it exercises: a schema with zero payload columns is legal on the wire; a
//! zero-instruction program is a non-empty blob, so the worker takes its
//! projection branch rather than the identity one; the reply carries no blob heap;
//! and the PK region round-trips byte-for-byte, including through a permuted,
//! non-adjacent compound PK whose OPK sign flip must survive.

use super::*;
use crate::txn::key_reply;
use gnitz_expr::CmpOp;
use gnitz_zset_testkit::cmp_all;

/// The key reply of `schema`'s rows matching `predicate`, checked to carry no
/// payload slot and no blob heap, every row at weight 1.
fn read_keys(client: &mut GnitzClient, tid: u64, schema: &Schema, predicate: Vec<u8>) -> ZSetBatch {
    let (reply_schema, sink) = key_reply(schema);
    assert_eq!(
        reply_schema.layout().num_payload_cols(),
        0,
        "the reply is nothing but the key"
    );
    assert_eq!(reply_schema.layout().pk_stride(), schema.layout().pk_stride());
    let spec = ReadSpec { bound: ReadBound::None, predicate, sink };
    let reply = block_on(client.scan_served(tid, &spec, &reply_schema))
        .expect("a PK-only reply must not be rejected")
        .batch;
    assert!(reply.view().schema().num_payload_cols() == 0 && reply.view().blob().is_empty());
    reply.validate().expect("the reply validates under its schema");
    // Weights reach the client unsummed: no top-k gate, one row per key.
    assert!(reply.view().weights().all(|w| w == 1), "per-row weights preserved");
    reply
}

/// A U64 PK, a TEXT column wide enough that an unprojected reply would carry a
/// blob heap, and the integer the predicate reads.
#[test]
fn a_pk_only_reply_returns_exactly_the_matching_keys() {
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (_, tid, schema) = create_table(
        &mut client,
        schema_of(&[("id", TypeCode::U64), ("v", TypeCode::I64), ("s", TypeCode::String)]),
    );

    let mut batch = ZSetBatch::new(&schema);
    let mut app = batch.builder();
    for i in 1u64..=200 {
        app.begin_row(i as u128, 1)
            .put_int((i as i64) as u128)
            .put_string(&"x".repeat(300));
    }
    block_on(client.push(tid, batch, WireConflictMode::Update)).unwrap();

    let reply = read_keys(
        &mut client,
        tid,
        &schema,
        cmp_all(&[(CmpOp::Gt, 1, 150)]).to_blob_bytes(),
    );
    let mut got: Vec<u64> = (0..reply.len()).map(|i| reply.pk(i) as u64).collect();
    got.sort_unstable();
    assert_eq!(got, (151u64..=200).collect::<Vec<_>>());
}

/// A **permuted, non-adjacent** compound PK — `pk_cols = [3, 0]`, an `I64` first
/// and a `U32` second, with negative key values so the OPK sign flip is in play.
/// The reply's PK columns are catalog clones in PK-list order, so each one's
/// `locate`'s `byte_off` is the same running sum and the region is byte-identical.
#[test]
fn a_permuted_compound_pk_round_trips_verbatim() {
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    // Column order deliberately unrelated to PK order: the key is (c3, c0).
    let schema = schema_of(&[
        ("c0", TypeCode::U32),
        ("c1", TypeCode::String),
        ("c2", TypeCode::I64),
        ("c3", TypeCode::I64),
    ]);
    let schema = Schema::from_parts(schema.columns().to_vec(), &[3, 0]).unwrap();
    let (_, tid, schema) = create_table(&mut client, schema);
    assert_eq!(schema.layout().pk_stride(), 12, "I64 then U32, tightly packed");

    let rows: Vec<(i64, u32, i64)> = vec![(-9_000_000_000, 7, 10), (-1, 4_294_967_295, 20), (0, 0, 30), (5, 1, 40)];
    let mut batch = ZSetBatch::new(&schema);
    let mut app = batch.builder();
    for &(c3, c0, c2) in &rows {
        app.begin_row_natives(&[c3 as u128, c0 as u128], 1)
            .put_string("payload")
            .put_int(c2 as u128);
    }
    block_on(client.push(tid, batch, WireConflictMode::Update)).unwrap();

    // c2 > 15 → every row but the first
    let reply = read_keys(
        &mut client,
        tid,
        &schema,
        cmp_all(&[(CmpOp::Gt, 2, 15)]).to_blob_bytes(),
    );
    let mut got: Vec<Vec<u8>> = (0..reply.len())
        .map(|i| reply.view().get_pk_bytes(i).to_vec())
        .collect();
    // The OPK image, spelled by hand: a signed I64 is big-endian with the sign
    // bit flipped, an unsigned U32 is plain big-endian.
    let mut want: Vec<Vec<u8>> = rows[1..]
        .iter()
        .map(|&(c3, c0, _)| [((c3 as u64) ^ (1u64 << 63)).to_be_bytes().as_slice(), &c0.to_be_bytes()].concat())
        .collect();
    got.sort();
    want.sort();
    assert_eq!(got, want, "both PK columns round-trip verbatim, sign flip included");
}
