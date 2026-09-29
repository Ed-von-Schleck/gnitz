#![cfg(feature = "integration")]

//! [`gnitz_core::key_reply`]: a `ReadSpec` rows sink whose reply is **nothing but
//! the primary key**, a PK-only reply schema plus a zero-instruction projection
//! program — a keys-only read over a table whose payload would not fit in one
//! reply frame.
//!
//! What it exercises: a schema with zero payload columns is legal on the wire; a
//! zero-instruction program is a non-empty blob, so the worker takes its
//! projection branch rather than the identity one; the reply carries no blob heap;
//! and the PK region round-trips byte-for-byte, including through a permuted,
//! non-adjacent compound PK whose OPK sign flip must survive.

use gnitz_core::Schema;
use gnitz_core::{key_reply, BatchAppender, GnitzClient, ZSetBatch};
use gnitz_expr::SchemaFacts;
use gnitz_expr::{CmpOp, ExprBuilder, LogicalInstr as L, Sink};
use gnitz_test_harness::{unique_schema, ServerHandle};
use gnitz_wire::{ColumnDef, TypeCode};
use gnitz_wire::{ReadBound, ReadSpec};
use gnitz_wire::{TableProps, WireConflictMode};

/// `col > threshold` as a compiled predicate blob, over the SOURCE schema's
/// column indices.
fn gt_predicate(col: usize, threshold: i64) -> Vec<u8> {
    let mut b = ExprBuilder::new();
    let c = b.emit(L::LoadColInt { col: col as u32 });
    let k = b.emit(L::LoadConst { val: threshold, unsigned: false });
    let cond = b.emit(L::Cmp { op: CmpOp::Gt, a: c, b: k });
    b.build(vec![Sink::Reg(cond)])
        .expect("a well-formed program")
        .to_blob_bytes()
}

/// No payload data crossed the wire: the reply has no payload slot, and there is
/// no blob heap behind it because the zero-instruction program relocates nothing.
fn assert_no_payload(reply: &ZSetBatch, reply_schema: &Schema) {
    assert!(reply.payload.is_empty());
    reply
        .validate(reply_schema)
        .expect("the reply validates under its schema");
}

/// A U64 PK, a TEXT column wide enough that an unprojected reply would carry a
/// blob heap, and the integer the predicate reads.
#[test]
fn a_pk_only_reply_returns_exactly_the_matching_keys() {
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("zpp");
    client.create_schema(&sn).unwrap();

    let cols = vec![
        ColumnDef::new("id", TypeCode::U64, false),
        ColumnDef::new("v", TypeCode::I64, false),
        ColumnDef::new("s", TypeCode::String, false),
    ];
    client
        .create_table(
            &sn,
            "t",
            &Schema { columns: cols, pk_cols: vec![0] },
            &[],
            TableProps::default(),
            &[],
        )
        .unwrap();
    let desc = client.resolve_relation(&sn, "t").unwrap();
    let (tid, schema) = (desc.tid, std::sync::Arc::clone(&desc.schema));

    let mut batch = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut batch, &schema);
    for i in 1u64..=200 {
        app.add_row(i as u128, 1).i64_val(i as i64).str_val(&"x".repeat(300));
    }
    client.push(tid, &schema, &batch, WireConflictMode::Update).unwrap();

    let (reply_schema, sink) = key_reply(&schema);
    assert_eq!(reply_schema.num_payload_cols(), 0, "the reply is nothing but the key");
    assert_eq!(reply_schema.pk_stride(), schema.pk_stride());

    let spec = ReadSpec {
        bound: ReadBound::None,
        predicate: gt_predicate(1, 150),
        sink,
    };
    let reply = client
        .scan_spec(tid, &spec, &reply_schema)
        .expect("a PK-only reply must not be rejected")
        .batch;

    // Weights reach the client unsummed: no top-k gate, one row per key.
    assert!(reply.weights.iter().all(|&w| w == 1), "per-row weights preserved");
    let mut got: Vec<u64> = (0..reply.pks.len())
        .map(|i| reply.pks.get(&reply_schema, i) as u64)
        .collect();
    got.sort_unstable();
    assert_eq!(got, (151u64..=200).collect::<Vec<_>>());

    assert_no_payload(&reply, &reply_schema);
}

/// A **permuted, non-adjacent** compound PK — `pk_cols = [3, 0]`, an `I64` first
/// and a `U32` second, with negative key values so the OPK sign flip is in play.
/// The reply's PK columns are catalog clones in PK-list order, so each one's
/// `locate`'s `byte_off` is the same running sum and the region is byte-identical.
#[test]
fn a_permuted_compound_pk_round_trips_verbatim() {
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("zpp");
    client.create_schema(&sn).unwrap();

    // Column order deliberately unrelated to PK order: the key is (c3, c0).
    let cols = vec![
        ColumnDef::new("c0", TypeCode::U32, false),
        ColumnDef::new("c1", TypeCode::String, false),
        ColumnDef::new("c2", TypeCode::I64, false),
        ColumnDef::new("c3", TypeCode::I64, false),
    ];
    client
        .create_table(
            &sn,
            "t",
            &Schema { columns: cols, pk_cols: vec![3, 0] },
            &[],
            TableProps::default(),
            &[],
        )
        .unwrap();
    let desc = client.resolve_relation(&sn, "t").unwrap();
    let (tid, schema) = (desc.tid, std::sync::Arc::clone(&desc.schema));
    assert_eq!(schema.pk_stride(), 12, "I64 then U32, tightly packed");

    // `(c3, c0)` packed native little-endian in PK-list order — c3 at offset 0,
    // c0 at 8 — which `add_row` OPK-encodes on append.
    let key = |c3: i64, c0: u32| -> u128 { (c3 as u64 as u128) | ((c0 as u128) << 64) };
    let rows: Vec<(i64, u32, i64)> = vec![(-9_000_000_000, 7, 10), (-1, 4_294_967_295, 20), (0, 0, 30), (5, 1, 40)];
    let mut batch = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut batch, &schema);
    for &(c3, c0, c2) in &rows {
        app.add_row(key(c3, c0), 1).str_val("payload").i64_val(c2);
    }
    client.push(tid, &schema, &batch, WireConflictMode::Update).unwrap();

    let (reply_schema, sink) = key_reply(&schema);
    assert_eq!(reply_schema.pk_stride(), schema.pk_stride());
    assert_eq!(reply_schema.num_payload_cols(), 0);

    // c2 > 15 → every row but the first
    let spec = ReadSpec {
        bound: ReadBound::None,
        predicate: gt_predicate(2, 15),
        sink,
    };
    let reply = client
        .scan_spec(tid, &spec, &reply_schema)
        .expect("a compound PK-only reply must not be rejected")
        .batch;

    let mut got: Vec<Vec<u8>> = (0..reply.pks.len()).map(|i| reply.pks.get_bytes(i).to_vec()).collect();
    // The OPK image, spelled by hand: a signed I64 is big-endian with the sign
    // bit flipped, an unsigned U32 is plain big-endian.
    let mut want: Vec<Vec<u8>> = rows[1..]
        .iter()
        .map(|&(c3, c0, _)| {
            let mut b = Vec::with_capacity(12);
            b.extend_from_slice(&((c3 as u64) ^ (1u64 << 63)).to_be_bytes());
            b.extend_from_slice(&c0.to_be_bytes());
            b
        })
        .collect();
    got.sort();
    want.sort();
    assert_eq!(got, want, "both PK columns round-trip verbatim, sign flip included");
    assert_no_payload(&reply, &reply_schema);

    // The un-projected read of the same rows carries the payload the key reply drops.
    let full = client
        .scan_spec(tid, &ReadSpec::all_rows(ReadBound::None), &schema)
        .unwrap()
        .batch;
    assert_eq!(
        full.payload[0].bytes.len(),
        rows.len() * 16,
        "the identity sink still returns the TEXT column",
    );
}
