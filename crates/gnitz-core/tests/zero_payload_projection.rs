#![cfg(feature = "integration")]

//! A `ReadSpec` rows sink whose reply is **nothing but the primary key**: a
//! PK-only reply schema plus a zero-instruction projection program.
//!
//! No planner builds this shape by any other route — the ad-hoc read path always
//! keeps at least one payload slot — and it is what lets `DELETE` read back only
//! the keys it will retract, over a table whose payload would not even fit in one
//! reply frame. What it exercises: a schema with zero payload columns is legal on
//! the wire; a zero-instruction program is a non-empty blob, so the worker takes
//! its projection branch rather than the identity one; the reply carries no blob
//! heap; and the PK region round-trips byte-for-byte, including through a
//! permuted, non-adjacent compound PK whose OPK sign flip must survive.

use gnitz_core::protocol::{ColumnDef, Schema, TypeCode};
use gnitz_core::{BatchAppender, ColData, ExprBuilder, GnitzClient, ZSetBatch};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::{ReadBound, ReadSink, ReadSpec};

/// Per-test unique schema name (each test owns its server; uniqueness keeps a
/// failure unambiguous).
fn unique_schema() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!("zpp{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

/// The PK-only reply for `schema`: its PK columns in `pk_indices()` order, keyed
/// on all of them, with no payload column at all.
fn pk_only_reply_schema(schema: &Schema) -> Schema {
    let cols: Vec<ColumnDef> = schema
        .pk_indices()
        .iter()
        .map(|&ci| schema.columns[ci].clone())
        .collect();
    let k = cols.len();
    Schema::from_parts(cols, (0..k).collect()).expect("a PK-only reply schema is admissible")
}

/// `col > threshold` as a compiled predicate blob, over the SOURCE schema's
/// column indices.
fn gt_predicate(col: usize, threshold: i64) -> Vec<u8> {
    let mut b = ExprBuilder::new();
    let c = b.load_col_int(col);
    let k = b.load_const(threshold);
    let cond = b.cmp_gt(c, k);
    b.build(cond).encode()
}

/// No payload data crossed the wire. `ZSetBatch::new` keeps one `columns` slot
/// per *schema* column, so a PK-only reply still has `pk_count()` of them — every
/// one an empty placeholder, and there is no blob heap behind them because the
/// zero-instruction program relocates nothing.
fn assert_no_payload(reply: &ZSetBatch, reply_schema: &Schema) {
    assert_eq!(reply.columns.len(), reply_schema.pk_indices().len());
    for (i, c) in reply.columns.iter().enumerate() {
        let empty = match c {
            ColData::Fixed(v) => v.is_empty(),
            ColData::Strings(v) => v.is_empty(),
            ColData::Bytes(v) => v.is_empty(),
        };
        assert!(empty, "PK slot {i} of a PK-only reply must stay an empty placeholder");
    }
    reply
        .validate(reply_schema)
        .expect("the reply validates under its schema");
}

/// The keys-only rows sink: a zero-instruction projection, no ORDER BY, no limit.
fn keys_only_sink() -> ReadSink {
    ReadSink::Rows {
        projection: ExprBuilder::new().build(0).encode(),
        order: Vec::new(),
        limit_k: 0,
    }
}

/// A U64 PK, a TEXT column wide enough that an unprojected reply would carry a
/// blob heap, and the integer the predicate reads.
#[test]
fn a_pk_only_reply_returns_exactly_the_matching_keys() {
    let Some(srv) = ServerHandle::start_n(4) else {
        return;
    };
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema();
    client.create_schema(&sn).unwrap();

    let cols = vec![
        ColumnDef::new("id", TypeCode::U64, false),
        ColumnDef::new("v", TypeCode::I64, false),
        ColumnDef::new("s", TypeCode::String, false),
    ];
    client.create_table(&sn, "t", &cols, &[0], false, 0, &[]).unwrap();
    let (tid, schema) = client.resolve_table_id(&sn, "t").unwrap();

    let mut batch = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut batch, &schema);
    for i in 1u64..=200 {
        app.add_row(i as u128, 1).i64_val(i as i64).str_val(&"x".repeat(300));
    }
    client.push(tid, &schema, &batch).unwrap();

    let reply_schema = pk_only_reply_schema(&schema);
    assert_eq!(reply_schema.num_payload_cols(), 0, "the reply is nothing but the key");
    assert_eq!(reply_schema.pk_stride(), schema.pk_stride());

    let spec = ReadSpec::encode_parts(&ReadBound::None, &gt_predicate(1, 150), &keys_only_sink());
    let reply = client
        .scan_spec(tid, &spec, &reply_schema)
        .expect("a PK-only reply must not be rejected")
        .expect("150 of 200 rows match");

    // Weights reach the client unsummed: no top-k gate, one row per key.
    assert!(reply.weights.iter().all(|&w| w == 1), "per-row weights preserved");
    let mut got: Vec<u64> = (0..reply.pks.len()).map(|i| reply.pks.get(i) as u64).collect();
    got.sort_unstable();
    assert_eq!(got, (151u64..=200).collect::<Vec<_>>());

    assert_no_payload(&reply, &reply_schema);
}

/// A **permuted, non-adjacent** compound PK — `pk_cols = [3, 0]`, an `I64` first
/// and a `U32` second, with negative key values so the OPK sign flip is in play.
/// The reply's PK columns are catalog clones in PK-list order, so each one's
/// `pk_byte_offset` is the same running sum and the region is byte-identical.
#[test]
fn a_permuted_compound_pk_round_trips_verbatim() {
    let Some(srv) = ServerHandle::start_n(4) else {
        return;
    };
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema();
    client.create_schema(&sn).unwrap();

    // Column order deliberately unrelated to PK order: the key is (c3, c0).
    let cols = vec![
        ColumnDef::new("c0", TypeCode::U32, false),
        ColumnDef::new("c1", TypeCode::String, false),
        ColumnDef::new("c2", TypeCode::I64, false),
        ColumnDef::new("c3", TypeCode::I64, false),
    ];
    client.create_table(&sn, "t", &cols, &[3, 0], false, 0, &[]).unwrap();
    let (tid, schema) = client.resolve_table_id(&sn, "t").unwrap();
    assert_eq!(schema.pk_stride(), 12, "I64 then U32, tightly packed");

    // `(c3, c0)` packed as the PK region: c3's native LE at offset 0, c0's at 8.
    let key = |c3: i64, c0: u32| -> u128 { (c3 as u64 as u128) | ((c0 as u128) << 64) };
    let rows: Vec<(i64, u32, i64)> = vec![(-9_000_000_000, 7, 10), (-1, 4_294_967_295, 20), (0, 0, 30), (5, 1, 40)];
    let mut batch = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut batch, &schema);
    for &(c3, c0, c2) in &rows {
        app.add_row(key(c3, c0), 1).str_val("payload").i64_val(c2);
    }
    client.push(tid, &schema, &batch).unwrap();

    let reply_schema = pk_only_reply_schema(&schema);
    assert_eq!(reply_schema.pk_stride(), schema.pk_stride());
    assert_eq!(reply_schema.num_payload_cols(), 0);

    // c2 > 15 → every row but the first
    let spec = ReadSpec::encode_parts(&ReadBound::None, &gt_predicate(2, 15), &keys_only_sink());
    let reply = client
        .scan_spec(tid, &spec, &reply_schema)
        .expect("a compound PK-only reply must not be rejected")
        .expect("three rows match");

    let mut got: Vec<Vec<u8>> = (0..reply.pks.len())
        .map(|i| reply.pks.get_tuple(i).as_bytes().to_vec())
        .collect();
    let mut want: Vec<Vec<u8>> = rows[1..]
        .iter()
        .map(|&(c3, c0, _)| {
            let mut b = Vec::with_capacity(12);
            b.extend_from_slice(&c3.to_le_bytes());
            b.extend_from_slice(&c0.to_le_bytes());
            b
        })
        .collect();
    got.sort();
    want.sort();
    assert_eq!(got, want, "both PK columns round-trip verbatim, sign flip included");
    assert_no_payload(&reply, &reply_schema);

    // The un-projected read of the same rows carries the payload the DELETE shape
    // drops — the whole point of the projection.
    let full = client
        .scan_spec(
            tid,
            &ReadSpec::encode_parts(&ReadBound::None, &Vec::new(), &ReadSink::all_rows()),
            &schema,
        )
        .unwrap()
        .expect("the table is non-empty");
    assert!(
        matches!(&full.columns[1], ColData::Strings(v) if v.len() == rows.len()),
        "the identity sink still returns the TEXT column",
    );
}
