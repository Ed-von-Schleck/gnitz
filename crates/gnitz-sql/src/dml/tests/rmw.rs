use super::*;
use crate::expr_lower::compile_wire_conjuncts;
use crate::test_support::{bind_where, pk_schema, table};
use gnitz_core::{retraction_batch, BatchAppender, PkColumn, TxnBuffer, TypeCode, WireConflictMode};
use gnitz_expr::SchemaFacts;
use gnitz_wire::PkKeys;

const TID: u64 = 7;

/// `(id U64 pk, v I64)`.
fn schema() -> Arc<Schema> {
    Arc::new(pk_schema(TypeCode::U64))
}

/// `(id, v, weight)` rows.
fn rows(schema: &Schema, rows: &[(u128, i64, i64)]) -> ZSetBatch {
    let mut b = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut b, schema);
    for &(pk, v, w) in rows {
        app.add_row(pk, w).i64_val(v);
    }
    b
}

fn del(schema: &Schema, pk: u128) -> ZSetBatch {
    retraction_batch(schema, PkColumn::from_natives(schema, [pk]))
}

/// A read of `TID` under `bound` and the WHERE `predicate` (SQL over `t`).
fn read(schema: &Arc<Schema>, bound: ReadBound, predicate: Option<&str>, keys: bool) -> TargetRead {
    let predicate = predicate.map_or_else(Vec::new, |sql| {
        compile_wire_conjuncts(&bind_where(sql, schema), &schema.columns).expect("the predicate compiles")
    });
    let target = table(TID, schema.columns.clone(), schema.pk_cols.clone());
    TargetRead::new(&target, bound, predicate, keys).unwrap()
}

/// `read`'s merge of `committed` with `buf`'s ops on `TID`.
fn merged(read: &TargetRead, committed: ZSetBatch, buf: &mut TxnBuffer) -> ZSetBatch {
    let txn = buf
        .reads(TID, &read.schema)
        .unwrap()
        .expect("the transaction wrote TID");
    read.merge(committed, &txn).unwrap()
}

/// `(id, v, weight)`, sorted.
fn contents(schema: &Schema, b: &ZSetBatch) -> Vec<(u128, i64, i64)> {
    let mut out: Vec<_> = (0..b.len())
        .map(|i| {
            let v = i64::from_le_bytes(b.payload[0].bytes[i * 8..i * 8 + 8].try_into().unwrap());
            (b.pks.get(schema, i), v, b.weights[i])
        })
        .collect();
    out.sort();
    out
}

#[test]
fn the_last_buffered_op_on_a_key_wins() {
    let s = schema();
    let mut buf = TxnBuffer::default();
    buf.push(TID, &s, rows(&s, &[(1, 11, 1)]), WireConflictMode::Update, 3)
        .unwrap();
    buf.push(TID, &s, rows(&s, &[(1, 12, 1)]), WireConflictMode::Update, 3)
        .unwrap();
    let committed = rows(&s, &[(1, 10, 1), (2, 20, 1)]);
    let out = merged(&read(&s, ReadBound::None, None, false), committed, &mut buf);
    assert_eq!(contents(&s, &out), [(1, 12, 1), (2, 20, 1)]);
}

#[test]
fn a_delete_then_reinsert_is_present() {
    let s = schema();
    let mut buf = TxnBuffer::default();
    buf.push(TID, &s, del(&s, 5), WireConflictMode::Update, 3).unwrap();
    buf.push(TID, &s, rows(&s, &[(5, 99, 1)]), WireConflictMode::Error, 3)
        .unwrap();
    let committed = rows(&s, &[(5, 50, 1)]);
    let out = merged(&read(&s, ReadBound::None, None, false), committed, &mut buf);
    assert_eq!(contents(&s, &out), [(5, 99, 1)]);
}

#[test]
fn a_tid_scopes_its_ops() {
    let s = schema();
    let mut buf = TxnBuffer::default();
    buf.push(TID, &s, rows(&s, &[(1, 10, 1)]), WireConflictMode::Update, 3)
        .unwrap();
    buf.push(TID + 1, &s, rows(&s, &[(2, 20, 1)]), WireConflictMode::Update, 3)
        .unwrap();
    let out = merged(&read(&s, ReadBound::None, None, false), ZSetBatch::new(&s), &mut buf);
    assert_eq!(contents(&s, &out), [(1, 10, 1)], "another tid's row stays out");
}

#[test]
fn a_tombstone_drops_the_committed_row() {
    let s = schema();
    let mut buf = TxnBuffer::default();
    buf.push(TID, &s, del(&s, 2), WireConflictMode::Update, 3).unwrap();
    let committed = rows(&s, &[(1, 10, 1), (2, 20, 1)]);
    let out = merged(&read(&s, ReadBound::None, None, false), committed, &mut buf);
    assert_eq!(contents(&s, &out), [(1, 10, 1)]);
}

#[test]
fn a_pk_set_restricts_the_buffered_side_to_its_keys() {
    let s = schema();
    let mut buf = TxnBuffer::default();
    buf.push(
        TID,
        &s,
        rows(&s, &[(1, 11, 1), (2, 22, 1)]),
        WireConflictMode::Update,
        3,
    )
    .unwrap();
    let keys = PkKeys::from_keys(s.pk_stride(), [s.opk_key_cols(&[1]).pk_bytes()]);
    let out = merged(
        &read(&s, ReadBound::PkSet(keys), None, false),
        ZSetBatch::new(&s),
        &mut buf,
    );
    assert_eq!(contents(&s, &out), [(1, 11, 1)]);
}

#[test]
fn the_predicate_filters_buffered_rows() {
    let s = schema();
    let mut buf = TxnBuffer::default();
    buf.push(
        TID,
        &s,
        rows(&s, &[(1, 10, 1), (2, 20, 1)]),
        WireConflictMode::Update,
        3,
    )
    .unwrap();
    let out = merged(
        &read(&s, ReadBound::None, Some("v > 15"), false),
        ZSetBatch::new(&s),
        &mut buf,
    );
    assert_eq!(contents(&s, &out), [(2, 20, 1)]);
}

/// A keys reply carries no payload, so a buffered row joins it as its key alone.
#[test]
fn a_keys_reply_appends_keys_only() {
    let s = schema();
    let mut buf = TxnBuffer::default();
    buf.push(TID, &s, rows(&s, &[(3, 30, 1)]), WireConflictMode::Update, 3)
        .unwrap();
    let read = read(&s, ReadBound::None, None, true);
    let mut committed = ZSetBatch::new(&read.reply);
    committed.pks.push_natives(&read.reply, &[1]);
    committed.weights.push(1);
    committed.nulls.push(0);
    let out = merged(&read, committed, &mut buf);
    assert!(out.payload.is_empty(), "the key reply has no payload column");
    let keys: Vec<u128> = (0..out.len()).map(|i| out.pks.get(&read.reply, i)).collect();
    assert_eq!(keys, [1, 3]);
    assert_eq!(
        (out.weights.as_slice(), out.nulls.as_slice()),
        (&[1, 1][..], &[0, 0][..])
    );
}

/// The driver against a real server: two connections on one table `t(pk, val)`
/// holding `(1, 0)`, and the statement `UPDATE t SET val = val + 1 WHERE pk = 1`
/// run by `a` as a `TargetRead` plus an `apply_set` build.
#[cfg(feature = "integration")]
mod occ {
    use super::*;
    use crate::dml::mutate::{apply_set, bind_set_list, SetClause};
    use crate::test_support::parse_stmt;
    use gnitz_core::{ClientError, GnitzClient, WireFault, WireStatus};
    use gnitz_test_harness::ServerHandle;

    struct Fixture {
        _srv: ServerHandle,
        a: GnitzClient,
        b: GnitzClient,
        target: Arc<RelDescriptor>,
    }

    fn boot() -> Fixture {
        let srv = ServerHandle::start_n(2);
        let mut a = GnitzClient::connect(srv.sock_path()).unwrap();
        let b = GnitzClient::connect(srv.sock_path()).unwrap();
        a.create_schema("occ").unwrap();
        crate::execute(
            &mut a,
            "occ",
            "CREATE TABLE t (pk BIGINT PRIMARY KEY, val BIGINT NOT NULL); INSERT INTO t VALUES (1, 0)",
        )
        .unwrap();
        let target = a.resolve_relation("occ", "t").unwrap();
        Fixture { _srv: srv, a, b, target }
    }

    /// Commit `(pk, val)` into `t` through `client`.
    fn commit(client: &mut GnitzClient, target: &RelDescriptor, pk: u128, val: i64) {
        let s = &target.schema;
        let mut batch = ZSetBatch::new(s);
        BatchAppender::new(&mut batch, s).add_row(pk, 1).i64_val(val);
        client.push(target.tid, s, &batch).unwrap();
    }

    /// The read of pk 1 and the `val = val + 1` build the statement runs, which
    /// calls `side` first on every attempt with its 0-based attempt number.
    fn run_increment(
        f: &mut Fixture,
        mut side: impl FnMut(&mut GnitzClient, &RelDescriptor, usize),
    ) -> (Result<usize, GnitzSqlError>, usize) {
        let s = Arc::clone(&f.target.schema);
        let keys = PkKeys::from_keys(s.pk_stride(), [s.opk_key_cols(&[1]).pk_bytes()]);
        let read = TargetRead::new(&f.target, ReadBound::PkSet(keys), Vec::new(), false).unwrap();
        let sqlparser::ast::Statement::Update(u) = parse_stmt("UPDATE t SET val = val + 1") else {
            unreachable!()
        };
        let mut set = bind_set_list(&u.assignments, &s, "t", SetClause::Update).unwrap();
        let mut attempts = 0;
        let Fixture { a, b, target, .. } = f;
        let result = commit_rmw(a, &read, |rows| {
            side(&mut *b, target, attempts);
            attempts += 1;
            apply_set(&mut set, rows, None, &s)
        });
        (result, attempts)
    }

    /// Row pk 1 of `t`, as `(val, weight)`.
    fn row_1(client: &mut GnitzClient, target: &RelDescriptor) -> Vec<(i64, i64)> {
        let reply = client.scan(target.tid).unwrap();
        let s = &target.schema;
        (0..reply.batch.len())
            .filter(|&i| reply.batch.pks.get(s, i) == 1)
            .map(|i| {
                let v = i64::from_le_bytes(reply.batch.payload[0].bytes[i * 8..i * 8 + 8].try_into().unwrap());
                (v, reply.batch.weights[i])
            })
            .collect()
    }

    /// A write landing after every read exhausts the bound, and the conflict
    /// names the table.
    #[test]
    fn sustained_contention_surfaces_a_conflict_naming_the_table() {
        let mut f = boot();
        let (result, attempts) = run_increment(&mut f, |b, t, i| commit(b, t, 2, i as i64));
        let err = result.expect_err("every attempt conflicts");
        assert!(
            matches!(
                &err,
                GnitzSqlError::Client(ClientError::Refused(WireFault { status: WireStatus::TxnConflict, .. }))
            ),
            "{err:?}"
        );
        assert!(err.to_string().contains("'occ.t'"), "{err}");
        assert_eq!(attempts, RMW_MAX_ATTEMPTS);
    }

    /// A write landing after the first read forces one retry, which re-reads:
    /// the increment applies to the row that write left, so no update is lost.
    #[test]
    fn a_conflict_retries_over_a_fresh_read() {
        let mut f = boot();
        let (result, attempts) = run_increment(&mut f, |b, t, i| {
            if i == 0 {
                commit(b, t, 1, 100);
            }
        });
        assert_eq!(result.unwrap(), 1);
        assert_eq!(attempts, 2);
        assert_eq!(row_1(&mut f.a, &f.target), [(101, 1)]);
    }

    /// A write committed before the statement's read is inside its watermark: the
    /// first attempt commits, in one read and one push.
    #[test]
    fn a_write_the_read_saw_is_no_conflict() {
        let mut f = boot();
        let target = Arc::clone(&f.target);
        commit(&mut f.b, &target, 1, 100);
        let before = f.a.requests_sent();
        let (result, attempts) = run_increment(&mut f, |_, _, _| {});
        assert_eq!(result.unwrap(), 1);
        assert_eq!(attempts, 1);
        assert_eq!(f.a.requests_sent() - before, 2, "SCAN_SPEC and PUSH_TXN");
        assert_eq!(row_1(&mut f.a, &target), [(101, 1)]);
    }
}
