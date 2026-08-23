//! Fixtures shared by the mirror's integration tests.

pub mod perf;

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use gnitz_core::{ColData, GnitzClient, Schema, ZSetBatch};
use gnitz_sql::{SqlPlanner, SqlResult};

/// Every test in this binary takes this lock.
///
/// Not a workaround for the one-handle-per-process guard: `cargo test` runs a
/// target's tests as threads of one process, and what the guard protects —
/// `worker_ctx`'s committed generation, and the `Seam` / `io_uring` verdicts
/// read once per process — is shared between them either way. A parallel test
/// would be racing that state whether or not the guard caught it.
static SERIAL: Mutex<()> = Mutex::new(());

pub fn serial() -> MutexGuard<'static, ()> {
    // A test that panics while holding it must not fail every later test with a
    // poisoned lock; the state it protects is re-established by the next open.
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// Run `sql` for effect, panicking with the statement on failure.
pub fn sql(client: &mut GnitzClient, schema: &str, statements: &str) {
    SqlPlanner::new(client, schema)
        .execute(statements)
        .unwrap_or_else(|e| panic!("{statements}: {e}"));
}

/// Run one `SELECT` and return `(schema, rows)`.
pub fn query(reads: &mut dyn gnitz_core::ReadTarget, schema: &str, s: &str) -> (Schema, ZSetBatch) {
    match gnitz_sql::execute_query(reads, schema, s).unwrap_or_else(|e| panic!("{s}: {e}")) {
        SqlResult::Rows { schema, batch } => (schema, batch),
        other => panic!("{s} did not return rows: {other:?}"),
    }
}

/// One canonical row: its OPK image, then one cell per payload column.
pub type Row = (Vec<u8>, Vec<Option<Vec<u8>>>);

/// The Z-set a reply denotes: `(OPK image, cells) → summed weight`, net-zero
/// entries dropped.
///
/// **PK-canonical because `ZSetBatch::pks` holds native LE**, not the OPK image:
/// the client's block decoder walks the OPK region back and its encoder is the
/// single point that re-encodes it. A comparison that chunked `pks.buf` raw
/// would compare LE tuples against OPK images and pass only for an unsigned
/// single-byte PK — it would fail on every signed and multi-byte one.
///
/// **Weight-exact because that is what correctness means here.** A row-set
/// comparison would accept a replicated view read as if it were keyed, which
/// returns W copies and inflates every weight W-fold while replying OK.
pub fn canonical(schema: &Schema, batch: &ZSetBatch) -> BTreeMap<Row, i64> {
    let stride = schema.pk_stride();
    let codes: Vec<(usize, u8)> = schema.pk_col_codes().collect();
    let mut out: BTreeMap<Row, i64> = BTreeMap::new();
    for row in 0..batch.weights.len() {
        let mut opk = vec![0u8; stride];
        gnitz_wire::encode_pk_tuple(
            codes.iter().copied(),
            &batch.pks.buf[row * stride..(row + 1) * stride],
            &mut opk,
        );
        let cells = schema
            .payload_columns()
            .map(|(pi, ci, cd)| {
                if gnitz_core::null_word_get(batch.nulls[row], pi) {
                    return None;
                }
                Some(match &batch.columns[ci] {
                    ColData::Fixed(b) => {
                        let w = cd.type_code.wire_stride();
                        b[row * w..(row + 1) * w].to_vec()
                    }
                    ColData::Strings(v) => v[row].clone().unwrap_or_default().into_bytes(),
                    ColData::Bytes(v) => v[row].clone().unwrap_or_default(),
                })
            })
            .collect();
        *out.entry((opk, cells)).or_insert(0) += batch.weights[row];
    }
    out.retain(|_, w| *w != 0);
    out
}

/// Assert two replies denote the same Z-set, naming the first difference, and
/// refuse a vacuous pass: two empty replies agree about nothing.
///
/// Returns the rows compared, so a caller can hold a sum to a floor of its own —
/// worth doing, since a global aggregate replies with one row whatever it read.
pub fn assert_same_zset(what: &str, a: (&Schema, &ZSetBatch), b: (&Schema, &ZSetBatch)) -> usize {
    assert_eq!(
        a.0.columns.len(),
        b.0.columns.len(),
        "{what}: the two replies have different shapes"
    );
    let (ca, cb) = (canonical(a.0, a.1), canonical(b.0, b.1));
    assert!(
        !(ca.is_empty() && cb.is_empty()),
        "{what}: both replies are empty, so they agree about nothing"
    );
    for (row, wa) in &ca {
        match cb.get(row) {
            Some(wb) if wb == wa => {}
            Some(wb) => panic!("{what}: row {row:?} has weight {wa} locally and {wb} on the server"),
            None => panic!("{what}: row {row:?} at weight {wa} is missing from the server's reply"),
        }
    }
    for row in cb.keys() {
        assert!(
            ca.contains_key(row),
            "{what}: row {row:?} is missing from the local reply"
        );
    }
    ca.len()
}

/// An env var set for one test and removed on the way out: the tests are threads
/// of one process, and a panicking one must not leave it set for its siblings.
pub struct EnvVar(&'static str);

impl EnvVar {
    pub fn set(name: &'static str, value: &str) -> EnvVar {
        std::env::set_var(name, value);
        EnvVar(name)
    }
}

impl Drop for EnvVar {
    fn drop(&mut self) {
        std::env::remove_var(self.0);
    }
}
