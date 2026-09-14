//! Fixtures shared by the mirror's integration tests.

pub mod perf;

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use gnitz_core::{GnitzClient, Schema, ZSetBatch};
use gnitz_sql::{SqlPlanner, SqlResult};

/// Every test in this binary takes this lock.
///
/// `cargo test` runs a target's tests as threads of one process, so the state a
/// mirror open touches process-wide is shared between them: the one-shot fault
/// seams, the `io_uring` verdict latched once per process, and the environment
/// variables an open re-reads. A parallel test would be racing all three.
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

/// Run one `SELECT` and return `(schema, rows)`. Local-first: a client holding a
/// valid copy of the relation answers off it.
pub fn query(client: &mut GnitzClient, schema: &str, s: &str) -> (std::sync::Arc<Schema>, ZSetBatch) {
    let mut results = SqlPlanner::new(client, schema)
        .execute(s)
        .unwrap_or_else(|e| panic!("{s}: {e}"));
    assert_eq!(results.len(), 1, "{s} is not one statement");
    match results.remove(0) {
        SqlResult::Rows { schema, batch } => (schema, batch),
        other => panic!("{s} did not return rows: {other:?}"),
    }
}

/// One canonical row: its OPK image, then one cell per payload column.
pub type Row = (Vec<u8>, Vec<Option<Vec<u8>>>);

/// The Z-set a reply denotes: `(OPK image, cells) → summed weight`, net-zero
/// entries dropped.
///
/// **PK-canonical because `ZSetBatch::pks` holds OPK**, the store's own key
/// bytes, so keys are compared directly: byte-equal is key-equal.
///
/// **Weight-exact because that is what correctness means here.** A row-set
/// comparison would accept a replicated view read as if it were keyed, which
/// returns W copies and inflates every weight W-fold while replying OK.
pub fn canonical(batch: &ZSetBatch) -> BTreeMap<Row, i64> {
    let mut out: BTreeMap<Row, i64> = BTreeMap::new();
    for (row, w) in canonical_rows(batch) {
        *out.entry(row).or_insert(0) += w;
    }
    out.retain(|_, w| *w != 0);
    out
}

/// The same canonical rows **in reply order**, weights beside them — what an
/// `ORDER BY` comparison needs, since the multiset above is order-blind and so
/// accepts the right rows in the wrong order.
pub fn canonical_rows(batch: &ZSetBatch) -> Vec<(Row, i64)> {
    let mut out: Vec<(Row, i64)> = Vec::with_capacity(batch.weights.len());
    for row in 0..batch.weights.len() {
        let opk = batch.pks.get_bytes(row).to_vec();
        let cells = batch
            .payload
            .iter()
            .enumerate()
            .map(|(pi, col)| {
                if gnitz_wire::null_word_get(batch.nulls[row], pi) {
                    return None;
                }
                let w = col.stride();
                let cell = &col.bytes[row * w..(row + 1) * w];
                Some(if gnitz_wire::is_german_string(col.tc() as u8) {
                    gnitz_wire::german_string_content(cell, &batch.blob).to_vec()
                } else {
                    cell.to_vec()
                })
            })
            .collect();
        out.push(((opk, cells), batch.weights[row]));
    }
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
    let (ca, cb) = (canonical(a.1), canonical(b.1));
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

/// Assert two replies are the same rows in the same order, and refuse the
/// vacuous pass.
///
/// **Only for a query carrying `ORDER BY`.** Row order is otherwise
/// unspecified, and the mirror is one partition where the server is W, so two
/// correct replies legitimately differ in sequence.
pub fn assert_same_sequence(what: &str, a: (&Schema, &ZSetBatch), b: (&Schema, &ZSetBatch)) -> usize {
    let (ra, rb) = (canonical_rows(a.1), canonical_rows(b.1));
    assert!(
        !ra.is_empty(),
        "{what}: the ordered reply is empty, so it agrees about nothing"
    );
    assert_eq!(
        ra.len(),
        rb.len(),
        "{what}: the ordered replies are {} rows locally and {} on the server",
        ra.len(),
        rb.len()
    );
    for (i, (x, y)) in ra.iter().zip(rb.iter()).enumerate() {
        assert_eq!(x, y, "{what}: the replies differ at position {i}");
    }
    ra.len()
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
