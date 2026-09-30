//! Fixtures shared by the mirror's integration tests.

use std::collections::BTreeMap;
use std::sync::Arc;

use gnitz_core::{GnitzClient, Schema, ZSetBatch};
use gnitz_sql::SqlResult;

pub mod common;

/// Run `sql` for effect, panicking with the statement on failure.
pub fn sql(client: &mut GnitzClient, schema: &str, statements: &str) {
    gnitz_sql::execute(client, schema, statements).unwrap_or_else(|e| panic!("{statements}: {e}"));
}

/// Run one `SELECT` and return `(schema, rows)`. Local-first: a client holding a
/// valid copy of the relation answers off it.
pub fn query(client: &mut GnitzClient, schema: &str, s: &str) -> (Arc<Schema>, ZSetBatch) {
    let mut results = gnitz_sql::execute(client, schema, s).unwrap_or_else(|e| panic!("{s}: {e}"));
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
                Some(if col.tc().is_german_string() {
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

/// Where a read through a mirroring client must be answered.
#[derive(Clone, Copy, Debug)]
pub enum Answer {
    /// Off the copy, with no request sent.
    Local,
    /// Upstream: the client holds no valid copy of the relation.
    Upstream,
}

/// Assert `sql_text` reads the same through `mirror` as against `server`, that
/// `mirror` answered it where `answer` says, and return how many rows the
/// agreement covered — worth holding to a floor, since a global aggregate
/// replies with one row whatever it read.
///
/// The locality check is what keeps this from comparing the server with itself:
/// a copy whose gate is shut is delegated, and then agrees by construction.
///
/// A query carrying `ORDER BY` is compared as a **sequence**. Any other is a
/// multiset comparison: its row order is unspecified, and the mirror is one
/// partition where the server is W, so two correct replies differ in sequence.
pub fn differential(
    mirror: &mut GnitzClient,
    server: &mut GnitzClient,
    schema: &str,
    sql_text: &str,
    answer: Answer,
) -> usize {
    let before = mirror.requests_sent();
    let local = query(mirror, schema, sql_text);
    let sent = mirror.requests_sent() - before;
    match answer {
        Answer::Local => assert_eq!(sent, 0, "{sql_text}: must be answered off the copy"),
        Answer::Upstream => assert!(sent > 0, "{sql_text}: must be delegated, not answered off a copy"),
    }
    let remote = query(server, schema, sql_text);
    assert_eq!(local.0, remote.0, "{sql_text}: the two replies have different schemas");
    if sql_text.contains("ORDER BY") {
        assert_same_sequence(sql_text, &local.1, &remote.1)
    } else {
        assert_same_zset(sql_text, &local.1, &remote.1)
    }
}

/// Assert two replies denote the same Z-set, naming the first difference, and
/// refuse a vacuous pass: two empty replies agree about nothing.
fn assert_same_zset(what: &str, a: &ZSetBatch, b: &ZSetBatch) -> usize {
    let (ca, cb) = (canonical(a), canonical(b));
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
fn assert_same_sequence(what: &str, a: &ZSetBatch, b: &ZSetBatch) -> usize {
    let (ra, rb) = (canonical_rows(a), canonical_rows(b));
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
