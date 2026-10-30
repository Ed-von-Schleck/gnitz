//! Instruction benchmarks for decoding an expression blob.

use crate::test_support::{in_set, result_prog, schema_pk_ints, Meter};
use crate::LogicalProgram;

/// The decode a predicate-only read pays per request per worker: `from_blob`
/// and the `resolve_filter` that decodes an IN list's pool entry, per entry.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn from_blob_bench() {
    let meter = Meter::new();
    let schema = schema_pk_ints(1, false);
    let n = 262_144usize;
    let (instrs, consts) = in_set(0..n as i64);
    let blob = result_prog(instrs, consts).to_blob_bytes();
    meter.pass("from_blob_bench", "IN list", n, || {
        let prog = LogicalProgram::from_blob(std::hint::black_box(&blob)).expect("decodes");
        prog.resolve_filter(&schema).expect("resolves")
    });
}
