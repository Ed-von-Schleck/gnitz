//! The store-level test helpers other crates' tests share with this crate's own,
//! compiled here and as `gnitz-store-testkit` from this one source. Every path is
//! spelled `gnitz_store::`, so a helper sees only the public API; one that needs a
//! crate-internal belongs in [`super::internal`].

use proptest::prelude::*;

use gnitz_store::schema::{SchemaColumn, SchemaDescriptor, SchemaFacts};
use gnitz_store::storage::{Batch, BatchBuilder, Layout};
use gnitz_wire::TypeCode;

/// A schema with one PK column per type code in `tcs`, plus a single trailing
/// I64 payload column — the generic PK-shape builder for the merge/sort/OPK
/// tests, parameterized by the whole key rather than by one named shape.
/// [`pk_only_schema`] is the no-payload counterpart.
pub fn pk_payload_schema(tcs: &[TypeCode]) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = tcs.iter().map(|&t| SchemaColumn::new(t, false)).collect();
    cols.push(SchemaColumn::new(TypeCode::I64, false));
    let pk: Vec<u32> = (0..tcs.len() as u32).collect();
    SchemaDescriptor::new(&cols, &pk)
}

/// `cols` of `schema` as join-key slots, each at the column's own slot type.
pub fn self_typed_slots(schema: &SchemaDescriptor, cols: &[u32]) -> Vec<gnitz_wire::ReindexSlot> {
    cols.iter()
        .map(|&c| (c, schema.columns[c as usize].type_code.reindex_output_type()))
        .collect()
}

/// The canonical narrow test schema: U64 pk + a single I64 payload column.
pub fn make_schema_u64_i64() -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::U64])
}

/// Build a batch over [`make_schema_u64_i64`]-shaped schemas from native
/// `(pk, weight, payload)` tuples and certify it `Consolidated`. **Rows must
/// arrive pre-sorted by (PK, payload) with no net-zero duplicates** — the
/// certification is a claim the caller makes, and `certify_layout` only
/// debug-verifies it; a lying claim would let a consumer skip-point silently
/// mis-fold weights.
pub fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
    let mut b = make_batch_raw(schema, rows);
    b.certify_layout(Layout::Consolidated);
    b
}

/// [`make_batch`] without the `Consolidated` certification: the batch stays
/// honestly `Raw`, for tests whose rows are unsorted or that exercise the
/// sort/fold paths themselves.
pub fn make_batch_raw(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, w, val) in rows {
        b.begin_row(pk as u128, w);
        b.put_int(val as u128);
        b.end_row();
    }
    b.finish()
}

/// A U64 PK plus one `payload` column — the reindex/promotion tests' counterpart
/// to [`pk_payload_schema`], parameterized by the payload column rather than only its
/// type, so a nullable payload needs no second builder.
pub fn u64_pk_schema(payload: SchemaColumn) -> SchemaDescriptor {
    SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false), payload], &[0])
}

/// Every wire `TypeCode`. The single source of truth for the schema-generating
/// proptests across the crate; since any column type is a valid payload, this
/// doubles as the arbitrary-payload-type strategy.
pub fn arb_type_code() -> impl Strategy<Value = TypeCode> {
    prop_oneof![
        Just(TypeCode::U8),
        Just(TypeCode::I8),
        Just(TypeCode::U16),
        Just(TypeCode::I16),
        Just(TypeCode::U32),
        Just(TypeCode::I32),
        Just(TypeCode::U64),
        Just(TypeCode::I64),
        Just(TypeCode::U128),
        Just(TypeCode::I128),
        Just(TypeCode::Date),
        Just(TypeCode::Timestamp),
        Just(TypeCode::Decimal),
        Just(TypeCode::UUID),
        Just(TypeCode::F32),
        Just(TypeCode::F64),
        Just(TypeCode::String),
        Just(TypeCode::Blob),
    ]
}

/// Set by [`run_test_in_child`] on the child it spawns; the body test guards on
/// [`in_child_test`] so a normal sweep skips it.
const CHILD_TEST_VAR: &str = "GNITZ_RUN_CHILD_TEST";

/// True only inside a re-exec'd child spawned by [`run_test_in_child`].
pub fn in_child_test() -> bool {
    std::env::var(CHILD_TEST_VAR).is_ok()
}

/// What a child prints once it has run every assertion — see [`assert_child_ok`].
/// A child that must fail-stop instead never reaches it, and its caller asserts
/// on the exit code.
pub const CHILD_OK: &str = "the child ran every assertion";

/// Re-run the current test binary filtered to `internal_test`, with `envs` set,
/// and hand back the child's exit status and captured output.
///
/// The reason a test needs a child at all rather than setting the variable
/// itself: a `Seam` — and the `io_uring` verdict — reads its variable once per
/// process, and the test runner runs a crate's tests as
/// threads of one process, so an in-process `set_var` would race every other
/// test and lose to whichever read first. A direct in-process fail-stop would
/// also terminate the runner. `internal_test` must return early unless
/// [`in_child_test`], so a normal test sweep skips it.
///
/// `--exact`, so the filter cannot also pick up a sibling whose name contains
/// this one. Pass the caller's `module_path!()` as `module`: libtest's filter
/// path is the module path minus the crate segment, so deriving it here keeps
/// the filter right when a test module moves.
pub fn run_test_in_child(module: &str, internal_test: &str, envs: &[(&str, &str)]) -> std::process::Output {
    let filter = match module.split_once("::") {
        Some((_krate, path)) => format!("{path}::{internal_test}"),
        None => internal_test.to_string(),
    };
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.arg("--exact").arg(&filter).arg("--nocapture");
    cmd.env(CHILD_TEST_VAR, "1");
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.output().unwrap()
}

/// Assert the child exited cleanly **and** reached its final [`CHILD_OK`] print.
/// The exit code alone proves nothing: `libtest` exits 0 when its filter matches
/// nothing, so renaming a child would otherwise leave its caller green covering
/// nothing.
pub fn assert_child_ok(out: &std::process::Output, what: &str) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.code() == Some(0) && stdout.contains(CHILD_OK),
        "{what}\n-- child stdout --\n{stdout}-- child stderr --\n{stderr}",
    );
}

/// Flip each bit of `buf[span]` in turn, run `check(byte, bit, buf)` with it
/// damaged, and restore it — so no case can leave the fixture altered for the
/// ones after it.
pub fn sweep_bit_flips(buf: &mut [u8], span: std::ops::Range<usize>, mut check: impl FnMut(usize, usize, &[u8])) {
    for byte in span {
        for bit in 0..8 {
            buf[byte] ^= 1 << bit;
            check(byte, bit, buf);
            buf[byte] ^= 1 << bit;
        }
    }
}

/// A scratch directory for a test that needs a real on-disk tree, wiped at the
/// start of the run so the previous same-user run self-cleans. `scope` names the
/// subsystem, `name` the test.
///
/// The path is namespaced by `$USER` (pid if unset): `/tmp` is shared and
/// sticky, so a directory left by a *different* user occupies the bare path
/// forever — the start-of-test `remove_dir_all` cannot delete it (sticky bit)
/// and `CatalogEngine::open` then fails EACCES creating subdirs under it.
pub fn scratch_dir(scope: &str, name: &str) -> String {
    let owner = std::env::var("USER").unwrap_or_else(|_| std::process::id().to_string());
    let path = std::env::temp_dir()
        .join(format!("gnitz_{scope}_test_{owner}_{name}"))
        .to_str()
        .unwrap()
        .to_owned();
    let _ = std::fs::remove_dir_all(&path);
    path
}

/// A row's logical identity: its PK bytes plus one entry per payload column.
pub type RowKey = (Vec<u8>, Vec<Option<Vec<u8>>>);

/// Reduce a row to its logical identity. NULL is `None`, distinct from an empty
/// string `Some(vec![])` — keeping the raw null word out of the key, so its
/// non-load-bearing unused bits cannot cause a spurious mismatch while every
/// meaningful null bit is still reflected. STRING / BLOB cells are **decoded**:
/// the 16-byte German-string struct embeds a blob offset, so two batches holding
/// the same logical string carry different struct bytes.
pub fn row_key(batch: &Batch, schema: &SchemaDescriptor, row: usize) -> RowKey {
    let nw = batch.get_null_word(row);
    let vals = schema
        .payload_columns()
        .map(|(pi, col)| {
            if gnitz_wire::null_word_get(nw, pi) {
                return None;
            }
            let cs = col.size() as usize;
            let raw = batch.get_col_ptr(row, pi, cs);
            Some(if col.type_code.is_german_string() {
                let st: [u8; 16] = raw.try_into().unwrap();
                gnitz_wire::try_decode_german_string(&st, batch.blob()).unwrap()
            } else {
                raw.to_vec()
            })
        })
        .collect();
    (batch.get_pk_bytes(row).to_vec(), vals)
}

// ── Helpers `gnitz-server`'s own tests share ──────────────────────────────
//
// Here rather than in `internal` because their callers straddle the crate seam:
// the VM and compiler tests over in `gnitz-server` drive the same fixtures the
// storage tests here do.

/// An all-PK schema: one column per type code in `types`, every column a PK
/// column (`pk_indices = 0..n`) and no payload — the generic PK-shape builder
/// for the OPK encode/compare/route tests.
pub fn pk_only_schema(types: &[TypeCode]) -> SchemaDescriptor {
    let cols: Vec<SchemaColumn> = types.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
    let pk: Vec<u32> = (0..types.len() as u32).collect();
    SchemaDescriptor::new(&cols, &pk)
}

/// OPK-encode native PK column values (one per PK column, in PK-list order) into
/// the canonical order-preserving key — the exact bytes the ingest path
/// produces. Signed columns are passed as `v as u128` (the low `size()`
/// little-endian bytes are the two's-complement image the encoder sign-flips).
///
/// The oracle is `opk_key_cols` itself — the encoder the ingest path writes
/// through — not a second spelling of it.
pub fn opk_pk(schema: &SchemaDescriptor, vals: &[u128]) -> Vec<u8> {
    schema.opk_key_cols(vals).pk_bytes().to_vec()
}

/// [`make_batch_raw`] over [`make_schema_u128_i64`]-shaped schemas — native
/// u128 PKs, rows left `Raw` in the order given.
pub fn make_batch_u128_raw(schema: &SchemaDescriptor, rows: &[(u128, i64, i64)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, w, val) in rows {
        b.begin_row(pk, w);
        b.put_int(val as u128);
        b.end_row();
    }
    b.finish()
}

/// [`make_batch_u128_raw`] plus the `Consolidated` certification. **Rows must
/// arrive pre-sorted by (PK, payload) with no net-zero duplicates** — the
/// certification is a claim the caller makes, and `certify_layout` only
/// debug-verifies it.
pub fn make_batch_u128(schema: &SchemaDescriptor, rows: &[(u128, i64, i64)]) -> Batch {
    let mut b = make_batch_u128_raw(schema, rows);
    b.certify_layout(Layout::Consolidated);
    b
}

/// U128 pk + a single I64 payload column — the 16-byte-PK sibling of
/// [`make_schema_u64_i64`].
pub fn make_schema_u128_i64() -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::U128])
}

/// Payload column `col`'s string on row `row`.
pub fn read_german_string(batch: &Batch, col: usize, row: usize) -> Vec<u8> {
    let off = row * 16;
    let gs: &[u8; 16] = batch.col_data(col)[off..off + 16].try_into().unwrap();
    gnitz_wire::try_decode_german_string(gs, batch.blob()).unwrap()
}

/// U64 pk + a single STRING payload column.
pub fn make_schema_pk_u64_payload_string() -> SchemaDescriptor {
    u64_pk_schema(SchemaColumn::new(TypeCode::String, false))
}

/// [`make_batch_bytes`] under [`make_schema_pk_u64_payload_string`].
pub fn make_string_batch(rows: &[(u64, i64, &[u8])]) -> Batch {
    make_batch_bytes(&make_schema_pk_u64_payload_string(), rows)
}

/// Payload column 0's string on every row, in order.
pub fn read_strings(batch: &Batch) -> Vec<Vec<u8>> {
    (0..batch.len()).map(|row| read_german_string(batch, 0, row)).collect()
}

/// U64 pk + a single BLOB payload column.
pub fn make_schema_pk_u64_payload_blob() -> SchemaDescriptor {
    u64_pk_schema(SchemaColumn::new(TypeCode::Blob, false))
}

/// [`make_batch_bytes_raw`] certified `Consolidated`: the rows must already be in
/// (PK, payload) order.
pub fn make_batch_bytes(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8])]) -> Batch {
    let mut b = make_batch_bytes_raw(schema, rows);
    b.certify_layout(Layout::Consolidated);
    b
}

/// A `(U64 pk, STRING|BLOB payload)` batch of `(pk, weight, bytes)` rows, in the
/// order given.
pub fn make_batch_bytes_raw(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8])]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, w, val) in rows {
        b.begin_row(pk as u128, w);
        b.put_blob(val);
        b.end_row();
    }
    b.finish()
}

/// The batch as the Z-Set it denotes: `Σ weight` per logical row identity, with
/// the net-zero entries dropped. Emission order and the physical encoding of a
/// string both drop out, which is what lets two independently produced batches
/// be compared.
pub fn zset_of(batch: &Batch, schema: &SchemaDescriptor) -> std::collections::HashMap<RowKey, i64> {
    let mut z: std::collections::HashMap<RowKey, i64> = std::collections::HashMap::new();
    for row in 0..batch.len() {
        *z.entry(row_key(batch, schema, row)).or_insert(0) += batch.get_weight(row);
    }
    z.retain(|_, w| *w != 0);
    z
}

/// Every row of `batch` as its logical identity and weight, in batch order —
/// [`zset_of`] for a comparison where order and unfolded duplicates count.
pub fn weighted_rows(batch: &Batch) -> Vec<(RowKey, i64)> {
    (0..batch.len())
        .map(|row| (row_key(batch, batch.schema(), row), batch.get_weight(row)))
        .collect()
}

/// Whether the kind admits the pair of PK regions `(dpk, tpk)`. `rel` relates the
/// **left** slot to the right one, so the delta's side decides the operand order.
fn join_pair_matches(kind: gnitz_wire::JoinKind, delta_is_right: bool, eq_size: usize, dpk: &[u8], tpk: &[u8]) -> bool {
    match kind {
        gnitz_wire::JoinKind::Cross => true,
        gnitz_wire::JoinKind::Equi => dpk == tpk,
        gnitz_wire::JoinKind::Range { rel, .. } => {
            if dpk[..eq_size] != tpk[..eq_size] {
                return false;
            }
            // Equal-width OPK slot slices, so a raw byte compare IS the typed
            // comparison the relation names.
            let (d, s) = (&dpk[eq_size..], &tpk[eq_size..]);
            let (l, r) = match delta_is_right {
                true => (s, d),
                false => (d, s),
            };
            match rel {
                gnitz_wire::RangeRel::Lt => l < r,
                gnitz_wire::RangeRel::Le => l <= r,
                gnitz_wire::RangeRel::Gt => l > r,
                gnitz_wire::RangeRel::Ge => l >= r,
            }
        }
    }
}

/// Brute-force reference: every `(delta row, trace row)` pair the kind admits,
/// composed at weight `w_d · w_t` into the key, the left side's payload cells and
/// then the right's — spelled out independently of the join kernel.
/// Returns the Z-Set it denotes and the number of pairs with a non-zero product.
pub fn join_reference(
    kind: gnitz_wire::JoinKind,
    delta_is_right: bool,
    delta_schema: &SchemaDescriptor,
    trace_schema: &SchemaDescriptor,
    delta: &Batch,
    trace: &Batch,
) -> (std::collections::HashMap<RowKey, i64>, usize) {
    let eq_size = match kind {
        gnitz_wire::JoinKind::Range { n_eq, .. } => 8 * n_eq as usize,
        _ => 0,
    };
    let mut m: std::collections::HashMap<RowKey, i64> = std::collections::HashMap::new();
    let mut rows = 0usize;
    for i in 0..delta.len() {
        let dpk = delta.get_pk_bytes(i);
        for j in 0..trace.len() {
            let tpk = trace.get_pk_bytes(j);
            if !join_pair_matches(kind, delta_is_right, eq_size, dpk, tpk) {
                continue;
            }
            let w = delta.get_weight(i).wrapping_mul(trace.get_weight(j));
            if w == 0 {
                continue;
            }
            rows += 1;
            let key: Vec<u8> = match (kind, delta_is_right) {
                (gnitz_wire::JoinKind::Cross, true) => [tpk, dpk].concat(),
                (gnitz_wire::JoinKind::Cross, false) => [dpk, tpk].concat(),
                _ => dpk.to_vec(),
            };
            let d_cells = row_key(delta, delta_schema, i).1;
            let t_cells = row_key(trace, trace_schema, j).1;
            let (mut cells, tail) = match delta_is_right {
                true => (t_cells, d_cells),
                false => (d_cells, t_cells),
            };
            cells.extend(tail);
            *m.entry((key, cells)).or_insert(0) += w;
        }
    }
    m.retain(|_, w| *w != 0);
    (m, rows)
}

/// `batch` framed as one WAL block in a buffer of its own.
pub fn encode_to_wire_vec(batch: &Batch) -> Vec<u8> {
    let mut out = vec![0u8; batch.wire_byte_size()];
    assert_eq!(
        batch.encode_to_wire(&mut out),
        out.len(),
        "wire_byte_size must size its own encode"
    );
    out
}
