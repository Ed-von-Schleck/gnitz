//! The store-level test helpers `gnitz-server`'s and `gnitz-mirror`'s tests
//! share with this crate's own.
//!
//! **This file is compiled three times, from one source** — as a `cfg(test)`
//! module here, as the whole of `gnitz-store-testkit` (which is how
//! `gnitz-mirror`'s tests reach it), and through a `#[path]` in `gnitz-server`'s
//! own `test_support`. A copy per crate would be three independent Z-set batch
//! builders free to drift on the one thing that fails with no error and no
//! assertion — a batch whose layout claim or weights do not match what the
//! encoder produced.
//!
//! Every path below is spelled `gnitz_store::`, which resolves through
//! `extern crate self as gnitz_store` in this crate and to the real dependency
//! elsewhere. So this compilation sees only `gnitz-store`'s public API, and a
//! helper that reached a crate-internal fails to build here. When that happens
//! the fix is to move the helper to [`super::internal`], not to publish the
//! internal — see this module's parent for what that costs.

use proptest::prelude::*;

use gnitz_store::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, BatchBuilder, Layout, RecoverySource, StoreBudgets, Table};
use gnitz_wire::type_code;

/// A schema with one PK column per type code in `tcs`, plus a single trailing
/// I64 payload column — the generic PK-shape builder for the merge/sort/OPK
/// tests, parameterized by the whole key rather than by one named shape.
/// [`pk_only_schema`] is the no-payload counterpart.
pub fn pk_payload_schema(tcs: &[u8]) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = tcs.iter().map(|&t| SchemaColumn::new(t, 0)).collect();
    cols.push(SchemaColumn::new(type_code::I64, 0));
    let pk: Vec<u32> = (0..tcs.len() as u32).collect();
    SchemaDescriptor::new(&cols, &pk)
}

/// The canonical narrow test schema: U64 pk + a single I64 payload column.
pub fn make_schema_u64_i64() -> SchemaDescriptor {
    pk_payload_schema(&[type_code::U64])
}

/// Build a batch over [`make_schema_u64_i64`]-shaped schemas from native
/// `(pk, weight, payload)` tuples and certify it `Consolidated`. **Rows must
/// arrive pre-sorted by (PK, payload) with no net-zero duplicates** — the
/// certification is a claim the caller makes, and `certify_layout` only
/// debug-verifies it; a lying claim would let a consumer skip-point silently
/// mis-fold weights.
pub fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
    let mut b = make_batch_raw(schema, rows);
    b.certify_layout(Layout::Consolidated, schema);
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
    SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0), payload], &[0])
}

/// Every wire `TypeCode`. The single source of truth for the schema-generating
/// proptests across the crate; since any column type is a valid payload, this
/// doubles as the arbitrary-payload-type strategy.
pub fn arb_type_code() -> impl Strategy<Value = u8> {
    prop_oneof![
        Just(type_code::U8),
        Just(type_code::I8),
        Just(type_code::U16),
        Just(type_code::I16),
        Just(type_code::U32),
        Just(type_code::I32),
        Just(type_code::U64),
        Just(type_code::I64),
        Just(type_code::U128),
        Just(type_code::I128),
        Just(type_code::DATE),
        Just(type_code::TIMESTAMP),
        Just(type_code::DECIMAL),
        Just(type_code::UUID),
        Just(type_code::F32),
        Just(type_code::F64),
        Just(type_code::STRING),
        Just(type_code::BLOB),
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
/// process into a `OnceLock`, and the test runner runs a crate's tests as
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
            Some(if gnitz_wire::is_german_string(col.type_code) {
                let st: [u8; 16] = raw.try_into().unwrap();
                gnitz_wire::try_decode_german_string(&st, &batch.blob).unwrap()
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
pub fn pk_only_schema(types: &[u8]) -> SchemaDescriptor {
    let cols: Vec<SchemaColumn> = types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
    let pk: Vec<u32> = (0..types.len() as u32).collect();
    SchemaDescriptor::new(&cols, &pk)
}

/// One row per supplied OPK key — weight 1, null word 0, one zeroed I64 payload
/// column, in the order given. What the PK-sort and routing tests need, which
/// read only `pk_stride` and `get_pk_bytes`. Each key must be `pk_stride` bytes.
pub fn batch_of_pk_bytes(schema: &SchemaDescriptor, pks: &[impl AsRef<[u8]>]) -> Batch {
    let mut b = Batch::with_capacity(schema, pks.len().max(1));
    for pk in pks {
        b.push_zero_filled_row(pk.as_ref(), 1, 0);
    }
    b
}

/// OPK-encode native PK column values (one per PK column, in PK-list order) into
/// the canonical order-preserving key — the exact bytes the ingest path
/// produces. Signed columns are passed as `v as u128` (the low `size()`
/// little-endian bytes are the two's-complement image the encoder sign-flips).
///
/// Read back out of a one-row batch rather than encoded here, so the oracle is
/// [`Batch::extend_pk_opk`] itself and not a second spelling of it.
pub fn opk_pk(schema: &SchemaDescriptor, vals: &[u128]) -> Vec<u8> {
    let mut b = Batch::with_capacity(schema, 1);
    b.extend_pk_opk(schema, vals);
    b.get_pk_bytes(0).to_vec()
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
    b.certify_layout(Layout::Consolidated, schema);
    b
}

/// A rederived table under `dir` at the default budgets — nothing a test puts
/// here spills, since that needs the whole 32 MiB RAM tier. For a test that just
/// needs somewhere to put rows.
pub fn scratch_table(dir: &str, schema: SchemaDescriptor, table_id: u32) -> Table {
    Table::new(
        dir,
        schema,
        table_id,
        RecoverySource::Rederive { resume_at: None },
        StoreBudgets::default(),
    )
    .unwrap()
}

/// U128 pk + a single I64 payload column — the 16-byte-PK sibling of
/// [`make_schema_u64_i64`].
pub fn make_schema_u128_i64() -> SchemaDescriptor {
    pk_payload_schema(&[type_code::U128])
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
