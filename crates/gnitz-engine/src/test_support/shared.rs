//! The test helpers `gnitz-server`'s tests share with this crate's own.
//!
//! **This file is compiled twice, from one source.** Here it is a `cfg(test)`
//! module of `gnitz-engine`; `gnitz-engine-testkit` compiles the same path as
//! its whole library, which is how `gnitz-server`'s tests reach it. A copy in
//! each crate would be two independent Z-set batch builders free to drift on the
//! one thing that fails with no error and no assertion — a batch whose layout
//! claim or weights do not match what the encoder produced.
//!
//! Every path below is spelled `gnitz_engine::`, which resolves through
//! `extern crate self as gnitz_engine` in this crate and to the real dependency
//! in the testkit. So this compilation sees only `gnitz-engine`'s public API,
//! and a helper that reached a crate-internal fails to build here. When that
//! happens the fix is to move the helper to [`super::internal`], not to publish
//! the internal — see this module's parent for what that costs.

use proptest::prelude::*;

use gnitz_engine::catalog::{CatalogEngine, ColumnDef, SysFamily};
use gnitz_engine::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_engine::storage::{Batch, BatchBuilder, Layout};
use gnitz_wire::sys_rows::{write_col_tab_row, write_table_tab_row, ColTabRow, TableTabRow};
use gnitz_wire::type_code;

/// A single-column PK of type `tc` plus one I64 payload column — the shape
/// almost every storage unit test wants, parameterized by PK type.
pub fn pk_i64_schema(tc: u8) -> SchemaDescriptor {
    SchemaDescriptor::new(&[SchemaColumn::new(tc, 0), SchemaColumn::new(type_code::I64, 0)], &[0])
}

/// The canonical narrow test schema: U64 pk + a single I64 payload column.
pub fn make_schema_u64_i64() -> SchemaDescriptor {
    pk_i64_schema(type_code::U64)
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
    let mut b = Batch::with_capacity(*schema, rows.len().max(1));
    let width = payload_slot_width(schema);
    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes()[..width]);
        b.count += 1;
    }
    b
}

/// How many of an `i64` payload's little-endian bytes belong in payload slot 0.
/// `extend_col` writes `d.len()` bytes at `row * col_size`, so an over-wide
/// write into a sub-64-bit column spills into the next row — this is what lets
/// the one builder serve every fixed-int payload width.
pub fn payload_slot_width(schema: &SchemaDescriptor) -> usize {
    schema
        .payload_columns()
        .next()
        .map_or(8, |(_, c)| (c.size() as usize).min(8))
}

/// A U64 PK plus one payload column of type `tc` — the reindex/promotion tests'
/// counterpart to [`pk_i64_schema`], parameterized by payload type instead.
pub fn u64_pk_schema(tc: u8) -> SchemaDescriptor {
    SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)], &[0])
}

/// U64 pk + a single STRING payload column.
pub fn make_schema_pk_u64_payload_string() -> SchemaDescriptor {
    u64_pk_schema(type_code::STRING)
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
/// this one.
pub fn run_test_in_child(internal_test: &str, envs: &[(&str, &str)]) -> std::process::Output {
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.arg("--exact").arg(internal_test).arg("--nocapture");
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

// ---------------------------------------------------------------------------
// SharedRegion — anonymous shared-memory test region
// ---------------------------------------------------------------------------

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

/// Anonymous `MAP_SHARED` region for IPC-shaped tests; unmapped on drop.
/// Pages are kernel-zeroed and lazily populated (no explicit memset, so a
/// huge reservation stays cheap). `MAP_SHARED` so a `fork()`ed child sees
/// the same pages (a child that `_exit`s never runs drops, so only the
/// parent unmaps).
pub struct SharedRegion {
    ptr: *mut u8,
    size: usize,
}

impl SharedRegion {
    pub fn new(size: usize) -> Self {
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1,
                0,
            )
        };
        assert_ne!(ptr, libc::MAP_FAILED, "SharedRegion mmap failed");
        SharedRegion {
            ptr: ptr as *mut u8,
            size,
        }
    }

    pub fn ptr(&self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for SharedRegion {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.size);
        }
    }
}

// ── Catalog ColumnDef fixtures ────────────────────────────────────────────
//
// `ColumnDef: Default` is the plain column, so each builder names only what it
// varies and a new field costs no construction site anything.

/// A plain non-nullable, non-FK, non-hidden column of the given type.
pub fn col_def(name: &str, type_code: u8) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code,
        ..Default::default()
    }
}

/// Column defs carrying just the names, for a test that needs a *named* schema
/// block. Type and nullability come off the descriptor, so only `name` matters.
pub fn named_col_defs<S: AsRef<str>>(names: &[S]) -> Vec<ColumnDef> {
    names.iter().map(|n| col_def(n.as_ref(), 0)).collect()
}

// ---------------------------------------------------------------------------
// CatalogTestExt — register a relation the way a worker does
// ---------------------------------------------------------------------------

/// In-process table registration for tests that need a real relation store
/// without a server.
///
/// It writes the same COL_TAB and TABLE_TAB rows a DDL bundle carries and hands
/// each to [`CatalogEngine::ddl_sync`], which is the ingest tail the wire path
/// reaches too — so the register hooks fire and the relation store is built.
/// The columns go first: the TABLE_TAB register hook reads them back out of
/// `sys_columns` to derive the schema.
pub trait CatalogTestExt {
    /// Register a base table at a caller-chosen `tid`. `pk` names the PK column
    /// indices into `cols`, in key order.
    ///
    /// A caller-chosen id lifts the engine's allocator past itself (the register
    /// hook raises the id counter to `tid + 1`), so a later engine-side
    /// allocation cannot collide with one chosen here.
    fn register_table(
        &mut self,
        tid: i64,
        schema_id: i64,
        name: &str,
        cols: &[ColumnDef],
        pk: &[u32],
    ) -> Result<(), String>;
}

impl CatalogTestExt for CatalogEngine {
    fn register_table(
        &mut self,
        tid: i64,
        schema_id: i64,
        name: &str,
        cols: &[ColumnDef],
        pk: &[u32],
    ) -> Result<(), String> {
        let mut bb = BatchBuilder::new(SysFamily::Column.schema());
        for (i, cd) in cols.iter().enumerate() {
            write_col_tab_row(
                &mut bb,
                &ColTabRow {
                    owner_id: tid as u64,
                    owner_kind: gnitz_wire::OWNER_KIND_TABLE,
                    col_idx: i as u64,
                    name: &cd.name,
                    type_code: cd.type_code as u64,
                    is_nullable: cd.is_nullable,
                    fk_table_id: cd.fk_table_id as u64,
                    fk_col_idx: cd.fk_col_idx as u64,
                    is_serial: cd.is_serial,
                    is_hidden: cd.is_hidden,
                },
                1,
            )?;
        }
        self.ddl_sync(SysFamily::Column.id(), bb.finish())?;

        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        write_table_tab_row(
            &mut bb,
            &TableTabRow {
                table_id: tid as u64,
                schema_id: schema_id as u64,
                name,
                pk_col_idx: gnitz_wire::pack_pk_cols(pk),
                flags: gnitz_wire::TableProps::default().pack(),
            },
            1,
        );
        self.ddl_sync(SysFamily::Table.id(), bb.finish())
    }
}
