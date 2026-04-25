# Architectural Brittleness: Type System Fragmentation and the Distributed Column Limit

*Written 2026-04-25. Triggered by the UUID first-class type implementation, which took
multiple context windows and five E2E runs to land cleanly. This document records what
the implementation exposed, diagnoses the underlying structural problems, and proposes
concrete fixes. It is intended as a planning document for a focused refactoring sprint.*

---

## Background

The UUID feature (`TypeCode::UUID = 13`) was straightforward in concept: add a new type
code, plumb it through the layers, format it differently on output. The implementation
touched 19 files and added 576 lines net. It took five full E2E runs to get to green.
Along the way it exposed four distinct architectural problems. Two of them (Issues 1 and
2) are mechanical and have clear fixes. One (Issue 3) required correcting an initial
misdiagnosis. One (Issue 4) produced a panic with a root cause that remains unresolved.

The problems are real regardless of the implementation that revealed them.

---

## Issue 1: TypeCode Has No Single Owner

### What the type code system looks like today

TypeCode flows through the stack in four independent representations:

```
gnitz-wire      pub mod type_code { pub const U64: u8 = 8; ... pub const UUID: u8 = 13; }
                wire_stride(): 11 | 12 | 13 => 16, ...

gnitz-core      #[repr(u8)] pub enum TypeCode { U64 = tc::U64, ..., UUID = tc::UUID }
                TypeCode::try_from_u64(): 13 → Ok(TypeCode::UUID)

gnitz-sql       uses gnitz_core::TypeCode (no duplication — clean)

gnitz-engine    imports gnitz_wire::type_code directly (raw u8)
                uses type_code::UUID, type_code::U128, etc. throughout
                does NOT depend on gnitz-core at all

gnitz-py        #[classattr] pub const U64:  u32 = 8;
                #[classattr] pub const UUID: u32 = 13;
                (hardcoded mirror of wire constants, as u32 not u8)
```

The crate dependency graph confirms this:

```
gnitz-wire  ←  gnitz-core  ←  gnitz-sql  ←  gnitz-py
gnitz-wire  ←  gnitz-engine   (engine does NOT depend on core)
```

gnitz-engine is intentionally decoupled from gnitz-core. Both depend on gnitz-wire for
the numeric constants. The engine operates on raw `u8` type codes throughout.

### Why this causes problems when adding a new type

Adding `TypeCode::UUID = 13` required manual changes in four separate places with no
compile-time enforcement that all four were updated consistently:

1. **gnitz-wire**: add `pub const UUID: u8 = 13` and extend `wire_stride()`.
2. **gnitz-core**: add `UUID = tc::UUID` variant, extend `try_from_u64()`, extend
   `ZSetBatch::new()`, `BatchAppender::zero_val()`, `BatchAppender::u128_val()`, and
   `ZSetBatch::validate()`.
3. **gnitz-engine**: add `type_code::UUID` arms to every exhaustive match in
   `compiler.rs`, `catalog/utils.rs`, `catalog/hooks.rs`, `catalog/ddl.rs`, and
   `schema.rs`. The compiler alone required four separate sites:
   `agg_value_idx_eligible`, `make_agg_output_schema_single_gc`, the GROUP BY
   hash-key exclusion, and the join-schema column encoding.
4. **gnitz-py**: add `#[classattr] pub const UUID: u32 = 13` to `PyTypeCode`, add
   the `format_uuid` helper, and extend three separate column serialization arms.

Missing any of these produces a compile-time error only for the gnitz-core enum (because
Rust's exhaustive match forces all enum variants to be handled). For the engine, which
uses raw `u8` values, a missing case in a match arm compiles cleanly and silently
ignores the new type at runtime. A UUID column routed through an unpatched engine match
arm would be silently mishandled — wrong output, wrong aggregation, wrong index
eligibility — with no diagnostic output.

### The gnitz-py duplication problem

The Python layer re-declares all thirteen type code constants as `u32` classattrs. This
is a third independent representation (wire = `u8`, core enum = `repr(u8)`, py = `u32`).
Adding a type code means adding it to `PyTypeCode` manually. There is no mechanism that
makes a missing Python constant a build or test error. The `test_all_values_present` test
in `test_ergonomics.py` partially covers this — it asserts `{tc.value for tc in
TypeCode} == set(range(1, 14))` — but this test requires a manual upper-bound update
every time a new type is added, which is exactly the kind of change that gets forgotten.

The test should be:

```python
# Instead of:
assert {tc.value for tc in TypeCode} == set(range(1, 14))

# Use:
expected = set(range(1, max(tc.value for tc in TypeCode) + 1))
assert {tc.value for tc in TypeCode} == expected
# ...or enumerate by name to make missing constants visible:
assert {tc.name for tc in TypeCode} == {
    "U8", "I8", "U16", "I16", "U32", "I32", "F32",
    "U64", "I64", "F64", "String", "U128", "UUID",
}
```

### The fix

**Make gnitz-engine depend on gnitz-core.** The only reason it doesn't today is likely
historical — the engine predates gnitz-core's TypeCode enum, or the decoupling was
intentional to keep the engine binary small. Verify whether there is a technical
constraint preventing the dependency before proceeding.

If the dependency is added, replace all raw `type_code::*` match arms in the engine with
`TypeCode::*` enum arms. The compiler then enforces exhaustiveness at every match site.
Adding a new type variant becomes a *compile error cascade* that points exactly to every
site that needs updating. The current model inverts this: the type system tells you
nothing, and the developer must maintain a mental checklist.

If the dependency cannot be added (e.g., due to a genuine circular dependency introduced
by some future change), consider generating the engine match arms via a macro that is
driven from the wire constants, so at minimum the wire constants and engine arms stay in
sync automatically.

For gnitz-py, replace the hand-written classattrs with a build-time or test-time
assertion that the Python constants match the wire constants exactly. The current
`try_from_u64` call in `py_col_to_rust` already serves as an implicit runtime check, but
a compile-time binding would be stronger.

---

## Issue 2: `MAX_COLUMNS = 64` Is a Distributed Bare Literal

### The actual extent of the problem

The number `64` as a column-count limit appears in approximately **30 places** across the
codebase, with no named constant. The full inventory, by category:

**The struct definition that sets the law:**
- `schema.rs:37`: `pub columns: [SchemaColumn; 64]` — this is the actual constraint.
  Every other occurrence must agree with this one, but nothing enforces that agreement.

**Temporary schema construction in operators and storage:**
- `storage/table.rs:576`
- `storage/partitioned_table.rs:303`
- `storage/columnar.rs:132, 225, 241, 348`
- `storage/memtable.rs:324, 649, 718, 1168`
- `storage/shard_index.rs:682`
- `ops/reduce.rs:1307, 1322, 1362, 1408, 1494, 1617, 1808, 1855, 1899, 1943`
- `ops/index.rs:119, 135`
- `ops/linear.rs:339`
- `catalog/sys_tables.rs:108`
- `compiler.rs:694`
- `runtime/master.rs:1609, 1616`

**Validation guards (the enforcement perimeter):**
- `catalog/ddl.rs:137`: `if col_defs.len() > 64` (CREATE TABLE path)
- `catalog/ddl.rs:220`: `if graph.output_col_defs.len() > 64` (CREATE VIEW path)
- `catalog/hooks.rs:137`: `if col_defs.len() > 64` (hook registration, added during
  UUID implementation as a panic-to-error conversion after a server crash)
- `catalog/store.rs:579`: `assert!(col_defs.len() <= 64, ...)` (in
  `build_schema_from_col_defs`, belt-and-suspenders backstop)
- `compiler.rs`: join schema merge check with a panic message citing "64-column limit"

**The Python layer — a different number:**
- `gnitz-py/src/lib.rs`: validates `len > 65` (not 64), with comment "1 PK + up to 64
  payload columns". The Python schema object counts the PK slot; the Rust
  `SchemaDescriptor` counts total columns including the PK. This means the Python client
  accepts exactly one more column than the Rust struct can hold, if the off-by-one in
  the Python validation comment is wrong or misleading.

The discrepancy between Python (`> 65`) and Rust (`> 64`) requires understanding the
counting semantics to verify. It is documented in the Python wrapper but not in
`schema.rs` or in the error message from `ddl.rs`, which just says "Maximum 64 columns
supported" with no explanation of whether the PK is counted.

### Why this matters

Changing the column limit is a one-line change in `schema.rs` — resize the array — but
requires finding and updating roughly 30 other sites manually. There is no compile-time
signal that a site was missed. Tests would catch most missed validation guards (the DDL
rejection tests exercise `ddl.rs`), but missed temp-array initializations in operators
would be silently wrong: a schema built with a 128-element array passed to a function
expecting a 64-element array would compile and misbehave silently.

More immediately, the duplication makes the code harder to read. A reader encountering
`[SchemaColumn::new(0, 0); 64]` in `ops/reduce.rs` has no immediate way to know that
`64` here is "the column limit" rather than some unrelated constant. It requires
global knowledge.

### The fix

Define a single named constant in `gnitz-engine/src/schema.rs` (the file that owns
`SchemaDescriptor`):

```rust
/// Maximum number of columns in a single table or view schema.
/// Drives the fixed-size columns array in SchemaDescriptor and
/// all temporary schema-construction buffers throughout the engine.
/// Changing this requires updating: the array size, all temporary
/// [SchemaColumn; N] buffers, and the Python wrapper's validation.
pub const MAX_COLUMNS: usize = 64;

pub struct SchemaDescriptor {
    pub num_columns: u32,
    pub pk_index:    u32,
    pub columns:     [SchemaColumn; MAX_COLUMNS],
}
```

Export it from `schema.rs` so all engine modules can use it. Replace every `64` in a
column-array context with `MAX_COLUMNS`. The Python `65` becomes `MAX_COLUMNS + 1` with
an explicit comment:

```rust
// Python schema counts PK + payload; Rust MAX_COLUMNS is total including PK.
if len == 0 || len > MAX_COLUMNS + 1 {
```

This doesn't fix the distributed literal problem retroactively, but it makes the
invariant auditable in one grep and makes future changes safe.

---

## Issue 3: Two-Phase DDL — What Is and Is Not Atomic

### Initial misdiagnosis in the post-mortem

The first post-mortem draft claimed that a crash between the COL_TAB write and the
TABLE_TAB write could cause table ID reuse that would then corrupt the column count for a
subsequent table. This was wrong. This section records the correct analysis.

### The actual CREATE TABLE RPC sequence

`client.rs::create_table` issues three sequential RPCs, each a full roundtrip:

```
RPC 1: FLAG_ALLOCATE_TABLE_ID  →  server allocates new_tid, persists sequence advance, returns new_tid
RPC 2: push(COL_TAB, batch)    →  server writes N column records to sys_columns shard + SAL (LSN = K)
RPC 3: push(TABLE_TAB, batch)  →  server writes table row to sys_tables shard + SAL (LSN = K+1)
                                    → fires hook_table_register
                                    → reads column defs from sys_columns
                                    → builds SchemaDescriptor
                                    → registers table in DAG
```

These are three separate LSNs. The two column-related writes (COL_TAB and TABLE_TAB) do
NOT share an LSN and are not atomic in the database-transaction sense.

### Why a crash between RPC 2 and RPC 3 is less dangerous than it looks

A crash after RPC 2 but before RPC 3 leaves:
- The sequence counter for table IDs has been advanced past `new_tid` (this happened in
  RPC 1 and was persisted in `sys_sequences`).
- `sys_columns` contains N column records for `new_tid` (persisted in the shard file and
  in the SAL epoch).
- `sys_tables` has no row for `new_tid`.
- The DAG has no entry for `new_tid`.

On the next server startup, `CatalogEngine::open()` reads `sys_columns` from its shard
file and calls `replay_catalog()`, which fires hooks from the live system table rows.
`replay_catalog` processes `TABLE_TAB` rows first and then `COL_TAB` rows (for FK
wiring). Since there is no `TABLE_TAB` row for `new_tid`, no hook fires for it, and the
orphaned `COL_TAB` records are loaded into memory but never processed.

Critically: the orphaned records are isolated to `pack_column_id(new_tid, 0..N-1)` —
a PK range that belongs exclusively to `new_tid`. No future table will be assigned
`new_tid` because the sequence counter was already advanced. Therefore:

- `read_column_defs(some_other_tid)` never sees the orphaned records.
- No other table's column count is inflated by the orphan.
- The orphaned records are inert but permanent.

### What IS a problem

**Space leak.** The orphaned COL_TAB records occupy space in `sys_columns` forever. There
is no garbage collection path for orphaned records from incomplete DDL. On a
long-running instance with many crashed CREATE TABLE operations (e.g., due to repeated
client errors), `sys_columns` accumulates dead records. This is unlikely to be
practically significant today but is architecturally unsatisfying.

**No DDL idempotence.** If a client retries a failed CREATE TABLE (after the crash that
killed RPC 3), it will call `alloc_table_id()` again, getting `new_tid + 1`. It then
writes N new COL_TAB records for `new_tid + 1`, followed by a TABLE_TAB record. This
succeeds. But the orphaned records for `new_tid` remain, and `new_tid` can never be used.
Over time, the sequence counter drifts ahead of the actual table count. This is harmless
but surprising.

**No commit record.** There is no "DDL complete" marker in the WAL. An operator
inspecting the raw shard files or SAL cannot distinguish a complete CREATE TABLE from a
partial one without comparing `sys_columns` against `sys_tables`. A future tooling or
compaction path that tries to reclaim space needs this information.

### The SAL and Z-set protection against double-application

The `recover_system_tables_from_sal` function (in `runtime/bootstrap.rs`) has no LSN
deduplication check, unlike `recover_from_sal` (the user-table recovery function) which
skips entries with `msg.lsn <= flushed`. This looks alarming but is safe in practice due
to two mechanisms:

1. **Epoch fence.** The SAL recovery loop breaks when it sees a decreasing epoch.
   `flush_all_system_tables()` is called at checkpoint time, and the post-checkpoint SAL
   entries have a higher epoch number. This prevents pre-checkpoint SAL entries from being
   replayed after the shard files already contain those records.

2. **Z-set arithmetic.** If an entry were replayed twice (say, due to an epoch-fence
   failure), applying a +1 row twice to a Z-set accumulates weight +2, not a duplicate
   row. `read_column_defs` filters `current_weight > 0`, so weight +2 is observed as one
   live row. The Z-set model provides natural idempotence-under-reapplication for
   insertions.

### The 65-column panic: root cause still unresolved

During the uuid3 E2E run, `build_schema_from_col_defs` panicked with
`index out of bounds: the len is 64 and the index is 64`, called from
`hook_table_register`. This means `read_column_defs` returned 65 entries for some
table's owner_id.

What was ruled out:

- **ID reuse.** Table IDs are monotonic and never reused. Orphaned records from a crashed
  CREATE TABLE cannot inflate another table's column count.
- **SAL double-application.** Z-set arithmetic and the epoch fence prevent duplicate rows
  from accumulating.
- **TestSchemaColumnLimit.** The `test_exactly_65_columns_ok` test creates a
  `gnitz.Schema` Python object with 65 column definitions — it does NOT push a CREATE
  TABLE to the server. It cannot create COL_TAB records.
- **Post-compaction context loss.** The panic was observed before the context was
  compacted and cannot be re-examined in full.

What remains possible:

- A test in the session created a legitimate 65-column table (1 PK + 64 payload is
  within the Python wrapper's limit of 65 and would produce 65 COL_TAB records). If such
  a table exists in the session server's catalog, and if some cascade or retry path called
  `hook_table_register` for that table's ID after it was already registered, the duplicate
  hook call would re-read 65 COL_TAB records and attempt to build a 65-column
  `SchemaDescriptor`.
- A subtle double-hook-fire from a cascade retract+reinsert path in a test that exercises
  complex DDL sequences.
- An as-yet-unidentified path that writes duplicate COL_TAB records for the same
  `pack_column_id` key, which the Z-set would treat as weight +2 (still a single row).

The mitigation applied during the UUID implementation — an early `Err` return in
`hook_table_register` before calling `build_schema_from_col_defs` when
`col_defs.len() > 64` — converts the panic into a propagated error. This prevents the
server from crashing, but it does not explain or prevent the 65-entry scenario.

**Until the root cause is found, this is a latent bug.** A 65-column table in the session
server will cause a catalog error on any operation that re-triggers `hook_table_register`
for that table's ID. Under the current code, that error is propagated and the server
survives, but the DDL operation is rejected with an internal error message.

---

## Issue 4: Test Fixture Isolation

### The session-server shared-state model

The pytest suite uses a single session-scoped server process for 29 of 31 test files.
The remaining two (`test_persistence.py`, `test_checkpoint.py`) use function-scoped
server instances — one server per test — because they explicitly need isolated storage.

For the other 29 files, all tests share:
- One gnitz-server process
- One WAL / SAL file
- One sys_columns / sys_tables shard
- One sequence counter state
- One DAG / cache state

Tests use `_uid()` to generate unique schema names, preventing direct name collisions.
But they share the same table ID space, the same sys_columns PK range pool, and the same
catalog state. A test that creates a table and fails to drop it leaves state that
persists for the entire session.

### How the cascade failure manifested

In the uuid3 E2E run, the server panicked (at that point, before the hook-guard was
added) during a catalog operation. The panic was inside `guard_panic("DDL", ...)` in
`executor.rs`, which caught it and returned an error. The server process survived, but
the catalog's `pending_broadcasts` queue may have had partial entries, and the DDL
operation was rejected.

From that point, tests that depended on the catalog being in a consistent state
received errors. Depending on what the failed DDL was, some tables might be partially
registered or missing from the DAG. Tests that tried to use those tables saw
connection-level errors or internal errors — not "table not found" (which would be a
meaningful diagnostic), but generic failures. Pytest reported these as `ERROR` rather
than `FAILED`, producing 305 apparent failures whose actual common cause (one DDL panic
earlier in the run) was not visible in the test output.

This makes triage difficult. The correct first response to a 305-ERROR run is:
1. Search the output for the first FAILED or ERROR chronologically (not the first in file
   order, which may be different).
2. Check the server's stderr log (`server_debug.log`) for panic output.
3. Run that one test in isolation against a fresh server to determine if it has a real
   bug or if it's a cascade victim.

None of this is documented, and the test suite provides no health-check fixture that
detects server death or catalog corruption early and fails fast.

### Why the persistence tests were immune

`test_persistence.py` defines its own `function`-scoped server fixture. Each test gets a
fresh server with a clean catalog. When the session server's catalog was corrupted, the
persistence tests were unaffected because they never touched the shared server.

This is the correct model for tests that either (a) exercise crash/recovery paths, (b)
create wide schemas, or (c) create tables that must have predictable IDs or structures.

### The recommended fix

Introduce a `class`-scoped server fixture as a middle ground between the session server
(one for everything) and the function-scoped server (one per test). Heavy-DDL test
classes — those that create many tables, wide tables, or rely on specific schema states —
should each get a fresh server for the duration of the class. Lightweight tests (DML
operations on pre-existing tables, type checks, expression evaluations) can continue to
share the session server.

The startup cost of the server is approximately 1–2 seconds in the E2E harness. With
4 workers (`GNITZ_WORKERS=4`), a class-scoped server costs roughly 2 seconds per class.
The test suite has approximately 15 heavy-DDL test classes. Switching them to class-scoped
fixtures would add ~30 seconds to the suite runtime, which is acceptable given that the
current suite runs in ~145 seconds.

A simpler short-term fix: add a server health-check fixture that runs after every test
class and verifies the server is still responding. If it's not — or if its catalog is in
an inconsistent state — it restarts the server before the next class begins, preventing
cascade failures from propagating.

---

## Summary of recommended work

### Mechanical / low-risk

1. **`MAX_COLUMNS` constant** (`gnitz-engine/src/schema.rs`):
   Define `pub const MAX_COLUMNS: usize = 64`, replace all `[SchemaColumn; 64]` and
   related validation comparisons. Grep target: ~30 sites. Python changes: replace
   `> 65` with `> MAX_COLUMNS + 1` with a comment explaining the off-by-one.

2. **Fix `test_all_values_present`** (`crates/gnitz-py/tests/test_ergonomics.py`):
   Replace the hardcoded `set(range(1, 14))` with a self-describing assertion that
   enumerates the expected names. This prevents the test from becoming stale silently
   again when the next type code is added.

### Moderate complexity

3. **Engine depends on `gnitz-core::TypeCode`** (requires crate dependency change):
   Add `gnitz-core` to gnitz-engine's `Cargo.toml` dependencies. Replace all
   `type_code::*` match arms in `compiler.rs`, `catalog/`, and `schema.rs` with
   `TypeCode::*` enum arms. Verify no circular dependency is introduced (the current
   graph is acyclic and this edge would remain so, since gnitz-core has no engine
   dependency). This makes new type additions compile-error-driven throughout the engine
   rather than silent-miss-driven.

4. **Class-scoped server fixtures for heavy-DDL tests**:
   Identify test classes that create wide schemas, test error paths that may leave the
   catalog in a dirty state, or exercise DDL cascade paths. Move them to
   `scope="class"` server fixtures. Start with `TestUUID`, `TestTypeErrors`, and
   any test class that creates tables with more than 20 columns.

### Requires design work

5. **DDL commit record in the WAL**:
   Write a "DDL begin (table_id=X)" entry to the SAL before the COL_TAB write, and a
   "DDL commit (table_id=X)" entry after the TABLE_TAB write succeeds. On recovery,
   scan for begin records with no corresponding commit record and retract the orphaned
   COL_TAB rows. This makes CREATE TABLE crash-safe with no orphan accumulation.

   Alternatively, as a lighter fix: add a startup garbage-collection pass in
   `CatalogEngine::open()` that scans `sys_columns` for owner IDs with no corresponding
   `sys_tables` row and retracts those records. This doesn't prevent the orphans but
   prevents their accumulation across restarts.

6. **Root cause investigation for the 65-column panic**:
   Add a structured log line in `read_column_defs` that records the owner_id and the
   returned count whenever the count exceeds some threshold (e.g., 32). Run the full E2E
   suite with this logging enabled and examine which table ID produces an anomalous count.
   Cross-reference against the test sequence to determine which test created that table
   and whether its cleanup ran. Without this instrumentation, the root cause cannot be
   determined from static analysis alone.

---

## Appendix: TypeCode change checklist

Until Issue 1 (engine TypeCode decoupling) is resolved, adding a new TypeCode requires
manual updates at the following sites. This list should be kept current and consulted
before each new type.

| File | What to change |
|---|---|
| `gnitz-wire/src/lib.rs` | Add `pub const NEWTYPE: u8 = N` in `type_code` module; extend `wire_stride()` if stride differs from existing types |
| `gnitz-core/src/protocol/types.rs` | Add `NewType = tc::NEWTYPE` variant; extend `try_from_u64()`; extend `ZSetBatch::new()` (ColData arm); extend `BatchAppender::zero_val()` and any type-specific `_val()` helpers; extend `ZSetBatch::validate()` |
| `gnitz-core/src/protocol/wal_block.rs` | Extend encode match arm; extend decode match arm |
| `gnitz-sql/src/types.rs` | Add `DataType::NewType => Ok(TypeCode::NewType)` if sqlparser has a corresponding `DataType` variant |
| `gnitz-sql/src/dml.rs` | `extract_pk_value()`: add string/literal parsing arm if the type has a non-numeric literal syntax; `append_value_to_col()`: same; `try_col_eq_literal()`: same; residual filter error message |
| `gnitz-sql/src/expr.rs` | `compile_bound_expr()`: decide if the type is allowed in view expressions (likely no for U128-alike types) |
| `gnitz-engine/src/catalog/ddl.rs` | PK type validation: add `type_code::NEWTYPE` to the allowed-PK-types check |
| `gnitz-engine/src/catalog/utils.rs` | `get_index_key_type()`: add arm if the type can be used as an index key |
| `gnitz-engine/src/catalog/hooks.rs` | No change needed if validation is in `ddl.rs`; review `hook_table_register` PK type guard |
| `gnitz-engine/src/compiler.rs` | `agg_value_idx_eligible()`: exclude if not MIN/MAX-eligible; `make_agg_output_schema_single_gc()`: include if usable as GROUP BY key; GROUP BY hash-key exclusion; join schema column encoding |
| `gnitz-py/src/lib.rs` | Add `#[classattr] pub const NEWTYPE: u32 = N` to `PyTypeCode`; extend column serialization match arm in scan output; add formatting helper if needed |
| `gnitz-py/python/gnitz/_types.py` | Add to Python type stub if it exists |
| `crates/gnitz-py/tests/test_ergonomics.py` | Update `test_all_values_present` expected set |
| Tests | Add unit tests for the new type in `dml.rs`, `types.rs`, engine integration tests, and Python E2E tests |

The length of this checklist is itself an argument for resolving Issue 1.
