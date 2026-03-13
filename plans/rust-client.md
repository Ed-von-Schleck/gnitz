# Plan: Rust Client Library

## Overview

Build a production-grade GnitzDB client in Rust, exposing:
- A native Rust API (low-level ZSet-graph + high-level DDL/circuit)
- A stable C API via `cbindgen` (for Go, Java, .NET, C consumers)
- Python bindings via PyO3 + maturin

The Python client (`py_client/gnitz_client`) is the authoritative reference implementation
and stays in place as the E2E test harness throughout. Each phase is independently
compilable and testable without the next phase existing.

---

## Crate structure

```
gnitz/rust_client/          (Cargo workspace)
├── Cargo.toml              (workspace manifest)
├── gnitz-protocol/         Phase 1-2: wire codec + transport
├── gnitz-core/             Phase 3-5: client logic + builders
├── gnitz-capi/             Phase 6:   C FFI + cbindgen
└── gnitz-py/               Phase 7:   PyO3 extension
```

---

## Phase 0 — Workspace skeleton ✓ DONE

**Goal**: Cargo workspace compiles cleanly. No logic yet.

### Deliverables

1. `rust_client/Cargo.toml` — workspace manifest listing all four member crates.
2. `gnitz-protocol/` — `Cargo.toml` + `src/lib.rs` (empty `pub mod` stubs).
3. `gnitz-core/` — `Cargo.toml` (dep: `gnitz-protocol`) + `src/lib.rs`.
4. `gnitz-capi/` — `Cargo.toml` (`crate-type = ["cdylib", "staticlib"]`, dep: `gnitz-core`) + `src/lib.rs`.
5. `gnitz-py/` — `Cargo.toml` (`crate-type = ["cdylib"]`, dep: `gnitz-core`, pyo3 feature `extension-module`) + `src/lib.rs`.
6. `gnitz-py/pyproject.toml` — maturin build backend, module name `gnitz`.

### Key dependencies

```toml
# gnitz-protocol
libc = "0.2"

# gnitz-core
gnitz-protocol = { path = "../gnitz-protocol" }

# gnitz-capi
gnitz-core = { path = "../gnitz-core" }

# gnitz-py
pyo3 = { version = "0.23", features = ["extension-module", "abi3-py310"] }
gnitz-core = { path = "../gnitz-core" }
# Note: abi3-py310 used instead of bare extension-module — avoids Python 3.14 >
# pyo3-max-3.13 version check; wheel works with any CPython >= 3.10.
```

### Test

```bash
cd rust_client && cargo build --workspace 2>&1 | tail -5
```

---

## Phase 1 — `gnitz-protocol`: wire types and codec

**Goal**: Complete encode/decode of the gnitz IPC v2 wire format, no I/O.
Reference: `py_client/gnitz_client/protocol.py`, `batch.py`, `types.py`.

### Deliverables

#### Types (`gnitz-protocol/src/types.rs`)

- `TypeCode` enum: `U8=1 .. STRING=11, U128=12`. Implement `wire_stride() -> usize`.
- `ColumnDef { name: String, type_code: TypeCode, is_nullable: bool }`.
- `Schema { columns: Vec<ColumnDef>, pk_index: usize }`. Implement `META_SCHEMA` constant.
- `ColData` enum: `Fixed(Vec<u8>)` (raw packed bytes), `Strings(Vec<Option<String>>)`,
  `U128s(Vec<u128>)`. Using raw bytes for fixed-width avoids a per-type enum explosion
  while still being zero-copy for encode.
- `ZSetBatch { pk_lo: Vec<u64>, pk_hi: Vec<u64>, weights: Vec<i64>, nulls: Vec<u64>, columns: Vec<ColData> }`.
  Methods: `new(schema) -> Self`, `len() -> usize`, `is_empty() -> bool`.

#### Header (`gnitz-protocol/src/header.rs`)

Constants mirroring `protocol.py`:
```rust
pub const MAGIC_V2: u64 = 0x474E49545A325043;
pub const HEADER_SIZE: usize = 96;
pub const ALIGNMENT: usize = 64;

pub const FLAG_ALLOCATE_TABLE_ID: u64 = 1;
pub const FLAG_ALLOCATE_SCHEMA_ID: u64 = 2;
pub const FLAG_SHUTDOWN: u64 = 4;
pub const FLAG_DDL_SYNC: u64 = 8;
pub const FLAG_EXCHANGE: u64 = 16;
pub const FLAG_PUSH: u64 = 32;
pub const FLAG_HAS_PK: u64 = 64;

pub const STATUS_OK: u32 = 0;
pub const STATUS_ERROR: u32 = 1;
```

`Header` struct with all 11 fields. `Header::pack() -> [u8; 96]`.
`Header::unpack(buf: &[u8]) -> Result<Header, ProtocolError>` — validates magic.

#### Codec (`gnitz-protocol/src/codec.rs`)

- `fn align_up(val: usize, align: usize) -> usize`
- `fn layout(schema: &Schema, count: usize) -> Layout` — computes `pk_lo_off`, `pk_hi_off`,
  `weight_off`, `null_off`, `col_offsets: Vec<usize>`, `col_strides: Vec<usize>`, `blob_off`.
  Mirrors `_walk_layout` in `batch.py`.
- `fn encode_zset(schema: &Schema, batch: &ZSetBatch) -> (Vec<u8>, u64)` — returns
  `(section_bytes, blob_size)`. All column types: U8..U64/I8..I64/F32/F64 (fixed, raw
  pack), U128 (two u64 LE), STRING (u32 offset + u32 length + global blob arena).
- `fn decode_zset(data: &[u8], offset: usize, schema: &Schema, count: usize, blob_sz: usize) -> Result<ZSetBatch>`.
- `fn schema_to_batch(schema: &Schema) -> ZSetBatch` — encodes as META_SCHEMA batch.
- `fn batch_to_schema(batch: &ZSetBatch) -> Result<Schema>`.

#### Error type (`gnitz-protocol/src/error.rs`)

```rust
#[derive(Debug)]
pub enum ProtocolError {
    BadMagic(u64),
    UnknownTypeCode(u64),
    DecodeError(String),
    IoError(std::io::Error),
}
impl std::error::Error for ProtocolError {}
```

### Tests (`gnitz-protocol/src/tests/`)

Unit tests only — no server required:

- `test_roundtrip_all_types`: encode then decode a 10-row batch containing one column of
  every TypeCode. Assert decoded == original for every row/column.
- `test_roundtrip_strings`: batch with nullable STRING columns; NULL strings, empty strings,
  and UTF-8 strings with multi-byte chars.
- `test_roundtrip_u128`: batch with U128 PK and U128 payload column.
- `test_header_pack_unpack`: pack a Header, unpack it, assert all fields round-trip.
- `test_schema_meta_roundtrip`: `schema_to_batch` then `batch_to_schema`, assert identical schema.
- `test_align_up`: boundary values (0, 1, 63, 64, 65, 127, 128).

```bash
cd rust_client && cargo test -p gnitz-protocol
```

---

## Phase 2 — `gnitz-protocol`: transport

**Goal**: Unix SOCK_SEQPACKET + SCM_RIGHTS memfd passing. No server logic.
Reference: `py_client/gnitz_client/transport.py`.

### Deliverables

`gnitz-protocol/src/transport.rs`:

- `fn connect(socket_path: &str) -> Result<RawFd, ProtocolError>` — `AF_UNIX SOCK_SEQPACKET`.
- `fn send_memfd(sock_fd: RawFd, data: &[u8]) -> Result<(), ProtocolError>` — `memfd_create`,
  `ftruncate`, `mmap`, `sendmsg` with `SCM_RIGHTS` ancillary data (1-byte dummy payload `b"G"`).
- `fn recv_memfd(sock_fd: RawFd) -> Result<Vec<u8>, ProtocolError>` — `recvmsg`, extract fd
  from ancdata, `fstat` for size, `mmap`, copy bytes, `close`.

All `libc` calls — no `std::net` (it doesn't expose `SOCK_SEQPACKET` or `sendmsg`).

#### Higher-level helpers in `gnitz-protocol/src/message.rs`

```rust
pub struct Message {
    pub header: Header,
    pub schema_batch: Option<ZSetBatch>,  // decoded using META_SCHEMA
    pub data_batch: Option<ZSetBatch>,    // decoded using provided schema
}

pub fn send_message(
    sock_fd: RawFd,
    header: Header,
    schema: Option<&Schema>,
    schema_batch: Option<&ZSetBatch>,
    data_batch: Option<&ZSetBatch>,
) -> Result<(), ProtocolError>

pub fn recv_message(
    sock_fd: RawFd,
    data_schema: Option<&Schema>,   // needed to decode data section
) -> Result<Message, ProtocolError>
```

`send_message` serializes into a single buffer (header + schema section + data section),
passes via memfd. `recv_message` does the reverse.

### Tests

Integration tests using `socketpair(AF_UNIX, SOCK_SEQPACKET)` — no server:

- `test_transport_loopback`: send 4096 bytes, receive same bytes.
- `test_message_roundtrip_empty`: send/recv a push message with empty data batch.
- `test_message_roundtrip_data`: 100-row batch with mixed types, verify full round-trip via
  `send_message` + `recv_message` on a socketpair.
- `test_message_roundtrip_strings`: 50-row batch with STRING columns, round-trip.

```bash
cd rust_client && cargo test -p gnitz-protocol -- transport
```

---

## Phase 3 — `gnitz-core`: low-level client

**Goal**: Connect to a running `gnitz-server-c` and execute raw operations.

### Deliverables

`gnitz-core/src/connection.rs`:

```rust
pub struct Connection {
    sock_fd: RawFd,
    pub client_id: u64,  // assigned at connect time (PID-based)
}

impl Connection {
    pub fn connect(socket_path: &str) -> Result<Self, ClientError>
    pub fn close(self)

    // Raw request/response
    pub fn roundtrip(
        &self,
        target_id: u64,
        flags: u64,
        schema: Option<&Schema>,
        data: Option<&ZSetBatch>,
    ) -> Result<Message, ClientError>
}
```

`gnitz-core/src/ops.rs` — thin wrappers over `roundtrip`:

```rust
pub fn alloc_table_id(conn: &Connection) -> Result<u64, ClientError>
pub fn alloc_schema_id(conn: &Connection) -> Result<u64, ClientError>
pub fn push(conn: &Connection, target_id: u64, schema: &Schema, batch: &ZSetBatch)
    -> Result<(Schema, ZSetBatch), ClientError>
pub fn scan(conn: &Connection, target_id: u64, schema: &Schema)
    -> Result<(Schema, ZSetBatch), ClientError>
```

`ClientError` wraps `ProtocolError` and adds `ServerError(String)` (non-zero status).

### Tests (integration — requires server)

`gnitz-core/tests/integration.rs` — guarded by `#[cfg(feature = "integration")]`.
Run with: `GNITZ_SOCK=/tmp/gnitz_test.sock cargo test -p gnitz-core --features integration`.

- `test_connect_disconnect`: connect, close, no crash.
- `test_alloc_ids`: alloc 10 table IDs, verify they are distinct and >= FIRST_USER_TABLE_ID.
- `test_push_scan_roundtrip`: push a 5-row batch to a system table via raw ops, scan back,
  verify rows and weights.

The test harness in `gnitz-core/tests/helpers.rs` starts `gnitz-server-c` as a subprocess
(using `std::process::Command`) and cleans up after the test — same pattern as `conftest.py`.

---

## Phase 4 — `gnitz-core`: DDL and DML helpers

**Goal**: Full typed API for schema management and data operations.
Reference: `py_client/gnitz_client/client.py` (DDL section, ~250 LOC).

### Deliverables

`gnitz-core/src/client.rs` — `GnitzClient` struct:

```rust
pub struct GnitzClient {
    conn: Connection,
}

impl GnitzClient {
    pub fn connect(socket_path: &str) -> Result<Self, ClientError>

    // ID allocation
    pub fn alloc_table_id(&self) -> Result<u64, ClientError>
    pub fn alloc_schema_id(&self) -> Result<u64, ClientError>

    // DDL
    pub fn create_schema(&self, name: &str) -> Result<u64, ClientError>
    pub fn drop_schema(&self, name: &str) -> Result<(), ClientError>
    pub fn create_table(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        pk_col_idx: usize,
        unique_pk: bool,
    ) -> Result<u64, ClientError>
    pub fn drop_table(&self, schema_name: &str, table_name: &str) -> Result<(), ClientError>

    // DML
    pub fn push(
        &self, table_id: u64, schema: &Schema, batch: &ZSetBatch,
    ) -> Result<(Schema, ZSetBatch), ClientError>
    pub fn scan(&self, table_id: u64, schema: &Schema)
        -> Result<(Schema, ZSetBatch), ClientError>
    pub fn delete(
        &self, table_id: u64, schema: &Schema, pks: &[(u64, u64)],
    ) -> Result<(), ClientError>

    // Internal helpers (pub for testing)
    pub fn resolve_table_id(&self, schema_name: &str, table_name: &str)
        -> Result<(u64, Schema), ClientError>
}
```

The internal helpers replicate the system-table scan + filter logic from `client.py`
(`_resolve_schema_id`, `_resolve_table_id`, `_scan_col_tab`).

### Tests (integration)

Ports of `test_ddl.py`, `test_dml.py`, `test_catalog.py`:

- `test_create_drop_schema`
- `test_create_drop_table`
- `test_push_and_scan`: insert rows, scan, verify.
- `test_delete_rows`: insert then delete by PK, scan returns empty.
- `test_duplicate_schema_error`: creating same schema twice returns `ClientError`.
- `test_scan_system_tables`: scan SCHEMA_TAB, TABLE_TAB directly.
- `test_string_columns`: push/scan table with STRING columns including NULLs.

```bash
GNITZ_SOCK=... cargo test -p gnitz-core --features integration -- ddl dml catalog
```

---

## Phase 5 — `gnitz-core`: circuit and expression builders

**Goal**: Build and deploy DBSP circuit graphs from Rust.
Reference: `py_client/gnitz_client/circuit.py`, `expr.py`.

### Deliverables

`gnitz-core/src/expr.rs` — `ExprBuilder`:

```rust
pub struct ExprBuilder { ... }

impl ExprBuilder {
    pub fn new() -> Self
    pub fn load_col_int(&mut self, col_idx: usize) -> u32   // returns reg_id
    pub fn load_col_float(&mut self, col_idx: usize) -> u32
    pub fn load_const(&mut self, value: i64) -> u32
    pub fn add(&mut self, a: u32, b: u32) -> u32
    pub fn sub(&mut self, a: u32, b: u32) -> u32
    pub fn cmp_eq(&mut self, a: u32, b: u32) -> u32
    pub fn cmp_ne(&mut self, a: u32, b: u32) -> u32
    pub fn cmp_gt(&mut self, a: u32, b: u32) -> u32
    pub fn cmp_ge(&mut self, a: u32, b: u32) -> u32
    pub fn cmp_lt(&mut self, a: u32, b: u32) -> u32
    pub fn cmp_le(&mut self, a: u32, b: u32) -> u32
    pub fn bool_and(&mut self, a: u32, b: u32) -> u32
    pub fn bool_or(&mut self, a: u32, b: u32) -> u32
    pub fn bool_not(&mut self, a: u32) -> u32
    pub fn is_null(&mut self, a: u32) -> u32
    pub fn is_not_null(&mut self, a: u32) -> u32
    pub fn build(self, result_reg: u32) -> ExprProgram
}

pub struct ExprProgram {
    pub num_regs: u32,
    pub result_reg: u32,
    pub code: Vec<u32>,   // 4-word instructions
}
```

`gnitz-core/src/circuit.rs` — `CircuitBuilder`:

```rust
pub type NodeId = u64;

pub struct CircuitBuilder { ... }

impl CircuitBuilder {
    pub fn new(view_id: u64, primary_source_id: u64) -> Self

    pub fn input_delta(&mut self) -> NodeId
    pub fn trace_scan(&mut self, table_id: u64) -> NodeId
    pub fn filter(&mut self, input: NodeId, expr: ExprProgram) -> NodeId
    pub fn map(&mut self, input: NodeId, projection: &[usize]) -> NodeId
    pub fn negate(&mut self, input: NodeId) -> NodeId
    pub fn union(&mut self, a: NodeId, b: NodeId) -> NodeId
    pub fn delay(&mut self, input: NodeId) -> NodeId
    pub fn join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId
    pub fn reduce(
        &mut self, input: NodeId, group_cols: &[usize], agg_col_idx: usize,
    ) -> NodeId
    pub fn distinct(&mut self, input: NodeId) -> NodeId
    pub fn anti_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId
    pub fn semi_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId
    pub fn shard(&mut self, input: NodeId, shard_cols: &[usize]) -> NodeId
    pub fn gather(&mut self, input: NodeId, worker_id: u64) -> NodeId
    pub fn sink(&mut self, input: NodeId, view_id: u64) -> NodeId

    pub fn build(self) -> CircuitGraph
}
```

`GnitzClient` additions:

```rust
pub fn create_view(
    &self,
    schema_name: &str,
    view_name: &str,
    source_table_id: u64,
    output_columns: &[ColumnDef],
) -> Result<u64, ClientError>

pub fn create_view_with_circuit(
    &self,
    schema_name: &str,
    view_name: &str,
    circuit: CircuitGraph,
    output_columns: &[ColumnDef],
) -> Result<u64, ClientError>
```

### Tests (integration)

Ports of core cases from `test_dbsp_ops.py` and `test_workers.py`:

- `test_filter_view`: create table, create filter view, push rows, scan view.
- `test_reduce_view`: create table, create reduce (SUM) view, push rows, verify aggregation.
- `test_join_view`: two tables, join view, push to both, scan join result.
- `test_anti_join_view`: anti-join excludes matched rows.
- `test_exchange_multi_worker`: run with `GNITZ_WORKERS=4`, push 1000 rows, scan and verify
  partition-correctness (all rows present, no duplicates).
- `test_incremental_update`: push initial rows, scan view, push delta, scan again —
  verify incremental result.

```bash
GNITZ_WORKERS=4 GNITZ_SOCK=... cargo test -p gnitz-core --features integration -- circuit
```

---

## Phase 6 — `gnitz-capi`: C FFI layer

**Goal**: Stable C API with auto-generated header. No C++ yet.

### Deliverables

`gnitz-capi/src/lib.rs` — opaque handles + extern "C" functions:

```c
// Opaque types (gnitz.h will contain forward declarations)
typedef struct GnitzConn GnitzConn;
typedef struct GnitzBatch GnitzBatch;
typedef struct GnitzSchema GnitzSchema;
typedef struct GnitzCircuit GnitzCircuit;

// Connection
GnitzConn* gnitz_connect(const char* socket_path);
void       gnitz_disconnect(GnitzConn* conn);
const char* gnitz_last_error(void);  // thread-local

// Schema / ColumnDef
GnitzSchema* gnitz_schema_new(int pk_index);
void         gnitz_schema_add_col(GnitzSchema*, const char* name, int type_code, int nullable);
void         gnitz_schema_free(GnitzSchema*);

// Batch
GnitzBatch* gnitz_batch_new(GnitzSchema*);
void        gnitz_batch_append_row(GnitzBatch*, uint64_t pk_lo, uint64_t pk_hi,
                                   int64_t weight, uint64_t null_mask,
                                   const void* col_data, size_t col_data_len);
void        gnitz_batch_free(GnitzBatch*);
size_t      gnitz_batch_len(GnitzBatch*);

// DDL
uint64_t gnitz_create_schema(GnitzConn*, const char* name);
uint64_t gnitz_create_table(GnitzConn*, const char* schema_name,
                             const char* table_name, GnitzSchema*,
                             int unique_pk);
int      gnitz_drop_schema(GnitzConn*, const char* name);
int      gnitz_drop_table(GnitzConn*, const char* schema_name, const char* table_name);

// DML
int         gnitz_push(GnitzConn*, uint64_t table_id, GnitzSchema*, GnitzBatch*);
GnitzBatch* gnitz_scan(GnitzConn*, uint64_t table_id, GnitzSchema*);

// Views
uint64_t gnitz_create_view(GnitzConn*, const char* schema_name, const char* view_name,
                            uint64_t source_table_id, GnitzSchema* output_schema);
uint64_t gnitz_create_view_with_circuit(GnitzConn*, const char* schema_name,
                                         const char* view_name, GnitzCircuit*,
                                         GnitzSchema* output_schema);

// String helpers
void gnitz_free_string(char*);
```

Return convention: `0` / `NULL` on error; call `gnitz_last_error()` to retrieve message.

`gnitz-capi/build.rs` — runs `cbindgen` to emit `gnitz-capi/gnitz.h`.

`Cargo.toml` additions:
```toml
[features]
default = []
integration = ["gnitz-core/integration"]

[[example]]
name = "c_test_runner"
```

**`gnitz-capi/examples/c_test.c`** — a C program that:
1. Connects to server
2. Creates schema + table
3. Pushes 10 rows
4. Scans back and prints row count
5. Drops table + schema
6. Prints `OK` on success

Build it via a small `build.rs` or a standalone `Makefile` in `gnitz-capi/`.

### Tests

```bash
# Build C API
cd rust_client && cargo build -p gnitz-capi --features integration

# Compile and run C test
cc -o /tmp/c_test gnitz-capi/examples/c_test.c \
    -I gnitz-capi/ \
    -L target/debug/ -lgnitz_capi \
    -Wl,-rpath,target/debug/ && \
GNITZ_SOCK=... /tmp/c_test
```

---

## Phase 7 — `gnitz-py`: Python bindings

**Goal**: Full Python API equivalent to `py_client/gnitz_client`, verified by running the
existing E2E test suite against the new bindings.

### Deliverables

`gnitz-py/src/lib.rs` — PyO3 module `gnitz`:

```python
# Python API surface
import gnitz

# Types
gnitz.TypeCode.U8  # ... through U128, STRING
gnitz.ColumnDef(name="x", type_code=gnitz.TypeCode.I64, is_nullable=False)
gnitz.Schema(columns=[...], pk_index=0)

# Batch
batch = gnitz.ZSetBatch(schema)
batch.append(pk_lo=1, pk_hi=0, weight=1, nulls=0, columns=[42])
len(batch)                    # row count
batch.pk_lo                   # list[int]
batch.pk_hi                   # list[int]
batch.weights                 # list[int]
batch.get_column(ci)          # list[int|float|str|None]

# Client
client = gnitz.GnitzClient("/run/gnitz/gnitz.sock")
schema_id = client.create_schema("myschema")
table_id  = client.create_table("myschema", "t", columns, pk_col_idx=0)
client.push(table_id, schema, batch)
schema_out, result = client.scan(table_id, schema)
client.drop_table("myschema", "t")
client.drop_schema("myschema")
client.close()

# Circuit + Expr builders
expr = gnitz.ExprBuilder()
r0 = expr.load_col_int(0)
r1 = expr.load_const(50)
cond = expr.cmp_gt(r0, r1)
prog = expr.build(result_reg=cond)

builder = gnitz.CircuitBuilder(view_id, source_table_id)
inp = builder.input_delta()
filtered = builder.filter(inp, prog)
sink = builder.sink(filtered, view_id)
circuit = builder.build()

view_id = client.create_view_with_circuit("myschema", "v", circuit, output_cols)
```

The Python module is intentionally API-compatible with `gnitz_client` so the existing
test suite can be ported with minimal changes (import swap + minor API differences).

### Compatibility shim

Create `py_client/gnitz_client_rs/__init__.py` as a thin wrapper that re-exports `gnitz`
(the Rust extension) with the same names as `gnitz_client`. This allows running the
existing tests with `import gnitz_client_rs as gnitz_client` and no other changes.

### Tests

Port the full E2E suite to use `gnitz-py`. The `py_client/tests/` directory stays
unchanged as the reference. A new `rust_client/gnitz-py/tests/` pytest suite duplicates
coverage against the Rust bindings.

Verification:
```bash
# Build wheel
cd rust_client/gnitz-py && maturin develop

# Run full suite (must match GNITZ_WORKERS=4 policy)
GNITZ_WORKERS=4 uv run pytest rust_client/gnitz-py/tests/ -x --tb=short

# Regression check: original py_client suite still passes
GNITZ_WORKERS=4 uv run pytest py_client/tests/ -x --tb=short
```

All tests that pass against `gnitz_client` must pass against `gnitz-py`.

---

## Phase 8 (optional) — Arrow interop

**Goal**: Zero-copy exchange between `ZSetBatch` and PyArrow `RecordBatch`.

Gated behind `gnitz-py` feature flag `arrow`. Not in the critical path.

### Deliverables

- `ZSetBatch.to_arrow(self) -> pyarrow.RecordBatch` — maps columns to Arrow arrays.
  Weight and nulls become metadata columns (`__weight__`, null validity bitmap).
  U128 becomes `pa.uint64()` lo/hi pair or `pa.large_binary(16)`.
- `ZSetBatch.from_arrow(rb: pyarrow.RecordBatch, pk_col: str, weight_col: str) -> ZSetBatch` —
  constructs a ZSetBatch from an Arrow table with designated PK and weight columns.

### Tests

```bash
maturin develop --features arrow
python -c "
import gnitz, pyarrow as pa
# build batch, to_arrow, verify RecordBatch column types
# from_arrow round-trip
"
```

---

## Verification matrix

| Phase | Unit tests | Integration tests | Depends on server |
|-------|-----------|-------------------|-------------------|
| 0     | cargo build | — | No |
| 1     | cargo test -p gnitz-protocol | — | No |
| 2     | cargo test -p gnitz-protocol | socketpair loopback | No |
| 3     | — | connect + raw ops | Yes |
| 4     | — | DDL + DML + catalog | Yes |
| 5     | — | circuits + exchange | Yes (GNITZ_WORKERS=4) |
| 6     | — | C test binary | Yes |
| 7     | — | full E2E suite | Yes (GNITZ_WORKERS=4) |
| 8     | pyarrow round-trip | — | No |

Phases 0–2 are fully offline (no server). Phases 3–7 require `gnitz-server-c` to be built
(`make server` from repo root).

---

## Implementation order notes

- Phases 0–2 can be done in a single session: pure Rust, no server.
- Phase 3 is the first integration phase; the test harness setup (subprocess spawn) is
  the main new work.
- Phases 4 and 5 are additive to Phase 3's infrastructure.
- Phase 6 (C FFI) can be done in parallel with Phase 5 once Phase 4 is done.
- Phase 7 (Python) depends on Phase 5.
- Phase 8 is independent after Phase 7.
