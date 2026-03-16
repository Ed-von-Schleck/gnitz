# Plan: SQL Support via Rust Frontend (sqlparser-rs)

---

## Crate Architecture

```
rust_client/
  gnitz-core/      — existing: IPC, CircuitBuilder, ExprBuilder
  gnitz-protocol/  — existing: wire format, Header
  gnitz-capi/      — existing: C FFI header
  gnitz-py/        — existing: PyO3 bindings
  gnitz-sql/       — Phase 1 DONE: SQL frontend
    src/
      lib.rs          — public API: SqlPlanner
      types.rs        — SQL data types → gnitz TypeCode
      binder.rs       — name resolution, schema lookup, index lookup
      expr.rs         — BoundExpr → ExprBuilder → ExprProgram (server-side)
      eval.rs         — BoundExpr evaluator (client-side, Phase 3+)
      planner.rs      — DDL execution: CREATE TABLE/VIEW/INDEX, DROP
      dml.rs          — INSERT/UPDATE/DELETE/SELECT execution
      logical_plan.rs — internal relational algebra nodes
    Cargo.toml
      [dependencies]
        sqlparser = "0.56"   # pinned; API is not stable across minor versions
        gnitz-core = { path = "../gnitz-core" }
```

The planner is entirely client-side. The RPython server receives the same `CircuitGraph`
and `ZSetBatch` messages it always has. New server-side additions are small, flag-gated
handlers in `executor.py`.

---

## IPC Header: Tagged-Union Protocol

This section is the authoritative specification for the 96-byte IPC header.

### Wire layout

```
Bytes   Field           Type   Always valid?
──────  ──────────────  ─────  ─────────────
 0- 7   magic           u64    Yes — 0x474E49545A325043 ("GNITZ2PC" LE)
 8-11   status          u32    Yes — 0=OK, 1=ERROR (responses only)
12-15   err_len         u32    Yes — byte length of error string (0 if none)
16-23   target_id       u64    Yes — table/view/entity ID; 0 for ID-allocate ops
24-31   client_id       u64    Yes — sender's PID
72-79   flags           u64    Yes — operation discriminant (see table below)
```

The remaining 56 bytes are **parameter slots** whose meaning is entirely determined
by which flags are set. No two simultaneously-active flags may own the same slot.

```
Bytes   Slot  FLAG_PUSH          FLAG_SEEK          FLAG_SEEK_BY_INDEX   FLAG_RANGE_SEEK (future)
──────  ────  ─────────────────  ─────────────────  ───────────────────  ────────────────────────
32-39   p0    schema_count       —                  —                    —
40-47   p1    schema_blob_sz     —                  —                    —
48-55   p2    data_count         —                  —                    —
56-63   p3    data_blob_sz       —                  —                    range_hi_lo
64-71   p4    (unused/zero)      —                  seek_col_idx         range_hi_hi
80-87   p5    —                  seek_key_lo        seek_key_lo          range_lo_lo
88-95   p6    —                  seek_key_hi        seek_key_hi          range_lo_hi
```

**Why p4 is free for FLAG_PUSH:** Slot p4 was historically named `data_pk_index` in
the Rust client and was written as `schema.pk_index` for push operations. A full audit
confirmed it is read into a local variable and immediately discarded — never stored in
`IPCPayload`, never accessed in `executor.py`. The PK index is already encoded in the
schema batch via the `META_FLAG_IS_PK` column flag. The slot (now renamed `p4` in both
ipc.py and header.rs) is genuinely free in all FLAG_PUSH operations. Phase 2 adds its
first real use: `col_idx` for FLAG_SEEK_BY_INDEX.

### Protocol flags

```python
# ipc.py — authoritative flag assignments (matches gnitz/server/ipc.py exactly)
FLAG_SCAN               = 0    # explicit name for "no flags" (no wire change) ✅ DONE
FLAG_ALLOCATE_TABLE_ID  = 1    # target_id=0; server responds with new table/view ID
FLAG_ALLOCATE_SCHEMA_ID = 2    # target_id=0; server responds with new schema ID
FLAG_SHUTDOWN           = 4
FLAG_DDL_SYNC           = 8
FLAG_EXCHANGE           = 16
FLAG_PUSH               = 32
FLAG_HAS_PK             = 64
FLAG_SEEK               = 128
FLAG_SEEK_BY_INDEX      = 256  # Phase 2: p4=col_idx, p5/p6=key ✅ DONE
FLAG_ALLOCATE_INDEX_ID  = 512  # Phase 2: allocate new index ID  ✅ DONE
```

### Rust: semantic accessor methods

The `Header` struct retains its existing field names for binary compatibility.
Semantic accessor methods express per-flag meaning without confusion:

```rust
impl Header {
    // FLAG_SEEK_BY_INDEX view  (p4 = col_idx, p5/p6 = key) — ✅ DONE (in header.rs)
    pub fn set_seek_by_index(&mut self, col_idx: u64, key_lo: u64, key_hi: u64) {
        self.p4         = col_idx;
        self.seek_pk_lo = key_lo;
        self.seek_pk_hi = key_hi;
    }
    pub fn seek_col_idx(&self)    -> u64 { self.p4 }
    pub fn seek_idx_key_lo(&self) -> u64 { self.seek_pk_lo }
    pub fn seek_idx_key_hi(&self) -> u64 { self.seek_pk_hi }

    // TODO Phase 2: FLAG_SEEK convenience wrappers — NOT YET ADDED
    // pub fn set_seek_key(&mut self, lo: u64, hi: u64) { self.seek_pk_lo = lo; self.seek_pk_hi = hi; }
    // pub fn seek_key_lo(&self) -> u64 { self.seek_pk_lo }
    // pub fn seek_key_hi(&self) -> u64 { self.seek_pk_hi }
}
```

The `set_seek_by_index`, `seek_col_idx`, `seek_idx_key_lo`, `seek_idx_key_hi` accessors are
implemented. The convenience `set_seek_key`/`seek_key_lo`/`seek_key_hi` wrappers for
`FLAG_SEEK` are still TODO — callers currently write `seek_pk_lo`/`seek_pk_hi` directly.

### Python: semantic offset aliases

```python
# ipc.py — aliases for slot meanings per flag — ✅ DONE
OFF_SEEK_IDX_COL    = OFF_P4              # p4 when FLAG_SEEK_BY_INDEX
OFF_SEEK_IDX_KEY_LO = OFF_SEEK_PK_LO     # p5
OFF_SEEK_IDX_KEY_HI = OFF_SEEK_PK_HI     # p6
```

### FLAG_SCAN: making the implicit explicit

`FLAG_SCAN = 0` is added to `ipc.py` as an explicit constant (✅ DONE). The executor
dispatch still falls through all flag checks for the scan/push path — the explicit
`elif intmask(flags) == ipc.FLAG_SCAN:` branch is TODO (Phase 2 cleanup item).

No wire change. `FLAG_SCAN = 0` is both a constant and documentation.

---

## Parser: sqlparser-rs

sqlparser-rs 0.56 (pinned; API is not stable across minor versions).

Key AST types used:

```rust
// Entry point
let stmts = Parser::parse_sql(&GenericDialect, sql)?;

// DDL
Statement::CreateTable(CreateTable { name, columns, constraints, .. })
Statement::CreateView { name, query, .. }
Statement::Drop { object_type: ObjectType::Table | View | Index, names, .. }
Statement::CreateIndex(CreateIndex { name, table_name, columns, unique, .. })

// DML write
Statement::Insert(Insert { table: TableObject::TableName(..), columns, source, .. })
Statement::Update(Update { table, assignments, selection, .. })
Statement::Delete(Delete { from, selection, .. })

// DML read
Statement::Query(Box<Query {
    body: Box<SetExpr>,
    limit_clause: Option<LimitClause>,   // NOT limit: Option<Expr> — changed in 0.56
    ..
}>)

// sqlparser 0.56 breaking API notes (relevant for all phases):
// - Expr::Value wraps ValueWithSpan, not Value directly
// - ObjectNamePart is an enum; access ident via .as_ident().map(|i| i.value.clone())
// - Insert.table is TableObject enum: TableObject::TableName(obj_name)
// - query.limit_clause is LimitClause enum (LimitOffset / OffsetCommaLimit)
// - Unsigned DataType variants: BigIntUnsigned(_), IntUnsigned(_), etc.
// - Double variant: Double(_) with args, not Double alone
```

---

## Internal Architecture

```
SQL string
    ↓
[sqlparser::Parser]
    ↓  Vec<Statement>
[Binder]   — resolves names via GnitzClient::resolve_table_or_view_id()
            — caches index map via GnitzClient::find_index_for_column()
    ↓  bound AST (table IDs + col indices + index IDs instead of names)
[Planner / DmlPlanner]
    │  DDL  → GnitzClient DDL calls (create_table, create_view_with_circuit, …)
    │  DML write → ZSetBatch construction + client::push() / delete()
    │  DML read  → client::scan() / seek() / seek_by_index()
    │  CREATE VIEW → CircuitBuilder → CircuitGraph → create_view_with_circuit()
    ↓  SqlResult
returned to caller
```

### LogicalPlan (internal IR, grows per phase)

```rust
// Phase 1 — implemented
pub enum LogicalPlan {
    TableScan { table_id: u64, schema: Schema },
    Filter    { input: Box<Self>, predicate: BoundExpr },
    Project   { input: Box<Self>, cols: Vec<usize> },
    // Phase 4+: MapExpr, Join, Aggregate — stubs only
}

// BoundExpr — Phase 1 actual implementation
pub enum BoundExpr {
    ColRef(usize),
    LitInt(i64),
    LitFloat(f64),
    // LitStr(String) — TODO Phase 3: add for string literals in UPDATE SET / WHERE
    BinOp(Box<Self>, BinOp, Box<Self>),
    UnaryOp(UnaryOp, Box<Self>),
    IsNull(usize),           // column index (only column IS NULL supported)
    IsNotNull(usize),
}

pub enum BinOp { Add, Sub, Mul, Div, Mod, Eq, Ne, Gt, Ge, Lt, Le, And, Or }
pub enum UnaryOp { Neg, Not }
```

### Public API

```rust
// Actual Phase 1 implementation
pub struct SqlPlanner<'a> {
    client:      &'a GnitzClient,
    schema_name: String,
}

impl<'a> SqlPlanner<'a> {
    pub fn new(client: &'a GnitzClient, schema_name: impl Into<String>) -> Self;
    pub fn execute(&self, sql: &str) -> Result<Vec<SqlResult>, GnitzSqlError>;
}

pub enum SqlResult {
    TableCreated { table_id: u64 },
    ViewCreated  { view_id:  u64 },
    IndexCreated { index_id: u64 },   // Phase 2
    Dropped,
    RowsAffected { count: usize },
    Rows         { schema: Schema, batch: ZSetBatch },
}
```

`SqlPlanner<'a>` uses a lifetime reference rather than `Arc<GnitzClient>`. This avoids
requiring `Arc` in gnitz-capi (which owns `GnitzClient` directly as a Box). The planner
is cheap to construct (one reference + one String) and is discarded after each call.
This pattern scales to all future phases without change.

---

## DML Semantics

### gnitz's fundamental model

gnitz is a **Z-set database**. Every row carries an integer weight:
- weight = +1 : row is present
- weight = -1 : row is retracted (deletion)
- weight > 1  : multiset accumulation (`unique_pk=false` only)

`push(tid, schema, batch)` applies a weighted batch. With `unique_pk=true` (the default),
a push with the same PK as an existing row performs an automatic **upsert**: the server
retracts the old row and inserts the new one.

### DBSP view ordering constraint

**Views only process deltas that arrive after the view is created.** Historical data
is not backfilled. This is fundamental DBSP semantics.

```sql
CREATE VIEW v AS SELECT * FROM t WHERE val > 10;  -- create first
INSERT INTO t VALUES (1, 15);                      -- then push
SELECT * FROM v;                                   -- populated ✓

-- Wrong order: view sees nothing
INSERT INTO t VALUES (1, 15);
CREATE VIEW v AS SELECT * FROM t WHERE val > 10;   -- sees no delta
SELECT * FROM v;                                   -- empty ✗
```

Indices created with `CREATE INDEX` on an already-populated table **are** backfilled
immediately by `IndexEffectHook._backfill_index()` — this is intentional and different
from DBSP views.

### Primary key type constraint

gnitz requires all primary keys to be `U64` or `U128`. SQL `BIGINT PRIMARY KEY` maps to
`I64`, which is rejected by the server. The planner automatically coerces:

```
I8 → U8,  I16 → U16,  I32 → U32,  I64 → U64
```

This coercion applies only to the PK column. Non-PK signed columns retain their type.

### INSERT

`INSERT INTO t (cols) VALUES (r1), (r2), …` maps directly to `push(tid, schema, batch{w=1})`.

Constraints:
- PK column must be provided (no auto-increment in Phase 2; SEQ_TAB exists but is not
  wired to SQL yet).
- With `unique_pk=true` (default): duplicate PK silently replaces the existing row
  (upsert / INSERT OR REPLACE semantics). Document this clearly to users.
- NULL permitted only in nullable columns; enforced by the server.
- `INSERT INTO t SELECT …` is not supported. Use a view.

### UPDATE

`UPDATE t SET col = expr [WHERE predicate]`

gnitz has no native UPDATE. The client implements it as: for each matching row, push a
new version with the same PK and `weight=+1`. With `unique_pk=true` the server retracts
the old row automatically — the client never sends a retraction.

**Sub-cases by WHERE clause:**

| Case | Mechanism | Cost |
|---|---|---|
| `WHERE pk = literal` | seek(tid, pk) → compute new row → push | O(log n) |
| `WHERE indexed_col = literal` | seek_by_index(tid, col_idx, key) → for each match: seek original → compute → push | O(log n + k) |
| `WHERE non_indexed predicate` | scan(tid) → eval predicate client-side → compute → push matching | O(table_size) |
| No WHERE | scan(tid) → compute for all → push all | O(table_size) |

**SET expression evaluation** is done client-side in Rust using `eval_bound_expr()` (a
Rust-native tree walker over `BoundExpr`). When the SET expression references current
column values (e.g., `SET val = val * 2`), the current row must first be read. When
the SET expression is a pure literal, the current row does not need to be fetched.
The planner detects this: if the SET RHS `BoundExpr` contains any `ColRef`, a read is
required; otherwise, it is skipped.

**Multi-table UPDATE** (`UPDATE t1 SET … FROM t1 JOIN t2 ON …`) is not supported.

### DELETE

`DELETE FROM t [WHERE predicate]`

The `client.delete(table_id, schema, pks: &[(u64, u64)])` API sends a batch with
`weight=-1` for each PK. Payload columns are zero-filled (ignored by the server for
deletions). No special IPC flag — standard push with negative weights.

| Case | Mechanism | Cost |
|---|---|---|
| `WHERE pk = literal` | delete(tid, schema, [(pk_lo, pk_hi)]) | O(log n) |
| `WHERE pk IN (v1, v2, …)` | delete(tid, schema, [(v1_lo, v1_hi), …]) | O(k log n) |
| `WHERE indexed_col = literal` | seek_by_index → collect source_pks → delete | O(log n + k) |
| `WHERE non_indexed predicate` | scan → eval predicate → collect PKs → delete | O(table_size) |
| No WHERE (`DELETE FROM t`) | scan all PKs → delete all | O(table_size) |

There is no TRUNCATE at the server level. Document the O(table_size) cost of `DELETE FROM t`.

**Multi-table DELETE** is not supported.

### SELECT

**gnitz's model is declare-then-read, not query-on-demand.** The right usage pattern:

```sql
CREATE VIEW active_orders AS                    -- 1. declare computation once
    SELECT o.id, o.amount FROM orders o
    WHERE o.status = 'active';
INSERT INTO orders VALUES (…);                  -- 2. push changes continuously
SELECT * FROM active_orders;                    -- 3. read (just a scan — always current)
```

**Supported SELECT forms (Phase 1+):**

- `SELECT [cols] FROM t` → `scan(tid)`, project columns in Rust if needed
- `SELECT * FROM t WHERE pk = 42` → `seek(tid, pk=42)`, O(log n)
- `SELECT * FROM t WHERE indexed_col = 42` → `seek_by_index(tid, col_idx=N, key=42)`, O(log n + k) — Phase 2
- `SELECT * FROM t LIMIT n` → `scan(tid)`, truncate client-side

**Non-indexed WHERE in direct SELECT is rejected** with a clear error:

> WHERE on non-primary-key column not supported in direct SELECT; use CREATE VIEW
> for server-side filtering, or CREATE INDEX on the column first.

`SELECT … JOIN …` and `SELECT … GROUP BY …` are also rejected: use `CREATE VIEW`.

Z-set weights in output: for `unique_pk=true` tables (default), all weights are 1
and invisible. For multiset tables, each row is emitted once (weight is not surfaced
to SQL users unless they use a non-standard `SELECT *, _weight` extension).

---

## Indices

### What already exists (no changes needed)

**`IndexCircuit`** (`catalog/index_circuit.py`): descriptor for one secondary index.
Schema: 2-column Z-set `(index_key PK, source_pk payload)`.
- `index_key` = the indexed column value, promoted to U64 or U128
- `source_pk` = the original row's PK (U64 or U128)

**`IdxTab`** (system table ID=5): persists index metadata.
Columns (1-indexed from PK): `index_id(PK), owner_id, owner_kind, source_col_idx,
name, is_unique, cache_directory`.

**`IndexEffectHook`** (`catalog/hooks.py`): fires on every IdxTab insert/retract.
On insert: creates `IndexCircuit` via `_make_index_circuit()`, immediately backfills
from current table state via `_backfill_index()`, appends to `family.index_circuits`,
registers in `registry._index_by_id`.

**Stage 3 of `ingest_to_family`** (`catalog/registry.py:282–295`): after every push
to a user table, iterates `family.index_circuits` and calls
`circuit.table.ingest_projection(batch, col_idx, col_type, payload_accessor, is_unique)`
for each index. Secondary index maintenance is fully automatic and transparent.

The Python-side index infrastructure requires **zero changes** for Phase 2.

### What `CREATE INDEX` maps to

```sql
CREATE INDEX idx_name ON table_name (col_name);
CREATE UNIQUE INDEX idx_name ON table_name (col_name);
DROP INDEX idx_name;
```

The SQL planner:
1. Resolves `table_name` + `col_name` → `(table_id, col_idx)` via `binder.resolve()`.
2. Calls `client.alloc_index_id()` → gets `index_id` from server.
3. Builds one IdxTab row: `(index_id, table_id, OWNER_KIND_TABLE, col_idx, index_name, is_unique, "")`.
4. Pushes the row to IdxTab via `client.push(IDX_TAB, &idx_schema, &batch)`.
5. `IndexEffectHook` fires server-side, creates the circuit, backfills. Done.

No new RPython code required for integer-typed columns. The Rust client needs
`alloc_index_id()`, `create_index()`, and `drop_index_by_name()` (analogous to existing
table methods; the full index name includes the schema prefix, so no schema_name param needed).

**Index naming convention** (must match Python's `make_secondary_index_name()`):
`"{schema_name}__{table_name}__idx_{col_name}"` (double-underscore separators, `idx_` prefix).
The planner constructs this name before pushing to IdxTab.

### Column type support

| Type | Index key type | Range queries |
|---|---|---|
| I8, U8, I16, U16, I32, U32, I64, U64 | promoted to U64 | Yes — integer order preserved |
| U128 | stays U128 | Yes |
| F32, F64 | blocked by `get_index_key_type()` — Phase B | — |
| STRING | blocked by `get_index_key_type()` — Phase B | — |

Attempting to create an index on a float or string column raises `GnitzSqlError::Unsupported`
(the server would raise `LayoutError`; the planner should pre-validate to give a better
error message).

### FLAG_SEEK_BY_INDEX: the seek-by-index IPC operation

**Design rationale:** The existing `FLAG_SEEK` uses slots p5/p6 for the U128 key.
A seek-by-index additionally needs a `col_idx` parameter. The header has no spare
bytes — it is fully packed at 96 bytes. However, slot p4 (`data_pk_index`) is a dead
field: confirmed by code audit to be written by the Rust client but never read from
incoming requests by the Python server, and written by the Python server but never
consumed from responses by the Rust client. Repurposing p4 for `col_idx` in
`FLAG_SEEK_BY_INDEX` fills a genuinely unused slot — no semantic conflict with any
existing flag, since `FLAG_SEEK_BY_INDEX` is mutually exclusive with `FLAG_PUSH`.

```
FLAG_SEEK_BY_INDEX = 256  ✅ DONE

Request header:
  target_id       = original table ID (not the index ID)
  flags           = FLAG_SEEK_BY_INDEX
  p4              = col_idx  (which column to seek the index on)
  p5 (seek_pk_lo) = key_lo   (lower 64 bits of the 128-bit index key)
  p6 (seek_pk_hi) = key_hi   (upper 64 bits)
```

**Server-side handler** (`executor.py`), ~25 lines:

```python
if intmask(flags) & ipc.FLAG_SEEK_BY_INDEX:
    col_idx = intmask(payload.seek_col_idx)
    result  = self._seek_by_index(target_id, col_idx,
                                  payload.seek_idx_key_lo, payload.seek_idx_key_hi)
    resp_schema = (result._schema if result is not None
                   else self.engine.registry.get_by_id(target_id).schema)
    ipc.send_batch(fd, target_id, result, STATUS_OK, "", client_id, schema=resp_schema)
    if result is not None:
        result.free()
    return 0

def _seek_by_index(self, table_id, col_idx, key_lo, key_hi):
    family = self.engine.registry.get_by_id(table_id)
    circuit = None
    for c in family.index_circuits:
        if c.source_col_idx == col_idx:
            circuit = c
            break
    if circuit is None:
        return None
    key = ipc.r_uint128((ipc.r_uint64(key_hi) << 64) | ipc.r_uint64(key_lo))
    idx_cursor = circuit.table.create_cursor()
    idx_cursor.seek(key)
    if not idx_cursor.is_valid() or idx_cursor.key() != key:
        return None
    # Read source_pk from the index row (column 1 = source_pk payload)
    source_pk = idx_cursor.get_accessor().get_u128(1)
    src_lo = intmask(source_pk & ipc.r_uint64(-1))
    src_hi = intmask(source_pk >> 64)
    return self._seek_family(table_id, src_lo, src_hi)
```

The server resolves the full pipeline (index key → source PK → original row) in a
**single roundtrip**. The response is a batch of full rows from the original table with
the same schema. The client needs no knowledge of index internals.

**For non-unique indices** where multiple source_pks share the same index_key: the
above implementation returns the first match only (the behavior of `_seek_family`).
Full multi-match support (iterating all index entries with the same key, emitting
multiple rows) is **not implemented and deferred to a future phase**. For unique
indices — the common case — the single-match path is correct and complete.

### Planner integration for index-aware SELECT/UPDATE/DELETE

The `Binder` caches index lookups. New method:

```rust
// gnitz-core/src/client.rs
pub fn find_index_for_column(
    &self,
    table_id: u64,
    col_idx:  usize,
) -> Result<Option<u64>, ClientError>
```

Scans `IDX_TAB` (system table ID=5), filters rows where `owner_id == table_id` and
`source_col_idx == col_idx`, returns the `index_id` of the first match (or `None`).
O(n_indices) scan; expected to be very small.

`Binder` calls this once per WHERE column reference that is not the PK, and caches
`(table_id, col_idx) → Option<index_id>`. The planner then:

- If `index_id = Some(iid)`: use `client.seek_by_index(table_id, col_idx, key_lo, key_hi)`
- If `index_id = None`: reject non-indexed WHERE in SELECT; full-scan in UPDATE/DELETE

The index key is promoted from the SQL literal using the same rules as
`get_index_key_type()`: signed integers are zero-extended to U64, and passed as
`(key as u64, 0u64)` in the `(seek_pk_lo, seek_pk_hi)` pair.

---

## Gap Analysis for CREATE VIEW (future phases)

### 1. General equi-join operator — Phase 5

Current joins match on U128 PK. SQL `JOIN ON t1.a = t2.b` requires matching on
arbitrary columns. New operators: `op_equijoin_delta_trace` + `op_equijoin_delta_delta`
in RPython. Parameters: `left_key_cols[], right_key_cols[]`. Implementation: Murmur3-hash
the key columns into a U128 and delegate to the existing join kernel.

Rust: new `CircuitBuilder::equijoin(delta, trace_tid, left_cols, right_cols)`.

### 2. Computed projections (`SELECT a + b AS c`) — Phase 4

Current `op_map` supports column reordering via a `ScalarFunction` callback. Need a
new operator `op_map_expr` that takes one `ExprProgram` per output column and runs
`eval_expr()` for each. This is a new opcode — extending `op_map` would conflate two
very different parameter formats and make the circuit graph harder to evolve.

The output schema must be inferred from expression types during planning (type inference
in the Rust `Binder`). The PK column must remain at output position 0.

### 3. String comparison in ExprVM — Phase 5

ExprVM (31 opcodes, Phase 1 DONE) has no string opcodes. String predicates in views
(`WHERE name = 'Alice'`, `ON t1.name = t2.name`) require new opcodes.

UPDATE/DELETE WHERE filtering is done **client-side** in the Rust evaluator and does not
use ExprVM. String opcodes are needed only for server-side view predicates. They are
correctly placed in Phase 5 (alongside equijoins, where `ON t1.name = t2.name` is the
primary use case).

### 4. LEFT OUTER JOIN — Phase 6

No new RPython operators. Composite circuit: equijoin ∪ (anti-join → null-fill map).
The planner emits this multi-node pattern automatically.

### 5. AVG aggregate — Phase 6

`UniversalAccumulator` supports COUNT, SUM, MIN, MAX. Add `AGG_AVG = 5`: maintains
`(sum, count)` pair, emits `sum/count` on finalise.

---

## ExprBuilder (DONE — prerequisite satisfied)

All 31 VM opcodes are present and the `load_const` split-encoding bug is fixed.

| Group | Opcodes | Rust ExprBuilder |
|---|---|---|
| Column loads | `LOAD_COL_INT` (1), `LOAD_COL_FLOAT` (2) | ✅ |
| Constant load | `LOAD_CONST` (3) — full i64 via lo/hi split | ✅ |
| Integer arithmetic | `INT_ADD` (4)–`INT_NEG` (9) | ✅ |
| Float arithmetic | `FLOAT_ADD`–`FLOAT_NEG` (10–14) | ✅ |
| Integer comparison | `CMP_EQ`–`CMP_LE` (15–20) | ✅ |
| Float comparison | `FCMP_EQ`–`FCMP_LE` (21–26) | ✅ |
| Boolean | `BOOL_AND/OR/NOT` (27–29) | ✅ |
| Null checks | `IS_NULL` (30), `IS_NOT_NULL` (31) | ✅ |

---

## Phased Plan

---

### Phase 1 — Foundation: DDL + INSERT + simple SELECT + PK seek ✅ DONE

**Commit**: `0de22e3` — 24 files changed, 1531 insertions.
**Tests**: 11 integration tests in `rust_client/gnitz-py/tests/test_sql.py`
(TestSqlDdl×2, TestSqlInsert×2, TestSqlSelect×5, TestSqlCreateView×2).
All 103 total tests pass.

**Goals achieved:**
- `CREATE TABLE`, `DROP TABLE`, `DROP VIEW` ✅
- `INSERT INTO t VALUES (…)` ✅
- `SELECT * FROM t` / `SELECT * FROM view` ✅
- `SELECT * FROM t WHERE pk = 42` (O(log n) PK seek, FLAG_SEEK=128) ✅
- `SELECT * FROM t LIMIT n` ✅
- `CREATE VIEW v AS SELECT cols FROM t [WHERE predicate]` ✅

**Key implementation notes carried forward:**
- `SqlPlanner<'a>` uses `&'a GnitzClient` (not `Arc`). Per-call construction pattern.
- `gnitz_execute_sql(conn, sql, schema, out_id)` — no `sql_len`, has `schema`.
- `execute_sql(sql, schema_name="public")` in Python, schema first in PyO3.
- PK coercion: I8→U8, I16→U16, I32→U32, I64→U64 at CREATE TABLE time.
- `build_projection` always places source PK at output position 0.
- `apply_projection` deduplicates when mixing `SELECT *, col` patterns.
- `resolve_table_or_view_id`: 3 scans on table path (SCHEMA+COL+TABLE), 4 on view
  path (SCHEMA+COL+TABLE+VIEW). COL_TAB scanned once, shared between branches.

---

### Phase 2 — Protocol cleanup + secondary index infrastructure + indexed SELECT ✅ DONE

**Commit**: `a11c02b` — Secondary index infrastructure: CREATE/DROP INDEX, indexed SELECT, multi-worker seek.

**Goals achieved:**
- `CREATE [UNIQUE] INDEX idx ON t (customer_id)` ✅
- `DROP INDEX idx` ✅
- `SELECT * FROM t WHERE customer_id = 42` — O(log n) via index ✅
- `SELECT * FROM t WHERE customer_id = 42 AND status = 'active'` — O(log n) + client-side filter ✅
- Protocol cleanup/documentation improvements ✅

**Remaining TODOs from Phase 2 (low priority, no user-visible impact):**
- Explicit `elif intmask(flags) == ipc.FLAG_SCAN:` dispatch in executor (currently falls through)
- `set_seek_key`/`seek_key_lo`/`seek_key_hi` accessor convenience methods in header.rs

#### 2A — Protocol cleanup (gnitz-protocol + gnitz-core, ipc.py)

**gnitz-protocol/src/header.rs:**
- ✅ DONE: `pub const FLAG_SEEK_BY_INDEX: u64 = 256;`
- ✅ DONE: `pub const FLAG_ALLOCATE_INDEX_ID: u64 = 512;`
- ✅ DONE: accessor methods `set_seek_by_index()`, `seek_col_idx()`,
  `seek_idx_key_lo()`, `seek_idx_key_hi()`
- ✅ DONE: wire layout doc comment updated with full slot table
- TODO: add `set_seek_key()`, `seek_key_lo()`, `seek_key_hi()` convenience wrappers
  for FLAG_SEEK (callers still write `seek_pk_lo`/`seek_pk_hi` directly)
- TODO: test roundtrip via accessor methods

**gnitz/server/ipc.py:**
- ✅ DONE: header comment updated with authoritative slot assignment table
- ✅ DONE: `FLAG_SCAN = 0`, `FLAG_SEEK_BY_INDEX = 256`, `FLAG_ALLOCATE_INDEX_ID = 512`
- ✅ DONE: `OFF_P4 = 64` (field renamed from `OFF_DATA_PK_INDEX`)
- ✅ DONE: `OFF_SEEK_IDX_COL = OFF_P4` semantic alias
- ✅ DONE: `send_batch()` no longer writes `data_pk_index = schema.pk_index`
- ✅ DONE: `_recv_and_parse()` conditionally parses `seek_col_idx` only when
  `FLAG_SEEK_BY_INDEX` is set; `seek_pk_lo/hi` parsed for FLAG_SEEK_BY_INDEX or FLAG_SEEK
- ✅ DONE: `seek_col_idx` added to `IPCPayload`
- TODO: executor still uses `else:` fallthrough for scan/push — add explicit
  `elif intmask(flags) == ipc.FLAG_SCAN:` check

**gnitz/server/executor.py:**
- ✅ DONE: `FLAG_SEEK_BY_INDEX` handler + `_seek_by_index()` method
- ✅ DONE: `FLAG_ALLOCATE_INDEX_ID` handler
- TODO: replace `else: # scan/push fallthrough` with explicit
  `elif intmask(flags) == ipc.FLAG_SCAN:` for clarity
- The `FLAG_ALLOCATE_INDEX_ID` handler:
  ```python
  if payload.flags & ipc.FLAG_ALLOCATE_INDEX_ID:
      new_id = self.engine.registry.allocate_index_id()
      self.engine._advance_sequence(sys.SEQ_ID_INDICES, new_id - 1, new_id)
      ipc.send_batch(fd, new_id, None, STATUS_OK, "", client_id)
      return 0
  ```

#### 2B — gnitz-core: new IPC operations and client methods

**gnitz-core/src/ops.rs:**
- Add `pub const IDX_TAB: u64 = 5;` alongside existing system table constants.
- Add `pub fn alloc_index_id(conn: &Connection) -> Result<u64, ClientError>`:
  sends `FLAG_ALLOCATE_INDEX_ID` with `target_id=0`; returns `msg.header.target_id`.
  Mirrors existing `alloc_table_id()`.
- Add `pub fn seek_by_index(conn, table_id, col_idx, key_lo, key_hi)
  -> Result<(Option<Schema>, Option<ZSetBatch>), ClientError>`:
  sends `FLAG_SEEK_BY_INDEX` using `hdr.set_seek_by_index(col_idx as u64, key_lo, key_hi)`.
  Response parsing identical to `seek()`.

**gnitz-core/src/client.rs:**
- Add `pub fn alloc_index_id(&self) -> Result<u64, ClientError>` — wrapper.
- Add `pub fn seek_by_index(&self, table_id, col_idx, key_lo, key_hi)`  — wrapper.
- Add `pub fn find_index_for_column(&self, table_id: u64, col_idx: usize)
  -> Result<Option<u64>, ClientError>`:
  Scans IDX_TAB. Iterates rows, finds the first where `owner_id == table_id`
  and `source_col_idx == col_idx`. Returns its `index_id`, or `None`.
  Column layout of IdxTab: `(index_id[0], owner_id[1], owner_kind[2],
  source_col_idx[3], name[4], is_unique[5], cache_directory[6])`.
- Add `pub fn create_index(&self, schema_name, table_name, col_name, index_name,
  is_unique) -> Result<u64, ClientError>`:
  1. `binder.resolve(table_name)` → `(table_id, schema)`
  2. Find `col_idx` from schema columns (case-insensitive match on `col_name`)
  3. Validate column type via `get_index_key_type_from_typecode()` (replicate the
     Python logic: reject F32/F64/STRING with `GnitzSqlError::Unsupported`)
  4. `alloc_index_id()` → `index_id`
  5. Build and push one IdxTab row: `(index_id, table_id, OWNER_KIND_TABLE=0,
     col_idx, index_name, is_unique as u64, "")` to `IDX_TAB`
  6. Return `index_id`

  Note: `index_name` must use the Python convention
  `"{schema_name}__{table_name}__idx_{col_name}"`. The Rust planner constructs this.
- Add `pub fn drop_index_by_name(&self, index_name: &str) -> Result<(), ClientError>`:
  (no `schema_name` parameter — the full index name already includes the schema prefix)
  1. Scan IDX_TAB, find the row where `name == index_name`
  2. If not found: `ClientError::ServerError("Index not found")`
  3. Build a retract batch (weight=-1) with the same row data, push to IDX_TAB
  4. `IndexEffectHook` fires and unregisters the circuit

**gnitz-core/src/lib.rs:**
- Re-export `alloc_index_id`, `seek_by_index`, `IDX_TAB` from `ops`.

**gnitz-core/src/connection.rs:**
- Add `roundtrip_seek_by_index(table_id, col_idx, key_lo, key_hi)` using
  `hdr.set_seek_by_index(col_idx as u64, key_lo, key_hi)`.
- Rename direct field accesses in existing `roundtrip_seek` to use accessor methods:
  `hdr.set_seek_key(pk_lo, pk_hi)` instead of `hdr.seek_pk_lo = pk_lo`.

#### 2C — gnitz-sql: SQL planner additions

**gnitz-sql/src/binder.rs:**
- Extend `Binder` with an `index_cache: HashMap<(u64, usize), Option<u64>>` mapping
  `(table_id, col_idx) → Option<index_id>`.
- Add `pub fn find_index(&mut self, table_id, col_idx) -> Result<Option<u64>, GnitzSqlError>`:
  checks cache first, then calls `client.find_index_for_column()`.

**gnitz-sql/src/planner.rs:**
- Handle `Statement::CreateIndex(ci)` in `execute_statement`:
  - Extract `table_name`, `col_name` (only single-column supported; reject multi-column
    with `Unsupported`), `is_unique`, `index_name` (or generate from naming convention)
  - Call `client.create_index(schema_name, table_name, col_name, full_index_name, is_unique)`
  - Return `SqlResult::IndexCreated { index_id }`
- Handle `Statement::Drop { object_type: ObjectType::Index, names, .. }`:
  - For each name: construct full name `"{schema}__{table}__idx_{col}"`, call `client.drop_index_by_name(full_name)`
  - Return `SqlResult::Dropped`
- Add `SqlResult::IndexCreated { index_id: u64 }` variant to `lib.rs`.

**gnitz-sql/src/dml.rs:**
- Extend `execute_select`: after the `WHERE pk = literal` path, before the "reject
  non-indexed WHERE" error, add an `WHERE indexed_col = literal` path:
  1. Identify the WHERE column (must be an `Eq` binary op with a `ColRef` and `LitInt`)
  2. Call `binder.find_index(table_id, col_idx)` → `Option<index_id>`
  3. If `Some`: promote literal to U64 key, call `client.seek_by_index(table_id, col_idx, key_lo, 0)`
  4. If `None`: return `GnitzSqlError::Unsupported("…no index on column…")`
  5. Apply any additional (non-index) predicates from the WHERE clause client-side
     (AND of remaining sub-expressions after extracting the index predicate)

#### 2D — gnitz-capi + gnitz-py

**gnitz-capi/src/lib.rs:**
- Add `gnitz_seek_by_index(conn, table_id, col_idx: u32, key_lo, key_hi, out_batch)`:
  low-level direct access to the index seek operation for non-SQL callers.
- `gnitz_execute_sql` already handles `CREATE INDEX` / `DROP INDEX` via `SqlPlanner`;
  `out_id` receives `index_id` for `IndexCreated`, 0 for `Dropped`.

**gnitz-py/src/lib.rs + gnitz-py/python/gnitz/_client.py:**
- Add `PyGnitzClient::seek_by_index(py, table_id, col_idx, key_lo, key_hi) -> PyObject`.
- Add `Connection.seek_by_index(table_id, col_idx, key_lo, key_hi=0) -> ScanResult`.
- `execute_sql` result list gains `{"type": "IndexCreated", "index_id": N}` entries.

#### 2E — Tests

**`rust_client/gnitz-py/tests/test_indices.py`** (new file, online tests):

```
TestProtocolCleanup
  test_header_slot_table         verify FLAG_SEEK=128, FLAG_SEEK_BY_INDEX=256 constants
  test_alloc_index_id            alloc_index_id() returns a valid u64

TestIndexDdl
  test_create_drop_index         CREATE TABLE; CREATE INDEX; DROP INDEX
  test_create_unique_index       CREATE UNIQUE INDEX; verify is_unique=1 in IdxTab
  test_create_index_on_view      CREATE INDEX on a view (owner_kind=1 path)
  test_create_index_float_col    expects GnitzSqlError for float column
  test_create_index_string_col   expects GnitzSqlError for string column
  test_drop_nonexistent_index    expects error

TestIndexSeek
  test_seek_by_index_unique      seek_by_index on unique idx → 1 row
  test_seek_by_index_not_found   key not in index → empty result
  test_seek_by_index_after_insert index is updated by subsequent INSERT
  test_seek_by_index_after_delete index is updated by DELETE (Phase 3)

TestIndexSql
  test_select_where_indexed_col  SELECT * FROM t WHERE customer_id = 42
  test_select_where_pk           SELECT * FROM t WHERE pk = 42 (regression)
  test_select_nonindexed_rejects SELECT * FROM t WHERE val = 5 → error
  test_select_where_extra_filter SELECT * FROM t WHERE customer_id = 42 AND status = 1
                                 (index seek + client-side filter)
```

Run: `cd rust_client/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_indices.py -v --tb=short`

**Build order for Phase 2:**
1. `gnitz-protocol` — FLAG_SEEK_BY_INDEX, FLAG_ALLOCATE_INDEX_ID, accessor methods
2. `gnitz-core` — connection.rs, ops.rs, client.rs, lib.rs
3. `gnitz/server/ipc.py` + `executor.py` — protocol cleanup + new handlers + server rebuild
4. `gnitz-sql` — binder.rs, planner.rs, dml.rs, lib.rs
5. `gnitz-capi` — gnitz_seek_by_index()
6. `gnitz-py` — seek_by_index() + execute_sql result type
7. Tests

Verify: `cd rust_client && cargo build -p gnitz-py && make server`

---

### Phase 3 — UPDATE + DELETE

**Goals (Phase 3 scope):**
- `UPDATE t SET val = 42 WHERE pk = 1` — O(log n) via PK seek
- `UPDATE t SET amount = amount * 2 WHERE customer_id = 7` — O(log n) via unique index; falls back to O(table_size) full scan for non-unique index (see §3B)
- `UPDATE t SET active = 0 WHERE val < 0` — O(table_size) full scan + client-side filter
- `UPDATE t SET flag = 1` (no WHERE) — O(table_size) full scan
- `DELETE FROM t WHERE pk = 42` — O(log n) via PK
- `DELETE FROM t WHERE customer_id = 7` — O(log n) via unique index; falls back to full scan for non-unique index (see §3B)
- `DELETE FROM t WHERE val < 0` — O(table_size) full scan + client-side filter
- `DELETE FROM t` — O(table_size) full wipe

Phase 3 requires **no new IPC operations** and **no new RPython code**.
All filtering and computation happen client-side in Rust.

**Correctness invariant:** UPDATE relies on `unique_pk=True` being set for all
SQL-created tables (confirmed: `planner.rs` passes `true` to `create_table`). The
server's Stage 0 unique-PK enforcement auto-retracts the old row when a new row
with the same PK is pushed with weight=+1. Workers receive pre-validated batches.

**Scope note — full-scan WHERE in SELECT:** `execute_select` currently returns
`GnitzSqlError::Unsupported` for `WHERE` on non-indexed, non-PK columns (directs
users to CREATE INDEX or CREATE VIEW). Phase 3 does NOT change SELECT behaviour.
UPDATE/DELETE add full-scan WHERE support because they are write operations and
returning an error is more disruptive than a slow scan. The asymmetry is intentional.

**Scope note — multi-row index seeks:** `seek_by_index` returns at most one row
(the first match). Multi-match index iteration is deferred to `FLAG_RANGE_SEEK`
(future phase). Consequence: for a **non-unique** index, the indexed fast path
cannot be used safely — Phase 3 falls back to full scan. For a **unique** index
(declared `CREATE UNIQUE INDEX`), k=1 is guaranteed and `seek_by_index` is correct.
The `binder.find_index` result must carry the `is_unique` flag (already stored in
`IdxTab`) so the dispatcher can choose the right path.

#### 3A — Two evaluators in `dml.rs`

`dml.rs` already contains `eval_expr` (returns `i64`) and `eval_pred_row` (wraps it
for boolean use) for SELECT's residual filter. Phase 3 adds a second evaluator for the
SET clause, which needs to produce typed column values rather than just booleans.

**Existing (keep unchanged — used for WHERE predicate evaluation):**
```rust
// Already in dml.rs — do not change signature
fn eval_expr(expr: &BoundExpr, batch: &ZSetBatch, i: usize, schema: &Schema)
    -> Result<i64, GnitzSqlError>

fn eval_pred_row(pred: &BoundExpr, batch: &ZSetBatch, i: usize, schema: &Schema)
    -> Result<bool, GnitzSqlError>
```
The existing `eval_expr` handles `ColRef` (integer-typed columns only), `LitInt`,
`BinOp`, `UnaryOp`, `IsNull`/`IsNotNull`. It explicitly returns `Unsupported` for
`LitFloat` and string columns — this is acceptable for WHERE predicates in Phase 3
(float/string WHERE deferred; users get a clear error).

**New — add to `dml.rs` (used for SET clause evaluation):**
```rust
#[derive(Debug)]
enum ColumnValue { Int(i64), Float(f64), Str(String), Null }

/// Evaluate a SET-clause BoundExpr against row `row_idx` in `batch`.
/// The `target_type` is the TypeCode of the column being assigned — used to
/// dispatch ColRef reads and to validate arithmetic types at runtime.
fn eval_set_expr(
    expr:        &BoundExpr,
    batch:       &ZSetBatch,
    row_idx:     usize,
    schema:      &Schema,
    target_type: TypeCode,
) -> Result<ColumnValue, GnitzSqlError>
```

`eval_set_expr` walks the `BoundExpr` tree:
- `ColRef(i)` — reads from `batch.columns[i]` at `row_idx`; dispatches on
  `schema.columns[i].type_code` to produce `Int`, `Float`, or `Str`.
- `LitInt(v)` → `ColumnValue::Int(v)`
- `LitFloat(v)` → `ColumnValue::Float(v)`
- `LitStr` — **not yet in `BoundExpr`; string SET deferred** (see prerequisites below)
- `BinOp` — dispatches on operand types; `Int op Int → Int`, `Float op Float → Float`;
  mixed Int/Float promotes to Float. Comparison operators return `Int(0 or 1)`.
- `UnaryOp` — Neg on Int/Float; Not on Int.

**Prerequisites for string SET/WHERE support (all in Phase 3):**

1. Add `LitStr(String)` to `BoundExpr` in `logical_plan.rs`.
2. Handle `Value::SingleQuotedString` / `Value::DoubleQuotedString` in
   `binder.rs` `bind_expr` (currently only handles identifiers and numbers).
3. Handle `LitStr` in `eval_set_expr` above → `ColumnValue::Str`.
4. Handle `LitStr` in `eval_expr` (WHERE predicate) for string equality: return
   `Unsupported` for non-equality string ops (ordering); return `1` or `0` for
   string `Eq`/`Ne` by reading `ColData::Strings` at row `i`.
5. In `write_set_columns` (new helper, see §3B): handle `ColumnValue::Str` →
   push `Some(s)` into `ColData::Strings`.

These steps are small and self-contained. String WHERE filtering (e.g.,
`WHERE status = 'inactive'`) does not require any new IPC — it is purely
client-side evaluation over the result of `client.scan()`.

#### 3B — gnitz-sql/src/dml.rs additions

**Helper: `write_set_columns`**

Building the new-row batch for UPDATE requires filling all non-PK columns,
either from the current row (via `eval_set_expr` with `ColRef`) or from the SET
expression (constant). Extract this into a shared helper called by all UPDATE paths:

```rust
/// Build a one-row ZSetBatch for the updated row.
/// `current`: full current row from seek/scan.
/// `assignments`: (col_idx, BoundExpr) pairs from the SET clause.
/// Non-assigned columns are copied from `current`.
fn write_set_columns(
    current:     &ZSetBatch,
    row_idx:     usize,
    assignments: &[(usize, BoundExpr)],
    schema:      &Schema,
) -> Result<ZSetBatch, GnitzSqlError>
```

The function always reads the current row for every non-PK column and then
overlays SET assignments. This is simpler and correct in all cases — there is
no "skip seek" optimisation (see below).

**No "skip seek" optimisation.** An earlier version of this plan proposed skipping
the current-row seek when no SET expression contains a `ColRef`. This is incorrect:
building a complete new-row batch requires all non-PK column values, which come from
the current row for any column not covered by the SET clause. Since tables typically
have more than one non-PK column, skipping the seek would zero-fill unset columns.
The only safe case (SET covers every non-PK column with constants) is too narrow to
be worth a special path. Always seek/scan before building the new batch.

---

**`execute_update(client, schema_name, update, binder)`:**

1. Resolve source table → `(table_id, schema)` via `binder.resolve()`.
2. Reject UPDATE on PK column: `GnitzSqlError::Unsupported("cannot UPDATE primary key")`.
3. Bind SET assignments: `Vec<(col_idx, BoundExpr)>`. Resolve column names against
   `schema`; error on unknown column name.
4. Bind WHERE predicate (optional) → `Option<BoundExpr>`.
5. Dispatch by WHERE pattern (reuse helpers already in `dml.rs`):
   - **PK equality** (`WHERE pk = literal`, via `try_extract_pk_seek`):
     - `client.seek(table_id, pk_lo, pk_hi)` → `(schema_opt, batch_opt)`.
     - If `batch_opt` is `None`: return `SqlResult::RowsAffected { count: 0 }`.
     - `write_set_columns(current, 0, &assignments, &schema)` → `new_batch`.
     - `client.push(table_id, &schema, &new_batch)`.
     - Return `SqlResult::RowsAffected { count: 1 }`.
   - **Unique-indexed column equality** (`WHERE col = literal`, index found AND
     `index.is_unique`; via `try_extract_index_seek`):
     - `client.seek_by_index(table_id, col_idx, key_lo, key_hi)` → current row.
     - If `None`: return `SqlResult::RowsAffected { count: 0 }`.
     - `write_set_columns` → push. Return `count: 1`.
   - **Full scan (all other cases, including non-unique indexed columns)**:
     - `client.scan(table_id)` → all rows.
     - For each row: evaluate WHERE predicate with `eval_pred_row` (skip if false).
     - For matching rows: `write_set_columns` → accumulate into push batch.
     - `client.push(table_id, &schema, &all_new_rows)`.
     - Return `SqlResult::RowsAffected { count }`.
6. Return `SqlResult::RowsAffected { count }`.

**`execute_delete(client, schema_name, delete, binder)`:**

1. Resolve source table → `(table_id, schema)`.
2. Bind WHERE predicate (optional).
3. Dispatch by WHERE pattern:
   - **PK equality** (`WHERE pk = literal`):
     `client.delete(table_id, &schema, &[(pk_lo, pk_hi)])`.
     Note: `client.delete` zero-fills non-PK columns — correct because ZSet deletion
     uses only the PK key. No seek needed to verify existence; if the row is absent,
     the negative-weight push is a no-op in the ZSet.
   - **PK IN (list)** (`WHERE pk IN (a, b, c)`):
     `client.delete(table_id, &schema, &pks_vec)`.
   - **Unique-indexed column equality** (`WHERE col = literal`, unique index found):
     `client.seek_by_index(table_id, col_idx, key_lo, key_hi)` → read `(pk_lo, pk_hi)`
     from result batch → `client.delete(table_id, &schema, &[(pk_lo, pk_hi)])`.
   - **Full scan / non-unique indexed / no WHERE**:
     `client.scan(table_id)` → apply `eval_pred_row` (if WHERE present) → collect
     `(pk_lo, pk_hi)` pairs → `client.delete(table_id, &schema, &all_pks)`.
4. Return `SqlResult::RowsAffected { count }`.

**Index path: unique vs non-unique.** `binder.find_index` calls `client.find_index_for_column`,
which returns the index entry from `IdxTab`. The `is_unique` field (already stored in
`IdxTab`) determines which path to take. Add a helper:

```rust
fn try_extract_unique_index_seek(
    expr:     &Expr,
    schema:   &Schema,
    binder:   &mut Binder<'_>,
    table_id: u64,
) -> Result<Option<(usize, u64, u64)>, GnitzSqlError>
```

Same logic as `try_extract_index_seek` but only returns `Some` when the found index
has `is_unique = true`. Non-unique indexed columns fall through to full scan.

**`planner.rs`:** Add to the `match stmt` block:
```rust
Statement::Update(_) => dml::execute_update(client, schema_name, stmt, &mut binder),
Statement::Delete(_) => dml::execute_delete(client, schema_name, stmt, &mut binder),
```

#### 3C — Tests

**`rust_client/gnitz-py/tests/test_dml.py`** — extend the existing file (already has
5 raw-client push/scan/delete tests; preserve them and add the SQL cases below):

```
TestUpdateSQL
  test_update_pk_literal_set        UPDATE t SET val = 99 WHERE id = 1
  test_update_pk_expr_set           UPDATE t SET val = val * 2 WHERE id = 1
  test_update_unique_index_where    UPDATE t SET val = 0 WHERE category_id = 5
                                    (category_id has UNIQUE INDEX)
  test_update_nonunique_index_scan  UPDATE t SET val = 0 WHERE tag_id = 5
                                    (tag_id has non-unique INDEX → full scan, k>1 rows)
  test_update_full_scan_predicate   UPDATE t SET active = 0 WHERE val < 0
  test_update_no_where              UPDATE t SET flag = 1 (all rows)
  test_update_pk_column_rejects     UPDATE t SET id = 999 → GnitzSqlError::Unsupported
  test_update_row_not_found         UPDATE WHERE pk = 99999 (not present) → count=0

TestDeleteSQL
  test_delete_pk_eq                 DELETE FROM t WHERE id = 1
  test_delete_pk_in                 DELETE FROM t WHERE id IN (1, 2, 3)
  test_delete_unique_index          DELETE FROM t WHERE category_id = 5 (unique index)
  test_delete_nonunique_index_scan  DELETE FROM t WHERE tag_id = 5 (non-unique → full scan)
  test_delete_full_scan             DELETE FROM t WHERE val < 0
  test_delete_no_where              DELETE FROM t (full wipe)
  test_delete_updates_index         delete a row, verify seek_by_index no longer returns it
  test_update_view_reflects         UPDATE followed by SELECT on a view shows new value
```

---

### Phase 4 — Computed projections in CREATE VIEW

**Goals:**
- `CREATE VIEW v AS SELECT id, a + b AS total FROM t`
- `CREATE VIEW v AS SELECT id, price * 1.1 AS adjusted FROM t`
- `CREATE VIEW v AS SELECT id, CASE WHEN val > 0 THEN 1 ELSE 0 END AS flag FROM t`
  (CASE WHEN is a later addition; Phase 4 covers arithmetic expressions)

Phase 4 requires new RPython code (new operator) and new circuit graph encoding.

#### 4A — RPython: op_map_expr

Add a new operator **`op_map_expr`** (distinct from `op_map` which does column reordering
via a `ScalarFunction`). The parameter format in the circuit graph:

```
Node type: OP_MAP_EXPR (opcode 22)
Parameters:
  - input_node_id: u64
  - n_output_cols: u64
  - For each output column i in 0..n_output_cols:
    - col_type: u64 (TypeCode of the output column)
    - expr_bytecode_len: u64
    - expr_bytecode: [u8; expr_bytecode_len]  (ExprProgram bytes)
```

Implementation in RPython (gnitz/dbsp/ops/linear.py or a new file):

```python
def op_map_expr(in_batch, out_writer, expr_programs, out_schema):
    """
    Compute each output column using a per-column ExprProgram.
    expr_programs: list of ExprProgram, one per output column.
    """
    builder = RowBuilder(out_schema, out_writer._batch)
    n = in_batch.length()
    for i in range(n):
        in_acc = in_batch.get_accessor(i)
        for col_idx in range(len(expr_programs)):
            result = eval_expr(expr_programs[col_idx], in_acc)
            builder.put_from_value(col_idx, result)
        builder.commit_row(in_batch.get_pk(i), in_batch.get_weight(i))
    out_writer.mark_sorted(in_batch._sorted)
```

`put_from_value(col_idx, eval_result)` dispatches on the output column's declared type
and writes the appropriate typed value to the RowBuilder. The `eval_expr` return type
is an integer (for int/bool/null results) or a float — same representation used
internally by the existing ExprVM.

The PK column (index 0) in a map_expr view is always a pure column reference from
the source table — it cannot be a computed expression (enforced by the planner).
The ExprProgram for the PK column is simply `LOAD_COL_INT col_idx`. This ensures the
output schema always has a valid U64/U128 PK.

#### 4B — Circuit graph encoding in Rust

**gnitz-core/src/circuit.rs (or equivalent):** Add `MapExpr` node variant:

```rust
pub struct MapExprNode {
    pub input:      NodeId,
    pub out_schema: Vec<(TypeCode, ExprProgram)>,  // (output_col_type, expr_program)
}
```

`CircuitBuilder::map_exprs(input: NodeId, cols: &[(TypeCode, ExprProgram)]) -> NodeId`

The serialized circuit graph encodes `ExprProgram` bytes inline with the node parameters.
The `compile_from_graph` pipeline in `program_cache.py` must be extended to decode
`OP_MAP_EXPR` nodes and construct the appropriate `op_map_expr` call with deserialized
`ExprProgram` objects.

#### 4C — Type inference in gnitz-sql

**gnitz-sql/src/binder.rs:** Add `bind_expr_with_type(expr, schema) -> Result<(BoundExpr, TypeCode), GnitzSqlError>`.

Type rules (Phase 4 scope — integer + float only, no strings):
- `ColRef(i)` → type is `schema.columns[i].type_code`
- `LitInt(_)` → `TypeCode::I64`
- `LitFloat(_)` → `TypeCode::F64`
- `BinOp(l, Add|Sub|Mul|Div|Mod, r)` where both sides are integer → `TypeCode::I64`
- `BinOp(l, Add|Sub|Mul|Div, r)` where either side is float → `TypeCode::F64`
- Comparison operators → `TypeCode::I64` (boolean as 0/1)
- Type mismatch (integer op applied to string) → `GnitzSqlError::Bind(...)`

**gnitz-sql/src/expr.rs:** Add `compile_bound_expr_to_program(expr, schema, eb) -> Result<ExprProgram, GnitzSqlError>`. This calls the existing `compile_bound_expr` and invokes `eb.build(result_reg)`.

**gnitz-sql/src/planner.rs:** Extend `execute_create_view` to detect computed
projections. When a `SelectItem` is not a bare `Identifier` (column name), bind it
via `bind_expr_with_type` and compile it to an `ExprProgram`. If all output columns
are bare identifiers, use the existing `CircuitBuilder::map()` path (column reordering).
If any output column is a computed expression, use `CircuitBuilder::map_exprs()`.

Enforce PK constraint: the source table's PK column must appear as a bare identifier
in the SELECT list (not as a computed expression). If omitted, prepend it silently
(same as Phase 1 behavior). If expressed as a computation, return
`GnitzSqlError::Unsupported("PK column must be a direct reference in CREATE VIEW")`.

#### 4D — Tests

**`rust_client/gnitz-py/tests/test_phase4_projections.py`** (new file):

```
TestComputedProjections
  test_add_columns           CREATE VIEW v AS SELECT id, a + b AS total FROM t
  test_multiply_literal      CREATE VIEW v AS SELECT id, price * 2 AS doubled FROM t
  test_mixed_arith           CREATE VIEW v AS SELECT id, (a + b) * c AS result FROM t
  test_comparison_expr       CREATE VIEW v AS SELECT id, val > 0 AS positive FROM t
  test_float_expr            CREATE VIEW v AS SELECT id, price * 1.1 AS adjusted FROM t
  test_pk_auto_prepend       SELECT val + 1 AS v FROM t — id auto-prepended
  test_computed_pk_rejects   SELECT id * 2 AS id FROM t — error
  test_projection_after_insert insert rows, verify view reflects computed values
```

---

### Phase 5 — String support + equijoins in CREATE VIEW

**Goals:**
- `CREATE VIEW v AS SELECT * FROM t WHERE name = 'Alice'`
- `CREATE VIEW v AS SELECT t1.id, t2.name FROM t1 JOIN t2 ON t1.cid = t2.id`
- `CREATE VIEW v AS SELECT t1.id FROM t1 JOIN t2 ON t1.name = t2.name`

#### 5A — ExprVM string opcodes (RPython)

Add 8 new opcodes (continuing from opcode 31):

```
EXPR_LOAD_COL_STR   = 32   # load string column value → string register
EXPR_LOAD_CONST_STR = 33   # load string constant from constant pool → string register
EXPR_STR_EQ         = 34
EXPR_STR_NE         = 35
EXPR_STR_LT         = 36
EXPR_STR_LE         = 37
EXPR_STR_GT         = 38
EXPR_STR_GE         = 39
```

The 4-word fixed-width instruction format is preserved. String constants are not
embedded inline in the bytecode (variable length). Instead, `ExprProgram` gains an
optional `const_strings: [str]` pool; `EXPR_LOAD_CONST_STR` carries a pool index
in words 1–2 (u64 index, zero-padded). The pool is serialized alongside the bytecode
in the circuit graph encoding.

`eval_expr` gains a string register alongside the existing integer and float registers
for the string result. Boolean results from string comparisons are stored in the integer
register (0 or 1) as with other comparisons.

**Scope:** string column-to-column and string column-to-constant comparisons only.
String arithmetic (concatenation) is out of scope.

#### 5B — Equijoin operators (RPython)

New operators: `op_equijoin_delta_trace(left_delta, right_trace, left_key_cols,
right_key_cols, out_schema)` and `op_equijoin_delta_delta(left_delta, right_delta,
left_key_cols, right_key_cols, out_schema)`.

Implementation: Murmur3-hash the specified key columns → U128 → delegate to the existing
join kernel (`op_join_delta_trace` / `op_join_delta_delta`). The key columns need not
be the PK of either input.

Output schema = concatenation of left and right schemas, with duplicate column names
disambiguated by table alias prefix (resolved at bind time in the Rust planner).

New opcodes: `OP_EQUIJOIN_DELTA_TRACE = 23`, `OP_EQUIJOIN_DELTA_DELTA = 24`.

Existing opcode assignments (from `gnitz/core/opcodes.py`):
```
0=HALT   1=FILTER   2=MAP      3=NEGATE   4=UNION
5=JOIN_DELTA_TRACE   6=JOIN_DELTA_DELTA   7=INTEGRATE   8=DELAY
9=REDUCE  10=DISTINCT  11=SCAN_TRACE  12=SEEK_TRACE
13=unallocated  14=unallocated
15=CLEAR_DELTAS
16=ANTI_JOIN_DELTA_TRACE  17=ANTI_JOIN_DELTA_DELTA
18=SEMI_JOIN_DELTA_TRACE  19=SEMI_JOIN_DELTA_DELTA
20=EXCHANGE_SHARD  21=EXCHANGE_GATHER
22=OP_MAP_EXPR (Phase 4)
23=OP_EQUIJOIN_DELTA_TRACE (Phase 5)
24=OP_EQUIJOIN_DELTA_DELTA (Phase 5)
```
20/21 are EXCHANGE_SHARD/GATHER — do NOT use them for equijoin.

#### 5C — Rust planner additions

**gnitz-core:** new opcode constants + `CircuitBuilder::equijoin(left, right_trace_tid,
left_key_cols: &[usize], right_key_cols: &[usize], out_schema: &[ColumnDef]) -> NodeId`.

**gnitz-sql/src/planner.rs:** Parse `TableWithJoins.joins` in `execute_create_view`.
For each `JoinOperator::Inner(JoinConstraint::On(Expr))`:
- Extract `ON t1.a = t2.b` from the expression.
- Resolve column references to `(table_idx, col_idx)` pairs.
- Emit `CircuitBuilder::equijoin(left_node, right_table_id, left_key_cols, right_key_cols, out_schema)`.
- Handle multi-table joins by chaining equijoin nodes (left-deep tree).

`JOIN … USING (col)` desugars to `ON t1.col = t2.col`. Cross joins (`CROSS JOIN`,
`JOIN` without `ON`) are rejected (`GnitzSqlError::Unsupported`).

**Schema inference for joins:** The binder resolves all table references in the `FROM`
clause, fetches their schemas, and produces a merged schema for the output. Duplicate
column names are prefixed with their table alias: `t1.id`, `t2.id`. The join output
PK is a synthetic U128 (XOR or concatenation hash of both table PKs).

#### 5D — Tests

```
TestStringViews
  test_view_string_eq_literal  CREATE VIEW v AS SELECT * FROM t WHERE name = 'Alice'
  test_view_string_cmp         CREATE VIEW v AS SELECT * FROM t WHERE name > 'M'
  test_view_string_col_to_col  (requires join, tested in TestJoins)

TestJoins
  test_inner_join_basic        JOIN on integer PK
  test_inner_join_string_key   JOIN ON t1.name = t2.name
  test_inner_join_multi        three-table join (chained equijoin nodes)
  test_join_using              JOIN USING (col)
  test_join_cross_rejects      CROSS JOIN → error
  test_join_non_equi_rejects   JOIN ON t1.a < t2.b → error (Phase 5 = equijoin only)
```

---

### Phase 6 — Aggregates + set operations + LEFT JOIN + CTEs

**Goals:**
- `CREATE VIEW v AS SELECT cat, COUNT(*), SUM(val), AVG(price) FROM t GROUP BY cat`
- `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT`
- `SELECT DISTINCT`
- `LEFT JOIN`
- `WITH cte AS (…) SELECT …`
- `IN (subquery)`, `EXISTS (…)`

#### 6A — Aggregates (RPython + Rust)

- Add `AGG_AVG = 5` to `UniversalAccumulator`. Internally maintains `(sum, count)`,
  emits `sum / count` as F64 on finalise.
- `gnitz-core/ops.rs` or `types.rs`: add aggregate function constants alongside
  `AGG_AVG`:
  ```rust
  pub const AGG_COUNT: u64 = 1;
  pub const AGG_SUM:   u64 = 2;
  pub const AGG_MIN:   u64 = 3;
  pub const AGG_MAX:   u64 = 4;
  pub const AGG_AVG:   u64 = 5;
  ```
  (These map to the `agg_func_id` field in the `REDUCE_OPS` circuit table row.)
- `gnitz-sql/planner.rs`: parse `GroupByExpr` + aggregate function calls
  (`COUNT(*)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)`). Emit `op_reduce`
  nodes. Multiple aggregates → one `op_reduce` per function, chained.
- Phase 4's type inference extended to handle aggregate result types (SUM/AVG → F64
  for float inputs, I64 for integer; COUNT → I64; MIN/MAX same as input).

#### 6B — Set operations (Rust planner, no RPython changes)

- `UNION ALL` → `op_union` (already exists)
- `UNION` → `op_union` + `op_distinct`
- `INTERSECT` → semi-join on PKs
- `EXCEPT` → anti-join on PKs
- `SELECT DISTINCT` → `op_distinct` after projection

All are expressible with existing RPython operators. The planner emits composite
circuit patterns.

#### 6C — LEFT JOIN (Rust planner, no RPython changes)

LEFT OUTER JOIN = equijoin ∪ (anti-join → null-fill map). The planner detects
`JoinOperator::Left(...)` and emits a 3-node pattern: equijoin node, anti-join node,
union node. The anti-join result is fed through a map node that fills right-side
columns with NULL. No new RPython operators.

#### 6D — CTEs + subqueries

- `WITH cte AS (select) SELECT …` → materialise each CTE as an ephemeral named
  binding in the planner; inline it into the outer query's circuit.
- `IN (subquery)` → semi-join; `NOT IN (subquery)` → anti-join; `EXISTS (…)` →
  semi-join.
- Scalar subqueries (SELECT returning one value) in WHERE are out of scope; emit
  `GnitzSqlError::Unsupported`.

#### 6E — Ephemeral SELECT (opt-in, controlled by `SqlPlanner` flag)

For users who want ad-hoc `SELECT … WHERE non_indexed / JOIN / GROUP BY` semantics,
the planner can create a temporary view, scan it, then drop it. This is opt-in via a
`SqlPlanner` option `allow_ephemeral: bool` (default: false). The user pays one
full-table pass as setup cost. Ephemeral views are never the default.

---

## C API surface

```c
/* Execute one or more SQL statements (DDL, DML, CREATE VIEW, CREATE INDEX).
   schema: schema context for name resolution (e.g. "public").
   For CREATE TABLE/VIEW/INDEX: sets *out_id to the allocated ID.
   For INSERT/UPDATE/DELETE: sets *out_id to the number of rows affected.
   For DROP: sets *out_id to 0.
   out_id may be NULL. Returns 0 on success, -1 on error (gnitz_last_error). */
int gnitz_execute_sql(GnitzConn *conn,
                      const char *sql,
                      const char *schema,
                      uint64_t   *out_id);

/* Point seek on a table or view by U128 PK.
   Returns the matching row (weight > 0) as a ZSetBatch, or an empty batch.
   Caller must free with gnitz_batch_free. */
int gnitz_seek(GnitzConn *conn, uint64_t table_id,
               uint64_t pk_lo, uint64_t pk_hi,
               GnitzBatch **out_batch);

/* Phase 2: seek via a secondary index on col_idx, returning full original rows.
   The server resolves: index key → source_pk → full row(s) in one roundtrip.
   Uses FLAG_SEEK_BY_INDEX with p4=col_idx, p5/p6=key.
   For non-unique indices with multiple matches, all matching rows are returned. */
int gnitz_seek_by_index(GnitzConn *conn, uint64_t table_id, uint32_t col_idx,
                        uint64_t key_lo, uint64_t key_hi,
                        GnitzBatch **out_batch);
```

Notes:
- `gnitz_execute_sql` does not take a `sql_len` parameter. All SQL strings are
  null-terminated. This is consistent with all other string parameters in the C API.
- `gnitz_seek_by_index` is a low-level API for callers who want direct index access
  without SQL. SQL users can write `SELECT … WHERE indexed_col = val` instead.
- No other new C API functions are needed through Phase 6. All SQL features are
  accessible through `gnitz_execute_sql`.

---

## What is explicitly out of scope

- **`INSERT INTO t SELECT …`** — use a view.
- **Auto-increment PK** — SEQ_TAB exists but is not yet wired to SQL.
- **Ad-hoc non-indexed SELECT** — use `CREATE VIEW` or `CREATE INDEX`.
- **Multi-column indices** — only single-column supported by `IndexCircuit`.
- **Float/string column indices** — `get_index_key_type()` explicitly rejects them (Phase B).
- **Range queries on string indices** — hashing destroys order; equality only.
- **`UPDATE … RETURNING`** — gnitz has no server-side row returning.
- **Transactions / rollback** — gnitz is Z-set append-only; no rollback.
- **Foreign key constraints** — gnitz does not enforce them via SQL.
- **`ORDER BY` in views** — not incrementalisable; views are unordered Z-sets.
  `ORDER BY` in a top-level SELECT is client-side sort (future addition to SELECT).
- **Window functions** — possible future addition, not in this plan.
- **Multi-table UPDATE / DELETE** — use a view to compute the target set, then push.
- **`INSERT OR IGNORE`** — the upsert (INSERT OR REPLACE) semantics are inherent;
  ignore-on-duplicate would require a seek-before-insert which conflicts with the
  bulk-insert model.
