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
      planner.rs      — DDL execution: CREATE TABLE/VIEW/INDEX, DROP
      dml.rs          — INSERT/UPDATE/DELETE/SELECT execution + client-side BoundExpr evaluator
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
    // LitStr(String) — Phase 3: string literals in UPDATE SET (✅ DONE)
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
) -> Result<Option<(u64, bool)>, ClientError>   // (index_id, is_unique) — updated Phase 3
```

Scans `IDX_TAB` (system table ID=5), filters rows where `owner_id == table_id` and
`source_col_idx == col_idx`, returns `(index_id, is_unique)` of the first match (or `None`).
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
arbitrary columns. New operators (NOT wrappers around existing PK join):
`op_equijoin_delta_trace` + `op_equijoin_delta_delta`. Parameters: `left_key_cols[],
right_key_cols[]`. Hash key columns inline using `_extract_group_key`-style Murmur3.
The trace cursor can't be reused (sorted by PK, not join key) — needs a temporary
sorted structure or hash table. `merge_schemas_for_join` can't be reused either
(requires matching PK types).

Rust: new `CircuitBuilder::equijoin(delta, trace_tid, left_cols, right_cols)`.

### 2. Computed projections (`SELECT a + b AS c`) — Phase 4

Reuse `OPCODE_MAP` with a new `ExprMapFunction(ScalarFunction)`. The ExprProgram
handles two kinds of output columns:

- **Computed columns** (expressions): compiled via `compile_bound_expr` → registers
  → `EXPR_EMIT` (32). Type-agnostic for int/float due to shared `r_int64`
  representation. `EXPR_INT_TO_FLOAT` (33) handles SQL type promotion.
- **Pass-through columns** (bare identifiers): `EXPR_COPY_COL` (34), a single
  type-dispatched opcode that copies directly from input accessor to output builder.
  Handles ALL types (int, float, string, U128) without touching the register file.

This split is critical: `EXPR_EMIT` works for int/float (they share `_lo[]` via
`r_int64`), but strings use `_strs[]` and U128 uses `_hi[]` — both are invisible to
`append_int`. `EXPR_COPY_COL` bypasses registers entirely, using the same type dispatch
as `UniversalProjection.evaluate_map` to handle every type correctly.

No new DBSP opcode — `PARAM_EXPR_NUM_REGS > 0` on a MAP node discriminates expr-map
from projection-map. Output schema from the view's registered schema. PK column must
remain at output position 0.

**Pre-existing bug fixed by Phase 4:** `compile_bound_expr` always emits integer
opcodes for BinOp/UnaryOp, even for float operands. The fix changes the return type
from `u32` to `(u32, bool)` — the bool is a compile-time type tag that propagates
through the recursive expression tree, selecting int vs float opcodes at each node.
No separate type inference pass needed. This also fixes float comparison bugs in
existing Phase 1 WHERE predicates (signed i64 comparison of IEEE 754 bit patterns
gives wrong ordering for negative floats).

**NULL propagation:** Shadow null registers (`null = [False] * num_regs`) track
null state per register alongside the value computation. One boolean OR per binary
opcode, JIT-optimized to O(source_columns) at native code. EMIT branches on null
state to call `append_null` or `append_int`. COPY_COL checks `accessor.is_null()`
directly. Also fixes the pre-existing filter bug where `NULL = 0` incorrectly
matches (`eval_expr` returns `(result, is_null)`; `ExprPredicate` returns False when
is_null).

### 3. String expressions in ExprVM — Phase 5

ExprVM (31 base opcodes + EXPR_EMIT (32), EXPR_INT_TO_FLOAT (33), EXPR_COPY_COL (34)
from Phase 4) has no string expression opcodes. String **predicates** in views
(`WHERE name = 'Alice'`, `ON t1.name = t2.name`) require new opcodes (40-47) for
loading and comparing strings. String **pass-through** in computed projections
(`SELECT name, a + b AS total FROM t`) is already handled by Phase 4's
`EXPR_COPY_COL` — no Phase 5 dependency for that case.

Phase 5 adds string *computation* opcodes: LOAD_COL_STR, LOAD_CONST_STR, STR_EQ..GE,
and EMIT_STR. These require a separate string register file (`str_regs`) alongside the
existing `r_int64` registers, and a three-way type tag in `compile_bound_expr` (from
`(u32, bool)` to `(u32, TypeTag)` where TypeTag = Int | Float | String). The
`const_strings` pool encoding for `EXPR_LOAD_CONST_STR` needs design work — strings
can't fit in u64 param values.

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

All 31 base VM opcodes are present and the `load_const` split-encoding bug is fixed.
Phase 4 adds `EXPR_EMIT` (32), `EXPR_INT_TO_FLOAT` (33), and `EXPR_COPY_COL` (34).

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
| Output (Phase 4) | `EMIT` (32) — int/float col write via `append_int` | Phase 4 |
| Cast (Phase 4) | `INT_TO_FLOAT` (33) — SQL type promotion | Phase 4 |
| Pass-through (Phase 4) | `COPY_COL` (34) — type-dispatched direct copy (all types) | Phase 4 |

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

#### 2A–2E — Implementation details ✅ ALL DONE

All protocol, core, SQL planner, C API, Python, and test changes are implemented.
See commit `a11c02b` for full diff.

**Key APIs added:**
- `gnitz-protocol`: `FLAG_SEEK_BY_INDEX=256`, `FLAG_ALLOCATE_INDEX_ID=512`, header accessors
- `gnitz-core`: `alloc_index_id()`, `seek_by_index()`, `find_index_for_column() → Option<(u64, bool)>`,
  `create_index()`, `drop_index_by_name()`
- `gnitz-sql`: `Binder::find_index()` with cache, `try_extract_index_seek()`,
  `CreateIndex`/`Drop Index` in planner, `SqlResult::IndexCreated`
- `gnitz-py`: `seek_by_index()`, `IndexCreated` result type
- `gnitz-capi`: `gnitz_seek_by_index()`
- Server: `FLAG_SEEK_BY_INDEX` + `FLAG_ALLOCATE_INDEX_ID` handlers in executor.py
- Index naming: `"{schema}__{table}__idx_{col}"` (matches Python's `make_secondary_index_name`)
- IdxTab column layout: `(index_id[0], owner_id[1], owner_kind[2],
  source_col_idx[3], name[4], is_unique[5], cache_directory[6])`

**Remaining low-priority TODOs (no user-visible impact):**
- Explicit `elif intmask(flags) == ipc.FLAG_SCAN:` dispatch in executor (falls through)
- `set_seek_key`/`seek_key_lo`/`seek_key_hi` accessors in header.rs (callers use raw fields)

---

### Phase 3 — UPDATE + DELETE ✅ DONE

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

#### 3A–3C — Implementation details ✅ ALL DONE

**Commits:** `00296f3` (implementation), `f557c3d` (simplify review).

**Key additions to `dml.rs`:**
- `eval_set_expr(expr, batch, row_idx, schema) → ColumnValue` — SET-clause evaluator;
  handles `LitStr`/string `ColRef` directly, delegates numeric to `eval_expr`
- `append_column_value(col, cv, tc)` — encodes `ColumnValue` into `ColData` by TypeCode
- `write_set_columns(current, row_idx, assignments, schema, dst)` — appends one updated
  row to `dst`; copies non-assigned columns from `current[row_idx]`
- `try_extract_pk_in(expr, schema) → Option<Vec<(u64, u64)>>` — matches `WHERE pk IN (...)`
- `try_extract_index_seek` extended with `require_unique: bool` parameter
- `execute_update` — 4 paths: PK seek, unique index seek, full scan, no WHERE
- `execute_delete` — 5 paths: PK eq, PK IN, unique index seek, full scan, no WHERE

**Other changes:**
- `logical_plan.rs`: added `LitStr(String)` to `BoundExpr`
- `binder.rs`: string literal binding; `index_cache`/`find_index` updated to `Option<(u64, bool)>`
- `expr.rs`: `LitStr` arm returns `Unsupported` for view predicates
- `client.rs`: `find_index_for_column` returns `(index_id, is_unique)`; `unique_pk as u64`
- `planner.rs`: `Statement::Update { .. }` and `Statement::Delete(_)` match arms

**Design notes (carried forward):**
- UPDATE pushes `weight=+1` rows with same PK; `unique_pk=true` auto-retracts old row
- No "skip seek" optimisation — always read current row for non-assigned columns
- Non-unique indexes fall through to full scan (`seek_by_index` returns at most one row)
- `DELETE FROM t WHERE pk IN (...)` returns list length as count (no per-pk existence check)

**Tests:** 16 tests in `test_dml.py` (8 `TestUpdateSQL` + 8 `TestDeleteSQL`), all pass.
Includes: PK/index/scan paths, non-unique index fallback, PK column rejection,
view circuit propagation, index retraction after delete.

---

### Phase 4 — Computed projections in CREATE VIEW

**Goals:**
- `CREATE VIEW v AS SELECT id, a + b AS total FROM t`
- `CREATE VIEW v AS SELECT id, price * 1.1 AS adjusted FROM t`
- `CREATE VIEW v AS SELECT id, CASE WHEN val > 0 THEN 1 ELSE 0 END AS flag FROM t`
  (CASE WHEN is a later addition; Phase 4 covers arithmetic expressions)

#### Key design insight: EMIT for computation, COPY_COL for pass-through

Pass-through and computation are fundamentally different operations. A computed column
transforms values through the ExprVM register file; a pass-through column copies data
between schemas unchanged. Forcing pass-through through registers creates an impedance
mismatch: RowBuilder stores strings in `_strs[]` and U128 high bits in `_hi[]`, but
`LOAD_COL_INT` + `EMIT → append_int` only touches `_lo[]`.

Phase 4 uses two output opcodes:

- **`EXPR_EMIT` (32)**: writes a register value to the output via `append_int`.
  Type-agnostic for int/float because both the register array (`regs[]`) and
  RowBuilder's `_lo[]` use `r_int64` with floats stored via `float2longlong()`.
  The schema-aware `append_from_accessor` in `commit_row` interprets the bits
  correctly. **Does NOT work for string or U128 columns.**

- **`EXPR_COPY_COL` (34)**: copies a column directly from input accessor to output
  builder using full type dispatch (same as `UniversalProjection.evaluate_map`).
  Handles ALL types: int, float, string, U128. **Bypasses the register file entirely**
  — no register allocated, no LOAD instruction needed. One instruction per pass-through
  column instead of two (LOAD + EMIT).

This separation keeps the register file simple (int/float only, no string registers
needed in Phase 4) while supporting mixed views like
`SELECT id, name, a + b AS total FROM t` where `name` is a string pass-through and
`total` is a computed int/float.

#### 4A — RPython: new expression opcodes + null-propagating eval functions

**No new DBSP operator or opcode.** Reuse `OPCODE_MAP` with a new `ScalarFunction`
subclass. The existing `op_map` handles RowBuilder creation, `commit_row`, and
`mark_sorted` — all unchanged.

**New expression opcodes** in `gnitz/dbsp/expr.py`:

```python
EXPR_EMIT          = 32   # builder.append_int(regs[a1]) or append_null(a2)
EXPR_INT_TO_FLOAT  = 33   # regs[dst] = float2longlong(float(intmask(regs[a1])))
EXPR_COPY_COL      = 34   # type-dispatched direct copy: input col → output col
```

`EXPR_EMIT` writes a computed register value to the output. `a1` = source register,
`a2` = output payload column index (needed by `append_null` on the null path).
Works for int and float only (both stored as `r_int64` in registers and `_lo[]`).

`EXPR_INT_TO_FLOAT` converts an integer register value to its IEEE 754 double bit
representation. Required for mixed-type expressions like `int_col + 1.1` where the
integer value 42 must become `float2longlong(42.0)`, not raw 42. Maps to a single
x86 instruction (`cvtsi2sd`); the JIT inlines it. See §4C for when this cast is
inserted.

`EXPR_COPY_COL` copies a column directly from input accessor to output builder using
full type dispatch. Encoding: `(EXPR_COPY_COL, type_code, src_col_idx, payload_col_idx)`
— the `dst` word is repurposed as the type code since no register is allocated. Handles
string (`append_string`), U128 (`append_u128`), float (`append_float`), and int
(`append_int`) columns. See §4A-copy below for the full handler.

**Phase 5 numbering:** Phase 5's string opcodes start at 40+. Opcodes 32-34 are
EXPR_EMIT, EXPR_INT_TO_FLOAT, and EXPR_COPY_COL from Phase 4. 35-39 reserved.

##### Shadow null registers — SQL NULL propagation

SQL requires `NULL + 1 = NULL` and `NULL > 5 = NULL` (falsy). The ExprVM has no
built-in NULL propagation: `EXPR_LOAD_COL_INT` on a NULL column loads 0 (columns
are zero-filled), so `a + b` where `a` is NULL yields `0 + b` instead of NULL.

The fix exploits the ExprVM's straight-line, branch-free architecture. Every
instruction always executes and writes to exactly one destination register. This
makes null tracking trivial — add a 1-bit "taint tag" per register:

```python
@jit.unroll_safe
def eval_expr_map(program, accessor, builder):
    """Evaluate expression program, writing output columns via EMIT."""
    program = jit.promote(program)
    code = program.code
    regs = [r_int64(0)] * program.num_regs
    null = [False] * program.num_regs          # shadow null registers

    i = 0
    while i < program.num_instrs:
        base = i * 4
        op  = intmask(code[base])
        dst = intmask(code[base + 1])
        a1  = intmask(code[base + 2])
        a2  = intmask(code[base + 3])

        if op == EXPR_LOAD_COL_INT:
            regs[dst] = accessor.get_int_signed(a1)
            null[dst] = accessor.is_null(a1)
        elif op == EXPR_LOAD_COL_FLOAT:
            regs[dst] = float2longlong(accessor.get_float(a1))
            null[dst] = accessor.is_null(a1)
        elif op == EXPR_LOAD_CONST:
            regs[dst] = r_int64(intmask((a2 << 32) | (a1 & 0xFFFFFFFF)))
            null[dst] = False

        elif op == EXPR_INT_ADD:
            regs[dst] = r_int64(intmask(regs[a1] + regs[a2]))
            null[dst] = null[a1] or null[a2]
        # ... all binary ops:   null[dst] = null[a1] or null[a2]
        # ... all unary ops:    null[dst] = null[a1]

        elif op == EXPR_INT_TO_FLOAT:
            regs[dst] = float2longlong(float(intmask(regs[a1])))
            null[dst] = null[a1]

        elif op == EXPR_IS_NULL:
            regs[dst] = r_int64(1) if accessor.is_null(a1) else r_int64(0)
            null[dst] = False      # IS_NULL itself never returns NULL
        elif op == EXPR_IS_NOT_NULL:
            regs[dst] = r_int64(0) if accessor.is_null(a1) else r_int64(1)
            null[dst] = False

        elif op == EXPR_EMIT:
            if null[a1]:
                builder.append_null(a2)     # a2 = payload col index
            else:
                builder.append_int(regs[a1])  # type-agnostic (int/float only)

        elif op == EXPR_COPY_COL:
            # dst = type_code (repurposed — no register destination)
            # a1 = src_col_idx, a2 = payload_col_idx
            if accessor.is_null(a1):
                builder.append_null(a2)
            elif dst == types.TYPE_STRING.code:
                res = accessor.get_str_struct(a1)
                s = strings.resolve_string(res[2], res[3], res[4])
                builder.append_string(s)
            elif dst == types.TYPE_F64.code or dst == types.TYPE_F32.code:
                builder.append_float(accessor.get_float(a1))
            elif dst == types.TYPE_U128.code:
                val = accessor.get_u128(a1)
                builder.append_u128(
                    r_uint64(intmask(val)), r_uint64(intmask(val >> 64)))
            else:
                builder.append_int(accessor.get_int_signed(a1))

        i += 1
```

**CRITICAL ORDERING CONSTRAINT:** `append_null(payload_col_idx)` at `batch.py:219`
validates `payload_col_idx == self._curr` and raises `LayoutError` on mismatch.
All output instructions (EMIT, COPY_COL) MUST appear in sequential payload column
order (0, 1, 2, ...). The Rust planner enforces this by appending output instructions
in order of the output column list.

**Null propagation rules** (every opcode falls into exactly one category):

| Category | Rule |
|---|---|
| `LOAD_COL_*` | `null[dst] = accessor.is_null(col_idx)` |
| `LOAD_CONST` | `null[dst] = False` |
| Binary ops (arith, cmp, bool_and/or) | `null[dst] = null[a1] or null[a2]` |
| Unary ops (neg, not, int_to_float) | `null[dst] = null[a1]` |
| `IS_NULL` / `IS_NOT_NULL` | `null[dst] = False` |
| `EMIT` | branch on `null[a1]`: `append_null(a2)` or `append_int(regs[a1])` |
| `COPY_COL` | branch on `accessor.is_null(a1)`: `append_null(a2)` or type-dispatch copy |

**Why this is nearly free — JIT optimization:** Both `regs` and `null` are local
arrays that never escape the function; the JIT virtualizes them into CPU registers
(zero heap allocation). For `SELECT id, a + b * 2 AS total FROM t`, the null chain:

```
null[0] = is_null(a)                     # LOAD a
null[1] = is_null(b)                     # LOAD b
null[2] = False                          # LOAD_CONST 2
null[3] = null[1] or False  → null[1]    # MUL b, 2
null[4] = null[0] or null[1]             # ADD a, result
if null[4]: append_null() ...            # EMIT
```

The JIT constant-folds `False`, eliminates intermediate copies, and reduces the
chain to `is_null(a) or is_null(b)` — exactly two bitmap lookups and one OR. The
shadow register approach *looks* O(num_instructions) but collapses to
O(num_source_columns) at native code level — the same cost as a hand-written
dependency mask, derived automatically by the JIT.

##### eval_expr (filter path) — same fix

The existing `eval_expr` (used by `ExprPredicate` for filter predicates) has the
same NULL bug: `WHERE a = 0` with `a` NULL loads 0, compares `0 == 0 → true`, and
the NULL row incorrectly passes the filter. SQL says `NULL = 0 → NULL → falsy`.

Fix: add shadow null registers to `eval_expr`, change return to include null state:

```python
@jit.unroll_safe
def eval_expr(program, accessor):
    # ... same null tracking as eval_expr_map, minus EMIT ...
    return regs[program.result_reg], null[program.result_reg]
```

`ExprPredicate` becomes:

```python
class ExprPredicate(ScalarFunction):
    _immutable_fields_ = ['program']

    def __init__(self, program):
        self.program = program

    def evaluate_predicate(self, row_accessor):
        result, is_null = eval_expr(self.program, row_accessor)
        return (not is_null) and (result != r_int64(0))
```

SQL three-valued logic falls out naturally: NULL predicate → `is_null=True` →
return False → row filtered out. This fixes the pre-existing filter bug while
adding zero cost to the non-null fast path (the JIT eliminates all null tracking
when no nullable columns are loaded).

##### ExprMapFunction (ScalarFunction subclass)

```python
class ExprMapFunction(ScalarFunction):
    _immutable_fields_ = ['program']

    def __init__(self, program):
        self.program = program

    def evaluate_map(self, row_accessor, output_row):
        eval_expr_map(self.program, row_accessor, output_row)
```

Plugs directly into `op_map` — no changes to operator code, no new instruction type.

**PK handling:** ExprPrograms only compute payload columns. The PK passes through
via `commit_row(in_batch.get_pk(i), in_batch.get_weight(i))` in `op_map`, unchanged.

##### Why EMIT works for both int and float (but not string/U128)

Both the ExprVM register array (`regs[]`) and RowBuilder's internal buffer (`_lo[]`)
use the same representation: `r_int64`, with floats stored via `float2longlong()`.
The full bit pipeline:

```
float2longlong(f64) → regs[dst] → EMIT → append_int → _lo[curr]
    → commit_row → get_float → longlong2float → f64 in column buffer
```

For ints, `_lo[curr]` stores the raw value; `get_int` reads it back. For floats,
`_lo[curr]` stores the longlong bits; `get_float` applies `longlong2float`. Both
paths work because `append_int` stores the raw `r_int64` without conversion — no
runtime type dispatch needed. The schema-aware `append_from_accessor` in
`commit_row` interprets the bits correctly using the output column's TypeCode.

**Why EMIT does NOT work for string/U128:** String columns store data in `_strs[]`
via `append_string(val_str)`; `append_int` only writes `_lo[]`, leaving `_strs[]`
at its initial `""`. Similarly, U128 columns store the high 64 bits in `_hi[]` via
`append_u128(lo, hi)`; `append_int` only writes `_lo[]`, zeroing the high bits.
`EXPR_COPY_COL` solves this by using the correct RowBuilder method per type.

#### 4B — Circuit graph encoding

**No new opcode, no new system table, no new param slots.** Reuse `OPCODE_MAP` with
the existing FILTER-style expression encoding. `PARAM_EXPR_NUM_REGS > 0` on a MAP
node is the mode discriminator.

**Slot layout for expr-map (on OPCODE_MAP node):**

| Slot | Param | Value |
|------|-------|-------|
| 0 | `PARAM_FUNC_ID` | 0 |
| 7 | `PARAM_EXPR_NUM_REGS` | register count (mode discriminator) |
| 64..64+K | `PARAM_EXPR_BASE` | bytecode words (u32 stored as u64) |

No `PARAM_EXPR_RESULT_REG` needed — EMIT writes directly, there's no single result
register. No output type codes needed — the view's registered schema (already in
the registry from `create_view_with_circuit`) provides them.

**Output schema:** `compile_from_graph` already has `out_schema = view_family.schema`
(line 581). For the Phase 4 circuit pattern (`input → filter → map_expr → sink`),
the map node's output schema IS the view's schema.

**Disambiguation in compile_from_graph's MAP handler:**

```python
elif op == opcodes.OPCODE_MAP:
    in_reg = cur_reg_file.registers[in_regs[opcodes.PORT_IN]]
    num_regs = node_params.get(opcodes.PARAM_EXPR_NUM_REGS, 0)

    if num_regs > 0:
        # Expr-map mode (Phase 4+)
        code = []
        idx = 0
        while (opcodes.PARAM_EXPR_BASE + idx) in node_params:
            code.append(r_int64(node_params[opcodes.PARAM_EXPR_BASE + idx]))
            idx += 1
        prog = ExprProgram(code, num_regs, 0)
        func = ExprMapFunction(prog)
        node_schema = out_schema   # view's registered schema

    elif opcodes.PARAM_PROJ_BASE in node_params:
        # Projection mode (existing, unchanged)
        ...

    else:
        # Identity mode (existing, unchanged)
        ...
```

Three modes, same opcode, same `map_op` instruction.

**Rust side — `CircuitBuilder::map_expr`:**

```rust
pub fn map_expr(&mut self, input: NodeId, program: ExprProgram) -> NodeId {
    let nid = self.alloc_node(OPCODE_MAP);
    self.connect(input, nid, PORT_IN);
    self.params.push((nid, PARAM_FUNC_ID, 0));
    self.params.push((nid, PARAM_EXPR_NUM_REGS, program.num_regs as u64));
    for (i, &word) in program.code.iter().enumerate() {
        self.params.push((nid, PARAM_EXPR_BASE + i as u64, word as u64));
    }
    nid
}
```

Identical pattern to `filter()`'s expression params, on a MAP node.

**Capacity:** 192 bytecode slots (64-255) = 48 four-word instructions. A pass-through
column costs 1 instruction (COPY_COL), a computed column costs 3-4 (LOAD × N + op +
EMIT), with shared loads amortized. Practical limit: ~20 output columns with moderate
complexity. If needed later: pack 2×u32 per u64 slot → 96 instructions.

**Unified program:** All output columns compile into a single ExprProgram. Computed
columns share one register file; column loads happen once even when referenced by
multiple expressions (e.g., `a` in both `a+b` and `a-c`). Pass-through columns use
COPY_COL (no registers consumed). The program ends with one output instruction (EMIT
or COPY_COL) per payload column, in sequential order.

#### 4C — Type-aware expression compilation in gnitz-sql

##### The pre-existing bug: `compile_bound_expr` always emits integer opcodes

The current `compile_bound_expr` (`expr.rs:34-52`) always emits integer opcodes for
all `BinOp` and `UnaryOp` arms, even when operands are float:

```rust
// Current (WRONG): always integer opcodes
BinOp::Add => eb.add(l, r),     // EXPR_INT_ADD — even for float columns
BinOp::Gt  => eb.cmp_gt(l, r),  // EXPR_CMP_GT  — even for float columns
UnaryOp::Neg => eb.neg_int(a),  // EXPR_INT_NEG  — even for float columns
```

The function correctly uses `schema.columns[idx].type_code` to choose between
`load_col_int` and `load_col_float` at `ColRef` leaves, but discards this type
knowledge when returning — it returns only `u32` (register index).

**Impact:** Float arithmetic gives garbage results (`EXPR_INT_ADD` on IEEE 754 bit
patterns is meaningless). Float comparison gives wrong results for negative values:
signed i64 comparison of IEEE 754 bits reverses ordering for negatives. Example:
`WHERE price > -0.5` with `price = -1.0` → i64 comparison says `-1.0_bits >
-0.5_bits` = TRUE (wrong; -1.0 is NOT > -0.5), because `-1.0` has a less-negative
i64 bit pattern than `-0.5` in IEEE 754 sign-magnitude encoding.

This affects existing Phase 1 WHERE predicates on float columns, not just Phase 4.

##### The fix: return `(u32, bool)` — type tag bubbles up for free

Change `compile_bound_expr` return type from `Result<u32, ...>` to
`Result<(u32, bool), ...>`. The bool means "this register holds float bits."

No changes to `BoundExpr`, `Binder`, or the ExprVM. No separate type inference
function needed. The type propagates naturally through the existing recursion:

```rust
pub fn compile_bound_expr(
    expr: &BoundExpr, schema: &Schema, eb: &mut ExprBuilder,
) -> Result<(u32, bool), GnitzSqlError> {
    match expr {
        BoundExpr::ColRef(idx) => {
            let tc = schema.columns[*idx].type_code;
            let is_float = matches!(tc, TypeCode::F32 | TypeCode::F64);
            let reg = if is_float { eb.load_col_float(*idx) }
                      else        { eb.load_col_int(*idx) };
            Ok((reg, is_float))
        }
        BoundExpr::LitInt(v)   => Ok((eb.load_const(*v), false)),
        BoundExpr::LitFloat(v) => Ok((eb.load_const(v.to_bits() as i64), true)),

        BoundExpr::BinOp(left, op, right) => {
            let (l, l_float) = compile_bound_expr(left, schema, eb)?;
            let (r, r_float) = compile_bound_expr(right, schema, eb)?;
            let is_float = l_float || r_float;

            // SQL type promotion: if either operand is float, cast the int operand
            let (l, r) = if is_float {
                let l = if l_float { l } else { eb.int_to_float(l) };
                let r = if r_float { r } else { eb.int_to_float(r) };
                (l, r)
            } else {
                (l, r)
            };

            let result = match (op, is_float) {
                (BinOp::Add, false) => eb.add(l, r),
                (BinOp::Add, true)  => eb.float_add(l, r),
                (BinOp::Sub, false) => eb.sub(l, r),
                (BinOp::Sub, true)  => eb.float_sub(l, r),
                (BinOp::Mul, false) => eb.mul(l, r),
                (BinOp::Mul, true)  => eb.float_mul(l, r),
                (BinOp::Div, false) => eb.div(l, r),
                (BinOp::Div, true)  => eb.float_div(l, r),
                (BinOp::Mod, false) => eb.modulo(l, r),
                (BinOp::Mod, true)  => return Err(GnitzSqlError::Unsupported(
                    "float modulo not supported".into())),
                (BinOp::Eq,  false) => eb.cmp_eq(l, r),
                (BinOp::Eq,  true)  => eb.fcmp_eq(l, r),
                (BinOp::Ne,  false) => eb.cmp_ne(l, r),
                (BinOp::Ne,  true)  => eb.fcmp_ne(l, r),
                (BinOp::Gt,  false) => eb.cmp_gt(l, r),
                (BinOp::Gt,  true)  => eb.fcmp_gt(l, r),
                (BinOp::Ge,  false) => eb.cmp_ge(l, r),
                (BinOp::Ge,  true)  => eb.fcmp_ge(l, r),
                (BinOp::Lt,  false) => eb.cmp_lt(l, r),
                (BinOp::Lt,  true)  => eb.fcmp_lt(l, r),
                (BinOp::Le,  false) => eb.cmp_le(l, r),
                (BinOp::Le,  true)  => eb.fcmp_le(l, r),
                (BinOp::And, _)     => eb.bool_and(l, r),
                (BinOp::Or,  _)     => eb.bool_or(l, r),
            };

            // Comparisons and boolean ops always return int (0/1)
            let result_float = is_float && matches!(op,
                BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod);
            Ok((result, result_float))
        }

        BoundExpr::UnaryOp(UnaryOp::Neg, inner) => {
            let (a, a_float) = compile_bound_expr(inner, schema, eb)?;
            let r = if a_float { eb.float_neg(a) } else { eb.neg_int(a) };
            Ok((r, a_float))
        }
        BoundExpr::UnaryOp(UnaryOp::Not, inner) => {
            let (a, _) = compile_bound_expr(inner, schema, eb)?;
            Ok((eb.bool_not(a), false))
        }

        BoundExpr::IsNull(idx)    => Ok((eb.is_null(*idx), false)),
        BoundExpr::IsNotNull(idx) => Ok((eb.is_not_null(*idx), false)),
        BoundExpr::LitStr(_)      => Err(GnitzSqlError::Unsupported(
            "string literals not supported in view predicates".into())),
    }
}
```

**Type propagation rules** (each node determines its own `is_float`):

| Node | `is_float` |
|---|---|
| `ColRef(i)` | `schema.columns[i].type_code ∈ {F32, F64}` |
| `LitInt` | `false` |
| `LitFloat` | `true` |
| Arithmetic BinOp | `left_float \|\| right_float` (SQL promotion) |
| Comparison / Boolean BinOp | always `false` (result is 0/1) |
| `Neg` | same as operand |
| `Not` | always `false` |
| `IsNull` / `IsNotNull` | always `false` |

**Mixed-type promotion:** When `is_float && !l_float` (or `!r_float`), the integer
operand gets an `EXPR_INT_TO_FLOAT` cast injected. The cast converts the integer
VALUE (e.g., 42) to its float BIT representation (`float2longlong(42.0)`). This
happens lazily — pure-int and pure-float expressions emit zero casts. Most SQL
expressions are homogeneous, so the cast is rarely needed.

**Example — `int_col + 1.1`:**
```
LOAD_COL_INT   r0, int_col       # r0 = 42 (raw integer)
LOAD_CONST     r1, 1.1_bits      # r1 = float2longlong(1.1)
INT_TO_FLOAT   r2, r0            # r2 = float2longlong(42.0)
FLOAT_ADD      r3, r2, r1        # r3 = float2longlong(42.0 + 1.1) = 43.1 bits
```

**Example — `float_col > -0.5` (fixes the pre-existing comparison bug):**
```
LOAD_COL_FLOAT r0, float_col     # r0 = float bits
LOAD_CONST     r1, (-0.5)_bits   # r1 = float bits of -0.5
FCMP_GT        r2, r0, r1        # correct float comparison (not signed i64!)
```

##### Rust ExprBuilder additions

```rust
const EXPR_EMIT:          u32 = 32;
const EXPR_INT_TO_FLOAT:  u32 = 33;
const EXPR_COPY_COL:      u32 = 34;

pub fn int_to_float(&mut self, src: u32) -> u32 {
    let dst = self.alloc_reg();
    self.emit(EXPR_INT_TO_FLOAT, dst, src, 0);
    dst
}

/// Emit a computed payload column value (int/float only). src_reg = register
/// holding the value, payload_col_idx = output column index.
pub fn emit_col(&mut self, src_reg: u32, payload_col_idx: u32) {
    self.emit(EXPR_EMIT, 0, src_reg, payload_col_idx);
}

/// Direct pass-through copy from input column to output column.
/// Handles ALL types (int, float, string, U128) via type dispatch in the VM.
/// No register allocated — bypasses the register file entirely.
pub fn copy_col(&mut self, type_code: u32, src_col_idx: u32, payload_col_idx: u32) {
    self.emit(EXPR_COPY_COL, type_code, src_col_idx, payload_col_idx);
}
```

##### RPython ExprBuilder additions

```python
EXPR_EMIT          = 32
EXPR_INT_TO_FLOAT  = 33
EXPR_COPY_COL      = 34

def int_to_float(self, src):
    dst = self._alloc_reg()
    self._emit(EXPR_INT_TO_FLOAT, dst, src, 0)
    return dst

def emit_col(self, src_reg, payload_col_idx):
    self._emit(EXPR_EMIT, 0, src_reg, payload_col_idx)

def copy_col(self, type_code, src_col_idx, payload_col_idx):
    # No _alloc_reg() — COPY_COL bypasses the register file
    self._emit(EXPR_COPY_COL, type_code, src_col_idx, payload_col_idx)
```

##### Caller update — existing WHERE predicate path

The only existing caller (`planner.rs:180`) destructures the new return type:

```rust
// Before:
let result_reg = compile_bound_expr(&bound, &source_schema, &mut eb)?;
// After:
let (result_reg, _) = compile_bound_expr(&bound, &source_schema, &mut eb)?;
```

The `_is_float` is unused for WHERE predicates — the result is always boolean.
This single-line change fixes the pre-existing float comparison bug in Phase 1
WHERE predicates at zero additional cost.

##### Planner changes for Phase 4 computed projections

**gnitz-sql/src/planner.rs:** Extend `execute_create_view` to detect computed
projections. When a `SelectItem` is not a bare `Identifier` (column name), bind it
via `binder.bind_expr` and compile it via `compile_bound_expr`. The returned
`is_float` determines the output column's TypeCode (`F64` if float, `I64` if int).

If all output columns are bare identifiers, use the existing `CircuitBuilder::map()`
path (column reordering). If any output column is a computed expression, use
`CircuitBuilder::map_expr()`.

**Per-column code generation** (inside the shared ExprBuilder, for each payload column
in output order):

```rust
for (payload_idx, item) in output_payload_items.iter().enumerate() {
    match item {
        // Pass-through: single COPY_COL — handles ALL types
        OutputCol::PassThrough { src_col } => {
            let tc = source_schema.columns[*src_col].type_code as u32;
            eb.copy_col(tc, *src_col as u32, payload_idx as u32);
        }
        // Computed: compile expression → register → EMIT
        OutputCol::Computed { bound_expr } => {
            let (reg, _is_float) = compile_bound_expr(bound_expr, &source_schema, &mut eb)?;
            eb.emit_col(reg, payload_idx as u32);
        }
    }
}
let program = eb.build(0); // result_reg=0 unused — EMIT/COPY_COL write directly
```

Pass-through columns use `COPY_COL` (type-dispatched, 1 instruction, 0 registers).
Computed columns use `compile_bound_expr` → `EMIT` (register-based, N instructions).
Both advance the RowBuilder's `_curr` counter sequentially — the ordering constraint
is satisfied because the loop iterates in payload column order.

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
  test_string_passthrough    CREATE VIEW v AS SELECT id, name, a + b AS total FROM t
                             where name is VARCHAR — verifies COPY_COL handles strings
                             alongside computed int/float columns
  test_mixed_passthrough     CREATE VIEW v AS SELECT id, val, a * 2 AS doubled FROM t
                             verifies COPY_COL + EMIT interleave correctly
```

**Float type-awareness tests** (validates §4C fixes):

```
TestFloatExpressions
  test_float_arith           int_col + float_col: verifies FLOAT_ADD (not INT_ADD)
  test_mixed_multiply        int_col * 1.5: verifies INT_TO_FLOAT + FLOAT_MUL
  test_float_neg             -float_col: verifies FLOAT_NEG (not INT_NEG)
  test_float_cmp_negative    WHERE float_col > -0.5 with negative rows: verifies
                             FCMP_GT (not CMP_GT — signed i64 comparison of IEEE 754
                             bits reverses ordering for negatives)
  test_pure_int_unchanged    int_col + int_col: verifies INT_ADD still used (no cast)
  test_pure_float_no_cast    float_col + float_col: verifies no INT_TO_FLOAT emitted
```

**NULL propagation tests** (validates §4A shadow null registers):

```
TestNullPropagation
  test_null_plus_int         a + b where a is NULL: output is NULL (not 0 + b)
  test_null_comparison       a > 5 where a is NULL: predicate returns false (not 0 > 5)
  test_non_null_computes     a + b where neither is NULL: normal arithmetic result
  test_is_null_not_null      SELECT (a IS NULL) AS flag where a IS NULL: flag = 1
                             (IS_NULL itself is never NULL)
  test_null_chain            a + b + c where b is NULL: result is NULL
  test_null_filter_eq_zero   WHERE a = 0 with a NULL: row filtered out (not matched)
                             (validates eval_expr NULL fix)
```

---

### Phase 5 — String support + equijoins in CREATE VIEW

**Goals:**
- `CREATE VIEW v AS SELECT * FROM t WHERE name = 'Alice'`
- `CREATE VIEW v AS SELECT t1.id, t2.name FROM t1 JOIN t2 ON t1.cid = t2.id`
- `CREATE VIEW v AS SELECT t1.id FROM t1 JOIN t2 ON t1.name = t2.name`

#### 5A — ExprVM string opcodes (RPython)

Add 9 new opcodes (starting from 40; opcodes 32-33 = EXPR_EMIT + EXPR_INT_TO_FLOAT
from Phase 4; 34-39 reserved):

```
EXPR_LOAD_COL_STR   = 40   # load string column value → string register
EXPR_LOAD_CONST_STR = 41   # load string constant from constant pool → string register
EXPR_STR_EQ         = 42
EXPR_STR_NE         = 43
EXPR_STR_LT         = 44
EXPR_STR_LE         = 45
EXPR_STR_GT         = 46
EXPR_STR_GE         = 47
EXPR_EMIT_STR       = 48   # builder.append_string(str_regs[a1])
```

The 4-word fixed-width instruction format is preserved. String constants are not
embedded inline in the bytecode (variable length). Instead, `ExprProgram` gains an
optional `const_strings: [str]` pool; `EXPR_LOAD_CONST_STR` carries a pool index
in words 1–2 (u64 index, zero-padded).

**const_strings pool encoding:** Strings can't fit in u64 param values. Options
to evaluate at implementation time: (a) a new system table for bytecode string pools,
(b) pack strings into the blob area of the IPC batch alongside the circuit params,
(c) encode each string character-by-character in sequential param slots with a length
prefix. Option (a) is cleanest for large pools.

`eval_expr` and `eval_expr_map` (Phase 4) both gain a string register array alongside
the existing r_int64 registers. Boolean results from string comparisons are stored
in the integer register (0 or 1) as with other comparisons.

**Why EXPR_EMIT_STR is needed:** Phase 4's type-agnostic `EXPR_EMIT` works because
int/float share the `r_int64` representation in both ExprVM registers and
RowBuilder `_lo[]`. Strings are different — they use `_strs[]` via
`append_string(val_str)`, not `append_int`. So string output columns in computed
projections require their own emit opcode.

**Scope:** string column-to-column and string column-to-constant comparisons only.
String arithmetic (concatenation) is out of scope.

#### 5B — Equijoin operators (RPython)

New operators: `op_equijoin_delta_trace` and `op_equijoin_delta_delta`. These are
**new implementations**, not wrappers around the existing PK-based join operators.

**Why new operators:** The existing `op_join_delta_trace` matches rows by PK —
it calls `delta_batch.get_pk(i)` and `trace_cursor.seek(key)`. Equijoin matches
on arbitrary key columns that may not be the PK. The hashing must happen inline
per row during the join loop.

**Implementation approach:** For each input row, compute a U128 key from the
specified columns using the same Murmur3 hashing as `_extract_group_key` in
`reduce.py` (which already handles int, u128, and string columns). Then:

- `op_equijoin_delta_trace`: For each delta row, hash left key cols → U128.
  Build a temporary sorted structure (or hash table) from the trace's key col
  hashes, then match. The trace cursor cannot be used directly (it's sorted by
  the trace's PK, not by the join key hash).
- `op_equijoin_delta_delta`: Sort-merge or hash join on key col hashes.

**Output schema:** Cannot reuse `merge_schemas_for_join` (it requires matching PK
types on both sides). Equijoin needs its own schema builder:
- Output PK = synthetic U128 hash of (left_PK, right_PK)
- Output columns = left PK + left payloads + right payloads
- Duplicate column names disambiguated by table alias prefix (resolved at bind
  time in the Rust planner)

New opcodes: `OP_EQUIJOIN_DELTA_TRACE = 22`, `OP_EQUIJOIN_DELTA_DELTA = 23`.

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
22=OP_EQUIJOIN_DELTA_TRACE (Phase 5)
23=OP_EQUIJOIN_DELTA_DELTA (Phase 5)
```
Note: Phase 4 computed projections reuse OPCODE_MAP (2) — no new DBSP opcode.
Phase 4 adds expression opcodes EXPR_EMIT (32) and EXPR_INT_TO_FLOAT (33) — these
are in the ExprVM opcode namespace, NOT the DBSP VM namespace.
20/21 are EXCHANGE_SHARD/GATHER — do NOT use them for equijoin.

**Instruction format:** New instructions must carry key column indices. The
`Instruction` class needs new fields (`left_key_cols`, `right_key_cols`) or the
key cols can be encoded as params and passed to the operator at construction time
(similar to how reduce receives `group_cols`).

#### 5C — Rust planner additions

**gnitz-core:** new opcode constants + `CircuitBuilder::equijoin(left, right_trace_tid,
left_key_cols: &[usize], right_key_cols: &[usize]) -> NodeId`. Key column indices
encoded as params (similar to `PARAM_SHARD_COL_BASE` for exchange).

**gnitz-sql/src/planner.rs:** Parse `TableWithJoins.joins` in `execute_create_view`.
For each `JoinOperator::Inner(JoinConstraint::On(Expr))`:
- Extract `ON t1.a = t2.b` from the expression.
- Resolve column references to `(table_idx, col_idx)` pairs.
- Emit `CircuitBuilder::equijoin(left_node, right_table_id, left_key_cols, right_key_cols)`.
- Handle multi-table joins by chaining equijoin nodes (left-deep tree).

`JOIN … USING (col)` desugars to `ON t1.col = t2.col`. Cross joins (`CROSS JOIN`,
`JOIN` without `ON`) are rejected (`GnitzSqlError::Unsupported`).

**Schema inference for joins:** The binder resolves all table references in the `FROM`
clause, fetches their schemas, and produces a merged schema for the output. Duplicate
column names are prefixed with their table alias: `t1.id`, `t2.id`. The join output
PK is a synthetic U128 (hash of both table PKs).

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
