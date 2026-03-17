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

### Known limitation: unique index enforcement on row modification (multi-worker)

**Status: must fix.**

When `GNITZ_WORKERS>1`, UPDATE and INSERT-as-UPSERT that change a unique-indexed column
value may silently violate the constraint if the conflicting row lives on a different worker.

**Root cause:** Both UPDATE and INSERT-with-existing-PK go through the UPSERT path (push
`weight=+1` with same PK). The master's pre-push unique index check must skip these rows —
otherwise every UPSERT (even same-value) would false-positive. The worker-level check in
`ingest_projection` runs after `_enforce_unique_pk` retracts the old value and inserts
the new one, but it only sees its local index partition. If the conflicting value is held
by a row on a different worker, the violation is invisible.

**Example:**
```sql
INSERT INTO t VALUES (1, 42);   -- pk=1 on worker A
INSERT INTO t VALUES (2, 99);   -- pk=2 on worker B
UPDATE t SET val = 99 WHERE pk = 1;  -- should fail, but passes if A ≠ B
```

New INSERTs (genuinely new PKs) are fully enforced across all partitions. Only modifications
to existing rows have this gap.

**Fix options:**
1. **Index re-partitioning:** Partition index stores by index key instead of main-table PK.
   The broadcast becomes a split-route, and the worker-level check always sees all entries
   for a given value. Major architectural change — every index write needs cross-partition
   routing.
2. **Two-phase protocol:** After `_enforce_unique_pk` computes the effective delta on the
   worker, send the new index keys back to the master for a second broadcast check before
   committing. Adds an extra IPC round-trip per UPSERT batch.

**Tests:** `test_unique_upsert_to_existing_value` and `test_unique_update_to_existing_value`
in `test_indices.py` are marked `xfail` for `GNITZ_WORKERS>1`.

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

## Gap Analysis for CREATE VIEW

### 1. General equi-join — ✅ Phase 5

Pre-indexing approach: `PARAM_REINDEX_COL` on MAP changes output PK to join key value.
Existing PK-based join operators work unchanged. No new DBSP opcodes.

### 2. Computed projections — ✅ Phase 4

`EXPR_EMIT` (32) for computed columns, `EXPR_COPY_COL` (34) for pass-through.
Shadow null registers for SQL NULL propagation. Type-aware `compile_bound_expr`.

### 3. String predicates — ✅ Phase 5

6 fused opcodes (40-45) on raw column buffer memory. No string register file.
`const_strings` pool in `CircuitParamsTab` via nullable STRING column.

### 4. LEFT OUTER JOIN — Phase 6

Decomposed circuit: equijoin ∪ (anti-join → null-fill map).
Requires `EXPR_EMIT_NULL` opcode (see Phase 6C).

### 5. Aggregates (COUNT, SUM, MIN, MAX, AVG) — Phase 6

COUNT/SUM/MIN/MAX exist in `UniversalAccumulator`. AVG decomposes to SUM/COUNT at the
SQL planner level (both linear) + a post-reduce MAP for division. No new RPython
accumulator class needed. Multi-accumulator reduce (list of `UniversalAccumulator`)
handles multiple aggregates in a single pass. See Phase 6A.

---

## ExprBuilder — opcode reference

| Group | Opcodes | Status |
|---|---|---|
| Column loads | `LOAD_COL_INT` (1), `LOAD_COL_FLOAT` (2) | ✅ |
| Constant load | `LOAD_CONST` (3) — full i64 via lo/hi split | ✅ |
| Integer arithmetic | `INT_ADD` (4)–`INT_NEG` (9) | ✅ |
| Float arithmetic | `FLOAT_ADD`–`FLOAT_NEG` (10–14) | ✅ |
| Integer comparison | `CMP_EQ`–`CMP_LE` (15–20) | ✅ |
| Float comparison | `FCMP_EQ`–`FCMP_LE` (21–26) | ✅ |
| Boolean | `BOOL_AND/OR/NOT` (27–29) | ✅ |
| Null checks | `IS_NULL` (30), `IS_NOT_NULL` (31) | ✅ |
| Output | `EMIT` (32) — int/float col write | ✅ |
| Cast | `INT_TO_FLOAT` (33) — SQL type promotion | ✅ |
| Pass-through | `COPY_COL` (34) — type-dispatched direct copy (all types) | ✅ |
| | 35–39 reserved | — |
| String predicates | `STR_COL_EQ/LT/LE_CONST` (40–42), `STR_COL_EQ/LT/LE_COL` (43–45) | ✅ |
| Null output | `EMIT_NULL` (46) — append NULL to output column | Phase 6 |

---

## Phased Plan

---

### Phase 1 — DDL + INSERT + simple SELECT + PK seek ✅

**Commit:** `0de22e3` — 11 integration tests, 103 total tests pass.

`CREATE TABLE`, `DROP TABLE/VIEW`, `INSERT INTO VALUES`, `SELECT * FROM`,
PK seek (`WHERE pk = literal`), `LIMIT`, `CREATE VIEW ... WHERE`.

Key design: `SqlPlanner<'a>` with `&'a GnitzClient` (per-call construction).
PK coercion: I8→U8 … I64→U64 at CREATE TABLE time.
`build_projection` always places source PK at output position 0.

---

### Phase 2 — Secondary index infrastructure + indexed SELECT ✅

**Commit:** `a11c02b` — `CREATE/DROP INDEX`, indexed SELECT via `FLAG_SEEK_BY_INDEX=256`.
`Binder::find_index()` with cache. Index naming: `"{schema}__{table}__idx_{col}"`.

**Remaining low-priority TODOs:**
- Explicit `FLAG_SCAN` dispatch in executor (currently falls through)
- `set_seek_key` convenience accessors in header.rs

---

### Phase 3 — UPDATE + DELETE ✅

**Commits:** `00296f3`, `f557c3d` — 16 tests in `test_dml.py`.

4 UPDATE paths (PK seek, unique index, full scan, no WHERE) + 5 DELETE paths
(PK eq, PK IN, unique index, full scan, no WHERE). All client-side in Rust,
no new IPC operations or RPython code.

UPDATE pushes weight=+1 with same PK; `unique_pk=true` auto-retracts.
Non-unique indexes fall back to full scan (single-match `seek_by_index` limitation).

---

### Phase 4 — Computed projections in CREATE VIEW ✅

**Commit:** `9f04e85` — 22 new tests.

`SELECT id, a + b AS total FROM t` in CREATE VIEW. No new DBSP opcode —
`PARAM_EXPR_NUM_REGS > 0` on MAP node discriminates expr-map from projection-map.

**Key additions:**
- `EXPR_EMIT` (32): writes computed register value to output (int/float only)
- `EXPR_INT_TO_FLOAT` (33): SQL type promotion (`cvtsi2sd`)
- `EXPR_COPY_COL` (34): type-dispatched direct copy (all types including string/U128)
- Shadow null registers: `null = [False] * num_regs` for SQL NULL propagation
- `ExprMapFunction(ScalarFunction)` — plugs into existing `op_map`
- `compile_bound_expr` returns `(u32, bool)` — bool tracks float type; fixes pre-existing
  float comparison bug in WHERE predicates (signed i64 comparison of IEEE 754 bits)
- Strict output column ordering: EMIT/COPY_COL must appear in sequential payload order
- Capacity: 48 four-word instructions (192 bytecode slots, opcodes 64-255)

---

### Phase 5 — String predicates + equijoins in CREATE VIEW ✅

**Commit:** `e663604` — `WHERE name = 'Alice'`, `JOIN ON t1.cid = t2.id`.

**5A — Fused string comparison opcodes:**
6 opcodes (40-45): STR_COL_EQ/LT/LE_CONST, STR_COL_EQ/LT/LE_COL. NE/GT/GE composed
via BOOL_NOT and operand swap. Zero-allocation via `compare_structures`/`string_equals`
on raw column buffer memory. No string register file.
`const_strings` pool: nullable STRING column added to `CircuitParamsTab`
(`PARAM_CONST_STR_BASE = 160`).

**5B — Equijoins via pre-indexing:**
`PARAM_REINDEX_COL` (10) on MAP: output PK becomes join key value (promoted to U128).
Single circuit per join view with two input paths + delta-delta path + union.
`PARAM_JOIN_SOURCE_TABLE` (11) tags each SCAN_TRACE with its source table ID.
`source_reg_map` in ExecutablePlan routes deltas to correct input register.
Integer keys: exact matching (zero hash collisions). String keys: xxh64 → U128.
O(D log N) per step via trace cursor seek (vs O(N) for hash join).

**5C — Rust planner:** `extract_equijoin_keys`, multi-input circuit emission,
post-join projection to canonical output schema. Multi-table joins: left-deep chain.

---

### Phase 6 — Aggregates + set operations + LEFT JOIN + CTEs

**Goals:**
- `CREATE VIEW v AS SELECT cat, COUNT(*), SUM(val), AVG(price) FROM t GROUP BY cat`
- `HAVING`
- `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT`
- `SELECT DISTINCT`, `COUNT(DISTINCT col)`
- `LEFT JOIN`
- `WITH cte AS (…) SELECT …`
- `IN (subquery)`, `EXISTS (…)`

**Design principle:** compose existing operators. Phase 6 adds ~50 lines of RPython
(multi-accumulator reduce widening + `EXPR_EMIT_NULL` + `AGG_COUNT_NON_NULL`) and
delegates everything else to Rust planner circuit-wiring over existing operators:
MAP (Phase 4), reindex MAP (Phase 5), op_union, op_distinct, anti/semi-join,
op_filter, ExprMapFunction, ExprPredicate.

#### Prerequisite: exchange/reduce hash consistency fixes

Two latent inconsistencies between `exchange.py` and `reduce.py` that will become bugs
when GROUP BY is exposed via SQL (string and U128 group-by columns route rows to
different workers than where reduce stores results). **Must be fixed before 6A.**

**1. String group-by columns:**
`exchange.py:56` reads raw integer bits from the column buffer via `_read_col_int`.
`reduce.py:300-313` hashes actual string content via `xxh.compute_checksum`.
Different hash inputs → different partition assignment for the same string value.

Fix: add string content hashing to `hash_row_by_columns` in exchange.py, matching
reduce.py's approach (access `get_str_struct`, hash via `xxh.compute_checksum`).

**2. U128 single-column group-by:**
`reduce.py:293-294` fast-path returns the raw 128-bit value as PK.
`exchange.py:45-50` has no U128 fast-path — falls through to general path which reads
only the low 64 bits, then Murmur3-combines. Different hash → different partition.

Fix: add `if col_type == types.TYPE_U128.code:` fast-path to exchange.py returning
`_partition_for_key(lo, hi)` matching reduce's raw-value semantics.

#### 6A — Aggregates: multi-accumulator reduce + AVG decomposition

**Core insight:** multiple aggregates per GROUP BY run in a **single reduce pass**
via a list of accumulators. AVG decomposes to SUM + COUNT (both linear) at the SQL
planner level, with division handled by a post-reduce MAP using the existing ExprVM.
No new RPython accumulator class. No new operators. The three most common SQL
aggregates (COUNT, SUM, AVG) all use the **fast O(delta) linear merge path**.

**Existing infrastructure (no changes needed):**
- `UniversalAccumulator` (`functions.py:133`) supports COUNT(1), SUM(2), MIN(3), MAX(4)
- `CircuitBuilder::reduce()` (`circuit.rs:174`) takes `agg_func_id` + `agg_col_idx`
- `compile_from_graph` (`program_cache.py:540`) reads `PARAM_AGG_FUNC_ID` and
  instantiates `UniversalAccumulator` with the correct function — this is live, NOT a stub
- `ExprMapFunction` (Phase 4) handles arbitrary scalar expressions on operator output

**Multi-accumulator reduce (~40 lines RPython):**

The reduce currently takes one `AggregateFunction` and produces one aggregate column.
Widen it to take a list of `UniversalAccumulator` and produce N aggregate columns:

1. **`_build_reduce_output_schema`** (`types.py:223`): currently appends one
   `ColumnDefinition(agg_func.output_column_type())`. Change: append N columns,
   one per accumulator in the list.

2. **`ReduceAccessor`** (`reduce.py:39`): currently maps one output position to `-1`
   (the aggregate value). Extend to map N positions using negative sentinel values:
   `-1` for agg[0] (backward compatible), `-3` for agg[1], `-4` for agg[2], etc.
   (`-2` is already "synthetic PK"). Dispatch formula: if `src == -1` then `agg_idx=0`,
   if `src <= -3` then `agg_idx = -src - 2`. The `get_int()` / `get_float()` / `is_null()`
   methods index into the accumulator list by `agg_idx`.

3. **Inner loop** (`reduce.py:382-395`): currently `agg_func.step(acc_in, weight)`.
   Change: `for agg in agg_funcs: agg.step(acc_in, weight)`. Same row, all accumulators
   stepped. Single pass over the data.

4. **Linear merge** (`reduce.py:411-413`): currently reads one `old_val_bits` from
   `output_schema.columns[-1]`. Extend: for each accumulator, read its corresponding
   column from the history row and merge independently. The history table's schema has
   all N aggregate columns.

   ```python
   if all_linear and has_old:
       for i in range(len(agg_funcs)):
           old_bits = trace_out_accessor.get_int(agg_col_positions[i])
           agg_funcs[i].merge_accumulated(old_bits, r_int64(1))
   ```

5. **Non-linear replay** (`reduce.py:490-497`): already iterates consolidated rows.
   Change: reset and step all accumulators. `all_linear = all(agg.is_linear() for agg
   in agg_funcs)` — if any accumulator is non-linear (MIN/MAX), the replay path runs
   for all, which is correct since replay visits every row in the group anyway.

6. **Retraction** (`reduce.py:397-408`): currently reads one old value and emits `-1`.
   Extend: `set_context` receives all old values; `ReduceAccessor` resolves each
   aggregate column to its old value for the retraction row.

**`is_linear()` for the composite:** True iff ALL accumulators are linear. Consequence:
queries using only COUNT + SUM + AVG (the common case) always take the fast linear
merge path. Only queries including MIN or MAX fall back to the non-linear replay.

**AVG — zero RPython changes:**

AVG is non-linear as a single accumulator (partial averages can't be combined by
weighted addition). But it decomposes into two **linear** accumulators:

```
AVG(col) = SUM(col) / COUNT(col)
```

The Rust planner emits this circuit:

```
input → EXCHANGE_SHARD(group_cols)
      → REDUCE([SUM(col), COUNT(col)])      ← both linear, fast merge path
      → MAP(ExprProgram:                     ← Phase 4 ExprMapFunction
            INT_TO_FLOAT(LOAD_COL(sum_col))
          / INT_TO_FLOAT(LOAD_COL(count_col))
          → EMIT)
      → output
```

The MAP runs once per group per step (negligible cost). The division uses existing
ExprVM opcodes (`LOAD_COL_INT`, `INT_TO_FLOAT`, `FLOAT_DIV`, `EXPR_EMIT`). No new
RPython accumulator. No non-linear replay. The reduce stays on the fast linear path.

**Full example — `SELECT cat, COUNT(*), SUM(val), AVG(price) FROM t GROUP BY cat`:**

```
input → EXCHANGE_SHARD(cat)
      → REDUCE([COUNT_STAR, SUM_val, SUM_price, COUNT_price])  ← all LINEAR
      → MAP(ExprProgram:
            COPY_COL(cat),
            COPY_COL(count_star),
            COPY_COL(sum_val),
            INT_TO_FLOAT(LOAD_COL(sum_price)) / INT_TO_FLOAT(LOAD_COL(count_price)) → EMIT)
      → output
```

Intermediate reduce schema: `(group_hash PK, cat, count_star, sum_val, sum_price,
count_price)` — 6 columns, all linear accumulators.
Final output: `(group_hash PK, cat, count_star, sum_val, avg_price)` — 5 columns.

**COUNT(col) vs COUNT(*) — `AGG_COUNT_NON_NULL` (~3 lines RPython):**

`UniversalAccumulator.step()` for AGG_COUNT (functions.py:153) never checks `is_null`
— it always increments. This is correct for `COUNT(*)` but wrong for `COUNT(col)`
when col is nullable (e.g., after LEFT JOIN produces NULLs for unmatched right columns).

Fix: add `AGG_COUNT_NON_NULL = 5`. Identical to AGG_COUNT but with a NULL check:

```python
elif op == AGG_COUNT_NON_NULL:
    if not row_accessor.is_null(self.col_idx):
        self._acc = r_int64(intmask(self._acc + weight))
```

`is_linear()` returns True for AGG_COUNT_NON_NULL (same algebraic property as COUNT).
The SQL planner emits AGG_COUNT for `COUNT(*)` (using pk_index, never null) and
AGG_COUNT_NON_NULL for `COUNT(col)` when col is nullable.

**Aggregate result types (complete table):**

| Function | Integer input | Float input |
|----------|--------------|-------------|
| COUNT(*) | I64          | I64         |
| COUNT(col) | I64        | I64         |
| SUM      | I64          | F64         |
| MIN      | same as input| same as input|
| MAX      | same as input| same as input|
| AVG      | F64 (post-reduce MAP) | F64 (post-reduce MAP) |

AVG is not a row in the accumulator table because it does not exist as an accumulator.
It is decomposed to SUM + COUNT at the SQL planner level.

**Parameter encoding for multi-accumulator reduce:**

```python
PARAM_AGG_COUNT     = 12      # number of accumulators (new)
PARAM_AGG_SPEC_BASE = 13      # PARAM_AGG_SPEC_BASE + i = pack(func_id, col_idx)
```

Each accumulator spec packed as `(func_id << 32) | col_idx` in a single u64.
`compile_from_graph` reads `PARAM_AGG_COUNT`, loops to unpack, and builds the list
of `UniversalAccumulator` instances.

Backward compatible: when `PARAM_AGG_COUNT` is absent but `PARAM_AGG_FUNC_ID > 0`,
fall back to the current single-accumulator path (existing behavior unchanged).

**Rust planner work:**
- Parse `GroupByExpr` + aggregate function calls in `execute_create_view`
- Currently rejects GROUP BY with `GnitzSqlError::Unsupported` (planner.rs:169, 428)
- Detect `AVG(col)` → emit `SUM(col)` + `COUNT(col)` accumulators in the reduce,
  plus a post-reduce MAP with ExprProgram for `sum / count` division
- Detect `COUNT(col)` where col is nullable → emit `AGG_COUNT_NON_NULL`
- `CircuitBuilder::reduce()`: extend to accept `Vec<(u64, usize)>` aggregate specs,
  emit `PARAM_AGG_COUNT` + `PARAM_AGG_SPEC_BASE + i` params
- Rust constants: `AGG_COUNT=1, AGG_SUM=2, AGG_MIN=3, AGG_MAX=4,
  AGG_COUNT_NON_NULL=5`

**HAVING — falls out for free:**

`SELECT cat, COUNT(*) FROM t GROUP BY cat HAVING COUNT(*) > 5` →
`REDUCE → FILTER(ExprPredicate: count_col > 5)`. Existing `op_filter` +
`ExprPredicate`. Zero new code. The Rust planner emits a FILTER node after the
reduce (and after the post-reduce MAP, if AVG is present).

**COUNT(DISTINCT col) — falls out for free:**

`SELECT COUNT(DISTINCT cat) FROM t` →
`MAP(reindex by cat) → DISTINCT → REDUCE([COUNT_STAR])`. Existing reindex MAP
(Phase 5) + `op_distinct` + reduce. Zero new code.

#### 6B — Set operations (Rust planner, no RPython changes)

All set operations compose from existing operators + Phase 5 reindexing infrastructure.
No new RPython code.

**`UNION ALL`** → `op_union`. Correct as-is (Z-set addition, weights sum).

**`UNION`** → reindex both inputs by all-column hash → `op_union` → `op_distinct`.

Bare `op_union + op_distinct` is insufficient for the general case: two subqueries
over different tables may produce identical rows with different PKs. These are distinct
Z-set elements, so `op_distinct` alone won't collapse them. Reindexing both sides by
all-column hash (same Phase 5 `PARAM_REINDEX_COL` infrastructure as equijoins) makes
identical rows share the same PK, so `op_distinct` correctly deduplicates.

For integer-only rows, the reindex uses collision-free encoding (column values
concatenated into U128). For mixed/string types, xxh64 hash — same collision tradeoff
as Phase 5 string equijoins.

**`SELECT DISTINCT`** → reindex by all-column hash + `op_distinct`.

`op_distinct` operates on Z-set element identity (PK + all payloads). After projection,
two rows with different PKs but identical projected columns remain distinct Z-set
elements — `op_distinct` alone won't deduplicate them. The correct pattern: reindex
via `map_reindex` with a hash of all projected columns as the new PK, then apply
`op_distinct`. Same reindexing infrastructure as Phase 5 equijoins.

**`INTERSECT`** → reindex both inputs by all-column hash + semi-join.

SQL INTERSECT compares full-row identity, not PK. Two tables may have different PKs
for rows with identical column values. Reindex both sides by hash of all columns
(making full-row identity the PK), then semi-join on the synthetic PK. Same pattern
as equijoin pre-indexing from Phase 5. No new RPython operators.

**`EXCEPT`** → reindex both inputs by all-column hash → negate second → union → distinct.

Uses Z-set algebra directly: `A - B` in set semantics is `A + (-B)` followed by distinct.
After reindexing both sides by all-column hash (so identical rows share the same PK):
- Rows only in A: weight 1 → kept by distinct
- Rows in both A and B: weight 1 + (-1) = 0 → removed by distinct
- Rows only in B: weight -1 → removed by distinct

This is simpler than anti-join (fewer circuit nodes: reindex MAP + negate + union +
distinct vs anti-join delta-trace + anti-join delta-delta + traces) and naturally extends
to N inputs: `EXCEPT(A, B, C)` = `A + (-B) + (-C) → distinct`. Uses only existing
operators: `op_negate`, `op_union`, `op_distinct`. Same approach as Feldera's `visitMinus`.

**`INTERSECT ALL` / `EXCEPT ALL`:** For `unique_pk=true` tables (the default), all
weights are 1, so ALL variants are equivalent to their non-ALL counterparts (semi-join
for INTERSECT, negate + union + distinct for EXCEPT give correct results). Multiset
variants (weight > 1, `unique_pk=false`) require custom min/max-weight logic and are
out of scope.

Note: the `SetExpr` enum in sqlparser handles these. Currently `execute_select` only
matches `SetExpr::Select` (dml.rs:191). Extend to match `SetExpr::SetOperation`.

#### 6C — LEFT JOIN (Rust planner + one new ExprVM opcode)

**Decomposed circuit:** equijoin ∪ (anti-join → null-fill map → union).

The anti-join output has left-schema only. The null-fill map must produce output with
left columns + NULL right columns (more output columns than input). `ExprMapFunction`
supports variable output width (builder is independent of input schema), but needs a
way to emit NULL values for arbitrary column positions.

**New opcode — `EXPR_EMIT_NULL` (46):**

```python
EXPR_EMIT_NULL = 46  # builder.append_null(a1); a1 = payload_col_idx
```

One new opcode, ~3 lines in `eval_expr_map`. The null-fill ExprProgram consists of:
`COPY_COL` for each left column, then `EMIT_NULL` for each right column position.
Output schema: left columns (original types) + right columns (all marked nullable).

**LEFT JOIN + GROUP BY composition:** The null-fill MAP produces NULLs for right-side
columns. Aggregates on these columns must handle NULLs correctly:
- `COUNT(*)` (AGG_COUNT): counts all rows including NULLs → correct (returns group size)
- `COUNT(right_col)` (AGG_COUNT_NON_NULL): skips NULLs → correct (returns 0 for
  unmatched groups)
- `SUM/MIN/MAX(right_col)`: already skip NULLs (functions.py:153) → correct

**Performance note:** The decomposition runs the input through both equijoin and
anti-join (two passes over the delta). A dedicated `op_left_join_delta_trace` operator
(single-pass) is possible but a larger change. Decomposition is correct-first; the
dedicated operator can be added later via `plans/join-optimizations.md`.

**Opcode numbering:** `plans/join-optimizations.md` proposed opcodes 20/21 for
dedicated LEFT JOIN operators, but those are already taken by `OPCODE_EXCHANGE_SHARD`
(20) / `OPCODE_EXCHANGE_GATHER` (21). Future dedicated operators should use 22/23.

#### 6D — CTEs + subqueries (Rust planner, no RPython changes)

**Non-recursive CTEs — circuit DAG handles it natively:**

`compile_from_graph` already supports DAG topology: a single circuit node can feed
multiple downstream consumers via `source_reg_map` and per-port edge iteration
(program_cache.py:758). A non-recursive CTE is simply a subgraph node in the circuit.

`WITH cte AS (SELECT * FROM t WHERE x > 10) SELECT cte.a, COUNT(*) FROM cte GROUP BY cte.a`
compiles to:

```
t → FILTER(x > 10)           ← CTE "cte", just a circuit node
  → EXCHANGE_SHARD(a)
  → REDUCE([COUNT_STAR])
  → output
```

No temporary view, no table ID allocation. The Rust planner:
1. Parses `WITH` clauses, compiles each CTE as a circuit subgraph
2. Tracks `cte_name → node_id` in a name-resolution map within the planner
3. Resolves CTE references during outer query compilation to the subgraph's output node

If the same CTE is referenced multiple times, the graph fans out naturally (one node
feeding N consumers). This is ~50 lines of Rust planner code with no RPython changes.

For direct `SELECT ... WITH cte AS (...)` (not CREATE VIEW), the ephemeral view
approach (6E) wraps it transparently.

Recursive CTEs (`WITH RECURSIVE`) are out of scope (require fixpoint operators).

**Subqueries:**
- `IN (subquery)` → semi-join. The subquery compiles as a circuit subgraph; its output
  becomes the right side of a semi-join with the outer query's input. For
  `WHERE col IN (SELECT x FROM t2 WHERE ...)`, t2 is a registered table. The circuit:
  `t2 → FILTER → semi-join with main_table → output`.
- `EXISTS (subquery)` → semi-join. Uncorrelated `EXISTS` is a boolean check (trivial).
  Correlated `EXISTS (SELECT 1 FROM t2 WHERE t2.fk = t1.pk)` is detected by the
  planner as an equi-semi-join on the correlation column — same pattern as `IN`.
- `NOT IN (subquery)` → anti-join with NULL filter on subquery output (see below).

**`NOT IN` NULL semantics — pragmatic approach with documented deviation:**

SQL `NOT IN` with NULLs in the subquery result should return zero rows for ALL outer
values (UNKNOWN → FALSE). The NULL-filtering `op_filter` on the subquery output before
the anti-join is too permissive: it removes NULLs and then matches against the
remaining non-NULL values, potentially returning rows that strict SQL would exclude.

This is a known semantic deviation, documented to users. The pragmatic fix:
1. Default: NULL filter + anti-join (correct when subquery has no NULLs — the common case)
2. Documentation: advise `NOT EXISTS` for NULL-safe semantics
3. Full NULL-aware anti-join is deferred

Correlated subqueries and scalar subqueries (`SELECT (SELECT ...)`) are out of scope;
emit `GnitzSqlError::Unsupported`.

#### 6E — Ephemeral SELECT (opt-in, controlled by `SqlPlanner` flag)

For users who want ad-hoc `SELECT … WHERE non_indexed / JOIN / GROUP BY` semantics,
the planner can create a temporary view, scan it, then drop it. This is opt-in via a
`SqlPlanner` option `allow_ephemeral: bool` (default: false). The user pays one
full-table pass as setup cost. Ephemeral views are never the default.

#### Phase 6 RPython change summary

| Change | File | Lines | Purpose |
|--------|------|-------|---------|
| Multi-accumulator reduce | `reduce.py` | ~30 | List of accumulators, loop in step/merge/replay |
| `ReduceAccessor` N-agg mapping | `reduce.py` | ~10 | Negative sentinel dispatch to accumulator list |
| `_build_reduce_output_schema` | `types.py` | ~5 | Accept list of agg_funcs, append N columns |
| `compile_from_graph` multi-agg | `program_cache.py` | ~15 | Read PARAM_AGG_COUNT + PARAM_AGG_SPEC_BASE |
| `AGG_COUNT_NON_NULL` | `functions.py` | ~3 | NULL-skipping COUNT for COUNT(col) |
| `EXPR_EMIT_NULL` (46) | `expr.py` | ~3 | Append NULL to output column for LEFT JOIN |
| Exchange hash: string + U128 | `exchange.py` | ~15 | Prerequisite consistency fixes |
| **Total** | | **~80** | |

Everything else is Rust planner circuit-wiring over existing operators.

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
- **`INTERSECT ALL` / `EXCEPT ALL` on multisets** — for `unique_pk=true` (default),
  ALL variants are equivalent to non-ALL. Multiset weight-aware variants are deferred.
- **`WITH RECURSIVE`** — requires fixpoint operators not present in gnitz DBSP.
- **Correlated subqueries / scalar subqueries** — `SELECT (SELECT ...)` and correlated
  `WHERE` subqueries require per-row evaluation; emit `GnitzSqlError::Unsupported`.
- **Full NULL-aware `NOT IN`** — `NOT IN (subquery)` uses NULL-filter + anti-join
  (correct when subquery has no NULLs). Strict SQL semantics (return empty when any
  NULL in subquery) deferred. Use `NOT EXISTS` for NULL-safe semantics.
