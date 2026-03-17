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

### 1. General equi-join — Phase 5 (pre-indexing approach, no new DBSP operators)

SQL `JOIN ON t1.a = t2.b` requires matching on arbitrary columns. The original plan
proposed new `op_equijoin_delta_trace` + `op_equijoin_delta_delta` operators with
inline hash table construction. **This was superseded** by a pre-indexing approach
after discovering that gnitz Z-sets do NOT enforce unique PKs at the storage level
(see §5B for full analysis).

**Key insight:** gnitz consolidation groups by full row identity (PK + all payload
columns), not PK alone. `compare_indices` → `core_comparator.compare_rows` compares
every non-PK column when PKs match. Two rows with the same PK but different payloads
survive as independent Z-set elements. Cursors iterate all same-PK entries:
`op_join_delta_trace` already has `while cursor.key() == key: advance()`.

**Approach:** A MAP variant with `PARAM_REINDEX_COL` changes the output PK to a
specified column's value (promoted to U128). This reindexes a relation by join key.
The existing PK-based join operators then work unchanged — the trace is sorted by
join key (the new PK), cursor seeks are O(log N), multiple rows with the same join
key but different payloads coexist as separate Z-set elements.

No new DBSP opcodes. No new RPython join operators. Just one MAP parameter.

Rust: `CircuitBuilder::map_reindex(input, reindex_col_idx, type_code)` — emits MAP
node with `PARAM_REINDEX_COL` set.

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

### 3. String predicates in ExprVM — Phase 5 (fused opcodes, no string registers)

ExprVM (31 base opcodes + EXPR_EMIT (32), EXPR_INT_TO_FLOAT (33), EXPR_COPY_COL (34)
from Phase 4) has no string expression opcodes. String **predicates** in views
(`WHERE name = 'Alice'`) require new opcodes (40-45) for comparing strings. String
**pass-through** in computed projections (`SELECT name, a + b AS total FROM t`) is
already handled by Phase 4's `EXPR_COPY_COL` — no Phase 5 dependency for that case.

**Original plan (superseded):** LOAD_COL_STR, LOAD_CONST_STR, STR_EQ..GE, EMIT_STR
with a separate `str_regs = [""] * N` string register file and a three-way TypeTag
in `compile_bound_expr`. This was replaced by fused comparison opcodes after
discovering that gnitz already has zero-allocation string comparison infrastructure
(`compare_structures` and `string_equals` in `gnitz/core/strings.py`) that operates
directly on raw column buffer memory via `accessor.get_str_struct()`, without
materializing Python string objects. See §5A for full analysis.

**Revised design:** 6 fused opcodes (40-45). Each reads column values and/or string
constants inline, compares via `compare_structures`/`string_equals`, writes 0/1 to
an integer register. No string register file. No LOAD/EMIT opcodes. The register
file stays homogeneous `[r_int64]`. `compile_bound_expr` stays `(u32, bool)`.

**const_strings pool:** Stored in `CircuitParamsTab` via a new nullable STRING column
`str_value`. String constants are just param rows with `str_value` populated. No new
system table needed.

UPDATE/DELETE WHERE filtering is done **client-side** in the Rust evaluator and does not
use ExprVM. String opcodes are needed only for server-side view predicates.

String join predicates (`ON t1.name = t2.name`) are handled at the circuit level by
pre-indexing both inputs with `hash(name)` as the new PK (see §1 above). No ExprVM
string comparison needed for join keys.

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

### Phase 5 — String predicates + equijoins in CREATE VIEW

**Goals:**
- `CREATE VIEW v AS SELECT * FROM t WHERE name = 'Alice'`
- `CREATE VIEW v AS SELECT t1.id, t2.name FROM t1 JOIN t2 ON t1.cid = t2.id`
- `CREATE VIEW v AS SELECT t1.id FROM t1 JOIN t2 ON t1.name = t2.name`

#### 5A — Fused string comparison opcodes (RPython) — no string register file

**Design revision:** The original plan proposed `str_regs = [""] * N` alongside the
existing `regs = [r_int64(0)] * N`, with LOAD_COL_STR / LOAD_CONST_STR / STR_EQ..GE /
EMIT_STR opcodes (9 total). This was replaced by fused comparison opcodes after
analysis of RPython JIT behavior and discovery of gnitz's existing zero-allocation
string comparison infrastructure.

##### Why string registers are harmful

The ExprVM's performance depends on RPython's JIT virtualizing local arrays into CPU
registers. For `regs = [r_int64(0)] * N`:
- Each element is an unboxed 64-bit integer → lives in a CPU register
- The array never escapes → JIT removes the allocation entirely (`ArrayPtrInfo` virtual)
- Zero GC interaction

A `str_regs = [""] * N` array would be virtualized at the container level (same
`ArrayPtrInfo` handler for `_I` and `_R` variants in `optimizeopt/virtualize.py`),
but the elements are **GC pointers to heap-allocated `rstr.STR` objects**:

1. **Allocation per load:** `LOAD_COL_STR` must call `resolve_string()` →
   `unpack_string()` → `rffi.charpsize2str()` → nursery allocation. The string must
   be a concrete `rstr.STR` because `ll_streq()` dereferences `.chars[j]`.
2. **GC map pressure:** Each `str_regs` slot holding a string reference requires a
   GC map entry for guard recovery frames. `regs` slots (raw integers) do not.
3. **Comparison cost:** RPython's `ll_streq()` is a **function call** (not inlined by
   the JIT for dynamic strings). It dereferences both string pointers, compares lengths,
   then runs a byte loop. Compare to integer `CMP_EQ`: one `CMP` instruction.
4. **No escape analysis help:** `unpack_string()` allocates the string, then it's
   passed to `ll_streq()` — a function call that forces the virtual. The JIT cannot
   remove the allocation.

##### The existing zero-allocation path

gnitz already has `compare_structures()` and `string_equals()` in `gnitz/core/strings.py`
that compare string column values **directly on raw column buffer memory** without
creating Python string objects:

```python
# gnitz/core/strings.py — operates on raw CCHARP pointers, zero allocation
def compare_structures(len1, pref1, ptr1, heap1, str1,
                       len2, pref2, ptr2, heap2, str2):
    # 1. Compare 4-byte prefix integers (single CMP instruction via JIT)
    # 2. If prefixes differ → early exit (covers most unequal strings)
    # 3. Short string (≤4 bytes): prefix was exhaustive, compare lengths
    # 4. Compare suffix bytes via raw pointer arithmetic
    # Returns -1, 0, or 1
```

The input is the 5-tuple from `accessor.get_str_struct(col_idx)`:
```
(length: int, prefix: int, struct_ptr: CCHARP, heap_base_ptr: CCHARP, py_string: str|None)
```

This tuple contains **only integers and raw pointers** — no GC references. The JIT
virtualizes it perfectly (fixed-size struct, never escapes). `compare_structures`
reads bytes directly from the column buffer and blob arena via raw pointer arithmetic.

For column-vs-constant equality, `string_equals(packed_ptr, packed_heap_ptr, search_str)`
is even more optimized — it compares a German String struct against a Python search
string without unpacking.

##### Fused comparison opcodes

6 opcodes, each self-contained. Read directly from accessor/const pool, compare via
`compare_structures`/`string_equals`, write 0/1 to an integer register:

```
EXPR_STR_COL_EQ_CONST = 40   # dst = (col[a1] == const_strings[a2]) ? 1 : 0
EXPR_STR_COL_LT_CONST = 41   # dst = (col[a1] <  const_strings[a2]) ? 1 : 0
EXPR_STR_COL_LE_CONST = 42   # dst = (col[a1] <= const_strings[a2]) ? 1 : 0
EXPR_STR_COL_EQ_COL   = 43   # dst = (col[a1] == col[a2]) ? 1 : 0
EXPR_STR_COL_LT_COL   = 44   # dst = (col[a1] <  col[a2]) ? 1 : 0
EXPR_STR_COL_LE_COL   = 45   # dst = (col[a1] <= col[a2]) ? 1 : 0
```

NE, GT, GE are composed: `NE(a,b) = BOOL_NOT(EQ(a,b))`, `GT(a,b) = LT(b,a)` (swap
operands at compile time), `GE(a,b) = LE(b,a)`. This halves the opcode count vs the
original plan's 6+3 (load/compare/emit) with no expressiveness loss.

**Column vs. constant implementation (eval_expr / eval_expr_map):**

```python
elif op == EXPR_STR_COL_EQ_CONST:
    null[dst] = accessor.is_null(a1)
    if not null[dst]:
        _len, _pref, ptr, heap, pystr = accessor.get_str_struct(a1)
        const_str = program.const_strings[a2]
        regs[dst] = r_int64(1) if string_equals(ptr, heap, const_str) else r_int64(0)
    else:
        regs[dst] = r_int64(0)
```

- **Zero allocation:** `get_str_struct` returns integers + raw pointers (JIT-virtualized
  tuple). `string_equals` compares raw bytes in the column buffer against the constant.
  No `unpack_string`, no `rffi.charpsize2str`, no nursery allocation.
- **Prefix fast-path:** `string_equals` compares the 4-byte prefix integer first —
  one CPU `CMP` instruction rejects most non-matching strings before touching suffix
  bytes.

**Column vs. column implementation:**

```python
elif op == EXPR_STR_COL_EQ_COL:
    null[dst] = accessor.is_null(a1) or accessor.is_null(a2)
    if not null[dst]:
        l1, p1, ptr1, h1, s1 = accessor.get_str_struct(a1)
        l2, p2, ptr2, h2, s2 = accessor.get_str_struct(a2)
        regs[dst] = r_int64(1) if compare_structures(
            l1, p1, ptr1, h1, s1, l2, p2, ptr2, h2, s2
        ) == 0 else r_int64(0)
    else:
        regs[dst] = r_int64(0)
```

Both 5-tuples are JIT-virtualized (10 integer/pointer values on the stack).
`compare_structures` operates on raw memory. Zero GC interaction.

**LT/LE variants** use the same pattern but check `compare_structures(...) < 0`
or `compare_structures(...) <= 0`.

##### What this eliminates

- ~~`str_regs = [""] * N`~~ — no string register file
- ~~`EXPR_LOAD_COL_STR` (40)~~ — columns accessed inline by fused opcodes
- ~~`EXPR_LOAD_CONST_STR` (41)~~ — constants accessed inline from `const_strings` pool
- ~~`EXPR_STR_EQ..GE` (42-47)~~ — replaced by 6 fused column-level opcodes
- ~~`EXPR_EMIT_STR` (48)~~ — `COPY_COL` (Phase 4) handles string output
- ~~Three-way `TypeTag`~~ — `compile_bound_expr` stays `(u32, bool)`

**Performance comparison** (per-row cost for `WHERE name = 'Alice'`):

| | String registers (original) | Fused opcode (revised) |
|---|---|---|
| Heap allocations | 1-3 (charpsize2str) | **0** |
| GC pointers in reg file | 1 | **0** |
| Comparison | `ll_streq()` call (~20 instrs) | `string_equals()` inline (prefix CMP + memcmp) |
| Register file type | Mixed (int64 + GC refs) | **Homogeneous `[r_int64]`** |

##### Null propagation

Same shadow null tracking as Phase 4's integer/float opcodes:
- Column-vs-constant: `null[dst] = accessor.is_null(col_idx)`
- Column-vs-column: `null[dst] = accessor.is_null(a1) or accessor.is_null(a2)`
- Result is always in integer `regs[dst]` — downstream `BOOL_AND`/`BOOL_OR` work
  unchanged.

##### Scope

String column-to-constant and column-to-column **comparisons** only. String
computation (concatenation, SUBSTRING, UPPER) is out of scope. If string computation
is added in a future phase, THAT is when string registers would be needed — and the
requirements will be clearer then.

String **pass-through** in projections (`SELECT name, a + b AS total FROM t`) is
already handled by Phase 4's `EXPR_COPY_COL` opcode.

String **join predicates** (`ON t1.name = t2.name`) are handled at the circuit level
by pre-indexing with `hash(name)` as the new PK (see §5B). No ExprVM string
comparison needed for join keys.

##### compile_bound_expr: no signature change

When the Rust compiler encounters `BinOp(ColRef(name_idx), Eq, LitStr("Alice"))`:
1. Detect left is `TypeCode::String` (from schema), right is `LitStr`
2. Add `"Alice"` to the const_strings pool → `const_idx`
3. Emit `STR_COL_EQ_CONST dst, name_idx, const_idx`
4. Return `(dst, false)` — result is integer 0/1

No `TypeTag` enum. No new return type. String comparisons are pattern-matched at
compile time and lowered directly to fused opcodes. The existing `(u32, bool)` return
works because fused opcodes always produce integer results.

For `WHERE name > 'M'`: the compiler emits `STR_COL_LE_CONST` with swapped sense
(name > 'M' ↔ NOT(name <= 'M')), wraps in `BOOL_NOT`, returns `(result, false)`.

For `WHERE first_name = last_name` (same-row column comparison): both operands are
`ColRef` with `TypeCode::String` → emit `STR_COL_EQ_COL dst, col_a, col_b`.

##### ExprProgram changes

```python
class ExprProgram(object):
    _immutable_fields_ = ['code[*]', 'num_regs', 'result_reg', 'num_instrs',
                          'const_strings[*]']

    def __init__(self, code, num_regs, result_reg, const_strings=None):
        self.code = code[:]
        self.num_regs = num_regs
        self.result_reg = result_reg
        self.num_instrs = len(code) // 4
        self.const_strings = const_strings[:] if const_strings else []
```

The `const_strings` list is immutable (`[*]`), so the JIT can inline constant lookups
when `program` is promoted. For programs without string constants (the vast majority),
`const_strings` is an empty list — zero overhead.

##### Rust ExprBuilder additions

```rust
const EXPR_STR_COL_EQ_CONST: u32 = 40;
const EXPR_STR_COL_LT_CONST: u32 = 41;
const EXPR_STR_COL_LE_CONST: u32 = 42;
const EXPR_STR_COL_EQ_COL:   u32 = 43;
const EXPR_STR_COL_LT_COL:   u32 = 44;
const EXPR_STR_COL_LE_COL:   u32 = 45;

pub fn str_col_eq_const(&mut self, col_idx: usize, const_idx: u32) -> u32 {
    let dst = self.alloc_reg();
    self.emit(EXPR_STR_COL_EQ_CONST, dst, col_idx as u32, const_idx);
    dst
}
// ... analogous methods for LT_CONST, LE_CONST, EQ_COL, LT_COL, LE_COL
```

#### 5A-const — String constant pool: extend CircuitParamsTab

String constants in expressions (e.g., `'Alice'` in `WHERE name = 'Alice'`) must be
persisted alongside the circuit graph and loaded at compile time.

**Design:** Add a nullable STRING column to `CircuitParamsTab`:

```python
# CircuitParamsTab.schema() — revised
cols = [
    ColumnDefinition(TYPE_U128, is_nullable=False, name="param_pk"),
    ColumnDefinition(TYPE_U64,  is_nullable=False, name="value"),
    ColumnDefinition(TYPE_STRING, is_nullable=True, name="str_value"),   # NEW
]
```

String constants are stored as regular param rows with `str_value` populated:
```
(view_id, node_id, PARAM_CONST_STR_BASE + 0) → value=0, str_value="Alice"
(view_id, node_id, PARAM_CONST_STR_BASE + 1) → value=0, str_value="Bob"
```

New constant: `PARAM_CONST_STR_BASE = 160` (slots 160-191, up to 32 string constants
per node — matching the capacity of other PARAM_*_BASE ranges).

**Why this approach (not a new system table):**

- **Zero new tables.** `CircuitGroupColsTab` is the precedent for per-node auxiliary
  data, but adding a table costs ~150 lines of boilerplate (system_tables.py,
  loader.py, metadata.py, types.rs, client.rs, engine.py, program_cache.py). Adding
  one nullable column to an existing table costs ~40 lines.
- **gnitz-native string storage.** The blob arena handles string persistence,
  deduplication, and GC automatically — same as all other string columns.
- **Existing serialization works.** `CircuitParamsTab.append()` gains a `str_value`
  parameter. The Rust client already knows how to push rows with string columns.
- **Existing retraction works.** `_retract_circuit_graph` scans and retracts all
  params for a view — strings are retracted automatically.
- **Clean semantics.** A param row with `str_value=NULL` is a numeric param (existing
  behavior). A param row with `str_value` set is a string constant. No ambiguity.
- **Future-proof.** If other constant types are needed later (dates, blobs), add
  another nullable column. No new tables.

**Loading in program_cache.py:**

`_load_params` returns a second dict alongside the existing params:
```python
def _load_params(self, view_id):
    # ... existing code reads numeric params into result dict ...
    str_params = {}  # node_id → {slot → string}
    # When str_value column is present and non-null:
    #   str_params[node_id][slot - PARAM_CONST_STR_BASE] = str_value
    return result, str_params
```

Then in the FILTER/MAP handler:
```python
const_strings = []
if nid in str_params:
    idx = 0
    while (opcodes.PARAM_CONST_STR_BASE + idx) in str_params[nid]:
        const_strings.append(str_params[nid][opcodes.PARAM_CONST_STR_BASE + idx])
        idx += 1
prog = ExprProgram(code, num_regs, result_reg, const_strings)
```

**Rust CircuitBuilder additions:**

```rust
pub struct CircuitBuilder {
    // ... existing fields ...
    const_strings: Vec<(u64, u64, String)>,  // (node_id, slot, value)
}

impl CircuitBuilder {
    pub fn add_const_string(&mut self, node_id: NodeId, index: u32, value: String) {
        let slot = PARAM_CONST_STR_BASE + index as u64;
        self.const_strings.push((node_id, slot, value));
    }
}
```

Serialization in `create_view_with_circuit`: push const_string entries as
CircuitParamsTab rows with the `str_value` column populated.

**Schema migration:** System tables are created fresh at database initialization.
Existing databases without the new column will not have any string constants in their
circuits (Phase 5 is not yet deployed). The NULL default is correct for all existing
rows.

#### 5B — Equijoins via pre-indexing (no new DBSP operators)

**Design revision:** The original plan proposed two new equijoin operators
(`op_equijoin_delta_trace` + `op_equijoin_delta_delta`, opcodes 22-23) that would
hash join key columns inline and build temporary hash tables per step. This was
replaced by a pre-indexing approach after discovering that **gnitz Z-sets do NOT
enforce unique PKs at the storage level**.

##### The foundational insight: Z-set element identity is (PK + all payloads)

gnitz's `unique_pk` flag is a DML-level enforcement (Stage 0 of `ingest_to_family`
in `registry.py`). At the storage level:

1. **Consolidation groups by full row**, not PK alone.
   `ArenaZSetBatch.to_consolidated()` calls `compare_indices()`, which calls
   `core_comparator.compare_rows(schema, acc_a, acc_b)` when PKs match. Two rows with
   the same PK but different payloads are **independent Z-set elements** — they survive
   consolidation with separate weights.

2. **Cursors iterate all same-PK entries.**
   `SortedBatchCursor.advance()` increments position unconditionally.
   `UnifiedCursor._find_next_non_ghost()` compares via `TournamentTree._compare_nodes`,
   which calls `compare_rows` after PK comparison. Same-PK, different-payload entries
   are emitted separately.

3. **Join operators handle multiple matches per PK.**
   `op_join_delta_trace` (`join.py:124`) already has:
   ```python
   trace_cursor.seek(key)
   while trace_cursor.is_valid():
       if trace_cursor.key() != key:
           break
       # ... process match, emit output row ...
       trace_cursor.advance()
   ```
   It iterates ALL trace entries matching the PK, producing one output per match with
   multiplied weights. `op_join_delta_delta` similarly processes PK groups.

4. **`merge_schemas_for_join` works** when both sides have the same PK type — which
   is guaranteed when both are reindexed by the same join key column type.

This means Feldera's pre-indexing approach (project join key to PK, then use standard
PK-based join) works in gnitz without modification to any join operator.

##### The approach: PARAM_REINDEX_COL on MAP

Add a single parameter to the existing `OPCODE_MAP`: `PARAM_REINDEX_COL`. When set,
`op_map` uses the specified column's value (promoted to U128) as the output PK,
instead of passing through the input PK. The original PK is preserved as a payload
column via `COPY_COL`.

**RPython changes** (`gnitz/dbsp/ops/linear.py`, ~20 lines):

```python
def op_map(in_batch, out_register, func, out_schema, reindex_col=-1):
    for i in range(length):
        if reindex_col >= 0:
            # Read join key column value, promote to U128 for use as new PK
            new_pk = _promote_col_to_u128(accessor, reindex_col, in_schema)
            weight = in_batch.get_weight(i)
            func.evaluate_map(accessor, builder)
            builder.commit_row(new_pk, weight)
        else:
            # Existing behavior: pass through input PK
            pk = in_batch.get_pk(i)
            weight = in_batch.get_weight(i)
            func.evaluate_map(accessor, builder)
            builder.commit_row(pk, weight)
```

`_promote_col_to_u128` promotes the column value to U128 based on type:
- Integer types (I8..U64): zero-extend to U128 (same as `get_index_key_type`)
- U128: direct value
- String: xxh64 hash → U128 (same hash used in `_extract_group_key`)
- Float: bit reinterpretation → U128

New constant: `PARAM_REINDEX_COL = 10` in `opcodes.py`.

**program_cache.py** MAP handler: read `reindex_col` from node_params and pass to
`op_map`:
```python
reindex_col = node_params.get(opcodes.PARAM_REINDEX_COL, -1)
return instructions.map_op(in_reg, out_reg, func, reindex_col=reindex_col)
```

##### Single circuit, multiple entry points

The Rust planner emits **one circuit** per join view containing both join directions
and a delta-delta path. The circuit has **two primary inputs**, one per source table.
At execution time, the triggering delta is bound to the matching input register; the
other input remains empty, and its entire path naturally produces zero output.

**The key property:** every operator in the codebase guards on input length.
`op_map` (`linear.py:61`), `op_join_delta_trace` (`join.py:136`),
`op_join_delta_delta` (`join.py:183`), `op_integrate` (`linear.py:102`),
`op_filter` (`linear.py:31`) — all iterate `for i in range(n)` where `n` is the
input batch length. **Empty in → empty out → zero side effects.** Inactive paths
are completely inert with no code changes to any operator.

##### Circuit pattern for JOIN ON t1.cid = t2.id

One circuit, one `create_view_with_circuit` call, one cached `ExecutablePlan`:

```
input_A (SCAN_TRACE source=0, PARAM_JOIN_SOURCE_TABLE=T1)
input_B (SCAN_TRACE source=0, PARAM_JOIN_SOURCE_TABLE=T2)

                                                                      ┌──────────┐
input_A → reindex(cid) → fork ──→ integrate(T1_reindex_trace)        │          │
                              └──→ join_delta_trace(trace=T2_reindex) │          │
                                     └──→ project_A ─────────────────→│          │
                                                                      │  union   │→ sink(V)
input_B → reindex(id) → fork ──→ integrate(T2_reindex_trace)         │          │
                             └──→ join_delta_trace(trace=T1_reindex)  │          │
                                    └──→ project_B ──────────────────→│          │
                                                                      │          │
join_delta_delta(reindex_A, reindex_B) → project_DD ─────────────────→│          │
                                                                      └──────────┘
```

When T1 is ingested, `execute_epoch` binds ΔT1 to input_A. input_B stays empty.
Tracing the inactive Path B: `reindex(empty)` → empty → `integrate(empty)` → no-op →
`join(empty, T1_trace)` → loop runs 0 times → empty → `project_B(empty)` → empty.
`join_delta_delta(non-empty, empty)` → `while idx_a < n_a and idx_b < n_b` with
n_b=0 → loop never enters → empty. Union receives only Path A's results. ✓

**Why the circuit always produces exactly the right DBSP terms:**

Using the `z^{-1}` formulation (all trace cursors reflect state BEFORE this step,
since `prepare_for_tick` refreshes them once at the start, before any INTEGRATEs):

- Path A: `ΔA ⋈ z^{-1}(I(B))`
- Path B: `ΔB ⋈ z^{-1}(I(A))`
- Path DD: `ΔA ⋈ ΔB`
- Sum: `ΔA ⋈ z^{-1}(I(B)) + z^{-1}(I(A)) ⋈ ΔB + ΔA ⋈ ΔB` = correct incremental join

For cross-table joins (T1 ≠ T2): exactly one input is non-zero per step, so two of
three terms vanish automatically. For self-joins (`FROM t1 a JOIN t1 b ON a.x = b.y`):
both inputs are bound to the same ΔT1, all three terms are computed, and the result
is correct without any special-casing.

**Execution order does not matter** — all trace cursors were created at
`prepare_for_tick` time. INTEGRATEs happen later in the instruction stream but cannot
affect cursors created before them (writes go to the memtable; the cursor was opened
on pre-existing shards). Both directions read `z^{-1}` state regardless of
topological ordering.

##### Circuit structure walkthrough

Each path in the circuit:
1. **Reindexes** its delta via `map_reindex` + `ExprMapFunction` (Phase 4):
   copies the original PK to a payload position via `COPY_COL`, other payload columns
   are copied, and the join key column's value becomes the new PK via
   `PARAM_REINDEX_COL`.
2. **Forks** the reindexed delta to two downstream nodes (two edges from the same
   source in the circuit graph — the VM reads from the same DeltaRegister without
   consuming it).
3. **Integrates** its reindexed delta into a persistent trace table (for the
   other direction's join to seek against in future steps).
4. **Joins** the reindexed delta against the other table's reindexed trace via the
   existing `op_join_delta_trace` — cursor seeks by PK = join key, O(log N).
5. **Projects** the join output to V's canonical output schema. This is needed because
   `CompositeAccessor` puts the delta side's columns first: Path A produces
   `[T1_payloads, T2_payloads]` while Path B produces `[T2_payloads, T1_payloads]`.
   Each path's projection maps its direction-specific column indices to V's column
   order. This reuses the existing MAP/projection infrastructure (PARAM_PROJ_BASE).

The **delta-delta path** takes the two reindexed deltas and joins them directly via
the existing `op_join_delta_delta` (sort-merge join, `join.py:164`). Its output is
projected to V's schema and unioned with the other two paths. For cross-table joins,
this path always receives at least one empty input and produces nothing — zero overhead.

The **union** combines all three projected paths into a single delta written to V's sink.

**Shared reindexed trace tables:** T1_reindexed_trace and T2_reindexed_trace are
persistent tables managed as children of V's store (same pattern as `_hist_` tables
for DISTINCT). Path A writes to T1_reindexed_trace and reads T2_reindexed_trace.
Path B writes to T2_reindexed_trace and reads T1_reindexed_trace. These are created
once during circuit compilation and cleaned up when V is dropped.

##### Infrastructure changes

**`compile_from_graph`** (~15 lines): Handle multiple SCAN_TRACE(table_id=0) nodes.
For each, read `PARAM_JOIN_SOURCE_TABLE` from params, look up the source table's
schema from the registry, create a DeltaRegister with that schema:

```python
if table_id == 0:
    source_tid = node_params.get(opcodes.PARAM_JOIN_SOURCE_TABLE, 0)
    if source_tid > 0:
        node_schema = self.registry.get_by_id(source_tid).schema
    else:
        node_schema = in_schema  # backward compat: single-source views
    reg = runtime.DeltaRegister(reg_id, node_schema)
    cur_reg_file.registers[reg_id] = reg
    source_reg_map[source_tid] = reg_id  # built alongside state
    return None
```

For single-source views (no `PARAM_JOIN_SOURCE_TABLE`), `source_tid=0` falls through
to `in_schema` — fully backward compatible.

**`ExecutablePlan`** (~8 lines): Add `source_reg_map` field (dict or None).
`execute_epoch` gains a `source_id` parameter:

```python
def execute_epoch(self, input_delta, source_id=0):
    self.reg_file.prepare_for_tick()   # all DeltaRegisters clear()'d to empty
    self.context.reset()
    if self.source_reg_map is not None and source_id in self.source_reg_map:
        in_reg_idx = self.source_reg_map[source_id]
    else:
        in_reg_idx = self.in_reg_idx   # single-source fallback
    in_reg = self.reg_file.get_register(in_reg_idx)
    in_reg.bind(input_delta)
    # ... rest unchanged ...
```

**`evaluate_dag`** (~8 lines): Track `source_id` alongside each pending entry.
First-layer entries use `initial_source_id`. Downstream entries use `target_view_id`
(the view that produced the output):

```python
# pending tuples: (depth, view_id, source_id, batch)
for v_id in first_layer:
    pending.append((d, v_id, initial_source_id, initial_delta.clone()))
# ...
depth, target_view_id, source_id, incoming_delta = pending[-1]
out_delta = plan.execute_epoch(incoming_delta, source_id)
# ...
for dep_id in dependents:
    pending.append((d, dep_id, target_view_id, out_delta.clone()))
```

**`opcodes.py`**: +1 constant (`PARAM_JOIN_SOURCE_TABLE = 11`).

**No changes to `get_program`** — one circuit per view_id, one cached plan.
No per-source plan routing. No new system tables. No graph partitioning.

##### Performance: O(D log N), not O(N)

The trace is sorted by the new PK (= join key value). `op_join_delta_trace` seeks
each delta row's PK in the trace via binary search: **O(D log N_trace)** per step,
where D = delta size and N = trace size.

Compare to the original plan's hash-based equijoin operators:
- Hash join: **O(N_trace)** per step — must scan entire trace to build hash map
- Pre-indexing: **O(D log N_trace)** — D << N in the typical DBSP case (small deltas
  against large accumulated state)

Pre-indexing is asymptotically better for the incremental use case.

##### Integer join keys: exact matching, zero hash collisions

For integer join columns (the overwhelming common case):
- The column value IS the new PK (e.g., I64 promoted to U64 in the PK)
- No hashing needed. PK-based join matches exactly by join key value
- Zero collision risk

For string join keys:
- xxh64 hash → U128 (same as `_extract_group_key` in `reduce.py`). Effective
  collision resistance is 64 bits (~2^-64 per pair). xxh64 produces 64 bits; the
  U128 is derived deterministically from those 64 bits via `_mix64`.
- At 1 billion distinct strings, expected collisions ≈ 0.03. Practically negligible.
- For absolute correctness, a post-join filter could verify actual string equality,
  but this is practically unnecessary at any realistic scale.

##### What this eliminates from the original plan

- ~~`op_equijoin_delta_trace`~~ — reuse existing `op_join_delta_trace`
- ~~`op_equijoin_delta_delta`~~ — reuse existing `op_join_delta_delta`
- ~~Opcodes 22, 23~~ — no new DBSP opcodes
- ~~Inline hash table construction~~ — trace IS sorted by join key
- ~~Custom output schema builder~~ — `merge_schemas_for_join` works (matching PK types)
- ~~New `Instruction` fields for key cols~~ — key cols are implicit (the PK)
- ~~`CircuitJoinKeysTab` system table~~ — no per-join key column metadata needed
- ~~Two-circuit persistence~~ — one circuit per view, one cached plan
- ~~Per-source plan routing~~ — bind delta to matching input register
- ~~Multi-input delta routing~~ — inactive paths auto-zero via empty-in/empty-out
- ~~Graph partitioning in compiler~~ — single connected graph, standard topo sort

**Total RPython changes for equijoin support:** ~40 lines in `op_map` (reindex mode
+ `_promote_col_to_u128` + threading through `Instruction`/`interpreter.py`) +
~15 lines in `compile_from_graph` (multi-input handling + source_reg_map) +
~8 lines in `execute_epoch` (source_id param + register selection) +
~8 lines in `evaluate_dag` (source_id tracking in pending tuples) +
1 new constant in `opcodes.py`.

##### Rust CircuitBuilder

```rust
/// Map with PK reindexing. The output PK becomes the value of
/// `reindex_col` (promoted to U128). Used for equijoin pre-indexing.
pub fn map_reindex(
    &mut self, input: NodeId, reindex_col: usize, program: ExprProgram,
) -> NodeId {
    let nid = self.alloc_node(OPCODE_MAP);
    self.connect(input, nid, PORT_IN);
    self.params.push((nid, PARAM_FUNC_ID, 0));
    self.params.push((nid, PARAM_REINDEX_COL, reindex_col as u64));
    self.params.push((nid, PARAM_EXPR_NUM_REGS, program.num_regs as u64));
    for (i, &word) in program.code.iter().enumerate() {
        self.params.push((nid, PARAM_EXPR_BASE + i as u64, word as u64));
    }
    nid
}
```

#### 5C — Rust planner additions

**gnitz-sql/src/planner.rs:** Parse `TableWithJoins.joins` in `execute_create_view`.

For each `JoinOperator::Inner(JoinConstraint::On(Expr))`:
1. Extract equijoin keys via a dedicated `extract_equijoin_keys(on_expr, left_schema,
   right_schema) → Vec<(usize, usize)>` function. Pattern-matches
   `BinaryOp { op: Eq, left: Identifier, right: Identifier }` against both schemas.
   No full binder refactor needed — this is a targeted pattern match.
2. Call `input_delta()` twice, tag each with `PARAM_JOIN_SOURCE_TABLE`.
3. Build reindex MAP for each side: `ExprMapFunction` copies original PK + relevant
   columns to payload, `PARAM_REINDEX_COL` sets the join key as new PK.
4. Emit the full join circuit: two `join_delta_trace` paths (one per direction) +
   one `join_delta_delta` path + integrate nodes for both reindexed traces + post-join
   projections + three-way `union` + sink. All in one `CircuitBuilder`, one call to
   `create_view_with_circuit`.
5. For multi-table joins, chain left-deep: result of first join → INTEGRATE → DELAY →
   reindex by next join key → join with next table.

`JOIN … USING (col)` desugars to `ON t1.col = t2.col`. Cross joins (`CROSS JOIN`,
`JOIN` without `ON`) are rejected (`GnitzSqlError::Unsupported`).

**Schema inference for joins:** The planner builds the output schema by combining
left and right schemas after reindexing. The output PK is the join key value. Original
PKs from both sides are preserved as payload columns. Duplicate column names are
disambiguated by table alias prefix at bind time.

**Binder changes:** Minimal. No full multi-table refactor. `extract_equijoin_keys`
resolves column names against left and right schemas independently. The existing
`binder.resolve()` fetches schemas for each table. Qualified column references
(`t1.col`) are split on `.` and matched against table aliases tracked in a local
`HashMap<String, (u64, Schema)>`.

#### 5D — Tests

```
TestStringPredicates
  test_view_string_eq_literal    CREATE VIEW v AS SELECT * FROM t WHERE name = 'Alice'
  test_view_string_ne_literal    WHERE name != 'Bob' (composed: BOOL_NOT + EQ)
  test_view_string_gt_literal    WHERE name > 'M' (composed: LT with swapped operands)
  test_view_string_le_literal    WHERE name <= 'M'
  test_view_string_col_eq_col    WHERE first_name = last_name (same-row comparison)
  test_view_string_null          WHERE name = 'Alice' with NULL name → filtered out
  test_view_string_and_int       WHERE name = 'Alice' AND age > 21 (mixed predicate)

TestJoins
  test_inner_join_int_key        JOIN ON t1.cid = t2.id (integer key, exact matching)
  test_inner_join_string_key     JOIN ON t1.name = t2.name (string key, hash-based)
  test_inner_join_multi_match    JOIN with one-to-many (multiple rows per key)
  test_inner_join_self           FROM t a JOIN t b ON a.x = b.y (self-join, both paths + DD)
  test_inner_join_three_tables   three-table join (left-deep chain)
  test_join_using                JOIN USING (col)
  test_join_view_propagation     insert after join view → output updates
  test_join_cross_rejects        CROSS JOIN → error
  test_join_non_equi_rejects     JOIN ON t1.a < t2.b → error (equijoin only)
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
