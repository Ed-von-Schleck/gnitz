# GnitzDB Evolution Plan v2: Schemas, Indices, Views, and System Metadata

## Framing: Hard State and Soft State

Before diving into specifics, one architectural concept unifies almost every
decision in this document.

GnitzDB is fundamentally a system for incrementally maintaining derived views
over source data. This means it naturally operates in two tiers:

**Hard state** is the ground truth. It consists of the raw records written by
external ingestion and must survive crashes with full fidelity. Source table
primary data lives here: WAL-backed, fsync'd, shard-compacted.

**Soft state** is derived. It consists of everything the engine can reconstruct
from hard state alone — indices and materialized views. Soft state is maintained
incrementally during normal operation (the whole point of DBSP), but on startup
after a crash, all soft state is discarded and rebuilt from hard state. This
means soft state never needs its own WAL, never needs fsync, and never needs to
be part of crash recovery protocol. It can use write-ahead logging as a startup-
time *optimisation* if rebuild cost becomes prohibitive, but that is optional and
never load-bearing for correctness.

This distinction cuts through every design question:
- Secondary indices → soft state
- Materialized views → soft state
- Index-on-view → soft state
- Source table primary data → hard state
- System metadata table data → hard state (it is the recovery source)

Everything soft is a DBSP circuit that transforms source records. An index is a
simple MAP circuit (one-column extraction and key promotion). A view is an
arbitrary DBSP circuit (filter, project, join, aggregate, compose). Both are
maintained the same way: delta-in from source, delta-out to their own in-memory
Z-Set, with in-memory-only shard snapshots for fast restart if available.

---

## 1. Two-Level Namespace: Schemas

### The Rule

All named entities — source tables and views — live in a two-level namespace
`schema.name`. The separator is `.`. System schemas have names starting with `_`.
User schemas and user tables/views may not start with `_`.

The `_system` schema is the only system schema. It contains the five system
tables that describe everything else. The `public` schema is the default user
schema (as in PostgreSQL). A bare name `orders` resolves to `public.orders`.

### Identifier Validation

```python
def validate_user_identifier(name):
    if not name or name[0] == '_':
        raise LayoutError("'%s': user identifiers cannot start with '_'" % name)
    for ch in name:
        if ch not in VALID_IDENT_CHARS:   # [a-zA-Z0-9_]
            raise LayoutError("Invalid character '%s' in '%s'" % (ch, name))
```

Applied at every DDL entry point for both the schema name and the entity name.
The engine's internal DDL paths (bootstrap, index auto-creation, FK index
creation) bypass this check.

### Initial Implementation

Schemas are cheap to implement: they are just one extra table in the system
metadata and one extra string field on every entity. No `USE SCHEMA` or session
scope is needed initially — fully-qualified names are always accepted, and bare
names default to `public`.

`_system._schemas` (table_id = 1):

| col | Name | Type | Notes |
|---|---|---|---|
| 0 | `schema_id` | `TYPE_U64` | **PK**, engine-assigned |
| 1 | `name` | `TYPE_STRING` | e.g. `"public"`, `"_system"` |

Bootstrap creates `_system` (id=1) and `public` (id=2) on first startup and
never touches them again. User DDL (`CREATE SCHEMA`) adds rows to this table.
`DROP SCHEMA` is rejected if any entity still exists in the schema.

---

## 2. The System Metadata Layer

### Design Philosophy

System metadata uses the engine's own Z-Set machinery. DDL operations —
`CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `DROP ...` — are delta insertions
and retractions into system Z-Set tables. DDL is therefore crash-safe and WAL-
recoverable by construction, using exactly the same code paths as user data
mutations. No special schema log is needed.

System tables are **hard state**. Their schemas are compiled into the engine
binary (hardcoded table IDs, hardcoded column lists). Their data lives in the
`_system/` directory under normal shard+WAL storage. On startup, the engine
opens these tables by hardcoded path without consulting any registry, reads
their data to rebuild all runtime state, then makes user tables and views
available.

### The Five System Tables

#### `_system._schemas` (table_id = 1)
Described above.

#### `_system._tables` (table_id = 2)

Records source tables only. Views are in `_system._views`. Index tables are
internal and not recorded here.

| col | Name | Type | Notes |
|---|---|---|---|
| 0 | `table_id` | `TYPE_U64` | **PK** |
| 1 | `schema_id` | `TYPE_U64` | FK → `_system._schemas` |
| 2 | `name` | `TYPE_STRING` | Unqualified name within the schema |
| 3 | `directory` | `TYPE_STRING` | Absolute path to shard/WAL data |
| 4 | `pk_col_idx` | `TYPE_U64` | Index of the PK column in `_system._columns` |
| 5 | `created_lsn` | `TYPE_U64` | WAL LSN at creation time |

#### `_system._views` (table_id = 3)

Records materialized incremental views.

| col | Name | Type | Notes |
|---|---|---|---|
| 0 | `view_id` | `TYPE_U64` | **PK** |
| 1 | `schema_id` | `TYPE_U64` | FK → `_system._schemas` |
| 2 | `name` | `TYPE_STRING` | Unqualified name |
| 3 | `sql_definition` | `TYPE_STRING` | The SQL query the view computes |
| 4 | `cache_directory` | `TYPE_STRING` | Path for optional soft-state shard snapshots (empty = no disk cache) |
| 5 | `created_lsn` | `TYPE_U64` | |

#### `_system._columns` (table_id = 4)

One record per column of every source table and every view.

| col | Name | Type | Notes |
|---|---|---|---|
| 0 | `column_id` | `TYPE_U64` | **PK**. Packed: `(owner_id << 9) | col_idx` |
| 1 | `owner_id` | `TYPE_U64` | `table_id` or `view_id` depending on `owner_kind` |
| 2 | `owner_kind` | `TYPE_U64` | 0 = source table, 1 = view |
| 3 | `col_idx` | `TYPE_U64` | 0-based physical position |
| 4 | `name` | `TYPE_STRING` | Column name |
| 5 | `type_code` | `TYPE_U64` | Matches `FieldType.code` |
| 6 | `is_nullable` | `TYPE_U64` | 0 or 1 |
| 7 | `fk_table_id` | `TYPE_U64` | 0 = not a FK; otherwise a source `table_id` |
| 8 | `fk_col_idx` | `TYPE_U64` | Referenced column's `col_idx` (only when `fk_table_id ≠ 0`) |

The packed `column_id` key `(owner_id << 9) | col_idx` assumes ≤ 512 columns
per entity, well above the current 64-column limit.

#### `_system._indices` (table_id = 5)

One record per secondary index on any source table or view.

| col | Name | Type | Notes |
|---|---|---|---|
| 0 | `index_id` | `TYPE_U64` | **PK** |
| 1 | `owner_id` | `TYPE_U64` | `table_id` or `view_id` |
| 2 | `owner_kind` | `TYPE_U64` | 0 = on source table, 1 = on view |
| 3 | `source_col_idx` | `TYPE_U64` | Column being indexed |
| 4 | `name` | `TYPE_STRING` | Auto-generated, e.g. `"orders__idx_customer_id"` |
| 5 | `is_unique` | `TYPE_U64` | 0 = non-unique, 1 = unique (reserved; not enforced in phase 1) |
| 6 | `cache_directory` | `TYPE_STRING` | Path for optional soft-state shard snapshots |

Index tables are soft state. The `cache_directory` is an optimisation hint for
fast restart — the engine may write periodic shard snapshots there, but they are
never required for correctness and are discarded on any inconsistency.

### Column and Index Auto-naming

Index names are auto-generated on creation:
```
{owner_schema}__{owner_name}__idx_{column_name}
```
For a FK-implied index, the prefix `fk_` is added:
```
{owner_schema}__{owner_name}__fk_{column_name}
```
All underscores in constituent parts are preserved. The resulting name is stored
in `_system._indices.name` and is how users refer to the index in `DROP INDEX`.

### Bootstrap Sequence

**First startup** (no `_system/` directory):
1. Create `_system/` directory.
2. Instantiate five `PersistentTable` objects with hardcoded schemas.
3. Ingest the self-describing records: both schemas (`_system`, `public`),
   all five system tables, and all their columns.
4. Flush to shards and write manifests.

**Subsequent startups:**
1. Open the five system tables from hardcoded paths (no registry lookup needed).
2. Scan `_system._schemas` → populate schema registry.
3. Scan `_system._tables` → open each `PersistentTable` from its `directory`;
   populate `TableRegistry` with `TableFamily` objects.
4. Scan `_system._indices` where `owner_kind = 0` → rebuild each index in
   memory (see §4.4 — no WAL, rebuild from source).
5. Scan `_system._views` → reconstruct each DBSP circuit from `sql_definition`;
   rebuild view state in memory (see §5.4).
6. Scan `_system._indices` where `owner_kind = 1` → rebuild each view index.
7. Engine ready.

The startup rebuild (steps 4–6) is the only recovery protocol for soft state.
There is no WAL replay for indices or views.

### DDL as Z-Set Operations

Every DDL statement is implemented as delta mutations on the system tables,
executed inside the system tables' own WAL transaction. If the process crashes
mid-DDL, the system WAL is replayed on the next startup and the system tables
converge to a consistent state. The user table or view may not exist on disk yet
(if the crash happened before directory creation), but the system tables will
not reference it either.

---

## 3. Secondary Indices

### What a Secondary Index Is

A secondary index on column `C` of table (or view) `T` is a soft-state Z-Set
`T__idx_C` whose schema is:

```
PK:      index_key   (promoted type of C, see §3.2)
Payload: source_pk   (the type of T's own PK)
```

For a source batch record `(pk=99, C=42, weight=+1)`:
```
index receives: (index_key=42, source_pk=99, weight=+1)
```

This is a DBSP MAP operator: linear, streaming, zero-copy for key extraction.
The index Z-Set lives entirely in memory plus an optional shard snapshot on
disk. It has no WAL of its own.

### Key Type Promotion

| Source column type | Index PK type | Rule |
|---|---|---|
| `TYPE_U64` | `TYPE_U64` | Identity |
| `TYPE_U128` | `TYPE_U128` | Identity |
| `TYPE_U32`, `U16`, `U8` | `TYPE_U64` | Zero-extend |
| `TYPE_I64` | `TYPE_U64` | `rffi.cast(rffi.ULONGLONG, val)` — bit-reinterpret |
| `TYPE_I32`, `I16`, `I8` | `TYPE_U64` | Sign-extend to 64, then bit-reinterpret |
| `TYPE_F64`, `F32` | — | Deferred (phase 2+; NaN total-order question unresolved) |
| `TYPE_STRING` | — | Deferred (phase 2+; requires PK type extension or separate B-tree) |

For signed integer columns, the bit-reinterpret promotion preserves sort order
correctly: `-1` (as i64) maps to `0xFFFFFFFFFFFFFFFF`, which sorts *after* `0`
as u64. Seeks into a signed-column index must apply the same promotion to the
query predicate value. The engine determines the correct promotion from the
column's type code in `_system._columns`.

### Index Table Structure

An index is an `IndexCircuit` object containing a `MemTable` (and optional shard
snapshots). It is not a full `PersistentTable` — it has no WAL and no manifest.
Its schema is:

```
PK:      index_key   (u64 or u128)
Payload: source_pk   (u64 or u128)
```

On startup, the `IndexCircuit` is instantiated fresh with an empty `MemTable`
and immediately backfilled (§3.3).

### Rebuild on Startup

Because the index has no WAL, the startup procedure always rebuilds it from
scratch. The source table (which does have a WAL and shards) is opened first.
Then a full cursor scan over the source table drives `compute_index_delta` to
populate the index MemTable. The cost is O(N) where N is the source table's
live record count — bounded by the same memory footprint that the MemTable
already occupies.

Benefits of this design:
- No WAL sync needed for index writes → lower write latency for source ingestion
- No WAL file management for indices → simpler storage layer
- No LSN coordination between source WAL and index WAL → no recovery edge cases
- Index creation is conceptually free from a durability standpoint — if it fails,
  just try again on next startup

The optional shard snapshots in `cache_directory` are an optimisation: if
present and consistent (snapshot LSN ≤ source table's LSN), the engine can load
the snapshot and replay only the delta rather than a full scan. This is an
optimisation that can be added after correctness is established.

### Index Maintenance During Operation

When the source `TableFamily` receives a batch:

```python
def ingest_batch(self, batch):
    self.primary.ingest_batch(batch)       # WAL write + MemTable upsert
    for circuit in self.index_circuits:
        idx_batch = circuit.compute_index_delta(batch)
        circuit.memtable.upsert(idx_batch) # No WAL write
```

Retractions must carry the full payload (already required by the existing
`ingest_batch` API — `ArenaZSetBatch` records always contain PK + full payload).
The engine uses the payload's value at retraction time to generate the correct
index retraction. This is an API contract; callers must supply the correct
current payload when retracting.

### Uniqueness: Reserved Design Space

`_system._indices.is_unique` is stored now so the schema is forward-compatible.
In phase 1, the engine ignores this field — it stores it, reports it, but does
not enforce it. Unique constraint enforcement will be an incremental check
circuit (similar to FK enforcement) added in a later phase. The compiler may
use the `is_unique` hint for query optimisation even before enforcement is
active.

### Index Drop Behaviour

`DROP INDEX foo__idx_bar` retracts the `_system._indices` record and stops
populating the circuit. The circuit's MemTable is released. If the index was
FK-implied, it may not be dropped independently — it is dropped automatically
when the FK constraint is removed.

---

## 4. Foreign Keys

### FK as Index Plus Annotation

A FK declaration `orders.customer_id → customers.pk` consists of:
1. A secondary index on `orders.customer_id` — created automatically.
2. A referential integrity constraint — opt-in, initially unenforced.

The FK is recorded in `_system._columns.fk_table_id` and `fk_col_idx`.
The index is recorded in `_system._indices` with the `fk_` name prefix.

### FK Targets

FKs may only reference the **PK column** of the target table. Non-PK unique
index references are a future extension (depends on unique index enforcement
being active). This keeps constraint semantics unambiguous: the referenced value
is uniquely present or absent with no query required to find it.

### FK Validation at Declaration Time

When a FK column is declared:
1. Verify the target table exists in the registry.
2. Verify `fk_col_idx` is the PK column of the target table (i.e., equals
   `_system._tables.pk_col_idx` for that table).
3. Verify the FK column's promoted type is compatible with the target PK type.
4. Auto-create the secondary index (`CREATE INDEX` path, §3).

### Constraint Enforcement (Synchronous, Opt-in)

Enforcement is activated per FK by a flag on the constraint (initially all FKs
are unenforced — annotation only). When enforcement is enabled, `ingest_batch`
on the FK-side table runs a pre-commit check before writing to the WAL:

```
for each record in batch with weight > 0:
    if record.fk_column NOT IN target_table.pk_index:
        reject entire batch with ReferentialIntegrityError
```

"In the target table's PK index" means a PK seek on the target table's
MemTable or shard returns a live record (positive total weight).

Synchronous enforcement is chosen because it requires no compensating
transaction machinery — a rejected batch is simply never committed. The
trade-off is that a batch inserting both a new customer and a new order
referencing that customer cannot be applied to `orders` before `customers` is
updated. Callers must order DDL batches appropriately (customers first, then
orders). This is the same convention SQL databases require in practice.

### Drop Semantics

`DROP TABLE customers` is rejected with an error if any column in any table has
`fk_table_id = customers.table_id`. The user must first drop the FK constraint
(via `ALTER TABLE orders DROP CONSTRAINT ...`) or drop the referencing table.
This is the standard SQL default behaviour.

---

## 5. Views: Incrementally Maintained Derived Tables

### The Core Product Concept

Materialized incremental views are the primary reason GnitzDB exists. A view is
a named, persistent, continuously-updated query result that processes only
deltas — never full re-scans. The DBSP circuit maintains the view by applying
incremental operators whenever any source table (or upstream view) receives new
data.

### What a View Is and Is Not

A view **is**:
- A named entity in a schema, queryable like a table
- Backed by a live DBSP circuit running inside the engine process
- Backed by an in-memory Z-Set that reflects the current computed state
- Optionally backed by shard snapshots in `cache_directory` for faster restart
- Capable of having secondary indices (also soft state)

A view **is not**:
- A source of truth for anything
- Required to survive crashes with fidelity
- Ever written to a WAL
- Ever fsync'd

On startup, every view is rebuilt from its source tables. The SQL definition in
`_system._views.sql_definition` is re-parsed and re-compiled to a DBSP circuit.
The circuit then processes the full current state of its source tables to
reconstruct the view's Z-Set. If a shard snapshot exists in `cache_directory`
and is consistent with the source tables' LSNs, the engine may load it and
replay only the delta — but this is a performance optimisation, not a
correctness requirement.

### `_system._views` and Column Metadata

Views are registered in `_system._views`. Their column schemas are stored in
`_system._columns` with `owner_kind = 1` (view). This is hard state — the
column definitions survive crashes. Only the view's *data* is soft.

This distinction is important: you always know what columns a view has, even
if its data needs to be rebuilt. Schema introspection is always fast.

### View Circuit Lifecycle

**Create view:**
1. Validate the SQL definition (parse + plan with Calcite).
2. Derive the output schema from the plan.
3. Assign a `view_id`. Write to `_system._views` and `_system._columns`.
4. Instantiate the DBSP circuit. Subscribe the circuit to its source tables
   and upstream views.
5. Run the circuit over the full current state of all source inputs (initial
   population — this may take time for large sources).
6. View is ready to query.

**Startup rebuild:**
1. Load `_system._views` records.
2. For each view (in dependency order — a view may be built on another view):
   a. Re-parse `sql_definition` → DBSP circuit.
   b. Check for optional shard snapshot consistency.
   c. If snapshot is usable: load snapshot, compute delta since snapshot LSN.
   d. Otherwise: run circuit over full source state from scratch.
3. Rebuild any indices on the view (§3, soft state).

**Drop view:**
1. Verify no other view depends on this view. Reject if so (or cascade if
   explicitly requested — not implemented in phase 1).
2. Drop all indices on the view.
3. Retract from `_system._views` and `_system._columns`.
4. Disconnect and destroy the circuit.

### Dependency Ordering

Views can be built on other views (`CREATE VIEW v2 AS SELECT ... FROM v1`). The
engine must track this dependency graph to:
- Apply updates in the correct order (upstream views before downstream views)
- Rebuild in the correct order on startup
- Reject circular dependencies at `CREATE VIEW` time

The dependency graph is implicit in the SQL definition. At compile time, Calcite
resolves all referenced names and the engine extracts the dependency edges. These
are stored denormalized in a `_system._view_deps` table:

`_system._view_deps` (table_id = 6):

| col | Name | Type | Notes |
|---|---|---|---|
| 0 | `dep_id` | `TYPE_U64` | **PK** |
| 1 | `view_id` | `TYPE_U64` | The downstream view |
| 2 | `dep_view_id` | `TYPE_U64` | The upstream view it depends on (0 if depends on a source table) |
| 3 | `dep_table_id` | `TYPE_U64` | The source table it depends on (0 if depends on a view) |

A topological sort over this table gives the correct rebuild order on startup.
A cycle in this graph is detected at `CREATE VIEW` time and is rejected.

### The `ViewFamily` Abstraction

A view and its secondary indices form a `ViewFamily`, mirroring `TableFamily`:

```python
class ViewFamily(object):
    def __init__(self, name, schema, circuit, index_circuits):
        self.name = name
        self.schema = schema          # TableSchema of the output
        self.circuit = circuit        # DBSPCircuit object
        self.indices = index_circuits # List[IndexCircuit] (soft state)

    def create_cursor(self):
        """Returns a cursor over the view's current Z-Set."""
        return self.circuit.output_memtable.create_cursor()

    def create_index_cursor(self, col_idx):
        for idx in self.indices:
            if idx.source_col_idx == col_idx:
                return idx.memtable.create_cursor()
        raise StorageError("No index on column %d" % col_idx)
```

The `circuit` object encapsulates the DBSP operator graph. When a source table
(or upstream view) receives a delta, it notifies all downstream circuits via a
subscription mechanism. The circuit processes the delta and updates its output
Z-Set, then fans out to its own index circuits.

### Views Are Queryable Exactly Like Tables

From the query compiler's perspective, a `ViewFamily` and a `TableFamily`
present identical interfaces:
- Both have a `schema` (column definitions, PK)
- Both support `create_cursor()` and `create_index_cursor(col_idx)`
- Both appear in the `EntityRegistry` (the unified registry, see §6.2)
- Both may be joined to other entities (subject to the join restriction, §7)

The only difference visible to the compiler is a flag `is_view: bool`. This
matters for join planning (a view's internal computation already processes
deltas, so the join circuit must account for double-delta semantics correctly —
but this is a compiler concern, not a storage concern).

---

## 6. The Unified Abstractions

### 6.1 `TableFamily` (Source Tables)

```
TableFamily
├── primary: PersistentTable      # WAL-backed, shard-compacted, fsync'd
└── indices: List[IndexCircuit]   # soft state, no WAL
```

All writes go through `TableFamily.ingest_batch()`. Reads go through
`primary.create_cursor()` or `IndexCircuit.memtable.create_cursor()`.

### 6.2 `ViewFamily` (Derived Views)

```
ViewFamily
├── circuit: DBSPCircuit          # subscribes to source families/views
├── output_memtable: MemTable     # soft state, the view's current Z-Set
└── indices: List[IndexCircuit]   # soft state, on the view's output
```

The `output_memtable` is populated by the circuit as it processes deltas.

### 6.3 `EntityRegistry` (Unified Lookup)

The `TableRegistry` is renamed `EntityRegistry` and holds both `TableFamily`
and `ViewFamily` objects:

```python
class EntityRegistry(object):
    def get(self, schema, name):
        """Returns TableFamily or ViewFamily. Raises if not found."""

    def get_by_id(self, table_id=None, view_id=None):
        """Returns the family for a given numeric ID."""

    def has_index_on(self, owner_id, owner_kind, col_idx):
        """True if a secondary index exists on this column."""

    def is_joinable(self, owner_id, owner_kind, col_idx):
        """True if col_idx is a PK or has a secondary index."""
```

From outside the engine — query API, Calcite backend — `EntityRegistry.get()`
is the single entry point. The distinction between a source table and a view is
an implementation detail of the returned family object.

### 6.4 `IndexCircuit`

Unchanged from the previous plan, but clarified: it operates identically
whether it is attached to a `TableFamily` (source index) or a `ViewFamily`
(view index). The only difference is the source of deltas — source index
circuits are driven by `ingest_batch`; view index circuits are driven by the
view's DBSP circuit output.

```python
class IndexCircuit(object):
    source_col_idx: int
    index_key_type: FieldType      # u64 or u128 (promoted from source column)
    source_pk_type: FieldType
    memtable: MemTable             # no WAL — soft state
    cache_directory: str           # empty = no disk cache

    def compute_index_delta(self, source_batch): ...
```

---

## 7. Join Restrictions

### The Rule

**A join is permitted only if both sides of the join predicate reference a PK
column or a column with a secondary index.**

This is a compile-time restriction, enforced by the Calcite backend's GnitzDB
planner before any VM bytecode is emitted. Violation produces a descriptive
compile-time error naming the unindexed column and suggesting `CREATE INDEX`.

### Why This Rule

Without a secondary index, a non-PK join requires a full scan of one input for
every delta record in the other — O(|Δ| × |T|) complexity, incompatible with
the incremental model. With a secondary index, both join directions are
O(|Δ| × log(|T|)):
- Delta on the indexed side → seek the PK index of the other side
- Delta on the PK side → seek the secondary index of the other side

The join restriction is therefore not an arbitrary limitation; it is the
condition under which incremental join maintenance is tractable.

### Join Forms

With the restriction in place, every VM-level join takes one of three forms:

**Form 1 — PK-to-PK equijoin.** Both sides are PK columns (same domain type).
This is the existing `JOIN_DELTA_TRACE` instruction. No change needed.

**Form 2 — PK-to-index equijoin (FK join).** One side is a PK; the other is
an indexed payload column. The PK side is the probe; the index side is the
lookup target. New VM instruction: `JOIN_VIA_INDEX`. Execution:
1. For each record in the PK-side delta, seek the index at `index_key = pk_val`.
2. Collect `source_pk` values from the index cursor.
3. For each `source_pk`, seek the source entity's primary cursor to retrieve the
   full payload.
4. Emit output records with `weight = w_delta × w_source`.

**Form 3 — index-to-index equijoin.** Both sides are indexed non-PK columns
with compatible promoted types. The two index cursors (both sorted by index key)
are merged. For matching index keys, the `source_pk` sets from each side are
cross-producted. Each resulting `(pk_left, pk_right)` pair triggers two primary
lookups. This form is rare and expensive for high-cardinality non-unique indices.

### Compiler Validation

```python
def validate_join_columns(registry, left_id, left_kind, left_col,
                                    right_id, right_kind, right_col):
    left_ok  = registry.is_joinable(left_id,  left_kind,  left_col)
    right_ok = registry.is_joinable(right_id, right_kind, right_col)
    if not left_ok or not right_ok:
        names = []
        if not left_ok:
            names.append("left column %d (no index)" % left_col)
        if not right_ok:
            names.append("right column %d (no index)" % right_col)
        raise CompileError(
            "Join references unindexed column(s): %s. "
            "Use CREATE INDEX or join on PK columns." % ", ".join(names)
        )
```

This function is called for every join node in the logical plan. FK-implied
indices satisfy the restriction automatically — declaring a FK creates the index,
making the FK column immediately joinable.

---

## 8. Phased Implementation Plan

### Phase A — Namespace and System Metadata Foundation

**Goal:** Replace ad-hoc table management with a self-describing metadata layer.

**Deliverables:**
- Identifier validation (underscore prefix, character set)
- Two-level name resolution (`schema.table`, default schema `public`)
- Six system tables with hardcoded schemas and IDs (including `_system._views`
  and `_system._view_deps` even though views are not yet implemented)
- Bootstrap sequence: self-describing metadata on first startup
- Restart: rebuild `EntityRegistry` from system tables
- `CREATE TABLE` / `DROP TABLE` writing to system tables
- `CREATE SCHEMA` / `DROP SCHEMA` DDL

**Risk:** Low. Entirely additive. Existing `PersistentTable` code is unchanged.
System tables are additional table instances, not special code paths.

### Phase B — Secondary Indices (Numeric Columns)

**Goal:** Soft-state secondary indices with no WAL, rebuild-on-startup semantics.

**Deliverables:**
- `IndexCircuit` class with `compute_index_delta` and MemTable
- `TableFamily` wrapping primary + index circuits
- `EntityRegistry` updated to use `TableFamily`
- `ingest_batch` fan-out in `TableFamily`
- Startup rebuild from source table cursor scan
- `CREATE INDEX` DDL (writes `_system._indices`, backfills, then attaches circuit)
- `DROP INDEX` DDL (retracts `_system._indices`, destroys circuit)
- Promotion rules for all numeric types (§3.2)

**Risk:** Medium. The fan-out in `ingest_batch` is a hot-path change. RPython
type annotations must handle the `indices` list as a typed list of `IndexCircuit`
objects. The rebuild-on-startup path is new. Signed integer sort-order semantics
must be tested carefully.

### Phase C — Foreign Keys

**Goal:** FK as annotation + auto-index + optional enforcement.

**Deliverables:**
- `fk_table_id` / `fk_col_idx` on `ColumnDefinition`
- System table columns populated by FK DDL
- Auto-create index on FK column at `CREATE TABLE` time
- FK validation at declaration time (target must be PK)
- Opt-in synchronous pre-commit enforcement
- `DROP TABLE` rejection when referenced by FK

**Risk:** Low-medium. FK is bookkeeping on top of Phase B. The enforcement
check adds a PK seek on another table during `ingest_batch` — latency impact
must be measured.

### Phase D — Materialized Views

**Goal:** Incremental view maintenance as first-class engine feature.

**Deliverables:**
- `DBSPCircuit` abstraction (wraps existing DBSP operator graph)
- `ViewFamily` class
- `EntityRegistry` updated to hold both `TableFamily` and `ViewFamily`
- Subscription mechanism: source tables notify downstream circuits on delta
- `CREATE VIEW` DDL: parse SQL, compile circuit, backfill, register
- `DROP VIEW` DDL: validate no dependents, retract, destroy circuit
- Dependency graph: `_system._view_deps`, topological sort on startup
- Startup rebuild in dependency order (views-on-views)
- `VIEW ON VIEW` support (downstream views subscribe to upstream view output)

**Risk:** High. This is the core product capability and touches the DBSP/VM
layer directly. The circuit compilation step requires the Calcite integration
to be complete. The subscription/notification mechanism between families is new.
The dependency order constraint on startup adds complexity. Start here only
after Phases A–C are stable.

### Phase E — Join Restriction Enforcement and `JOIN_VIA_INDEX`

**Goal:** Compiler validation + VM instruction for index-mediated joins.

**Deliverables:**
- `EntityRegistry.is_joinable()` method
- Compiler validation in Calcite backend
- `JOIN_VIA_INDEX` VM instruction (Form 2 joins)
- Index-to-index join support (Form 3)
- View joins (compiler handles double-delta semantics)

**Risk:** Medium-high. The VM instruction is new and must handle the two-step
index→primary lookup with correct weight arithmetic. Requires Phase D (views
are join targets too).

### Phase F — String and Float Indices (Future)

Requires resolving the NaN total-order question for floats, and either extending
the PK type system to allow `TYPE_STRING` PKs or implementing a separate B-tree-
style index for string columns. Neither is scheduled — this document records the
open question for future design work.

---

## 9. The Soft-State Unified View

It is worth stating explicitly what this design achieves architecturally once
all phases are complete.

The engine maintains two categories of state in a clean separation:

**Hard state** — write-once, WAL-backed, crash-safe:
- Source table primary records (one per `TableFamily`)
- System table records (schemas, source tables, view definitions, column defs,
  index registrations)

**Soft state** — derived, no WAL, rebuilt on startup:
- Secondary indices on source tables (`IndexCircuit` per registered index)
- Materialized view Z-Sets (`ViewFamily.output_memtable`)
- Secondary indices on views (`IndexCircuit` on view output)

The total set of soft-state objects is described completely by the hard-state
system tables. Given the hard state alone, the engine can reconstruct all soft
state by replaying the DBSP circuits. This is exactly the property that makes
the system robust without complexity: crash recovery is simply startup.

The query layer sees none of this distinction. `EntityRegistry.get()` returns
a family object that supports `create_cursor()` and `create_index_cursor()`.
Whether the cursor is backed by shards (source table) or a MemTable (view) is
invisible to the query compiler and to users.
