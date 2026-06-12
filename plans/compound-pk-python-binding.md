# Compound primary keys in the Python `Struct` binding

## Goal

Let Python (`gnitz-py`) clients declare and use a table with a compound
primary key (more than one PK column, in a chosen sort order). Today the
declarative `Struct` API rejects more than one primary key; everything
below it — the native `PySchema`, the row appender, delete, seek, the
wire protocol, the core protocol `Schema`, and the C API — is already
compound-ready. The only blockers are the high-level Python `Struct`
class and one native `create_table` signature.

## Background

The engine schema supports 1–4 PK columns (`MAX_PK_COLUMNS = 5` engine
cap; `validate_pk_cols` allows 1..=4). PK column order is significant: it
is the sort order, so `pk_indices = [2, 1]` sorts by column 2 then
column 1.

What is **already compound-ready** (verified against current code, do
not rebuild):

- **`PySchema::new`** (`crates/gnitz-py/src/lib.rs:102`): signature is
  `(columns, pk_index=None, pk_indices=None)`. It accepts an ordered
  `pk_indices: Vec<usize>`, rejects passing both, validates via
  `Schema::validate_pk_cols`. The `pk_index()` getter (`:174`) raises if
  the PK is compound; `pk_indices()` (`:170`) returns the full list. So
  the native schema object already round-trips compound PKs.
- **`BatchAppender`** (`gnitz-py/src/lib.rs`): `append_pk_from_dict`
  (`:356`) and `append_pk_from_list` (`:372`) iterate
  `self.schema.pk_indices()` (plural) and pack each PK column at its
  `pk_byte_offset` — no single-PK assumption.
- **`delete`** (`:1339`) builds `PkColumn` from per-row `PkTuple`s using
  `pk_stride()` — compound-ready.
- **`seek` / `seek_lazy`** (`:1421`): `pk_tuple_from_py` (`:1702`) accepts
  an `int` (narrow back-compat) **or** `bytes` (compound/wide).
- **Wire / core protocol**: the control block carries `seek_pk: U128`
  plus a `seek_pk_extra: BLOB` companion for PK bytes past offset 16
  (`gnitz-wire/src/lib.rs:878`, `:897`). `gnitz-core` `Schema`
  (`protocol/types.rs:25`) stores `pk_cols: Vec<usize>` (plural) and
  exposes `pk_indices()`/`pk_count()`/`pk_stride()`/`pk_byte_offset()`.
  WAL decode handles `pk_count() >= 2` into `PkColumn::Bytes`
  (`protocol/wal_block.rs:371`).
- **C API** (`crates/gnitz-capi/src/lib.rs`): `gnitz_schema_new(pk_count,
  pk_cols: *const u32)` (`:177`) already takes an ordered PK array and
  documents compound sort order; `gnitz_create_table` passes the full
  slice (`:578`). The C ABI is the reference shape the Python surface
  should reach. It does **not** mirror the Python limitation.
- **Schema round-trip**: `resolve_table_id` returns the server schema to
  Python via `rust_schema_to_py` (`lib.rs:204`), copying `pk_cols` into
  `PySchema.pk_indices`. Clients already *learn* compound PKs.

### The two blockers

1. **`crates/gnitz-py/python/gnitz/_struct.py`** — `__init_subclass__`
   (`:64`–`:87`) tracks a single `pk_idx` (overwritten in the loop, so
   only the last PK column survives), and `:82`–`:85` raises
   `"multiple primary keys are not allowed"`. It then calls
   `Schema(list(cols), pk_index=pk_idx)` (`:87`). `field(*,
   primary_key=False)` (`:15`) has no way to express PK *ordering*.

2. **`PyClient::create_table`** (`crates/gnitz-py/src/lib.rs:1280`) —
   signature `(…, columns, pk_col_idx=0, unique_pk=true)` takes a single
   index and wraps it in a one-element slice (`:1295`) before calling the
   already-compound `client.create_table(…, &pk_slice, …)`. The native
   core (`gnitz-core` client) accepts `&[u32]`; only this pyo3 wrapper is
   single-PK.

`crates/gnitz-py/python/gnitz/_client.py::create_table` (`:27`–`:36`)
sits between them: it picks the *first* PK column via `next((i for i,c …
if c.primary_key), 0)` (`:30`) and passes `pk_col_idx=pk_idx` (`:34`).

## Constraints

- **Single-PK declarations stay byte-identical.** Existing `Struct`
  subclasses with one `primary_key=True` field must produce the same
  schema and table as before. The compound path is additive.
- **PK order must be explicit and unambiguous.** Because PK column order
  is the sort order, the API must let the user state it. Two viable
  shapes (pick one — see step 1).
- **No engine or wire change.** Everything below `PySchema` /
  `PyClient::create_table` is already compound-ready; this plan touches
  only the Python layer and that one pyo3 signature.
- **Python tests green.** Run via `uv` (`uv run pytest …` / the project's
  Python test entry), not bare `pip`/`python`.

## The change, by file

### 1. `python/gnitz/_struct.py` — accept an ordered PK list

Give `field()` a way to express PK ordering, then collect the ordered
list. Two options:

- **(a) Ordinal on `field`**: `field(primary_key=True, pk_order=N)`.
  `primary_key=True` with no `pk_order` keeps order = declaration order.
  Explicit `pk_order` overrides. Most flexible.
- **(b) Declaration order only**: a field is a PK iff
  `primary_key=True`, and PK sort order = the order the PK fields appear
  in the class body. Simplest; sufficient for the common case; loses the
  ability to sort by columns in a different order than declared.

Pick **(a)** but default to declaration order when `pk_order` is omitted
(covers (b) for free). Update `_FieldSpec` (`:9`) to carry an optional
`pk_order`, and `field()` (`:15`) to accept it.

In `__init_subclass__`:

```python
pk_cols = []          # list of (pk_order_or_decl_index, col_index)
for i, (name, hint) in enumerate(ann.items()):
    ...
    if is_pk:
        order = spec.pk_order if spec.pk_order is not None else i
        pk_cols.append((order, i))
...
if not pk_cols:
    # keep existing behavior: default PK = column 0? or require one.
    # Today pk_idx defaults to 0 — preserve that.
    pk_indices = [0]
else:
    pk_cols.sort(key=lambda t: t[0])
    pk_indices = [ci for _, ci in pk_cols]
cls._columns = tuple(cols)
cls._schema  = Schema(list(cols), pk_indices=pk_indices)   # plural
```

Remove the `pk_count > 1` rejection (`:82`–`:85`). Validation (count
1..=4, type allow-list, non-nullable, stride) is enforced by
`Schema::validate_pk_cols` inside `PySchema::new` — let it raise.

Note: `_struct.py` currently lacks `UUID`, `I128`, and `BLOB` type
markers (it has `U8..U128`, `F32/F64`, `STRING` at `:22`–`:33`). UUID is
a valid PK type; if compound-PK users want UUID PKs, add a `UUID` marker.
This is adjacent, not required — flag it but keep it optional.

### 2. `src/lib.rs::PyClient::create_table` — accept `pk_indices`

Change the signature to accept an ordered list, keeping `pk_col_idx` for
back-compat:

```rust
#[pyo3(signature = (schema_name, table_name, columns,
                    pk_col_idx = None, pk_indices = None, unique_pk = true))]
pub fn create_table(
    &mut self, _py: Python<'_>,
    schema_name: &str, table_name: &str,
    columns: Bound<'_, PyList>,
    pk_col_idx: Option<usize>,
    pk_indices: Option<Vec<usize>>,
    unique_pk: bool,
) -> PyResult<u64> {
    let cols = /* unchanged */;
    let pk_slice: Vec<u32> = match (pk_col_idx, pk_indices) {
        (Some(_), Some(_)) => return Err(GnitzError::new_err(
            "pass either pk_col_idx or pk_indices, not both")),
        (_, Some(list)) => list.into_iter().map(|i| i as u32).collect(),
        (Some(i), None)  => vec![i as u32],
        (None, None)     => vec![0],   // preserve old default
    };
    client!(self).create_table(schema_name, table_name, &cols, &pk_slice, unique_pk)
        .map_err(|e| GnitzError::new_err(e.to_string()))
}
```

Delete the `// The Python binding stays single-PK` comment at `:1291`.

### 3. `python/gnitz/_client.py::create_table` — forward the list

```python
def create_table(self, schema_name, table_name, columns, unique_pk=True):
    if isinstance(columns, type) and hasattr(columns, '_columns'):
        # Struct subclass: use its resolved pk_indices (sort order).
        pk_indices = list(columns._schema.pk_indices())
        columns = columns._columns
    else:
        pk_indices = [i for i, c in enumerate(columns) if c.primary_key] or [0]
    return self._client.create_table(
        schema_name, table_name, list(columns),
        pk_indices=pk_indices, unique_pk=unique_pk,
    )
```

When a `Struct` is passed, take the authoritative ordered list from its
`_schema.pk_indices()` (step 1). When a raw column list is passed, derive
the list from `primary_key` flags in declaration order.

### 4. Cosmetic: relax type hints

`_client.py` `seek(table_id, pk: int = 0)` (`:82`) and
`seek_by_index(…, key: int = 0)` (`:85`) annotate `int`; the native
`pk_tuple_from_py` already accepts `bytes`. Widen the hints to
`int | bytes` so compound/wide PK seeks aren't flagged by type checkers.
(The async variants `aio.py` carry the same hints.)

## Migration order

1. **`PyClient::create_table`** (step 2) — additive `pk_indices` param,
   old `pk_col_idx` path unchanged. Rebuild the extension; existing
   Python tests stay green.
2. **`_struct.py`** (step 1) — ordered PK list + drop the rejection.
3. **`_client.py`** (steps 3, 4) — forward the list; relax hints.

Each step is independently shippable; after step 1 a power user can pass
`pk_indices=` directly; steps 2–3 make the declarative `Struct` surface
use it.

## Out of scope

- **Engine / wire / core-protocol / C-API changes** — already compound-
  ready; this plan must not touch them.
- **Wide PKs (`pk_stride > 16`) reaching the server.** The Python surface
  can *express* a compound PK once this lands, but whether the server
  *admits* a given PK (e.g. a wide one whose stride is not in
  `{1,2,4,8,16}`) is decided by the server's `CREATE TABLE` admission
  rule, unchanged here. A narrow compound PK (e.g. `(U32, U32)`,
  `(U64, U64)`) is the realistic target this plan unlocks.
- **The `pk_index()` (singular) getter** on `PySchema` — keep it; it
  already raises a clear error for compound PKs (`lib.rs:174`).

## Testing

Python tests via `uv` (timeout each):

- **Single-PK regression**: an existing one-PK `Struct` produces the same
  schema and creates the same table; no behavior change.
- **Compound declaration**: a `Struct` with two `primary_key=True` fields
  creates a table with `pk_indices` in declaration order; assert the
  server-resolved schema (via `resolve_table`) reports the same two PK
  columns in that order.
- **Explicit `pk_order`**: declare PK fields out of sort order with
  `pk_order=` and assert the resolved `pk_indices` reflect the requested
  order, not declaration order.
- **Round-trip insert + seek**: insert rows into a `(U32, U32)`-keyed
  table and `seek` them back by a `bytes` PK; assert the rows match.
- **Validation surfacing**: a 5-PK-column `Struct`, a nullable PK, and a
  `STRING` PK each raise the engine's validation error through
  `PySchema::new` (not a Python-side ad-hoc check).

Rust (`gnitz-py` crate): a pyo3 test (or doctest) that
`PyClient::create_table` with `pk_indices=[1, 0]` forwards `[1, 0]` to
the core client.
