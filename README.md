```markdown
# Gnitz

A high-performance, incremental database engine and Virtual Machine implemented in RPython. Gnitz implements the **DBSP** (Differential Dataflow) formal model, treating all data as **Z-Sets**—multisets where every record has an associated integer weight, enabling rapid algebraic coalescing of updates.

## Architecture

Gnitz is built on a layered architecture decoupled by the **Z-Store** interface:

- **`gnitz.core`**: Fundamental primitives, the type system, and the `ZSetStore` abstraction.
- **`gnitz.storage`**: Durable engine featuring a SkipList MemTable, machine-word columnar shards, and a Write-Ahead Log (WAL).
- **`gnitz.catalog`**: DDL management, secondary indices (implemented via high-performance projection kernels), and incremental Foreign Key enforcement.
- **`gnitz.vm`**: The DBSP execution engine. It constructs incremental circuits that process delta batches into resulting updates.

## Key Abstractions

### Z-Sets & Weights
Updates are represented as deltas. An insertion has a weight of `+1`, a deletion `-1`. Algebraic summation across shards and the MemTable determines the current state.

### Z-Store Interface
Defined in `gnitz.core.store`, this interface allows the VM and Catalog to operate against primary tables (`PersistentTable`), indices, or operator state (`EphemeralTable`) via a unified API for ingestion and cursors.

### RPython & Meta-Tracing JIT
The engine is written in RPython, a statically analyzable subset of Python 2.7. It translates to highly optimized C code with an integrated Meta-Tracing JIT that compiles hot DBSP circuit traces into machine code.

## Directory Structure

- `gnitz/core/`: Type system, serialization, and the `ZSetStore` interface.
- `gnitz/storage/`: LSM-style storage implementation (MemTable, Shards, WAL, Manifest).
- `gnitz/catalog/`: Engine for DDL, Table Families, and Index Projection.
- `gnitz/dbsp/`: Implementations of DBSP operators (Join, Reduce, Distinct).
- `gnitz/vm/`: Virtual machine interpreter, register file, and circuit builder.

## Building and Testing

Translation requires the PyPy/RPython toolchain.

```bash
# Run tests
pytest rpython_tests/

# Translate to C (requires RPython toolchain)
rpython gnitz/target.py
```

Refer to `appendix.md` for RPython implementation constraints and internal memory alignment requirements.
```
