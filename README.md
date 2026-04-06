# Gnitz

A high-performance, incremental database engine implemented in Rust. Gnitz implements the **DBSP** (Differential Dataflow) formal model, treating all data as **Z-Sets**—multisets where every record has an associated integer weight, enabling rapid algebraic coalescing of updates.

## Architecture

Gnitz is built on a layered architecture decoupled by the Z-Store interface:

- **Storage engine** (`crates/gnitz-engine/`): Durable columnar storage with LSM-style compaction, Write-Ahead Log, and machine-word columnar shards.
- **Catalog** (`catalog.rs`): DDL management, secondary indices, and incremental Foreign Key enforcement.
- **VM** (`vm.rs`, `dag.rs`): The DBSP execution engine. It constructs incremental circuits that process delta batches into resulting updates.
- **IPC** (`ipc.rs`, `master.rs`, `worker.rs`): Multi-process execution with shared-memory IPC for parallel query evaluation.

## Key Abstractions

### Z-Sets & Weights
Updates are represented as deltas. An insertion has a weight of `+1`, a deletion `-1`. Algebraic summation across shards and the MemTable determines the current state.

### Z-Store Interface
Defined in `table.rs` and `partitioned_table.rs`, this interface allows the VM and Catalog to operate against primary tables, indices, or operator state via a unified API for ingestion and cursors.

## Building and Testing

```bash
# Build the server binary
make server

# Run unit tests
make test

# Run E2E tests (multi-worker)
make e2e

# Build release binary
make release-server
```

## Directory Structure

- `crates/gnitz-engine/` — Core engine: storage, operators, VM, catalog, IPC, server binary
- `crates/gnitz-transport/` — Wire protocol codec
- `crates/gnitz-py/` — Python client library and E2E test suite
- `crates/gnitz-core/` — Integration tests
