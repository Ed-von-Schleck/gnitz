# Multi-Core gnitz: Roadmap

## Architecture Decision

**Approach: Multi-Process (Feldera-style data parallelism)**

Each worker process runs a complete Engine instance on a disjoint partition of
the data. Workers communicate via the existing IPC protocol (memfd + Unix domain
sockets). A master process handles client connections, DDL, and dispatch.

### Why multi-process over multi-thread

- RPython has a GIL — threads cannot achieve true CPU parallelism for pure-RPython code
- Multi-process sidesteps the GIL entirely; each process has full CPU access
- Fault isolation: a worker crash doesn't corrupt peers
- JIT benefits: each worker's JIT specializes for its workload
- gnitz already has the transport: memfd IPC, columnar wire format, send_batch/receive_payload
- Symmetric workers: every process runs identical code, just on different data

### System tables: Master + Replication (no consensus)

DDL is low-volume, inherently serialized metadata. The master is the single source
of truth for system tables. Workers are read replicas:

- Client sends DDL to master
- Master applies to local system tables
- Master broadcasts the change (as a regular IPC batch) to all workers
- Workers apply to their local registry

No Raft/Paxos needed — if the master dies, the cluster is down anyway.

### Path to multi-machine

The wire protocol is self-describing (header + schema + data). Replace memfd+Unix
socket transport with TCP and the same format works over the network. Feldera does
exactly this: local=shared memory, remote=tarpc/TCP.

---

## Phase 0: Partitioned Storage (no parallelism)

**Goal:** Refactor storage so data is logically partitioned by `hash(PK) % N`,
but still processed sequentially by a single Engine. Pure internal refactoring —
external API unchanged, all tests pass, on-disk layout ready for multi-process.

**Changes:**

- `TableFamily` gains a `num_partitions` field (default 1 for backward compat)
- `ingest_batch` routes rows to partition `hash(pk) % num_partitions`
- Each partition is an independent `EphemeralTable`/`PersistentTable` in its own
  subdirectory: `<table_dir>/part_0/`, `<table_dir>/part_1/`, etc.
- Scan merges across partitions (merge-sort by PK)
- Views inherit partitioning from their source tables
- System tables remain unpartitioned (num_partitions=1)

**Deliverable:** Single-process gnitz with partitioned on-disk layout. All
existing tests pass. New tests verify partition routing and cross-partition scan.

---

## Phase 1: Process Spawning + Fan-Out

**Goal:** Master process spawns N worker processes. Each worker opens only its
partition. Master becomes a thin dispatcher.

**Master responsibilities:**

- Accept client connections (existing poll loop)
- On push: hash-partition incoming batch into N sub-batches, send each to the
  appropriate worker via IPC
- On scan: send scan request to all N workers, merge responses
- On DDL: apply locally, broadcast to workers
- Epoch management: coordinate worker barriers

**Worker responsibilities:**

- Open its partition of each table (partition directory assigned at fork)
- Accept push/scan from master via IPC
- Apply mutations to local partition
- Return results to master via IPC

**Communication:**

- Master ↔ Worker: Unix domain sockets (one per worker), existing IPC protocol
- Workers do NOT communicate with each other in this phase

**Deliverable:** DML scales linearly with N for partition-local operations.
DDL remains serialized through master.

---

## Phase 2: DAG Evaluation on Workers

**Goal:** Move reactive DAG evaluation from master to workers. Workers are
self-contained for partition-local circuit evaluation.

**Changes:**

- Each worker has its own `ProgramCache` and compiled circuit plans
- When a push arrives at a worker and the target table has dependent views,
  the worker evaluates the circuit on its local partition
- Master doesn't need to know about the DAG for partition-local operators
- Master broadcasts DDL that affects views; workers recompile circuits locally

**Operators that work partition-locally:**

- filter, map (linear — partition-independent)
- integrate (per-partition state)
- scan_trace (per-partition trace)
- delay (per-partition delay buffer)

**Operators that need Phase 3:**

- join on non-PK columns (data not co-partitioned by join key)
- global distinct (same key may appear on different partitions)
- global aggregate (partial results on each partition need merging)

**Deliverable:** Views with partition-local operators evaluate in parallel
across workers. Significant speedup for filter/map/integrate pipelines.

---

## Phase 3: Exchange Operators

**Goal:** Add `shard()` and `gather()` operators enabling cross-partition
computation.

**New operators:**

- `shard(key_expr)`: hash-partition output by arbitrary key expression,
  send sub-batches to peer workers. Used before join/distinct/aggregate
  when the required key differs from the partition key.
- `gather(worker_id)`: collect all partitions at a single worker.
  Used for global aggregates or final output assembly.

**Exchange protocol:**

- Worker-to-worker IPC (Unix domain sockets between workers, or via master relay)
- Per-epoch: each worker sends one batch to each peer, peers wait until all N
  batches arrive, then proceed
- This is the all-to-all barrier from Feldera's Exchange primitive
- Barrier can be built from N IPC round-trips (no atomics needed since
  we're multi-process)

**Circuit compilation changes:**

- `compile_from_graph` inserts shard/gather operators when the circuit graph
  requires cross-partition data movement
- Heuristic: if a join's key columns match the partition key, no exchange needed;
  otherwise insert shard before both inputs

**Deliverable:** Full DBSP operator set works across partitions. Joins,
aggregates, and distinct operate correctly on distributed data.

---

## Phase 4: Network Transport

**Goal:** Extend worker communication from Unix domain sockets to TCP for
multi-machine deployment.

**Changes:**

- Add a `Host` abstraction: `(address, port, worker_range)`
- Transport layer abstraction: `LocalTransport` (memfd+UDS) vs
  `NetworkTransport` (TCP + same wire format)
- Master routes sub-batches to the correct host, which routes to the
  correct local worker
- Exchange operators use network transport for remote peers, local
  transport for same-host peers

**Wire format:** Unchanged — the existing columnar protocol is
self-describing and works over any byte stream. Only the transport
framing differs (memfd size via fstat vs. length-prefix over TCP).

**Deliverable:** gnitz cluster spanning multiple machines. Linear
horizontal scaling for partitioned workloads.

---

## Key Design Questions (to resolve during implementation)

1. **Partition count**: Fixed at startup or dynamic? Feldera fixes it at
   circuit build time. Recommend: fixed N at startup, stored in system tables.

2. **Rebalancing**: When adding workers, do we reshuffle data? Recommend:
   defer to post-Phase 4. Initially, partition count is fixed for the
   cluster lifetime.

3. **Worker failure**: What happens when a worker dies mid-epoch? Recommend:
   abort the epoch, restart the worker, replay from WAL. No partial results.

4. **Skew handling**: What if one partition is much larger? Recommend:
   monitor partition sizes, alert on skew. True rebalancing is Phase 4+.

5. **Transaction semantics**: Currently single-threaded = serializable.
   Multi-process = need epoch-level serialization. Recommend: one epoch
   per client push (no concurrent pushes to the same table within an epoch).

---

## Files Likely Affected Per Phase

### Phase 0
- `gnitz/catalog/engine.py` — partition-aware table creation
- `gnitz/catalog/registry.py` — TableFamily with num_partitions
- `gnitz/storage/table.py` — partition routing in ingest_batch
- `gnitz/storage/ephemeral_table.py` — partition subdirectories
- `gnitz/storage/cursor.py` — merge cursor across partitions
- `gnitz/catalog/hooks.py` — partition-aware table/view hooks
- `rpython_tests/` — new partition tests

### Phase 1
- `gnitz/server/main.py` — fork workers at startup
- `gnitz/server/executor.py` — dispatcher logic (fan-out/merge)
- `gnitz/server/ipc.py` — master↔worker IPC channels
- `gnitz/server/worker.py` — new: worker process entry point

### Phase 2
- `gnitz/server/executor.py` — move DAG eval to worker
- `gnitz/catalog/program_cache.py` — per-worker cache
- Worker process needs its own ProgramCache + circuit compilation

### Phase 3
- `gnitz/dbsp/ops/` — new shard/gather operators
- `gnitz/dbsp/compile.py` — insert exchange operators in circuit graph
- `gnitz/catalog/system_tables.py` — circuit graph schema for exchange nodes
- Inter-worker IPC channels

### Phase 4
- `gnitz/server/transport.py` — new: transport abstraction
- `gnitz/server/ipc.py` — refactor to use transport abstraction
- `gnitz/server/network.py` — new: TCP transport
- Configuration: host/port/worker mapping

---

## References

- Feldera DBSP source: multi-worker runtime in `crates/dbsp/src/circuit/runtime.rs`,
  exchange in `crates/dbsp/src/operator/communication/exchange.rs`,
  sharding in `shard.rs`, gathering in `gather.rs`
- DBSP paper: "DBSP: Automatic Incremental View Maintenance" (Budiu et al., VLDB 2023)
- RPython threading: `rpython/rlib/rthread.py` (locks, start_new_thread),
  `rpython/rlib/rgil.py` (GIL control, releasegil=True for FFI)
