# Exchange Operator Optimizations

The exchange mechanism has one structural inefficiency and several tactical gaps. The structural
issue is the deeper one: the exchange is a runtime routing correction for a push-time routing
decision that was made without accounting for downstream compute partitioning requirements. The
tactical gaps are independently fixable. All optimizations are correct with the existing
multi-worker protocol; none require changes to the circuit compiler or operator algebra.

---

## Background: what the exchange does

gnitz uses a hub-and-spoke exchange topology. When a push arrives, the flow is:

```
1. fan_out_push receives batch B (master has all rows)
2. _split_batch_by_pk(B) → N sub-batches sent to workers (FLAG_PUSH)

3. Worker w: ingest_to_family(sub_batch[w])
4. Worker w: plan.execute_epoch(sub_batch[w])  ← pre-plan
5. Worker w: do_exchange sends pre_result to master (FLAG_EXCHANGE)

6. Master collects all N exchange batches
7. _relay_exchange: merge all N → repartition by shard_cols → send N sub-batches back
8. Worker w: plan.exchange_post_plan.execute_epoch(received_slice)  ← post-plan
9. Worker w: sends ACK
```

The exchange exists because data is stored partitioned by PK (step 2), but the REDUCE operator
in the post-plan needs data partitioned by GROUP-BY key (step 7). The SHARD node in the circuit
graph marks this boundary.

---

## Optimization 1: Eliminate the intermediate merge in `_relay_exchange`

### Problem

`master.py:159–176`:

```python
def _relay_exchange(self, view_id, worker_batches, schema):
    merged = ArenaZSetBatch(schema)           # ← full allocation, O(total_rows)
    for w in range(self.num_workers):
        if worker_batches[w] is not None:
            merged.append_batch(worker_batches[w])   # O(total_rows) copies
            worker_batches[w].free()
    dest_batches = repartition_batch(merged, shard_cols, ...)   # O(total_rows)
    merged.free()
```

The `merged` batch is a pass-through: it is allocated, filled from N sources, immediately
iterated by `repartition_batch`, then freed. It serves no purpose beyond holding the rows
during the single `repartition_batch` call.

### Fix

Add `repartition_batches` to `gnitz/dbsp/ops/exchange.py` — a multi-source variant that
repartitions N source batches in a single pass without an intermediate merge:

```python
def repartition_batches(source_batches, shard_col_indices, num_workers, assignment):
    """Repartition N source batches directly into destination sub-batches.
    Equivalent to merging all sources then calling repartition_batch, but
    without the intermediate allocation."""
    schema = None
    for sb in source_batches:
        if sb is not None:
            schema = sb._schema
            break
    if schema is None:
        return [None] * num_workers

    dest_batches = [None] * num_workers
    for sb in source_batches:
        if sb is None:
            continue
        for i in range(sb.length()):
            p = hash_row_by_columns(sb, i, shard_col_indices)
            w = assignment.worker_for_partition(p)
            if dest_batches[w] is None:
                dest_batches[w] = ArenaZSetBatch(schema)
            dest_batches[w]._direct_append_row(sb, i, sb.get_weight(i))
    return dest_batches
```

`_relay_exchange` becomes:

```python
def _relay_exchange(self, view_id, worker_batches, schema):
    shard_cols = self.program_cache.get_shard_cols(view_id)
    if len(shard_cols) > 0:
        dest_batches = repartition_batches(
            worker_batches, shard_cols, self.num_workers, self.assignment
        )
    else:
        # GATHER path: merge and split by PK
        merged = ArenaZSetBatch(schema)
        for w in range(self.num_workers):
            if worker_batches[w] is not None:
                merged.append_batch(worker_batches[w])
        dest_batches = self._split_batch_by_pk(merged, schema)
        merged.free()

    for w in range(self.num_workers):
        wb = worker_batches[w]
        if wb is not None:
            wb.free()
        ipc.send_batch(
            self.worker_fds[w], view_id, dest_batches[w], schema=schema
        )
        if dest_batches[w] is not None:
            dest_batches[w].free()
```

### Impact

Peak memory at the master during exchange: one per-worker source batch (already allocated by
the collect loop) plus N destination sub-batches growing incrementally. The full merged-batch
allocation is eliminated. For N=4 uniform distribution, peak intermediate memory drops from
`total_rows` to `total_rows / N`.

---

## Optimization 2: Push-time routing for trivial pre-plans (eliminates the exchange round-trip)

### The fundamental issue

Trace the trivial pre-plan case — `INPUT → SHARD → REDUCE`, no operators between INPUT and
SHARD — through the full exchange flow:

```
Step 1: master has batch B
Step 2: _split_batch_by_pk(B) → sub_batch[0..N-1] sent to workers
Step 4: plan.execute_epoch(sub_batch[w]) → returns sub_batch[w] unchanged
Step 5: worker sends sub_batch[w] back to master as FLAG_EXCHANGE
Step 7: master merges sub_batch[0..N-1] → ≈ B (reconstructs what it already had)
        repartition_batch(≈B, shard_cols) → new sub-batches by GROUP-BY hash
        sends to workers for post-plan
```

Step 7 reveals the round-trip for what it is: the master split B by PK (step 2), sent it to
workers, workers sent it back unchanged (steps 4–5), the master merged it back into ≈B (step
7a) — and only then did the actual useful work: repartitioning by shard columns (step 7b).

**The master had B the whole time.** For trivial pre-plans, the entire PK-split → round-trip
→ merge is pure overhead. The exchange is a retroactive correction for a push-time routing
decision made without knowing that downstream computation needs a different partitioning.

This is an instance of a well-known distributed systems principle: if the partitioning scheme
used for storage (PK hash) matches the partitioning scheme needed for computation (GROUP-BY
hash), no data movement is required. When they differ, exchange is the runtime workaround.
The static fix is to make the push-time routing decision computation-aware.

### Root cause generalisation

gnitz uses **one global partitioning scheme** — PK hash — for all tables. Every exchange in
the system is a symptom of the same mismatch:

| Operator | Needs co-location by | Storage partitioned by | Exchange needed? |
|---|---|---|---|
| REDUCE | GROUP-BY key | PK | Yes (current) |
| DISTINCT | PK | PK | No — already satisfied |
| JOIN (delta-trace) | join key | PK | Not yet implemented |

For the trivial pre-plan case, the master already has all information needed to make the
shard-column routing decision at push time, in the same pass over the input batch that
routes by PK. No round-trip to workers required.

### Precise condition for elimination

The exchange round-trip is eliminable for view V when:

1. V has `exchange_post_plan` (has a SHARD node in its circuit)
2. The pre-plan of V is **trivial**: no operator nodes exist between the INPUT node and the
   SHARD node in the circuit graph
3. The source table of V is the push target (direct dependency, not indirect)

Condition 2 is strictly stronger than "pre-plan doesn't modify shard columns." FILTER, NEGATE,
and DELAY all preserve shard column values, but they can remove rows (FILTER) or defer them
(DELAY), making the master unable to replicate the pre-plan without executing it. Only the
identity pre-plan (INPUT → SHARD with no intermediate nodes) is safe to bypass.

A pre-plan is trivial iff the SHARD node's single incoming edge originates directly from the
INPUT node (or equivalently: there are no operator nodes in the topological prefix before
SHARD). This is a compile-time graph property checkable in O(|edges|).

### Changes required

#### `gnitz/catalog/program_cache.py` — `get_shard_cols`

Extend to also detect trivial pre-plans. Rename or augment to return a flag:

```python
def get_exchange_info(self, view_id):
    """Returns (shard_cols, is_trivial_preplan).
    is_trivial_preplan = True iff SHARD node's input edge comes directly from
    an INPUT node with no intermediate operator nodes."""
    if view_id in self._exchange_info_cache:
        return self._exchange_info_cache[view_id]

    nodes = self._load_nodes(view_id)
    params = self._load_params(view_id)
    edges = self._load_edges(view_id)

    opcode_of = {}
    for nid, op in nodes:
        opcode_of[nid] = op

    # Find the SHARD node
    shard_nid = -1
    shard_cols = []
    for nid, op in nodes:
        if op == opcodes.OPCODE_EXCHANGE_SHARD:
            shard_nid = nid
            node_params = params.get(nid, {})
            idx = 0
            while (opcodes.PARAM_SHARD_COL_BASE + idx) in node_params:
                shard_cols.append(node_params[opcodes.PARAM_SHARD_COL_BASE + idx])
                idx += 1
            break

    if shard_nid == -1:
        result = ([], False)
        self._exchange_info_cache[view_id] = result
        return result

    # Check if SHARD's input comes directly from an INPUT node (no operators in between)
    # "INPUT node" = a node with no incoming edges in the circuit graph (source node)
    # and opcode not in the operator set (it's a placeholder for the delta register)
    in_degree = {}
    for nid, _ in nodes:
        in_degree[nid] = 0
    for _, src, dst, port in edges:
        in_degree[dst] = in_degree.get(dst, 0) + 1

    shard_inputs = []
    for _, src, dst, port in edges:
        if dst == shard_nid:
            shard_inputs.append(src)

    is_trivial = False
    if len(shard_inputs) == 1:
        src_nid = shard_inputs[0]
        src_op = opcode_of.get(src_nid, -1)
        # A source node has in_degree 0 and represents the input delta.
        # In gnitz's circuit graph, OPCODE_INPUT (or the implicit input node) is
        # the only node with in_degree 0 that feeds the circuit.
        if in_degree.get(src_nid, 0) == 0:
            is_trivial = True

    result = (shard_cols, is_trivial)
    self._exchange_info_cache[view_id] = result
    return result
```

Add `_exchange_info_cache = {}` to `__init__` and `invalidate` / `invalidate_all`.

The existing `get_shard_cols` can delegate to `get_exchange_info`:
```python
def get_shard_cols(self, view_id):
    shard_cols, _ = self.get_exchange_info(view_id)
    return shard_cols
```

#### `gnitz/catalog/program_cache.py` — new method `get_trivial_exchange_views`

```python
def get_trivial_exchange_views(self, source_table_id, registry):
    """Returns list of (view_id, shard_cols) for views that:
    (a) directly depend on source_table_id, and
    (b) have a trivial pre-plan (INPUT -> SHARD, no intermediate operators).
    Safe to call from master; reads only system tables."""
    dep_map = self.get_dep_map(registry)
    dependent_view_ids = dep_map.get(source_table_id, [])
    result = []
    for vid in dependent_view_ids:
        shard_cols, is_trivial = self.get_exchange_info(vid)
        if is_trivial and len(shard_cols) > 0:
            result.append((vid, shard_cols))
    return result
```

#### `gnitz/server/master.py` — `fan_out_push`

Replace the current `_split_batch_by_pk` + exchange collect loop with a single combined pass
for the trivial case:

```python
def fan_out_push(self, target_id, batch, schema):
    n = batch.length()

    # Detect views with trivial pre-plans depending on target_id.
    trivial_views = self.program_cache.get_trivial_exchange_views(
        target_id, self.engine.registry
    )

    # --- Single combined pass over the input batch ---
    pk_batches   = [None] * self.num_workers   # PK-partitioned, for storage
    # shard_batches: view_id -> [None] * num_workers
    shard_batches = {}
    for vid, sc in trivial_views:
        shard_batches[vid] = [None] * self.num_workers

    for i in range(n):
        pk_lo = r_uint64(batch._read_pk_lo(i))
        pk_hi = r_uint64(batch._read_pk_hi(i))
        pw = self.assignment.worker_for_partition(_partition_for_key(pk_lo, pk_hi))
        if pk_batches[pw] is None:
            pk_batches[pw] = ArenaZSetBatch(schema)
        pk_batches[pw]._direct_append_row(batch, i, batch.get_weight(i))

        for vid, sc in trivial_views:
            p = hash_row_by_columns(batch, i, sc)
            sw = self.assignment.worker_for_partition(p)
            sb = shard_batches[vid]
            if sb[sw] is None:
                sb[sw] = ArenaZSetBatch(schema)
            sb[sw]._direct_append_row(batch, i, batch.get_weight(i))

    # --- Send to workers ---
    # Pre-loaded exchange batches are sent BEFORE the FLAG_PUSH so workers
    # can stash them before evaluate_dag reaches do_exchange.
    for w in range(self.num_workers):
        for vid, sc in trivial_views:
            ipc.send_batch(
                self.worker_fds[w], vid, shard_batches[vid][w],
                schema=schema, flags=ipc.FLAG_PRELOADED_EXCHANGE,
            )
            if shard_batches[vid][w] is not None:
                shard_batches[vid][w].free()

        ipc.send_batch(
            self.worker_fds[w], target_id, pk_batches[w],
            schema=schema, flags=ipc.FLAG_PUSH,
        )
        if pk_batches[w] is not None:
            pk_batches[w].free()

    # --- Collect ACKs ---
    # No exchange messages expected for trivial views (workers use preloaded stash).
    # Non-trivial exchange messages still handled via the existing poll loop.
    acked = [False] * self.num_workers
    num_acked = 0
    exchange_buffers = {}
    exchange_counts  = {}
    exchange_schemas = {}

    while num_acked < self.num_workers:
        # ... (existing poll loop, unchanged) ...
```

The existing poll loop is unchanged. Since trivial views never send `FLAG_EXCHANGE`,
`exchange_counts[vid]` never reaches `num_workers` for them and `_relay_exchange` is never
called. Non-trivial views still go through the existing exchange path.

#### `gnitz/server/ipc.py` — new flag

```python
FLAG_PRELOADED_EXCHANGE = 1024   # pre-routed shard slice; worker stashes, no IPC reply
```

#### `gnitz/server/worker.py` — `WorkerExchangeHandler`

Add a stash dict and drain-preloaded logic:

```python
class WorkerExchangeHandler(object):
    def __init__(self, master_fd):
        self.master_fd = master_fd
        self._stash = {}   # view_id -> ArenaZSetBatch (pre-loaded from master)

    def stash_preloaded(self, view_id, batch):
        """Called by the worker's message loop before processing FLAG_PUSH."""
        if batch is not None and batch.length() > 0:
            self._stash[view_id] = batch.clone()

    def do_exchange(self, view_id, batch, shard_cols):
        schema = batch._schema
        if view_id in self._stash:
            # Pre-loaded by master at push time — no IPC round-trip needed.
            result = self._stash.pop(view_id)
            return result

        # Normal exchange path (non-trivial pre-plan).
        ipc.send_batch(
            self.master_fd, view_id, batch,
            schema=schema, flags=ipc.FLAG_EXCHANGE,
        )
        payload = ipc.receive_payload(self.master_fd)
        if payload.batch is not None and payload.batch.length() > 0:
            result = payload.batch.clone()
        else:
            result = ArenaZSetBatch(schema)
        payload.close()
        return result
```

#### `gnitz/server/worker.py` — `_handle_request`

The master sends FLAG_PRELOADED_EXCHANGE frames before FLAG_PUSH. The worker must drain all
leading preloaded frames before processing the push:

```python
def _handle_request(self):
    payload = None
    try:
        payload = ipc.receive_payload(self.master_fd)

        # Drain any pre-loaded exchange frames that precede the actual request.
        while payload.flags & ipc.FLAG_PRELOADED_EXCHANGE:
            vid = intmask(payload.target_id)
            self.exchange_handler.stash_preloaded(vid, payload.batch)
            payload.close()
            payload = ipc.receive_payload(self.master_fd)

        # ... dispatch on FLAGS as before ...
```

Message ordering on a socketpair is FIFO, so FLAG_PRELOADED_EXCHANGE frames always arrive
before the FLAG_PUSH they precede. The stash is populated before `evaluate_dag` is called,
so `do_exchange` finds the preloaded result immediately.

### Impact

For `INPUT → SHARD → REDUCE` (every simple GROUP BY / aggregate without WHERE or computed
GROUP BY expressions):

| Phase | Before | After |
|---|---|---|
| Round-trips per push | 2N (N push + N exchange relay) | N (N combined push, no exchange) |
| IPC messages total | 3N (N push + N exchange + N relay) | N |
| Master work | O(2 × total_rows) serial | O(total_rows) single pass |
| Worker work | pre-plan + do_exchange + post-plan | ingest + post-plan |
| `_relay_exchange` called | yes | no |

For `FILTER → SHARD → REDUCE` and all other non-trivial pre-plans: unchanged. The existing
exchange mechanism handles these correctly.

### The deeper principle

This optimization is an instance of **co-partitioning**: store data (or route it at ingestion)
according to the partitioning scheme needed for the dominant downstream computation, eliminating
data movement during query execution. The same principle applies to future JOIN exchange:
if two tables that will be joined are co-partitioned by their join key, no exchange is needed
for the join. The master, as the sole data entry point, is the natural place to implement
computation-aware routing.

---

## Optimization 3: Sorted/consolidated flag propagation over IPC

### Problem

`WorkerExchangeHandler.do_exchange` (worker.py:34):

```python
result = payload.batch.clone()   # _sorted = False, _consolidated = False always
```

The `ArenaZSetBatch` reconstructed from IPC is always created with `_sorted = False`. After
`source-optimizations.md` adds the seal in `execute_epoch`, every exchange-triggered
`execute_epoch` call will pay O(N log N) to sort the exchanged batch, even when the
repartitioned batch is already sorted.

Is the exchange output sorted? In `repartition_batch`, rows are iterated in batch order
(`for i in range(batch.length())`). If the input batch is sorted by PK (which it will be
after the `source-optimizations.md` seal), rows routed to the same destination worker maintain
their relative order. **Sub-batches from `repartition_batch` are sorted when the input is
sorted.** Feldera explicitly notes this: "tuples added to each shard are also ordered, so we
can use the more efficient Builder API (instead of Batcher)."

### Fix

**In `repartition_batch` / `repartition_batches` / `_split_batch_by_pk`:**

```python
for w in range(num_workers):
    if sub_batches[w] is not None:
        sub_batches[w]._sorted = batch._sorted
        sub_batches[w]._consolidated = batch._consolidated   # once that flag exists
```

**In the IPC protocol (`gnitz/server/ipc.py`):** The `flags` field at header offset 72 has
spare bits. Add two batch-property bits transmitted with every message:

```python
FLAG_BATCH_SORTED       = 2048   # batch._sorted == True on the sender side
FLAG_BATCH_CONSOLIDATED = 4096   # batch._consolidated == True (after source-opts)
```

`serialize_to_memfd` includes these bits when the batch has the corresponding properties.
`receive_payload` restores them on the reconstructed batch. Then `do_exchange` receives a
batch with `_sorted = True` (or `_consolidated = True`), and the seal in `execute_epoch`
short-circuits to a no-op.

### Impact

After `source-optimizations.md`, every exchange-triggered `execute_epoch` currently pays
O(N log N) to sort an already-sorted batch. With these two fixes, the sort is eliminated.
The seal becomes O(1) for exchange results, matching the cost for non-exchange pushes.

---

## Optimization 4: Pre-allocation in batch partitioning

### Problem

`repartition_batch` and `_split_batch_by_pk` create `ArenaZSetBatch(schema)` lazily with no
capacity hint. Feldera pre-allocates builders with `key_count / shards` as capacity estimate.

### Fix

Pass `initial_capacity = batch.length() // num_workers` when creating the first sub-batch per
destination worker. This is a conservative lower bound (some workers may receive more rows if
the hash is skewed, but the estimate is correct on average):

```python
# In repartition_batch:
if sub_batches[w] is None:
    cap = batch.length() // num_workers
    sub_batches[w] = ArenaZSetBatch(schema, initial_capacity=cap)
```

Same change in `_split_batch_by_pk`. For large batches this eliminates multiple internal
`_cols` buffer reallocations during row appending.

---

## Optimization 5: `fan_out_push` poll array rebuilt on every iteration

### Problem

`master.py:108–116`: Three `newlist_hint` allocations + N appends per iteration of the
`while num_acked < num_workers` loop, including no-event timeout iterations:

```python
while num_acked < self.num_workers:
    poll_fds    = newlist_hint(self.num_workers)   # re-allocated every loop
    poll_events = newlist_hint(self.num_workers)
    poll_wids   = newlist_hint(self.num_workers)
    for w in range(self.num_workers):
        if not acked[w]:
            poll_fds.append(...)
```

### Fix

Build the poll arrays once before the loop. When a worker ACKs, swap-remove its entry (O(1)
swap-delete, same pattern as `_cleanup_client`):

```python
poll_fds    = newlist_hint(self.num_workers)
poll_events = newlist_hint(self.num_workers)
poll_wids   = newlist_hint(self.num_workers)
for w in range(self.num_workers):
    poll_fds.append(self.worker_fds[w])
    poll_events.append(ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP)
    poll_wids.append(w)

num_acked = 0
while num_acked < self.num_workers:
    revents = ipc_ffi.poll(poll_fds, poll_events, 10)
    for pi in range(len(poll_fds)):
        if not (revents[pi] & ...):
            continue
        w = poll_wids[pi]
        payload = ipc.receive_payload(self.worker_fds[w])
        if payload.flags & ipc.FLAG_EXCHANGE:
            ...
        else:
            # ACK — swap-remove from poll arrays
            last = len(poll_fds) - 1
            poll_fds[pi]    = poll_fds[last];    del poll_fds[last]
            poll_events[pi] = poll_events[last]; del poll_events[last]
            poll_wids[pi]   = poll_wids[last];   del poll_wids[last]
            num_acked += 1
            break   # pi is now a different worker; restart inner loop
```

---

## Optimization 6: `fan_out_seek_by_index` — master-side index routing table

### Problem

`master.py:241–262`: Broadcasts the index seek to all N workers and returns the first
non-empty result. For most secondary index seeks the target value is on exactly one worker.
All N-1 non-owning workers do a seek + miss, wasting N-1 IPC round-trips.

The naïve fix — route by `_partition_for_key(key_lo, key_hi)` on the index key — does **not**
work. Index entries are distributed by **source row PK hash**, not by index key hash. Worker
`w` receives source rows with PK in its partition range, runs `ingest_to_family`, and builds
index entries into its local `EphemeralTable` from those rows. The index entry for column value
`v` lands on whichever worker owns the source row with `v`, which is determined by the source
PK — unknown at seek time.

### Fix

The master already has all information needed at push time: `fan_out_push` scans every row to
assign it to a worker via `_split_batch_by_pk`. In that same pass, the full row data is
available including every indexed column value. Record the mapping
`(table_id, col_idx, index_key) → worker_id` in a master-side routing dict as the scan
proceeds.

Add `_index_routing = {}` to `MasterDispatcher.__init__` and call a helper from
`fan_out_push` after the per-row worker assignment is known:

```python
# For each row i, after w = assignment.worker_for_partition(_partition_for_key(...)):
for circuit in family.index_circuits:
    idx_key = promote_to_index_key_from_batch(batch, i, circuit)
    idx_lo = intmask(r_uint64(idx_key))
    idx_hi = intmask(r_uint64(idx_key >> 64))
    if batch.get_weight(i) >= r_int64(0):
        _routing_set(self._index_routing, target_id, circuit.source_col_idx,
                     idx_lo, idx_hi, w)
    else:
        _routing_del(self._index_routing, target_id, circuit.source_col_idx,
                     idx_lo, idx_hi)
```

`fan_out_seek_by_index` gains a fast path:

```python
def fan_out_seek_by_index(self, target_id, col_idx, key_lo, key_hi, schema):
    w = _routing_lookup(self._index_routing, target_id, col_idx, key_lo, key_hi)
    if w >= 0:
        # Fast path: routing table hit — 1 IPC round-trip.
        ipc.send_batch(
            self.worker_fds[w], target_id, None, schema=schema,
            flags=ipc.FLAG_SEEK_BY_INDEX,
            seek_col_idx=col_idx, seek_pk_lo=key_lo, seek_pk_hi=key_hi,
        )
        payload = ipc.receive_payload(self.worker_fds[w])
        ...
    else:
        # Fallback: key not yet seen — broadcast as before, no regression.
        ...
```

### Scope and constraints

- Works correctly for **unique** indices (the primary use case: FK validation, secondary index
  point lookups): one source row per index key → one routing entry per key → perfect routing.
- For **non-unique** indices (multiple source rows share an index value, potentially on
  different workers): the routing entry holds the last-seen worker only. The fallback broadcast
  handles misses. A complete solution for non-unique indices requires a `{key: list[worker_id]}`
  structure; deferred.
- The routing table is in-memory on the master, size proportional to rows × indexed columns.
  Consistent with the worker `EphemeralTable` index storage — both are rebuilt on restart.
- No IPC protocol changes. No worker changes. No index storage changes.

### Impact

For unique index seeks (FK validation, secondary index point lookups):

| Before | After |
|---|---|
| N IPC round-trips, N-1 are miss | 1 IPC round-trip after routing table is populated |
| All N workers do seek | 1 worker does seek |

Keys not yet in the routing table (pre-push, or non-unique miss): fall back to existing
broadcast. No regression.

---

## Correctness note: STRING group-by across multiple pushes

`hash_row_by_columns` and `_extract_group_key` both hash STRING group-by columns using
`_mix64(blob_offset_int)` — the integer offset of the string in the batch's blob arena, not
the string's content. Within a single batch all strings share one arena so routing is
self-consistent. Across separate pushes the same string value can have different blob offsets,
potentially routing to different workers and splitting a group's reduce state.

This is a pre-existing design limitation shared by `_extract_group_key` in `reduce.py` and
`hash_row_by_columns` in `exchange.py`. Feldera hashes key content directly. The fix is to
call `string_logic.resolve_string(...)` on string columns and hash the resolved bytes via
`xxh.hash64` rather than hashing the blob offset integer. Both `hash_row_by_columns` and
`_extract_group_key` must be updated together to preserve consistency.

This is documented here as a known risk but not planned for this optimization pass, since it
requires coordinated changes to the reduce and exchange subsystems and is a separate correctness
concern from the performance optimizations above.

---

## Dependencies and interactions

- **`source-optimizations.md`**: Optimization 3 (IPC sorted/consolidated flag) is only
  beneficial after the seal in `execute_epoch` is implemented. Without the seal, there is no
  `to_consolidated()` call at circuit entry and the flag is never checked.
- **`source-optimizations.md` Opt 2 (seal)**: After the seal, exchange-triggered
  `execute_epoch` pays O(N log N) for every push unless Optimization 3 is also applied.
  The two are a natural pair.
- Optimization 2 (push-time routing) is independent of all other plans and can be implemented
  first.
- Optimization 1 (`repartition_batches`) is a strict improvement to the fallback path that
  remains for non-trivial pre-plans after Optimization 2 is in place.

---

## Implementation order

```
1. Optimization 1: repartition_batches (eliminates merge allocation in _relay_exchange)
   — exchange.py: add repartition_batches
   — master.py: replace merge+repartition_batch with repartition_batches in _relay_exchange

2. Optimization 4: pre-allocation hints
   — exchange.py: initial_capacity in repartition_batch + repartition_batches
   — master.py: initial_capacity in _split_batch_by_pk

3. Optimization 5: poll array build-once
   — master.py: fan_out_push poll loop

4. Optimization 6: fan_out_seek_by_index master-side routing table
   — master.py: _index_routing dict in __init__; routing update in fan_out_push row scan
     (same pass as _split_batch_by_pk); fast-path lookup in fan_out_seek_by_index

5. Optimization 3: IPC sorted/consolidated flags        ← after source-optimizations.md Step 1
   — ipc.py: FLAG_BATCH_SORTED, FLAG_BATCH_CONSOLIDATED in serialize/receive
   — exchange.py: propagate _sorted/_consolidated in repartition_batch / repartition_batches
   — master.py: propagate in _split_batch_by_pk

6. Optimization 2: push-time routing for trivial pre-plans  ← biggest win, most protocol change
   — program_cache.py: get_exchange_info, get_trivial_exchange_views
   — ipc.py: FLAG_PRELOADED_EXCHANGE
   — master.py: fan_out_push combined pass + preloaded send
   — worker.py: WorkerExchangeHandler stash, _handle_request drain loop
```

Steps 1–4 are independent and can be done in any order. Step 5 requires `source-optimizations.md`
Step 1 (the `_consolidated` flag). Step 6 is the largest change and benefits most from having
Step 5 complete first (preloaded batches arrive with correct sorted flags).

---

## Test coverage

No new test files required. Existing tests cover correctness.

- `exchange_test.py::test_repartition_batch`: exercises `repartition_batch` directly.
  Add an assertion that sub-batch `_sorted` equals input `_sorted`.
- `exchange_test.py::test_compile_split_plan` / `test_compile_shard_with_negate`: verify
  pre-plan / post-plan split — regression coverage for Optimization 2.
- `rust_client/gnitz-py/tests/test_workers.py::test_push_scan_multiworker`: end-to-end
  multi-worker push with partition distribution. This is the primary regression test for
  Optimization 2; any routing error manifests as wrong row counts or missing rows.
- `test_workers.py::test_zset_union_invariant` and `test_unique_pk_across_workers`:
  regression for correctness of the combined PK+shard routing pass.
- All tests run with `GNITZ_WORKERS=4` per project convention.

A targeted test for Optimization 2 to add to `test_workers.py`:

```
test_trivial_preplan_no_exchange:
  Create a table T and a view V = SELECT group_col, COUNT(*) FROM T GROUP BY group_col
  (identity pre-plan: INPUT -> SHARD -> REDUCE).
  Push 100 rows across multiple transactions. Assert correct COUNT per group.
  Instrument: confirm _relay_exchange is never called for V during any push
  (can use a counter in MasterDispatcher or verify via log absence).
  Run with GNITZ_WORKERS=4.
```
