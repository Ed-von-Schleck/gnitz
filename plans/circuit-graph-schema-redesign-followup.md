# Circuit Graph Schema Redesign — Follow-up

The structural rewrite from `circuit-graph-schema-redesign.md` landed and all
669 E2E tests pass. Three items remain.

## 1. Replace the synthesis layer with native `OpNode` dispatch

`compiler.rs::load_circuit` currently translates the new schema back into the
legacy `params: HashMap<i32, HashMap<i32, i64>>` representation so the rest of
the compiler doesn't have to change. The right end state is an exhaustive
`match op_node` in `emit_node()` — it eliminates `extract_map_code`,
`extract_const_strings`, the `PARAM_*_BASE` constants, and the slot-encoding
round-trip. As a side effect, `ann.in_schema` / `resolve_primary_input_schema()`
become removable, and the `trace_side_sources` derivation can be simplified to
read directly from `OpNode::ScanTrace` variants.

Sequence:

1. Add a `LoadedCircuit { ordered: Vec<NodeId>, nodes: HashMap<NodeId,
   OpNode>, outgoing, incoming }` struct alongside the existing
   `CircuitGraph`.
2. Have `load_circuit` populate **both** representations transitionally.
3. Migrate `emit_node` arms one opcode at a time to read from
   `loaded.nodes[&nid]` instead of `graph.params`.
4. When the last consumer of `params` / `str_params` / `sources` /
   `group_cols` is gone, delete the synthesis layer and drop the legacy
   slot constants from `gnitz-wire`.

This is mechanical but touches every operator handler. Best done
incrementally while keeping E2E green.

## 2. Test sweep cleanup

- **`view_tests.rs` is a stub.** Re-port the survivable assertions over the
  wire path via a real `GnitzClient` instance, or confirm they are already
  covered by E2E and delete the file.
- **`test_circuit_table_surface_introspectable` only checks `scan_family`.**
  Add a follow-up assertion for `seek_family` once the seek path for system
  tables is debugged (it currently returns `None` for system tables seeded
  with a single row, unrelated to this redesign).

## 3. Schema agreement hardening

Today `circuit_nodes_schema()` exists independently in
`gnitz-core::types` and `gnitz-engine::catalog::sys_tables`. They drift
silently and surface as `"schema mismatch: client schema differs from server
schema"` only at runtime on first push.

Cheapest immediate guard: **boot-time invariant check**. In
`bootstrap.rs::open()`, after constructing the engine schemas, compare every
sys-table schema descriptor against a freshly-imported gnitz-core
`Schema::to_descriptor()` round-trip. Fail to start with a precise diff if
any column's `(type_code, nullable, size)` differs.

Long-term: move the system-table schema definitions into `gnitz-wire` (which
both client and server already depend on) and import the same `const fn`
from both sides. The descriptors are already `const`-constructible on the
engine side; the client side uses `OnceLock<Schema>` only because
`ColumnDef::name` is a `String` — that can change to `&'static str` for
system tables specifically.

## Out of scope (still)

Same as the original plan: incremental graph editing
(`ALTER VIEW ADD NODE`), cross-view program reuse / interning, Z-Set algebra
over programs, native composite PK columns in the catalog, server-synthesised
`GatherReduce` planning. The redesign unblocks the typed-enum
foundation but does not pursue these.
