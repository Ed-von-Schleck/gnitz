# gnitz-wire contract hardening

`gnitz-wire` is the zero-dependency wire-protocol contract shared by the client
(`gnitz-core`/`sql`/`capi`/`py`) and the server (`gnitz-engine`). It is already
high quality: strong trust-boundary validation (`RangeDescriptor::decode`,
`try_decode_german_string`, the `MAP_EXPR`/range arms of `decode_op_node`),
compile-time bit-layout guards (`flags.rs`, `catalog.rs`), and load-bearing
invariant comments.

This plan applies a focused set of committed changes that (1) remove a real
silent-wrong-width hazard, (2) delete legacy/vestigial circuit-schema
scaffolding the project's no-legacy policy forbids, (3) close two decode
trust-boundary gaps, (4) make an over-wide enum's illegal states
unrepresentable, and (5) trim dead public surface and stale docs. Every snippet
below was validated against current source; the two with const-eval subtlety
(§A, §C2) were compile-checked on the system toolchain (rustc 1.96.0).

Because this is a wire crate, several items ripple into `gnitz-core` and
`gnitz-engine`; per-item "Blast radius" lists the exact external edits. There
are no backward-compatibility constraints (pre-alpha, never released), so the
clean path is always taken.

## Change summary (ordered by value)

| # | Change | Value | Effort | Crates touched |
|---|--------|-------|--------|----------------|
| A | Single-source type byte-width; delete `wire_stride` magic-range table | High | Small | wire |
| B1 | Remove vestigial `reindex_col` column + `reindex` param + legacy fallback | Med-High | Medium | wire, core, engine |
| B2 | Delete legacy `OPCODE_SCAN_TRACE` alias | Medium | Small | wire, core |
| C1 | Reject SCAN rows with a missing `source_table` instead of coercing to id 0 | Medium | Small | wire |
| C2 | Collapse `decode_op_node`'s `x if x ==` guard chain into a const-pattern `match` | Medium | Small | wire |
| D | Introduce `SetJoinKind` for anti/semi joins; delete 6 `unreachable!()` | Medium | Small | wire, core, engine |
| E1 | Trim dead public surface (flags consts, `REGION_PK`/`WEIGHT`, `col_index`, `HELLO_STATUS_ERROR`) | Low | Small | wire |
| E2 | Structural dedup (`align8`, `col()` constructor, shared PK-width message) | Low | Small | wire, engine |
| E3 | Doc hygiene (nonexistent crate name, stale byte literal, `MAX_PK_BYTES` I128, legacy labels) | Low | Small | wire |

---

## A. Single-source the type byte-width (delete the `wire_stride` magic-range table)

**Problem.** Byte-width-per-type is encoded by two independent tables.
`TypeCode::stride()` (`types.rs:79-87`) is an exhaustive named `match` — a new
variant is a compile error. The free `wire_stride(tc: u8)` (`types.rs:478-487`)
re-derives the same widths from hard-coded numeric ranges with no link to the
enum:

```rust
pub const fn wire_stride(tc: u8) -> usize {
    match tc {
        1 | 2 => 1, 3 | 4 => 2, 5..=7 => 4, 8..=10 => 8, 11..=15 => 16, _ => 8,
    }
}
```

`wire_stride` is the load-bearing form on the cross-process region-sizing path:
`gnitz-engine/src/schema.rs:970` wraps it in `const fn type_size`, and
`index_key_types` (`types.rs:217`) sums it to check PK-region stride against
`MAX_PK_BYTES`. The two tables agree today, but `stride()` and `wire_stride`
live in different maintenance regimes: adding a 16th type code (next free value
`16`) makes `stride()` a compile error while `wire_stride(16)` silently falls to
`_ => 8`. A 16-byte addition then under-allocates its region by 8 bytes/value,
surfacing only as the far-side `"WAL block PK region size mismatch"` hard-reject
the doc at `types.rs:142` warns about. `TypeCode::wire_stride()` (the method,
`types.rs:130-133`) already routes through `stride()`, so the free fn is a
genuinely separate code path, and `FixedInt::width()` (`types.rs:305-312`)
states the narrow subset a third time.

**Design.** Make `TypeCode::stride()` the single source of truth and delegate
the `u8` form to it. This deletes the second width table outright (not merely
renames its constants), so the two can no longer disagree by construction. The
delegation requires `try_from_u8` to be `const` (its body is already a pure
`match`; making a fn `const` is strictly more permissive and breaks no caller).
Add a compile-time guard pinning discriminant integrity so the single source
stays trustworthy. Keep the load-bearing `unknown => 8` default.

**Edits** (`gnitz-wire/src/types.rs`), compile-checked on rustc 1.96.0:

```rust
// types.rs:56 — make const (no body change). Strictly more permissive.
pub const fn try_from_u8(v: u8) -> Option<Self> { /* unchanged match body */ }

// types.rs:478-487 — delegate; the magic-range table is gone.
/// Wire stride (byte width) for a column type code. Delegates to the single
/// width source `TypeCode::stride()`; unknown codes return 8 (load-bearing —
/// engine `compare_rows` and the `SchemaColumn` trailing-slot fill depend on it).
pub const fn wire_stride(tc: u8) -> usize {
    match TypeCode::try_from_u8(tc) {
        Some(t) => t.stride() as usize,
        None => 8,
    }
}

// types.rs — new compile-time guard (place after wire_stride). A mis-assigned
// discriminant fails the build; width can no longer drift from stride() because
// there is only one width table now.
const _: () = {
    let mut v: u16 = 0;
    while v <= 255 {
        if let Some(t) = TypeCode::try_from_u8(v as u8) {
            assert!(t as u8 == v as u8, "TypeCode discriminant must round-trip through try_from_u8");
        }
        v += 1;
    }
    assert!(wire_stride(0) == 8 && wire_stride(16) == 8, "unknown-code default must be 8");
};
```

> Note: `Option::map_or` is **not** const-stable on this toolchain — use the
> explicit `match` above, not `try_from_u8(tc).map_or(8, ...)`.

`FixedInt::width()` is left as-is: it is reachable only by *holding* a `FixedInt`
(constructed via the exhaustive `from_type_code`, `types.rs:284`), so it cannot
drift for an unhandled code — it is a typestate accessor, not a parallel table.

**Blast radius.** Body-only change to one public `const fn`; its ~14 raw-`u8`
call sites (`schema.rs:970/1131/1132`, `sys_tables.rs:101`, `ops/reindex.rs:353`,
plus `pk.rs` internals) and the const `type_size` wrapper are unaffected. The
`try_from_u8` `const` upgrade does not touch its 4 non-wire callers.

**Verification.** `make test` (the new `const _` guard runs at compile time);
the existing `fixed_int_*` and `reindex_output_type_policy` tests must stay green.

**Out of scope (decided against):** macro-generating the three 15-entry listings
(`type_code::*` consts, the enum, `try_from_u8`). The residual gap after this
change — a code added to the enum + `stride()` but forgotten in `try_from_u8`,
yielding `wire_stride => 8` for that code — is bounded by the *already* exhaustive
`stride()` and `from_type_code()` matches, which drag any new-variant author into
`types.rs` three lines from `try_from_u8`. A macro would trade that small residual
for reduced greppability of the most-referenced symbols in the crate.

---

## B. Purge legacy / vestigial circuit-schema scaffolding

The no-legacy policy is explicit: keeping legacy code is "strictly not allowed."
Two such remnants survive in the circuit layer.

### B1. Remove the vestigial `reindex_col` column, the `reindex` decode param, and the legacy fallback

**Problem.** `CIRCUIT_NODES_COLS` carries a `reindex_col` column
(`catalog.rs:77-81`) that is **always null**. The producers say so outright:
`gnitz-core/src/circuit.rs:20` ("the physical `reindex_col` cell is now
permanently `None` for `Map` nodes") and `gnitz-core/src/circuit.rs:148`
("the physical column persists for **back-compat**"). The live reindex column
list moved to `CircuitNodeColumns` under `NODE_COL_KIND_REINDEX`. The dead chain:

- Encode (`gnitz-core` `encode_op_node`) emits `reindex = None` for every node.
- Persisted as a null `reindex_col` cell.
- Engine load (`load.rs:96-99`) reads the always-null cell → `Option<u16> = None`.
- `decode_op_node`'s `reindex: Option<u16>` param feeds only the `MAP_EXPR`
  legacy fallback (`circuit.rs:333-336`), which `extend`s a `None` (no-op) and
  `resize`s to the same length (no-op).

The `for c in cols` loop in the `MAP_EXPR` arm already builds `reindex_cols` /
`reindex_target_tcs` in lockstep, so the fallback is dead for every circuit a
current producer can write (pre-alpha → no pre-migration persisted circuits exist).

**Design.** Delete the column, the param, and the fallback across all three
crates. Indices are derived via `col_index_in`/`cidx`, so they auto-adjust to the
6-column schema; the bootstrap schema (which must mirror `CIRCUIT_NODES_COLS`)
drops the same entry.

**Edits.**

`gnitz-wire/src/catalog.rs` — remove the `reindex_col` `WireSysCol` from
`CIRCUIT_NODES_COLS` (`:77-81`), leaving `view_id, sub, node_id, opcode,
source_table, expr_program` (7 → 6 columns).

`gnitz-wire/src/circuit.rs` — drop the `reindex` param from `decode_op_node`
(`:282`) and delete the legacy fallback in the `MAP_EXPR` arm:

```rust
// circuit.rs decode_op_node signature — drop `reindex: Option<u16>`:
pub fn decode_op_node(
    opcode: u64,
    src_tab: Option<TableId>,
    expr_blob: Option<Vec<u8>>,
    cols: &[CircuitNodeColumn],
) -> Result<OpNode, String> {

// MAP_EXPR arm: the loop already fills both vecs in lockstep — DELETE this block:
-   if reindex_cols.is_empty() {
-       reindex_cols.extend(reindex);
-       reindex_target_tcs.resize(reindex_cols.len(), 0);
-   }
```

Also retitle the `MAP_EXPR` doc comment and the `MapKind::Expression` field doc
(`circuit.rs:189-192`) to drop the "legacy circuits" / "may be shorter than
`reindex_cols`" wording — with the fallback gone the two vecs are always equal
length. Update the two `decode_reindex_cols_legacy_cell_fallback` /
`decode_legacy_cell_yields_zero_target_tcs` tests (`circuit.rs:449-453,492-496`)
to drop the removed `reindex` argument (they remain valid as plain-compute-map
cases passing no reindex rows).

**Blast radius (external edits required to compile):**

- `gnitz-core/src/circuit.rs`: drop the 4th tuple element (`reindex`) from
  `encode_op_node`'s return and from the `rows.nodes` tuple (`:73-74,108`);
  drop the arg at the `decode_op_node` call (`:110`); delete the `reindex` slot
  write and its back-compat comment (`:148`).
- `gnitz-engine/src/query/compiler/load.rs`: delete `NODES_COL_REINDEX_COL`
  (`:19`), the `reindex` read (`:96-99`), and the arg at the call (`:116`).
- `gnitz-engine/src/query/compiler/mod.rs`: delete `bb.put_null(); // reindex_col`
  (`:1688`).
- `gnitz-engine/src/catalog/bootstrap.rs`: delete the `("reindex_col", …)` entry
  (`:233`) so the bootstrap schema matches `CIRCUIT_NODES_COLS`.

**Verification.** `make e2e WORKERS=4` — exercises catalog persistence and the
circuit load path end-to-end (a column-count mismatch between `CIRCUIT_NODES_COLS`
and bootstrap surfaces immediately as a schema/region mismatch). `make test` for
the wire/core unit tests.

### B2. Delete the `OPCODE_SCAN_TRACE` legacy alias

**Problem.** `circuit.rs:25` defines `pub const OPCODE_SCAN_TRACE: u64 =
OPCODE_SCAN_DELTA;` with the comment "Legacy alias retained temporarily during
the circuit-graph schema rewrite. New code should use `OPCODE_SCAN_DELTA`." It is
referenced only in two `gnitz-core` import lists (`lib.rs:45`, `types.rs:37`) and
in no code path — `gnitz-core`'s circuit body uses the distinct
`OPCODE_SCAN_TRACE_TABLE` (= 31). The duplicate-valued alias is also why any
future `match` on these opcodes would need guards rather than const patterns.

**Design / edits.**

- `gnitz-wire/src/circuit.rs`: delete the alias and its comment (`:23-25`); keep
  the one-line provenance note on `OPCODE_SCAN_DELTA` (`:18-22`) but drop the
  "Legacy alias retained temporarily" sentence.
- `gnitz-core/src/lib.rs:45` and `gnitz-core/src/types.rs:37`: remove
  `OPCODE_SCAN_TRACE` from the import lists.

**Blast radius.** Two import-list deletions; no code path uses the symbol.

**Verification.** `make test` (compile is the test — an orphaned import fails).

---

## C. Tighten two decode trust-boundary gaps

### C1. Reject SCAN rows with a missing `source_table` instead of coercing to id 0

**Problem.** `decode_op_node` enforces a loud-fail policy everywhere
(`circuit.rs:309/324/376/379/399`) except the two SCAN arms (`:304-305`):

```rust
x if x == OPCODE_SCAN_DELTA => OpNode::ScanDelta(src_tab.unwrap_or(0)),
x if x == OPCODE_SCAN_TRACE_TABLE => OpNode::ScanTrace(src_tab.unwrap_or(0)),
```

The encode side always writes `Some(tid)`, so `None` means a corrupt null
`source_table` cell. Table id 0 is provably invalid (`FIRST_USER_TABLE_ID = 16`,
system ids occupy `0 < id < 16`), so `ScanDelta(0)` matches no relation at
`emit.rs:179` and silently produces empty output — the silent-wrong-result mode
the function's own comments reject. The historical "SCAN_TRACE with source = 0"
sentinel was removed (`gnitz-core/src/circuit.rs:268`), so no path emits source 0.

**Design / edits** (`gnitz-wire/src/circuit.rs:304-305`):

```rust
x if x == OPCODE_SCAN_DELTA => {
    let tid = src_tab.ok_or_else(|| "SCAN_DELTA missing source_table".to_string())?;
    OpNode::ScanDelta(tid)
}
x if x == OPCODE_SCAN_TRACE_TABLE => {
    let tid = src_tab.ok_or_else(|| "SCAN_TRACE missing source_table".to_string())?;
    OpNode::ScanTrace(tid)
}
```

Add a unit test alongside `decode_rejects_non_pk_eligible_target_tc`:

```rust
#[test]
fn decode_rejects_scan_missing_source_table() {
    assert!(decode_op_node(OPCODE_SCAN_DELTA, None, None, &[]).unwrap_err().contains("source_table"));
    assert!(decode_op_node(OPCODE_SCAN_TRACE_TABLE, None, None, &[]).unwrap_err().contains("source_table"));
}
```

(Signature shown post-§B1, i.e. without the `reindex` param.)

**Blast radius.** Both `decode_op_node` callers already handle `Err`:
`gnitz-core` `Circuit::from_rows` propagates via `?`; `gnitz-engine` `load.rs:116`
maps `Err → None → abort load` via `.ok()?`. No valid circuit changes behavior.

**Verification.** `make test`; `make e2e WORKERS=4`.

### C2. Collapse the `x if x ==` guard chain into a const-pattern `match`

**Problem.** `decode_op_node` dispatches 28 opcodes as `x if x == OPCODE_FOO =>`
guard arms. The opcodes are `pub const u64` in scope, so they are valid const
patterns; the guard form is verbose and unlike the rest of the crate (`AggFunc`/
`RangeRel`/everything else matches constants directly). All matched values are
distinct (the only duplicate-valued const, `OPCODE_SCAN_TRACE`, is removed in
§B2 and never appeared here anyway).

**Design / edits** (`gnitz-wire/src/circuit.rs`): rewrite the dispatch as
`match opcode { OPCODE_SCAN_DELTA => …, …, _ => return Err(format!("unknown opcode {opcode}")) }`.
Const-pattern matching of `u64` constants compiles cleanly (compile-checked on
rustc 1.96.0) and lets the optimizer pick its own dispatch instead of a 28-deep
guard chain. The conversion is mechanical and
compiler-validated: a const that fell out of scope would become an irrefutable
binding and trigger an `unreachable pattern` error on the following arms, so a
mistake cannot pass silently. Decode is a cold path, so the motivation is
clarity/idiom consistency, not speed.

**Blast radius.** Internal to one function; no signature or behavior change.

**Verification.** `make test` (the existing `decode_*` tests cover every arm).

---

## D. Make illegal anti/semi-join states unrepresentable

**Problem.** `JoinKind` (`circuit.rs:169-175`) has four variants
(`DeltaTrace, DeltaTraceOuter, DeltaDelta, DeltaTraceRange{…}`), but anti/semi
joins only ever use `{DeltaTrace, DeltaDelta}` — there is no outer or range
anti/semi opcode, and the planner never builds one. `OpNode::AntiJoin(JoinKind)`
/ `SemiJoin(JoinKind)` (`circuit.rs:234-235`) therefore admit impossible states
held back only by **six `unreachable!()`** (4 in `gnitz-core` `encode_op_node`,
2 in `gnitz-engine` `emit.rs:542/559`) plus a misleading
`DeltaTrace | DeltaTraceOuter` fold in `emit.rs:535/552` whose `Outer` half is
dead. This is overwidth enforced by convention, not by the type.

**Design.** Add a narrow `SetJoinKind { DeltaTrace, DeltaDelta }` and have
anti/semi carry it; `OpNode::Join` keeps the full `JoinKind` (it alone uses
`DeltaTraceRange`). The wire bytes are unchanged — opcodes 16–19 already encode
exactly this set — so this is a pure in-memory typestate refinement. All six
`unreachable!()` and the dead `Outer` fold delete; the affected matches become
exhaustive with no catch-all.

**Edits** (`gnitz-wire/src/circuit.rs`):

```rust
/// Physical strategy for anti-/semi-join. Narrower than `JoinKind`: the
/// set-difference operators have no outer (null-fill) or range form, so the
/// type cannot name one. Opcodes 16-19 already encode exactly this set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetJoinKind {
    DeltaTrace,
    DeltaDelta,
}

pub enum OpNode {
    // ...
    Join(JoinKind),            // all four, incl. DeltaTraceRange (Join carries it alone)
    AntiJoin(SetJoinKind),
    SemiJoin(SetJoinKind),
    // ...
}

// decode arms — no widening, no extra arms:
x if x == OPCODE_ANTI_JOIN_DELTA_TRACE => OpNode::AntiJoin(SetJoinKind::DeltaTrace),
x if x == OPCODE_ANTI_JOIN_DELTA_DELTA => OpNode::AntiJoin(SetJoinKind::DeltaDelta),
x if x == OPCODE_SEMI_JOIN_DELTA_TRACE => OpNode::SemiJoin(SetJoinKind::DeltaTrace),
x if x == OPCODE_SEMI_JOIN_DELTA_DELTA => OpNode::SemiJoin(SetJoinKind::DeltaDelta),
```

**Blast radius** (`JoinKind` has 66 internal refs; **zero** in `sql`/`capi`/`py`):

- `gnitz-core/src/circuit.rs` `encode_op_node` (`~:196-209`): match anti/semi over
  the two `SetJoinKind` arms; delete the 4 `unreachable!()`. The public builder
  methods (`anti_join`, `semi_join`, …) keep identical signatures (they only ever
  built `DeltaTrace`).
- `gnitz-engine/src/query/compiler/emit.rs` (`~:530-562`): match `SetJoinKind`
  exhaustively in the anti/semi blocks; delete the `DeltaTrace | DeltaTraceOuter`
  fold and the 2 range `unreachable!()`. Update the `AntiJoin(JoinKind::DeltaTrace)`
  pattern at `emit.rs:1113` to `AntiJoin(SetJoinKind::DeltaTrace)`. The
  `AntiJoin(_)`/`SemiJoin(_)` wildcards in `load.rs` keep compiling unchanged.

Keep the load-bearing comment at `circuit.rs:166-168` (it now correctly describes
a `Join`-only `JoinKind`).

**Verification.** `make test`; `make e2e WORKERS=4 K='join'` (anti/semi/except/
intersect coverage). The compiler enforces exhaustiveness at every match site.

---

## E. Leanness and doc hygiene

All edits below are grep-verified zero-external-reference within their cited
scope. Low value individually; bundled because they touch the same files §A–§D
already open.

### E1. Trim dead public surface

`gnitz-wire/src/flags.rs` — demote 7 wire-internal `pub const` to `const` (the
in-module const-guard and the 6 typed accessors are the real public API and keep
compiling; `pub use flags::*` simply stops re-exporting the raw bit math). Keep
all range comments verbatim:

```rust
const SAL_FLAGS_MASK: u64 = 0x0000_FFFF;           // :11
const WIRE_CONFLICT_MODE_SHIFT: u32 = 16;          // :50-51
const WIRE_CONFLICT_MODE_MASK:  u64 = 0xFF_u64 << WIRE_CONFLICT_MODE_SHIFT;
const WIRE_SCHEMA_VERSION_SHIFT: u32 = 24;         // :53-54
const WIRE_SCHEMA_VERSION_MASK:  u64 = 0xFFFF_u64 << WIRE_SCHEMA_VERSION_SHIFT;
const WIRE_INDEX_VERSION_SHIFT: u32 = 40;          // :56-57
const WIRE_INDEX_VERSION_MASK:  u64 = 0xFF_u64 << WIRE_INDEX_VERSION_SHIFT;
```

Also delete `HELLO_STATUS_ERROR` (`handshake.rs:55`): it is never passed — every
`encode_hello_ack` caller passes `HELLO_STATUS_OK`, and the failure path uses a
`STATUS_ERROR` control block, not the ACK. Update the `ack_decode_roundtrip` test
to assert with `HELLO_STATUS_OK` (or a literal `1`).

`gnitz-wire/src/control.rs` — delete `REGION_PK` and `REGION_WEIGHT` (`:128-129`,
zero external refs; both producers hardcode `sizes[0]`/`sizes[1]`, and the
layout is already documented in the comment two lines above). Demote
`col_index` (`:82`) from `pub const fn` to `const fn` (its 10 `COL_*` uses are
in-module; `pub mod control` has no glob re-export, so the demotion is invisible).

**Blast radius.** All zero external refs (verified across all 5 crates).
`WireConflictMode`, the accessors, `REGION_NULL_BMP`/`REGION_BLOB`/payload region
consts, `col_index_in`, and `HELLO_STATUS_OK` are untouched.

### E2. Structural dedup

- `gnitz-engine/src/storage/repr/batch.rs:103`: replace the sole open-coded
  `off = (off + 7) & !7;` with `off = gnitz_wire::align8(off);` (the engine
  already calls `gnitz_wire::*`; this restores greppability with the ~28 other
  `align8` sites).
- `gnitz-wire/src/catalog.rs`: add a `const fn` constructor and collapse the four
  `WireSysCol` tables (30 four-line literal blocks → 30 one-line calls);
  positional order is the wire layout and is preserved. `const fn` is load-bearing
  (`from_wire_cols`, `cidx`, `col_index_in` are const):

```rust
pub const fn col(name: &'static str, type_code: TypeCode, nullable: bool) -> WireSysCol {
    WireSysCol { name, type_code, nullable }
}
pub const CIRCUIT_NODES_COLS: &[WireSysCol] = &[
    col("view_id", TypeCode::U64, false),
    col("sub", TypeCode::U64, false),
    col("node_id", TypeCode::U64, false),
    col("opcode", TypeCode::U64, false),
    col("source_table", TypeCode::U64, true),
    col("expr_program", TypeCode::Blob, true),   // note: reindex_col column dropped
];
// CIRCUIT_EDGES_COLS, CIRCUIT_NODE_COLUMNS_COLS, and control.rs CONTROL_COLS
// (via `use crate::catalog::col;`) follow the same one-line-per-column form.
```

- `gnitz-wire/src/pk.rs`: the two `unreachable!("PK column size must be
  1/2/4/8/16, got {other}")` messages (`:33`, `:70`) are byte-identical; hoist
  the prefix to `const PK_WIDTH_MSG: &str = "PK column size must be 1/2/4/8/16,
  got";` and reference it from both arms. Do **not** merge the two `match dst.len()`
  bodies: they swap in opposite directions (LE→BE encode vs BE→LE decode) and the
  explicit `from_*_bytes`/`to_*_bytes` form is the load-bearing single-`bswap`
  lowering.

**Blast radius.** Field-for-field identical `WireSysCol` values; `align8` is a
behavior-preserving substitution; `pk.rs` change is body-only.

### E3. Doc hygiene

`gnitz-wire/src` doc-comment fixes (no behavior change):

- `control.rs:4`, `lib.rs:4`: `gnitz-protocol` → `gnitz-core` (no `gnitz-protocol`
  crate exists; the client protocol lives in `gnitz-core/src/protocol/`).
- `handshake.rs:31-38`: drop the stale `(>= 248 bytes)` parenthetical and the
  cross-crate `CTRL_BLOCK_SIZE_NO_BLOB` reference (it is `pub(crate)` in
  `gnitz-engine` and unresolvable here; the real value is 264, not 248). Lean on
  the length-prefix rule already stated correctly (`8 ⇒ HELLO, 12 ⇒ ACK, larger
  ⇒ control block`). Do not substitute another byte literal.
- `handshake.rs:197`: rename `hello_payload_size_excludes_legacy_frame_collision`
  → `…_control_block_collision`; reword "legacy framed control block" → "control
  WAL block".
- `catalog.rs:227-228`: append `I128` to the 16-byte-PK enumeration
  (`U128, UUID, I128`) — `I128` is PK-eligible at stride 16.
- `pk.rs:97,250`: relabel "back-compat / identity path" → "no-widening fast path"
  (policy: no "legacy"/"back-compat" framing on a never-released system). Preserve
  the surrounding co-partition and `debug_assert` invariant comments.

**Verification.** `make test` (the renamed test must still pass).

---

## Sequencing and final validation

1. **§A** (independent).
2. **§B1 → §C1 → §C2** in order (all touch `decode_op_node`; §B1 changes its
   signature, §C1 its SCAN arms, §C2 its dispatch form).
3. **§B2** (independent).
4. **§D** (independent of the above; touches different `OpNode` variants).
5. **§E1/§E2/§E3** last (mechanical; §E2's `CIRCUIT_NODES_COLS` rewrite must
   reflect §B1's column removal).

Gate the whole change on `make verify` (`fmt-check` + `clippy`-as-errors +
`test`) followed by `make e2e WORKERS=4` (multi-worker is mandatory — the catalog
schema change in §B1 and the join typestate change in §D exercise exchange/fanout
paths that single-worker mode skips).

## Non-goals (considered and dropped)

- **Macro-generating the type-code listings** — see §A; the existing exhaustive
  matches already force new variants into `types.rs`.
- **Validating `NULL_EXTEND` type codes in `decode_op_node`** — the codes are a
  width selector for a throwaway null-fill schema sourced from already-DDL-validated
  `right_schema` columns, and pass through the same raw-passthrough idiom as the
  `collect_cols` column indices; guarding only `collect_typecodes` would skew the
  boundary, not unify it.
- **Deduplicating the German-string 16-byte layout re-implemented in `gnitz-engine`
  helpers** — that is an engine-side concern (the helpers need in-place,
  zero-copy access the allocating wire codec does not provide), not a `gnitz-wire`
  change.
- **A full `Opcode` enum** with `from_u64`/`as_u64` — the const-pattern `match`
  in §C2 gives the dispatch clarity without threading an enum through the ~30
  `OPCODE_*` consumers in `core`/`engine`.
