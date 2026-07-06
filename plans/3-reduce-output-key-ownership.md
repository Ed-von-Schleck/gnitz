# Reduce output key: planner decides once, engine obeys and validates

## Problem

The natural-vs-synthetic decision for a GROUP BY view's output PK is made
three times, independently, and must agree byte-for-byte or the view's output
columns silently scramble:

1. SQL planner: `group_by.rs:364-399` computes `group_set_eq_pk` /
   `single_col_natural_pk` and lays out the reduce output schema (natural PK
   permutation / single natural group col / synthetic `_group_pk` U128 +
   group cols as payload).
2. Engine compiler: `build_reduce_output_schema`
   (`query/compiler/optimize.rs:397-445`) recomputes `use_natural_pk =
   group_cols_eq_pk || is_single_col_natural_pk` from the same inputs and
   rebuilds the same layout.
3. Engine runtime: `op_reduce` recomputes `group_by_pk =
   input_schema.group_cols_eq_pk(group_by_cols)` (`op_reduce.rs:226`) to pick
   the iteration order and the output-PK stamp (verbatim compound PK bytes /
   `mb.get_pk` / `extract_group_key`, `op_reduce.rs:483-495`), and recomputes
   `use_natural_pk = group_by_pk || is_single_col_natural_pk(..)` a second
   time (`op_reduce.rs:267`) to drive `emit_reduce_row`'s payload layout
   (whether group-exemplar columns are written) — its comment says "matches
   `build_reduce_output_schema`'s logic — must stay in sync".

The predicates are duplicated across crates with mirror-comment contracts:
`Schema::group_cols_eq_pk` + `Schema::is_single_col_natural_pk`
(`gnitz-core/src/protocol/types.rs:166-217`, "Mirrors
`engine::ops::is_single_col_natural_pk`; … planner and engine must agree on
this predicate") vs the engine's `is_single_col_natural_pk`
(`ops/reduce/agg.rs:443-458`) and `SchemaDescriptor::group_cols_eq_pk`. A
divergence today produces no error — just a wrong schema.

## Committed design

The planner ships its (existing, unchanged) decision on the wire as a
discriminant; the engine compiler *obeys* it for schema construction and
*validates* it against the input schema, turning silent divergence into a hard
compile error. `op_reduce` is untouched — its `group_by_pk` recomputation
drives iteration order (argsort choice, `op_reduce.rs:214-259`) and the output
stamp, and its `use_natural_pk` recomputation (`op_reduce.rs:267`) drives
`emit_reduce_row`'s payload layout; once emit-time validation pins
`kind == PkPermutation ⇔ group_cols_eq_pk` and
`kind != SyntheticFold ⇔ use_natural_pk`, both runtime recomputations are
definitionally consistent and survive as validation-pinned inputs.

**Wire.** The `Reduce` circuit node gains one field:

```rust
/// How the reduce output is keyed. Decided once, by the SQL planner (or any
/// circuit author); the engine compiler validates it against the input schema
/// and builds the output schema from it.
///   0 = SyntheticFold  — leading `_group_pk` U128 = null-distinct group fold;
///                        group columns ride as payload.
///   1 = PkPermutation  — group set is a permutation of the source PK; output
///                        PK = source PK columns in pk-list order, verbatim.
///   2 = SingleNaturalCol — one non-nullable U64/U128/UUID group column is the
///                        output PK directly.
pub out_key_kind: u8,
```

carried on the `OpNode::Reduce` variant and wire-encoded as a new
`NODE_COL_KIND_*` param-col row alongside `NODE_COL_KIND_GLOBAL_GROUND`
(`gnitz-core/src/circuit.rs:168-189` serialize arm; decode in
`gnitz-wire/src/circuit.rs::decode_node`, `:400-411`). A global (ungrouped)
aggregate ships `SyntheticFold` (its key is the constant `V₀` fold,
`ops/util.rs::global_group_key`).

**Planner.** `group_by.rs` step 4 already computes the three-way split
(`group_by.rs:364-384`); it passes the corresponding kind through the reduce
builders — the parameter threads through `reduce_node` and all three public
variants (`reduce`, `reduce_multi`, `reduce_multi_local`; the planner's
grouped reduces call `cb.reduce_multi`/`cb.reduce_multi_local`,
`group_by.rs:487-489`). The core-side predicates (`types.rs:171`, `:210`)
stay — they are now the *single* decision site; their "mirrors the engine"
caveats are rewritten to state ownership ("the engine validates, it does not
re-decide").

**Engine compiler.** `build_reduce_output_schema` takes `out_key_kind` and
branches on it instead of computing `use_natural_pk`
(`optimize.rs:407-409` deleted). At the emit site that decodes the Reduce node
(`query/compiler/emit.rs`), validate before building — a failed check rejects
the circuit with a hard error (the same failure class as the existing
hand-assembled-circuit guards, e.g. `ReindexPacker`'s release-mode asserts):

- `PkPermutation` requires `input.group_cols_eq_pk(group_cols)`.
- `SingleNaturalCol` requires `is_single_col_natural_pk(input, group_cols)`.
- `SyntheticFold` requires **neither natural predicate holds** — a planner
  that under-claims would produce a schema whose leading `_group_pk` the
  planner-side layout doesn't expect; reject symmetrically. Edge pinned:
  `group_cols_eq_pk([])` is true only for a 0-PK input, which no relation has
  (every schema carries ≥1 PK column), so every empty-group reduce — the two
  global-aggregate shapes (`group_by.rs:458,483`) and the pure-range threshold
  reduce over the `map_hash_row` leaf (`join.rs:1469-1470`, PK arity 1) —
  validates as `SyntheticFold`.

The engine's `is_single_col_natural_pk` (`agg.rs:443-458`) and
`SchemaDescriptor::group_cols_eq_pk` thus survive **as validators only**; the
lockstep comment on `agg.rs:446-447` ("Shared between
`query::compiler::build_reduce_output_schema` and `op_reduce` to keep schema
construction and execution in lockstep") is replaced by the validation
contract.

**Bytes.** The three kinds map 1:1 onto today's three arms; output schemas,
PK bytes, routing, and trace keys are bit-identical. No `STATE_FORMAT` bump.
The monotone-probe eligibility (`monotone_out_pk`, `op_reduce.rs:233`) keeps
its engine-side derivation via `single_col_canonical_group_key` — it is an
iteration-order/perf property of the *input*, independent of the shipped
output-key kind.

## Not in scope

The planner's mirrored *aggregate column* layout (`_agg` out-types,
`group_by.rs:388-395` vs `optimize.rs:440-443`) stays duplicated; it has no
silent-scramble failure mode of its own once the PK arm is pinned, and folding
it in would grow the wire node for no deletion. The AVI secondary index is not
a wire Reduce node (only `group_by.rs` and `join.rs` build Reduce nodes) and
never passes through `build_reduce_output_schema` — untouched by the
validation.

## Sequencing

- [ ] Wire + core: `out_key_kind` on the Reduce node (codec round-trip test);
      thread the parameter through `reduce_node` and `reduce` /
      `reduce_multi` / `reduce_multi_local`; planner passes its existing
      decision. The C API (`gnitz_circuit_reduce`,
      `gnitz-capi/src/lib.rs:1152-1162`) and Python (`reduce`,
      `gnitz-py/src/lib.rs:1930-1937`) circuit surfaces pass `SyntheticFold`:
      every extant hand-built reduce groups by a signed/I64 column
      (`test_dbsp_ops.py`, `test_tpch.py`, `test_multiworker_ops.py`), which
      resolves to the fold today. This is a deliberate narrowing: a hand-built
      reduce grouping by a full-PK or single natural U64/U128/UUID column —
      which the engine silently keyed naturally before — is now hard-rejected
      unless those surfaces grow a kind parameter; they do not (the SQL
      planner is the only natural-key producer).
- [ ] Engine: thread the kind through the Reduce opcode decode
      (`query/compiler/emit.rs`) into `build_reduce_output_schema`; add the
      three validation checks with hard errors; delete the
      `use_natural_pk` computation (`optimize.rs:407-409`); demote
      `agg.rs::is_single_col_natural_pk` to the validator and rewrite the
      lockstep comments on both sides.

## Testing

- Rust: codec round-trip for all three kinds; compiler tests feeding each
  kind with a *mismatched* input schema assert the hard rejection (all six
  cross pairings); existing reduce unit tests pass unchanged (bytes
  identical).
- `make verify` + `make e2e` (W=4): the aggregate suites
  (`test_aggregates.py`, GROUP BY natural/synthetic/compound-PK shapes,
  MIN/MAX retraction paths) are the byte-stability gate — any schema drift
  fails them loudly.
