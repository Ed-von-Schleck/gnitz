# String values in expressions: UPPER/LOWER, SUBSTRING, TRIM, LENGTH, CONCAT/`||`, string CASE/COALESCE, string CAST

## 1. Problem

The expression engine cannot hold a string value. Registers are 64-bit scalar lanes
(`EvalScratch.regs: Vec<i64>`, `crates/gnitz-engine/src/expr/batch.rs:22-36`); strings exist
in expressions only as the six column-vs-const/column comparison opcodes (40–45), which read
column cells directly and produce booleans. Consequently: no string function can exist, EMIT
writes ≤8-byte values only (`crates/gnitz-engine/src/expr/plan.rs:132`), a string-typed
CASE branch is rejected (`lower.rs` test `case_string_branch_rejected`), `COALESCE(strcol,
'default')` fails, and `||` is an unsupported operator.

This plan makes string values first-class in the expression VM — a **string register
class** backed by zero-copy views plus a per-morsel scratch arena — and ships on top of it:
`UPPER`, `LOWER`, `LENGTH`/`CHAR_LENGTH`/`CHARACTER_LENGTH`, `OCTET_LENGTH`,
`SUBSTRING`/`SUBSTR`, `TRIM`/`LTRIM`/`RTRIM`, `CONCAT(…)` and `||`, string-typed
CASE/COALESCE/NULLIF, string comparisons over computed operands, and CAST between STRING and
the numeric types. All are pure per-row transforms — linear map operators, incrementally
free. Deliberately excluded from this surface: REPLACE, POSITION/STRPOS, LEFT/RIGHT,
LPAD/RPAD, SPLIT_PART, REVERSE, INITCAP, and any BLOB-typed function argument (BLOB keeps
exactly its existing comparison support). LIKE is separate work on top of this
infrastructure and is not in this plan.

### Global semantic rules

1. **Totality: domain errors → NULL** (the fn1/div-by-zero rule): an unparsable
   `CAST('x' AS INT)`, a negative `FOR` length in SUBSTRING. Never an error, never a clamp.
   (PostgreSQL errors on these; the deviation is deliberate — a view tick cannot abort.)
2. **Determinism**: every function is a pure byte-level function of its inputs; retraction
   re-derives byte-identical outputs and cancels under (PK, payload) consolidation.
3. **STRING is byte-transparent in the engine; character semantics are UTF-8 with a lenient
   boundary rule.** STRING values are UTF-8-validated only at the client decode boundary
   (`gnitz-core/src/protocol/wal_block.rs:469-470`); the engine never validates. Character
   positions/counts treat every byte with `(b & 0xC0) != 0x80` (non-continuation) as a
   character start — correct for valid UTF-8, total and panic-free for arbitrary bytes.
4. **Case folding is ASCII-only** (`a-z`/`A-Z`). The engine has no Unicode tables and
   byte-transparent values make locale folding a hazard; `UPPER('straße') = 'STRASSE'` is
   explicitly NOT produced (`'STRAßE'` is). Documented deviation.
5. **STRING-only**: every new function argument must be STRING-typed; BLOB → typed bind
   error. (BLOB column comparisons via opcodes 40-45 are untouched.)
6. **Backend split**: the client residual interpreter rejects every string-valued node
   (`Unsupported`, as it already rejects string columns/literals,
   `crates/gnitz-sql/src/exec/eval.rs:68-73,98-104`) — no divergence is possible because
   only one backend evaluates.

## 2. Function semantics (fully decided)

| Function | Result | Semantics |
|---|---|---|
| `UPPER(s)` / `LOWER(s)` | String | ASCII fold (rule 4); non-ASCII bytes pass through. Arena copy. |
| `LENGTH(s)`, `CHAR_LENGTH(s)`, `CHARACTER_LENGTH(s)` | I64 | Character count per rule 3 (count of non-continuation bytes). |
| `OCTET_LENGTH(s)` | I64 | Byte count. |
| `SUBSTRING(s FROM start [FOR len])`, `SUBSTR(s, start[, len])` | String | PostgreSQL window semantics in character units: the result is the intersection of the half-open character window `[start, start+len)` (1-based; `start` may be ≤ 0; no `len` → to end of string) with `[1, N]`. `len < 0` → NULL (PG errors; rule 1). `start`/`len` are arbitrary integer expressions evaluated per row; the window is computed in **i128** (no `start+len` i64 overflow, rule 1 totality), with each operand widened per its register's U64 tracking (`SUBSTR(s, u64expr)` at ≥ 2⁶³ is a huge positive start → `''`, not a negative). Zero-copy sub-view. `SUBSTRING('abc' FROM 0 FOR 2)` = `'a'`; `SUBSTRING('abc' FROM -1)` = `'abc'`. |
| `TRIM([BOTH\|LEADING\|TRAILING] [chars] FROM s)`, `TRIM(s)`, `LTRIM(s[, chars])`, `RTRIM(s[, chars])` | String | Strip bytes in the trim set from the selected end(s). Trim set: a **string literal** of ASCII characters (default `' '`); a non-literal or non-ASCII set → bind error (ASCII bytes never occur inside UTF-8 multibyte sequences, so byte-wise stripping is character-safe). Zero-copy sub-view. |
| `a \|\| b` | String | SQL-standard concat: **NULL-propagating** (either side NULL → NULL). Both operands must be String-typed (no implicit casts on the operator). |
| `CONCAT(a, b, …)` | String | PostgreSQL `concat()`: **NULL args become empty**; the result is never NULL (all-NULL → `''`) **except** when it would exceed `u32::MAX` bytes — that is NULL (rule 1; the German-string length field is u32, `german_string.rs:19-22`; checked before appending, so `encode_german_string`'s release assert is unreachable from expression output). ≥ 1 arg; the lowering seeds the fold with `load_const_str("")`, making the 1-arg case non-NULL. `STR_CONCAT_NN`'s null rule is **asymmetric** so the overflow-NULL survives the fold: a NULL `a` (accumulator) propagates NULL, only a NULL `b` (argument) is skipped — since each intermediate is a prefix of the final result, intermediate overflow ⟺ final overflow, and the accumulator is otherwise never NULL (the seed isn't), so PG null-as-empty semantics are unaffected. Numeric args are implicitly cast to text via the same INT_TO_STR/FLOAT_TO_STR opcodes CAST uses; String/numeric only. |
| `CASE … THEN 'a' … END`, `COALESCE(s, 'd')`, `NULLIF(s, t)` | String | The existing bind-time desugars (`structural.rs:99-102,275-320`) are unchanged; the string-typed lowering channel (§5.3) makes them compile. COALESCE keeps its existing column-or-literal operand restriction (its null test resolves a column, `structural.rs:291`). |
| `=  <>  <  <=  >  >=` on computed strings | I64 bool | `UPPER(a) = 'X'`, `TRIM(a) < b` — general register-based string compare. Plain col-vs-col/const comparisons keep the existing specialized opcodes 40-45 (the lowerer's `try_compile_string_cmp` interception, `lower.rs:96-180`, stays the fast path). Order: lexicographic byte order with length tiebreak — provably identical to `compare_german_strings`' content order (4-byte BE prefix compare = memcmp of first 4 bytes, then tail memcmp, then length, `crates/gnitz-engine/src/schema.rs:1390-1418`). |
| `CAST(s AS <int type>)` | target | Trim ASCII spaces both sides; optional `+`/`-` sign; ≥ 1 decimal digit and nothing else; accumulate with `checked_mul`/`checked_add` in i128 — an accumulation overflow is NULL, never a wrap (every fixed target is ≤ 20 digits, so oversized values NULL at the range check or the accumulator, identically) — then range-check against the target (`CAST(' 42 ' AS INT)` = 42, `CAST('1.5' AS INT)` → NULL, `CAST('' AS INT)` → NULL, overflow → NULL). `'-0'` → 0 for unsigned targets. |
| `CAST(s AS DOUBLE/FLOAT)` | F64/F32 | UTF-8-check the space-trimmed bytes (`from_utf8(..).ok()` — the engine is byte-transparent, rule 3) then Rust `f64::from_str`; any failure → NULL (accepts `inf`/`NaN` spellings as Rust does — documented). F32 target: parse then `FLOAT_TO_F32` (fn1 opcode). |
| `CAST(n AS TEXT/VARCHAR/CHAR)` | String | Integer: decimal ASCII, signed per the U64 register tracking. Float: shortest round-trip decimal with a magnitude-switched notation — `x == 0.0 \|\| 1e-4 ≤ \|x\| < 1e15` → Rust `{}` positional (`"1.5"`, `"-0"`); otherwise `{:e}` scientific (`"1e300"`, `"5e-324"`) — Rust's positional Display is unbounded (`1e300` renders as 301 digits), so the switch is what keeps output bounded (≤ 32 bytes; the format buffer is 40). NaN → `"NaN"`, ∞ → `"inf"` (PG renders `Infinity` — documented deviation). `STR_TO_FLOAT` parses both notations back. Deterministic byte output for a given binary; pre-alpha, so cross-version stability is not a constraint. |

NULL propagation: NULL in → NULL out for every function except CONCAT (null-as-empty) and
the null-skip cases already defined.

**The LitNull-in-string-context rule (load-bearing, one mechanism everywhere):** lowering is
eager and side-effecting (instructions are appended during the walk), so a string context
must intercept `BoundExpr::LitNull` **before** recursing into it — the same pre-recursion
pattern `try_compile_string_cmp` uses — and emit `load_null_str` instead of the scalar
`load_null`. This applies at every point the string channel consumes an operand: a string
CASE's branch results and else (string-ness decided up front from §5.1's `infer_type` rule,
with `LitNull` branches exempt from the mixed-type check — they are polymorphic), `||`
operands (`s || NULL` is a NULL string → NULL result, not a type error), `CONCAT` args, and
string-function args (`UPPER(NULL)`, `TRIM(NULL)`, `SUBSTR(NULL, 1)` — the `str_call`/
`substr`/`trim_call` backend methods all apply it to their string operand). It deliberately
does **not** extend to comparisons: `UPPER(s) = NULL` stays a typed error, consistent with
`s = NULL` today (SQL users write `IS NULL`). Without this rule, NULLIF-on-strings — whose
desugar is `CASE WHEN a=b THEN NULL ELSE a END` (`structural.rs:316-319`) — would either
type-error or emit a scalar `LoadNull` into a string select.

## 3. Engine architecture: the string register class

### 3.1 StrView + arena (`crates/gnitz-engine/src/expr/batch.rs`)

```rust
/// One string register lane entry: a (buffer, offset, length) view. 16 bytes.
/// `src` selects the buffer. Arena/const offsets are engine-produced; blob
/// offsets come from ingested cells and are bounds-clamped on read (see
/// view_bytes below).
#[derive(Clone, Copy, Default)]
pub(in crate::expr) struct StrView {
    off: u64,
    len: u32,
    src: u32, // 0 = str_arena, 1 = mb.blob, 2 = prog.const_bytes
}
```

A dense str-lane remap (`reg → lane`) was considered and rejected: `str_views` sized to
`num_regs × MORSEL` is capacity-only over-allocation (≤ 256 KiB, pooled, never zeroed per
morsel — the in-order class validation guarantees no stale-lane reads), and the remap would
add an indirection to every string-op inner loop.

`EvalScratch` gains:

```rust
/// String register lanes, register-major like `regs`:
/// str_views[reg * MORSEL + row]. Sized only when the program has string
/// instructions (`has_strings`); zero-len Default views read as "".
pub(in crate::expr) str_views: Vec<StrView>,
/// Scratch arena for computed string bytes (case folds, concats, numeric→text).
/// Cleared (len = 0, capacity kept) at the top of every eval_batch call for a
/// has_strings program — views never outlive the morsel + its emit phase.
pub(in crate::expr) str_arena: Vec<u8>,
```

`ensure_capacity` takes `has_strings` and sizes `str_views` to `num_regs * MORSEL`
(≤ 64·256·16 B = 256 KiB, pooled and reused); the signature change ripples mechanically
into its three production callers (`plan.rs:348,377,505`) and ~18 engine test call sites. View resolution is one helper:

```rust
/// Resolve a view to its bytes. Out-of-bounds (a corrupt long-cell offset in
/// ingested data) degrades to the empty slice — the same corrupt-cell
/// convention as long_string_bytes / relocate_german_string_vec
/// (schema.rs:1300-1305, 1335) — never a panic.
fn view_bytes<'x>(v: StrView, arena: &'x [u8], blob: &'x [u8], consts: &'x [u8]) -> &'x [u8]
```

Sources: **column loads** produce blob views for long cells (offset from cell bytes
`[8..16]`, length from `[0..4]`) and **arena copies** for inline cells (content is
contiguous at `cell[4..4+len]` for `len ≤ 12`, `gnitz-wire/src/german_string.rs:28-33` —
copied to the arena so every view is (buffer, off, len) uniform; ≤ 12 bytes/row, cache-hot).
**Const loads** view into `const_bytes` — a resolve-time concatenation of the raw const-pool
entries with a `const_spans: Vec<(u64, u32)>` table (separate from the existing
`const_cells`/`const_blob` pair, which serves the col-vs-const compare path,
`program.rs:823-828`). **Computed values** append to the arena. Null lanes are the existing
`null_bits`; a NULL string is a zero view + null bit.

Growth of `str_arena` (a `Vec<u8>`) never invalidates views — views store offsets, not
pointers. The emit phase (§3.4) runs inside the same morsel iteration, before the next
`eval_batch` clears the arena.

### 3.2 Register classes

Each register has a static class, `Scalar` or `Str`, determined by its producer
instruction. The client builder is SSA (`alloc_reg` never reuses), but `validate` today does
NOT enforce single-writer on forged blobs — so the validation mechanism is fully specified
as an **in-order walk with per-register state** `Undefined | Scalar | Str`
(`compute_reg_classes(&[LogicalInstr]) -> [RegState; MAX_REGS]`, in `program.rs`), run by
`validate` (`program.rs:860-1001`) and reused by `resolve`:

- A write to an already-defined register is rejected (`ExprValidateErr::RegRewrite`) —
  single-writer becomes an enforced invariant, not a builder convention. This closes the
  forged-blob hazard where a string op reads a register *before* its `Str` writer and
  dereferences stale `str_views` lanes against an already-cleared arena.
- String-consuming operands (`StrCmp`, `StrLen`, `StrCase`, `StrSubstr.src`, `StrTrim`,
  `StrConcat`, `StrToInt`, `StrToFloat`, `StrSelect.a/b`, EMIT into a string column) must
  read a register that is `Str` **at that point of the walk**; `Undefined` or `Scalar` →
  `ExprValidateErr::RegClassMismatch { reg }`.
- Scalar operands (`StrSubstr.start/len`, `StrSelect.cond`, everything pre-existing) must
  not read a `Str` register; a filter's `result_reg` must be `Scalar`.
- **SSA anti-aliasing extends to the new multi-operand ops**: `StrSelect` joins the ternary
  `Select` arm (`dst ≠ cond, a, b`), and `StrConcat`/`StrCmp`/`StrSubstr` join the binary
  `RegisterAliasing` arm (`dst ≠` every register operand) — matching the split-borrow
  helpers' stated safety contract (`program.rs:891-921`, `batch.rs:68-96`). (Under
  `RegRewrite` + the class checks these arms are strictly redundant — `dst == operand`
  implies a rewrite or a read-before-def — but they keep the per-opcode contract uniform
  and locally checkable; deliberate belt-and-suspenders.)
- The schema-aware pass additionally requires: `LoadColStr`'s column is German-string typed
  (a new `ExprValidateErr::StrColNotString`, **also added to the existing
  `StrColConst`/`StrColCol` arms** — today a forged blob can point them at a non-string
  column and misread the region at 16-byte stride); and EMIT class/column agreement — a
  `Str` source requires a German-string output column and vice versa (a scalar EMIT into a
  16-stride column would panic slicing `to_le_bytes()[..16]`, `plan.rs:542`). New
  `ExprValidateErr::EmitClassMismatch`. A filter program (validated with
  `out_schema = None`, `program.rs:856-859`) cannot run the EMIT column check — accepted:
  EMIT is an eval no-op (`batch.rs:532`) and `from_predicate` builds no emit list, so a
  forged EMIT in a filter stays inert exactly as today.

`resolve` stores the class table on `ResolvedProgram` (`reg_classes`) — `from_map` routes
EMITs by it (§3.4) — plus `has_strings: bool` and `const_bytes`/`const_spans`.

### 3.3 Opcodes and eval arms

Engine variants (LogicalInstr + Instr, following the `CmpOp`/`StrOp` collapse pattern):

```rust
LoadColStr   { dst, col→pi },                 // view per row; nulls from bitmap
LoadConstStr { dst, const_idx },              // same const view all rows
LoadNullStr  { dst },                         // zero view + all-null (LoadNull twin)
StrSelect    { dst, cond, a, b },             // CASE blend over views (Select twin)
StrCmp       { op: StrOp, dst, a, b },        // Eq/Lt/Le → bool; byte-lex + len tiebreak
StrLen       { dst, a, chars: bool },         // → int reg
StrCase      { dst, a, upper: bool },         // ASCII fold → arena
StrSubstr    { dst, src, start_reg, len_reg: Option<u16> },  // char window → sub-view
StrTrim      { dst, a, mode: TrimMode, set_idx },            // byte-set strip → sub-view
StrConcat    { dst, a, b, skip_null: bool },  // arena append; ‖ vs CONCAT null rule
IntToStr     { dst, a },                      // Instr adds signed: bool (reg_tc)
FloatToStr   { dst, a },
StrToInt     { dst, a, tc },                  // → int reg; parse+range or NULL
StrToFloat   { dst, a },                      // → float reg; parse or NULL
```

Eval-arm shapes (all per-morsel loops over `str_views`, `null_bits` handling as noted;
every arm ends with `maybe_pack_bool_bits` only where it writes a scalar/bool dst):

- `LoadColStr`: read `mb.col_data(pi, 16)` cells; inline → arena copy, long → blob view
  **with the corrupt-cell clamp applied here** (an out-of-bounds `off + len` loads as the
  empty view, len 0 — so LENGTH, compares, transforms, and EMIT all agree on the degraded
  value, matching `relocate_german_string_vec`'s convention rather than diverging per
  consumer); `fill_null_bits_mask` for the column (like `LoadPayloadInt`).
- `StrSelect`: exactly `Select`'s two arms (`batch.rs:901-944`) with the value blend over
  `str_views` lanes instead of `regs`.
- `StrCmp`: `view_bytes(a).cmp(view_bytes(b))` (slice `Ord` = byte-lexicographic,
  shorter-prefix-less — the compare-order equivalence in §2); writes 0/1 into `regs[dst]`;
  nulls via `null_or2` + `zero_null_rows` (the `eval_str_cmp` pattern, `batch.rs:327-350`).
- `StrLen`: `len` field (bytes) or non-continuation count (chars) → `regs[dst]`;
  `null_copy1`.
- `StrCase`: append folded bytes to arena (`b ^ 0x20` on the matching ASCII range,
  branch-free), record arena view; `null_copy1`.
- `StrSubstr`: read `start` (and `len` unless absent) from scalar regs, each widened to
  i128 per its `signed` flag (`Instr::StrSubstr` carries `start_signed`/`len_signed` from
  the resolve-time `reg_tc` tracking, like fn1's `IntCast.src_signed`); compute the window
  `[start, start+len)` in i128 (overflow-free) and clamp; scan the source view's bytes
  counting character starts to the window's byte bounds; result = sub-view of the source
  view (same `src`, adjusted `off`/`len` — zero-copy). `len < 0` row → NULL (fail-mask
  merge like fn1's `unary_null_like!`); else `null_or2` over src/start/len nulls.
- `StrTrim`: byte-set membership strip from the selected end(s); sub-view; `null_copy1`.
  The trim set is decoded once at resolve into a 256-bit table (`[u64; 4]`) per `set_idx`.
- `StrConcat`: append `a`'s then `b`'s bytes to the arena, arena view; a combined length
  above `u32::MAX` → NULL row (fail mask, §2). Nulls: `skip_null` (CONCAT) is
  **asymmetric** per §2 — `dst_null = a_null | overflow`, a NULL `b` contributes empty
  bytes; plain (`||`) is `null_or2 | overflow`.
- `IntToStr`/`FloatToStr`: format into a 40-byte stack buffer (i64/u64 decimal ≤ 20 chars;
  f64 via the §2 magnitude-switched `{}`/`{:e}` rule ≤ 32 chars — never the unbounded
  positional form), append to arena; `null_copy1`.
- `StrToInt`/`StrToFloat`: parse per §2; fail-mask merge (null-producing, like `IntCast`).
  `StrToInt` sets `reg_tc[dst] = U64` for a U64 target (else 0) at resolve, exactly like
  fn1's `IntCast` — without it a `CAST('…' AS BIGINT UNSIGNED)` register would be
  signed-tainted and every downstream ordered compare / `IntToStr` / `StrSubstr` widening
  would mis-dispatch. `StrToFloat` sets 0.

### 3.4 EMIT of string registers (`plan.rs`)

The wire `EXPR_EMIT` opcode is reused — `from_map` (`plan.rs:284-325`) routes each emit by
the source register's class (`ResolvedProgram.reg_classes`): `Scalar` → today's `EmitCol`,
`Str` → a new `EmitStrCol { payload, reg }` list. In the per-morsel emit phase
(`plan.rs:517-575`), string emits run alongside scalar emits: per row, NULL → zero 16-byte
cell + null-bitmap bit; else `encode_german_string(view_bytes(..), &mut output.blob)`
(`schema.rs` re-export, `gnitz-wire/src/german_string.rs:9-40`) and write the returned cell
into `output.col_data_mut(payload)`.

**Blob interplay:** `compute_blob_passthrough` (`plan.rs:137-154`) is unchanged in logic
(its comment "a map never computes a string" is updated). When passthrough is chosen,
`share_blob_from` copies the input blob bytes and adopts its `blob_id`
(`storage/repr/batch.rs:1350-1358`); string emits then append to `output.blob` — existing
cell offsets stay valid (append-only) — and `evaluate_map_batch` re-mints the blob id whenever any `EmitStrCol` exists (the content
now differs from the source blob, so the shared identity token must not be kept; precedent:
`batch.rs:1344`). `next_blob_id` is module-private (`storage/repr/batch.rs:15`), so
`Batch` gains a one-line `pub(crate) fn remint_blob_id(&mut self)`. Downstream no-reloc
appends simply fall back to the relocate path — correct, no dangling offsets.

`evaluate_map_batch`'s output batch needs no other change: STRING computed columns get
their 16-byte region and blob from the client-shipped output schema (the engine's map
schema is `loaded.out_schema`, `query/compiler/emit.rs:426`), and `Batch::with_schema`
already builds string regions.

### 3.5 Analyses

- `each_reg_read`/`classify_registers`: string-consuming ops read their operands (value
  reads; none are bool inputs except `StrSelect.cond`, which mirrors `Select.cond` —
  `bool_input`); `StrCmp` and nothing else among the new ops is a bool *producer*.
  String-producing ops are in neither bool set (their dst is a `Str` register; `bit_only`
  masks apply to scalar lanes only — `compute_reg_classes` keeps them disjoint).
- `is_strictly_non_nullable`: `StrToInt`/`StrToFloat`/`StrSubstr` (negative-len rule) and
  `LoadNullStr` → `return false`; `LoadColStr` of a nullable column → `false` (join the
  `LoadPayloadInt` arm); pure transforms (`StrCase`/`StrTrim`/`StrLen`/`StrConcat`/
  `StrCmp`/`StrSelect`/`LoadConstStr`) propagate only → safe remainder. A fully non-null
  string program keeps the `no_nulls` fast path.
- `and_chain_mask`: unchanged (spine links are `BoolAnd`-written).

## 4. Wire (`crates/gnitz-wire/src/expr.rs`)

Nineteen new opcode constants, continuing after fn1's 56:

```rust
pub const EXPR_LOAD_COL_STR: u32 = 57;    // [op, dst, col, 0]
pub const EXPR_LOAD_CONST_STR: u32 = 58;  // [op, dst, const_idx, 0]
pub const EXPR_LOAD_NULL_STR: u32 = 59;   // [op, dst, 0, 0]
/// String CASE blend; operand packing identical to EXPR_SELECT
/// ([op, dst, cond, a|b<<16], encode/decode_select_operands).
pub const EXPR_STR_SELECT: u32 = 60;
pub const EXPR_STR_CMP_EQ: u32 = 61;      // [op, dst, a, b] → 0/1
pub const EXPR_STR_CMP_LT: u32 = 62;
pub const EXPR_STR_CMP_LE: u32 = 63;
pub const EXPR_STR_LEN_BYTES: u32 = 64;   // [op, dst, a, 0] → i64
pub const EXPR_STR_LEN_CHARS: u32 = 65;
pub const EXPR_STR_UPPER: u32 = 66;       // [op, dst, a, 0]
pub const EXPR_STR_LOWER: u32 = 67;
/// [op, dst, src_reg, start_reg | len_reg<<16]; len_reg = 0xFFFF ⇒ no FOR
/// clause (to end of string). 0xFFFF is unreachable as a real register
/// (MAX_REGS = 64).
pub const EXPR_STR_SUBSTR: u32 = 68;
/// [op, dst, src_reg | mode<<16, set_const_idx]; mode 0=BOTH 1=LEADING 2=TRAILING.
pub const EXPR_STR_TRIM: u32 = 69;
pub const EXPR_STR_CONCAT: u32 = 70;      // [op, dst, a, b]  NULL-propagating (‖)
/// CONCAT fold step: NULL b = ''; NULL a (the accumulator) propagates — the
/// asymmetry that carries the u32::MAX overflow-NULL through the fold.
pub const EXPR_STR_CONCAT_NN: u32 = 71;
pub const EXPR_INT_TO_STR: u32 = 72;      // [op, dst, a, 0]
pub const EXPR_FLOAT_TO_STR: u32 = 73;
pub const EXPR_STR_TO_INT: u32 = 74;      // [op, dst, a, target_tc]
pub const EXPR_STR_TO_FLOAT: u32 = 75;
```

(`EXPR_EMIT` is reused for string emits — §3.4. `from_wire` validates `mode ≤ 2` and the
`STR_TO_INT` target via fn1's `BadCastTarget` rule; const indices bound-checked as for
`StrColConst`.)

## 5. Client side (gnitz-sql, gnitz-core)

### 5.1 IR (`crates/gnitz-sql/src/ir.rs`)

```rust
/// Unary string transforms/measures.
enum StrFunc { Upper, Lower, LenBytes, LenChars }   // Len* produce I64

StrCall  { f: StrFunc, arg: Box<BoundExpr> },
Substr   { s: Box<BoundExpr>, start: Box<BoundExpr>, len: Option<Box<BoundExpr>> },
TrimCall { s: Box<BoundExpr>, mode: TrimMode, set: String },  // set: ASCII, bind-checked
/// CONCAT(args…): PG null-as-empty semantics; numeric args implicitly cast.
ConcatN  { args: Vec<BoundExpr> },
```

`BinOp` gains `Concat` (`||`, mapped from `BinaryOperator::StringConcat` in `map_binop`,
`structural.rs:223-244`).

`infer_type`: `StrCall{Upper|Lower}`/`Substr`/`TrimCall`/`ConcatN`/`BinOp::Concat` →
`TypeCode::String`; `StrCall{Len*}` → I64; and the **`Case` arm is fixed for strings** —
today it folds `unify_numeric`, which would silently map a string CASE to I64; new rule:
if any branch/else infers a German-string type, the CASE is String (mixed string/numeric
branches then fail in lowering with a typed error). `Cast { to }` already returns `to`
(fn1), covering `CAST(… AS TEXT)`.

### 5.2 Binding (`crates/gnitz-sql/src/bind/structural.rs`)

Structural interceptions alongside fn1's:

- Name arms: `"upper"`, `"lower"`, `"length"`, `"char_length"`, `"character_length"`,
  `"octet_length"` (1 arg → `StrCall`); `"ltrim"`/`"rtrim"` (1 or 2 args → `TrimCall` with
  mode Leading/Trailing; 2nd arg must be a string literal); `"concat"` (≥ 1 arg →
  `ConcatN`). All parse as plain `Expr::Function` (none is keyword-dispatched in
  sqlparser 0.62).
- `Expr::Substring { expr, substring_from, substring_for, .. }` (keyword-dispatched,
  sqlparser `parser/mod.rs:2918-2977`): `substring_from` defaulting to `LitInt(1)` when
  absent; both forms (`FROM/FOR` and comma) arrive in the same node → `Substr`.
- `Expr::Trim { expr, trim_where, trim_what, trim_characters }` (sqlparser
  `parser/mod.rs:2979-3012`): `trim_what` must be a string literal, ASCII-only → `set`
  (default `" "`); `trim_characters: Some(_)` → `Unsupported`; `trim_where` maps to
  `TrimMode` (default Both) → `TrimCall`.
- The three new AST nodes join `expr_operands` (`ast_util.rs:135-176`):
  `Expr::Substring` contributes `expr` + the two optional bounds, `Expr::Trim` contributes
  `expr` (+ `trim_what`), same walker invariant as fn1.

### 5.3 Lowering: the typed channel (`crates/gnitz-sql/src/lower.rs`)

`OpcodeBackend`'s output type widens from `(u32, bool)` to `(u32, ExprKind)` with
`enum ExprKind { Int, Float, Str }` — a mechanical rename of the existing `is_float` bool
through every arm (`Int`/`Float` behave exactly as `false`/`true` did). `InterpBackend`'s
`Out` is unchanged. The kind is how string values thread through the one shared walk:

- `lit_str` in the opcode backend stops erroring: it emits `add_const_string` +
  `load_const_str` → `(reg, Str)`. (The interpreter's `lit_str` rejection stands, rule 6.)
- `col_ref` on a STRING column emits `load_col_str` → `(reg, Str)` instead of erroring;
  BLOB columns keep the existing rejection text (rule 5). The wide-int rejection stands.
- `binop`: `try_compile_string_cmp` stays first as the fast path, with one change: it
  **declines** (returns `Ok(None)`) for any non-comparison operator instead of erroring —
  today its `_ =>` arms error (`lower.rs:148-152, 170-174`), which would kill
  `strcol || 'lit'` before the string channel ever ran. Declined operators fall through: if
  either operand is String-typed, comparisons require both `Str` (else typed error) and
  emit `str_cmp_*` (+ `bool_not`/operand swap for NE/GT/GE, exactly the 40-45 pattern);
  `BinOp::Concat` requires both operands `Str` **or `LitNull`** (per the §2 rule) and emits
  `str_concat`; every other operator on a `Str` operand is a typed error (preserving
  today's `s + 1` rejection, now raised in the channel rather than the interception).
- `case`: string-ness is decided **before lowering any result**, from §5.1's `infer_type`
  rule (any branch/else infers German-string → string CASE). In a string CASE: conds are
  scalar as today; each result lowers through the string channel with `LitNull` results
  intercepted to `load_null_str` (§2 rule); results fold right-to-left with `str_select`;
  an implicit ELSE is `load_null_str`. A non-LitNull branch that lowers to a scalar kind in
  a string CASE (or a `Str` result in a numeric CASE) → typed error. (This makes
  `COALESCE(strcol, 'd')`, `NULLIF(a, b)` on strings, and `CASE … THEN 'x' ELSE NULL END`
  compile through the existing desugars with zero binder changes — NULLIF's `a = b`
  condition compiles through the string-compare path.)
- `func`/`cast` (fn1) extend: `Cast` with String source/target routes to
  `str_to_int`/`str_to_float`/`int_to_str`/`float_to_str` (+ `float_to_f32` for F32);
  String→String is identity; fn1's numeric matrix is untouched.
- New backend methods `str_call`, `substr`, `trim_call`, `concat_n` mirror the IR nodes;
  `concat_n` seeds the fold with `load_const_str("")` (uniform n ≥ 1 handling; makes the
  1-arg case non-NULL per §2), lowers each arg (LitNull → `load_null_str`, auto-wraps
  `Int`/`Float` args with `int_to_str`/`float_to_str`), and folds left with
  `str_concat_nn`. `substr` lowers `start`/`len` as scalar ints (float `start`/`len` →
  typed error).
- `ExprBuilder` (`gnitz-core/src/expr.rs`) gains one emitter per §4 opcode (19), all over
  the existing `unary_op`/`binary_op`/`select`-style helpers; `str_substr` packs
  `start|len<<16` (`encode_select_operands` reuse), `str_trim` packs `src|mode<<16`.

`InterpBackend`: `str_call`/`substr`/`trim_call`/`concat_n` → `Unsupported`; `binop` adds a
`BinOp::Concat → Unsupported` arm; `cast` with a String side → `Unsupported` (rule 6).

### 5.4 Projection/schema plumbing — no changes needed

A computed String output column flows through the existing Computed arm
(`codec/project_schema.rs:72-76`: type from `infer_type`, nullable) and the engine builds
its region + blob from the shipped schema (§3.4). Aggregates over strings stay rejected
(`is_min_max_orderable`), GROUP BY on expressions stays unsupported, and computed string
columns of a view are ordinary STRING columns to every downstream consumer (consolidation
via `compare_rows`' German-string arm, relocation on merge — all pre-existing paths).

## 6. Edit surface

| File | Change |
|---|---|
| `crates/gnitz-wire/src/expr.rs` | 19 opcode constants (57-75) |
| `crates/gnitz-core/src/expr.rs` | 19 `ExprBuilder` emitters |
| `crates/gnitz-sql/src/ir.rs` | `StrCall`/`Substr`/`TrimCall`/`ConcatN` variants, `StrFunc`, `TrimMode`, `BinOp::Concat`; `infer_type` arms + string-aware `Case` rule |
| `crates/gnitz-sql/src/bind/structural.rs` | name arms upper/lower/length/char_length/character_length/octet_length/ltrim/rtrim/concat; `Expr::Substring`/`Expr::Trim` arms; `map_binop` StringConcat |
| `crates/gnitz-sql/src/ast_util.rs` | `expr_operands` arms for `Expr::Substring`/`Expr::Trim` |
| `crates/gnitz-sql/src/lower.rs` | `ExprKind` (replaces the `is_float` bool in `Out`); string channel in `binop`/`case`/`cast`; backend methods `str_call`/`substr`/`trim_call`/`concat_n`; `col_ref`/`lit_str` string arms; two inverted tests rewritten (`case_string_branch_rejected` → string CASE compiles; `string_cmp_unsupported_names_op` → the interception declines, the error moves to the channel) |
| `crates/gnitz-sql/src/exec/eval.rs` | `Unsupported` arms for the four new methods + `BinOp::Concat` + String casts |
| `crates/gnitz-engine/src/expr/program.rs` | 14 `LogicalInstr`/`Instr` variants; `from_wire`; `compute_reg_classes` + class validation (`RegClassMismatch`/`RegRewrite`/`StrColNotString`/`EmitClassMismatch`, incl. retrofit on `StrColConst`/`StrColCol` and EMIT); `resolve` (`const_bytes`/`const_spans`, trim-set tables, `reg_classes`, `has_strings`, `IntToStr.signed`, `StrToInt`/`StrSubstr` reg_tc/signed flags); analyses arms |
| `crates/gnitz-engine/src/expr/batch.rs` | `StrView`, `str_views`/`str_arena` on `EvalScratch`, `view_bytes`, 14 eval arms |
| `crates/gnitz-engine/src/expr/plan.rs` | `EmitStrCol` routing by reg class; string emit phase; blob-id re-mint; `compute_blob_passthrough` comment |
| `crates/gnitz-engine/src/storage/repr/batch.rs` | `Batch::remint_blob_id()` (one line over the private `next_blob_id`) |

## 7. Performance commitments

- **Zero-copy where possible**: SUBSTRING and TRIM are pure view arithmetic; column loads
  of long strings never touch the heap bytes; only value-changing ops (case fold, concat,
  numeric↔text) write the arena.
- **No per-row allocation**: the arena is one `Vec<u8>` cleared by `set_len(0)` per morsel
  with retained capacity; formatting uses stack buffers.
- **Resolve-time preparation**: trim-set bitmaps, const-span table, and register classes
  are computed once per program, never per row or morsel.
- **The hot filter path is untouched**: plain `s = 'x'` / `s < t` still lower to opcodes
  40-45 with pre-encoded const cells; the register channel only pays for expressions that
  need it. The `no_nulls` fast path survives non-null string programs.
- Arena footprint is morsel-bounded: 256 rows × (inline copies ≤ 12 B + computed output
  sizes); a CONCAT of two 1 MiB strings costs 256 × 2 MiB peak arena for that morsel —
  proportional to the data actually produced, reclaimed by the next morsel's clear.
- Register pressure: string expressions live under the same 64-register file as everything
  else — a string CASE of ~10 branches or a CONCAT of ~20 args reaches the cap and CREATE
  VIEW fails with the engine's `TooManyRegs` rejection (fn1's documented bound; string
  IN-lists cost ~2 regs/item via the OR-chain, capping around 30 items — pre-existing,
  untouched here).

## 8. Tests

**Engine unit (`crates/gnitz-engine/src/expr/tests/program_tests.rs`):**
- Per-opcode eval incl. NULL rows and empty strings: case folds (ASCII boundary bytes
  `` `a-1` ``/`z+1`, multibyte UTF-8 pass-through), LEN bytes vs chars on multibyte +
  invalid-UTF-8 bytes (lenient rule), SUBSTR windows (start ≤ 0, beyond end, `len < 0` →
  NULL, no-FOR sentinel, multibyte boundaries, sub-view of a computed string), TRIM all
  three modes + custom set + everything-trimmed → `''`, CONCAT both null rules, numeric↔text
  round-trips (i64::MIN, u64::MAX via signed flag, f64 shortest repr, parse failures →
  NULL, `' 42 '`), inline↔long cell boundary (11/12/13-byte strings).
- StrCmp differential test: register-channel compare ≡ `compare_german_strings` over a
  corpus crossing inline/long cells, shared prefixes, length ties.
- Class validation: string op on scalar reg / scalar op on string reg / read-before-def /
  double-write (`RegRewrite`) / dst-operand aliasing on `StrConcat`/`StrSelect`/`StrCmp`/
  `StrSubstr` / EMIT class-column mismatch / `LoadColStr` on an int column / `StrColConst`
  on an int column (retrofit) / bad trim mode / bad `STR_TO_INT` target — each rejected
  with its typed error.
- Overflow/edge totality: `STR_TO_INT` at 39+ digits (checked i128) → NULL, and a U64
  target sets the register's unsigned class (downstream compare pinned); `FloatToStr` of
  `1e300`/`5e-324` uses scientific notation (bounded ≤ 32 bytes) and round-trips through
  `STR_TO_FLOAT`; `SUBSTR` with `start`/`len` near `i64::MAX` and a U64-classed `start`
  (i128 window, no panic); `StrToFloat` on invalid UTF-8 → NULL. The `u32::MAX` concat cap
  is covered by the asymmetric-null unit semantics (a forced-NULL accumulator propagates
  through `STR_CONCAT_NN`), not by allocating multi-GiB strings.
- A map emitting UPPER of a long string: output cell + blob round-trip; blob-passthrough
  map with a string emit gets a fresh `blob_id` and correct downstream append behavior.
- `no_nulls` stays true for non-null string transforms; flips for `StrToInt`.

**SQL unit:**
- Lowering: `UPPER(s) = 'X'` uses `str_cmp_eq` (not 40-45); `s = 'x'` still uses opcode 40
  (fast-path regression); `s || 'lit'` and `s || t` compile through the string channel (the
  interception now declines non-comparison ops); `s || NULL` → NULL string, not an error;
  `||` null-propagating vs `CONCAT` null-as-empty programs; `CONCAT(s)` single-arg is
  non-NULL; `CONCAT(s, 42)` auto-casts; string CASE/COALESCE/NULLIF compile —
  `CASE … THEN 'x' ELSE NULL END` and string NULLIF (LitNull branch → `load_null_str`)
  pinned; mixed non-NULL string/int CASE branches → typed error; `s + 1` and `s || 1` →
  typed errors; BLOB arg → typed error; `LOWER(b)` on BLOB rejected; infer_type: string
  CASE is String (the §5.1 fix pinned).
- Binder: all name arms; TRIM full syntax matrix; non-literal/non-ASCII trim set rejected;
  `SUBSTRING(s)` (no FROM) → start=1; `expr_operands` reaches `EXCLUDED` under
  SUBSTRING/TRIM (walker regression).
- Interp: every string node `Unsupported` through `eval_expr`.

**E2E (`crates/gnitz-py/tests/test_string_functions.py`, `GNITZ_WORKERS=4`):**
- Views projecting each function; insert/update/delete cycles prove byte-exact retraction
  cancellation for computed string columns (incl. long strings crossing the 12-byte
  inline threshold and a Unicode corpus).
- Filters: `WHERE UPPER(name) = '…'`, `WHERE LENGTH(s) > k`, `WHERE TRIM(s) = ''`.
- `CONCAT`/`||` in projections with NULLs; `COALESCE(s, 'default')`; string CASE.
- CAST both directions incl. failure→NULL rows; a view chaining string ops
  (`UPPER(SUBSTRING(TRIM(s), 1, 3))`).
- Views-over-views reading a computed string column; ad-hoc SELECT with string expressions.

## 9. Sequencing

- [ ] Commit 1: wire constants + `ExprBuilder` emitters + engine infrastructure (StrView,
      arena, classes, `LoadColStr`/`LoadConstStr`/`LoadNullStr`/`StrSelect`/`StrCmp`,
      validation retrofit incl. `StrColNotString`/`EmitClassMismatch`, EMIT routing +
      string emit phase + blob-id re-mint) + engine tests.
- [ ] Commit 2: the function opcodes (`StrLen`/`StrCase`/`StrSubstr`/`StrTrim`/
      `StrConcat`/numeric↔text) + engine tests.
- [ ] Commit 3: SQL side (IR, binder, `ExprKind` channel, interp rejections) + SQL unit
      tests + E2E suite.
