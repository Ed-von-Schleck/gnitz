# 2. Scalar function library

**Status:** research handover, not a plan. Self-contained — no other file is needed.

## Provenance of the evidence below

- **Tree:** `main` at `fb013479`, with an uncommitted in-flight refactor across
  `crates/gnitz-engine/src/query/compiler/*` and `crates/gnitz-engine/src/catalog/write_path.rs`.
  Code is cited by **file and symbol only, never by line number** — symbols were verified to
  exist at the time of writing; line numbers would already be stale (they moved under that
  refactor during this session).
- **How the SQL results were obtained:** temporary integration tests in throwaway
  `git worktree`s (since removed; the working tree was never modified), calling
  `SqlPlanner::execute` on one statement at a time and printing accept/reject plus the error
  text. Server: `GNITZ_SERVER_BIN=<repo-root>/gnitz-server`, the prebuilt binary at the repo
  root dated 2026-08-31 13:35 — **not verified to have been built from `fb013479`**. The Rust
  test harness has no staleness check (that check lives in the Python E2E suite).
- **Workers: 1.** `ServerHandle::start()` is `start_n(1)`. Nothing here was exercised at W>1,
  so no claim below covers exchange, fanout or partitioning behaviour.
- **"OK" below means the planner and the engine's circuit compiler accepted the statement. It
  does NOT mean the resulting view produces correct rows.** No probe inserted a row, read a
  result, or compared weights against an oracle. Before building on any statement shown as
  compiling, validate its semantics — in a Z-set engine that means checking weights, not row
  presence. Any cost or complexity claim not backed by a quoted measurement is analytic
  reasoning from the code, and is labelled as such where it appears.

## Fixtures the quoted statements ran against

```sql
CREATE TABLE t  (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL,
                 b BIGINT NOT NULL, f DOUBLE NOT NULL, s TEXT NOT NULL, sn2 TEXT);
```

Not every column appears in every quoted statement. `sn2` is the only nullable TEXT column.


Batched work: each function is individually unreachable by any workaround, and they share
one binder table and a small number of kernel shapes.

## Verified rejections

Each expression below was probed as `CREATE VIEW fnN AS SELECT id, <expr> AS r FROM t`; only
the expression and the resulting error are shown. `unknown_function` lowercases the name it
reports, so the message spells `'replace'`, not `'REPLACE'`. The two `expression type not
supported` messages dump the raw `sqlparser` AST; it is elided here with `{ ... }`.

```
REPLACE(s, 'a', 'b')       -> unsupported: function 'replace' not supported
STRPOS(s, 'a')             -> unsupported: function 'strpos' not supported
LPAD(s, 5, ' ')            -> unsupported: function 'lpad' not supported
RPAD(s, 5, ' ')            -> unsupported: function 'rpad' not supported
SPLIT_PART(s, ',', 1)      -> unsupported: function 'split_part' not supported
REVERSE(s)                 -> unsupported: function 'reverse' not supported
LEFT(s, 2) / RIGHT(s, 2)   -> unsupported: function 'left' / 'right' not supported
REGEXP_REPLACE(s,'a','b')  -> unsupported: function 'regexp_replace' not supported
POWER(a, 2)                -> unsupported: function 'power' not supported
SQRT(f) / LN(f) / LOG(f) / EXP(f) / SIGN(a) / MD5(s)   -> function '<name>' not supported
IFNULL(sn2, s) / NVL(sn2, s) / IF(a > 1, 1, 0)         -> function '<name>' not supported
POSITION('a' IN s)         -> unsupported: expression type not supported: Position { ... }
a IS DISTINCT FROM b       -> unsupported: expression type not supported: IsDistinctFrom(...)
s LIKE CONCAT('a', '%')    -> unsupported: LIKE pattern must be a string literal
```

The last one is a separate sub-item: a non-literal LIKE pattern is rejected on **both** the
view path and the ad-hoc path.

## Verified as already working (do not re-derive)

`ABS, CEIL, FLOOR, CEILING, TRUNC, ROUND(x), ROUND(x,n), MOD, GREATEST, LEAST, UPPER,
LOWER, LENGTH, CHAR_LENGTH, OCTET_LENGTH, LTRIM, RTRIM, TRIM, TRIM(BOTH 'x' FROM s),
SUBSTRING(s FROM n FOR m), SUBSTR(s,n,m), CONCAT, ||, COALESCE, NULLIF, CASE, CAST (incl.
a::TEXT and CAST(s AS BIGINT)), + - * / %, unary -, comparisons, NOT, BETWEEN, IN (list),
LIKE, ILIKE, NOT LIKE, IS NULL, IS NOT NULL` — each verified compiling in a view body, in
the same `CREATE VIEW fnN AS SELECT id, <expr> AS r FROM t` shape.

## Code read

- `gnitz-sql/src/bind/structural.rs` — `SCALAR_CALLS`, the one name→call table; the
  `Call` enum above it. `scalar_call` matches case-insensitively; anything unmatched falls
  through to the leaf binder and then `unknown_function` (`gnitz-sql/src/ast_util.rs`).
- `gnitz-sql/src/ir.rs` — `NumFunc` (Abs, Floor, Ceil, Trunc, Round(i8)) and `StrFunc`
  (Upper, Lower, LenBytes, LenChars); `BExpr` carries the structural nodes.
- `gnitz-expr/src/program.rs` — `FloatUnaryOp { Neg, Abs, Floor, Ceil, Round, Trunc }`,
  `IntUnaryOp { Neg, Abs }`, `StrOp { Eq, Lt, Le }`.
- `gnitz-expr/src/program.rs` — `LogicalInstr`, 38 variants across two register
  classes (scalar and string), with a const pool (`ConstIdx`).
- `gnitz-expr/src/batch.rs` — the morsel kernels. `SRC_ARENA` is the string arena, described
  in-file as "the one buffer a kernel may be growing as it resolves a view".
  String-producing evaluation already exists in two forms: standalone functions
  `eval_str_substr` and `eval_str_concat` in `gnitz-expr/src/batch.rs` — `eval_str_concat` was
  read and does write `str_arena` — and inline arms in that file's main dispatch for
  `Instr::StrCase`, `Instr::StrTrim` and `Instr::IntToStr`. Which form a new
  function should take is unknown.

## Shape of the work, by function class

- **Float-domain unaries** (`SQRT`, `LN`, `LOG`, `EXP`): float in, float out, which is the
  shape `FloatUnaryOp` already has, so plausibly new variants rather than new `LogicalInstr`
  variants. Not confirmed.
- `POWER` is binary and `SIGN`'s result domain differs from its input (and `SIGN` over an
  integer column is not a float operation at all), so neither obviously fits `FloatUnaryOp`.
  Where they belong is unknown.
- **Integer-returning string functions** (`POSITION`, `STRPOS`): new instruction, but no
  arena write.
- **String-producing functions** (`REPLACE`, `LPAD`, `RPAD`, `LEFT`, `RIGHT`, `REVERSE`,
  `SPLIT_PART`): new instruction plus an arena-writing kernel, following `eval_str_concat`.
- **Free desugars over existing nodes**: `IFNULL`/`NVL` → `COALESCE`; `IF(c,a,b)` → `CASE`.
  These need no new opcode.
- `IS DISTINCT FROM` is *probably* a desugar too, but with a caveat: it needs `IS NULL` on
  both operands, and `LeafBinder::bind_null_test` resolves its operand to a single column
  (`gnitz-sql/src/hir/bind.rs` — `self.resolve(inner)` / `self.col_id(inner)`). So a desugar
  built on `IsNull` covers **column operands only**; `expr1 IS DISTINCT FROM expr2` with
  computed operands would need more. Not investigated.

## Explicitly NOT verified

- **`StrConcat` takes two `Reg` operands and writes the arena**, which is why register-operand
  string producers look feasible. It was **not** confirmed that a kernel can read from the
  arena and write to it in the same program (i.e. chained string producers), nor what the
  validator does in that case.
- Whether `ExprValidateErr` imposes register/const-pool limits that a wider function set
  would hit.
- The wire opcode numbering scheme for `ExprOp` was not read; `LogicalInstr::to_wire` and
  `LogicalProgram::decode_triple` are described in `gnitz-expr/src/lib.rs` as two tables
  checked against each other by `tests/program.rs` drift tests.
- `REGEXP_*` was grouped with the rest above purely because it is rejected the same way. It
  is not in the same effort class as the others and probably should not be in the same plan.
- `MD5` likewise: a hash function has different determinism/portability questions.

## Where to start

Split by class, not alphabetically. The desugar-only group is separable and needs no
`gnitz-expr` change at all.
