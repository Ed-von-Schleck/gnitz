# Residual-filter eval_expr: unsigned column truncation

`gnitz-sql/src/dml.rs::eval_expr` decodes every payload column value
into `i64` and then runs all binary operators (`Eq`, `Gt`, `Lt`, `Add`,
`Sub`, `Mul`, `Div`, `Mod`, ...) under signed `i64` arithmetic. The
per-typecode branch covers `U8/I8/U16/I16/U32/I32` explicitly and
drops everything else into a single fallback:

```rust
// gnitz-sql/src/dml.rs:964 (payload column read)
ColData::Fixed(buf) => {
    let stride = col_def.type_code.wire_stride();
    let start  = i * stride;
    let slice  = &buf[start..start + stride];
    let v: i64 = match col_def.type_code {
        TypeCode::U8  => slice[0] as i64,
        TypeCode::I8  => slice[0] as i8 as i64,
        TypeCode::U16 => u16::from_le_bytes(slice.try_into().unwrap()) as i64,
        TypeCode::I16 => i16::from_le_bytes(slice.try_into().unwrap()) as i64,
        TypeCode::U32 => u32::from_le_bytes(slice.try_into().unwrap()) as i64,
        TypeCode::I32 => i32::from_le_bytes(slice.try_into().unwrap()) as i64,
        _             => i64::from_le_bytes(slice.try_into().unwrap()),
    };
    Ok(Some(v))
}
```

The same shape repeats in `decode_pk_bytes_to_i64` (line 896) for PK
column reads:

```rust
fn decode_pk_bytes_to_i64(tc: TypeCode, slice: &[u8]) -> Result<i64, GnitzSqlError> {
    Ok(match tc {
        // ...
        TypeCode::U64 => u64::from_le_bytes(slice[..8].try_into().unwrap()) as i64,
        // ...
    })
}
```

For any `U64` value `v >= 2^63`, the `as i64` cast reinterprets `v` as
`v - 2^64` (a negative `i64`). Comparisons then run signed:

- `WHERE u >= 2^63` against a row with `u = 2^63` evaluates `lv >= rv`
  as `i64::MIN >= 2^63 (which itself wrapped)` → wrong arm.
- `WHERE u > 0` against a row with `u = 18446744073709551611`
  (`-5` as `i64`) evaluates `false`, dropping the row.
- Arithmetic on values straddling `2^63` wraps under `i64` rules
  instead of `u64` rules, so `WHERE u + 1 > u` is false for the
  highest `U64` even though it never overflows in `u64`.

The `_` fallback also covers `I64`, `F64`, and `U128`. `I64` happens
to round-trip through `i64::from_le_bytes` correctly. `F64` is decoded
as integer bits — every comparison is garbage. `U128` panics
upstream because `Fixed` columns aren't used for `U128` storage
(`U128s` is its own variant, handled separately).

## Fix

Carry signedness through `eval_expr`. The shortest viable change is
to widen the internal representation to `i128`:

- `i64` fits exactly in `i128`.
- `u64` fits exactly in `i128` (any value, including `2^64 - 1`).
- Arithmetic uses `i128::wrapping_*`; division by zero keeps the
  existing `0` short-circuit.
- Comparisons use native `i128` ordering, which is the unambiguous
  total order shared by signed and unsigned operand types.

`eval_expr`'s return type becomes `Result<Option<i128>, _>`. The
payload-column decode collapses to one arm per width and stays
sign-aware via the typecode:

```rust
let v: i128 = match col_def.type_code {
    TypeCode::U8  => slice[0] as u8  as i128,
    TypeCode::I8  => slice[0] as i8  as i128,
    TypeCode::U16 => u16::from_le_bytes(slice.try_into().unwrap()) as i128,
    TypeCode::I16 => i16::from_le_bytes(slice.try_into().unwrap()) as i128,
    TypeCode::U32 => u32::from_le_bytes(slice.try_into().unwrap()) as i128,
    TypeCode::I32 => i32::from_le_bytes(slice.try_into().unwrap()) as i128,
    TypeCode::U64 => u64::from_le_bytes(slice.try_into().unwrap()) as i128,
    TypeCode::I64 => i64::from_le_bytes(slice.try_into().unwrap()) as i128,
    TypeCode::F32 | TypeCode::F64 => return Err(GnitzSqlError::Unsupported(
        "residual filter on float column not supported".to_string()
    )),
    _ => unreachable!(
        "non-Fixed column types should be filtered out before this match"
    ),
};
Ok(Some(v))
```

The PK decoder follows the same pattern:

```rust
fn decode_pk_bytes_to_i128(tc: TypeCode, slice: &[u8]) -> Result<i128, GnitzSqlError> {
    Ok(match tc {
        TypeCode::I8  => slice[0] as i8 as i128,
        TypeCode::U8  => slice[0] as i128,
        TypeCode::I16 => i16::from_le_bytes(slice[..2].try_into().unwrap()) as i128,
        TypeCode::U16 => u16::from_le_bytes(slice[..2].try_into().unwrap()) as i128,
        TypeCode::I32 => i32::from_le_bytes(slice[..4].try_into().unwrap()) as i128,
        TypeCode::U32 => u32::from_le_bytes(slice[..4].try_into().unwrap()) as i128,
        TypeCode::I64 => i64::from_le_bytes(slice[..8].try_into().unwrap()) as i128,
        TypeCode::U64 => u64::from_le_bytes(slice[..8].try_into().unwrap()) as i128,
        _ => return Err(GnitzSqlError::Unsupported(format!(
            "residual filter on PK column of type {:?} not supported", tc
        ))),
    })
}
```

Binary operator implementations use `i128::wrapping_*` and direct
comparison:

```rust
BoundExpr::BinOp(l, op, r) => {
    let lv = match eval_expr(l, batch, i, schema)? { None => return Ok(None), Some(v) => v };
    let rv = match eval_expr(r, batch, i, schema)? { None => return Ok(None), Some(v) => v };
    Ok(Some(match op {
        BinOp::Add => lv.wrapping_add(rv),
        BinOp::Sub => lv.wrapping_sub(rv),
        BinOp::Mul => lv.wrapping_mul(rv),
        BinOp::Div => if rv == 0 { 0 } else { lv.wrapping_div(rv) },
        BinOp::Mod => if rv == 0 { 0 } else { lv.wrapping_rem(rv) },
        BinOp::Eq  => (lv == rv) as i128,
        BinOp::Ne  => (lv != rv) as i128,
        BinOp::Gt  => (lv >  rv) as i128,
        BinOp::Ge  => (lv >= rv) as i128,
        BinOp::Lt  => (lv <  rv) as i128,
        BinOp::Le  => (lv <= rv) as i128,
        // ... etc
    }))
}
```

Float operands stay rejected. `LitInt` widens trivially: `i64 -> i128`
loses no information.

## Touchpoints

- `crates/gnitz-sql/src/dml.rs::eval_expr` — payload column decode at
  line 964 (the inner `match col_def.type_code`); binary operator
  arms at lines 1015-1026.
- `crates/gnitz-sql/src/dml.rs::decode_pk_bytes_to_i64` (line 896) —
  rename to `_to_i128`, retype the U64 branch.
- `crates/gnitz-sql/src/dml.rs::eval_pred_row` (line 913), `eval_set_expr`
  (line 1182), and `eval_do_update_rhs` — same return-type widening;
  the bool projection (`v != 0`) stays correct for `i128`.

## Testing

- `CREATE TABLE t (id U64 PRIMARY KEY, u U64)`. Insert
  `(1, 18446744073709551611)` and `(2, 5)`. Then:
  - `SELECT * FROM t WHERE u > 10` returns `id=1` (and not `id=2`).
  - `SELECT * FROM t WHERE u < 10` returns `id=2` (and not `id=1`).
  - `SELECT * FROM t WHERE u = 18446744073709551611` returns `id=1`.
  - `SELECT * FROM t WHERE u + 1 > u` returns `id=2` (and not `id=1`,
    because `2^64 - 5 + 1` wraps under `u64` rules).
- Repeat each query against an `id U64 PRIMARY KEY` table with the
  same high value as the PK, confirming the PK-decode path is
  consistent.
- Non-regression: every signed-integer test that exercised
  `eval_expr` before this change continues to pass with identical
  results (`I64::MIN`, `I64::MAX`, mixed signs in arithmetic).
- A residual filter on `F32`/`F64` column reports `Unsupported`,
  preserving the previous behavior.
