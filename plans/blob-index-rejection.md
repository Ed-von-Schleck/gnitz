# Reject CREATE INDEX on BLOB columns

`gnitz-core/src/client.rs::validate_index_col_type` rejects float and
string PK columns from secondary indexes but lets `Blob` through. The
server then treats the blob column as if it were a fixed 8-byte
scalar in `promote_col_to_u64` (`gnitz-engine/src/ops/index.rs:14`),
producing index keys that collide on every blob sharing the same
`(length, prefix)` head:

```rust
// gnitz-core/src/client.rs:46
fn validate_index_col_type(tc: TypeCode) -> Result<(), ClientError> {
    match tc {
        TypeCode::F32 | TypeCode::F64 | TypeCode::String => Err(ClientError::ServerError(
            "index on float/string column not supported".to_string()
        )),
        _ => Ok(()),
    }
}
```

```rust
// gnitz-engine/src/ops/index.rs:14
fn promote_col_to_u64(mb: &MemBatch, row: usize, pi: usize, col_type_code: u8) -> u64 {
    let cs = match col_type_code {
        type_code::U8 | type_code::I8 => 1,
        type_code::U16 | type_code::I16 => 2,
        type_code::U32 | type_code::I32 | type_code::F32 => 4,
        _ => 8, // U64, I64, F64; U128 excluded by caller
    };
    // ...
}
```

`Blob` falls into the `_` branch and reads 8 bytes from the column's
German-string slot, which encodes `length: u32` + either the inline
prefix (short blobs) or the heap offset (long blobs). Two distinct
long blobs that happen to share `length` and live at the same heap
offset slot — or, more commonly, two short blobs whose first four
bytes match the prefix of a long blob — produce identical index keys.
A `UNIQUE INDEX` on the blob column then either rejects legitimate
inserts as duplicates or silently merges them, depending on which
side of the merge wins.

## Fix

Add `TypeCode::Blob` to the client-side rejection list:

```rust
fn validate_index_col_type(tc: TypeCode) -> Result<(), ClientError> {
    match tc {
        TypeCode::F32 | TypeCode::F64 | TypeCode::String | TypeCode::Blob => {
            Err(ClientError::ServerError(
                "index on float/string/blob column not supported".to_string()
            ))
        }
        _ => Ok(()),
    }
}
```

Symmetric guard on the SQL planner side so the rejection reports as
`Unsupported` rather than `ServerError` for SQL callers. Locate the
`execute_create_index` site that already rejects float/string indexes
and extend the type list:

```rust
// gnitz-sql/src/planner.rs — execute_create_index
let tc = schema.columns[col_idx].type_code;
if matches!(tc, TypeCode::F32 | TypeCode::F64 | TypeCode::String | TypeCode::Blob) {
    return Err(GnitzSqlError::Unsupported(format!(
        "CREATE INDEX on {:?} column not supported", tc,
    )));
}
```

## Touchpoints

- `crates/gnitz-core/src/client.rs::validate_index_col_type` (line 46)
- `crates/gnitz-sql/src/planner.rs::execute_create_index` — the type
  rejection block that already filters `F32`/`F64`/`String`.

## Testing

- `CREATE TABLE t (id U64 PRIMARY KEY, payload BLOB)`. Then
  `CREATE INDEX ix ON t(payload)` — must be rejected with a clean
  `Unsupported` error.
- Same rejection via a direct `client.create_index(...)` call against
  a `Blob` column (simulating gnitz-py / gnitz-capi callers that skip
  the SQL planner).
- Non-regression: `CREATE INDEX` on a non-blob payload column (e.g.
  `I64`) still succeeds.
