# Data types

| Type | Spellings | Stored as | Key |
|---|---|---|---|
| 8–64-bit signed integer | `TINYINT`, `SMALLINT`, `INT`/`INTEGER`, `BIGINT` | 1, 2, 4, 8 bytes | yes |
| 8–64-bit unsigned integer | `TINYINT UNSIGNED`, `SMALLINT UNSIGNED`, `INT UNSIGNED`/`INTEGER UNSIGNED`/`UNSIGNED INTEGER`, `BIGINT UNSIGNED` | 1, 2, 4, 8 bytes | yes |
| 128-bit integer | `HUGEINT`/`INT128`, `UHUGEINT`/`UINT128` | 16 bytes | yes |
| Auto-numbered integer | `SMALLSERIAL`, `SERIAL`, `BIGSERIAL` (also `SERIAL2`, `SERIAL4`, `SERIAL8`) | 2, 4, 8 bytes | yes |
| Fixed point | `DECIMAL(p, s)`, `NUMERIC(p, s)`, `DEC(p, s)`, `p` ≤ 18 | 8 bytes, scaled by 10<sup>s</sup> | yes |
| Boolean | `BOOLEAN`, `BOOL` | 1 byte | yes |
| Date | `DATE` | days since 1970-01-01 | yes |
| Timestamp | `TIMESTAMP`, `TIMESTAMP WITHOUT TIME ZONE`, `TIMESTAMP_NTZ`, `DATETIME` | microseconds since 1970-01-01, no time zone | yes |
| UUID | `UUID` | 16 bytes | yes |
| Floating point | `FLOAT`/`REAL` (32-bit), `DOUBLE`/`DOUBLE PRECISION` (64-bit) | 4, 8 bytes | no |
| Text | `TEXT`, `VARCHAR(n)`, `CHAR(n)` | UTF-8, any length | no |
| Bytes | `BLOB`, `BYTEA` | any length | no |

"Key" says whether the type can be a primary-key, `UNIQUE` or indexed column;
see [Keys and identity](../concepts/keys.md).

```sql
CREATE TABLE payments (
    id      BIGSERIAL PRIMARY KEY,
    account UUID NOT NULL,
    amount  DECIMAL(12, 2) NOT NULL,
    paid_at TIMESTAMP,
    settled BOOLEAN,
    memo    TEXT
);
INSERT INTO payments (account, amount, paid_at, settled, memo)
    VALUES ('6f1c1a8e-3b8e-4c47-9d1e-2f0b6f9a1c2d', 12.50, '2026-11-26 10:30:00', true, 'rent');
SELECT * FROM payments;
```

## Notes

- **Length limits.** `VARCHAR(n)` and `CHAR(n)` accept the length and do not
  enforce it; they are `TEXT`.
- **`DECIMAL`.** The value is a 64-bit integer at scale `s`, so `p` is at most
  18 and bounds nothing beyond what 64 bits hold. `DECIMAL` needs an explicit
  precision.
- **`TIMESTAMP`** carries no time zone; store UTC.
- **`SERIAL`** columns take the next number when an `INSERT` leaves them out.
  A `SERIAL` column must be the table's single-column primary key, so a table
  has at most one.
- **Literals.** Dates, timestamps and UUIDs are written as strings
  (`'2026-11-26'`, `'2026-11-26 10:30:00'`); `TRUE` and `FALSE` are booleans.

Types gnitz does not have: `INTERVAL` (write durations as microseconds),
arrays, JSON, `TIMESTAMP WITH TIME ZONE`, enums and user-defined types.
