# Functions and operators

Every function here works in a plain `SELECT` and in a view body. A function
of a `NULL` argument is `NULL` unless it says otherwise.

```sql
CREATE TABLE t (id INT PRIMARY KEY, x DOUBLE, n BIGINT, s TEXT, d DATE, ts TIMESTAMP);
INSERT INTO t VALUES (1, 2.5, 7, ' Hello ', '2026-11-26', '2026-11-26 10:30:00');
```

## Operators

| | |
|---|---|
| Arithmetic | `+`, `-`, `*`, `/`, `%` |
| Comparison | `=`, `<>`/`!=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN (…)` |
| Logic | `AND`, `OR`, `NOT` |
| Null tests | `IS NULL`, `IS NOT NULL` |
| Text | `\|\|` (concatenation), `LIKE`, `ILIKE` |

Integer division truncates. `DATE + n` adds `n` days, and subtracting two
dates gives days; subtracting a timestamp or a date from a timestamp gives
microseconds.

```sql
SELECT n / 2, n % 3, d + 1, ts - d, s || '!', s ILIKE '%ell%' FROM t;
```

## Conditionals

`CASE`, `COALESCE(a, b, …)`, `NULLIF(a, b)`, `IFNULL(a, b)` (also `NVL`),
`IF(condition, then, else)`, `GREATEST(a, b, …)`, `LEAST(a, b, …)`.
`GREATEST` and `LEAST` skip `NULL` arguments.

```sql
SELECT CASE WHEN n > 5 THEN 'big' ELSE 'small' END, COALESCE(NULL, n), IF(n > 1, 'y', 'n') FROM t;
```

## Numbers

`ABS`, `SIGN`, `CEIL`/`CEILING`, `FLOOR`, `ROUND(x)`, `ROUND(x, digits)`,
`TRUNC`, `MOD(a, b)`, `POWER(a, b)`/`POW`, `SQRT`, `EXP`, `LN`, `LOG` (base 10).

```sql
SELECT ROUND(x, 1), FLOOR(x), POWER(x, 2), MOD(n, 3) FROM t;
```

## Text

| Function | Result |
|---|---|
| `UPPER(s)`, `LOWER(s)` | case-converted text |
| `LENGTH(s)` (`CHAR_LENGTH`, `CHARACTER_LENGTH`), `OCTET_LENGTH(s)` | length in characters, in bytes |
| `TRIM(s)`, `LTRIM(s)`, `RTRIM(s)` | `s` without surrounding spaces |
| `SUBSTRING(s, start[, len])` (`SUBSTR`), `LEFT(s, n)`, `RIGHT(s, n)` | part of `s`; positions start at 1 |
| `STRPOS(s, sub)`, `POSITION(sub IN s)` | position of `sub` in `s`, 0 if absent |
| `REPLACE(s, from, to)` | `s` with every `from` replaced |
| `REVERSE(s)` | `s` backwards |
| `LPAD(s, n, fill)`, `RPAD(s, n, fill)` | `s` padded to `n` characters |
| `SPLIT_PART(s, sep, i)` | the `i`-th field of `s` split at `sep` |
| `CONCAT(a, b, …)` | the arguments joined, `NULL`s skipped |

```sql
SELECT UPPER(TRIM(s)), SUBSTRING(s, 2, 3), SPLIT_PART('a,b,c', ',', 2) FROM t;
```

## Dates and times

`EXTRACT(field FROM t)` and `DATE_PART('field', t)` take `YEAR`, `QUARTER`,
`MONTH`, `WEEK` (also `ISOWEEK`), `DAY`, `DOY` (also `DAYOFYEAR`), `DOW` (also
`DAYOFWEEK`), `ISODOW`, `HOUR`, `MINUTE`, `SECOND` and `EPOCH`.
`DATE_TRUNC('unit', t)` rounds down to a `year`, `quarter`, `month`, `week`,
`day`, `hour`, `minute` or `second`.

```sql
SELECT EXTRACT(YEAR FROM d), DATE_PART('hour', ts), DATE_TRUNC('month', ts) FROM t;
```

`NOW()` and `CURRENT_TIMESTAMP` are allowed only in a view's `WHERE`; see
[Views](views.md#what-a-view-body-can-contain).

## Casts

`CAST(x AS type)` gives `NULL` where the cast fails; `TRY_CAST` and
`SAFE_CAST` are the same. A float cast to an integer is truncated.

```sql
SELECT CAST(n AS TEXT), CAST('12' AS BIGINT), TRY_CAST('twelve' AS BIGINT), CAST(ts AS DATE) FROM t;
```

## Aggregates

`COUNT(*)`, `COUNT(x)`, `COUNT(DISTINCT x)`, `SUM`, `MIN`, `MAX`, `AVG`. In a
view they are maintained incrementally: `COUNT` and `SUM` by adding the change,
`MIN` and `MAX` through an index of each group's values, so deleting the current
minimum does not rescan the group.

`SUM` and `AVG` of floating-point values depend on the order rows are added
in, so the last digits can differ between two computations of the same data.
Use `DECIMAL` or integers where exact totals matter.

## Window functions

In a view body only: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, and the aggregates
above with `OVER (…)`. See [Views](views.md#what-a-view-body-can-contain).
