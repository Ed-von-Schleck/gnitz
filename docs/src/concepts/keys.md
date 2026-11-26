# Keys and identity

## Every table has a primary key

A table must declare a primary key, of one column or several:

```sql
CREATE TABLE readings (sensor INT, at TIMESTAMP, value DOUBLE, PRIMARY KEY (sensor, at));
```

```sql,refused
CREATE TABLE notes (body TEXT);
```

```text
primary key must name at least one column
```

The primary key decides how rows are sorted on disk and which worker process
holds them, and a read that names the whole key finds its row directly.

## Key columns are integers

A key column must be of a type stored as an integer: the integer types, `DATE`,
`TIMESTAMP`, `DECIMAL`, `BOOLEAN` and `UUID` (see [Data types](../sql/types.md)).
Key columns are never nullable. `TEXT`, `BLOB` and floating-point columns cannot
be keys:

```sql,refused
CREATE TABLE users (email TEXT PRIMARY KEY, name TEXT);
```

```text
primary key column 'email' has type TEXT; only a type stored as an integer can be a key column
```

Floats are excluded because `-0.0` and `+0.0` compare equal while their bytes
differ, and `NaN` has no single representation; a key needs equal values to be
equal bytes. Text is excluded because keys are fixed-width.

Where data has a natural text key, give the table a surrogate key — a `SERIAL`
column, or a `UUID` computed by the client — and keep the text as an ordinary
column.

`UNIQUE` columns and indexed columns follow the same rule as primary keys.
Joins, `GROUP BY`, `SELECT DISTINCT` and window partitions take text columns,
but no float:

```sql
CREATE TABLE people (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE);
CREATE VIEW per_name AS SELECT name, COUNT(*) AS n FROM people GROUP BY name;
```

```sql,refused
CREATE VIEW per_score AS SELECT score, COUNT(*) AS n FROM people GROUP BY score;
```

```text
GROUP BY: float column 'score' cannot be a key (IEEE-754 -0.0/+0.0 and NaN break key equality)
```

## Identity is the whole row

A table's primary key is unique, and inserting a second row with the same key
is an error. Inside a view, though, gnitz identifies a row by all of its
columns, not by a key, and a view's result may hold the same row more than
once — a `UNION ALL` of two copies of a table holds each row twice. Each row
carries a count of its copies, called its *weight*.

A plain `SELECT` returns a row once per copy, so weights are mostly invisible.
They appear in a view's [delta feed](../sql/views.md#delta-feeds), where a
removed row arrives with a negative weight.
