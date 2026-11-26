# Writing data

Every write to a table is durable when it is acknowledged, and every view
reading the written table reflects it in the next read of that view.

```sql
CREATE TABLE stock (id BIGINT PRIMARY KEY, item TEXT, qty BIGINT);
```

## `INSERT`

```sql
INSERT INTO stock VALUES (1, 'bolt', 100), (2, 'nut', 250);
INSERT INTO stock (id, item) VALUES (3, 'washer');
INSERT INTO stock VALUES (4, 'screw', 10) RETURNING id;
```

A column left out is `NULL`, or the next number for a `SERIAL` column.
Inserting a key that already exists is an error, unless the statement says how
to resolve it:

```sql,refused
INSERT INTO stock VALUES (1, 'bolt', 90);
```

```text
duplicate key value violates unique constraint "public_stock_pkey": Key (id)=(1) already exists
```

```sql
INSERT INTO stock VALUES (1, 'bolt', 90) ON CONFLICT (id) DO NOTHING;
INSERT INTO stock VALUES (1, 'bolt', 90) ON CONFLICT (id) DO UPDATE SET qty = EXCLUDED.qty;
```

`INSERT` takes `VALUES` only; `INSERT … SELECT` is not supported. For bulk
loading, a client pushes batches of rows directly; see
[Python](../clients/python.md#pushing-rows).

## `UPDATE` and `DELETE`

```sql
UPDATE stock SET qty = qty - 10 WHERE item = 'bolt';
DELETE FROM stock WHERE qty IS NULL;
```

`UPDATE` cannot change a primary-key column; delete the row and insert it under
its new key instead.

## Transactions

```sql
BEGIN;
INSERT INTO stock VALUES (5, 'rivet', 40);
UPDATE stock SET qty = 0 WHERE id = 2;
COMMIT;
```

A transaction's writes become visible together at `COMMIT`, and `ROLLBACK`
discards them. Inside a transaction:

- Only `INSERT`, `UPDATE`, `DELETE` and `SELECT` are allowed; DDL is refused.
- A `SELECT` sees the state before the transaction, not its own writes.
- An `INSERT` into a stream is refused, and the transaction stays open.

```sql
CREATE TABLE clicks (id BIGINT PRIMARY KEY, page BIGINT) WITH (stream = true);
BEGIN;
INSERT INTO stock VALUES (6, 'pin', 5);
```

```sql,refused
INSERT INTO clicks VALUES (1, 7);
```

```text
'clicks' is a stream; INSERT inside a transaction requires a base table
```

```sql
COMMIT;
```

An `UPDATE` or `DELETE` reads the rows it changes. If another client writes
that table between the read and the `COMMIT`, the `COMMIT` is refused with
nothing written, and the transaction is yours to run again. Outside a
transaction the statement runs itself again a few times, and reports the
conflict only when another client keeps writing the table.
