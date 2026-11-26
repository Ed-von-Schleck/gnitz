# Python

```sh
uv add gnitz                  # the client
uv add "gnitz[server]"        # the client and gnitz-server, for development and tests
```

The client needs Python 3.14 on Linux, and a server of the same release.

## Connecting

```python
import gnitz

with gnitz.connect("/run/gnitz/gnitz.sock") as conn:
    ...
with gnitz.connect("tls://db.example.com:7543?token_file=app.token", schema="analytics") as conn:
    ...
```

`connect()` with no target connects to `GNITZ_TARGET`. Unqualified names
resolve in `schema` (default `public`). [Security](../ops/security.md#target-syntax)
describes TLS targets.

For development, `gnitz.local_server()` runs a `gnitz-server` with a temporary
data directory that is deleted on exit:

```python
with gnitz.local_server() as server, gnitz.connect(server.target) as conn:
    ...
```

## SQL

`execute_sql` runs one or more statements and returns one result per statement,
a dict told apart by its `"type"`:

| `type` | Other keys |
|---|---|
| `"Rows"` | `rows`: the rows of a `SELECT` |
| `"RowsAffected"` | `count` |
| `"Ddl"` | — |
| `"TransactionStarted"`, `"TransactionCommitted"`, `"TransactionRolledBack"` | `lsn` for a commit |

```python
conn.execute_sql("""
    CREATE TABLE orders (id BIGINT PRIMARY KEY, customer TEXT, amount BIGINT);
    CREATE VIEW revenue AS SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer;
""")
conn.execute_sql("INSERT INTO orders VALUES (1, 'ann', 30), (2, 'bob', 12), (3, 'ann', 5)")

[result] = conn.execute_sql("SELECT * FROM revenue")
for row in result["rows"]:
    print(row.customer, row.total)       # by attribute
    print(row["total"], row[0])          # by name or position
    print(row._asdict())
```

A row a view holds more than once (a `UNION ALL`, say) comes back once, with
its count in `row._weight`.

## Pushing rows

For loading data, push batches of rows instead of building `INSERT` text:

```python
table_id, schema = conn.resolve_table("orders")
batch = gnitz.ZSetBatch(schema)
for i in range(4, 1000):
    batch.append(id=i, customer="carl", amount=i)
conn.push(table_id, batch)
```

A push upserts: a row whose key already exists replaces it. `mode="error"`
refuses the batch instead, as `INSERT` does. To delete rows by key:

```python
conn.delete(table_id, schema, [4, 5, 6])
```

`scan(table_id, schema)` reads a whole table or view, and `seek(table_id,
schema, key)` one row by its key.

## Transactions

```python
with conn.transaction() as txn:
    txn.push(table_id, batch)
    txn.execute_sql("UPDATE orders SET amount = 0 WHERE id = 1")
```

A clean exit commits, an exception rolls back. A commit that conflicts with
another client's write raises `gnitz.GnitzConflictError` with nothing written;
run the transaction again.

## Errors

A failed request raises a `gnitz.GnitzError`; an argument the client rejects
before sending raises a `TypeError` or `ValueError`:

| Exception | Meaning |
|---|---|
| `GnitzRefusedError` | The request was understood and declined; the connection is intact. |
| — `GnitzNotFoundError` | The named relation or schema does not exist. |
| — `GnitzIntegrityError` | The write would break a primary key, unique index or foreign key; nothing was written. |
| — `GnitzConflictError` | Nothing was written; run it again. |
| — `GnitzSalFullError` | The server's log had no room for the request; run it again. |
| — `GnitzDeltaExpiredError` | The feed does not continue this delta cursor; discard the copy and bootstrap again. |
| `GnitzConnectionError` | The connection is unusable: it failed, was lost, or was closed. |
| `GnitzMirrorPoisonedError` | The mirror store may be torn; recovery is `close_mirror()` and nothing else. |

## asyncio

`gnitz.aio.connect` returns a client with the same methods, each returning an
awaitable, and `aclose()` for `close()`. Calls that are one request each —
`push`, `delete`, `scan`, `seek` — share a round trip when started together:

```python
from gnitz import aio

async with aio.connect(target) as conn:
    await conn.execute_sql("SELECT * FROM revenue")
```

## Testing

`pytest_plugins = ["gnitz.testing"]` in a `conftest.py` provides the fixtures
`gnitz_server` (a server for the test session), `gnitz_target` (its target) and
`gnitz_client` (a connection in a fresh schema of each test's own, dropped with
everything in it when the test ends):

```python
def test_revenue(gnitz_client):
    gnitz_client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
    ...
```

For a FastAPI app, see [fastapi-gnitz](https://github.com/gnitzlabor/fastapi-gnitz).
The full API is in the package's docstrings and type stubs:
`help(gnitz.GnitzClient)`.
