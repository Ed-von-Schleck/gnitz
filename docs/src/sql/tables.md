# Schemas, tables and indexes

## Schemas

Relations live in schemas; `public` is the default. A name without a schema
resolves in the connection's schema (`\use` in the shell, `schema=` in
Python), and `schema.name` reaches another one.

```sql
CREATE SCHEMA analytics;
CREATE TABLE analytics.visits (id BIGINT PRIMARY KEY, page TEXT);
DROP SCHEMA analytics CASCADE;
```

`DROP SCHEMA … CASCADE` drops everything in the schema; without `CASCADE` the
schema must be empty. `_system` holds the [catalog](../concepts/relations.md#the-catalog)
and cannot be written.

## `CREATE TABLE`

```sql
CREATE TABLE accounts (
    id    BIGSERIAL PRIMARY KEY,
    owner BIGINT NOT NULL,
    code  BIGINT UNIQUE,
    label TEXT
);
CREATE TABLE IF NOT EXISTS accounts (id BIGINT PRIMARY KEY);
```

- **`PRIMARY KEY`** is required, on one column or as `PRIMARY KEY (a, b)`. Key
  columns must be of a [key type](types.md) and are never null.
- **`NOT NULL`** is enforced on every write.
- **`UNIQUE`** on a key-type column is enforced on every write.
- **`REFERENCES t (col)`** declares a foreign key: a write whose value is not in
  `t` is refused, and so is deleting a referenced row.

```sql
CREATE TABLE ledger (id BIGINT PRIMARY KEY, account BIGINT REFERENCES accounts (id), amount BIGINT);
```

```sql,refused
INSERT INTO ledger VALUES (1, 999, 10);
```

```text
Foreign Key violation in 'public.ledger': value not found in target 'public.accounts'
```

There are no column defaults and no `CHECK` constraints.

### Options

`WITH (…)` after the column list takes:

| Option | Effect |
|---|---|
| `stream = true` | The table is a [stream](../concepts/relations.md#streams): it keeps no rows. |
| `replicated = true` | Every worker holds a full copy. For small tables that many views join against. |
| `compress = true` | Text columns are dictionary-coded on disk, where that saves space. |

`CLUSTER BY col` names a leading prefix of a compound primary key to
distribute rows by, so that rows sharing it live on the same worker:

```sql
CREATE TABLE readings (sensor INT, at TIMESTAMP, value DOUBLE, PRIMARY KEY (sensor, at))
    CLUSTER BY sensor;
CREATE TABLE regions (id INT PRIMARY KEY, name TEXT) WITH (replicated = true);
```

## `ALTER TABLE`

```sql
ALTER TABLE accounts ADD COLUMN note TEXT;
ALTER TABLE accounts RENAME COLUMN note TO remark;
ALTER TABLE accounts DROP COLUMN remark;
ALTER TABLE accounts ADD CONSTRAINT accounts_owner_key UNIQUE (owner);
ALTER TABLE accounts RENAME TO wallets;
```

An added column is nullable: `ADD COLUMN … NOT NULL` is refused, since the rows
already there have no value for it. A column's type cannot be changed, and
`SET NOT NULL` is refused; `DROP NOT NULL` works.

## Indexes

An index lets a plain `SELECT` find rows by a column other than the primary
key without a scan. Indexed columns must be of a key type.

```sql
CREATE INDEX wallets_by_owner ON wallets (owner);
SELECT * FROM wallets WHERE owner = 7;
DROP INDEX wallets_by_owner;
```

`CREATE UNIQUE INDEX` also enforces uniqueness, as `UNIQUE` does.

Index names are global rather than per schema: an index name takes no schema
qualifier, and two schemas cannot each hold an index of the same name. An index
created without a name is named after its schema, table and columns.

## `DROP`

```sql
DROP TABLE IF EXISTS ledger;
```

A table or view that a view reads cannot be dropped; drop the dependent views
first. `DROP TABLE … CASCADE` is not supported; `DROP SCHEMA … CASCADE` is.
