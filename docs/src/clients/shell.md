# The shell

```text
gnitz [OPTIONS] [TARGET]
```

`gnitz` is an interactive SQL shell in the style of `psql`. `TARGET` is a Unix
socket path or a [`tls://` target](../ops/security.md#target-syntax); without
one it uses `GNITZ_TARGET`, and with neither it runs a `gnitz-server` installed
beside it or on `PATH` for the session, deleting its data on exit.

| Option | |
|---|---|
| `--data DIR` | Run the session's own server on `DIR`, and keep the data there. |
| `-s`, `--schema SCHEMA` | The schema unqualified names resolve in (default `public`). |
| `-c`, `--command CMD` | Run `CMD` — SQL or a meta-command — and exit. Repeatable. |
| `-f`, `--file FILE` | Run the commands in `FILE` (`-` for stdin) and exit. Repeatable. |
| `-F`, `--format FMT` | `aligned` (default), `unaligned`, `csv` or `json`. |
| `-x`, `--expanded` | One field per line. |
| `--continue-on-error` | Keep running a script after a command fails. |
| `--vi` | vi key bindings. |
| `--no-color` | No styling. |

```sh
gnitz /run/gnitz/gnitz.sock -c 'SELECT * FROM revenue' -F csv > revenue.csv
gnitz --data ./devdb -f schema.sql
```

Statements end with `;`. Tab completes keywords, names and meta-commands.

## Meta-commands

| Command | |
|---|---|
| `\?` | Help. |
| `\q` | Quit. |
| `\c [TARGET]` | Reconnect, to `TARGET` if given. |
| `\conninfo` | Show the connection. |
| `\use SCHEMA` | Resolve unqualified names in `SCHEMA`. |
| `\d [NAME]` | List relations, or describe `NAME`; `\d+ NAME` also shows hidden columns and a view's definition. |
| `\dt`, `\dv`, `\ds`, `\di`, `\dS` `[PATTERN]` | List tables, views, streams, indexes or system relations, those matching `PATTERN` if given. |
| `\dn` | List schemas. |
| `\watch VIEW`, `\watch SELECT …` | Print a [delta-fed](../sql/views.md#delta-feeds) view's changes, or those a `SELECT` over it keeps, as they happen, weights included, until Ctrl-C. |
| `\x [on\|off\|auto]` | Expanded output. |
| `\a` | Toggle aligned and unaligned output. |
| `\pset [OPTION [VALUE]]` | Show or set output options. |
| `\timing [on\|off]` | Time each statement. |
| `\o [FILE]` | Send results to `FILE`, or back to the terminal. |
| `\i FILE` | Run the commands in `FILE`. |
| `\e [FILE]` | Edit the last statement, or `FILE`, in `$EDITOR`. |
| `\echo [TEXT]` | Print `TEXT`. |
| `\! [COMMAND]` | Run a shell command. |
| `\dump DIR`, `\restore DIR` | Dump the database into `DIR`, or restore a dump; see [Backup and upgrades](../ops/backup.md#dumps). |
| `\backup NAME` | Have the server make a [backup](../ops/backup.md#backups) named `NAME`. |

## Watching a view

```sql
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer TEXT, amount BIGINT);
CREATE VIEW big WITH (delta = '16 MB') AS SELECT id, amount FROM orders WHERE amount > 100;
```

`\watch big` then prints each change to `big` as it is made from another
session: inserted rows with weight `+1`, deleted ones with `-1`, and an update
as one of each.
