# Backup and upgrades

gnitz has two ways to copy a database:

| | Backup | Dump |
|---|---|---|
| What it is | A copy of the data directory | The schema as SQL and the tables' rows as Parquet |
| Made by | The server, into its `--backup-dir` | The shell, over a connection |
| Restored by | Starting a server on the copy | `\restore` into a running server |
| Views | Kept as they are | Rebuilt from their definitions |
| Across releases | Same storage format only | Any later release |

## Backups

Start the server with a backup directory, then ask for a backup by name:

```sh
gnitz-server --backup-dir=/srv/gnitz-backups /var/lib/gnitz /run/gnitz/gnitz.sock
gnitz /run/gnitz/gnitz.sock -c '\backup nightly-2026-11-26'
```

The server checkpoints and copies the data directory into
`/srv/gnitz-backups/nightly-2026-11-26`. The copy holds every write
acknowledged before the request, as one consistent cut; DDL waits while it is
made. Where the backup directory is on the same filesystem as the data
directory, files are hard-linked rather than copied, so a backup is fast and
takes little space until the data moves on.

To restore, stop the server and start one on the backup, at any worker count:

```sh
cp -a /srv/gnitz-backups/nightly-2026-11-26 /var/lib/gnitz-restored
gnitz-server /var/lib/gnitz-restored /run/gnitz/gnitz.sock
```

A backup on the same filesystem shares files with the live data directory, so
copy it before starting a server on it, and copy it elsewhere for safety
against disk loss.

## Dumps

```sh
gnitz /run/gnitz/gnitz.sock -c '\dump /srv/gnitz-dump'
gnitz /run/gnitz/gnitz-new.sock -c '\restore /srv/gnitz-dump'
```

A dump holds every definition — schemas, tables, views, indexes — and every
table's rows, read from one snapshot, and is written into a new or empty
directory. Streams hold no rows, so only their definitions are dumped.
`\restore` creates the schemas and tables, loads the rows, and then creates the
views and indexes, so each view is computed once from the loaded data. The
target database must not already hold the dump's names.

## Upgrading

A data directory is stamped with the storage format of the release that last
opened it. A release with the same format opens it in place; just install the
new binaries, and new clients with them, since a client talks only to a server
of its own release.

A release whose format changed refuses an older data directory at boot with
exit code 65, naming both releases, and changes nothing in it. Move the data with a dump, using each release's own
shell and server:

```sh
# with gnitz and gnitz-server of the old release
gnitz --data /var/lib/gnitz -c '\dump /srv/gnitz-dump'
# with gnitz and gnitz-server of the new release
gnitz --data /var/lib/gnitz-new -c '\restore /srv/gnitz-dump'
```

`gnitz --data` runs the `gnitz-server` installed beside the shell, or else the
one on `PATH`, on that directory for the duration of the command. `\dump` and `\restore` also work
from a shell connected to a running server, such as a container of the old
release's image tag.
