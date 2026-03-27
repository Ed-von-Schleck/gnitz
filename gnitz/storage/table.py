# gnitz/storage/table.py
#
# PersistentTable is now a factory that creates an EphemeralTable with
# durable=True.  The Rust Table handle manages WAL + manifest internally.

from gnitz.storage.ephemeral_table import EphemeralTable


def PersistentTable(
    directory,
    name,
    schema,
    table_id=1,
    memtable_arena_size=1 * 1024 * 1024,
    validate_checksums=False,
):
    return EphemeralTable(
        directory,
        name,
        schema,
        table_id=table_id,
        memtable_arena_size=memtable_arena_size,
        validate_checksums=validate_checksums,
        durable=True,
    )
