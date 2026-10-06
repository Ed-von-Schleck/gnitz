from gnitz._native import (
    GnitzError, GnitzRefusedError, GnitzConnectionError,
    GnitzConflictError, GnitzDeltaExpiredError,
    GnitzSalFullError, GnitzMirrorPoisonedError, GnitzNotFoundError,
    GnitzIntegrityError, Row, ScanResult,
    ColumnDef, Schema, ZSetBatch, GnitzClient, AsyncGnitzClient,
    PollResult, Pending,
    SCHEMA_TAB, TABLE_TAB, COL_TAB, IDX_TAB,
    FIRST_USER_TABLE_ID, MAX_COLUMNS,
    debug_assertions, sys_schema,
)
from gnitz._types import TypeCode


def connect(target, schema="public"):
    """A connection whose unqualified relation names resolve in `schema`;
    `other.name` names a relation of another schema."""
    return GnitzClient(target, schema)
