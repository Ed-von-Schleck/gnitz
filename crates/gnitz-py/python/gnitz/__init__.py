from gnitz._native import (
    GnitzError, GnitzConflictError, GnitzDeltaExpiredError,
    GnitzSalFullError, GnitzMirrorPoisonedError, GnitzNotFoundError,
    GnitzIntegrityError, Row, ScanResult,
    ColumnDef, Schema, ZSetBatch, GnitzClient, DeltaReply,
    PollResult,
    SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, IDX_TAB,
    FIRST_USER_TABLE_ID, MAX_COLUMNS,
    debug_assertions, sys_schema,
)
from gnitz._types import TypeCode


def connect(target):
    return GnitzClient(target)
