from gnitz._native import (
    GnitzError, GnitzConflictError, GnitzDeltaExpiredError,
    GnitzSalFullError, GnitzMirrorPoisonedError, GnitzNotFoundError, Row, ScanResult,
    ColumnDef, Schema, ZSetBatch, GnitzClient, DeltaReply, delta_reply_schema,
    PollResult,
    SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, IDX_TAB, FIRST_USER_TABLE_ID,
    unpack_pk_cols, debug_assertions,
)
from gnitz._types import Opcode, TypeCode


def connect(socket_path):
    return GnitzClient(socket_path)
