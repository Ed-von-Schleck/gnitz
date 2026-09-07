from gnitz._native import (
    GnitzError, GnitzConflictError, GnitzDeltaExpiredError,
    GnitzSalFullError, GnitzMirrorPoisonedError, GnitzNotFoundError, Row, ScanResult,
    ColumnDef, Schema, ZSetBatch, GnitzClient, DeltaReply, delta_reply_schema,
    PollResult,
    TABLE_TAB, FIRST_USER_TABLE_ID,
)
from gnitz._types import Opcode, TypeCode


def connect(socket_path):
    return GnitzClient(socket_path)
