from gnitz._native import (
    GnitzError, GnitzConflictError, GnitzDeltaExpiredError, Row, ScanResult,
    ColumnDef, Schema, ZSetBatch, GnitzClient, DeltaReply, delta_reply_schema,
    TABLE_TAB, FIRST_USER_TABLE_ID,
)
from gnitz._types import TypeCode


def connect(socket_path):
    return GnitzClient(socket_path)
