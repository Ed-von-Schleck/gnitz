from gnitz._native import (
    GnitzError, GnitzConflictError, Row, ScanResult,
    ColumnDef, Schema, ZSetBatch, GnitzClient,
    SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, IDX_TAB, DEP_TAB, SEQ_TAB,
    FIRST_USER_TABLE_ID, FIRST_USER_SCHEMA_ID, unpack_pk_cols,
)
from gnitz._types import TypeCode


def connect(socket_path):
    return GnitzClient(socket_path)
