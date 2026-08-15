from gnitz._native import (
    GnitzError, GnitzConflictError, Row, ScanResult,
    ColumnDef, Schema, ZSetBatch, GnitzClient,
    TABLE_TAB, IDX_TAB, FIRST_USER_TABLE_ID, unpack_pk_cols,
    IDXTAB_COL_OWNER_ID, IDXTAB_COL_SOURCE_COLS, IDXTAB_COL_IS_UNIQUE,
)
from gnitz._types import TypeCode


def connect(socket_path):
    return GnitzClient(socket_path)
