from gnitz._native import (
    GnitzError, GnitzRefusedError, GnitzConnectionError,
    GnitzConflictError, GnitzDeltaExpiredError,
    GnitzSalFullError, GnitzMirrorPoisonedError, GnitzNotFoundError,
    GnitzIntegrityError, Row, ScanResult,
    ColumnDef, Schema, ZSetBatch, GnitzClient, AsyncGnitzClient,
    PollResult, Pushed, Pending,
    SCHEMA_TAB, TABLE_TAB, COL_TAB, IDX_TAB,
    FIRST_USER_TABLE_ID, MAX_COLUMNS,
    debug_assertions, sys_schema,
)
from gnitz._types import (
    TypeCode, SqlResult,
    DdlResult, RowsAffectedResult, RowsResult,
    TransactionStartedResult, TransactionCommittedResult,
    TransactionRolledBackResult,
)

# A type checker takes a typed package's imports as private unless they are
# listed here.
__all__ = [
    "GnitzError", "GnitzRefusedError", "GnitzConnectionError",
    "GnitzConflictError", "GnitzDeltaExpiredError",
    "GnitzSalFullError", "GnitzMirrorPoisonedError", "GnitzNotFoundError",
    "GnitzIntegrityError", "Row", "ScanResult",
    "ColumnDef", "Schema", "ZSetBatch", "GnitzClient", "AsyncGnitzClient",
    "PollResult", "Pushed", "Pending",
    "SCHEMA_TAB", "TABLE_TAB", "COL_TAB", "IDX_TAB",
    "FIRST_USER_TABLE_ID", "MAX_COLUMNS",
    "debug_assertions", "sys_schema",
    "TypeCode", "SqlResult",
    "DdlResult", "RowsAffectedResult", "RowsResult",
    "TransactionStartedResult", "TransactionCommittedResult",
    "TransactionRolledBackResult",
    "connect",
]


def connect(target: str, schema: str = "public") -> GnitzClient:
    """A connection whose unqualified relation names resolve in `schema`;
    `other.name` names a relation of another schema."""
    return GnitzClient(target, schema)
