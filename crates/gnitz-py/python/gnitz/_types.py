from enum import IntEnum
from typing import Literal, TypedDict

from gnitz._native import ScanResult, type_codes


class TypeCode(IntEnum):
    """A column's type, as `ColumnDef` takes it."""

    U8 = 1
    I8 = 2
    U16 = 3
    I16 = 4
    U32 = 5
    I32 = 6
    F32 = 7
    U64 = 8
    I64 = 9
    F64 = 10
    STRING = 11
    U128 = 12
    UUID = 13
    BLOB = 14
    I128 = 15
    DATE = 16
    TIMESTAMP = 17
    DECIMAL = 18
    BOOLEAN = 19


# The members are written out because a type checker reads them from this file;
# the table the wire and the engine use is gnitz_wire's `TypeCode::ALL`. An
# extension built from a different table does not import.
if {m.name: m.value for m in TypeCode} != dict(type_codes()):
    raise ImportError(f"gnitz.TypeCode {list(TypeCode)} is not the extension's {type_codes()}")


# One statement's result from `execute_sql`, told apart by its `type`.


class DdlResult(TypedDict):
    type: Literal["Ddl"]


class RowsAffectedResult(TypedDict):
    type: Literal["RowsAffected"]
    count: int


class RowsResult(TypedDict):
    type: Literal["Rows"]
    rows: ScanResult


class TransactionStartedResult(TypedDict):
    type: Literal["TransactionStarted"]


class TransactionCommittedResult(TypedDict):
    type: Literal["TransactionCommitted"]
    lsn: int


class TransactionRolledBackResult(TypedDict):
    type: Literal["TransactionRolledBack"]


SqlResult = (
    DdlResult
    | RowsAffectedResult
    | RowsResult
    | TransactionStartedResult
    | TransactionCommittedResult
    | TransactionRolledBackResult
)
