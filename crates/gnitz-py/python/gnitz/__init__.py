from gnitz._native   import GnitzError, ExprBuilder, ExprProgram, Row, ScanResult
from gnitz._native   import ColumnDef, Schema
from gnitz._types    import TypeCode
from gnitz._batch    import ZSetBatch
from gnitz._client   import Connection
from gnitz._builders import CircuitBuilder, Circuit
from gnitz._struct   import (Struct, field,
    U8, I8, U16, I16, U32, I32, F32, U64, I64, F64, STRING, U128)

GnitzClient = Connection  # backward-compat alias


def connect(socket_path):
    return Connection(socket_path)
