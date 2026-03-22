from gnitz._native   import GnitzError, ExprBuilder, ExprProgram, Row
from gnitz._types    import TypeCode, ColumnDef, Schema
from gnitz._batch    import ZSetBatch, ScanResult
from gnitz._client   import Connection
from gnitz._builders import CircuitBuilder, CircuitGraph

GnitzClient = Connection  # backward-compat alias


def connect(socket_path):
    return Connection(socket_path)
