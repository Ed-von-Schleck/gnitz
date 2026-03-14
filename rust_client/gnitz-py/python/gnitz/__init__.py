from gnitz._native   import GnitzError, ExprBuilder, ExprProgram
from gnitz._types    import TypeCode, ColumnDef, Schema
from gnitz._batch    import ZSetBatch, Row, ScanResult
from gnitz._client   import Connection
from gnitz._builders import CircuitBuilder, CircuitGraph

GnitzClient = Connection  # backward-compat alias


def connect(socket_path):
    return Connection(socket_path)
