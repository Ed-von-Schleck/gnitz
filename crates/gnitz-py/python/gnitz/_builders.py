import gnitz._native as _native


class CircuitBuilder:
    def __init__(self, source_table_id):
        self._native = _native.CircuitBuilder(0, source_table_id)

    def input_delta(self):                        return self._native.input_delta()
    def trace_scan(self, table_id):               return self._native.trace_scan(table_id)
    def negate(self, input):                      return self._native.negate(input)
    def union(self, a, b):                        return self._native.union(a, b)
    def delay(self, input):                       return self._native.delay(input)
    def distinct(self, input):                    return self._native.distinct(input)
    def filter(self, input, expr=None):           return self._native.filter(input, expr)
    def map(self, input, projection=None):        return self._native.map(input, projection)
    def map_expr(self, input, expr):              return self._native.map_expr(input, expr)
    def join(self, delta, trace_table_id):        return self._native.join(delta, trace_table_id)
    def anti_join(self, delta, trace_table_id):   return self._native.anti_join(delta, trace_table_id)
    def semi_join(self, delta, trace_table_id):   return self._native.semi_join(delta, trace_table_id)
    def shard(self, input, shard_columns):        return self._native.shard(input, shard_columns)
    def gather(self, input):                      return self._native.gather(input)
    def sink(self, input):                        return self._native.sink(input)

    def reduce(self, input, group_by_cols, agg_func_id=0, agg_col_idx=0):
        """Aggregate input by group_by_cols.

        Note: automatically inserts an EXCHANGE_SHARD node before REDUCE
        for multi-worker correctness. The serialized circuit graph will
        contain both nodes.
        """
        return self._native.reduce(input, group_by_cols, agg_func_id, agg_col_idx)

    def build(self):
        return Circuit(self._native.build())


class Circuit:
    def __init__(self, circuit):
        self._graph = circuit  # _native.Circuit (consumed on create_view_with_circuit)
