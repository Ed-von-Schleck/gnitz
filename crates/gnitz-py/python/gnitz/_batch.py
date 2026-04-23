import gnitz._native as _native

# Row is now a Rust #[pyclass] in _native
Row = _native.Row


class ZSetBatch:
    def __init__(self, schema):
        if isinstance(schema, type) and hasattr(schema, '_schema'):
            schema = schema._schema
        self._schema = schema
        self._raw    = _native.ZSetBatch(schema)

    def append(self, weight=1, **values):
        self._raw.append_dict(values, weight)
        return self

    def extend(self, rows, weight=1):
        self._raw.extend_from_dicts(rows, weight)
        return self

    @property
    def pk_lo(self):   return self._raw.pk_lo

    @property
    def pk_hi(self):   return self._raw.pk_hi

    @property
    def weights(self): return self._raw.weights

    @property
    def nulls(self):   return self._raw.nulls

    @property
    def columns(self): return self._raw.columns

    def __len__(self):
        return len(self._raw)
