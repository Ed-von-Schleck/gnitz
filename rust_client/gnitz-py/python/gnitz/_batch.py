import gnitz._native as _native
from gnitz._types import TypeCode, _to_native_schema

# Row is now a Rust #[pyclass] in _native
Row = _native.Row


class ScanResult:
    def __init__(self, schema, batch, lsn=0):
        self._schema = schema
        self._batch  = batch
        self.lsn     = lsn

    @property
    def batch(self):
        return self._batch

    @property
    def schema(self):
        return self._schema

    def _iter_rows(self):
        if self._batch is None or len(self._batch.pk_lo) == 0:
            return
        schema     = self._schema
        batch      = self._batch
        n          = len(batch.pk_lo)
        fields     = tuple(c.name for c in schema.columns)
        pk_idx     = schema.pk_index
        pk_is_u128 = schema.columns[pk_idx].type_code == TypeCode.U128

        has_nulls = hasattr(batch, 'nulls') and len(batch.nulls) == n

        for i in range(n):
            null_word = batch.nulls[i] if has_nulls else 0
            values = []
            payload_idx = 0
            for ci in range(len(schema.columns)):
                if ci == pk_idx:
                    if pk_is_u128:
                        values.append(batch.pk_lo[i] | (batch.pk_hi[i] << 64))
                    else:
                        values.append(batch.pk_lo[i])
                else:
                    if null_word & (1 << payload_idx):
                        values.append(None)
                    else:
                        values.append(batch.columns[ci][i])
                    payload_idx += 1
            yield Row(fields, tuple(values), weight=batch.weights[i])

    def __iter__(self):
        return self._iter_rows()

    def __len__(self):
        return len(self._batch.pk_lo) if self._batch else 0

    def __bool__(self):
        return len(self) > 0

    def all(self):
        return list(self._iter_rows())

    def first(self):
        for row in self._iter_rows():
            return row
        return None

    def one(self):
        rows = list(self._iter_rows())
        if len(rows) != 1:
            raise ValueError(f"Expected exactly 1 row, got {len(rows)}")
        return rows[0]

    def one_or_none(self):
        rows = list(self._iter_rows())
        if len(rows) > 1:
            raise ValueError(f"Expected at most 1 row, got {len(rows)}")
        return rows[0] if rows else None

    def mappings(self):
        return [r._asdict() for r in self._iter_rows()]

    def scalars(self, col=0):
        return [r[col] for r in self._iter_rows()]


class ZSetBatch:
    def __init__(self, schema):
        self._schema = schema
        self._native_schema = _to_native_schema(schema)
        self._raw    = _native.ZSetBatch(self._native_schema)

    def append(self, weight=1, **values):
        row = [values.get(col.name) for col in self._schema.columns]
        self._raw.append_row(row, weight)
        return self

    def extend(self, rows, weight=1):
        for row in rows:
            w  = row.get("_weight", weight)
            kv = {k: v for k, v in row.items() if k != "_weight"}
            self.append(weight=w, **kv)
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
