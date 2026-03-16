import gnitz._native as _native
from gnitz._types import TypeCode, _to_native_schema


class Row:
    __slots__ = ("_fields", "_values", "_weight")

    def __init__(self, fields, values, weight):
        object.__setattr__(self, "_fields",  fields)
        object.__setattr__(self, "_values",  values)
        object.__setattr__(self, "_weight",  weight)

    @property
    def weight(self):
        return object.__getattribute__(self, "_weight")

    def __getattr__(self, name):
        fields = object.__getattribute__(self, "_fields")
        values = object.__getattribute__(self, "_values")
        try:
            return values[fields.index(name)]
        except ValueError:
            raise AttributeError(f"Row has no field {name!r}")

    def __getitem__(self, key):
        fields = object.__getattribute__(self, "_fields")
        values = object.__getattribute__(self, "_values")
        if isinstance(key, int):
            return values[key]
        try:
            return values[fields.index(key)]
        except ValueError:
            raise KeyError(key)

    def __iter__(self):
        return iter(object.__getattribute__(self, "_values"))

    def __len__(self):
        return len(object.__getattribute__(self, "_values"))

    def __repr__(self):
        fields = object.__getattribute__(self, "_fields")
        values = object.__getattribute__(self, "_values")
        weight = object.__getattribute__(self, "_weight")
        pairs  = ", ".join(f"{f}={v!r}" for f, v in zip(fields, values))
        return f"Row({pairs}, weight={weight})"

    def __eq__(self, other):
        if isinstance(other, Row):
            return (object.__getattribute__(self, "_values") ==
                    object.__getattribute__(other, "_values"))
        return NotImplemented

    def _asdict(self):
        fields = object.__getattribute__(self, "_fields")
        values = object.__getattribute__(self, "_values")
        return dict(zip(fields, values))

    def _tuple(self):
        return tuple(object.__getattribute__(self, "_values"))


class ScanResult:
    def __init__(self, schema, batch):
        self._schema = schema
        self._batch  = batch

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
            yield Row(fields, tuple(values), batch.weights[i])

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
        self._raw    = _native.ZSetBatch(_to_native_schema(schema))

    def append(self, weight=1, **values):
        schema  = self._schema
        pk_idx  = schema.pk_index
        pk_col  = schema.columns[pk_idx]
        pk_val  = values.get(pk_col.name)

        if pk_val is None:
            raise ValueError(f"Missing primary key column {pk_col.name!r}")

        if pk_col.type_code == TypeCode.U128:
            self._raw.pk_lo.append(pk_val & 0xFFFFFFFFFFFFFFFF)
            self._raw.pk_hi.append((pk_val >> 64) & 0xFFFFFFFFFFFFFFFF)
        else:
            self._raw.pk_lo.append(int(pk_val))
            self._raw.pk_hi.append(0)

        self._raw.weights.append(weight)

        nulls = 0
        for ci, col in enumerate(schema.columns):
            if ci == pk_idx:
                continue
            payload_idx = ci if ci < pk_idx else ci - 1
            val = values.get(col.name)
            if val is None:
                if not col.is_nullable:
                    raise ValueError(f"Non-nullable column {col.name!r} cannot be None")
                nulls |= (1 << payload_idx)
            self._raw.columns[ci].append(val)

        self._raw.nulls.append(nulls)
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
        return len(self._raw.pk_lo)
