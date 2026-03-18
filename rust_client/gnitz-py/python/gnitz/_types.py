from enum import IntEnum


class TypeCode(IntEnum):
    U8     = 1
    I8     = 2
    U16    = 3
    I16    = 4
    U32    = 5
    I32    = 6
    F32    = 7
    U64    = 8
    I64    = 9
    F64    = 10
    STRING = 11
    U128   = 12


class ColumnDef:
    def __init__(self, name, type_code, is_nullable=False, primary_key=False):
        self.name        = name
        self.type_code   = TypeCode(type_code)
        self.is_nullable = is_nullable
        self.primary_key = primary_key

    def __repr__(self):
        return (f"ColumnDef(name={self.name!r}, type_code={self.type_code!r}, "
                f"is_nullable={self.is_nullable}, primary_key={self.primary_key})")


class Schema:
    def __init__(self, columns, pk_index=None):
        cols = list(columns)
        if pk_index is None:
            pks = [i for i, c in enumerate(cols) if c.primary_key]
            if len(pks) > 1:
                raise ValueError("Multiple primary_key=True columns; specify pk_index explicitly")
            pk_index = pks[0] if len(pks) == 1 else 0
        self.columns  = cols
        self.pk_index = pk_index

    def __repr__(self):
        return f"Schema(pk_index={self.pk_index}, ncols={len(self.columns)})"


import gnitz._native as _native  # noqa: E402  (module-level, after class defs)


def _to_native_col(c):
    return _native.ColumnDef(c.name, int(c.type_code), c.is_nullable)


def _from_native_col(c):
    return ColumnDef(c.name, TypeCode(c.type_code), c.is_nullable)


def _to_native_schema(s):
    return _native.Schema([_to_native_col(c) for c in s.columns], s.pk_index)


def _from_native_schema(s):
    cols = [_from_native_col(c) for c in s.columns]
    cols[s.pk_index].primary_key = True
    return Schema(cols, pk_index=s.pk_index)
