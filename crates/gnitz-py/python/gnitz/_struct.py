"""Declarative schema definitions using type annotations."""

from gnitz._types import TypeCode
from gnitz._native import ColumnDef, Schema


# ── Field metadata ────────────────────────────────────────────

class _FieldSpec:
    __slots__ = ('primary_key',)
    def __init__(self, primary_key):
        self.primary_key = primary_key


def field(*, primary_key=False):
    """Attach metadata to a Struct field."""
    return _FieldSpec(primary_key=primary_key)


# ── Type markers (annotation-only) ───────────────────────────

class U8:     _tc = TypeCode.U8
class I8:     _tc = TypeCode.I8
class U16:    _tc = TypeCode.U16
class I16:    _tc = TypeCode.I16
class U32:    _tc = TypeCode.U32
class I32:    _tc = TypeCode.I32
class F32:    _tc = TypeCode.F32
class U64:    _tc = TypeCode.U64
class I64:    _tc = TypeCode.I64
class F64:    _tc = TypeCode.F64
class STRING: _tc = TypeCode.STRING
class U128:   _tc = TypeCode.U128


# ── Type resolution ───────────────────────────────────────────

def _resolve(name, ann):
    """(type_code, is_nullable) from a type annotation."""
    tc = getattr(ann, '_tc', None)
    if tc is not None:
        return int(tc), False
    # T | None  or  Optional[T]
    args = getattr(ann, '__args__', None)
    if args and len(args) == 2 and type(None) in args:
        other = args[0] if args[1] is type(None) else args[1]
        tc = getattr(other, '_tc', None)
        if tc is not None:
            return int(tc), True
    raise TypeError(
        f"Field '{name}': expected gnitz type (e.g. U64) or "
        f"U64 | None for nullable, got {ann!r}"
    )


# ── Struct base class ─────────────────────────────────────────

class Struct:
    """Declarative schema descriptor.  Subclass to define a table schema."""

    _schema  = None
    _columns = None

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        ann = cls.__annotations__
        if not ann:
            raise TypeError(f"Struct subclass '{cls.__name__}' has no fields")
        cols = []
        pk_idx = 0
        pk_count = 0
        for i, (name, hint) in enumerate(ann.items()):
            tc, nullable = _resolve(name, hint)
            spec = cls.__dict__.get(name)
            is_pk = isinstance(spec, _FieldSpec) and spec.primary_key
            if isinstance(spec, _FieldSpec):
                delattr(cls, name)
            if is_pk:
                pk_idx = i
                pk_count += 1
            cols.append(ColumnDef(name, tc, is_nullable=nullable, primary_key=is_pk))
        if pk_count > 1:
            raise TypeError(
                f"Struct '{cls.__name__}': multiple primary keys are not allowed"
            )
        cls._columns = tuple(cols)
        cls._schema  = Schema(list(cols), pk_index=pk_idx)

    def __init__(self):
        raise TypeError(
            f"{type(self).__name__} is a schema descriptor, not a data class"
        )
