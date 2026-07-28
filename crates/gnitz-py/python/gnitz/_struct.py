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

# One marker class per column type, generated from the enum that already holds
# the names, so a Struct field can name every type a ColumnDef can — including
# UUID, BLOB and I128 — and a type added to the wire needs no edit here.
__all__ = ['Struct', 'field'] + [tc.name for tc in TypeCode]

for _tc in TypeCode:
    globals()[_tc.name] = type(_tc.name, (), {'_tc': _tc})
del _tc


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
        for name, hint in ann.items():
            tc, nullable = _resolve(name, hint)
            spec = cls.__dict__.get(name)
            is_pk = isinstance(spec, _FieldSpec) and spec.primary_key
            if isinstance(spec, _FieldSpec):
                delattr(cls, name)
            cols.append(ColumnDef(name, tc, is_nullable=nullable, primary_key=is_pk))
        cls._columns = tuple(cols)
        # The primary_key flags on the columns say everything: Schema applies the
        # one PK rule (every flagged column in declaration order, else column 0),
        # so a compound key declared here is a compound key, not an error.
        cls._schema  = Schema(list(cols))

    def __init__(self):
        raise TypeError(
            f"{type(self).__name__} is a schema descriptor, not a data class"
        )
