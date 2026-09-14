"""The client-side batch writer: the schema rules gnitz-py owns, the one row
writer both `append` and `extend` resolve their call shape through, the
rollback contract they share, and value coercion at the binding boundary.

The shared rule set lives in gnitz-core and is tested there; what is pinned here
is only what the binding itself decides — the PK-defaulting ladder, the keyword
plan, and how Python values become column bytes.
"""
import inspect
import random
import uuid
from datetime import date, datetime
from decimal import Decimal

import pytest

from gnitz import TypeCode, ColumnDef, Schema, ZSetBatch


# The schema most cases here need. One object backs any number of batches, so a
# per-test copy would say nothing a shared one does not.
KV = Schema([ColumnDef("pk", TypeCode.U64, primary_key=True),
             ColumnDef("val", TypeCode.I64)])

def _col(batch, name):
    """Column `name` of every row written so far."""
    return [r[name] for r in batch.rows()]


def _dump(batch):
    """Every row written so far, hidden columns included, with its weight."""
    return [(tuple(r), r._weight) for r in batch.rows().including_hidden()]


_WRITERS = {"append": lambda b, row: b.append(**row),
            "extend": lambda b, row: b.extend([row])}
_EACH_WRITER = pytest.mark.parametrize("write", list(_WRITERS.values()), ids=list(_WRITERS))


# ---------------------------------------------------------------------------
# Schema construction — the PK rules this binding owns
# ---------------------------------------------------------------------------


class TestSchemaConstruction:

    def test_no_flagged_column_is_rejected(self):
        """A key-less schema is an error, as `CREATE TABLE` without a PRIMARY KEY
        is. The binding infers the PK list from the flags and nothing else — it
        does not invent one, which would key a table on whichever column the
        caller happened to declare first."""
        with pytest.raises(ValueError):
            Schema([ColumnDef("a", TypeCode.U64), ColumnDef("b", TypeCode.I64)])

    def test_the_pk_list_is_inferred_from_the_flags_in_declaration_order(self):
        assert Schema([ColumnDef("a", TypeCode.U64),
                       ColumnDef("b", TypeCode.I64, primary_key=True),
                       ColumnDef("c", TypeCode.U32, primary_key=True)]).pk_indices == [1, 2]

    def test_explicit_pk_indices_override_the_flag_and_keep_their_order(self):
        """pk_indices defines sort order — not just set membership."""
        cols = [ColumnDef("a", TypeCode.U64, primary_key=True),
                ColumnDef("b", TypeCode.U32),
                ColumnDef("v", TypeCode.I64)]
        assert Schema(cols, pk_indices=[1, 0]).pk_indices == [1, 0]

    def test_empty_column_list_rejected(self):
        with pytest.raises(ValueError, match="at least 1 column"):
            Schema([])

    def test_core_rule_violation_surfaces_as_value_error(self):
        """The shared rule set is enforced in gnitz-core; what this pins is that
        its rejection reaches Python as a ValueError rather than a panic or a
        silently accepted schema."""
        cols = [ColumnDef("pk", TypeCode.U64, is_nullable=True, primary_key=True),
                ColumnDef("v",  TypeCode.I64)]
        with pytest.raises(ValueError, match="nullable"):
            Schema(cols)


# ---------------------------------------------------------------------------
# `append`'s resolved keyword plan
# ---------------------------------------------------------------------------

# `append` resolves a call site's keyword-name tuple into a write plan once and
# reuses it. The tests below pin what that caching must not change.

_PK_TYPES = [TypeCode.U8, TypeCode.I8, TypeCode.U16, TypeCode.I16,
             TypeCode.U32, TypeCode.I32, TypeCode.U64, TypeCode.I64,
             TypeCode.U128, TypeCode.UUID, TypeCode.I128]
_PAYLOAD_ONLY_TYPES = [TypeCode.F32, TypeCode.F64, TypeCode.STRING, TypeCode.BLOB]
_ALL_TYPES = _PK_TYPES + _PAYLOAD_ONLY_TYPES

_RANGES = {
    TypeCode.U8:  (0, 255),          TypeCode.I8:  (-128, 127),
    TypeCode.U16: (0, 65535),        TypeCode.I16: (-32768, 32767),
    TypeCode.U32: (0, 2**32 - 1),    TypeCode.I32: (-(2**31), 2**31 - 1),
    TypeCode.U64: (0, 2**64 - 1),    TypeCode.I64: (-(2**63), 2**63 - 1),
}


def _rand_value(rng, tc):
    if tc in _RANGES:
        return rng.randint(*_RANGES[tc])
    if tc in (TypeCode.U128, TypeCode.UUID):
        return rng.getrandbits(128)
    if tc is TypeCode.I128:
        return rng.randint(-(2**127), 2**127 - 1)
    if tc in (TypeCode.F32, TypeCode.F64):
        return rng.uniform(-1e6, 1e6)
    if tc is TypeCode.STRING:
        return "s%d" % rng.randint(0, 10**6)
    if tc is TypeCode.BLOB:
        return bytes(rng.randrange(256) for _ in range(rng.randint(0, 5)))
    raise AssertionError("unhandled type code %r" % (tc,))


class TestAppendKeywordPlan:

    def test_matches_dict_path_across_random_schemas(self):
        """`append(**row)` and `extend([row])` resolve one write plan from two
        sources — a keyword-name tuple and a dict's key walk. They must produce
        byte-identical batches for any schema, and between them the two paths
        cover every type code."""
        rng = random.Random(20260729)
        seen = set()
        for _ in range(2000):
            ncols = rng.randint(2, 8)
            npk = rng.randint(1, min(3, ncols - 1))
            spec = []
            for i in range(ncols):
                is_pk = i < npk
                tc = rng.choice(_PK_TYPES if is_pk else _ALL_TYPES)
                seen.add(tc)
                spec.append(("c%d" % i, tc, not is_pk and rng.random() < 0.4))
            schema = Schema([ColumnDef(n, tc, is_nullable=nul) for n, tc, nul in spec],
                            pk_indices=list(range(npk)))

            row = {}
            for name, tc, nullable in spec:
                if nullable and rng.random() < 0.35:
                    if rng.random() < 0.5:
                        continue                    # omitted -> NULL
                    row[name] = None                # explicit None -> NULL
                else:
                    row[name] = _rand_value(rng, tc)
            keys = list(row)
            rng.shuffle(keys)
            row = {k: row[k] for k in keys}
            weight = rng.choice([1, -1, 7])

            got = ZSetBatch(schema)
            got.append(_weight=weight, **row)
            ref = ZSetBatch(schema)
            ref.extend([dict(row, _weight=weight)])
            assert _dump(got) == _dump(ref)
            assert [w for _, w in _dump(got)] == [weight]
        assert seen == set(_ALL_TYPES)

    def test_cycling_shapes_keeps_every_row_correct(self):
        """A batch caches the last call shape's resolved plan; both writers
        resolve against it, `append` by keyword-tuple pointer and `extend` by a
        dict's key walk. Sparse, shuffled, mixed-`_weight` rows make it rebuild
        constantly: no row may be written through the plan another row resolved,
        and a row carrying no `_weight` takes its writer's default rather than
        the previous row's weight. The two writers must also agree column for
        column over the same run."""
        cols = [ColumnDef("pk", TypeCode.U64, primary_key=True)] + [
            ColumnDef("c%d" % i, TypeCode.I64, is_nullable=True) for i in range(5)]
        schema = Schema(cols)
        names = [c.name for c in cols[1:]]

        rng = random.Random(11)
        rows = []
        for pk in range(500):
            present = rng.sample(names, rng.randint(0, len(names)))
            row = dict({"pk": pk}, **{n: pk * 10 + int(n[1:]) for n in present})
            if rng.random() < 0.4:
                row["_weight"] = rng.choice([-1, 2, 7])
            keys = list(row)
            rng.shuffle(keys)
            rows.append({k: row[k] for k in keys})

        by_append = ZSetBatch(schema)
        for row in rows:
            by_append.append(**row)
        by_extend = ZSetBatch(schema).extend(rows, 3)

        appended, extended = list(by_append.rows()), list(by_extend.rows())
        assert [r.pk for r in appended] == [r.pk for r in extended] == [r["pk"] for r in rows]
        assert [r._weight for r in appended] == [r.get("_weight", 1) for r in rows]   # append's default
        assert [r._weight for r in extended] == [r.get("_weight", 3) for r in rows]   # extend's batch-wide
        for name in names:
            expected = [r.get(name) for r in rows]
            assert [r[name] for r in appended] == [r[name] for r in extended] == expected

    def test_weight_keyword_yields_to_a_column_of_that_name(self):
        """`_weight` names the row weight only when no column claims it. The
        schema is the authority, and both writers have to agree on that."""
        schema = Schema([
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("_weight", TypeCode.I64),
        ])
        got = ZSetBatch(schema).append(pk=1, _weight=5)
        ref = ZSetBatch(schema).extend([{"pk": 1, "_weight": 5}])
        assert _dump(got) == _dump(ref) == [((1, 5), 1)]
        # The batch-wide parameter still reaches such a schema.
        assert _dump(ZSetBatch(schema).extend([{"pk": 1, "_weight": 5}], -1)) == [((1, 5), -1)]

    def test_positional_argument_rejected(self):
        with pytest.raises(TypeError, match="positional"):
            ZSetBatch(KV).append(1, val=10)

    def test_bad_weight_names_the_argument(self):
        """The failing argument is named in the exception's PEP 678 notes rather
        than its message — that is where the binding layer puts argument context,
        so a note-less exception would leave the caller with no idea which
        keyword was at fault."""
        with pytest.raises(TypeError) as exc:
            ZSetBatch(KV).append(pk=1, val=10, _weight=1.5)
        assert any("_weight" in n for n in exc.value.__notes__)

    def test_signature_is_introspectable(self):
        """There are no .pyi stubs, so the method's own text signature is the
        entire discoverability surface for the `_weight` spelling. A malformed
        `__text_signature__` on the raw method slot makes this call raise."""
        assert "_weight" in inspect.signature(ZSetBatch.append).parameters


# ---------------------------------------------------------------------------
# Row-level errors and the rollback contract
# ---------------------------------------------------------------------------


class TestRowErrors:

    @_EACH_WRITER
    def test_a_bad_row_raises_and_writes_nothing(self, write):
        """The rollback contract both writers share.

        Each refusal is distinct — a PK with no value, a NULL PK, a None in a
        NOT NULL column, a name no column holds, and a type error on a *later*
        column (so the PK is already appended and must be rolled back) — and
        after every one of them the batch is back to its pre-call length and
        still usable. `weight=` in particular must not slip past as a no-op that
        writes the row at +1.
        """
        schema = Schema([ColumnDef("pk", TypeCode.U64, primary_key=True),
                         ColumnDef("a", TypeCode.I64),
                         ColumnDef("b", TypeCode.I64)])
        batch = ZSetBatch(schema)
        assert write(batch, {"pk": 1, "a": 1, "b": 1}) is batch      # chainable

        for exc, match, row in [
            (ValueError, "pk",      {"a": 1, "b": 1}),               # missing PK
            (ValueError, "pk",      {"pk": None, "a": 1, "b": 1}),
            (ValueError, "a",       {"pk": 2, "a": None, "b": 1}),
            (TypeError,  "_weight", {"pk": 2, "a": 1, "b": 1, "weight": -1}),
            (TypeError,  None,      {"pk": 2, "a": 1, "b": "not-an-int"}),
        ]:
            with pytest.raises(exc, match=match):
                write(batch, row)
            assert _col(batch, "pk") == [1]

        write(batch, {"pk": 2, "a": 2, "b": 2})                      # still usable
        assert _col(batch, "pk") == [1, 2]

    @_EACH_WRITER
    def test_a_reentrant_write_raises_instead_of_aborting(self, write):
        """`extract` runs `__index__`, which can call back into the same batch.
        The raw method slot's `try_borrow_mut` is what catches that, as a
        `RuntimeError` — not a panic, which under `panic = "abort"` would take the
        interpreter down instead of failing the call."""
        batch = ZSetBatch(KV)

        class Reenter:
            def __index__(self):
                write(batch, {"pk": 99, "val": 99})
                return 5

        with pytest.raises(RuntimeError):
            write(batch, {"pk": 1, "val": Reenter()})
        assert len(batch) == 0
        assert _col(write(batch, {"pk": 2, "val": 2}), "pk") == [2]       # still usable

    def test_extend_is_atomic_across_rows(self):
        """`extend` is all-or-nothing: a bad row rolls back the good rows queued
        before it in the same call, not just itself."""
        batch = ZSetBatch(KV).append(pk=1, val=10)
        with pytest.raises(TypeError):
            batch.extend([{"pk": 2, "val": 20}, {"pk": 3, "val": "not-an-int"}])
        assert _col(batch, "pk") == [1]
        assert _col(batch.extend([{"pk": 4, "val": 40}]), "pk") == [1, 4]

    def test_generator_rows_match_a_list_of_the_same_rows(self):
        """A generator has no `__len__`, so `extend` cannot pre-size for it.
        Pre-sizing must be invisible in the result."""
        rows = [{"pk": i, "val": i * 10} for i in range(5)]
        from_list = ZSetBatch(KV).extend(rows)
        from_gen = ZSetBatch(KV).extend(dict(r) for r in rows)
        assert _dump(from_gen) == _dump(from_list)


def test_a_dropped_column_is_a_tombstone_the_writer_fills(client, schema_name):
    """`DROP COLUMN` flips `is_hidden` and nothing else, so a dropped NOT NULL
    column stays NOT NULL in the schema `resolve_table` hands back, and a re-ADD
    reuses its name — a schema off the wire can carry a hidden and a visible
    column of one name.

    Both writers resolve through the same schema walk: the visible column takes
    the supplied value, the tombstone takes the zero filler the SQL writer
    pushes, it is not presented on the way back, and a name that is nobody's
    column is still an error.
    """
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, "
        "a BIGINT NOT NULL, b BIGINT NOT NULL)", schema_name=schema_name)
    client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=schema_name)
    client.execute_sql("ALTER TABLE t ADD COLUMN a BIGINT", schema_name=schema_name)
    tid, schema = client.resolve_table(schema_name, "t")
    assert [(c.name, c.is_hidden) for c in schema.columns] == [
        ("id", False), ("a", True), ("b", False), ("a", False)]

    batch = ZSetBatch(schema).append(id=1, a=10, b=100)
    batch.extend([{"id": 2, "a": 20, "b": 200}])
    assert _dump(batch) == [((1, 0, 100, 10), 1), ((2, 0, 200, 20), 1)]
    client.push(tid, batch)

    rows = list(client.scan(tid))
    assert sorted((r.id, r.a, r.b, r._weight) for r in rows) == [
        (1, 10, 100, 1), (2, 20, 200, 1)]
    assert all(r._fields == ("id", "b", "a") for r in rows)
    with pytest.raises(TypeError, match="unexpected column name"):
        ZSetBatch(schema).extend([{"id": 3, "a": 30, "b": 300, "nosuch": 1}])


# ---------------------------------------------------------------------------
# Value coercion at the binding boundary
# ---------------------------------------------------------------------------


class TestValueCoercion:

    def test_a_uuid_column_takes_every_spelling_of_one_value(self):
        """Canonical hyphenated text, bare 32-hex and a `uuid.UUID` object are
        three renderings of one UUID. All three must reach a UUID column, PK or
        payload — the two go through one encoder — and land on the same value,
        presented back as canonical hyphenated text. Text that spells no UUID is
        refused."""
        canonical = "12345678-1234-5678-1234-567812345678"
        schema = Schema([ColumnDef("id", TypeCode.UUID, primary_key=True),
                         ColumnDef("v", TypeCode.UUID)])
        for form in (canonical, canonical.replace("-", ""), uuid.UUID(canonical)):
            batch = ZSetBatch(schema).append(id=form, v=form)
            assert _col(batch, "id") == [canonical]
            assert _col(batch, "v") == [canonical]
        with pytest.raises(ValueError):
            ZSetBatch(schema).append(id="not-a-uuid-string", v=canonical)

    def test_u128_column_rejects_text(self):
        """A string spells a value only for the types that admit one, and U128 is
        not among them — SQL reads a string literal into a U128 column as an
        error, so `append` must too, rather than accepting bare hex here alone.
        The same rule excludes a `uuid.UUID` object, which is a UUID, not a
        number."""
        schema = Schema([ColumnDef("k", TypeCode.U128, primary_key=True),
                         ColumnDef("v", TypeCode.U128, is_nullable=True)])
        for bad in ("f" * 32, uuid.uuid4()):
            for row in ({"k": bad, "v": 1}, {"k": 1, "v": bad}):
                with pytest.raises(TypeError):
                    ZSetBatch(schema).append(**row)

    def test_u128_keeps_its_full_unsigned_range(self):
        """A U128 value above `i128::MAX` is legal and must not be rejected as a
        signed overflow."""
        u128_max = (1 << 128) - 1
        schema = Schema([ColumnDef("k", TypeCode.U128, primary_key=True),
                         ColumnDef("v", TypeCode.U128)])
        batch = ZSetBatch(schema).append(k=u128_max, v=u128_max)
        assert _col(batch, "k") == [u128_max]
        assert _col(batch, "v") == [u128_max]

    def test_i128_keeps_its_full_signed_range(self):
        """The signed twin, and the half no join can reach: a matched join key
        always lands in the two operands' non-negative overlap, so the negative
        half of the 128-bit space is only addressable through this path. A key
        decoded as unsigned comes back as `2**128 - 1` where `-1` was written."""
        values = [-(2 ** 127), -1, 0, 1, (2 ** 127) - 1]
        schema = Schema([ColumnDef("k", TypeCode.I128, primary_key=True),
                         ColumnDef("v", TypeCode.I128)])
        batch = ZSetBatch(schema)
        for v in values:
            batch.append(k=v, v=v)
        assert _col(batch, "k") == values
        assert _col(batch, "v") == values

    def test_bytes_and_text_columns_stay_apart(self):
        """BLOB and STRING share one region form, so the extraction is where the
        two kinds stay separate: BLOB takes bytes and refuses `str`; STRING takes
        `str` and refuses bytes — nothing downstream would reject non-UTF-8, so
        this is where TEXT stays text."""
        schema = Schema([ColumnDef("pk", TypeCode.U64, primary_key=True),
                         ColumnDef("payload", TypeCode.BLOB),
                         ColumnDef("s", TypeCode.STRING)])
        batch = ZSetBatch(schema).append(pk=1, payload=b"hello\x00world", s="hi")
        assert _col(batch, "payload") == [b"hello\x00world"]
        assert _col(batch, "s") == ["hi"]

        for bad in ({"payload": "not bytes", "s": "hi"},
                    {"payload": b"", "s": b"\xff\xfe not utf-8"}):
            with pytest.raises(TypeError):
                ZSetBatch(schema).append(pk=2, **bad)

    def test_a_bad_compound_key_rolls_the_half_packed_key_back(self):
        """A compound key is packed column by column, so a refusal on the
        *second* one leaves bytes already in the key scratch. Both refusals — a
        NULL and an omitted column — must roll it back rather than leave a
        half-written key for the next row to inherit."""
        schema = Schema([ColumnDef("a", TypeCode.U64),
                         ColumnDef("b", TypeCode.U32),
                         ColumnDef("v", TypeCode.I64)], pk_indices=[0, 1])
        for row in ({"a": 7, "b": None, "v": 1}, {"b": 9, "v": 1}):
            batch = ZSetBatch(schema)
            with pytest.raises(ValueError):
                batch.append(**row)
            assert len(batch) == 0
            batch.append(a=1, b=2, v=3)        # the scratch is clean
            assert [(r.a, r.b, r.v) for r in batch.rows()] == [(1, 2, 3)]

    def test_a_decimal_column_takes_every_spelling_of_one_value(self):
        """A DECIMAL is a scaled integer, so the binding has to scale whatever it
        is handed: a `Decimal`, an `int`, a `float`, a string, and a `Decimal` in
        exponent form all name a value at the column's scale. A longer fraction
        rounds half away from zero rather than truncating, which is the rule the
        SQL literal path also follows.

        The refusals are the two ways a value has no scaled image: text that is
        not a number, and a magnitude past the `i64` behind the scale.
        """
        schema = Schema([ColumnDef("id", TypeCode.I64, primary_key=True),
                         ColumnDef("v", TypeCode.DECIMAL, scale=3)])
        assert schema.columns[1].scale == 3, "a client-authored schema carries the scale"

        batch = ZSetBatch(schema)
        for i, v in enumerate([Decimal("12.5"), 3, 1.1, "2.2505", Decimal("1E+2")], 1):
            batch.append(id=i, v=v)
        assert _col(batch, "v") == [Decimal("12.500"), Decimal("3.000"),
                                    Decimal("1.100"), Decimal("2.251"),
                                    Decimal("100.000")]

        with pytest.raises(ValueError):
            ZSetBatch(schema).append(id=9, v="abc")
        with pytest.raises(OverflowError):
            ZSetBatch(schema).append(id=9, v=10**16)

    def test_a_temporal_column_takes_an_object_or_its_stored_integer(self):
        """DATE and TIMESTAMP are a day count and a microsecond count, so the
        binding accepts either the calendar object or the integer itself, and the
        two must land on the same value. A `datetime` reaching a DATE column
        keeps its date, and a `date` reaching a TIMESTAMP column starts its day.

        Both refusals are about a value that looks convertible and is not: an
        aware `datetime` carries an offset the column cannot store, and a string
        is the SQL literal grammar rather than the builder's.
        """
        d = date(2024, 2, 29)
        ts = datetime(2024, 2, 29, 13, 45, 7, 250_000)
        epoch_days = (d - date(1970, 1, 1)).days
        schema = Schema([ColumnDef("id", TypeCode.I64, primary_key=True),
                         ColumnDef("d", TypeCode.DATE),
                         ColumnDef("ts", TypeCode.TIMESTAMP)])

        batch = ZSetBatch(schema)
        batch.append(id=1, d=d, ts=ts)
        batch.append(id=2, d=epoch_days, ts=epoch_days * 86_400_000_000)
        batch.append(id=3, d=ts, ts=d)
        assert _col(batch, "d") == [d, d, d]
        assert _col(batch, "ts") == [ts, datetime(2024, 2, 29), datetime(2024, 2, 29)]

        with pytest.raises(ValueError, match="naive"):
            ZSetBatch(schema).append(id=9, d=d, ts=datetime.now().astimezone())
        with pytest.raises(TypeError):
            ZSetBatch(schema).append(id=9, d="2024-02-29", ts=ts)
