"""The client-side batch writer: the schema rules gnitz-py owns, the two row
writers (`append`'s resolved keyword plan and `extend`'s dict walk) and the
rollback contract they share, and value coercion at the binding boundary.

The shared rule set lives in gnitz-core and is tested there; what is pinned here
is only what the binding itself decides — the PK-defaulting ladder, the keyword
plan, and how Python values become column bytes.
"""
import inspect
import random
import uuid

import pytest

from gnitz import TypeCode, ColumnDef, Schema, ZSetBatch
from _uid import uid as _uid




def _cleanup(client, sn, *names):
    for name in names:
        try:
            client.drop_table(sn, name)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _make_table(client):
    """Create a (pk U64 PK, val I64) table. Returns (sn, tid, schema)."""
    sn = "zb" + _uid()
    client.create_schema(sn)
    cols = [ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("val", TypeCode.I64)]
    tid = client.create_table(sn, "t", cols)
    return sn, tid, Schema(cols)


# ---------------------------------------------------------------------------
# Schema construction — the PK rules this binding owns
# ---------------------------------------------------------------------------


class TestSchemaConstruction:

    def test_no_flagged_column_is_rejected(self):
        """A key-less schema is an error, as `CREATE TABLE` without a PRIMARY KEY
        is. The binding infers the PK list from the flags and nothing else — it
        does not invent one, which would key a table on whichever column the
        caller happened to declare first."""
        cols = [ColumnDef("a", TypeCode.U64),
                ColumnDef("b", TypeCode.I64)]
        with pytest.raises(ValueError):
            Schema(cols)

    def test_pk_inferred_from_flagged_columns_in_declaration_order(self):
        cols = [ColumnDef("a", TypeCode.U64),
                ColumnDef("b", TypeCode.I64, primary_key=True)]
        assert Schema(cols).pk_indices == [1]

    def test_multiple_flags_infer_a_compound_pk(self):
        cols = [ColumnDef("a", TypeCode.U64, primary_key=True),
                ColumnDef("b", TypeCode.I64, primary_key=True)]
        assert Schema(cols).pk_indices == [0, 1]

    def test_explicit_pk_indices_override_the_flag(self):
        cols = [ColumnDef("a", TypeCode.U64, primary_key=True),
                ColumnDef("b", TypeCode.I64),
                ColumnDef("c", TypeCode.I64)]
        assert Schema(cols, pk_indices=[2]).pk_indices == [2]

    def test_pk_indices_order_is_preserved(self):
        """pk_indices defines sort order — not just set membership."""
        cols = [ColumnDef("a", TypeCode.U64),
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

    def test_max_arity_wide_pk_accepted(self):
        # The widest FFI PK is PK_LIST_MAX_COLS (4) columns; 4 × U128 = 64 bytes,
        # within MAX_PK_BYTES (80), so the max-arity wide PK round-trips through
        # validation. The PK-stride ceiling is unreachable from this surface: the
        # 4-column cap bounds a U128 PK at 64 bytes, below the 80-byte limit.
        cols = [ColumnDef(f"c{i}", TypeCode.U128) for i in range(4)]
        Schema(cols, pk_indices=list(range(4)))


class TestColumnDefCoercion:

    def test_raw_int_type_code_coerced(self):
        """Every other call site passes a TypeCode; a raw wire code must work
        too, since that is what a schema read back off the wire carries."""
        col = ColumnDef("x", 8)
        assert TypeCode(col.type_code) is TypeCode.U64

    def test_flag_defaults(self):
        col = ColumnDef("x", TypeCode.U64)
        assert col.is_nullable is False
        assert col.primary_key is False
        assert col.is_hidden is False


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
        """`append(**row)` and `extend([row])` are separate writers — one reads a
        resolved keyword plan, the other looks each column up in a dict. They must
        produce byte-identical batches for any schema, and between them the two
        paths cover every type code."""
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
            assert got.pks == ref.pks
            assert got.columns == ref.columns
            assert got.weights == ref.weights == [weight]
        assert seen == set(_ALL_TYPES)

    def test_cycling_call_sites_keeps_every_row_correct(self):
        """Rows arrive through a cache of resolved call sites. Cycling far more
        keyword shapes than that cache holds must not let one row's plan write
        another's row."""
        cols = [ColumnDef("pk", TypeCode.U64, primary_key=True)] + [
            ColumnDef("c%d" % i, TypeCode.I64, is_nullable=True) for i in range(5)]
        schema = Schema(cols)
        names = [c.name for c in cols[1:]]

        rng = random.Random(7)
        rows = []
        for pk in range(500):
            present = rng.sample(names, rng.randint(0, len(names)))
            rng.shuffle(present)
            rows.append(dict({"pk": pk}, **{n: pk * 10 + int(n[1:]) for n in present}))

        batch = ZSetBatch(schema)
        for row in rows:
            batch.append(**row)
        assert batch.pks == [r["pk"] for r in rows]
        for col_i, name in enumerate(names, start=1):
            assert batch.columns[col_i] == [r.get(name) for r in rows]

    def test_weight_keyword_yields_to_a_column_of_that_name(self):
        """`_weight` names the row weight only when no column claims it. The
        schema is the authority, and both writers have to agree on that."""
        schema = Schema([
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("_weight", TypeCode.I64),
        ])
        got = ZSetBatch(schema).append(pk=1, _weight=5)
        ref = ZSetBatch(schema).extend([{"pk": 1, "_weight": 5}])
        assert got.columns == ref.columns == [[], [5]]
        assert got.weights == ref.weights == [1]
        # The batch-wide parameter still reaches such a schema.
        assert ZSetBatch(schema).extend([{"pk": 1, "_weight": 5}], -1).weights == [-1]

    def test_one_name_feeds_every_visible_column_that_has_it(self):
        """A hidden column may shadow a visible one's name. The plan is resolved
        by walking the schema, so every *visible* column carrying the name gets
        the value — resolving name->column instead would leave a second visible
        one NULL, or fail outright were it NOT NULL.

        A hidden payload column takes neither: it is a DROP COLUMN tombstone, so
        it takes the same zero filler the SQL writer pushes, whatever the caller
        supplied under its name.
        """
        schema = Schema([
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("v",  TypeCode.I64),
            ColumnDef("v",  TypeCode.I64, is_hidden=True),
        ])
        batch = ZSetBatch(schema)
        batch.append(pk=1, v=7)
        assert batch.columns == [[], [7], [0]]

    def test_unknown_keyword_raises_type_error_and_rolls_back(self):
        """An unrecognised keyword is an error, not a value the writer absorbs:
        `weight=` must not slip past as a no-op that writes the row at +1."""
        schema = Schema([
            ColumnDef("pk",  TypeCode.U64, primary_key=True),
            ColumnDef("val", TypeCode.I64),
        ])
        batch = ZSetBatch(schema).append(pk=1, val=10)
        with pytest.raises(TypeError, match="_weight"):
            batch.append(pk=2, val=20, weight=-1)
        assert len(batch) == 1
        assert batch.pks == [1]

    def test_positional_argument_rejected(self):
        schema = Schema([
            ColumnDef("pk",  TypeCode.U64, primary_key=True),
            ColumnDef("val", TypeCode.I64),
        ])
        with pytest.raises(TypeError, match="positional"):
            ZSetBatch(schema).append(1, val=10)

    def test_bad_weight_names_the_argument(self):
        """The failing argument is named in the exception's PEP 678 notes rather
        than its message — that is where the binding layer puts argument context,
        so a note-less exception would leave the caller with no idea which
        keyword was at fault."""
        schema = Schema([
            ColumnDef("pk",  TypeCode.U64, primary_key=True),
            ColumnDef("val", TypeCode.I64),
        ])
        with pytest.raises(TypeError) as exc:
            ZSetBatch(schema).append(pk=1, val=10, _weight=1.5)
        assert any("_weight" in n for n in exc.value.__notes__)

    def test_reentrant_append_raises_instead_of_aborting(self):
        """`extract` runs `__index__`, which can call back into the same batch.
        The raw method slot's `try_borrow_mut` is what catches that, as a
        `RuntimeError` — not a panic, which under `panic = "abort"` would take the
        interpreter down instead of failing the call."""
        schema = Schema([
            ColumnDef("pk",  TypeCode.U64, primary_key=True),
            ColumnDef("val", TypeCode.I64),
        ])
        batch = ZSetBatch(schema)

        class Reenter:
            def __index__(self):
                batch.append(pk=99, val=99)
                return 5

        with pytest.raises(RuntimeError):
            batch.append(pk=1, val=Reenter())
        assert len(batch) == 0
        batch.append(pk=2, val=2)          # still usable
        assert batch.pks == [2]

    def test_signature_is_introspectable(self):
        """There are no .pyi stubs, so the method's own text signature is the
        entire discoverability surface for the `_weight` spelling. A malformed
        `__text_signature__` on the raw method slot makes this call raise."""
        sig = inspect.signature(ZSetBatch.append)
        assert "_weight" in sig.parameters


# ---------------------------------------------------------------------------
# Row-level errors and the rollback contract
# ---------------------------------------------------------------------------


class TestAppendErrors:

    def _schema(self, nullable_val=False):
        return Schema([
            ColumnDef("pk",  TypeCode.U64, primary_key=True),
            ColumnDef("val", TypeCode.I64, is_nullable=nullable_val),
        ])

    def test_missing_pk_raises(self):
        batch = ZSetBatch(self._schema())
        with pytest.raises(ValueError, match="pk"):
            batch.append(val=10)

    def test_pk_none_raises_and_writes_nothing(self):
        batch = ZSetBatch(self._schema())
        with pytest.raises(ValueError, match="pk"):
            batch.append(pk=None, val=1)
        assert len(batch) == 0

    def test_none_into_a_non_nullable_column_raises(self):
        batch = ZSetBatch(self._schema(nullable_val=False))
        with pytest.raises(ValueError, match="val"):
            batch.append(pk=1, val=None)

    def test_append_returns_self(self):
        batch = ZSetBatch(self._schema())
        assert batch.append(pk=1, val=5) is batch

    def test_partial_row_rolled_back_on_error(self):
        """A type error on a later column must not leave a half-written row.

        With three columns the failure happens on the second payload column;
        the batch must come back to length 0 (PK already appended, then
        rolled back) and remain reusable.
        """
        schema = Schema([
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("a",  TypeCode.I64),
            ColumnDef("b",  TypeCode.I64),
        ])
        batch = ZSetBatch(schema)
        with pytest.raises(Exception):
            batch.append(pk=1, a=10, b="not-an-int")
        assert len(batch) == 0
        # Reusing the batch with valid values still works.
        batch.append(pk=1, a=10, b=20)
        assert len(batch) == 1


class TestExtend:

    def _schema(self):
        return Schema([
            ColumnDef("pk",  TypeCode.U64, primary_key=True),
            ColumnDef("val", TypeCode.I64),
        ])

    def test_extend_returns_self(self):
        batch = ZSetBatch(self._schema())
        assert batch.extend([{"pk": 1, "val": 5}]) is batch

    def test_rejects_unknown_key(self):
        batch = ZSetBatch(self._schema())
        with pytest.raises(TypeError, match="_weight"):
            batch.extend([{"pk": 1, "val": 10, "weight": -1}])
        assert len(batch) == 0

    def test_is_atomic_on_error(self):
        """`extend` is all-or-nothing: a failure mid-iteration rolls the whole
        batch back to its pre-call length, matching the single-row appends.

        Pre-append one valid row, then extend with [good, bad] where `bad`
        carries a type error on a later row. The good row gets queued before
        `bad` fails, so per-row rollback alone would leave the batch at length 2
        (1 pre-existing + 1 from the partial extend). Batch-level rollback must
        bring it back to the pre-extend length of 1.
        """
        batch = ZSetBatch(self._schema())
        batch.append(pk=1, val=10)
        assert len(batch) == 1
        with pytest.raises(Exception):
            batch.extend([{"pk": 2, "val": 20}, {"pk": 3, "val": "not-an-int"}])
        assert len(batch) == 1
        # The batch stays reusable after the rolled-back extend.
        batch.extend([{"pk": 4, "val": 40}])
        assert len(batch) == 2

    def test_list_of_dicts_round_trip(self, client):
        sn, tid, schema = _make_table(client)
        try:
            batch = ZSetBatch(schema)
            batch.extend([{"pk": 1, "val": 10}, {"pk": 2, "val": 20}])
            client.push(tid, batch)
            result = client.scan(tid)
            assert len(result) == 2
            assert {row.pk: row.val for row in result} == {1: 10, 2: 20}
        finally:
            _cleanup(client, sn, "t")

    def test_cycling_shapes_through_extend_keeps_every_row_correct(self):
        """`extend` resolves a plan from each row's key sequence and reuses it
        while the sequence holds. Sparse, shuffled, mixed-`_weight` rows through
        one batch make it rebuild constantly: no row may be written through the
        plan another row resolved, and a row that carries no `_weight` must take
        the batch-wide default rather than the previous row's weight."""
        cols = [ColumnDef("pk", TypeCode.U64, primary_key=True)] + [
            ColumnDef("c%d" % i, TypeCode.I64, is_nullable=True) for i in range(5)]
        schema = Schema(cols)
        names = [c.name for c in cols[1:]]

        rng = random.Random(11)
        rows = []
        for pk in range(500):
            present = rng.sample(names, rng.randint(0, len(names)))
            rng.shuffle(present)
            row = dict({"pk": pk}, **{n: pk * 10 + int(n[1:]) for n in present})
            if rng.random() < 0.4:
                row["_weight"] = rng.choice([-1, 2, 7])
            keys = list(row)
            rng.shuffle(keys)
            rows.append({k: row[k] for k in keys})

        batch = ZSetBatch(schema)
        batch.extend(rows, 3)
        assert len(batch) == len(rows)
        assert batch.pks == [r["pk"] for r in rows]
        assert batch.weights == [r.get("_weight", 3) for r in rows]
        for col_i, name in enumerate(names, start=1):
            assert batch.columns[col_i] == [r.get(name) for r in rows]

    def test_extend_over_a_duplicate_name_schema(self, client):
        """A schema read back off the wire can carry two columns of one name: a
        DROP COLUMN leaves its slot behind, hidden, and a re-ADD reuses the name.
        `extend` resolves through the same schema walk `append` does, so the
        supplied value reaches the visible column and the tombstone takes its
        filler — and a name that is nobody's column is still an error."""
        sn = "zb" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=sn)
            client.execute_sql("ALTER TABLE t ADD COLUMN a BIGINT", schema_name=sn)
            tid, schema = client.resolve_table(sn, "t")
            assert [c.name for c in schema.columns].count("a") == 2

            batch = ZSetBatch(schema)
            batch.extend([{"id": 1, "a": 10}, {"id": 2, "a": 20}])
            client.push(tid, batch)
            assert {r.id: r.a for r in client.scan(tid)} == {1: 10, 2: 20}

            with pytest.raises(TypeError, match="_weight"):
                ZSetBatch(schema).extend([{"id": 3, "a": 30, "nosuch": 1}])
        finally:
            _cleanup(client, sn, "t")

    def test_push_after_dropping_a_not_null_column(self, client):
        """`DROP COLUMN` flips `is_hidden` and nothing else, so a dropped NOT NULL
        column stays NOT NULL in the schema `resolve_table` hands back. A binary
        push naming only the surviving columns has to fill that slot itself — the
        caller cannot name a column SQL has already taken away."""
        sn = "zb" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=sn)
            tid, schema = client.resolve_table(sn, "t")

            batch = ZSetBatch(schema)
            batch.append(id=1, b=100)
            batch.extend([{"id": 2, "b": 200}])
            client.push(tid, batch)

            rows = list(client.scan(tid))
            assert {r.id: r.b for r in rows} == {1: 100, 2: 200}
            assert all("a" not in r._fields for r in rows)

            # The tombstone is not a column the caller may write to.
            with pytest.raises(TypeError, match="unexpected column name"):
                ZSetBatch(schema).append(id=3, a=1, b=300)
        finally:
            _cleanup(client, sn, "t")

    def test_weights_survive_mixed_call_sites(self, client):
        """Weights written through the keyword plan have to reach the engine and
        accumulate there, not merely land in the batch."""
        sn, tid, schema = _make_table(client)
        try:
            batch = ZSetBatch(schema)
            batch.append(pk=1, val=10)                 # one call site
            batch.append(val=20, pk=2, _weight=3)      # a second: different shape
            batch.append(**{"pk": 3, "val": 30})       # a third: splat
            batch.append(pk=1, val=10, _weight=-1)     # retracts the first
            assert batch.weights == [1, 3, 1, -1]
            client.push(tid, batch)
            result = client.scan(tid)
            assert {row.pk: row.val for row in result} == {2: 20, 3: 30}
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Value coercion at the binding boundary
# ---------------------------------------------------------------------------


class TestValueCoercion:

    _UUID_STR = "12345678-1234-5678-1234-567812345678"

    def _uuid_pk_schema(self):
        return Schema([
            ColumnDef("id", TypeCode.UUID, primary_key=True),
            ColumnDef("v",  TypeCode.I64),
        ])

    def _compound_schema(self):
        return Schema([
            ColumnDef("a", TypeCode.U64),
            ColumnDef("b", TypeCode.U32),
            ColumnDef("v", TypeCode.I64),
        ], pk_indices=[0, 1])

    def test_uuid_pk_from_hex_string(self):
        batch = ZSetBatch(self._uuid_pk_schema())
        batch.append(id=self._UUID_STR, v=1)
        assert len(batch) == 1

    def test_uuid_pk_from_uuid_object(self):
        batch = ZSetBatch(self._uuid_pk_schema())
        batch.append(id=uuid.UUID(self._UUID_STR), v=1)
        assert len(batch) == 1

    def test_uuid_payload_from_uuid_object(self):
        schema = Schema([
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("id", TypeCode.UUID),
        ])
        batch = ZSetBatch(schema)
        batch.append(pk=1, id=uuid.UUID(self._UUID_STR))
        assert len(batch) == 1
        # A UUID column presents as canonical hyphenated text, whatever form the
        # value was written in.
        assert batch.columns[1] == [self._UUID_STR]

    def test_uuid_invalid_string_raises(self):
        batch = ZSetBatch(self._uuid_pk_schema())
        with pytest.raises(ValueError):
            batch.append(id="not-a-uuid-string", v=1)

    def test_uuid_accepts_both_text_forms(self):
        """Canonical hyphenated and bare 32-hex are the two renderings of one
        UUID; both must reach a UUID column, and land on the same value."""
        bare = self._UUID_STR.replace("-", "")
        a = ZSetBatch(self._uuid_pk_schema()).append(id=self._UUID_STR, v=1)
        b = ZSetBatch(self._uuid_pk_schema()).append(id=bare, v=1)
        assert a.pks == b.pks == [self._UUID_STR]

    def test_u128_column_rejects_text(self):
        """A string spells a value only for the types that admit one, and U128 is
        not among them — SQL reads a string literal into a U128 column as an
        error, so `append` must too, rather than accepting bare hex here alone.
        The same rule excludes a `uuid.UUID` object, which is a UUID, not a
        number."""
        for pk_type in (True, False):
            cols = [ColumnDef("k", TypeCode.U128, primary_key=True),
                    ColumnDef("v", TypeCode.U128, is_nullable=True)]
            schema = Schema(cols)
            bad = "ffffffffffffffffffffffffffffffff"
            with pytest.raises(TypeError):
                ZSetBatch(schema).append(**({"k": bad, "v": 1} if pk_type else {"k": 1, "v": bad}))
            with pytest.raises(TypeError):
                ZSetBatch(schema).append(
                    **({"k": uuid.uuid4(), "v": 1} if pk_type else {"k": 1, "v": uuid.uuid4()}))

    def test_u128_keeps_its_full_unsigned_range(self):
        """A U128 value above `i128::MAX` is legal and must not be rejected as a
        signed overflow."""
        u128_max = (1 << 128) - 1
        schema = Schema([ColumnDef("k", TypeCode.U128, primary_key=True),
                         ColumnDef("v", TypeCode.U128)])
        batch = ZSetBatch(schema).append(k=u128_max, v=u128_max)
        assert batch.pks == [u128_max]
        assert batch.columns[1] == [u128_max]

    def test_blob_payload_round_trip(self):
        schema = Schema([
            ColumnDef("pk", TypeCode.U64, primary_key=True),
            ColumnDef("payload", TypeCode.BLOB),
        ])
        batch = ZSetBatch(schema)
        batch.append(pk=1, payload=b"hello\x00world")
        assert batch.columns[1] == [b"hello\x00world"]

    def test_compound_pk_emits_packed_bytes(self):
        batch = ZSetBatch(self._compound_schema())
        batch.append(a=7, b=9, v=100)
        # Single-PK schemas keep returning ints; compound schemas return
        # raw packed PK bytes (LE-encoded a || LE-encoded b).
        pks = batch.pks
        assert len(pks) == 1
        assert pks[0] == (7).to_bytes(8, "little") + (9).to_bytes(4, "little")

    def test_compound_pk_none_rejected(self):
        batch = ZSetBatch(self._compound_schema())
        with pytest.raises(ValueError):
            batch.append(a=None, b=9, v=1)
        assert len(batch) == 0

    def test_compound_pk_missing_column_rejected(self):
        batch = ZSetBatch(self._compound_schema())
        with pytest.raises(ValueError, match="missing"):
            batch.append(b=9, v=1)
        assert len(batch) == 0

    def test_seek_accepts_a_uuid_object(self, client):
        """The seek key runs through the same typed PK encoder as `append`, so a
        `uuid.UUID` that can be written must also be usable to look the row up."""
        sn = "zb" + _uid()
        client.create_schema(sn)
        try:
            uid_val = uuid.uuid4()
            cols = [ColumnDef("pk", TypeCode.UUID, primary_key=True),
                    ColumnDef("val", TypeCode.I64)]
            tid = client.create_table(sn, "t", cols)
            batch = ZSetBatch(Schema(cols))
            batch.append(pk=uid_val, val=42)
            client.push(tid, batch)
            result = client.seek(tid, uid_val)
            assert len(result) == 1
            assert result.first().pk == str(uid_val)
        finally:
            _cleanup(client, sn, "t")
