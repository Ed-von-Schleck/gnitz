"""E2E tests: equi-join reindex-payload pruning.

Pruning drops source columns a join view never reads from the reindex payload and
the join trace. It must be *behaviorally invisible* — results and weights identical
to an unpruned view — while exercising the subtle cases the prune interacts with:
the outer-join null-fill (ν-coarsening) and the NULL-vs-real-key rule-3 split.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_join_payload_pruning.py -v --tb=short
"""
import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, tables=None, views=None):
    for name in (views or []):
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
    for name in (tables or []):
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _rows(client, vid):
    """Scan rows with positive net weight as (dict, weight) pairs."""
    out = []
    for r in client.scan(vid):
        if r.weight > 0:
            out.append((r._asdict(), r.weight))
    return out


class TestJoinPayloadPruning:
    def test_inner_omit_keys_and_columns(self, client):
        """INNER join whose SELECT omits both join keys and several payload cols —
        results identical to the full row projected client-side."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL, "
                "extra BIGINT NOT NULL)",
                schema_name=sn,
            )
            # Selects only fact.a and dim.name — drops both keys and b, c, extra.
            client.execute_sql(
                "CREATE VIEW v AS SELECT fact.a, dim.name FROM fact "
                "JOIN dim ON fact.k = dim.pk",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO dim VALUES (10, 111, 999), (20, 222, 888)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO fact VALUES (1, 10, 5, 6, 7), (2, 20, 8, 9, 1), (3, 10, 50, 60, 70)",
                schema_name=sn,
            )
            got = sorted((d["a"], d["name"], w) for d, w in _rows(client, vid))
            # fact(1)->dim(10): a=5,name=111 ; fact(2)->dim(20): a=8,name=222 ; fact(3)->dim(10): a=50,name=111
            assert got == [(5, 111, 1), (8, 222, 1), (50, 111, 1)], got
        finally:
            _cleanup(client, sn, tables=["fact", "dim"], views=["v"])

    def test_outer_joins_omit_keys(self, client):
        """LEFT / RIGHT / FULL joins omitting keys still null-fill correctly."""
        for jt in ("LEFT", "RIGHT", "FULL"):
            sn = "s" + _uid()
            client.create_schema(sn)
            try:
                client.execute_sql(
                    "CREATE TABLE l (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL)",
                    schema_name=sn,
                )
                client.execute_sql(
                    "CREATE TABLE r (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, y BIGINT NOT NULL)",
                    schema_name=sn,
                )
                client.execute_sql(
                    f"CREATE VIEW v AS SELECT l.x, r.y FROM l {jt} JOIN r ON l.k = r.k",
                    schema_name=sn,
                )
                vid = client.resolve_table(sn, "v")[0]
                # l(k=1) matches r(k=1); l(k=2) unmatched; r(k=3) unmatched.
                client.execute_sql("INSERT INTO l VALUES (1, 1, 100), (2, 2, 200)", schema_name=sn)
                client.execute_sql("INSERT INTO r VALUES (10, 1, 900), (11, 3, 300)", schema_name=sn)
                # None (NULL) sorts before any int via a sentinel key.
                key = lambda t: tuple(-1 if v is None else v for v in t)
                got = sorted(((d["x"], d["y"], w) for d, w in _rows(client, vid)), key=key)
                matched = (100, 900, 1)
                left_unmatched = (200, None, 1)   # r.y NULL
                right_unmatched = (None, 300, 1)  # l.x NULL
                if jt == "LEFT":
                    expect = sorted([matched, left_unmatched], key=key)
                elif jt == "RIGHT":
                    expect = sorted([matched, right_unmatched], key=key)
                else:
                    expect = sorted([matched, left_unmatched, right_unmatched], key=key)
                assert got == expect, f"{jt}: {got} != {expect}"
            finally:
                _cleanup(client, sn, tables=["l", "r"], views=["v"])

    def test_nu_coarsening_weight_two(self, client):
        """ν-coarsening: two preserved rows sharing (key, kept) but differing in a
        DROPPED column collapse to ONE null-filled view row at weight 2. A match
        retracts both; deleting the match restores the weight-2 null-fill. Pins that
        `positive_part` subtracts the RAW matched multiplicity (weight-exact)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "kept BIGINT NOT NULL, dropped BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE r (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, rval BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT l.kept, r.rval FROM l LEFT JOIN r ON l.k = r.k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # Two rows, same (k=7, kept=9), differ only in the dropped column.
            client.execute_sql("INSERT INTO l VALUES (1, 7, 9, 100), (2, 7, 9, 200)", schema_name=sn)
            rows = _rows(client, vid)
            assert len(rows) == 1, rows
            d, w = rows[0]
            assert (d["kept"], d["rval"], w) == (9, None, 2), (d, w)

            # A single matching right row: both left rows join it -> weight-2 inner
            # row, and the null-fill cancels (positive_part(2 - 2) = 0).
            client.execute_sql("INSERT INTO r VALUES (10, 7, 555)", schema_name=sn)
            rows = _rows(client, vid)
            assert len(rows) == 1, rows
            d, w = rows[0]
            assert (d["kept"], d["rval"], w) == (9, 555, 2), (d, w)

            # Delete the match: the weight-2 null-fill returns.
            client.execute_sql("DELETE FROM r WHERE pk = 10", schema_name=sn)
            rows = _rows(client, vid)
            assert len(rows) == 1, rows
            d, w = rows[0]
            assert (d["kept"], d["rval"], w) == (9, None, 2), (d, w)
        finally:
            _cleanup(client, sn, tables=["l", "r"], views=["v"])

    def test_rule3_null_vs_zero_int_key(self, client):
        """Rule 3: a nullable integer join key stays in the preserved side's payload
        so a NULL-keyed null-fill (S=0) is not coarsened with a real-0-keyed match
        (S>=2). Without it, positive_part(2-2)=0 would silently drop the null-fill."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT, x BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE r (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, rval BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT l.x, r.rval FROM l LEFT JOIN r ON l.k = r.k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # Left: NULL-key and real-0-key rows, both x=5. Right: TWO key-0 rows,
            # so the 0-key left row matches twice (S=2).
            client.execute_sql("INSERT INTO l VALUES (1, NULL, 5), (2, 0, 5)", schema_name=sn)
            client.execute_sql("INSERT INTO r VALUES (10, 0, 100), (11, 0, 200)", schema_name=sn)
            got = sorted(((d["x"], d["rval"], w) for d, w in _rows(client, vid)),
                         key=lambda t: (t[0], t[1] if t[1] is not None else -1, t[2]))
            # 0-key row joins both rights; NULL-key row null-fills exactly once.
            assert got == [(5, None, 1), (5, 100, 1), (5, 200, 1)], got
            null_fills = [t for t in got if t[1] is None]
            assert null_fills == [(5, None, 1)], f"NULL-key null-fill lost/coarsened: {got}"
        finally:
            _cleanup(client, sn, tables=["l", "r"], views=["v"])

    def test_rule3_null_vs_empty_string_key(self, client):
        """Rule 3 for a nullable STRING key: NULL and '' both pack to the empty-content
        hash, so the key column (with its null bit) must be retained to keep the
        NULL-keyed null-fill distinct from the ''-keyed match."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (pk BIGINT NOT NULL PRIMARY KEY, k TEXT, x BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE r (pk BIGINT NOT NULL PRIMARY KEY, k TEXT NOT NULL, rval BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT l.x, r.rval FROM l LEFT JOIN r ON l.k = r.k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO l VALUES (1, NULL, 5), (2, '', 5)", schema_name=sn)
            client.execute_sql("INSERT INTO r VALUES (10, '', 100), (11, '', 200)", schema_name=sn)
            got = sorted(((d["x"], d["rval"], w) for d, w in _rows(client, vid)),
                         key=lambda t: (t[0], t[1] if t[1] is not None else -1, t[2]))
            assert got == [(5, None, 1), (5, 100, 1), (5, 200, 1)], got
            null_fills = [t for t in got if t[1] is None]
            assert null_fills == [(5, None, 1)], f"NULL string-key null-fill lost/coarsened: {got}"
        finally:
            _cleanup(client, sn, tables=["l", "r"], views=["v"])

    def test_three_way_chain_residual_retained(self, client):
        """A 3-way chain whose middle step carries a residual-ON over columns dropped
        from the final SELECT: the residual columns must be retained in that step's
        pruned payload so the filter still evaluates correctly."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "x BIGINT NOT NULL, p BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "m BIGINT NOT NULL, q BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE c (pk BIGINT NOT NULL PRIMARY KEY, m BIGINT NOT NULL, z BIGINT NOT NULL)",
                schema_name=sn,
            )
            # Residual `a.p > b.q` on the a-JOIN-b step; final SELECT drops a.p, b.q,
            # a.k, b.k, b.m (all keys/residual cols) — only a.x and c.z survive.
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x, c.z FROM a "
                "JOIN b ON a.k = b.k AND a.p > b.q "
                "JOIN c ON b.m = c.m",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO c VALUES (100, 55, 777), (101, 66, 888)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (10, 1, 55, 4), (11, 2, 66, 9)", schema_name=sn)
            # a(1): k=1 -> b(10), p=5 > q=4 PASS, b.m=55 -> c z=777
            # a(2): k=2 -> b(11), p=8 > q=9 FAIL residual -> dropped
            # a(3): k=1 -> b(10), p=3 > q=4 FAIL residual -> dropped
            client.execute_sql(
                "INSERT INTO a VALUES (1, 1, 500, 5), (2, 2, 600, 8), (3, 1, 700, 3)",
                schema_name=sn,
            )
            got = sorted((d["x"], d["z"], w) for d, w in _rows(client, vid))
            assert got == [(500, 777, 1)], got
        finally:
            _cleanup(client, sn, tables=["a", "b", "c"], views=["v"])

    def test_three_way_chain_middle_left_join(self, client):
        """A chain with a middle LEFT JOIN on a nullable key, keys omitted from the
        final SELECT: null-fill in a hidden chain segment composes with pruning."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT, x BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "m BIGINT NOT NULL, y BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE c (pk BIGINT NOT NULL PRIMARY KEY, m BIGINT NOT NULL, z BIGINT NOT NULL)",
                schema_name=sn,
            )
            # (a LEFT JOIN b) JOIN c. a rows that don't match b get NULL b — then the
            # inner join to c on b.m drops them (b.m NULL). Only fully-matched survive.
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x, c.z FROM a "
                "LEFT JOIN b ON a.k = b.k "
                "JOIN c ON b.m = c.m",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO c VALUES (100, 55, 777)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (10, 1, 55, 9)", schema_name=sn)
            # a(1): k=1 -> b(10) m=55 -> c z=777.  a(2): k=NULL -> no b -> dropped by inner c.
            client.execute_sql("INSERT INTO a VALUES (1, 1, 500), (2, NULL, 600)", schema_name=sn)
            got = sorted((d["x"], d["z"], w) for d, w in _rows(client, vid))
            assert got == [(500, 777, 1)], got
        finally:
            _cleanup(client, sn, tables=["a", "b", "c"], views=["v"])

    def test_backfill_pruned_view(self, client):
        """Creating a pruned join view over PRE-POPULATED tables (backfill) yields the
        same contents as building it before inserts."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO dim VALUES (10, 111), (20, 222)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO fact VALUES (1, 10, 5, 6), (2, 20, 8, 9), (3, 20, 50, 60)",
                schema_name=sn,
            )
            # View created AFTER the data — backfill drives the same circuit.
            client.execute_sql(
                "CREATE VIEW v AS SELECT fact.a, dim.name FROM fact JOIN dim ON fact.k = dim.pk",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            got = sorted((d["a"], d["name"], w) for d, w in _rows(client, vid))
            assert got == [(5, 111, 1), (8, 222, 1), (50, 222, 1)], got
        finally:
            _cleanup(client, sn, tables=["fact", "dim"], views=["v"])
