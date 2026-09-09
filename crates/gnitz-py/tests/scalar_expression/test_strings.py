"""The Str register class: the string functions, and comparison between strings.

A string value is a 16-byte German cell: at most 12 bytes live inside the cell,
anything longer lives in a blob heap the cell points into. So a computed string
is not a value in a register — it is a cell plus a relocation, and every kernel
here has to handle both classes and produce its own heap.

Two consequences shape the file:

- **A computed string must be byte-stable across a retraction.** An insert and
  its retraction re-derive the value independently, and they cancel under (PK,
  payload) identity only if the bytes match exactly. One byte of uninitialized
  or recycled heap leaves both rows in the trace forever — at unchanged row
  count, visible only in the weights.
- **Character and byte are different units.** LENGTH, SUBSTRING, LEFT/RIGHT,
  STRPOS, REVERSE and the pads all count characters; OCTET_LENGTH counts bytes.
  An ASCII-only corpus cannot tell the two apart, so every case here carries
  multi-byte text.

Case folding is ASCII-only, a deliberate deviation from PostgreSQL under a UTF-8
locale. The per-value truth tables belong to `gnitz-expr`'s kernel tests, which
drive every cell class and both nullability arms of each opcode directly.
"""

import pytest
import gnitz
from _read import bag, scanned

# A value on each side of the 12-byte inline/heap boundary, plus non-ASCII and
# an empty string — the four cell shapes every kernel has to handle.
CORPUS = [(1, "abc"), (2, "exactly12chr"), (3, "thirteen-char"),
          (4, "Grüße-Österreich"), (5, "")]
_CORPUS_SQL = ", ".join(f"({i}, '{s}')" for i, s in CORPUS)


def test_every_transform_answers_over_each_cell_class_and_over_null(client, schema_name):
    """The basic transforms over a value that is padded, multi-byte and NULL at
    once. `Grüße` is five characters in seven bytes, so LENGTH and OCTET_LENGTH
    disagree wherever the unit is wrong."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, UPPER(s) AS u, LOWER(s) AS l, "
        "LENGTH(s) AS n, CHAR_LENGTH(s) AS cn, OCTET_LENGTH(s) AS bn, "
        "SUBSTRING(s FROM 2 FOR 3) AS sub, "
        "TRIM(s) AS tr, LTRIM(s) AS lt, RTRIM(s) AS rt FROM t", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, '  Grüße  '), (2, 'abc'), (3, NULL)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "u", "l", "n", "cn", "bn",
               "sub", "tr", "lt", "rt") == {
        # The umlaut and the ß are untouched by the ASCII-only fold; the four
        # spaces plus five characters are nine, in eleven bytes.
        (1, "  GRüßE  ", "  grüße  ", 9, 9, 11, " Gr", "Grüße", "Grüße  ", "  Grüße"): 1,
        (2, "ABC", "abc", 3, 3, 3, "bc", "abc", "abc", "abc"): 1,
        (3, None, None, None, None, None, None, None, None, None): 1,
    }


def test_the_library_functions_count_characters_and_propagate_null(client, schema_name):
    """LEFT/RIGHT/STRPOS/POSITION/REVERSE/REPLACE/LPAD/RPAD/SPLIT_PART over a
    multi-byte subject, a subject past the inline boundary, a NULL subject and a
    NULL count — a count in any argument position propagates like an operand."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT, n BIGINT)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, LEFT(s, n) AS l, RIGHT(s, n) AS r, "
        "LEFT(s, -1) AS ln, STRPOS(s, 'l') AS sp, POSITION('é' IN s) AS po, "
        "REVERSE(s) AS rv, REPLACE(s, 'l', '--') AS rp, "
        "LPAD(s, 8, 'xy') AS lp, RPAD(s, 8) AS rpd, LPAD(s, 3) AS trunc, "
        "SPLIT_PART(s, 'l', 2) AS f2, SPLIT_PART(s, 'l', -1) AS fl, "
        "SPLIT_PART(s, 'l', n) AS fn FROM t", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 'héllo', 2), (2, 'a-long-value-past-twelve', 0), "
        "(3, NULL, 1), (4, 'x', NULL)", schema_name=sn)
    by_id = {r.id: r for r in scanned(client, sn, "v")}

    r1 = by_id[1]
    # `é` is two bytes; every one of these indexes past it by character.
    assert (r1.l, r1.r, r1.ln) == ("hé", "lo", "héll")
    assert (r1.sp, r1.po) == (3, 2)
    assert r1.rv == "olléh" and r1.rp == "hé----o"
    assert (r1.lp, r1.rpd, r1.trunc) == ("xyxhéllo", "héllo   ", "hél")
    assert (r1.f2, r1.fl, r1.fn) == ("", "o", "")

    r2 = by_id[2]
    assert (r2.l, r2.r) == ("", ""), "a zero count takes nothing from either end"
    assert r2.lp == "a-long-v" and r2.trunc == "a-l", "a pad narrower than its subject truncates"
    assert r2.fl == "ve"
    assert r2.fn is None, "SPLIT_PART numbers its fields from one, so field 0 is NULL"

    for c in ("l", "r", "ln", "sp", "po", "rv", "rp", "lp", "rpd", "trunc", "f2", "fl", "fn"):
        assert getattr(by_id[3], c) is None, c
    assert by_id[4].l is None and by_id[4].fn is None and by_id[4].rv == "x"

    # Every row present exactly once, so nothing above rode an accumulated view.
    assert bag(scanned(client, sn, "v"), "id") == {(1,): 1, (2,): 1, (3,): 1, (4,): 1}


def test_the_alternate_spellings_are_the_same_function(client, schema_name):
    """`SUBSTR`/`SUBSTRING`, `CHAR_LENGTH`/`CHARACTER_LENGTH` and the explicit
    `TRIM(LEADING|TRAILING|BOTH …)` forms bind to the nodes their shorthands do,
    so a spelling decides nothing. The trim set is a byte set, not a prefix: it
    strips any of its characters, in any order."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, "
        "SUBSTR(s, 2, 3) AS a, SUBSTRING(s FROM 2 FOR 3) AS b, "
        "CHARACTER_LENGTH(s) AS c, CHAR_LENGTH(s) AS d, "
        "TRIM(LEADING ' ' FROM s) AS e, LTRIM(s) AS f, "
        "TRIM(TRAILING ' ' FROM s) AS g, RTRIM(s) AS h, "
        "TRIM(BOTH ' ' FROM s) AS i, TRIM(s) AS j FROM t", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, '  Grüße  ')", schema_name=sn)

    row = next(iter(scanned(client, sn, "v")))
    assert (row.a, row.c, row.e, row.g, row.i) == (row.b, row.d, row.f, row.h, row.j)
    assert row.a == " Gr" and row.c == 9 and row.i == "Grüße"


def test_a_trim_set_strips_any_of_its_characters(client, schema_name):
    """A non-default trim set is the case the default `' '` cannot reach: the
    argument is a set of bytes, so order within it does not matter and a
    character not in it stops the walk."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, LTRIM(s, 'ab') AS l, RTRIM(s, 'ba') AS r, "
        "TRIM('ab' FROM s) AS t FROM t", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 'abbaXabba'), (2, 'ab')", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "l", "r", "t") == {
        (1, "Xabba", "abbaX", "X"): 1,
        (2, "", "", ""): 1,
    }


def test_a_computed_string_survives_the_inline_heap_boundary_in_both_directions(
        client, schema_name):
    """A value of twelve bytes or fewer lives in its cell; a longer one lives in
    the heap. A map must relocate the second kind into its own heap, and the
    compute kernel is gated on the emit lists being non-empty — a gate that
    counted only the scalar emits would drop the kernel for a projection whose
    only computed column is a string, and ship that column's uninitialized
    region. Recycled heap bytes read differently across calls and never cancel,
    so the view is re-scanned and the two answers compared.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT id, UPPER(s) AS u FROM t", schema_name=sn)
    client.execute_sql(f"INSERT INTO t VALUES {_CORPUS_SQL}", schema_name=sn)

    got = bag(scanned(client, sn, "v"), "id", "u")
    assert got == {
        (1, "ABC"): 1,
        (2, "EXACTLY12CHR"): 1,
        (3, "THIRTEEN-CHAR"): 1,
        # ASCII-only: the umlaut, the eszett and the already-capital O-umlaut
        # are left exactly as they arrived.
        (4, "GRüßE-ÖSTERREICH"): 1,
        (5, ""): 1,
    }
    assert bag(scanned(client, sn, "v"), "id", "u") == got, \
        "a re-scan must give identical bytes"

    types = {c.name: c.type_code for c in client.resolve_table(sn, "v")[1].columns}
    assert types["u"] == gnitz.TypeCode.STRING


def test_a_retraction_re_derives_every_string_and_cancels_it(client, schema_name):
    """An UPDATE re-derives every computed string. Touching only the integer
    column forces the string-derived columns to reproduce byte for byte; touching
    the string itself moves a value across the inline/heap boundary in both
    directions. The whole failure is invisible in a row set."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, k BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, UPPER(s) AS u, "
        "SUBSTRING(TRIM(s) FROM 1 FOR 4) AS head, REVERSE(s) AS rv, "
        "LPAD(s, 30, '.') AS lp, SPLIT_PART(s, '-', k) AS sp, "
        "CONCAT(s, '-', CAST(k AS TEXT)) AS tag FROM t", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, ' a-long-value-past-twelve ', 2), (2, 'abc', 1)",
        schema_name=sn)

    ids = {(1,): 1, (2,): 1}
    assert bag(scanned(client, sn, "v"), "id") == ids
    by_id = {r.id: r for r in scanned(client, sn, "v")}
    assert by_id[1].head == "a-lo" and by_id[1].tag == " a-long-value-past-twelve -2"
    assert by_id[2].u == "ABC" and by_id[2].lp == "." * 27 + "abc"

    # Touch only `k`: every column derived from the untouched string must cancel.
    client.execute_sql("UPDATE t SET k = 3 WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == ids
    by_id = {r.id: r for r in scanned(client, sn, "v")}
    assert by_id[1].sp == "value" and by_id[1].rv == " evlewt-tsap-eulav-gnol-a "
    assert by_id[1].tag == " a-long-value-past-twelve -3"

    # Heap value shrinking inline, and inline value growing into the heap.
    client.execute_sql("UPDATE t SET s = 'xy' WHERE id = 1", schema_name=sn)
    client.execute_sql("UPDATE t SET s = 'another-long-one' WHERE id = 2", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == ids
    by_id = {r.id: r for r in scanned(client, sn, "v")}
    assert by_id[1].u == "XY" and by_id[2].u == "ANOTHER-LONG-ONE"

    client.execute_sql("DELETE FROM t", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {}


def test_a_multibyte_corpus_round_trips_and_cancels(client, schema_name):
    """Scripts outside Latin-1, a combining-free single character and an
    astral-plane pair: the character/byte split has to hold for each, and each
    has to cancel."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, LENGTH(s) AS n, OCTET_LENGTH(s) AS b, "
        "SUBSTRING(s FROM 2) AS tail, REVERSE(s) AS rv FROM t", schema_name=sn)
    corpus = ["日本語テキスト", "é", "Ω-omega-Ω", "мир", "🙂🙃"]
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({i}, '{s}')" for i, s in enumerate(corpus, 1)),
        schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "n", "b", "tail", "rv") == {
        (i, len(s), len(s.encode()), s[1:], s[::-1]): 1
        for i, s in enumerate(corpus, 1)}

    client.execute_sql("DELETE FROM t", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {}


def test_concat_and_the_operator_differ_only_in_their_null_rule(client, schema_name):
    """`||` propagates a NULL from either side; `CONCAT` treats one as empty and
    is never NULL — including at arity one, which the seeded fold makes non-NULL
    rather than a passthrough. CONCAT also casts a numeric argument; `||` does
    not accept one at all."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a TEXT, b TEXT, n BIGINT)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, a || b AS pipe, CONCAT(a, b) AS cat, "
        "CONCAT(a, '-', n) AS mixed, CONCAT(a) AS one FROM t", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 'x', 'y', 7), (2, NULL, 'y', 7), "
        "(3, 'x', NULL, NULL), (4, NULL, NULL, NULL)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "pipe", "cat", "mixed", "one") == {
        (1, "xy", "xy", "x-7", "x"): 1,
        (2, None, "y", "-7", ""): 1,
        (3, None, "x", "x-", "x"): 1,
        (4, None, "", "-", ""): 1,
    }


def test_a_string_case_carries_its_null_in_the_string_channel(client, schema_name):
    """A CASE with no ELSE is `ELSE NULL`, and in a string CASE that NULL has to
    be a string NULL — otherwise the branches would mix register classes and the
    result could not be stored as one column."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT, k BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, "
        "CASE WHEN k > 5 THEN 'big' WHEN k > 0 THEN 'small' ELSE 'none' END AS bucket, "
        "CASE WHEN k > 5 THEN s END AS maybe, COALESCE(s, 'default') AS c FROM t",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 'here', 9), (2, NULL, 3), (3, 'x', 0)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "bucket", "maybe", "c") == {
        (1, "big", "here", "here"): 1,
        (2, "small", None, "default"): 1,
        (3, "none", None, "x"): 1,
    }


def test_computed_operands_chain_through_the_arena(client, schema_name):
    """A function's argument may itself be a computed string, so the arena has to
    hold an intermediate while the next kernel reads it — including when the
    outer call's other argument is computed too."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, REPLACE(UPPER(s), 'X', LOWER('YY')) AS a, "
        "LEFT(REVERSE(s), LENGTH(s) - 1) AS b, "
        "STRPOS(CONCAT(s, s), RIGHT(s, 1)) AS c, "
        "UPPER(SUBSTRING(TRIM(s), 1, 3)) AS d FROM t", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 'axbx'), (2, 'q')", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "a", "b", "c", "d") == {
        (1, "AyyByy", "xbx", 2, "AXB"): 1,
        (2, "Q", "", 1, "Q"): 1,
    }


@pytest.mark.parametrize("pred,want", [
    ("name = 'bob'", {2}),
    ("name != 'bob'", {1, 3, 4, 5, 6, 7}),
    ("name > 'bob'", {3, 4}),
    ("name >= 'bob'", {2, 3, 4}),
    ("name <= 'bob'", {1, 2, 5, 6, 7}),
    # "ba" > "ac" lexicographically, so it must not pass. Comparing the cells'
    # leading bytes as a little-endian integer would rank "ba" (0x6162) below
    # "ac" (0x6361) and admit it; the prefix is big-endian for exactly this.
    ("name < 'ac'", {5, 6}),
    # The literal on the left takes the converse operator, not a second opcode.
    ("'bob' < name", {3, 4}),
    ("'bob' = name", {2}),
    # A computed operand leaves the fused column-vs-constant form for the
    # register channel and must answer the same way.
    ("UPPER(name) = 'BOB'", {2}),
    # A NULL never matches, and never matches the negation either.
    ("name IS NULL", {8}),
])
def test_a_string_comparison_orders_by_bytes_on_every_spelling(
        client, schema_name, pred, want):
    """One fused opcode carries all six operators over a column and a constant,
    and the order is the cells' plain byte order at any length."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, name TEXT)", schema_name=sn)
    client.execute_sql(f"CREATE VIEW v AS SELECT id FROM t WHERE {pred}", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie'), (4, 'dave'), "
        "(5, 'aa'), (6, 'ab'), (7, 'ba'), (8, NULL)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id") == {(i,): 1 for i in want}


def test_two_string_columns_compare_against_each_other(client, schema_name):
    """The column-against-column form is its own opcode; nothing about it is
    derivable from the column-against-constant one."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, u TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT id FROM t WHERE s < u", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 'x', 'y'), (2, 'z', 'a'), (3, 'abc', 'abcd')",
        schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {(1,): 1, (3,): 1}


def test_a_view_reads_another_views_computed_string(client, schema_name):
    """A computed string is an ordinary column of the view that produced it, so
    the next view reads it as it would a base column — across the wire, from the
    heap the first view wrote."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE VIEW v1 AS SELECT id, UPPER(s) AS u FROM t", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v2 AS SELECT id, LENGTH(u) AS n, LOWER(u) AS back FROM v1",
        schema_name=sn)
    client.execute_sql(f"INSERT INTO t VALUES {_CORPUS_SQL}", schema_name=sn)

    assert bag(scanned(client, sn, "v2"), "id", "n", "back") == {
        (1, 3, "abc"): 1,
        (2, 12, "exactly12chr"): 1,
        (3, 13, "thirteen-char"): 1,
        (4, 16, "grüße-Österreich"): 1,
        (5, 0, ""): 1,
    }


def test_edge_arguments_follow_postgresql(client, schema_name):
    """A negative RIGHT count drops from the left, an empty REPLACE pattern and
    an empty pad fill leave the subject alone, a non-positive pad width is the
    empty string, and an empty SPLIT_PART delimiter makes the whole string field
    one."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, RIGHT(s, -1) AS rn, REPLACE(s, '', 'z') AS re, "
        "LPAD(s, 8, '') AS lpe, RPAD(s, 0) AS rz, LPAD(s, -3) AS ln, "
        "SPLIT_PART(s, '', 1) AS s1, SPLIT_PART(s, '', 2) AS s2 FROM t",
        schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 'héllo'), (2, '')", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "rn", "re", "lpe", "rz", "ln", "s1", "s2") == {
        (1, "éllo", "héllo", "héllo", "", "", "héllo", ""): 1,
        (2, "", "", "", "", "", "", ""): 1,
    }


@pytest.mark.parametrize("expr,why", [
    # Each is refused while planning, naming the string operand — not accepted
    # and then failing inside the engine's class validator, which would surface
    # as an opaque internal enum.
    ("s + 1", "string"),
    ("ABS(s)", "string"),
    ("-s", "string"),
    ("s || 1", "expected a string value here"),
    ("s AND k", "is not supported on a string operand"),
    ("s = k", "needs both operands to be strings"),
    # A trim set has to be a literal: it is tokenized once per program, not read
    # per row.
    ("TRIM(s FROM s)", "ASCII string literal"),
    ("LEFT(s, f)", "must be an integer"),
    ("REPLACE(s, 'a')", "exactly three arguments"),
    ("LPAD(s)", "two or three arguments"),
    ("STRPOS(s, f)", "string"),
])
def test_a_string_in_a_position_that_cannot_hold_one_is_refused(
        client, schema_name, expr, why):
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, "
        "k BIGINT NOT NULL, f DOUBLE NOT NULL)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match=why):
        client.execute_sql(f"CREATE VIEW bad AS SELECT id, {expr} AS x FROM t",
                           schema_name=sn)
