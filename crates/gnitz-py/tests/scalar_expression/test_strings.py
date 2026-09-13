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
  count, visible only in a bag over the whole row.
- **Character and byte are different units.** LENGTH, SUBSTRING, LEFT/RIGHT,
  STRPOS, REVERSE and the pads all count characters; OCTET_LENGTH counts bytes.
  An ASCII-only corpus cannot tell the two apart, so every case here carries
  multi-byte text.

Case folding is ASCII-only, a deliberate deviation from PostgreSQL under a UTF-8
locale.
"""

from _read import bag, rows, scanned

G, L, P = "  Grüße  ", "a-long-value-past-twelve", "abbaXabba"

# `(s, n)` by id: padded multi-byte, multi-byte, past the inline boundary, a
# trim-set target, empty, a NULL subject and a NULL count.
_IDS = [1, 2, 3, 4, 5, 6, 7]
_ROWS = "(1, '  Grüße  ', 2), (2, 'héllo', 2), (3, 'a-long-value-past-twelve', 0), " \
        "(4, 'abbaXabba', -1), (5, '', 1), (6, NULL, 1), (7, 'x', NULL)"

# Each expression against its value on every row, in `_IDS` order.
_FUNCTIONS = {
    # `ß` and `ü` are untouched by the ASCII-only fold.
    "UPPER(s)": ["  GRüßE  ", "HéLLO", L.upper(), "ABBAXABBA", "", None, "X"],
    "LOWER(s)": ["  grüße  ", "héllo", L, "abbaxabba", "", None, "x"],
    # Four spaces and five characters are nine, in eleven bytes.
    "LENGTH(s)": [9, 5, 24, 9, 0, None, 1],
    "CHARACTER_LENGTH(s)": [9, 5, 24, 9, 0, None, 1],
    "OCTET_LENGTH(s)": [11, 6, 24, 9, 0, None, 1],
    "SUBSTRING(s FROM 2 FOR 3)": [" Gr", "éll", "-lo", "bba", "", None, ""],
    "SUBSTR(s, 2, 3)": [" Gr", "éll", "-lo", "bba", "", None, ""],
    "TRIM(s)": ["Grüße", "héllo", L, P, "", None, "x"],
    "TRIM(BOTH ' ' FROM s)": ["Grüße", "héllo", L, P, "", None, "x"],
    "LTRIM(s)": ["Grüße  ", "héllo", L, P, "", None, "x"],
    "TRIM(LEADING ' ' FROM s)": ["Grüße  ", "héllo", L, P, "", None, "x"],
    "RTRIM(s)": ["  Grüße", "héllo", L, P, "", None, "x"],
    "TRIM(TRAILING ' ' FROM s)": ["  Grüße", "héllo", L, P, "", None, "x"],
    # A trim set is a byte set: order within it does not matter, and a
    # character not in it stops the walk.
    "LTRIM(s, 'ab')": [G, "héllo", L[1:], "Xabba", "", None, "x"],
    "RTRIM(s, 'ba')": [G, "héllo", L, "abbaX", "", None, "x"],
    "TRIM('ab' FROM s)": [G, "héllo", L[1:], "X", "", None, "x"],
    # A count in any argument position propagates a NULL like an operand; a
    # negative one drops from the other end.
    "LEFT(s, n)": ["  ", "hé", "", "abbaXabb", "", None, None],
    "RIGHT(s, n)": ["  ", "lo", "", "bbaXabba", "", None, None],
    "STRPOS(s, 'l')": [0, 3, 3, 0, 0, None, 0],
    "POSITION('é' IN s)": [0, 2, 0, 0, 0, None, 0],
    "REVERSE(s)": ["  eßürG  ", "olléh", L[::-1], P, "", None, "x"],
    "REPLACE(s, 'l', '--')": [G, "hé----o", "a---ong-va--ue-past-twe--ve", P, "", None, "x"],
    # An empty pattern and an empty fill leave the subject alone; a pad narrower
    # than its subject truncates, and a non-positive width is the empty string.
    "REPLACE(s, '', 'z')": [G, "héllo", L, P, "", None, "x"],
    "LPAD(s, 8, 'xy')": ["  Grüße ", "xyxhéllo", "a-long-v", "abbaXabb", "xyxyxyxy", None,
                         "xyxyxyxx"],
    "RPAD(s, 8)": ["  Grüße ", "héllo   ", "a-long-v", "abbaXabb", "        ", None,
                   "x       "],
    "LPAD(s, 30, '')": [G, "héllo", L, P, "", None, "x"],
    "LPAD(s, -3)": ["", "", "", "", "", None, ""],
    # Fields number from one at either end, so field 0 is NULL; an empty
    # delimiter makes the whole string field one.
    "SPLIT_PART(s, 'l', 2)": ["", "", "ong-va", "", "", None, ""],
    "SPLIT_PART(s, 'l', -1)": [G, "o", "ve", P, "", None, "x"],
    "SPLIT_PART(s, 'l', n)": ["", "", None, P, "", None, None],
    "SPLIT_PART(s, '', 1)": [G, "héllo", L, P, "", None, "x"],
    "SPLIT_PART(s, '', 2)": ["", "", "", "", "", None, ""],
    # `||` propagates a NULL; CONCAT reads one as empty, casts a number, and is
    # never NULL — at arity one too.
    "s || '!'": [G + "!", "héllo!", L + "!", P + "!", "!", None, "x!"],
    "CONCAT(s, '-', n)": [G + "-2", "héllo-2", L + "-0", P + "--1", "-1", "-1", "x-"],
    "CONCAT(s)": [G, "héllo", L, P, "", "", "x"],
    # A string CASE with no ELSE carries its NULL in the string channel.
    "CASE WHEN n > 1 THEN s END": [G, "héllo", None, None, None, None, None],
    "COALESCE(s, 'none')": [G, "héllo", L, P, "", "none", "x"],
    # Computed operands chain through the arena, including when the outer
    # call's other argument is computed too.
    "REPLACE(UPPER(s), 'X', LOWER('YY'))": ["  GRüßE  ", "HéLLO", L.upper(), "ABBAyyABBA", "",
                                            None, "yy"],
    "UPPER(SUBSTRING(TRIM(s), 1, 3))": ["GRü", "HéL", "A-L", "ABB", "", None, "X"],
    "LEFT(REVERSE(s), LENGTH(s) - 1)": ["  eßürG ", "ollé", L[::-1][:-1], "abbaXabb", "", None,
                                        ""],
    "STRPOS(CONCAT(s, s), RIGHT(s, 1))": [1, 5, 12, 1, 1, None, 1],
}


def test_every_function_counts_characters_over_each_cell_class_and_null(client, schema_name):
    """One projection per function over a corpus holding every cell shape at
    once, served alike by a maintained view and an ad-hoc read. The functions
    are split across views only to stay inside one program's register file."""
    sn = schema_name
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT, n BIGINT)",
                       schema_name=sn)
    funcs = list(_FUNCTIONS.items())
    parts = [dict(funcs[k:k + 12]) for k in range(0, len(funcs), 12)]
    selects = ["SELECT id, " + ", ".join(f"{e} AS c{n}" for n, e in enumerate(part)) + " FROM t"
               for part in parts]
    for n, select in enumerate(selects):
        client.execute_sql(f"CREATE VIEW v{n} AS {select}", schema_name=sn)
    client.execute_sql(f"INSERT INTO t VALUES {_ROWS}", schema_name=sn)

    for n, (part, select) in enumerate(zip(parts, selects)):
        expected = {r: 1 for r in zip(_IDS, *part.values())}
        assert bag(scanned(client, sn, f"v{n}")) == expected
        assert bag(rows(client, sn, select)) == expected


def _ascii(s, fold):
    return "".join(fold(c) if c.isascii() else c for c in s)


def test_a_retraction_re_derives_every_string_and_cancels_it(client, schema_name):
    """An UPDATE re-derives every computed string. Touching only the integer
    column forces the string-derived columns to reproduce byte for byte;
    touching the string itself moves a value across the inline/heap boundary in
    both directions. A second view reads the first one's computed string as a
    column, from the heap the first view wrote.

    Every check is a bag over the whole row: a retraction whose bytes differ
    leaves its own row at -1 beside the stale one at +1, which a bag keyed on
    `id` alone sums back to 1."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, k BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, UPPER(s) AS u, LENGTH(s) AS n, OCTET_LENGTH(s) AS b, "
        "SUBSTRING(TRIM(s) FROM 1 FOR 4) AS head, SUBSTRING(s FROM 2) AS tail, "
        "REVERSE(s) AS rv, LPAD(s, 30, '.') AS lp, SPLIT_PART(s, '-', k) AS sp, "
        "CONCAT(s, '-', k) AS tag FROM t", schema_name=sn)
    client.execute_sql("CREATE VIEW v2 AS SELECT id, LENGTH(u) AS n, LOWER(u) AS back FROM v",
                       schema_name=sn)
    # Both sides of the 12-byte boundary, scripts outside Latin-1, an
    # astral-plane character and the empty string.
    corpus = {1: (" a-long-value-past-twelve ", 2), 2: ("abc", 1), 3: ("exactly12chr", 1),
              4: ("thirteen-char", 2), 5: ("Grüße-Österreich", 2), 6: ("日本語-テキスト", 1),
              7: ("🙂-мир", 2), 8: ("", 1)}
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({i}, '{s}', {k})" for i, (s, k) in corpus.items()),
        schema_name=sn)

    def check():
        want = {}
        for i, (s, k) in corpus.items():
            parts = s.split("-")
            want[(i, _ascii(s, str.upper), len(s), len(s.encode()), s.strip()[:4], s[1:],
                  s[::-1], s.rjust(30, ".")[:30], parts[k - 1] if k <= len(parts) else "",
                  f"{s}-{k}")] = 1
        assert bag(scanned(client, sn, "v")) == want
        assert bag(scanned(client, sn, "v2")) == {
            (i, len(s), _ascii(_ascii(s, str.upper), str.lower)): 1
            for i, (s, _) in corpus.items()}

    check()
    client.execute_sql("UPDATE t SET k = 3 WHERE id = 1", schema_name=sn)
    corpus[1] = (corpus[1][0], 3)
    check()
    # Heap value shrinking inline, and inline value growing into the heap.
    client.execute_sql("UPDATE t SET s = 'xy' WHERE id = 1", schema_name=sn)
    client.execute_sql("UPDATE t SET s = 'another-long-one' WHERE id = 2", schema_name=sn)
    corpus[1], corpus[2] = ("xy", 3), ("another-long-one", 1)
    check()
    client.execute_sql("DELETE FROM t", schema_name=sn)
    corpus.clear()
    check()


# `name` by id, and the ids each comparison admits. `other` is the column the
# column-against-column form reads.
_COMPARISONS = {
    "name = 'bob'": {2},
    "name != 'bob'": {1, 3, 4, 5, 6, 7},
    "name > 'bob'": {3, 4},
    "name >= 'bob'": {2, 3, 4},
    "name <= 'bob'": {1, 2, 5, 6, 7},
    # "ba" > "ac" lexicographically, so it must not pass. Comparing the cells'
    # leading bytes as a little-endian integer would rank "ba" (0x6162) below
    # "ac" (0x6361) and admit it; the prefix is big-endian for exactly this.
    "name < 'ac'": {5, 6},
    # The literal on the left takes the converse operator, not a second opcode.
    "'bob' < name": {3, 4},
    "'bob' = name": {2},
    # A computed operand leaves the fused column-vs-constant form for the
    # register channel and must answer the same way.
    "UPPER(name) = 'BOB'": {2},
    "name IS NULL": {8},
    # The column-against-column form is its own opcode; a prefix orders first.
    "name < other": {1, 4, 7},
}


def test_a_string_comparison_orders_by_bytes_on_every_spelling(client, schema_name):
    """One fused opcode carries all six operators over a column and a constant,
    and the order is the cells' plain byte order at any length. A NULL never
    matches, and never matches the negation either."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, name TEXT, other TEXT NOT NULL)",
        schema_name=sn)
    for n, pred in enumerate(_COMPARISONS):
        client.execute_sql(f"CREATE VIEW v{n} AS SELECT id FROM t WHERE {pred}", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 'alice', 'b'), (2, 'bob', 'bob'), (3, 'charlie', 'abc'), "
        "(4, 'dave', 'dave!'), (5, 'aa', 'a'), (6, 'ab', 'ab'), (7, 'ba', 'bz'), (8, NULL, 'x')",
        schema_name=sn)

    for n, (pred, want) in enumerate(_COMPARISONS.items()):
        assert bag(scanned(client, sn, f"v{n}")) == {(i,): 1 for i in want}, pred
