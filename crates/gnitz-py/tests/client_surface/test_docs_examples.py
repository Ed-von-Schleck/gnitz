"""Every SQL example of the documentation under `docs/src`, and of the README,
run against the server.

A page's `sql` blocks run in order in one fresh schema, so a later block may
use what an earlier one created. A `sql,refused` block must be refused with the
error the `text` block right after it shows, written for the schema `public`.
"""

import re

import pytest
import gnitz
from _paths import REPO_ROOT

DOCS = REPO_ROOT / "docs" / "src"

_FENCE = re.compile(r"^```(sql|sql,refused|text)\n(.*?)^```$", re.DOTALL | re.MULTILINE)

PAGES = sorted(
    p for p in [REPO_ROOT / "README.md", *DOCS.rglob("*.md")]
    if re.search(r"^```sql", p.read_text(), re.MULTILINE)
)


def test_docs_have_examples():
    assert len(PAGES) > 1, f"no sql blocks found under {DOCS}"


@pytest.mark.parametrize("page", PAGES, ids=lambda p: str(p.relative_to(REPO_ROOT)))
def test_page_examples(client, page):
    text = page.read_text()
    blocks = list(_FENCE.finditer(text))
    for i, m in enumerate(blocks):
        kind, sql = m.group(1), m.group(2)
        line = text.count("\n", 0, m.start()) + 1
        where = f"{page.relative_to(REPO_ROOT)}:{line}"
        if kind == "sql":
            try:
                client.execute_sql(sql)
            except gnitz.GnitzError as e:
                pytest.fail(f"{where}: example failed: {e}\n{sql}")
        elif kind == "sql,refused":
            shown = blocks[i + 1] if i + 1 < len(blocks) else None
            assert shown and shown.group(1) == "text", f"{where}: no text block shows the refusal"
            with pytest.raises(gnitz.GnitzRefusedError) as e:
                client.execute_sql(sql)
            got = str(e.value).replace(client.schema, "public")
            assert got == shown.group(2).strip(), f"{where}: refused with another error:\n{got}"
