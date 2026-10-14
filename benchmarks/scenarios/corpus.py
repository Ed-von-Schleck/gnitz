"""Real text: the paragraphs of public-domain books from Project Gutenberg.

Each book is fetched once and kept under `tmp/corpus/`; its body — the text
between Gutenberg's start and end markers, the licence boilerplate left out — is
pinned by its SHA-256, so every machine runs a scenario over the same bytes.
"""

from __future__ import annotations

import hashlib
import re
import urllib.request
from pathlib import Path

CACHE = Path(__file__).resolve().parents[2] / "tmp" / "corpus"
URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"

# (Gutenberg id, what it is, SHA-256 of its body)
BOOKS = [
    (100, "Shakespeare, Complete Works (en)", "8c20850ac24fbc51ae59381309c7dd03034fd1c2285bfa24b65ab8120d3371b6"),
    (10, "King James Bible (en)", "177648b66a029c3ec8a77368226c976954b14fd1d6f001f509e9f08ce22ae753"),
    (2600, "Tolstoy, War and Peace (en)", "cb6c666a1e9088f5993524c4c435db30aa2e87b28066a854284a2838ee210c36"),
    (135, "Hugo, Les Misérables (en)", "f6efd041668276b9275faba052fb0e0051f3520247b8613d7acb85e4aff841bc"),
    (1184, "Dumas, The Count of Monte Cristo (en)", "2aafcc204f5262d0e0f1babcb8b3427fa608f3db8319325e491088aa6aa51e46"),
    (28054, "Dostoyevsky, The Brothers Karamazov (en)", "5079966b332aae5834fec75c9aedb7572a0f42c75ce18a342b27607f5cb3c0fa"),
    (6593, "Fielding, Tom Jones (en)", "da513abb211ae6bf0f4002033630753391f466c599e9299914c2c0c7fc8c48b1"),
    (2554, "Dostoyevsky, Crime and Punishment (en)", "25809270f8085c6eeb426287052fc0c82a2051736705f995f37313d7622f3c2e"),
    (2000, "Cervantes, Don Quijote (es)", "9b66a97b875b88b999f0c198b243bf3d396a2785b0126140d0f1f0e68f016ae6"),
    (2650, "Proust, Du côté de chez Swann (fr)", "9626e3c01bf4ac978c0e67ba32bc15cf71430a0b707c51aee0831b2173d09fd7"),
    (17489, "Hugo, Les misérables I: Fantine (fr)", "b313b3239c29ec1e5fb829995e0f263151c1a1943466550753a6d95fdf18a054"),
    (34811, "Mann, Buddenbrooks (de)", "43e2b7fe984f7e8e736399861dd9d42d599cefa0b193e5c96b3693a943908112"),
    (7205, "Nietzsche, Also sprach Zarathustra (de)", "fabbaf796a25153d3a3e68682d5d3ee655286a3d9143c32255c0637397fa66ba"),
    (1012, "Dante, La Divina Commedia (it)", "06ac60aa469410a30096b0c3097a7743d28982644f93bd97d8d38b09a4873f05"),
]


def _body(book_id: int, sha256: str) -> str:
    path = CACHE / f"{book_id}.txt"
    if not path.exists():
        with urllib.request.urlopen(URL.format(id=book_id), timeout=60) as resp:
            raw = resp.read().decode("utf-8-sig").replace("\r\n", "\n")
        start = raw.index("\n", raw.index("*** START OF"))
        body = raw[start:raw.index("*** END OF")]
        got = hashlib.sha256(body.encode()).hexdigest()
        if got != sha256:
            raise RuntimeError(f"Gutenberg #{book_id} changed: its body hashes to {got}, pinned {sha256}; "
                               "pin the new hash in benchmarks/scenarios/corpus.py once its text checks out")
        CACHE.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".part")
        tmp.write_text(body, encoding="utf-8")
        tmp.rename(path)
    return path.read_text(encoding="utf-8")


def paragraphs() -> list[str]:
    """Every paragraph of every book, in book order, each one line with its
    whitespace runs collapsed: 142k of them, median 116 bytes, a tenth past
    480. A paragraph that recurs in its book (a chapter heading, a speaker's
    name) recurs here."""
    out = []
    for book_id, _, sha256 in BOOKS:
        out += [p for p in (" ".join(s.split()) for s in re.split(r"\n\s*\n", _body(book_id, sha256))) if p]
    return out
