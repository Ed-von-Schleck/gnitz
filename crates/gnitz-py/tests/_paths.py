"""The repo root, resolved from this file rather than from `$HOME` or the cwd.

Every `make` target that runs the suite `cd`s into a subdirectory first, so the
process cwd is never the checkout root. `tests/` is not packaged (maturin ships
`python-source = "python"`), so the source path is the only path this module is
ever imported from.
"""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]   # tests → gnitz-py → crates → repo
