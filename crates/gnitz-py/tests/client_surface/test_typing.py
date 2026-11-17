"""The package's type information: the `py.typed` marker, the stub of the
compiled module, and the annotations of the Python around it.

The stub is written by hand and the extension it describes is compiled, so what
holds them together is here: stubtest compares every name, parameter and default
against the built extension, a consumer's module type-checks under `--strict`,
and the verbs — one method each at runtime, two in the stub — are held to
each other.
"""
import ast
import subprocess
import sys
from pathlib import Path

import gnitz
from gnitz import _native

TESTS = Path(__file__).resolve().parents[1]
PACKAGE = Path(gnitz.__file__).parent

# The stub's two spellings of the verbs.
VERB_CLASSES = ("GnitzClient", "AsyncGnitzClient")


def _run(*args):
    done = subprocess.run([sys.executable, "-m", *args], cwd=TESTS, capture_output=True, text=True)
    assert done.returncode == 0, done.stdout + done.stderr


def test_the_package_is_marked_typed():
    assert (PACKAGE / "py.typed").is_file()
    assert (PACKAGE / "_native.pyi").is_file()


def test_the_stub_matches_the_extension():
    _run("mypy.stubtest", "gnitz", "--allowlist", "stubtest_allowlist.txt")


def test_a_consumer_type_checks():
    _run("mypy", "--strict", "_typed_usage.py")


def _stub_methods():
    """Each stub class's methods: name → its parameters, as source."""
    tree = ast.parse((PACKAGE / "_native.pyi").read_text())
    return {
        cls.name: {f.name: ast.unparse(f.args) for f in cls.body if isinstance(f, ast.FunctionDef)}
        for cls in tree.body if isinstance(cls, ast.ClassDef)
    }


def test_every_verb_is_typed_on_every_client():
    """stubtest reads a class's own members, and the verbs are their base's: a
    verb added there reaches every client, and has to reach both here."""
    stub = _stub_methods()
    verbs = {name for name, member in vars(_native._Client).items()
             if not name.startswith("_") and name != "schema"}
    assert verbs
    for cls in VERB_CLASSES:
        typed = set(stub[cls]) | set(stub["_Client"])
        assert verbs - typed == set(), f"{cls} leaves verbs untyped"


def test_a_verb_takes_the_same_arguments_on_every_client():
    """Only what a verb returns differs between the two."""
    blocking, on_loop = (_stub_methods()[cls] for cls in VERB_CLASSES)
    shared = set(blocking) & set(on_loop)
    assert {v: on_loop[v] for v in shared} == {v: blocking[v] for v in shared}


def test_a_generic_class_takes_a_parameter_at_runtime():
    """An annotation a consumer writes is evaluated."""
    assert _native.Txn[int].__origin__ is _native.Txn
