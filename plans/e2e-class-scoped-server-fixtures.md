# E2E Test: Class-Scoped Server Fixtures for Heavy-DDL Tests

## Problem

The pytest suite runs 665 tests across 31 files against a single session-scoped
gnitz-server process. When that server's catalog is corrupted — by a catalog panic caught
by `guard_panic`, by a partial DDL operation that leaves the DAG in an inconsistent state,
or by test state that was not cleaned up — every subsequent test in the session sees
generic errors. Pytest reports these as `ERROR` (connection failure or internal error)
rather than `FAILED` (the test's assertion failed), making triage difficult.

In the uuid3 E2E run, a single catalog anomaly produced 305 ERRORs whose common cause
was invisible in the test output. The only way to find the root cause was to search for
the first failure chronologically, capture the server's stderr, and work backward.

The two files that already use function-scoped servers (`test_persistence.py`,
`test_checkpoint.py`) were completely immune: they each spin up their own server per test
function and are unaffected by the shared server's state.

## Goal

Add a `class_server` fixture with `scope="class"` so that heavy-DDL test classes each get
a fresh server. Within the class, individual tests share the class server (so startup cost
is paid once per class, not once per test). Between classes, the server is torn down and
a new one is started.

The session server remains for lightweight tests that don't need isolation.

## Cost model

Server startup in the E2E harness takes approximately 1–2 seconds (including worker
fork + catalog open). With `GNITZ_WORKERS=4`, it is closer to 2 seconds due to the
worker barrier wait.

- Session server: 2s startup once, shared by all lightweight tests. No change.
- Class-scoped server: 2s per class, billed to tests in that class.

The test suite currently has approximately 50 test classes across all files. Moving the
high-risk subset (estimated 12–15 classes) to class-scoped servers adds ~25–30 seconds
to the total runtime. Current suite: ~145 seconds. Projected new runtime: ~175 seconds.
Acceptable.

## Implementation

### Step 1: Add `class_server` and `class_client` to `conftest.py`

```python
# conftest.py additions

def _start_server_process(sock_path, data_dir):
    """Shared helper: start gnitz-server, wait for socket, return proc."""
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__),
                                     "../../../gnitz-server")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    cmd = [binary, data_dir, sock_path]
    if w := os.environ.get("GNITZ_WORKERS"):
        cmd += [f"--workers={w}"]
    if ll := os.environ.get("GNITZ_LOG_LEVEL"):
        cmd += [f"--log-level={ll}"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    for _ in range(100):
        if os.path.exists(sock_path):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        proc.communicate()
        raise RuntimeError("Server did not start")
    return proc


@pytest.fixture(scope="class")
def class_server():
    """One fresh server per test class. Use for classes that mutate catalog state
    in ways that may leave it dirty (error-path DDL, wide schemas, cascade drops)."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"),
        prefix="gnitz_cls_",
    )
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    proc = _start_server_process(sock_path, data_dir)
    yield sock_path
    proc.kill()
    proc.wait()
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def class_client(class_server):
    """New connection per test; fresh server per class."""
    with gnitz.connect(class_server) as conn:
        yield conn
```

The existing `server` fixture should also be refactored to use `_start_server_process`
to avoid duplicating the server-start logic. That refactor is a separate line-item.

### Step 2: Switch identified classes to `class_client`

Each heavy-DDL class changes its test method signatures from `def test_*(self, client)`
to `def test_*(self, class_client)`, and renames the parameter `client = class_client`
internally, or simply renames the fixture parameter. Pytest resolves fixtures by
parameter name, so the rename is the entire change.

No other code changes are needed.

## Classification: which classes need isolation

### Tier 1: Definitely move (error-path DDL or persistent side effects)

These classes specifically exercise catalog error paths that, if they fail to clean up,
leave the server's sys_columns or sys_tables with orphaned or inconsistent records.

| Class | File | Reason |
|---|---|---|
| `TestFkErrorCases` | `test_fk.py` | Tests FK DDL rejections; a failed CREATE TABLE with FK leaves orphaned COL_TAB records if cleanup is skipped |
| `TestFkDeleteRestrict` | `test_fk.py` | Exercises cascade DROP TABLE; a failed drop leaves FK metadata intact while the table is gone |
| `TestSchemaMismatch` | `test_errors.py` | Creates tables with intentionally wrong schemas; error paths may skip cleanup |
| `TestUUID` | `test_types.py` | Creates UUID PK tables with FK chains; FK metadata is multi-table; proven to cause issues during UUID implementation |
| `TestU128` | `test_types.py` | Same pattern as TestUUID with DECIMAL(38,0) PK tables |
| `TestTableDDL` | `test_catalog.py` | Directly exercises DROP TABLE, DROP SCHEMA, and table-not-found error paths |
| `TestViewLifecycle` | `test_catalog.py` | Exercises view drop with dependency checks; a partially-dropped view leaves circuit metadata |

### Tier 2: Probably move (high DDL volume or complex schema topology)

These classes create many tables or views with non-trivial relationships. A single test
failure that skips the `finally: _cleanup(...)` block leaves the catalog more polluted
than a simple two-column table would.

| Class | File | Reason |
|---|---|---|
| `TestSqlTypeDDL` | `test_types.py` | Creates one table per SQL type (13+ tables per class run); wide surface area for orphan accumulation |
| `TestFkMultiChild` | `test_fk.py` | Multi-table FK chains; cleanup failure cascades across tables |
| `TestFkMultiWorker` | `test_fk.py` | FK tests under concurrency; interleaved failures are harder to clean up |
| `TestIndexIntegrity` | `test_indices.py` | Creates tables + secondary indexes; index metadata must match table metadata |
| `TestIndexReadBarrier` | `test_indices.py` | Same class of state as TestIndexIntegrity |
| `TestDMLEdgeCases` | `test_catalog.py` | Tests DML error paths on top of DDL; dirty DML state can outlive the test |
| `TestTypeErrors` | `test_types.py` | Tests that U128/UUID columns produce clear errors in views; specifically cited in the 305-ERROR cascade |

### Tier 3: Leave on session server

These classes create a small number of simple tables (1–2 columns, no FK, no indexes),
always clean up via `finally: _cleanup(...)`, and exercise logic that is independent of
catalog state from other tests.

- `TestFilterView`, `TestReduceView`, `TestMapOperator`, `TestNegateOperator`,
  `TestJoinOperator`, `TestOperatorCombinations`, `TestFilterRetraction`,
  `TestDistinctOperator`, `TestUnionOperator` (all in `test_dbsp_ops.py` — purely
  operator logic, 2-column tables)
- `TestGroupBy` (`test_aggregates.py`)
- `TestJoins` (`test_joins.py`)
- `TestSetOps` (`test_set_ops.py`)
- All tests in `test_incremental.py` (creates and drops tables cleanly, no FK or index)
- `TestComputedProjections`, `TestFloatExpressions`, `TestNullPropagation`
  (`test_computed_projections.py`)
- `TestStringPredicates`, `TestLeftJoin` (single-table, no DDL complexity)
- All function-level tests (not in a class) that use `_cleanup` in `finally` blocks

### Definitive classification heuristics

When evaluating whether a new class should use `class_server`:

1. **Does any test in the class call a DDL path that can raise an exception before
   `_cleanup` runs?** If yes, move it. This includes: `execute_sql("CREATE TABLE ...")`,
   `create_table`, `create_schema`. Any of these can fail after writing partial catalog
   state.
2. **Does the class create FK constraints or secondary indexes?** FK and index metadata
   involve multiple system tables. Partial cleanup of a table leaves orphaned rows in
   `sys_indices` or the column FK fields.
3. **Does the class test DROP TABLE or DROP SCHEMA behavior?** Drop paths retract
   cascade records; a failed drop leaves those records in place.
4. **Does the class create views?** View metadata spans `sys_views`,
   `sys_circuit_nodes`, `sys_circuit_edges`, `sys_circuit_sources`, `sys_circuit_params`,
   `sys_circuit_group_cols`, and `sys_view_deps`. A failed view drop leaves records in
   all of them.

If any of the above is true, use `class_client`.

## Verification

Run the E2E suite before and after, capturing timing and result counts:

```bash
# Before
time make e2e 2>&1 | grep -E "passed|failed|error"

# After
time make e2e 2>&1 | grep -E "passed|failed|error"
```

Expected outcome:
- Same pass count (665+).
- Zero failures that were previously hidden by cascade ERROR noise.
- Runtime increase of approximately 25–30 seconds.

Additionally: deliberately cause a catalog panic in a Tier 1 test (e.g., by
temporarily making `hook_table_register` return an error for a specific table name) and
verify that the panic produces a failure in that one test only, not a cascade of ERRORs
in subsequent tests in the same file.

## Additional hardening: server health check

Independently of the class-scoped fixture work, add a session-level health check that
detects a dead or non-responsive session server early:

```python
@pytest.fixture(autouse=True, scope="session")
def _server_health_guard(server):
    """Verify the session server is alive before each test module begins.
    Fails fast with a clear message instead of cascading ERRORs."""
    yield
    # After session, nothing to do.

# In a conftest.py hook:
def pytest_runtest_setup(item):
    """Before each test: if the test uses the session server, ping it."""
    if "client" in item.fixturenames:
        try:
            sock = item.session._server_sock  # set by server fixture
            with gnitz.connect(sock) as c:
                c.scan(1)  # scan SCHEMA_TAB; always present
        except Exception as e:
            pytest.fail(
                f"Session server is not responding before {item.name}: {e}. "
                "Check ~/git/gnitz/tmp/server_debug.log for panic output."
            )
```

This converts a 305-ERROR run into: one FAILED test (the one that killed the server) plus
a clear "server not responding" message on the next test, at which point the run aborts
rather than grinding through 300 more tests.

## What this does NOT fix

- The root cause of catalog state corruption (e.g., the 65-column panic). Class-scoped
  fixtures contain the blast radius; they don't prevent the initial failure.
- Tests within a class that fail before `_cleanup` runs. Even with a class-scoped server,
  state from a failed test within the class leaks to subsequent tests in the same class.
  Enforce `_cleanup` via `autouse` fixtures or `addFinalizer` if this becomes a problem.
- The session server's lack of health checking. The health check fixture described above
  should be implemented alongside or before this work.

## Implementation order

1. Refactor `server` fixture in conftest.py to extract `_start_server_process` helper.
2. Add `class_server` + `class_client` fixtures.
3. Move Tier 1 classes (7 classes): verify E2E still passes.
4. Move Tier 2 classes (7 classes): verify E2E still passes.
5. Add health-check fixture.
6. Document classification heuristics in a comment in conftest.py for future test authors.
