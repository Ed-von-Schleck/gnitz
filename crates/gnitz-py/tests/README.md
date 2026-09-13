# E2E test categories

Every test here crosses every subsystem, so a directory names the coordinate of
a scenario its files vary, not a subsystem. A test goes where its independent
variable is. A file that varies two coordinates is two files.

    client_surface     how the scenario is issued and how results come back
    read_verb          how a fixed answer is retrieved: scan, seek, spec, bound, multi
    relational_shape   the structure of the compiled body, the dataflow graph
    scalar_expression  the per-row program inside a fixed body
    value_domain       column types, widths, signedness, NULL, key encoding
    mutation_pattern   what writes occur and how they are grouped
    schema_lifetime    the schema changing mid-scenario: DDL, ALTER, create-order
    distribution       worker count, partitioning, replication, exchange
    storage_policy     a relation's WITH (...): capacity, delta, stream
    state_lifetime     the process boundary crossed: restart, crash, checkpoint,
                       SAL reclaim
    interleaving       timing between operations that would otherwise be serial
    admissibility      what the engine must refuse rather than accept

relational_shape is the graph, scalar_expression the per-row program. Varying
UPPER() under a plain projection is an expression test; varying the join shape
under identity projections is a shape test.

A test whose subject is a value under a fixed shape is value_domain: a NULL join
key varies the value, not the join.

A secondary index is not a coordinate of its own: what it changes is which walk
a read takes, so an index test is a read_verb test, its DDL rejections are
admissibility, its catalog row is schema_lifetime, and its key width is
value_domain.

GNITZ_WORKERS is a suite-wide run mode, so distribution is for a test that
varies worker count or placement, not one that merely needs W > 1.

Helpers (conftest.py, _read.py, _serverproc.py) stay at this level and are
imported as top-level modules, from any subdirectory.
