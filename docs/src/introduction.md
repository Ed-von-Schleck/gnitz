# Introduction

gnitz is a SQL database whose views are all **materialized and incrementally
maintained**. A view is computed once, when it is created, and from then on
every write to a table it reads updates it by the change alone: the cost of a
write is proportional to what changed, not to how much data the view covers.
Reading a view is reading stored rows, so it is always fast and never stale.

The other side of that trade is that a plain `SELECT` is deliberately small. It
reads one table or view, with a filter, grouping, ordering and a limit. Anything
that would derive a new relation — a join, a window function, a subquery — goes
into a view, where the engine keeps it up to date instead of recomputing it on
every read.

gnitz implements the DBSP model of incremental computation: every relation is a
multiset in which each row carries an integer weight, and every write is a
change to those weights. You rarely see the weights directly; they show up when
you follow a view's changes as a [delta feed](sql/views.md#delta-feeds).

gnitz is **pre-alpha**. It runs on Linux, stores its data on local disk, and is
a single server; client and server must be of the same release.

## Where to go next

- [Getting started](getting-started.md) installs gnitz and creates a first view.
- [Tables, views and streams](concepts/relations.md) explains what gnitz keeps
  and what it computes.
- [Limitations](sql/limitations.md) lists what the SQL planner refuses today,
  and how to work around it.
- [Deployment](ops/deployment.md) runs the server as a service or a container.
