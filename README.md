# Gnitz

`Gnitz` is a SQL database. What makes it different from most other SQL databases is that `VIEW`s are all _materialized and incrementally updated_, meaning: They never go stale, and they are updated _efficiently_ – the cost to update a `VIEW` is always proportional to the size of the _changed_ data, regardless of how much _existing_ data there is. It intentionally restricts `SELECT` statements to the bare minimum so that queries are _always fast, no footguns_. `Gnitz` spills these `VIEW`s to disk, so it trades disk-space for latency.

A Gnitz guarantees efficient `VIEW` updates by implementing the **DBSP** (Differential Dataflow) formal model, treating all data as **Z-Sets** – multisets where every record has an associated integer weight, enabling algebraic coalescing of updates.
