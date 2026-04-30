# Register `batch` Field: `Batch` → `Option<Batch>`

`Register.batch` is always a `Batch`, even for Trace registers (which never hold
batch data) and for the instant between an instruction consuming a Delta register
and the next epoch refilling it. Both cases use `Batch::empty(npc, 16)` as a
sentinel — a fake allocation that exists only to satisfy the type system. The fix
is `Option<Batch>`, which makes "no batch here" a first-class value.

---

## Current state

```rust
// vm.rs:625
pub struct Register {
    pub kind: RegisterKind,
    pub schema: SchemaDescriptor,
    /// Delta: current batch.  Trace: unused (empty).
    pub batch: Batch,
    pub cursor_ptr: *mut CursorHandle,
}
```

Three `mem::replace` sites manufacture the dummy:

```rust
// Delay — move src batch out, put dummy in its place
let npc = reg!(*src).batch.num_payload_cols();
let curr = std::mem::replace(&mut reg_mut!(*src).batch, Batch::empty(npc, 16));

// Distinct — consume delta batch, put dummy in its place
let npc = reg!(*in_reg).batch.num_payload_cols();
let delta = std::mem::replace(&mut reg_mut!(*in_reg).batch, Batch::empty(npc, 16));

// execute_epoch — extract output, leave register empty for next epoch
let npc = out.batch.num_payload_cols();
let result = std::mem::replace(&mut out.batch, Batch::empty(npc, 16));
```

In each case `npc` is computed only to construct the dummy; the dummy is never
read. `Option<Batch>` eliminates both the allocation and the `npc` derivation.

---

## Fix

### Step 1 — change the field

```rust
pub struct Register {
    pub kind: RegisterKind,
    pub schema: SchemaDescriptor,
    pub batch: Option<Batch>,
    pub cursor_ptr: *mut CursorHandle,
}
```

### Step 2 — add accessor helpers

50 sites in `vm.rs` read or write `.batch`. A pair of methods keeps the diff
mechanical and the call sites readable:

```rust
impl Register {
    pub fn batch(&self) -> &Batch {
        self.batch.as_ref().expect("register has no batch")
    }
    pub fn batch_mut(&mut self) -> &mut Batch {
        self.batch.as_mut().expect("register has no batch")
    }
}
```

### Step 3 — update RegisterFile::new

```rust
for m in metas {
    let batch = match m.kind {
        RegisterKind::Delta => Some(Batch::with_schema(
            m.schema,
            if m.schema.num_columns > 0 { 16 } else { 0 },
        )),
        RegisterKind::Trace => None,   // cursor-backed; never holds a batch
    };
    registers.push(Register { kind: m.kind, schema: m.schema, batch, cursor_ptr: null_mut() });
}
```

The zero-column `Batch::empty(0, 16)` special-case in the current `new()` goes
away — zero-column Delta registers get `Some(Batch::with_schema(...))` like any
other Delta.

### Step 4 — update the three mem::replace sites

```rust
// Delay
let curr = reg_mut!(*src).batch.take().unwrap();
let prev = reg_mut!(*state_reg).batch.replace(curr);
reg_mut!(*dst).batch = prev;   // prev was Some(...) — delay shifts the window

// Distinct
let delta = reg_mut!(*in_reg).batch.take().unwrap();
// ... op_distinct(delta, ...) ...

// execute_epoch output extraction
let out = &mut regfile.registers[output_reg as usize];
let result = out.batch.take().filter(|b| b.count > 0);
return Ok(result);   // caller already handles None as "no output"
```

### Step 5 — update clear_delta_batches

Currently resets each Delta register to an empty batch. With `Option<Batch>`,
reset to `Some(Batch::empty_with_schema(&reg.schema))` so the register is ready
for the next epoch without being `None` mid-execution:

```rust
fn clear_delta_batches(&mut self) {
    for reg in &mut self.registers {
        if reg.kind == RegisterKind::Delta {
            if let Some(b) = &mut reg.batch {
                b.clear();
            }
        }
    }
}
```

(`Batch::clear` already resets `count = 0` without deallocating — existing
behaviour, no change needed here.)

### Step 6 — mechanical read/write site updates

All `reg.batch.foo` → `reg.batch().foo` (using the new accessor).
All `reg_mut!(*r).batch = b` → `reg_mut!(*r).batch = Some(b)`.
All `reg_mut!(*r).batch = result.into_inner()` → `... = Some(result.into_inner())`.

The macros `reg!` and `reg_mut!` return `&Register` / `&mut Register`; no macro
changes needed.

---

## Scope

| File | `.batch` accesses | `mem::replace` sites |
|------|-------------------|----------------------|
| `vm.rs` | ~50 | 3 |
| `sal.rs` | 0 | 0 (separate `Batch::empty` pattern, not a replace) |

`sal.rs:653` uses `Batch::empty(npc, 16)` as a per-worker early-return sentinel,
not a `mem::replace` placeholder. That site should become `Batch::empty_with_schema`
and is independent of this plan.

---

## What this is not

This does not affect `Batch::empty(0, 16)` in `vm.rs:647` (the zero-column
initialisation path). That goes away as a consequence of Step 3 above.

This does not change the `Batch::empty` usages in `join.rs`, `linear.rs`,
`reduce.rs`, or test code — those are different patterns (schema-less early
returns and test fixtures) addressed separately if at all.

---

## Implementation order

1. Add `batch()` / `batch_mut()` accessors to `Register` while the field is still
   `Batch`. Mechanical find-and-replace all `reg.batch.` → `reg.batch().` and
   `reg_mut!(*r).batch.` → `reg_mut!(*r).batch_mut().`. Run `make test`.
2. Change the field to `Option<Batch>`, update `new()`, update `clear_delta_batches`.
   Change accessor bodies to `.as_ref().expect(...)`. Fix assignment sites
   (`= b` → `= Some(b)`). Fix the three `mem::replace` sites. Run `make test`.

Splitting into two steps keeps each diff reviewable and keeps tests green at each
checkpoint.
