# Surface panic source location in `guard_panic`

`gnitz-engine/src/util.rs::guard_panic` wraps async handlers in
`catch_unwind` and converts panics to clean `Err(...)` returns so the
worker / dispatcher stays alive. The current message is:

```text
internal server error (panic in <op>)
```

That's enough to know the server didn't crash, but tells nothing about
*where* it panicked. Debugging means re-running with `RUST_BACKTRACE=1`
and a tight grep window on `server_debug.log`, or instrumenting the
handler manually. During the compound-PK work, this generic message
hid the actual failure for the better part of an hour each time it
fired.

Fix: capture the panic's file/line via the panic hook and include
them in the error string.

## Scope

In:

- A process-wide panic hook (installed once via `std::sync::Once`)
  that records the last panic's file/line/column into a thread-local.
  Already-set hooks are chained so existing panic-reporting paths
  (e.g. `RUST_BACKTRACE=1` stderr dump) still fire.
- `guard_panic` reads the thread-local on the `Err` path and formats
  the message as:
  ```text
  internal server error (panic in <op> at <file>:<line>:<col>): <message>
  ```
  The `<message>` portion comes from `catch_unwind`'s payload via
  `downcast_ref::<&str>()` / `downcast_ref::<String>()` — the standard
  Rust idiom for extracting the panic message.
- Each existing `guard_panic` call site gets a one-line doc comment
  naming the specific panic source it's guarding against. If the
  answer is "we don't know," that's a code-smell flag to investigate
  before merging — `guard_panic` should not be a catch-all "make the
  test pass" wrapper.

Out:

- Removing `guard_panic` from paths that legitimately need it
  (validators, ingest handlers, etc.). The wrapper itself stays;
  only its error string improves.
- Reporting panic backtraces to clients. Stack traces leak
  implementation details; the file:line is enough for triage from
  the server log.

## Touchpoints

`crates/gnitz-engine/src/util.rs`:

```rust
use std::cell::RefCell;
use std::panic::{set_hook, take_hook, PanicHookInfo};
use std::sync::Once;

thread_local! {
    static LAST_PANIC: RefCell<Option<PanicSite>> = const { RefCell::new(None) };
}

#[derive(Clone, Debug)]
pub(crate) struct PanicSite {
    pub file: String,
    pub line: u32,
    pub col: u32,
    pub message: String,
}

static INSTALL_HOOK: Once = Once::new();

fn ensure_panic_hook_installed() {
    INSTALL_HOOK.call_once(|| {
        let prev = take_hook();
        set_hook(Box::new(move |info: &PanicHookInfo<'_>| {
            let site = info.location().map(|l| PanicSite {
                file: l.file().to_string(),
                line: l.line(),
                col: l.column(),
                message: panic_message(info.payload()),
            });
            if let Some(s) = site.clone() {
                LAST_PANIC.with(|cell| *cell.borrow_mut() = Some(s));
            }
            prev(info);          // keep the default backtrace path alive
        }));
    });
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() { return (*s).to_string(); }
    if let Some(s) = payload.downcast_ref::<String>() { return s.clone(); }
    "<panic payload not a string>".to_string()
}

pub fn guard_panic<T, F>(op: &'static str, f: F) -> Result<T, String>
where F: FnOnce() -> Result<T, String>,
{
    ensure_panic_hook_installed();
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(_) => {
            let site = LAST_PANIC.with(|cell| cell.borrow().clone());
            Err(match site {
                Some(s) => format!(
                    "internal server error (panic in {} at {}:{}:{}): {}",
                    op, s.file, s.line, s.col, s.message,
                ),
                None => format!("internal server error (panic in {})", op),
            })
        }
    }
}
```

The hook installation is idempotent (`Once`) and chains the existing
hook, so `RUST_BACKTRACE=1` still produces a full backtrace on stderr.
The thread-local is read inside the same thread that ran the closure,
so there's no cross-thread leakage.

Existing `guard_panic` callers in `crates/gnitz-engine/src/runtime/`:

- `executor.rs::handle_system_dml` — guards against malformed batches.
  Annotate: `// guards: BatchAppender / column-type mismatch panics`.
- `executor.rs::handle_scan` — guards against catalog-vs-storage
  inconsistencies. Annotate.
- Any other call site grep finds. The annotation comment is the
  audit artefact.

## Implementation sketch — example output

Before:
```text
exec error: server error: internal server error (panic in scan)
```

After:
```text
exec error: server error: internal server error (panic in scan at
gnitz-engine/src/schema.rs:381:18): read_signed: unexpected size 16
```

That single message would have pointed at the exact `unreachable!`
arm of `read_signed` the moment the compound-PK INSERT path tripped
it, cutting straight to the root cause instead of a half-hour of
grepping through the server log.

## Testing

- New unit test in `gnitz-engine/src/util.rs::tests`: a closure that
  deliberately panics inside `guard_panic` produces an `Err` whose
  string contains the file:line of the `panic!()` call.
- A second test verifies the hook is installed at most once
  (calling `guard_panic` twice doesn't stack hooks; `Once` enforces
  this).
- Regression: existing `cargo test -p gnitz-engine` runs that
  expected the old error string format need updating to a
  forward-compatible substring match (e.g. `contains("panic in scan")`
  instead of `assert_eq!`).
