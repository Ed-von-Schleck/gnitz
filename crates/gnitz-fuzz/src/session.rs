//! A session: one input's traffic played into a real server booted for it,
//! and what the server did judged.
//!
//! The server is the whole multi-process server, master and workers, over a
//! real transport. A session finds a server that stops, one that sends a frame
//! a client cannot decode, one that leaves a request unanswered, and one whose
//! state no client can read back or that does not boot again on its own data —
//! what any client on the network could otherwise do to every other.
//!
//! Every request is followed by a **barrier**: a scan of a relation id no
//! catalog allocates. Replies come in request order, so the barrier's refusal
//! arriving means the request before it was answered whole, however many
//! frames that took — which only the client that built the request knows.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};

use gnitz_core::{block_on, ClientError, ClientTransport, GnitzClient, RelName};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::control::{append_frame, peek_control_block, ControlHeader, Target};
use gnitz_wire::{ClientVerb, ReadBound, ReadSpec, WireStatus, CATALOG_ID_CEILING};

use crate::input::Input;
use crate::seed::{Frame, Seed};

/// The first barrier's relation id: past every id a catalog allocates.
const BARRIER_BASE: u64 = CATALOG_ID_CEILING << 1;
/// How long one session may take before an unanswered request is a finding.
const SESSION_BUDGET: Duration = Duration::from_secs(20);
/// The smallest SAL a server takes: each session fallocates its own.
const SAL_BYTES: &str = "16777216";
/// The SAL bytes between checkpoints of a session that asks for them often.
const CHECKPOINT_BYTES: &str = "65536";

/// What a session found wrong.
#[derive(Debug)]
pub enum Finding {
    /// The master exited or a worker died, during `step`.
    Died { step: Step },
    /// The server sent a frame whose control block the client's decoder refuses.
    Undecodable { error: String },
    /// Frame `frame` went unanswered for the whole session budget.
    Unanswered { frame: usize },
    /// A client could not read the server's state back.
    Unreadable { what: String, error: ClientError },
    /// The server did not boot again on its own data directory.
    NoReboot,
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Finding::Died { step } => write!(f, "the server died during {step}"),
            Finding::Undecodable { error } => write!(f, "the server sent an undecodable frame: {error}"),
            Finding::Unanswered { frame } => write!(f, "frame {frame} was not answered within {SESSION_BUDGET:?}"),
            Finding::Unreadable { what, error } => write!(f, "{what} could not be read back: {error}"),
            Finding::NoReboot => write!(f, "the server did not boot again on its own data"),
        }
    }
}

/// Where in a session a server died.
#[derive(Debug)]
pub enum Step {
    /// Frame `n` of the traffic, or opening the connection it went over.
    Frame(usize),
    /// Reading the session's state back.
    ReadBack,
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Step::Frame(n) => write!(f, "frame {n}"),
            Step::ReadBack => f.write_str("the read-back"),
        }
    }
}

/// How a session reports what it does.
#[derive(Clone, Copy, Debug, Default)]
pub struct Trace {
    /// Print every frame sent and every reply status.
    pub frames: bool,
}

/// One connection of a session, and the barrier it waits for.
struct Conn {
    transport: ClientTransport,
    barrier: u64,
    /// The barrier's refusal arrived, or the server closed the connection.
    settled: bool,
    /// The server closed it: the next frame on it opens it again.
    closed: bool,
}

/// Play `input`'s edit of `seed` into a server booted for it, and judge it.
/// The server is gone when this returns.
pub fn run(input: &Input, seed: &Seed, trace: Trace) -> Result<(), Finding> {
    let boot = input.boot;
    let mut env = vec![("GNITZ_SAL_BYTES", SAL_BYTES)];
    if boot.checkpoints {
        env.push(("GNITZ_CHECKPOINT_BYTES", CHECKPOINT_BYTES));
    }
    let (mut srv, target) = match boot.tls {
        true => {
            let (srv, token) = ServerHandle::start_tls_with_token(boot.workers(), &env);
            let target = srv.token_target(&token);
            (srv, target)
        }
        false => {
            let srv = ServerHandle::start_with_env(boot.workers(), &env);
            let target = srv.sock_path().to_owned();
            (srv, target)
        }
    };
    let deadline = Instant::now() + SESSION_BUDGET;
    let sent = input.edit(seed.sent.clone());
    let judged = play(&mut srv, &target, &sent, deadline, trace)
        .and_then(|()| read_back(&mut srv, &target))
        .and_then(|()| match boot.crash {
            true => std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| srv.restart()))
                .map_err(|_| Finding::NoReboot)
                .and_then(|()| read_back(&mut srv, &target)),
            false => Ok(()),
        });
    if judged.is_err() && trace.frames {
        print_worker_logs(srv.data_dir());
    }
    judged
}

/// The tail of every worker's log under `data_dir`: where a worker's own
/// failure is told, the master's being on its stderr.
fn print_worker_logs(data_dir: &std::path::Path) {
    for w in 0.. {
        let Ok(log) = std::fs::read_to_string(data_dir.join(format!("worker_{w}.log"))) else {
            break;
        };
        let lines: Vec<&str> = log.lines().collect();
        eprintln!("──── worker {w} (last 40 lines) ────");
        lines[lines.len().saturating_sub(40)..]
            .iter()
            .for_each(|l| eprintln!("{l}"));
    }
}

/// Send each of `sent` and wait until it is answered.
fn play(srv: &mut ServerHandle, target: &str, sent: &[Frame], deadline: Instant, trace: Trace) -> Result<(), Finding> {
    let mut conns: BTreeMap<u32, Conn> = BTreeMap::new();
    for (i, frame) in sent.iter().enumerate() {
        let conn = match conns.entry(frame.conn) {
            Entry::Occupied(c) => c.into_mut(),
            Entry::Vacant(v) => match ClientTransport::connect(target, Instant::now() + Duration::from_secs(5)) {
                Ok(transport) => v.insert(Conn {
                    transport,
                    barrier: 0,
                    settled: true,
                    closed: false,
                }),
                // Refused while it runs: the server may cap connections.
                Err(_) if srv.is_alive() => continue,
                Err(_) => return Err(Finding::Died { step: Step::Frame(i) }),
            },
        };
        if trace.frames {
            eprintln!("> conn {} frame {i}: {}", frame.conn, describe(&frame.bytes));
        }
        conn.transport.enqueue(frame.bytes.clone());
        conn.barrier = BARRIER_BASE + i as u64;
        conn.transport.enqueue(barrier(conn.barrier));
        conn.settled = false;
        let key = frame.conn;
        pump(&mut conns, key, deadline, trace).map_err(|e| match e {
            Pumped::Late => Finding::Unanswered { frame: i },
            Pumped::Bad(error) => Finding::Undecodable { error },
        })?;
        if !srv.is_alive() {
            return Err(Finding::Died { step: Step::Frame(i) });
        }
        conns.retain(|_, c| !c.closed);
    }
    Ok(())
}

/// Why [`pump`] stopped short.
enum Pumped {
    Late,
    Bad(String),
}

/// Read every connection until the one keyed `key` has its barrier answered or
/// is closed by the server.
fn pump(conns: &mut BTreeMap<u32, Conn>, key: u32, deadline: Instant, trace: Trace) -> Result<(), Pumped> {
    loop {
        if conns[&key].settled {
            return Ok(());
        }
        let mut fds: Vec<libc::pollfd> = conns
            .values_mut()
            .map(|c| {
                // A flush that fails is a connection the server closed; the
                // read below sees it.
                let _ = c.transport.flush();
                let events = if c.transport.wants_write() {
                    libc::POLLIN | libc::POLLOUT
                } else {
                    libc::POLLIN
                };
                libc::pollfd {
                    fd: c.transport.as_raw_fd(),
                    events,
                    revents: 0,
                }
            })
            .collect();
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(Pumped::Late);
        }
        let ms = left.as_millis().min(i32::MAX as u128) as i32;
        // SAFETY: `fds` is a live array of `fds.len()` pollfds.
        let n = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, ms) };
        if n <= 0 {
            continue;
        }
        for (c, pfd) in conns.values_mut().zip(&fds) {
            if pfd.revents == 0 || c.closed {
                continue;
            }
            let mut bad = None;
            let barrier = c.barrier;
            let mut answered = false;
            let read = c.transport.read(|frame| {
                match peek_control_block(&frame) {
                    Ok(ctrl) => {
                        if trace.frames && ctrl.hdr.target_id < BARRIER_BASE {
                            eprintln!(
                                "<   {:?} target {} lane {:?}",
                                ctrl.hdr.status, ctrl.hdr.target_id, ctrl.hdr.flags.lane
                            );
                        }
                        answered |= ctrl.hdr.target_id == barrier && ctrl.hdr.status != WireStatus::Ok;
                    }
                    Err(e) => bad = Some(e),
                }
                Ok(())
            });
            if let Some(e) = bad {
                return Err(Pumped::Bad(e));
            }
            if answered {
                c.settled = true;
            }
            if read.is_err() {
                c.settled = true;
                c.closed = true;
            }
        }
    }
}

/// A request the server answers with a refusal naming `id`, and nothing else.
fn barrier(id: u64) -> Vec<u8> {
    let mut out = Vec::new();
    append_frame(
        &mut out,
        &ControlHeader::naming(ClientVerb::ScanSpec, Target::from(id), 0),
        &[],
        None,
        None,
    );
    out
}

/// A frame's verb and target, for a trace.
fn describe(frame: &[u8]) -> String {
    match peek_control_block(frame) {
        Ok(c) => format!(
            "{:?} target {} ({} bytes)",
            c.hdr.flags.verb,
            c.hdr.target_id,
            frame.len()
        ),
        Err(e) => format!("undecodable ({e}; {} bytes)", frame.len()),
    }
}

/// Read the whole catalog as a client, and scan every relation in it: what
/// the session left must stay readable. A refusal is an answer; a lost
/// connection or an undecodable reply is not.
fn read_back(srv: &mut ServerHandle, target: &str) -> Result<(), Finding> {
    let lost = |srv: &mut ServerHandle, what: String, error: ClientError| match srv.is_alive() {
        true => Finding::Unreadable { what, error },
        false => Finding::Died { step: Step::ReadBack },
    };
    let mut client = match GnitzClient::connect(target) {
        Ok(c) => c,
        Err(error) => return Err(lost(srv, "a connection".into(), error)),
    };
    let catalog = match block_on(client.catalog()) {
        Ok(c) => c,
        Err(error) => return Err(lost(srv, "the catalog".into(), error)),
    };
    for rel in &catalog.relations {
        let what = rel.qualified();
        let Ok(name) = RelName::new(&rel.schema, &rel.name) else {
            continue;
        };
        let read = block_on(client.resolve_relation(&name)).and_then(|desc| {
            let spec = ReadSpec::all_rows(ReadBound::None);
            block_on(client.scan_spec(Target { tid: desc.tid, token: desc.token }, &spec, &desc.schema)).map(drop)
        });
        match read {
            Ok(()) | Err(ClientError::Refused(_)) => {}
            Err(error) => return Err(lost(srv, what, error)),
        }
    }
    if !srv.is_alive() {
        return Err(Finding::Died { step: Step::ReadBack });
    }
    Ok(())
}
