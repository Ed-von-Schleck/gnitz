use std::sync::atomic::{AtomicU64, Ordering};
use std::{env, fs, path::PathBuf, process::{Child, Command, Stdio},
          thread, time::Duration};

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct ServerHandle {
    process:       Child,
    pub sock_path: String,
    tmpdir:        PathBuf,
}

impl ServerHandle {
    pub fn start() -> Option<Self> {
        Self::start_n(1)
    }

    pub fn start_n(workers: usize) -> Option<Self> {
        let bin = env::var("GNITZ_SERVER_BIN").unwrap_or_else(|_| {
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../gnitz-server").to_string()
        });
        if !PathBuf::from(&bin).is_file() {
            return None;
        }

        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmpdir = env::temp_dir().join(format!("gnitz_sql_test_{}_{}", std::process::id(), id));
        fs::create_dir_all(&tmpdir).unwrap();
        let data_dir  = tmpdir.join("data");
        let sock_path = tmpdir.join("gnitz.sock");

        let mut cmd = Command::new(&bin);
        cmd.arg(&data_dir).arg(&sock_path)
           .stdout(Stdio::piped()).stderr(Stdio::piped());
        if workers > 1 {
            cmd.arg(format!("--workers={}", workers));
        }
        let mut proc = cmd.spawn().expect("failed to spawn server");

        for _ in 0..100 {
            if sock_path.exists() { break; }
            if let Ok(Some(status)) = proc.try_wait() {
                let out = proc.wait_with_output().unwrap();
                panic!("server exited early ({})\nstderr: {}",
                    status, String::from_utf8_lossy(&out.stderr));
            }
            thread::sleep(Duration::from_millis(100));
        }
        if !sock_path.exists() {
            proc.kill().ok();
            panic!("server did not create socket within 10s");
        }

        Some(ServerHandle {
            process: proc,
            sock_path: sock_path.to_str().unwrap().to_string(),
            tmpdir,
        })
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.process.kill().ok();
        self.process.wait().ok();
        fs::remove_dir_all(&self.tmpdir).ok();
    }
}
