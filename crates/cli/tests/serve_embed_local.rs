//! `phdb serve --memory --embed-local` end to end, with the model (#1673, cli692's N4 on
//! #1671): the `phdb` binary, started as an operator starts it, embeds the `Memory` service's
//! text in the process, pins a new store to the model, refuses a client's embedding, and
//! runs the embedder's threads where the server's placement puts the network side.
//!
//! It needs the model's files: `PIGEONHOLE_EMBED_MODEL_DIR` names them (CI's
//! `embed-reference` job restores them), and without it the test is skipped.
#![cfg(all(feature = "serve", feature = "embed-local", unix))]

mod common;

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use common::TempDir;
use serde_json::{Value, json};

/// The model `--embed-local` loads, as a store is pinned to it.
const MODEL: &str = "BAAI/bge-small-en-v1.5@5c38ec7c";

/// A running `phdb serve`, its stderr read line by line.
struct Serve {
    child: Child,
    url: String,
    stderr: Receiver<String>,
    /// The lines read so far.
    seen: Vec<String>,
    agent: ureq::Agent,
}

impl Serve {
    fn start(db: &std::path::Path, model_dir: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_phdb"))
            .arg("serve")
            .arg(db)
            .args(["--create", "--shards", "1", "--listen", "127.0.0.1:0"])
            .args([
                "--memory",
                "m",
                "--embed-local",
                "--embed-model-dir",
                model_dir,
            ])
            // The advice line names the CPU list only while these are unset.
            .env_remove("CANDLE_CPU_MASK")
            .env_remove("CANDLE_NUM_THREADS")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let (tx, stderr) = std::sync::mpsc::channel();
        let lines = BufReader::new(child.stderr.take().unwrap());
        std::thread::spawn(move || {
            for line in lines.lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        let mut seen = Vec::new();
        let url = loop {
            // Loading the model is part of the start.
            let line = match stderr.recv_timeout(Duration::from_secs(120)) {
                Ok(line) => line,
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("phdb serve exited: {}", seen.join("\n"))
                }
                Err(e) => panic!("phdb serve didn't start ({e}): {}", seen.join("\n")),
            };
            let addr = line
                .strip_prefix("phdb serve: serving")
                .and_then(|rest| rest.split(" on ").nth(1))
                .and_then(|rest| rest.split_whitespace().next())
                .map(str::to_owned);
            seen.push(line);
            if let Some(addr) = addr {
                break format!("http://{addr}/v1");
            }
        };
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        Self {
            child,
            url,
            stderr,
            seen,
            agent,
        }
    }

    /// `POST /v1/{path}`: its status and JSON body.
    fn rpc(&self, path: &str, body: &Value) -> (u16, Value) {
        let mut resp = self
            .agent
            .post(&format!("{}/{path}", self.url))
            .send_json(body)
            .unwrap();
        let status = resp.status().as_u16();
        (status, resp.body_mut().read_json().unwrap())
    }

    fn remember(&self, text: &str) -> String {
        let (status, r) = self.rpc(
            "Memory/Remember",
            &json!({"namespace": "a", "memory": {"text": text}}),
        );
        assert_eq!(status, 200, "{r}");
        r["id"].as_str().unwrap().to_owned()
    }

    /// The stderr lines written so far: everything the start wrote (the advice follows the
    /// serving line before a request is served), read until the reader has been quiet a
    /// moment.
    #[cfg(target_os = "linux")]
    fn lines(&mut self) -> &[String] {
        while let Ok(line) = self.stderr.recv_timeout(Duration::from_millis(200)) {
            self.seen.push(line);
        }
        &self.seen
    }

    /// Stops the server as an operator does (SIGTERM): its exit status.
    fn stop(mut self) -> (i32, String) {
        let killed = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .unwrap();
        assert!(killed.success());
        let status = self.child.wait().unwrap();
        let rest: Vec<String> = self.stderr.iter().collect();
        self.seen.extend(rest);
        (status.code().unwrap_or(-1), self.seen.join("\n"))
    }
}

#[test]
fn serve_embed_local_embeds_text_in_the_process() {
    let Some(model_dir) = std::env::var("PIGEONHOLE_EMBED_MODEL_DIR").ok() else {
        eprintln!("PIGEONHOLE_EMBED_MODEL_DIR is unset: skipping the local model");
        return;
    };
    let dir = TempDir::new("serve-embed-local");
    let path = dir.path().join("s.phdb");
    #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
    let mut s = Serve::start(&path, &model_dir);

    // Text in, embedded by the server: a query sharing no keyword finds its memory.
    let pg = s.remember("The March planning meeting chose Postgres for the billing service.");
    s.remember("Lunch on Friday was tacos from the place on Fifth Street.");
    let (status, hits) = s.rpc(
        "Memory/Recall",
        &json!({"namespaces": ["a"], "text": "which database did we pick?", "k": 1}),
    );
    assert_eq!(status, 200, "{hits}");
    assert_eq!(hits["hits"][0]["id"].as_str(), Some(pg.as_str()), "{hits}");

    // A client's embedding is refused: the server computes them.
    let data: Vec<u8> = std::iter::repeat_n(0.0f32.to_le_bytes(), 384)
        .flatten()
        .collect();
    let given = json!({"namespace": "a", "memory": {"text": "x", "embedding": {
        "element": "ELEMENT_F32", "dim": 384, "data": b64(&data)}}});
    let (status, e) = s.rpc("Memory/Remember", &given);
    assert_eq!(
        (status, e["code"].as_str()),
        (400, Some("InvalidArgument")),
        "{e}"
    );
    assert!(
        e["message"].as_str().unwrap().contains("send text only"),
        "{e}"
    );

    // The embedder's threads run where the network side's do.
    #[cfg(target_os = "linux")]
    embedder_threads_follow_the_placement(s.child.id(), s.lines());

    let (status, err) = s.stop();
    assert_eq!(status, 0, "{err}");

    // The store `--create` made is pinned to the model.
    let db = pigeonhole::Pigeonhole::open(&path, pigeonhole::Options::default().pin_threads(false))
        .unwrap();
    let store = pigeonhole_memory::Store::open(&db, "m", MODEL).unwrap();
    let model = store.embedding_model().unwrap();
    assert_eq!((model.id(), model.dim()), (MODEL, 384));
    drop(store);
    db.close().unwrap();
}

/// On Linux: each of the embedder's pool threads (`pigeonhole-embed-N`, `comm` cut to 15
/// bytes) may run on the CPUs the server's placement gives the network side. With the
/// shards pinned, that's the list the startup advice names for `CANDLE_CPU_MASK`; unpinned,
/// it's the process's own CPUs.
#[cfg(target_os = "linux")]
fn embedder_threads_follow_the_placement(pid: u32, stderr: &[String]) {
    let allowed = |status: &str| {
        status
            .lines()
            .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
            .map(|v| v.trim().to_owned())
            .unwrap()
    };
    let advised = stderr.iter().find_map(|l| {
        l.split_whitespace()
            .find_map(|w| w.strip_prefix("CANDLE_CPU_MASK="))
            .map(|v| v.trim_end_matches([',', '.', ';']).to_owned())
    });
    let want = match &advised {
        Some(list) => list.clone(),
        None => {
            eprintln!(
                "the shards aren't pinned here, so there's no network mask: checking the \
                 embedder's threads against the process's CPUs\n{}",
                stderr.join("\n")
            );
            allowed(&std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap())
        }
    };
    let mut pool = 0;
    for task in std::fs::read_dir(format!("/proc/{pid}/task")).unwrap() {
        let task = task.unwrap().path();
        let Ok(comm) = std::fs::read_to_string(task.join("comm")) else {
            continue;
        };
        if !comm.starts_with("pigeonhole-embe") {
            continue;
        }
        pool += 1;
        let status = std::fs::read_to_string(task.join("status")).unwrap();
        assert_eq!(allowed(&status), want, "{}: {comm}", task.display());
    }
    assert!(pool > 0, "no pigeonhole-embed threads in {pid}");
}

/// Standard base64 with padding, as the JSON mapping writes `bytes`.
fn b64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | u32::from(*b) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}
