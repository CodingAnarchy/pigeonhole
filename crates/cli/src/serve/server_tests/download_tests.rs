use sha2::Digest;

use super::replica_tests::stalling_proxy;
use super::*;
use crate::serve::download::{Break, Copy, Failed, Sink, Step, Waits, admin, download};

// ---- The receiving side of Admin.BackupDownload (#1830) ----

/// A copy kept in memory.
#[derive(Default)]
struct Mem {
    bytes: Vec<u8>,
    restarts: usize,
}

impl Sink for Mem {
    async fn write(&mut self, data: &prost::bytes::Bytes) -> std::io::Result<()> {
        self.bytes.extend_from_slice(data);
        Ok(())
    }

    async fn restart(&mut self) -> std::io::Result<()> {
        self.bytes.clear();
        self.restarts += 1;
        Ok(())
    }
}

/// A server holding `mib` MiB of rows that don't compress, so its copy spans that many of
/// the 1 MiB chunks a download sends.
fn server_with(mib: usize) -> Server {
    let s = Server::start("", limits());
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..16 * mib {
        let value: Vec<u8> = (0..64 << 10)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let (status, body) = s.post(
            "Mutate",
            &put(&format!("r{i:03}"), "f", "q", json!({"bytes": b64(&value)})),
            None,
        );
        assert_eq!(status, 200, "{body}");
    }
    s
}

async fn connect(url: &str) -> Result<crate::serve::download::Admin, String> {
    tonic::transport::Endpoint::from_shared(url.to_owned())
        .map_err(|e| e.to_string())?
        .connect()
        .await
        .map(admin)
        .map_err(|e| e.to_string())
}

fn say(b: Break<'_>) -> String {
    match b {
        Break::Connect(e) => format!("connect: {e}"),
        Break::NoAnswer(_) => "no answer".to_owned(),
        Break::NoChunk(_) => "no chunk".to_owned(),
        Break::Ended => "ended".to_owned(),
        Break::Status(s) => s.message().to_owned(),
    }
}

/// Downloads `url`'s backup with `client` first, counting the breaks it resumed from.
async fn take(
    url: &str,
    client: crate::serve::download::Admin,
    waits: Waits,
) -> (Result<Copy, Failed>, Mem, Vec<String>) {
    let mut sink = Mem::default();
    let mut resumed = Vec::new();
    let got = download(
        client,
        || connect(url),
        |m| Ok::<_, String>(tonic::Request::new(m)),
        proto::BackupDownloadRequest {
            durable: true,
            ..Default::default()
        },
        &mut sink,
        waits,
        say,
        |s| {
            if let Step::Resuming(why) = s {
                resumed.push(why.to_owned());
            }
        },
    )
    .await;
    (got, sink, resumed)
}

#[test]
fn a_download_that_stalls_at_every_chunk_finishes_while_each_pass_makes_progress() {
    // The rule a cluster backup takes from the replica's bootstrap (#1830): breaks are
    // counted in a row without progress, not in total. Each connection through the proxy
    // carries one 1 MiB chunk and then stalls, so a copy of 7 MiB and more breaks at least
    // six times, twice the three that give up, and still arrives whole and in order.
    let s = server_with(7);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let upstream = s.url.trim_start_matches("http://").to_owned();
    let proxy = stalling_proxy(&upstream, 3 << 19, None, Arc::clone(&stop));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (got, sink, resumed) = rt.block_on(async {
        let waits = Waits {
            answer: Duration::from_secs(5),
            data: Duration::from_millis(300),
        };
        take(&proxy, connect(&proxy).await.unwrap(), waits).await
    });
    stop.store(true, Ordering::Relaxed);
    let copy = got.unwrap_or_else(|e| panic!("{e:?}, after {resumed:?}"));
    assert!(resumed.len() >= 6, "{resumed:?}");
    assert!(resumed.iter().all(|why| why == "no chunk"), "{resumed:?}");
    assert_eq!(sink.restarts, 0, "resumed, never restarted");
    assert_eq!(copy.bytes, sink.bytes.len() as u64);
    assert_eq!(copy.last.total, copy.bytes);
    assert_eq!(
        sha2::Sha256::digest(&sink.bytes).as_slice(),
        copy.last.sha256.as_ref()
    );
}

#[test]
fn a_download_refuses_a_message_over_its_limit_and_doesnt_resume() {
    // The cap a replica's bootstrap now has too: a chunk longer than the client decodes
    // fails the download at once, by name, rather than counting as a break to resume from.
    let s = server_with(1);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (got, _, resumed) = rt.block_on(async {
        let channel = tonic::transport::Endpoint::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        // Smaller than one chunk; the heartbeats and the answer still fit.
        let small = crate::serve::download::Admin::new(channel).max_decoding_message_size(64 << 10);
        let waits = Waits {
            answer: Duration::from_secs(5),
            data: Duration::from_secs(5),
        };
        take(&s.url, small, waits).await
    });
    assert!(resumed.is_empty(), "{resumed:?}");
    match got {
        Err(Failed::TooLong(why)) => assert!(why.contains("too large"), "{why}"),
        other => panic!("{other:?}"),
    }
}
