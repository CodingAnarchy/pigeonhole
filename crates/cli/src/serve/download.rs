//! The receiving side of `Admin.BackupDownload`: a copy taken in order from a node, resumed
//! after a broken stream. A replica's bootstrap and `phdb cluster backup` both take theirs
//! through [`download`]; `phdb cluster move` reads one pass of its own with the same bounds.
//!
//! - **Every wait is bounded.** The node answers at once and sends a heartbeat (a chunk with
//!   no data) every second while it writes its copy (#1611). So the answer and each chunk
//!   are waited for only so long ([`Waits`]), and a stream that stalls with its connection
//!   up is a break, not a hang (#1595).
//! - **A break resumes** from the bytes written, on a new connection: a stalled connection
//!   stays stalled for every later request on it. Three breaks in a row without progress
//!   give up ([`Breaks`]).
//! - **A copy that is gone** (a resume's `NotFound`: it failed on the node, or expired)
//!   starts again from nothing, within the same tries.
//! - **A message is at most [`MAX_MESSAGE`] bytes.** A node sends chunks of 1 MiB.

use std::time::Duration;

use prost::bytes::Bytes;

use super::proto::{self, BackupChunk, BackupDownloadRequest};

/// A node's `Admin` client.
pub(crate) type Admin = proto::admin_client::AdminClient<tonic::transport::Channel>;

/// The longest message a download decodes: four of the chunks a node sends
/// ([`CHUNK`](super::backup::CHUNK)), plus framing. A longer one fails the download
/// ([`Failed::TooLong`]), so a peer can't make this side hold a message of any size.
pub(crate) const MAX_MESSAGE: usize = (4 << 20) + (16 << 10);

/// The longest an operator's download (`phdb cluster backup`, `phdb cluster move`) waits
/// for a node's answer or its next chunk. A node sends a heartbeat every second while it
/// writes a copy and data without pause after, so a minute of nothing is a stalled stream.
pub(crate) const STALL: Duration = Duration::from_secs(60);

/// `channel`'s `Admin` client, decoding messages of at most [`MAX_MESSAGE`] bytes.
pub(crate) fn admin(channel: tonic::transport::Channel) -> Admin {
    Admin::new(channel).max_decoding_message_size(MAX_MESSAGE)
}

/// Whether `status` is this side refusing a message over its decoding limit.
pub(crate) fn too_long(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::OutOfRange
        && status
            .message()
            .contains("decoded message length too large")
}

/// How long a download waits.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Waits {
    /// For the node's answer, and for each chunk until the copy's data starts (the node may
    /// be writing its copy, sending heartbeats).
    pub answer: Duration,
    /// For each chunk once data has arrived.
    pub data: Duration,
}

/// Where a download's bytes go.
pub(crate) trait Sink {
    /// The copy's next bytes, in order.
    async fn write(&mut self, data: &Bytes) -> std::io::Result<()>;
    /// The copy is gone and a fresh one starts: forget everything written.
    async fn restart(&mut self) -> std::io::Result<()>;
}

/// Why a pass broke. The caller words it ([`download`]'s `say`).
#[derive(Debug)]
pub(crate) enum Break<'a> {
    /// No connection for this pass.
    Connect(&'a str),
    /// No answer to the request within this long.
    NoAnswer(Duration),
    /// No chunk within this long.
    NoChunk(Duration),
    /// The stream ended before the copy's last chunk.
    Ended,
    /// The node's error, or the transport's.
    Status(&'a tonic::Status),
}

/// What a download tells its caller as it goes.
#[derive(Debug)]
pub(crate) enum Step<'a> {
    /// Asking for a copy from its start: the node may take a while to write it.
    Waiting,
    /// This many bytes are written.
    Data(u64),
    /// A pass broke for this reason, and the download resumes.
    Resuming(&'a str),
    /// The copy is gone (the node's reason), and a fresh one starts.
    Fresh(&'a str),
}

/// Why a download failed.
#[derive(Debug)]
pub(crate) enum Failed {
    /// A request couldn't be built.
    Request(String),
    /// The node refused the download before it named a copy.
    Refused(tonic::Status),
    /// Three breaks in a row without progress, or a break before any copy was named: the
    /// first break's reason since the last progress.
    GaveUp(String),
    /// The sink failed.
    Sink(std::io::Error),
    /// A chunk came out of order.
    Offset { got: u64, expected: u64 },
    /// The node sent a message over [`MAX_MESSAGE`] bytes (tonic's text).
    TooLong(String),
}

/// A finished download.
#[derive(Debug)]
pub(crate) struct Copy {
    /// The copy's last chunk: its total, SHA-256 and identity. Its data is written.
    pub last: BackupChunk,
    /// Bytes written.
    pub bytes: u64,
    /// The copy's feed token, from the first chunk that carried one.
    pub feed_token: Bytes,
}

/// A download's breaks: three in a row without progress give up, and progress starts the
/// count again (cli692's N1 on #1583), so a long copy over a flaky link isn't restarted. The
/// error a download that gives up reports is the first break since its last progress
/// (cli692's R2 and N5 on #1628): a copy that failed on the node says why, through the
/// NotFound of the resumes after it, and an old blip isn't a later failure's reason.
#[derive(Debug, Default)]
pub(crate) struct Breaks {
    tries: u64,
    /// The offset reached at the last break.
    at_last_break: u64,
    /// The first break's reason since the last progress.
    cause: Option<String>,
}

impl Breaks {
    /// A break at `offset` for `why`. Returns the error to give up with, after the third in a
    /// row without progress.
    fn note(&mut self, offset: u64, why: &str) -> Option<String> {
        if offset > self.at_last_break {
            self.tries = 0;
            self.at_last_break = offset;
            self.cause = None;
        }
        self.cause.get_or_insert_with(|| why.to_owned());
        self.tries += 1;
        (self.tries > 3).then(|| self.cause())
    }

    /// The copy is gone (`why`, a resume's NotFound): a fresh copy starts at offset 0, within
    /// the same tries, and the cause stays the break before it. Returns the error to give up
    /// with, as for [`note`](Self::note).
    fn gone(&mut self, why: &str) -> Option<String> {
        self.at_last_break = 0;
        self.tries += 1;
        (self.tries > 3).then(|| self.cause.clone().unwrap_or_else(|| why.to_owned()))
    }

    /// The reason to report now: the first break since the last progress.
    fn cause(&self) -> String {
        self.cause.clone().unwrap_or_default()
    }
}

/// Downloads the copy `template` asks for into `sink`, resuming after a break.
///
/// - `first` is the connection the first pass uses; `connect` makes each later one, and a
///   failed connect is that pass's break (cli692's R2 on #1609), so a blip doesn't restart
///   a long download.
/// - `request` wraps a message as the caller's requests are (its token).
/// - `template`'s `backup_id` and `offset` are set for each pass.
/// - `say` words a break, for [`Step::Resuming`] and for [`Failed::GaveUp`].
/// - `step` hears how it goes.
///
/// It returns once the copy's last chunk (the one with its SHA-256) is written. Checking
/// the copy against that chunk is the caller's.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn download<E, C>(
    first: Admin,
    mut connect: impl FnMut() -> C,
    request: impl Fn(BackupDownloadRequest) -> Result<tonic::Request<BackupDownloadRequest>, E>,
    template: BackupDownloadRequest,
    sink: &mut impl Sink,
    waits: Waits,
    say: impl Fn(Break<'_>) -> String,
    mut step: impl FnMut(Step<'_>),
) -> Result<Copy, Failed>
where
    E: std::fmt::Display,
    C: Future<Output = Result<Admin, String>> + Send,
{
    // `None` after a break: the next pass connects again.
    let mut client = Some(first);
    let (mut backup_id, mut offset, mut feed_token) = (String::new(), 0u64, Bytes::new());
    let mut breaks = Breaks::default();
    loop {
        let req = request(BackupDownloadRequest {
            backup_id: backup_id.clone(),
            offset,
            ..template.clone()
        })
        .map_err(|e| Failed::Request(e.to_string()))?;
        if offset == 0 {
            step(Step::Waiting);
        }
        let connected = match client.take() {
            Some(c) => Ok(c),
            None => connect().await,
        };
        let broke = match connected {
            Err(e) => say(Break::Connect(&e)),
            // Dropped with this pass: a broken stream's connection isn't reused.
            Ok(mut c) => {
                let answered = tokio::time::timeout(waits.answer, c.backup_download(req)).await;
                match answered {
                    Err(_) => say(Break::NoAnswer(waits.answer)),
                    Ok(Ok(r)) => {
                        let mut stream = r.into_inner();
                        // Until the copy's data starts, the node may be writing it.
                        let mut wait = waits.answer;
                        loop {
                            let Ok(next) = tokio::time::timeout(wait, stream.message()).await
                            else {
                                break say(Break::NoChunk(wait));
                            };
                            match next {
                                Ok(Some(chunk)) => {
                                    if chunk.offset != offset {
                                        return Err(Failed::Offset {
                                            got: chunk.offset,
                                            expected: offset,
                                        });
                                    }
                                    backup_id.clone_from(&chunk.backup_id);
                                    if feed_token.is_empty() {
                                        feed_token = chunk.feed_token.clone();
                                    }
                                    // A heartbeat while the node writes its copy carries no
                                    // data: still waiting for the copy.
                                    if !chunk.data.is_empty() {
                                        sink.write(&chunk.data).await.map_err(Failed::Sink)?;
                                        offset += chunk.data.len() as u64;
                                        step(Step::Data(offset));
                                        wait = waits.data;
                                    }
                                    if !chunk.sha256.is_empty() {
                                        return Ok(Copy {
                                            last: chunk,
                                            bytes: offset,
                                            feed_token,
                                        });
                                    }
                                }
                                Ok(None) => break say(Break::Ended),
                                Err(status) if too_long(&status) => {
                                    return Err(Failed::TooLong(status.message().to_owned()));
                                }
                                Err(status) => break say(Break::Status(&status)),
                            }
                        }
                    }
                    Ok(Err(status)) if backup_id.is_empty() => {
                        return Err(Failed::Refused(status));
                    }
                    // The copy is gone: it failed on the node (its error came first, as this
                    // break's cause) or expired. Start again with a fresh copy, within the
                    // same tries (cli692's R2 on #1628).
                    Ok(Err(status)) if status.code() == tonic::Code::NotFound => {
                        backup_id.clear();
                        offset = 0;
                        feed_token = Bytes::new();
                        sink.restart().await.map_err(Failed::Sink)?;
                        if let Some(why) = breaks.gone(status.message()) {
                            return Err(Failed::GaveUp(why));
                        }
                        step(Step::Fresh(status.message()));
                        continue;
                    }
                    Ok(Err(status)) => say(Break::Status(&status)),
                }
            }
        };
        let give_up = breaks.note(offset, &broke);
        // A fresh request that broke before its answer named a copy: nothing to resume.
        if let Some(why) = give_up.or_else(|| backup_id.is_empty().then(|| breaks.cause())) {
            return Err(Failed::GaveUp(why));
        }
        step(Step::Resuming(&broke));
        tokio::time::sleep(Duration::from_millis(200 * breaks.tries)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::Breaks;

    #[test]
    fn a_download_gives_up_with_the_first_break_since_its_last_progress() {
        // Three breaks in a row without progress give up, with the first one's reason.
        let mut b = Breaks::default();
        assert_eq!(b.note(0, "stalled"), None);
        assert_eq!(b.note(0, "refused"), None);
        assert_eq!(b.note(0, "refused").as_deref(), None);
        assert_eq!(b.note(0, "refused").as_deref(), Some("stalled"));
        // Progress starts the count and the cause again (cli692's N5 on #1628): an early blip
        // isn't the reason a later failure reports.
        let mut b = Breaks::default();
        assert_eq!(b.note(0, "an early blip"), None);
        assert_eq!(b.note(1 << 20, "stalled"), None);
        assert_eq!(b.note(1 << 20, "refused"), None);
        assert_eq!(b.note(1 << 20, "refused"), None);
        assert_eq!(b.note(1 << 20, "refused").as_deref(), Some("stalled"));
        // A copy that failed on the node (R2): its error, then the resumes' NotFound, each a
        // fresh copy within the same tries; giving up reports the copy's error.
        let mut b = Breaks::default();
        assert_eq!(b.note(0, "disk full"), None);
        assert_eq!(b.gone("no such backup download"), None);
        assert_eq!(b.note(0, "disk full"), None);
        assert_eq!(
            b.gone("no such backup download").as_deref(),
            Some("disk full")
        );
        // Gone with no break before it: the NotFound itself.
        let mut b = Breaks {
            tries: 3,
            ..Breaks::default()
        };
        assert_eq!(
            b.gone("no such backup download").as_deref(),
            Some("no such backup download")
        );
    }
}
