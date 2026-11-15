//! The sending side of `Admin.PartitionUpload`, for `phdb cluster move` and
//! `phdb cluster restore`: one pass of one file. The pass asks the node what it holds, then
//! sends the rest, header first, in chunks the node takes. What a caller does about a break
//! is its own: a move tells the operator to run it again, a restore resumes.

use std::pin::Pin;
use std::task::{Context, Poll};

use prost::bytes::Bytes;

use super::cli::request;
use crate::exit::Failure;
use crate::serve::proto::{
    self, PartitionUploadChunk, PartitionUploadHeader, PartitionUploadReply,
    PartitionUploadStatusRequest,
};

/// A chunk's size when a node doesn't say (`max_chunk` 0, a node from before it).
const DEFAULT_CHUNK: u64 = 512 << 10;

type Admin = proto::admin_client::AdminClient<tonic::transport::Channel>;

/// The most bytes a chunk carries to a node whose status says `max_chunk`: what the node
/// says, since it refuses a longer chunk, or [`DEFAULT_CHUNK`] when it says nothing.
fn chunk_size(max_chunk: u64) -> usize {
    usize::try_from(if max_chunk == 0 {
        DEFAULT_CHUNK
    } else {
        max_chunk
    })
    .unwrap_or(usize::MAX)
}

/// Why a pass of [`upload_file`] didn't finish, in the order a pass can fail.
pub(super) enum Stopped<E> {
    /// A request couldn't be built (the token isn't a header value).
    Request(Failure),
    /// The node refused the status call, or it broke.
    Status(tonic::Status),
    /// The source failed. It is reported before the upload's own answer.
    Source(E),
    /// The node refused the upload, or it broke.
    Upload(tonic::Status),
}

/// The upload's stream ended: the node answered or the call broke, and
/// [`upload_file`] has the reason.
#[derive(Debug)]
pub(super) struct Ended;

/// Where a source puts a file's bytes, in order from where the node's copy ends.
pub(super) struct Feed {
    tx: tokio::sync::mpsc::Sender<PartitionUploadChunk>,
    /// Rides the first message.
    header: Option<PartitionUploadHeader>,
    at: u64,
    max: usize,
}

impl Feed {
    /// The file offset the next byte goes to.
    pub(super) fn at(&self) -> u64 {
        self.at
    }

    /// The most bytes one message carries.
    pub(super) fn max(&self) -> usize {
        self.max
    }

    /// Sends `data` in messages of at most [`max`](Self::max) bytes. The first message of
    /// the pass carries the header, and is sent even when `data` is empty: a file with
    /// nothing left to send still needs its header for the node to finish it (an empty file,
    /// or one whose every byte arrived before a break).
    pub(super) async fn send(&mut self, data: Bytes) -> Result<(), Ended> {
        let mut k = 0;
        while k < data.len() || self.header.is_some() {
            let n = (data.len() - k).min(self.max);
            self.tx
                .send(PartitionUploadChunk {
                    header: self.header.take(),
                    offset: self.at,
                    data: data.slice(k..k + n),
                })
                .await
                .map_err(|_| Ended)?;
            k += n;
            self.at += n as u64;
        }
        Ok(())
    }
}

/// An upload's messages, as tonic's client stream.
struct Chunks(tokio::sync::mpsc::Receiver<PartitionUploadChunk>);

impl futures_core::Stream for Chunks {
    type Item = PartitionUploadChunk;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

/// One pass of `header`'s upload to the node behind `admin`.
///
/// `None`: nothing was sent, because the node holds the whole file, or an install of the
/// partition's set is in progress there (an earlier run stopped in it, and the stamp
/// finishes it, #1848). Otherwise `source` is given the offset the node's copy ends at and
/// a [`Feed`], and runs beside the upload; the upload's stream ends when `source` returns.
/// The result is what `source` returned and the node's reply. The reply's `complete` is
/// the caller's to check.
pub(super) async fn upload_file<T, E, F>(
    admin: &mut Admin,
    token: Option<&str>,
    header: PartitionUploadHeader,
    source: impl FnOnce(u64, Feed) -> F,
) -> Result<Option<(T, PartitionUploadReply)>, Stopped<E>>
where
    F: Future<Output = Result<T, E>>,
{
    let status = request(
        PartitionUploadStatusRequest {
            upload_id: header.upload_id.clone(),
            partition: header.partition.clone(),
        },
        token,
    )
    .map_err(Stopped::Request)?;
    let st = admin
        .partition_upload_status(status)
        .await
        .map_err(Stopped::Status)?
        .into_inner();
    if st.complete || st.installing {
        return Ok(None);
    }
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let feed = Feed {
        tx,
        header: Some(header),
        at: st.received,
        max: chunk_size(st.max_chunk),
    };
    let upload = async {
        let chunks = request(Chunks(rx), token).map_err(Stopped::Request)?;
        admin
            .partition_upload(chunks)
            .await
            .map(tonic::Response::into_inner)
            .map_err(Stopped::Upload)
    };
    // `source` owns the feed, so the upload's stream ends when it returns.
    let (sent, reply) = tokio::join!(source(st.received, feed), upload);
    let sent = sent.map_err(Stopped::Source)?;
    Ok(Some((sent, reply?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(max: usize, at: u64) -> (Feed, tokio::sync::mpsc::Receiver<PartitionUploadChunk>) {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let header = PartitionUploadHeader {
            upload_id: "u".into(),
            ..Default::default()
        };
        (
            Feed {
                tx,
                header: Some(header),
                at,
                max,
            },
            rx,
        )
    }

    /// (has a header, offset, bytes) of each message sent.
    fn sent(mut rx: tokio::sync::mpsc::Receiver<PartitionUploadChunk>) -> Vec<(bool, u64, usize)> {
        let mut out = Vec::new();
        while let Ok(c) = rx.try_recv() {
            out.push((c.header.is_some(), c.offset, c.data.len()));
        }
        out
    }

    #[test]
    fn a_node_that_names_no_chunk_size_gets_the_default_and_a_small_one_is_kept() {
        assert_eq!(chunk_size(0), 512 << 10);
        // Never raised: the node refuses a chunk longer than it said.
        assert_eq!(chunk_size(4), 4);
        assert_eq!(chunk_size(1 << 20), 1 << 20);
    }

    #[tokio::test]
    async fn the_header_rides_the_first_message_and_data_is_cut_to_the_chunk_size() {
        let (mut f, rx) = feed(4, 100);
        f.send(Bytes::from_static(b"0123456789")).await.unwrap();
        f.send(Bytes::from_static(b"ab")).await.unwrap();
        // Nothing left and the header sent: no message.
        f.send(Bytes::new()).await.unwrap();
        assert_eq!(f.at(), 112);
        assert_eq!(
            sent(rx),
            [
                (true, 100, 4),
                (false, 104, 4),
                (false, 108, 2),
                (false, 110, 2)
            ]
        );
    }

    #[tokio::test]
    async fn a_file_with_nothing_left_to_send_still_sends_its_header() {
        let (mut f, rx) = feed(4, 7);
        f.send(Bytes::new()).await.unwrap();
        f.send(Bytes::new()).await.unwrap();
        assert_eq!(sent(rx), [(true, 7, 0)]);
    }

    #[tokio::test]
    async fn a_send_after_the_upload_ended_says_so() {
        let (mut f, rx) = feed(4, 0);
        drop(rx);
        assert!(f.send(Bytes::from_static(b"x")).await.is_err());
    }
}
