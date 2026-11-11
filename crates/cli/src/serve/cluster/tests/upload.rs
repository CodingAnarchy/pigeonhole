//! `Admin.PartitionUpload` and `PartitionUploadStatus` through a cluster node (D317): a
//! file of a partition the node doesn't have open is staged; one it has open is refused;
//! a broken upload resumes from what the status says.

use std::pin::Pin;
use std::task::{Context, Poll};

use sha2::{Digest, Sha256};

use super::*;
use crate::serve::proto::{
    PartitionFile, PartitionUploadChunk, PartitionUploadHeader, PartitionUploadPurpose,
    PartitionUploadStatusRequest, admin_client::AdminClient,
};

/// A client stream fed by a channel, so a test controls when it ends.
struct Chunks(tokio::sync::mpsc::Receiver<PartitionUploadChunk>);

impl futures_core::Stream for Chunks {
    type Item = PartitionUploadChunk;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

async fn admin(port: u16) -> AdminClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    AdminClient::new(channel)
}

fn header(partition: &str, data: &[u8]) -> PartitionUploadHeader {
    let file = PartitionFile {
        name: String::new(),
        bytes: data.len() as u64,
        sha256: Sha256::digest(data).to_vec().into(),
    };
    PartitionUploadHeader {
        partition: partition.into(),
        cluster_id: "c1".into(),
        epoch: 1,
        upload_id: "0123456789abcdef".repeat(2),
        purpose: PartitionUploadPurpose::Restore as i32,
        file: Some(file.clone()),
        manifest: vec![file],
    }
}

/// Sends `chunks` (the first with `header`) and ends the stream.
async fn upload(
    port: u16,
    header: &PartitionUploadHeader,
    chunks: &[(u64, &[u8])],
) -> Result<crate::serve::proto::PartitionUploadReply, tonic::Status> {
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    for (i, (offset, data)) in chunks.iter().enumerate() {
        tx.send(PartitionUploadChunk {
            header: (i == 0).then(|| header.clone()),
            offset: *offset,
            data: data.to_vec().into(),
        })
        .await
        .unwrap();
    }
    drop(tx);
    admin(port)
        .await
        .partition_upload(Chunks(rx))
        .await
        .map(tonic::Response::into_inner)
}

#[test]
fn a_node_stages_a_partition_it_doesnt_hold_open_and_resumes_it() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    let data = vec![7u8; 3 << 19];
    rt.block_on(async {
        // p1 is open on node a: refused.
        let e = upload(ports[0], &header("p1", &data), &[(0, &data[..10])])
            .await
            .unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e:?}");
        // p2 isn't: staged. Two 512 KiB chunks, then the stream ends short.
        let h = header("p2", &data);
        let half = 512 << 10;
        let r = upload(
            ports[0],
            &h,
            &[(0, &data[..half]), (half as u64, &data[half..2 * half])],
        )
        .await
        .unwrap();
        assert_eq!((r.received, r.complete), (2 * half as u64, false));
        let st = admin(ports[0])
            .await
            .partition_upload_status(PartitionUploadStatusRequest {
                upload_id: h.upload_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(st.received, 2 * half as u64);
        // Resumed from there, in 512 KiB chunks: complete.
        let rest: Vec<(u64, &[u8])> = data[2 * half..]
            .chunks(half)
            .enumerate()
            .map(|(i, c)| (((2 + i) * half) as u64, c))
            .collect();
        let r = upload(ports[0], &h, &rest).await.unwrap();
        assert_eq!((r.received, r.complete), (data.len() as u64, true));
        // A chunk over 512 KiB is refused.
        let mut big = header("p3x", &data);
        big.upload_id = "f".repeat(32);
        let e = upload(ports[0], &big, &[(0, &data[..half + 1])])
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
    });
}
