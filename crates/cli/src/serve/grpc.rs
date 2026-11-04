//! gRPC: `Data`'s RPCs, decoded by tonic and answered by the Router. The token comes from the
//! `authorization` metadata, checked as for HTTP.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use super::Shared;
use super::proto::{self, data_server::Data};
use super::service::{ServiceOp, ServiceReply};

/// `Data` over gRPC.
#[derive(Debug, Clone)]
pub(crate) struct DataGrpc(pub(crate) Arc<Shared>);

impl DataGrpc {
    async fn call<T>(
        &self,
        req: Request<T>,
        op: impl FnOnce(T) -> ServiceOp,
    ) -> Result<ServiceReply, Status> {
        let header = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());
        let caller = self.0.caller(header)?;
        let op = op(req.into_inner());
        let rpc = op.name();
        let reply = self.0.router.dispatch(op, &caller).await;
        self.0.count(rpc, &reply);
        Ok(reply?)
    }
}

fn unexpected() -> Status {
    Status::internal("the service answered with another RPC's reply")
}

#[tonic::async_trait]
impl Data for DataGrpc {
    async fn get(
        &self,
        req: Request<proto::GetRequest>,
    ) -> Result<Response<proto::GetResponse>, Status> {
        match self.call(req, ServiceOp::Get).await? {
            ServiceReply::Get(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    async fn multi_get(
        &self,
        req: Request<proto::MultiGetRequest>,
    ) -> Result<Response<proto::MultiGetResponse>, Status> {
        match self.call(req, ServiceOp::MultiGet).await? {
            ServiceReply::MultiGet(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    async fn read_row(
        &self,
        req: Request<proto::ReadRowRequest>,
    ) -> Result<Response<proto::ReadRowResponse>, Status> {
        match self.call(req, ServiceOp::ReadRow).await? {
            ServiceReply::ReadRow(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    type ScanStream =
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<proto::ScanPage, Status>> + Send>>;

    async fn scan(
        &self,
        _req: Request<proto::ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, Status> {
        // Ticket 3b.
        Err(super::errors::ApiError::unimplemented("Data.Scan").to_status())
    }

    async fn mutate(
        &self,
        req: Request<proto::MutateRequest>,
    ) -> Result<Response<proto::MutateResponse>, Status> {
        match self.call(req, ServiceOp::Mutate).await? {
            ServiceReply::Mutate(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    async fn check_and_mutate(
        &self,
        req: Request<proto::CheckAndMutateRequest>,
    ) -> Result<Response<proto::CheckAndMutateResponse>, Status> {
        match self.call(req, ServiceOp::CheckAndMutate).await? {
            ServiceReply::CheckAndMutate(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    async fn batch(
        &self,
        req: Request<proto::BatchRequest>,
    ) -> Result<Response<proto::MutateResponse>, Status> {
        match self.call(req, ServiceOp::Batch).await? {
            ServiceReply::Mutate(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    async fn mutate_returning(
        &self,
        req: Request<proto::MutateRequest>,
    ) -> Result<Response<proto::MutateReturningResponse>, Status> {
        match self.call(req, ServiceOp::MutateReturning).await? {
            ServiceReply::MutateReturning(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }
}
