//! This demo shows how to establish a connection between server and client
//! and the usage of rdma `read`, `write` and `send&recv` APIs.
//!
//! You can try this example by running:
//!
//!     cargo run --example server
//!
//! And start client in another terminal by running:
//!
//!     cargo run --example client

use async_rdma::{LocalMrReadAccess, Rdma, RdmaListener};
use std::alloc::Layout;
use tracing::debug;

async fn read_rmr_from_client(rdma: &Rdma) {
    let mut lmr = rdma.alloc_local_mr(Layout::new::<char>()).unwrap();
    let rmr = rdma.receive_remote_mr().await.unwrap();
    rdma.read(&mut lmr, &rmr).await.unwrap();
    dbg!(unsafe { *(*lmr.as_ptr() as *const char) });
}

async fn receive_after_being_written(rdma: &Rdma) {
    let lmr = rdma.receive_local_mr().await.unwrap();
    dbg!(unsafe { *(*lmr.as_ptr() as *const char) });
}

async fn receive_data_from_client(rdma: &Rdma) {
    let lmr = rdma.receive().await.unwrap();
    dbg!(unsafe { *(*lmr.as_ptr() as *const char) });
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    debug!("server start");
    let rdmalistener = RdmaListener::bind("127.0.0.1:5555").await.unwrap();
    let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
    debug!("accepted");
    read_rmr_from_client(&rdma).await;
    receive_after_being_written(&rdma).await;
    receive_data_from_client(&rdma).await;
    debug!("server done");
}
