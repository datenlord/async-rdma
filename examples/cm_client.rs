//! This demo shows how to establish a connection between server and client
//! and send msg to the other end.
//!
//! You can try this example by running:
//!
//!     cargo run --example cm_server
//!
//! And then start client in another terminal by running:
//!
//!     cargo run --example cm_client <server_ip> <port>
//!
//! The default port is 7471.

use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};
use std::{alloc::Layout, env, io::Write, process::exit};
const BUF_SIZE: usize = 16;
const BUF_FILLER: u8 = 1;

async fn run(node: &str, service: &str) {
    // send/recv raw data ,so we don't need agent here
    let rdma = RdmaBuilder::default()
        .cm_connect(node, service)
        .await
        .unwrap();

    // send raw data to server
    let mut lmr = rdma
        .alloc_local_mr(Layout::new::<[u8; BUF_SIZE]>())
        .unwrap();
    let _ = lmr.as_mut_slice().write(&[BUF_FILLER; BUF_SIZE]).unwrap();
    let _ = rdma.send_raw(&lmr).await.unwrap();
    println!("send {:?}", *lmr.as_slice());

    // recv raw data from server
    let recv_mr = rdma
        .receive_raw(Layout::new::<[u8; BUF_SIZE]>())
        .await
        .unwrap();
    assert_eq!(*recv_mr.as_slice(), [BUF_FILLER; BUF_SIZE]);
    println!("recv: {:?}", *recv_mr.as_slice());
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    println!("cm_client: start");

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("usage : cargo run --example client <server_ip> <port>");
        println!("input : {:?}", args);
        exit(-1);
    }
    let ip = args.get(1).unwrap().as_str();
    let port = args.get(2).unwrap().as_str();

    run(ip, port).await;
    println!("cm_client: end");
}
