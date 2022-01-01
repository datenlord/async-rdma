use async_rdma::{Rdma, RdmaListener};
use std::alloc::Layout;
use tracing::debug;

async fn example1(rdma: &Rdma) {
    let mr = rdma.receive_local_mr().await.unwrap();
    dbg!(unsafe { *(mr.as_ptr() as *mut i32) });
}

async fn example2(rdma: &Rdma) {
    let rmr = rdma.receive_remote_mr().await.unwrap();
    debug!("e2 receive");
    let mut lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    rdma.read(&mut lmr, rmr.as_ref()).await.unwrap();
    debug!("e2 read");
    dbg!(unsafe { *(lmr.as_ptr() as *mut i32) });
}

async fn example3(rdma: &Rdma) {
    let mut lmr = rdma.receive().await.unwrap();
    debug!("e3 lmr : {:?}", unsafe { *(lmr.as_ptr() as *mut i32) });
    dbg!(unsafe { *(lmr.as_mut_ptr() as *mut i32) });
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    debug!("server start");
    let rdmalistener = RdmaListener::bind("127.0.0.1:5555").await.unwrap();
    let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
    debug!("accepted");
    example1(&rdma).await;
    example2(&rdma).await;
    example3(&rdma).await;
    println!("server done");
}
