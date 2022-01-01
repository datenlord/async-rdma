use async_rdma::Rdma;
use std::{alloc::Layout, sync::Arc};
use tracing::debug;

async fn example1(rdma: &Rdma) {
    let rmr = Arc::new(rdma.request_remote_mr(Layout::new::<i32>()).await.unwrap());
    let mut lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    unsafe { *(lmr.as_mut_ptr() as *mut i32) = 5 };
    rdma.write(&lmr, rmr.as_ref()).await.unwrap();
    debug!("e1 write");
    rdma.send_mr(rmr.clone()).await.unwrap();
    debug!("e1 send");
}

async fn example2(rdma: &Rdma) {
    let mut lmr = Arc::new(rdma.alloc_local_mr(Layout::new::<i32>()).unwrap());
    unsafe { *(Arc::get_mut(&mut lmr).unwrap().as_mut_ptr() as *mut i32) = 55 };
    rdma.send_mr(lmr.clone()).await.unwrap();
    debug!("e2 send");
}

async fn example3(rdma: &Rdma) {
    let mut lmr = Arc::new(rdma.alloc_local_mr(Layout::new::<i32>()).unwrap());
    unsafe { *(Arc::get_mut(&mut lmr).unwrap().as_mut_ptr() as *mut i32) = 555 };
    rdma.send(lmr.as_ref()).await.unwrap();
    debug!("e3 send");
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    debug!("client start");
    let rdma = Rdma::connect("127.0.0.1:5555", 1, 1, 512).await.unwrap();
    example1(&rdma).await;
    example2(&rdma).await;
    example3(&rdma).await;
    debug!("client done");
}
