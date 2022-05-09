mod test_utilities;
use async_rdma::{LocalMrReadAccess, MrAccess, Rdma};
use std::io;
use std::{alloc::Layout, sync::Arc, time::Duration};
use test_utilities::*;

async fn server(rdma: Rdma) -> io::Result<()> {
    let rdma = Arc::new(rdma);
    let mut handles = vec![];
    for _ in 0..10 {
        let rdma_clone = rdma.clone();
        handles.push(tokio::spawn(async move {
            let lm = rdma_clone.receive().await.unwrap();
            assert_eq!(unsafe { *(*lm.as_ptr() as *mut i32) }, 5);
            assert_eq!(lm.length(), 4);
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    // wait for the agent thread to send all reponses to the remote.
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(())
}

async fn client(rdma: Rdma) -> io::Result<()> {
    let rdma = Arc::new(rdma);
    let mut handles = vec![];
    for _ in 0..10 {
        let rdma_clone = rdma.clone();
        // `send` is faster than `receive` because the workflow of `receive` operation is
        // more complex. If we run server and client in the same machine like this test and
        // `send` without `sleep`, the receiver will be too busy to response sender.
        // So the sender's RDMA netdev will retry again and again which make the situation worse.
        // You can skip this `sleep` if your receiver's machine is fast enough.
        tokio::time::sleep(Duration::from_millis(1)).await;
        handles.push(tokio::spawn(async move {
            let lm = rdma_clone.alloc_local_mr(Layout::new::<i32>()).unwrap();
            unsafe { *(*lm.as_ptr() as *mut i32) = 5 };
            rdma_clone.send(&lm).await.unwrap();
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    Ok(())
}

#[test]
fn main() {
    test_server_client(server, client);
}
