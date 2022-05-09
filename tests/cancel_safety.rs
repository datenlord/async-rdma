use async_rdma::LocalMrReadAccess;
use async_rdma::LocalMrWriteAccess;
use async_rdma::Rdma;
use std::{alloc::Layout, io, time::Duration};
mod test_utilities;
use test_utilities::test_server_client;

async fn client(rdma: Rdma) -> io::Result<()> {
    const LEN: usize = 600 * 1024 * 1024;
    let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>())?;
    let mut rmr = rdma
        .request_remote_mr(Layout::new::<[u8; LEN]>())
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_nanos(100), rdma.write(&lmr, &mut rmr)).await;
    assert!(!lmr.is_writeable());
    assert!(lmr.is_readable());
    tokio::time::sleep(Duration::from_secs(1)).await;
    let _ = tokio::time::timeout(Duration::from_nanos(100), rdma.read(&mut lmr, &rmr)).await;
    assert!(!lmr.is_writeable());
    assert!(!lmr.is_readable());
    rdma.send_remote_mr(rmr).await?;
    Ok(())
}

async fn server(rdma: Rdma) -> io::Result<()> {
    let _lmr = rdma.receive_local_mr().await?;
    Ok(())
}

#[test]
fn main() {
    test_server_client(server, client);
}
