#![cfg(feature = "cancel_safety_test")]
use async_rdma::LocalMrReadAccess;
use async_rdma::LocalMrWriteAccess;
use async_rdma::Rdma;
use std::{alloc::Layout, io, time::Duration};
mod test_utilities;
use test_utilities::test_server_client;

async fn client(rdma: Rdma) -> io::Result<()> {
    const LEN: usize = 10;
    let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>())?;
    let mut rmr = rdma
        .request_remote_mr(Layout::new::<[u8; LEN]>())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rdma.write(&lmr, &mut rmr))
            .await
            .is_err()
    );
    assert!(!lmr.is_writeable());
    assert!(lmr.is_readable());
    assert!(rdma.read(&mut lmr, &rmr).await.is_err());
    assert!(rdma.write(&lmr, &mut rmr).await.is_ok());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rdma.read(&mut lmr, &rmr))
            .await
            .is_err()
    );
    assert!(!lmr.is_writeable());
    assert!(!lmr.is_readable());
    assert!(rdma.read(&mut lmr, &rmr).await.is_err());
    assert!(rdma.write(&lmr, &mut rmr).await.is_err());
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
