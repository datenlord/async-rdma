mod test_utilities;
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma};
use std::{alloc::Layout, io, time::Duration};
use test_utilities::test_server_client;
use tokio::time::sleep;

mod send_local_mr {
    use super::*;

    async fn server(rdma: Rdma) -> io::Result<()> {
        let mut lmr = rdma.alloc_local_mr(Layout::new::<char>()).unwrap();
        let rmr = rdma.receive_remote_mr().await.unwrap();
        sleep(Duration::from_millis(200)).await;
        // timeout, should panic
        rdma.read(&mut lmr, &rmr).await.unwrap();
        dbg!(unsafe { *(*lmr.as_ptr() as *const char) });
        Ok(())
    }

    async fn client(rdma: Rdma) -> io::Result<()> {
        let mut lmr = rdma.alloc_local_mr(Layout::new::<char>()).unwrap();
        unsafe { *(*lmr.as_mut_ptr() as *mut char) = 't' };
        rdma.send_local_mr_with_timeout(lmr, Duration::from_millis(100))
            .await
            .unwrap();
        Ok(())
    }

    #[should_panic]
    #[test]
    fn main() {
        test_server_client(server, client);
    }
}

mod request_remote_mr {
    use super::*;

    async fn server(rdma: Rdma) -> io::Result<()> {
        let lmr = rdma.receive_local_mr().await.unwrap();
        dbg!(unsafe { *(*lmr.as_ptr() as *const char) });
        Ok(())
    }

    async fn client(rdma: Rdma) -> io::Result<()> {
        let mut rmr = rdma
            .request_remote_mr_with_timeout(Layout::new::<char>(), Duration::from_millis(100))
            .await
            .unwrap();
        let mut lmr = rdma.alloc_local_mr(Layout::new::<char>()).unwrap();
        unsafe { *(*lmr.as_mut_ptr() as *mut char) = 't' };
        sleep(Duration::from_millis(200)).await;
        // timeout, should panic
        rdma.write(&lmr, &mut rmr).await.unwrap();
        rdma.send_remote_mr(rmr).await.unwrap();
        Ok(())
    }

    #[should_panic]
    #[test]
    fn main() {
        test_server_client(server, client);
    }
}

mod timeout_check {
    use super::*;
    use async_rdma::RemoteMrReadAccess;

    async fn server(rdma: Rdma) -> io::Result<()> {
        let lmr = rdma.receive_local_mr().await.unwrap();
        dbg!(unsafe { *(*lmr.as_ptr() as *const char) });
        Ok(())
    }

    async fn client(rdma: Rdma) -> io::Result<()> {
        let rmr_timeout = rdma
            .request_remote_mr_with_timeout(Layout::new::<char>(), Duration::from_millis(100))
            .await
            .unwrap();
        assert!(!rmr_timeout.timeout_check());
        sleep(Duration::from_millis(200)).await;
        assert!(rmr_timeout.timeout_check());
        assert!(rdma.send_remote_mr(rmr_timeout).await.is_err());

        let mut rmr = rdma
            .request_remote_mr_with_timeout(Layout::new::<char>(), Duration::from_millis(500))
            .await
            .unwrap();
        let mut lmr = rdma.alloc_local_mr(Layout::new::<char>()).unwrap();
        unsafe { *(*lmr.as_mut_ptr() as *mut char) = 't' };
        rdma.write(&lmr, &mut rmr).await.unwrap();
        assert!(!rmr.timeout_check());
        rdma.send_remote_mr(rmr).await.unwrap();
        Ok(())
    }

    #[test]
    fn main() {
        test_server_client(server, client);
    }
}
