use async_rdma::*;
use std::alloc::Layout;
use std::time::Duration;
use tokio::time::sleep;

mod local_mr_slice {
    use crate::*;
    async fn server(addr: &str) {
        let rdmalistener = RdmaListener::bind(addr).await.unwrap();
        let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
        const LEN: usize = 4096;
        let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
        assert_eq!(lmr.length(), LEN);
        let s1 = &lmr[..];
        let s2_offset = 2048;
        let s2 = &lmr[s2_offset..];
        let s3_start = 1000;
        let s3_end = 4000;
        let s3 = &lmr[s3_start..s3_end];
        let s4_pos = 1234;
        let s4 = &lmr[s4_pos..s4_pos + 1];
        let s5 = &lmr[..LEN];
        assert_eq!(s1.length(), LEN);
        assert_eq!(s1.addr(), lmr.addr());
        assert_eq!(s2.length(), LEN - s2_offset);
        assert_eq!(s2.addr(), lmr.addr() + s2_offset);
        assert_eq!(s3.length(), s3_end - s3_start);
        assert_eq!(s3.addr(), lmr.addr() + s3_start);
        assert_eq!(s4.length(), 1);
        assert_eq!(s4.addr(), lmr.addr() + s4_pos);
        assert_eq!(s5.length(), LEN);
        assert_eq!(s5.addr(), lmr.addr());
    }

    async fn client(addr: &str) {
        let _rdma = Rdma::connect(addr, 1, 1, 512).await.unwrap();
    }

    #[tokio::test]
    async fn test() {
        let addr = "127.0.0.1:19000";
        tokio::join!(server(addr), client(addr));
    }
}

mod local_mr_slice_overbound {
    mod test1 {
        use crate::*;
        async fn server(addr: &str) {
            let rdmalistener = RdmaListener::bind(addr).await.unwrap();
            let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
            const LEN: usize = 4096;
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            assert_eq!(lmr.length(), LEN);
            #[allow(clippy::reversed_empty_ranges)]
            let _s1 = &lmr[2..0];
        }

        async fn client(addr: &str) {
            let _rdma = Rdma::connect(addr, 1, 1, 512).await.unwrap();
        }

        #[tokio::test]
        #[should_panic]
        async fn test() {
            let addr = "127.0.0.1:19100";
            tokio::join!(server(addr), client(addr));
        }
    }

    mod test2 {
        use crate::*;
        async fn server(addr: &str) {
            let rdmalistener = RdmaListener::bind(addr).await.unwrap();
            let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
            const LEN: usize = 4096;
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            assert_eq!(lmr.length(), LEN);
            let _s1 = &lmr[0..0];
        }

        async fn client(addr: &str) {
            let _rdma = Rdma::connect(addr, 1, 1, 512).await.unwrap();
        }

        #[tokio::test]
        #[should_panic]
        async fn test() {
            let addr = "127.0.0.1:19101";
            tokio::join!(server(addr), client(addr));
        }
    }

    mod test3 {
        use crate::*;
        async fn server(addr: &str) {
            let rdmalistener = RdmaListener::bind(addr).await.unwrap();
            let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
            const LEN: usize = 4096;
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            assert_eq!(lmr.length(), LEN);
            let _s1 = &lmr[..LEN + 1];
        }

        async fn client(addr: &str) {
            let _rdma = Rdma::connect(addr, 1, 1, 512).await.unwrap();
        }

        #[tokio::test]
        #[should_panic]
        async fn test() {
            let addr = "127.0.0.1:19102";
            tokio::join!(server(addr), client(addr));
        }
    }
}

mod remote_mr_slice {
    use crate::*;
    const LEN: usize = 4096;
    async fn server(addr: &str) {
        let rdmalistener = RdmaListener::bind(addr).await.unwrap();
        let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
        let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
        rdma.send_local_mr(lmr).await.unwrap();
        sleep(Duration::from_secs(1)).await;
    }

    async fn client(addr: &str) {
        let rdma = Rdma::connect(addr, 1, 1, 512).await.unwrap();
        let rmr = rdma.receive_remote_mr().await.unwrap();
        assert_eq!(rmr.length(), LEN);
        let s1 = &rmr[..];
        let s2_offset = 2048;
        let s2 = &rmr[s2_offset..];
        let s3_start = 1000;
        let s3_end = 4000;
        let s3 = &rmr[s3_start..s3_end];
        let s4_pos = 1234;
        let s4 = &rmr[s4_pos..s4_pos + 1];
        let s5 = &rmr[..LEN];
        assert_eq!(s1.length(), LEN);
        assert_eq!(s1.addr(), rmr.addr());
        assert_eq!(s2.length(), LEN - s2_offset);
        assert_eq!(s2.addr(), rmr.addr() + s2_offset);
        assert_eq!(s3.length(), s3_end - s3_start);
        assert_eq!(s3.addr(), rmr.addr() + s3_start);
        assert_eq!(s4.length(), 1);
        assert_eq!(s4.addr(), rmr.addr() + s4_pos);
        assert_eq!(s5.length(), LEN);
        assert_eq!(s5.addr(), rmr.addr());
        sleep(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn test() {
        let addr = "127.0.0.1:19200";
        tokio::join!(server(addr), client(addr));
    }
}

mod remote_mr_slice_overbound {
    mod test1 {
        use crate::*;
        const LEN: usize = 4096;
        async fn server(addr: &str) {
            let rdmalistener = RdmaListener::bind(addr).await.unwrap();
            let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            rdma.send_local_mr(lmr).await.unwrap();
            sleep(Duration::from_secs(1)).await;
        }

        async fn client(addr: &str) {
            let rdma = Rdma::connect(addr, 1, 1, 512).await.unwrap();
            let rmr = rdma.receive_remote_mr().await.unwrap();
            assert_eq!(rmr.length(), LEN);
            #[allow(clippy::reversed_empty_ranges)]
            let _s1 = &rmr[2..0];
        }

        #[tokio::test]
        #[should_panic]
        async fn test() {
            let addr = "127.0.0.1:19300";
            tokio::join!(server(addr), client(addr));
        }
    }

    mod test2 {
        use crate::*;
        const LEN: usize = 4096;
        async fn server(addr: &str) {
            let rdmalistener = RdmaListener::bind(addr).await.unwrap();
            let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            rdma.send_local_mr(lmr).await.unwrap();
            sleep(Duration::from_secs(1)).await;
        }

        async fn client(addr: &str) {
            let rdma = Rdma::connect(addr, 1, 1, 512).await.unwrap();
            let rmr = rdma.receive_remote_mr().await.unwrap();
            assert_eq!(rmr.length(), LEN);
            let _s1 = &rmr[0..0];
        }

        #[tokio::test]
        #[should_panic]
        async fn test() {
            let addr = "127.0.0.1:19301";
            tokio::join!(server(addr), client(addr));
        }
    }

    mod test3 {
        use crate::*;
        const LEN: usize = 4096;
        async fn server(addr: &str) {
            let rdmalistener = RdmaListener::bind(addr).await.unwrap();
            let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            rdma.send_local_mr(lmr).await.unwrap();
            sleep(Duration::from_secs(1)).await;
        }

        async fn client(addr: &str) {
            let rdma = Rdma::connect(addr, 1, 1, 512).await.unwrap();
            let rmr = rdma.receive_remote_mr().await.unwrap();
            assert_eq!(rmr.length(), LEN);
            let _s1 = &rmr[..LEN + 1];
        }

        #[tokio::test]
        #[should_panic]
        async fn test() {
            let addr = "127.0.0.1:19302";
            tokio::join!(server(addr), client(addr));
        }
    }
}
