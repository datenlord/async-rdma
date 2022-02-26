use async_rdma::*;
use std::alloc::Layout;
// use std::time::Duration;
// use tokio::time::sleep;

mod local_mr_slice {
    use crate::*;
    async fn server(addr: &str) {
        let rdmalistener = RdmaListener::bind(addr).await.unwrap();
        let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
        const LEN: usize = 4096;
        let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
        assert_eq!(lmr.length(), LEN);
        let s1 = lmr.get(0..LEN).unwrap();
        let s2_offset = 2048;
        let s2 = lmr.get(s2_offset..LEN).unwrap();
        let s3_start = 1000;
        let s3_end = 4000;
        let s3 = lmr.get(s3_start..s3_end).unwrap();
        let s4_pos = 1234;
        let s4 = lmr.get(s4_pos..s4_pos + 1).unwrap();
        let hello = "hello";
        assert_eq!(s1.length(), LEN);
        assert_eq!(s1.addr(), lmr.addr());
        assert_eq!(s2.length(), LEN - s2_offset);
        assert_eq!(s2.addr(), lmr.addr() + s2_offset);
        assert_eq!(s3.length(), s3_end - s3_start);
        assert_eq!(s3.addr(), lmr.addr() + s3_start);
        assert_eq!(s4.length(), 1);
        assert_eq!(s4.addr(), lmr.addr() + s4_pos);
        let mut s5 = lmr.get_mut(0..hello.len()).unwrap();
        s5.as_mut_slice().copy_from_slice(hello.as_bytes());
        assert_eq!(s5.as_slice(), b"hello");
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
        use std::sync::Arc;

        use crate::*;
        async fn server(addr: &str) {
            let rdmalistener = RdmaListener::bind(addr).await.unwrap();
            let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
            const LEN: usize = 4096;
            let lmr = Arc::new(rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap());
            assert_eq!(lmr.length(), LEN);
            #[allow(clippy::reversed_empty_ranges)]
            let _s1 = lmr.get(2..0).unwrap();
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
        use std::sync::Arc;

        use crate::*;
        async fn server(addr: &str) {
            let rdmalistener = RdmaListener::bind(addr).await.unwrap();
            let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
            const LEN: usize = 4096;
            let lmr = Arc::new(rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap());
            assert_eq!(lmr.length(), LEN);
            let _s1 = lmr.get(0..0).unwrap();
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
        use std::sync::Arc;

        use crate::*;
        async fn server(addr: &str) {
            let rdmalistener = RdmaListener::bind(addr).await.unwrap();
            let rdma = rdmalistener.accept(1, 1, 128).await.unwrap();
            const LEN: usize = 4096;
            let lmr = Arc::new(rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap());
            assert_eq!(lmr.length(), LEN);
            let _s1 = lmr.get(0..LEN + 1).unwrap();
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
    use std::{sync::Arc, time::Duration};

    use tokio::time::sleep;

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
        let rmr = Arc::new(rdma.receive_remote_mr().await.unwrap());
        assert_eq!(rmr.length(), LEN);
        let s1 = rmr.get(0..LEN).unwrap();
        let s2_offset = 2048;
        let s2 = rmr.get(s2_offset..LEN).unwrap();
        let s3_start = 1000;
        let s3_end = 4000;
        let s3 = rmr.get(s3_start..s3_end).unwrap();
        let s4_pos = 1234;
        let s4 = rmr.get(s4_pos..s4_pos + 1).unwrap();
        assert_eq!(s1.length(), LEN);
        assert_eq!(s1.addr(), rmr.addr());
        assert_eq!(s2.length(), LEN - s2_offset);
        assert_eq!(s2.addr(), rmr.addr() + s2_offset);
        assert_eq!(s3.length(), s3_end - s3_start);
        assert_eq!(s3.addr(), rmr.addr() + s3_start);
        assert_eq!(s4.length(), 1);
        assert_eq!(s4.addr(), rmr.addr() + s4_pos);
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
        use std::{sync::Arc, time::Duration};

        use tokio::time::sleep;

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
            let rmr = Arc::new(rdma.receive_remote_mr().await.unwrap());
            assert_eq!(rmr.length(), LEN);
            #[allow(clippy::reversed_empty_ranges)]
            let _s1 = rmr.get(2..0).unwrap();
        }

        #[tokio::test]
        #[should_panic]
        async fn test() {
            let addr = "127.0.0.1:19300";
            tokio::join!(server(addr), client(addr));
        }
    }

    mod test2 {
        use std::{sync::Arc, time::Duration};

        use tokio::time::sleep;

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
            let rmr = Arc::new(rdma.receive_remote_mr().await.unwrap());
            assert_eq!(rmr.length(), LEN);
            let _s1 = rmr.get(0..0).unwrap();
        }

        #[tokio::test]
        #[should_panic]
        async fn test() {
            let addr = "127.0.0.1:19301";
            tokio::join!(server(addr), client(addr));
        }
    }

    mod test3 {
        use std::{sync::Arc, time::Duration};
        use tokio::time::sleep;

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
            let rmr = Arc::new(rdma.receive_remote_mr().await.unwrap());
            assert_eq!(rmr.length(), LEN);
            let _s1 = rmr.get(0..LEN + 1).unwrap();
        }

        #[tokio::test]
        #[should_panic]
        async fn test() {
            let addr = "127.0.0.1:19302";
            tokio::join!(server(addr), client(addr));
        }
    }
}