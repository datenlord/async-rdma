use async_rdma::*;
use std::alloc::Layout;
use std::time::Duration;
mod test_utilities;
use std::io;
use test_utilities::test_server_client;

const LEN: usize = 4096;

mod local_mr_slice {
    use super::*;
    async fn server(rdma: Rdma) -> io::Result<()> {
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
        assert_eq!(*s5.as_slice(), b"hello");
        Ok(())
    }

    async fn client(_rdma: Rdma) -> io::Result<()> {
        Ok(())
    }

    #[test]
    fn main() {
        test_server_client(server, client);
    }
}

mod local_mr_slice_overbound {
    use super::*;
    mod test1 {
        use super::*;
        async fn server(rdma: Rdma) -> io::Result<()> {
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            assert_eq!(lmr.length(), LEN);
            #[allow(clippy::reversed_empty_ranges)]
            let _s1 = lmr.get(2..0).unwrap();
            Ok(())
        }

        async fn client(_rdma: Rdma) -> io::Result<()> {
            Ok(())
        }

        #[test]
        #[should_panic]
        fn main() {
            test_server_client(server, client);
        }
    }

    mod test2 {
        use super::*;
        async fn server(rdma: Rdma) -> io::Result<()> {
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            assert_eq!(lmr.length(), LEN);
            let _s1 = lmr.get(0..0).unwrap();
            Ok(())
        }

        async fn client(_rdma: Rdma) -> io::Result<()> {
            Ok(())
        }
        #[test]
        #[should_panic]
        fn main() {
            test_server_client(server, client);
        }
    }

    mod test3 {
        use super::*;
        async fn server(rdma: Rdma) -> io::Result<()> {
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            assert_eq!(lmr.length(), LEN);
            let _s1 = lmr.get(0..LEN + 1).unwrap();
            Ok(())
        }

        async fn client(_rdma: Rdma) -> io::Result<()> {
            Ok(())
        }

        #[test]
        #[should_panic]
        fn main() {
            test_server_client(server, client);
        }
    }
}

mod remote_mr_slice {
    use super::*;
    async fn server(rdma: Rdma) -> io::Result<()> {
        let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
        rdma.send_local_mr(lmr).await
    }

    async fn client(rdma: Rdma) -> io::Result<()> {
        let rmr = rdma.receive_remote_mr().await.unwrap();
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
        // wait for the agent thread to send all reponses to the remote.
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    }
    #[test]
    fn main() {
        test_server_client(server, client);
    }
}

mod remote_mr_slice_overbound {
    use super::*;
    mod test1 {

        use super::*;
        async fn server(rdma: Rdma) -> io::Result<()> {
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            rdma.send_local_mr(lmr).await
        }

        async fn client(rdma: Rdma) -> io::Result<()> {
            let rmr = rdma.receive_remote_mr().await.unwrap();
            // wait for the agent thread to send all reponses to the remote.
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(rmr.length(), LEN);
            #[allow(clippy::reversed_empty_ranges)]
            let _s1 = rmr.get(2..0).unwrap();
            Ok(())
        }

        #[test]
        #[should_panic]
        fn main() {
            test_server_client(server, client);
        }
    }

    mod test2 {
        use super::*;
        async fn server(rdma: Rdma) -> io::Result<()> {
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            rdma.send_local_mr(lmr).await
        }

        async fn client(rdma: Rdma) -> io::Result<()> {
            let rmr = rdma.receive_remote_mr().await.unwrap();
            // wait for the agent thread to send all reponses to the remote.
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(rmr.length(), LEN);
            let _s1 = rmr.get(0..0).unwrap();
            Ok(())
        }

        #[test]
        #[should_panic]
        fn main() {
            test_server_client(server, client);
        }
    }

    mod test3 {
        use super::*;
        async fn server(rdma: Rdma) -> io::Result<()> {
            let lmr = rdma.alloc_local_mr(Layout::new::<[u8; LEN]>()).unwrap();
            rdma.send_local_mr(lmr).await
        }

        async fn client(rdma: Rdma) -> io::Result<()> {
            let rmr = rdma.receive_remote_mr().await.unwrap();
            // wait for the agent thread to send all reponses to the remote.
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(rmr.length(), LEN);
            let _s1 = rmr.get(0..LEN + 1).unwrap();
            Ok(())
        }

        #[should_panic]
        #[test]
        fn main() {
            test_server_client(server, client);
        }
    }
}
