mod test_utilities;
use async_rdma::{LocalMrReadAccess, Rdma};
use std::{alloc::Layout, io, time::Duration};
use test_utilities::test_server_client;

mod cas {
    use super::*;
    mod success {
        use super::*;

        async fn client(rdma: Rdma) -> io::Result<()> {
            // alloc 8 bytes remote memory
            let mut rmr = rdma.request_remote_mr(Layout::new::<[u8; 8]>()).await?;
            let new_value = u64::from_le_bytes([1_u8; 8]);
            // read, compare with rmr and swap `old_value` with `new_value`
            rdma.atomic_cas(0, new_value, &mut rmr).await?;
            // send rmr's meta data to the remote end
            rdma.send_remote_mr(rmr).await?;
            Ok(())
        }

        async fn server(rdma: Rdma) -> io::Result<()> {
            // receive mr's meta data from client
            let lmr = rdma.receive_local_mr().await?;
            // assert the content of lmr, which was write by cas
            let data = *lmr.as_slice();
            assert_eq!(data, [1_u8; 8]);
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        }

        #[test]
        fn main() {
            test_server_client(server, client);
        }
    }

    mod wrong_align {
        use super::*;

        async fn client(rdma: Rdma) -> io::Result<()> {
            // alloc 8 bytes remote memory
            let mut rmr = rdma.request_remote_mr(Layout::new::<[u8; 16]>()).await?;
            let new_value = u64::from_le_bytes([1_u8; 8]);
            // read, compare with rmr and swap `old_value` with `new_value`
            rdma.atomic_cas(0, new_value, &mut rmr.get_mut(1..9).unwrap())
                .await?;
            // send rmr's meta data to the remote end
            rdma.send_remote_mr(rmr).await?;
            Ok(())
        }

        async fn server(rdma: Rdma) -> io::Result<()> {
            // receive mr's meta data from client
            let lmr = rdma.receive_local_mr().await?;
            // assert the content of lmr, which was write by cas
            let data = *lmr.as_slice();
            assert_eq!(data, [1_u8; 8]);
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        }

        #[test]
        #[should_panic = "Atomic operations are legal only when the remote address is on a naturally-aligned 8-byte boundary"]
        fn main() {
            test_server_client(server, client);
        }
    }

    mod wrong_len {
        use super::*;

        async fn client(rdma: Rdma) -> io::Result<()> {
            // alloc 8 bytes remote memory
            let mut rmr = rdma.request_remote_mr(Layout::new::<[u8; 4]>()).await?;
            let new_value = u64::from_le_bytes([1_u8; 8]);
            // read, compare with rmr and swap `old_value` with `new_value`
            rdma.atomic_cas(0, new_value, &mut rmr).await?;
            // send rmr's meta data to the remote end
            rdma.send_remote_mr(rmr).await?;
            Ok(())
        }

        async fn server(rdma: Rdma) -> io::Result<()> {
            // receive mr's meta data from client
            let lmr = rdma.receive_local_mr().await?;
            // assert the content of lmr, which was write by cas
            let data = *lmr.as_slice();
            assert_eq!(data, [1_u8; 8]);
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        }

        #[test]
        #[should_panic = "The length of remote mr should be 8"]
        fn main() {
            test_server_client(server, client);
        }
    }
}
