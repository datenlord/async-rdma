use async_rdma::{LocalMrReadAccess, Rdma, RdmaListener};
use futures::Future;
use tokio::{io, net::ToSocketAddrs};

type RdmaFn<R> = fn(Rdma) -> R;

#[tokio::main]
async fn server<A: ToSocketAddrs, R: Future<Output = Result<(), io::Error>>>(
    addr: A,
    f: RdmaFn<R>,
) -> io::Result<()> {
    let rdma = RdmaListener::bind(addr).await?.accept(1, 1, 64).await?;
    f(rdma).await
}

#[tokio::main]
async fn client<A: ToSocketAddrs, R: Future<Output = Result<(), io::Error>>>(
    addr: A,
    f: RdmaFn<R>,
) -> io::Result<()> {
    let rdma = Rdma::connect(addr, 1, 1, 64).await?;
    f(rdma).await
}

fn test_server_client<
    A: 'static + ToSocketAddrs + Send + Copy,
    SR: Future<Output = Result<(), io::Error>> + 'static,
    CR: Future<Output = Result<(), io::Error>> + 'static,
>(
    addr: A,
    s: RdmaFn<SR>,
    c: RdmaFn<CR>,
) -> io::Result<()> {
    let server = std::thread::spawn(move || server(addr, s));
    std::thread::sleep(std::time::Duration::from_secs(1));
    let client = std::thread::spawn(move || client(addr, c));
    client.join().unwrap()?;
    server.join().unwrap()
}

mod test1 {
    use crate::*;
    use std::alloc::Layout;

    async fn server(rdma: Rdma) -> io::Result<()> {
        let mr = rdma.receive_local_mr().await.unwrap();
        assert_eq!(unsafe { *(mr.as_ptr() as *mut i32) }, 5);
        Ok(())
    }

    async fn client(rdma: Rdma) -> io::Result<()> {
        let mut rmr = rdma.request_remote_mr(Layout::new::<i32>()).await.unwrap();
        let lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
        unsafe { *(lmr.as_ptr() as *mut i32) = 5 };
        rdma.write(&lmr, &mut rmr).await.unwrap();
        rdma.send_remote_mr(rmr).await.unwrap();
        Ok(())
    }

    #[test]
    fn test() -> io::Result<()> {
        test_server_client("127.0.0.1:18000", server, client)
    }
}

mod test2 {
    use crate::*;
    use async_rdma::MrAccess;
    use std::{alloc::Layout, sync::Arc, time::Duration};

    async fn server(rdma: Rdma) -> io::Result<()> {
        let rdma = Arc::new(rdma);
        let mut handles = vec![];
        for _ in 0..10 {
            let rdma_clone = rdma.clone();
            handles.push(tokio::spawn(async move {
                let lm = rdma_clone.receive().await.unwrap();
                assert_eq!(unsafe { *(lm.as_ptr() as *mut i32) }, 5);
                assert_eq!(lm.length(), 4);
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        // wait for the agent thread to send all reponses to the remote.
        tokio::time::sleep(Duration::new(3, 0)).await;
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
                unsafe { *(lm.as_ptr() as *mut i32) = 5 };
                rdma_clone.send(&lm).await.unwrap();
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        Ok(())
    }

    #[test]
    fn test() -> io::Result<()> {
        test_server_client("127.0.0.1:18001", server, client)
    }
}
