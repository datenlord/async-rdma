mod test_utilities;
use async_rdma::{LocalMrReadAccess, Rdma};
use std::{alloc::Layout, io, time::Duration};
use test_utilities::test_server_client;
use tokio::time::sleep;

mod send_local_mr {
    use async_rdma::AccessFlag;

    use super::*;

    async fn server(rdma: Rdma) -> io::Result<()> {
        let lmr = rdma.alloc_local_mr(Layout::new::<char>())?;
        let mut rmr = rdma.receive_remote_mr().await?;
        // wrong mr access, should panic
        rdma.write(&lmr, &mut rmr).await.unwrap();
        dbg!(unsafe { *(*lmr.as_ptr() as *const char) });
        Ok(())
    }

    async fn client(rdma: Rdma) -> io::Result<()> {
        let access = AccessFlag::LocalWrite | AccessFlag::RemoteRead;
        let lmr = rdma.alloc_local_mr_with_access(Layout::new::<char>(), access)?;
        rdma.send_local_mr(lmr).await?;
        // wait for panic
        sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    #[should_panic]
    #[test]
    fn main() {
        test_server_client(server, client);
    }
}

mod request_remote_mr {
    use std::net::{Ipv4Addr, SocketAddrV4};

    use async_rdma::{AccessFlag, RdmaBuilder};
    use portpicker::pick_unused_port;

    use super::*;
    async fn client(addr: SocketAddrV4) -> io::Result<()> {
        let rdma = RdmaBuilder::default().connect(addr).await?;
        let lmr = rdma.alloc_local_mr(Layout::new::<char>())?;
        let mut rmr = rdma.request_remote_mr(Layout::new::<char>()).await?;
        // wrong mr access, should panic
        rdma.write(&lmr, &mut rmr).await.unwrap();
        rdma.send_remote_mr(rmr).await?;
        Ok(())
    }
    ///
    #[tokio::main]
    async fn server(addr: SocketAddrV4) -> io::Result<()> {
        let flags = AccessFlag::LocalWrite | AccessFlag::RemoteRead;
        let rdma = RdmaBuilder::default()
            .set_max_rmr_access(flags)
            .listen(addr)
            .await?;
        rdma.receive_local_mr().await?;
        Ok(())
    }

    #[should_panic]
    #[tokio::main]
    #[test]
    async fn main() {
        let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
        std::thread::spawn(move || server(addr));
        tokio::time::sleep(Duration::from_secs(3)).await;
        client(addr)
            .await
            .map_err(|err| println!("{}", err))
            .unwrap();
    }
}
