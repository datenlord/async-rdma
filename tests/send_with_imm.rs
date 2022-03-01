mod send_with_imm {
    use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess};
    use async_rdma::{Rdma, RdmaListener};
    use portpicker::pick_unused_port;
    use std::{
        alloc::Layout,
        io,
        net::{Ipv4Addr, SocketAddrV4},
        time::Duration,
    };

    struct Data(String);

    async fn client(addr: SocketAddrV4) -> io::Result<()> {
        let rdma = Rdma::connect(addr, 1, 1, 512).await?;
        let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
        // put data into lmr
        unsafe { *(lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
        // send the content of lmr to server
        rdma.send_with_imm(&lmr, 123).await?;
        rdma.send_with_imm(&lmr, 123).await?;
        rdma.send(&lmr).await?;
        rdma.send(&lmr).await?;
        Ok(())
    }

    #[tokio::main]
    async fn server(addr: SocketAddrV4) -> io::Result<()> {
        let rdma_listener = RdmaListener::bind(addr).await?;
        let rdma = rdma_listener.accept(1, 1, 512).await?;
        // receive the data sent by client and put it into an mr
        let (lmr, imm) = rdma.receive_with_imm().await?;
        assert_eq!(imm, Some(123));
        unsafe {
            assert_eq!(
                "hello world".to_string(),
                *(*(lmr.as_ptr() as *const Data)).0
            )
        };
        let lmr = rdma.receive().await?;
        unsafe {
            assert_eq!(
                "hello world".to_string(),
                *(*(lmr.as_ptr() as *const Data)).0
            )
        };
        let (lmr, imm) = rdma.receive_with_imm().await?;
        assert_eq!(imm, None);
        // read data from mr
        unsafe {
            assert_eq!(
                "hello world".to_string(),
                *(*(lmr.as_ptr() as *const Data)).0
            )
        };
        let lmr = rdma.receive().await?;
        unsafe {
            assert_eq!(
                "hello world".to_string(),
                *(*(lmr.as_ptr() as *const Data)).0
            )
        };
        Ok(())
    }
    #[tokio::main]
    #[test]
    async fn main() {
        let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
        let server_handle = std::thread::spawn(move || server(addr));
        tokio::time::sleep(Duration::new(1, 0)).await;
        client(addr)
            .await
            .map_err(|err| println!("{}", err))
            .unwrap();
        server_handle.join().unwrap().unwrap();
    }
}
