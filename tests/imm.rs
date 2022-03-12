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
    static IMM_NUM: u32 = 123;
    static MSG: &str = "hello world";

    async fn client(addr: SocketAddrV4) -> io::Result<()> {
        let rdma = Rdma::connect(addr, 1, 1, 512).await?;
        let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
        // put data into lmr
        unsafe { std::ptr::write(lmr.as_mut_ptr() as *mut Data, Data(MSG.to_string())) };
        // send the content of lmr to server
        rdma.send_with_imm(&lmr, IMM_NUM).await?;
        rdma.send_with_imm(&lmr, IMM_NUM).await?;
        rdma.send(&lmr).await?;
        rdma.send(&lmr).await?;
        Ok(())
    }

    #[tokio::main]
    async fn server(addr: SocketAddrV4) -> io::Result<()> {
        let rdma_listener = RdmaListener::bind(addr).await?;
        let rdma = rdma_listener.accept(1, 1, 512).await?;
        // receive the data and imm sent by the client
        let (lmr, imm) = rdma.receive_with_imm().await?;
        assert_eq!(imm, Some(IMM_NUM));
        unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
        // receive the data in mr while avoiding the immediate data is ok.
        let lmr = rdma.receive().await?;
        unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
        // `receive_with_imm` works well even if the client didn't send any immediate data.
        // the imm received will be a `None`.
        let (lmr, imm) = rdma.receive_with_imm().await?;
        assert_eq!(imm, None);
        unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
        // compared to the above, using `receive` is a better choice.
        let lmr = rdma.receive().await?;
        unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
        // wait for the agent thread to send all reponses to the remote.
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    }
    #[tokio::main]
    #[test]
    async fn main() {
        let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
        let server_handle = std::thread::spawn(move || server(addr));
        tokio::time::sleep(Duration::from_secs(3)).await;
        client(addr)
            .await
            .map_err(|err| println!("{}", err))
            .unwrap();
        server_handle.join().unwrap().unwrap();
    }
}

mod write_with_imm {
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
    static IMM_NUM: u32 = 123;
    static MSG: &str = "hello world";

    async fn client(addr: SocketAddrV4) -> io::Result<()> {
        let rdma = Rdma::connect(addr, 1, 1, 512).await?;
        let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
        let mut rmr = rdma.request_remote_mr(Layout::new::<Data>()).await?;
        unsafe { std::ptr::write(lmr.as_mut_ptr() as *mut Data, Data(MSG.to_string())) };
        // send the content of lmr to server with immediate data.
        rdma.write_with_imm(&lmr, &mut rmr, IMM_NUM).await?;
        // then send this mr to server to make server aware of this mr.
        rdma.send_remote_mr(rmr).await?;
        Ok(())
    }

    #[tokio::main]
    async fn server(addr: SocketAddrV4) -> io::Result<()> {
        let rdma_listener = RdmaListener::bind(addr).await?;
        let rdma = rdma_listener.accept(1, 1, 512).await?;
        // receive the immediate data sent by `write_with_imm`
        let imm = rdma.receive_write_imm().await?;
        assert_eq!(imm, IMM_NUM);
        let lmr = rdma.receive_local_mr().await?;
        // assert the content of lmr, which was `write` by client
        unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
        // wait for the agent thread to send all reponses to the remote.
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    }

    #[tokio::main]
    #[test]
    async fn main() {
        let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
        let server_handle = std::thread::spawn(move || server(addr));
        tokio::time::sleep(Duration::from_secs(3)).await;
        client(addr)
            .await
            .map_err(|err| println!("{}", err))
            .unwrap();
        server_handle.join().unwrap().unwrap();
    }
}
