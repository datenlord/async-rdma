mod test_utilities;
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma};
use std::{alloc::Layout, io, time::Duration};
use test_utilities::test_server_client;

mod send_with_imm {
    use super::*;
    struct Data(String);
    static IMM_NUM: u32 = 123;
    static MSG: &str = "hello world";

    async fn client(rdma: Rdma) -> io::Result<()> {
        let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
        // put data into lmr
        unsafe { std::ptr::write(*lmr.as_mut_ptr() as *mut Data, Data(MSG.to_string())) };
        // send the content of lmr to server
        rdma.send_with_imm(&lmr, IMM_NUM).await?;
        rdma.send_with_imm(&lmr, IMM_NUM).await?;
        rdma.send(&lmr).await?;
        rdma.send(&lmr).await?;
        Ok(())
    }

    async fn server(rdma: Rdma) -> io::Result<()> {
        // receive the data and imm sent by the client
        let (lmr, imm) = rdma.receive_with_imm().await?;
        assert_eq!(imm, Some(IMM_NUM));
        unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
        // receive the data in mr while avoiding the immediate data is ok.
        let lmr = rdma.receive().await?;
        unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
        // `receive_with_imm` works well even if the client didn't send any immediate data.
        // the imm received will be a `None`.
        let (lmr, imm) = rdma.receive_with_imm().await?;
        assert_eq!(imm, None);
        unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
        // compared to the above, using `receive` is a better choice.
        let lmr = rdma.receive().await?;
        unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
        // wait for the agent thread to send all reponses to the remote.
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    }

    #[test]
    fn main() {
        test_server_client(server, client);
    }
}

mod write_with_imm {
    use super::*;
    struct Data(String);
    static IMM_NUM: u32 = 123;
    static MSG: &str = "hello world";

    async fn client(rdma: Rdma) -> io::Result<()> {
        let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
        let mut rmr = rdma.request_remote_mr(Layout::new::<Data>()).await?;
        unsafe { std::ptr::write(*lmr.as_mut_ptr() as *mut Data, Data(MSG.to_string())) };
        // send the content of lmr to server with immediate data.
        rdma.write_with_imm(&lmr, &mut rmr, IMM_NUM).await?;
        // then send this mr to server to make server aware of this mr.
        rdma.send_remote_mr(rmr).await?;
        Ok(())
    }

    async fn server(rdma: Rdma) -> io::Result<()> {
        // receive the immediate data sent by `write_with_imm`
        let imm = rdma.receive_write_imm().await?;
        assert_eq!(imm, IMM_NUM);
        let lmr = rdma.receive_local_mr().await?;
        // assert the content of lmr, which was `write` by client
        unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
        // wait for the agent thread to send all reponses to the remote.
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    }

    #[test]
    fn main() {
        test_server_client(server, client);
    }
}
