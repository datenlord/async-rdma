//! This demo shows how to establish a connection between server and client
//! and the usage of rdma `read`, `write` and `send&recv` APIs.
//!
//! You can try this example by running:
//!
//!     cargo run --example server <server_ip> <port>
//!
//! And start client in another terminal by running:
//!
//!     cargo run --example client <server_ip> <port>

use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, MrAccess, RCStream, Rdma, RdmaBuilder};
use clippy_utilities::Cast;
use std::io::Write;
use std::{alloc::Layout, env, io, process::exit};

/// receive data from client
async fn receive_data_from_client(rdma: &Rdma) -> io::Result<()> {
    // receive data
    let lmr = rdma.receive().await?;
    let data = *lmr.as_slice();
    println!("{:?}", data);
    assert_eq!(data, [1_u8; 8]);
    Ok(())
}

/// receive data and immdiate number from client
async fn receive_data_with_imm_from_client(rdma: &Rdma) -> io::Result<()> {
    // receive data with imm
    let (lmr, imm) = rdma.receive_with_imm().await?;
    let data = *lmr.as_slice();
    println!("{:?}", data);
    assert_eq!(
        data,
        [imm.expect(
            "The value of immediate data flag may be different on RDMA devices.\
            And you can change this flag by this API `set_imm_flag_in_wc` to adapt your devices."
        )
        .cast(); 8]
    );
    Ok(())
}

/// read data from rmr by RDMA READ
async fn read_rmr_from_client(rdma: &Rdma) -> io::Result<()> {
    // alloc 8 bytes local memory
    let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
    // receive the mr's meta data from client
    let rmr = rdma.receive_remote_mr().await?;
    // get data in rmr through RDMA READ
    rdma.read(&mut lmr, &rmr).await?;
    let data = *lmr.as_slice();
    println!("{:?}", data);
    assert_eq!(data, [[1_u8; 4], [2_u8; 4]].concat());
    Ok(())
}

/// receive lmr which was written by client through RDMA WRITE
async fn receive_mr_after_being_written(rdma: &Rdma) -> io::Result<()> {
    // receive mr's meta data from client
    let lmr = rdma.receive_local_mr().await?;
    let data = *lmr.as_slice();
    println!("{:?}", data);
    assert_eq!(data, [[0_u8; 4], [1_u8; 4]].concat());
    Ok(())
}

/// receive lmr which was written by client through RDMA WRITE with an immediate number
async fn receive_mr_after_being_written_with_imm(rdma: &Rdma) -> io::Result<()> {
    // receive the immediate data sent by `write_with_imm`
    let imm = rdma.receive_write_imm().await?;
    // receive mr's meta data from client
    let lmr = rdma.receive_local_mr().await?;
    // assert the content of lmr, which was `write` by client
    let data = *lmr.as_slice();
    println!("{:?}", data);
    assert_eq!(data, [[0_u8; 4], [1_u8; 4]].concat());
    // RC supports a message size of zero to 2^31 bytes so we can use u32 as usize
    assert_ne!(data[imm.wrapping_sub(1) as usize], data[imm as usize]);
    Ok(())
}

/// receive lmr which was written by client through RDMA ATOMIC_CAS
async fn receive_mr_after_being_written_by_cas(rdma: &Rdma) -> io::Result<()> {
    // receive mr's meta data from client
    let lmr = rdma.receive_local_mr().await?;
    // assert the content of lmr, which was write by cas
    let data = *lmr.as_slice();
    println!("{:?}", data);
    assert_eq!(data, [1_u8; 8]);
    Ok(())
}

async fn rcstream_send(stream: &mut RCStream) -> io::Result<()> {
    for i in 0..10 {
        // alloc 8 bytes local memory
        let mut lmr = stream.alloc_local_mr(Layout::new::<[u8; 8]>())?;
        // write data into lmr
        let _num = lmr.as_mut_slice().write(&[i as u8; 8])?;
        // send data in mr to the remote end
        stream.send_lmr(lmr).await?;
        println!("stream send datagram {} ", i);
    }
    Ok(())
}

async fn rcstream_recv(stream: &mut RCStream) -> io::Result<()> {
    for i in 0..10 {
        // recieve data from the remote end
        let mut lmr_vec = stream.recieve_lmr(8).await?;
        println!("stream recieve datagram {}", i);
        // check the length of the recieved data
        assert!(lmr_vec.len() == 1);
        let lmr = lmr_vec.pop().unwrap();
        assert!(lmr.length() == 8);
        let buff = *(lmr.as_slice());
        // check the data
        assert_eq!(buff, [i as u8; 8]);
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    println!("server start");

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("usage : cargo run --example server <server_ip> <port>");
        println!("input : {:?}", args);
        exit(-1);
    }
    let ip = args.get(1).unwrap().as_str();
    let port = args.get(2).unwrap().as_str();
    let addr = format!("{}:{}", ip, port);

    let mut rdma = RdmaBuilder::default().listen(addr).await.unwrap();
    println!("accepted");
    receive_data_from_client(&rdma).await.unwrap();
    receive_data_with_imm_from_client(&rdma).await.unwrap();
    read_rmr_from_client(&rdma).await.unwrap();
    receive_mr_after_being_written(&rdma).await.unwrap();
    receive_mr_after_being_written_with_imm(&rdma)
        .await
        .unwrap();
    receive_mr_after_being_written_by_cas(&rdma).await.unwrap();
    println!("server done");

    // create new `Rdma`s (connections) that has the same `mr_allocator` and `event_listener` as parent
    for _ in 0..3 {
        let rdma = rdma.listen().await.unwrap();
        println!("accepted");
        receive_data_from_client(&rdma).await.unwrap();
        receive_data_with_imm_from_client(&rdma).await.unwrap();
        read_rmr_from_client(&rdma).await.unwrap();
        receive_mr_after_being_written(&rdma).await.unwrap();
        receive_mr_after_being_written_with_imm(&rdma)
            .await
            .unwrap();
        receive_mr_after_being_written_by_cas(&rdma).await.unwrap();
    }
    let mut stream: RCStream = rdma.into();
    rcstream_recv(&mut stream).await.unwrap();
    rcstream_send(&mut stream).await.unwrap();
    println!("server done");
}
