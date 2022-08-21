//! This demo shows how to establish a connection between server and client
//! and the usage of rdma `read`, `write` and `send&recv` APIs.
//!
//! You can try this example by running:
//!
//!     cargo run --example server
//!
//! And then start client in another terminal by running:
//!
//!     cargo run --example client

use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaBuilder};
use std::{
    alloc::Layout,
    io::{self, Write},
};

/// send data to serer
async fn send_data_to_server(rdma: &Rdma) -> io::Result<()> {
    // alloc 8 bytes local memory
    let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
    // write data into lmr
    let _num = lmr.as_mut_slice().write(&[1_u8; 8])?;
    // send data in mr to the remote end
    rdma.send(&lmr).await?;
    Ok(())
}

/// send data and immdiate number to server
async fn send_data_with_imm_to_server(rdma: &Rdma) -> io::Result<()> {
    // alloc 8 bytes local memory
    let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
    // write data into lmr
    let _num = lmr.as_mut_slice().write(&[1_u8; 8])?;
    // send data and imm to the remote end
    rdma.send_with_imm(&lmr, 1_u32).await?;
    Ok(())
}

/// write data into local mr and send mr's meta data to server and wait to be read
async fn send_lmr_to_server(rdma: &Rdma) -> io::Result<()> {
    // alloc 8 bytes local memory
    let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
    println!("{:?}", *lmr.as_slice());
    assert_eq!(*lmr.as_slice(), [0_u8; 8]);

    // write data into lmr
    let _num = lmr.as_mut_slice().write(&[1_u8; 8])?;
    println!("{:?}", *lmr.as_slice());
    assert_eq!(*lmr.as_slice(), [1_u8; 8]);

    // write data to a part of it
    let _num = lmr
        .get_mut(4..8)
        .unwrap()
        .as_mut_slice()
        .write(&[2_u8; 4])?;
    println!("{:?}", *lmr.as_slice());
    assert_eq!(*lmr.as_slice(), [[1_u8; 4], [2_u8; 4]].concat());

    // send lmr's meta data to the remote
    rdma.send_local_mr(lmr).await?;
    Ok(())
}

/// request remote memory region and write data to it by RDMA WRITE
async fn request_then_write(rdma: &Rdma) -> io::Result<()> {
    // alloc 8 bytes remote memory
    let mut rmr = rdma.request_remote_mr(Layout::new::<[u8; 8]>()).await?;
    // alloc 8 bytes local memory
    let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
    // write data into lmr
    let _num = lmr.as_mut_slice().write(&[1_u8; 8])?;
    // write the second half of the data in lmr to the rmr
    rdma.write(&lmr.get(4..8).unwrap(), &mut rmr.get_mut(4..8).unwrap())
        .await?;
    // send rmr's meta data to the remote end
    rdma.send_remote_mr(rmr).await?;
    Ok(())
}

/// request remote memory region and write data to it with immdiate number by RDMA WRITE
async fn request_then_write_with_imm(rdma: &Rdma) -> io::Result<()> {
    // alloc 8 bytes remote memory
    let mut rmr = rdma.request_remote_mr(Layout::new::<[u8; 8]>()).await?;
    // alloc 8 bytes local memory
    let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
    // write data into lmr
    let _num = lmr.as_mut_slice().write(&[1_u8; 8])?;
    // write the second half of the data in lmr to the rmr
    rdma.write_with_imm(
        &lmr.get(4..8).unwrap(),
        &mut rmr.get_mut(4..8).unwrap(),
        4_u32,
    )
    .await?;
    // send rmr's meta data to the remote end
    rdma.send_remote_mr(rmr).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    println!("client start");
    let addr = "localhost:5555";
    let rdma = RdmaBuilder::default().connect(addr).await.unwrap();
    println!("connected");
    send_data_to_server(&rdma).await.unwrap();
    send_data_with_imm_to_server(&rdma).await.unwrap();
    send_lmr_to_server(&rdma).await.unwrap();
    request_then_write(&rdma).await.unwrap();
    request_then_write_with_imm(&rdma).await.unwrap();
    println!("client done");

    // create new `Rdma`s (connections) that has the same `mr_allocator` and `event_listener` as parent
    for _ in 0..3 {
        let rdma = rdma.new_connect(addr).await.unwrap();
        println!("connected");
        send_data_to_server(&rdma).await.unwrap();
        send_data_with_imm_to_server(&rdma).await.unwrap();
        send_lmr_to_server(&rdma).await.unwrap();
        request_then_write(&rdma).await.unwrap();
        request_then_write_with_imm(&rdma).await.unwrap();
    }
    println!("client done");
}
