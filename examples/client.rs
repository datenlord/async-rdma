//! This demo shows how to establish a connection between server and client
//! and the usage of rdma `read`, `write` and `send&recv` APIs.
//!
//! You can try this example by running:
//!
//!     cargo run --example server <server_ip> <port>
//!
//! And then start client in another terminal by running:
//!
//!     cargo run --example client <server_ip> <port>

use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, MrAccess, RCStream, Rdma, RdmaBuilder};
use std::{
    alloc::Layout,
    env,
    io::{self, Write},
    process::exit,
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
    {
        // use cursor to append data to lmr
        // use this in a scope to drop mr_cursor automatically
        let mut mr_cursor = lmr.as_mut_slice_cursor();
        let _num = mr_cursor.write(&[1_u8; 4])?;
        let _num = mr_cursor.write(&[1_u8; 4])?;
    }
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

/// request remote memory region and write data to it by RDMA ATOMIC_CAS
async fn request_then_write_cas(rdma: &Rdma) -> io::Result<()> {
    // alloc 8 bytes remote memory
    let mut rmr = rdma.request_remote_mr(Layout::new::<[u8; 8]>()).await?;
    let new_value = u64::from_le_bytes([1_u8; 8]);
    // read, compare with rmr and swap `old_value` with `new_value`
    rdma.atomic_cas(0, new_value, &mut rmr).await?;
    // send rmr's meta data to the remote end
    rdma.send_remote_mr(rmr).await?;
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
    println!("client start");

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("usage : cargo run --example client <server_ip> <port>");
        println!("input : {:?}", args);
        exit(-1);
    }
    let ip = args.get(1).unwrap().as_str();
    let port = args.get(2).unwrap().as_str();
    let addr = format!("{}:{}", ip, port);

    let mut rdma = RdmaBuilder::default().connect(addr.clone()).await.unwrap();
    println!("connected");
    send_data_to_server(&rdma).await.unwrap();
    send_data_with_imm_to_server(&rdma).await.unwrap();
    send_lmr_to_server(&rdma).await.unwrap();
    request_then_write(&rdma).await.unwrap();
    request_then_write_with_imm(&rdma).await.unwrap();
    request_then_write_cas(&rdma).await.unwrap();
    println!("client done");

    // create new `Rdma`s (connections) that has the same `mr_allocator` and `event_listener` as parent
    for _ in 0..3 {
        let rdma = rdma.new_connect(addr.clone()).await.unwrap();
        println!("connected");
        send_data_to_server(&rdma).await.unwrap();
        send_data_with_imm_to_server(&rdma).await.unwrap();
        send_lmr_to_server(&rdma).await.unwrap();
        request_then_write(&rdma).await.unwrap();
        request_then_write_with_imm(&rdma).await.unwrap();
        request_then_write_cas(&rdma).await.unwrap();
    }
    let mut stream: RCStream = rdma.into();
    rcstream_send(&mut stream).await.unwrap();
    rcstream_recv(&mut stream).await.unwrap();
    println!("client done");
}
